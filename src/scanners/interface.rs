use anyhow;
use async_trait::async_trait;
use futures::future::join_all;
use futures::{StreamExt, TryStreamExt as _};
use k8s_openapi::chrono::Utc;
use k8s_openapi::serde_json;
use kube::api::WatchEvent;
use kube::core::gvk::ParseGroupVersionError;
use kube::core::params::ListParams;
use kube::core::{DynamicObject, ErrorResponse, ResourceExt};
use kube::Api;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use thiserror::Error;
use tracing::instrument;

use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::Retry;
use trait_set::trait_set;

use crate::gather::config::Secrets;
use crate::gather::representation::{ArchivePath, Representation, TypeMetaGetter};
use crate::gather::writer::Writer;

trait_set! {
    pub trait Base = Clone + Debug;
    pub trait ThreadSafe = Send + Sync;
    pub trait SerDe = Serialize + DeserializeOwned;
    pub trait ResourceReq = Base + ThreadSafe + SerDe;
    pub trait ResourceThreadSafe = ResourceReq + ResourceExt;
}

/// Indicates failure of conversion to Expression
#[derive(Debug, Error)]
pub enum CollectError {
    #[error("Failed to list resources: {0}")]
    List(kube::Error),

    #[error("Unable to parse froup versoin for object: {0}")]
    GroupVersion(ParseGroupVersionError),
}

#[derive(Debug, Error)]
pub enum WatchError {
    #[error("Failed to watch object: {0}")]
    Watch(#[from] kube::Error),

    #[error("Failed to sync object: {0}")]
    Sync(#[from] anyhow::Error),

    #[error("Failed to stream object events: {0}")]
    Stream(#[from] ErrorResponse),

    #[error("Unable to parse froup versoin for object: {0}")]
    GroupVersion(#[from] ParseGroupVersionError),
}

pub const ADDED_ANNOTATION: &str = "crust-gather.io/added";
pub const UPDATED_ANNOTATION: &str = "crust-gather.io/updated";
pub const DELETED_ANNOTATION: &str = "crust-gather.io/deleted";

#[async_trait]
/// Collect defines a trait for collecting Kubernetes object representations.
pub trait Collect<R: ResourceThreadSafe>: Send {
    /// Default delay iterator - exponential backoff.
    /// Starts at 10ms, doubles each iteration, up to max of 60s.
    fn delay() -> impl Iterator<Item = Duration> + Send {
        ExponentialBackoff::from_millis(10).max_delay(Duration::from_secs(60))
    }

    /// Returns the Secrets instance to filter any secrets in the representation
    fn get_secrets(&self) -> Secrets;

    /// Returns the Writer instance for this scanner to write object
    /// representations to.
    fn get_writer(&self) -> Arc<Mutex<Writer>>;

    /// Constructs the path for storing the collected Kubernetes object.
    ///
    /// The path is constructed differently for cluster-scoped vs namespaced objects.
    /// Cluster-scoped objects are stored under `cluster/{api_version}/{kind}/{name}.yaml`.
    /// Namespaced objects are stored under `namespaces/{namespace}/{api_version}/{kind}/{name}.yaml`.
    ///
    /// Example output: `crust-gather/namespaces/default/pod/nginx-deployment-549849849849849849849
    fn path(&self, obj: &R) -> ArchivePath {
        ArchivePath::to_path(obj, self.resource().to_type_meta())
    }

    /// Filters objects based on their GroupVersionKind and the object itself.
    /// Returns true if the object should be included, false otherwise.
    fn filter(&self, object: &R) -> Result<bool, CollectError>;

    /// Converts the provided DynamicObject into a vector of Representation
    /// with YAML object data and output path for the object.
    #[instrument(skip_all, fields(
        kind = self.resource().to_type_meta().kind,
        apiVersion = self.resource().to_type_meta().api_version,
        name = object.name_any(),
        namespace = object.namespace(),
    ), err)]
    async fn representations(&self, object: &R) -> anyhow::Result<Vec<Representation>> {
        tracing::debug!("Collecting representation");

        let data = DynamicObject {
            types: Some(self.resource().to_type_meta()),
            metadata: Default::default(),
            data: serde_json::to_value(object)?,
        };

        Ok(vec![Representation::new()
            .with_path(self.path(object))
            .with_data(serde_yaml::to_string(&data)?.as_str())])
    }

    /// Returns the Kubernetes API client for the resource type this scanner handles.
    fn get_api(&self) -> Api<R>;

    /// Returns the TypeMetaGetter for the API resource type this scanner handles.
    /// Used to set the TypeMeta on the returned objects in the list,
    /// as the API server does not provide this data in the response.
    fn resource(&self) -> impl TypeMetaGetter;

    /// Lists Kubernetes objects of the type handled by this scanner, and set
    /// the get_type_meta() information on the objects. Objects are filtered
    /// before getting added to the result.
    #[instrument(skip_all, fields(kind = self.resource().to_type_meta().kind, apiVersion = self.resource().to_type_meta().api_version), err)]
    async fn list(&self) -> anyhow::Result<Vec<R>> {
        let data = self
            .get_api()
            .list(&ListParams::default())
            .await
            .map_err(CollectError::List)?;

        Ok(data
            .items
            .into_iter()
            .filter_map(|o| self.filter(&o).ok()?.then_some(o))
            .collect())
    }

    /// Lists all object and collects representations for them.
    #[instrument(skip_all, err)]
    async fn collect(&self) -> anyhow::Result<()> {
        join_all(
            self.list()
                .await?
                .iter()
                .map(|c| async { self.write_with_retry(c).await }),
        )
        .await;

        Ok(())
    }

    /// Retries collecting representations using an exponential backoff with jitter.
    /// This helps handle transient errors and spreading load.
    async fn collect_retry(&self) {
        Retry::spawn(Self::delay(), || async { self.collect().await })
            .await
            .unwrap();
    }

    /// Retries watching representations using an exponential backoff with jitter.
    /// This helps handle transient errors and spreading load.
    async fn watch_retry(&self) {
        Retry::spawn(Self::delay(), || async { self.watch_collect().await })
            .await
            .unwrap();
    }

    /// Retries collecting representations using an exponential backoff with jitter.
    /// This helps handle transient errors and spreading load.
    async fn write_with_retry(&self, object: &R) -> anyhow::Result<()> {
        let representations = Retry::spawn(Self::delay(), || async {
            self.representations(object).await
        })
        .await?;

        let writer = self.get_writer();
        for repr in representations {
            writer
                .lock()
                .unwrap()
                .store(&self.get_secrets().strip(&repr))?;
        }

        Ok(())
    }

    /// Collect objects from watch events, storing difference from original as a series of json pathes
    #[instrument(skip_all, err)]
    async fn watch_collect(&self) -> Result<(), WatchError> {
        let mut stream = self
            .get_api()
            .watch(&Default::default(), "0")
            .await?
            .boxed();

        while let Some(e) = stream.try_next().await? {
            let now = Utc::now().to_string();
            match e {
                WatchEvent::Added(obj) => {
                    let mut obj = obj.clone();
                    obj.annotations_mut()
                        .insert(ADDED_ANNOTATION.to_string(), now);
                    self.sync_with_retry(&obj).await?
                }
                WatchEvent::Modified(obj) => {
                    let mut obj = obj.clone();
                    obj.annotations_mut()
                        .insert(UPDATED_ANNOTATION.to_string(), now);
                    self.sync_with_retry(&obj).await?
                }
                WatchEvent::Deleted(obj) => {
                    let mut obj = obj.clone();
                    obj.annotations_mut()
                        .insert(DELETED_ANNOTATION.to_string(), now);
                    self.sync_with_retry(&obj).await?
                }
                WatchEvent::Error(e) => Err(WatchError::Stream(e))?,
                WatchEvent::Bookmark(_) => (),
            }
        }

        Ok(())
    }

    /// Retries collecting representations using an exponential backoff with jitter.
    /// This helps handle transient errors and spreading load.
    async fn sync_with_retry(&self, object: &R) -> anyhow::Result<()> {
        let representations = Retry::spawn(Self::delay(), || async {
            self.representations(object).await
        })
        .await?;

        let writer = self.get_writer();
        for repr in representations {
            writer
                .lock()
                .unwrap()
                .sync(&self.get_secrets().strip(&repr))?;
        }

        Ok(())
    }
}
