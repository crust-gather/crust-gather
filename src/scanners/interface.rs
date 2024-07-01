use anyhow::{self, bail};
use async_trait::async_trait;
use futures::future::join_all;
use k8s_openapi::serde_json;
use kube::core::params::ListParams;
use kube::core::{ApiResource, DynamicObject, GroupVersionKind, ResourceExt, TypeMeta};
use kube::Api;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;

use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::Retry;
use trait_set::trait_set;

use crate::gather::config::Secrets;
use crate::gather::representation::{ArchivePath, Representation};
use crate::gather::writer::Writer;

trait_set! {
    pub trait Base = Clone + Debug;
    pub trait ThreadSafe = Send + Sync;
    pub trait SerDe = Serialize + DeserializeOwned;
    pub trait ResourceReq = Base + ThreadSafe + SerDe;
    pub trait ResourceThreadSafe = ResourceReq + ResourceExt;
}

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
        ArchivePath::to_path(obj, self.get_type_meta())
    }

    /// Filters objects based on their GroupVersionKind and the object itself.
    /// Returns true if the object should be included, false otherwise.
    fn filter(&self, object: &R) -> anyhow::Result<bool>;

    /// Converts the provided DynamicObject into a vector of Representation
    /// with YAML object data and output path for the object.
    async fn representations(&self, object: &R) -> anyhow::Result<Vec<Representation>> {
        log::debug!(
            "Collecting representation for {} {}/{}",
            self.get_type_meta().kind,
            object.namespace().unwrap_or_default(),
            object.name_any(),
        );

        let data = DynamicObject {
            types: Some(self.get_type_meta()),
            metadata: Default::default(),
            data: serde_json::to_value(object)?,
        };

        Ok(vec![Representation::new()
            .with_path(self.path(object))
            .with_data(serde_yaml::to_string(&data)?.as_str())])
    }

    /// Returns the Kubernetes API client for the resource type this scanner handles.
    fn get_api(&self) -> Api<R>;

    /// Returns the TypeMeta for the API resource type this scanner handles.
    /// Used to set the TypeMeta on the returned objects in the list,
    /// as the API server does not provide this data in the response.
    fn get_type_meta(&self) -> TypeMeta;

    /// Returns the ApiResource with the resource type for object
    fn get_api_resource(&self) -> anyhow::Result<ApiResource> {
        Ok(ApiResource::from_gvk(&GroupVersionKind::try_from(
            self.get_type_meta(),
        )?))
    }

    /// Lists Kubernetes objects of the type handled by this scanner, and set
    /// the get_type_meta() information on the objects. Objects are filtered
    /// before getting added to the result.
    async fn list(&self) -> anyhow::Result<Vec<R>> {
        let data = match self.get_api().list(&ListParams::default()).await {
            Ok(items) => items,
            Err(e) => {
                log::error!("Failed to list resources: {:?}", e);
                bail!(e)
            }
        };

        Ok(data
            .items
            .into_iter()
            .filter_map(|o| match self.filter(&o) {
                Ok(true) => Some(o),
                Ok(false) => None,
                Err(e) => {
                    log::error!(
                        "Unable to filter object {:?}: {:?}",
                        self.get_type_meta(),
                        e
                    );
                    None
                }
            })
            .collect())
    }

    /// Lists all object and collects representations for them.
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
}
