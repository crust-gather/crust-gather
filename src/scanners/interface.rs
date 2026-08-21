use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use chrono::Utc;
use futures::future::join_all;
use futures::{StreamExt, TryStreamExt as _};
use kube::Api;
use kube::api::WatchEvent;
use kube::core::gvk::ParseGroupVersionError;
use kube::core::params::{ListParams, WatchParams};
use kube::core::{ResourceExt, Status};
use serde::Serialize;
use serde::de::DeserializeOwned;
use snafu::{ResultExt, Snafu, Whatever};
use std::fmt::Debug;
use std::future::Future;
use tokio::sync::Mutex;
use tracing::instrument;

use std::sync::Arc;
use std::time::Duration;
use trait_set::trait_set;

use crate::gather::config::Secrets;
use crate::gather::json_resource::{Format, JsonResource, ReadState};
use crate::gather::representation::{ArchivePath, Representation, TypeMetaGetter};
use crate::gather::writer::Writer;
use crate::scanners::type_meta::TypeMetaDefaulter;

trait_set! {
    pub trait Base = Clone + Debug;
    pub trait ThreadSafe = Send + Sync;
    pub trait SerDe = Serialize + DeserializeOwned;
    pub trait ResourceReq = Base + ThreadSafe + SerDe;
    pub trait ResourceThreadSafe = ResourceReq + ResourceExt + TypeMetaDefaulter;
}

/// File extension for serialization format.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    Debug,
    Hash,
    Default,
    derive_more::FromStr,
    derive_more::Display,
    serde::Serialize,
    serde::Deserialize,
)]
#[from_str(rename_all = "lowercase")]
#[display(rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum Extension {
    Json,
    #[default]
    Yaml,
}

impl Extension {
    /// Serialises a `Serialize` value to a string.
    /// JSON uses `serde_json`, YAML uses `serde-saphyr`.
    pub fn string<T: serde::Serialize>(&self, obj: &T) -> Result<String, Whatever> {
        match self {
            Self::Json => {
                Ok(serde_json::to_string(obj).whatever_context("failed to serialize to JSON")?)
            }
            Self::Yaml => {
                Ok(serde_saphyr::to_string(obj).whatever_context("failed to serialize to YAML")?)
            }
        }
    }

    /// Deserialises a `serde::de::DeserializeOwned` value.
    /// JSON uses `serde_json`, YAML uses `serde-saphyr`.
    pub fn from_slice<T: serde::de::DeserializeOwned>(&self, data: &[u8]) -> Result<T, Whatever> {
        match self {
            Self::Json => {
                Ok(serde_json::from_slice(data)
                    .whatever_context("failed to deserialize from JSON")?)
            }
            Self::Yaml => Ok(serde_saphyr::from_slice(data)
                .whatever_context("failed to deserialize from YAML")?),
        }
    }

    /// Deserialises an arbitrary Kubernetes object into a `JsonResourceExt`.
    ///
    /// JSON uses `serde_json` directly; YAML is first parsed to a `Value` via
    /// `serde_saphyr`, then wrapped with its raw text via `to_raw_value`.
    pub fn object(&self, data: &[u8], state: ReadState) -> Result<JsonResource, Whatever> {
        match self {
            Extension::Json => Ok(JsonResource::deserialize_state(
                &mut Format::new(state, *self),
                &mut serde_json::Deserializer::from_slice(data),
            )
            .whatever_context("failed to deserialize JSON resource")?),
            Extension::Yaml => Ok(serde_saphyr::with_deserializer_from_slice(data, |de| {
                JsonResource::deserialize_state(&mut Format::new(state, *self), de)
            })
            .whatever_context("failed to deserialize YAML resource")?),
        }
    }
}

/// Indicates failure of conversion to Expression
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum CollectError {
    #[snafu(display("Failed to list resources: {source}"))]
    List { source: kube::Error },

    #[snafu(display("Unable to parse group version for object: {source}"))]
    #[snafu(context(suffix(Collect)))]
    GroupVersion { source: ParseGroupVersionError },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum WatchError {
    #[snafu(display("Failed to watch object: {source}"))]
    Watch { source: kube::Error },

    #[snafu(display("Failed to sync object: {source}"))]
    Sync { source: Whatever },

    #[snafu(display("Failed to stream object events: {source}"))]
    Stream { source: Box<Status> },

    #[snafu(display("Unable to parse group version for object: {source}"))]
    GroupVersion { source: ParseGroupVersionError },
}

pub const ADDED_ANNOTATION: &str = "crust-gather.io/added";
pub const UPDATED_ANNOTATION: &str = "crust-gather.io/updated";
pub const DELETED_ANNOTATION: &str = "crust-gather.io/deleted";

#[async_trait]
/// Collect defines a trait for collecting Kubernetes object representations.
pub trait Collect<R: ResourceThreadSafe>: Send {
    /// Default retry policy - exponential backoff.
    /// Starts at 10ms, doubles each iteration, up to max of 60s.
    #[must_use]
    fn retry_policy() -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(10))
            .with_max_delay(Duration::from_secs(60))
            .without_max_times()
    }

    async fn retry<T, Fut, F>(&self, action: F) -> Result<T, Whatever>
    where
        T: Send,
        Fut: Future<Output = Result<T, Whatever>> + Send,
        F: FnMut() -> Fut + Send,
    {
        action.retry(Self::retry_policy()).await
    }

    /// Returns the Secrets instance to filter any secrets in the representation
    fn get_secrets(&self) -> Secrets;

    /// Returns the Writer instance for this scanner to write object
    /// representations to.
    fn get_writer(&self) -> Arc<Mutex<Writer>>;

    /// Returns suffix for the generic file extension.
    fn extension(&self) -> Extension;

    /// Constructs the path for storing the collected Kubernetes object.
    ///
    /// The path is constructed differently for cluster-scoped vs namespaced objects.
    /// Cluster-scoped objects are stored under `cluster/{api_version}/{kind}/{name}.json`.
    /// Namespaced objects are stored under `namespaces/{namespace}/{api_version}/{kind}/{name}.json`.
    ///
    /// Example output: `crust-gather/namespaces/default/pod/nginx-deployment-549849849849849849849
    fn path(&self, obj: &R) -> ArchivePath {
        ArchivePath::to_path(obj, self.resource().to_type_meta(), self.extension())
    }

    /// Filters objects based on their `GroupVersionKind` and the object itself.
    /// Returns true if the object should be included, false otherwise.
    fn filter(&self, object: &R) -> Result<bool, CollectError>;

    /// Converts the provided `DynamicObject` into a vector of Representation
    /// with YAML object data and output path for the object.
    #[instrument(skip_all, fields(
        kind = self.resource().to_type_meta().kind,
        apiVersion = self.resource().to_type_meta().api_version,
        name = object.name_any(),
        namespace = object.namespace(),
    ), err)]
    async fn representations(&self, object: &R) -> Result<Vec<Representation>, Whatever> {
        tracing::debug!("Collecting representation");

        let mut object = object.clone();
        object.default_type_meta(self.resource().to_type_meta());

        Ok(vec![
            Representation::new()
                .with_path(self.path(&object))
                .with_data(self.extension().string(&object)?.as_str()),
        ])
    }

    /// Returns the Kubernetes API client for the resource type this scanner handles.
    fn get_api(&self) -> Api<R>;

    /// Returns the `TypeMetaGetter` for the API resource type this scanner handles.
    /// Used to set the `TypeMeta` on the returned objects in the list,
    /// as the API server does not provide this data in the response.
    fn resource(&self) -> impl TypeMetaGetter;

    /// Lists Kubernetes objects of the type handled by this scanner, and set
    /// the `get_type_meta()` information on the objects. Objects are filtered
    /// before getting added to the result.
    #[instrument(skip_all, fields(kind = self.resource().to_type_meta().kind, apiVersion = self.resource().to_type_meta().api_version), err)]
    async fn list(&self) -> Result<Vec<R>, Whatever> {
        let data = self
            .get_api()
            .list(&ListParams::default())
            .await
            .map_err(|e| CollectError::List { source: e })
            .whatever_context("failed to list resources")?;

        Ok(data
            .items
            .into_iter()
            .filter_map(|o| self.filter(&o).ok()?.then_some(o))
            .collect())
    }

    /// Lists all object and collects representations for them.
    #[instrument(skip_all, err)]
    async fn collect(&self) -> Result<(), Whatever> {
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
        (|| async { self.collect().await })
            .retry(Self::retry_policy())
            .await
            .unwrap();
    }

    /// Retries watching representations using an exponential backoff with jitter.
    /// This helps handle transient errors and spreading load.
    async fn watch_retry(&self) {
        (|| async { self.watch_collect().await })
            .retry(Self::retry_policy())
            .await
            .unwrap();
    }

    /// Retries collecting representations using an exponential backoff with jitter.
    /// This helps handle transient errors and spreading load.
    async fn write_with_retry(&self, object: &R) -> Result<(), Whatever> {
        let representations = self
            .retry(|| async { self.representations(object).await })
            .await?;

        let writer = self.get_writer();
        for repr in representations {
            writer
                .lock()
                .await
                .store(&self.get_secrets().strip(&repr))
                .await
                .whatever_context("failed to store representation")?;
        }

        Ok(())
    }

    /// Collect objects from watch events, storing difference from original as a series of json pathes
    #[instrument(skip_all, err)]
    async fn watch_collect(&self) -> Result<(), WatchError> {
        self.collect().await.context(SyncSnafu)?;

        let mut stream = self
            .get_api()
            .watch(&WatchParams::default(), "0")
            .await
            .context(WatchSnafu)?
            .boxed();

        while let Some(e) = stream.try_next().await.context(WatchSnafu)? {
            let now = Utc::now().to_string();
            match e {
                WatchEvent::Added(mut obj) => {
                    obj.annotations_mut()
                        .insert(ADDED_ANNOTATION.to_string(), now);
                    self.sync_with_retry(&obj).await.context(SyncSnafu)?;
                }
                WatchEvent::Modified(mut obj) => {
                    obj.annotations_mut()
                        .insert(UPDATED_ANNOTATION.to_string(), now);
                    self.sync_with_retry(&obj).await.context(SyncSnafu)?;
                }
                WatchEvent::Deleted(mut obj) => {
                    obj.annotations_mut()
                        .insert(DELETED_ANNOTATION.to_string(), now);
                    self.sync_with_retry(&obj).await.context(SyncSnafu)?;
                }
                WatchEvent::Error(e) => Err(WatchError::Stream { source: e })?,
                WatchEvent::Bookmark(_) => (),
            }
        }

        Ok(())
    }

    /// Retries collecting representations using an exponential backoff with jitter.
    /// This helps handle transient errors and spreading load.
    #[instrument(skip_all, err, fields(name = obj.name_any(), namespace = obj.namespace(), gvk))]
    async fn sync_with_retry(&self, obj: &R) -> Result<(), Whatever> {
        let representations = self
            .retry(|| async { self.representations(obj).await })
            .await?;

        let writer = self.get_writer();
        for repr in representations {
            writer
                .lock()
                .await
                .sync(&self.get_secrets().strip(&repr))
                .await?;
        }

        Ok(())
    }
}
