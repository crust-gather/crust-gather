use anyhow::{self, bail};
use async_trait::async_trait;
use futures::future::join_all;
use kube::Api;
use kube_core::params::ListParams;
use kube_core::{DynamicObject, GroupVersionKind, TypeMeta};
use serde_yaml;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::Retry;

use crate::gather::config::Secrets;
use crate::gather::writer::{Representation, Writer};

#[async_trait]
/// Collect defines a trait for collecting Kubernetes object representations.
pub trait Collect: Sync + Send {
    /// Default delay iterator - exponential backoff.
    /// Starts at 2ms, doubles each iteration, up to max of 60s.
    fn delay() -> impl Iterator<Item = Duration> + Send {
        ExponentialBackoff::from_millis(10).max_delay(Duration::from_secs(60))
    }

    /// Returns the Secrets instance to filter any secrets in the representation
    fn get_secrets(&self) -> Secrets;

    /// Returns the Writer instance for this scanner to write object
    /// representations to.
    fn get_writer(&self) -> Arc<Mutex<Writer>>;

    /// Returns the path where the representation of the given object
    /// should be stored.
    fn path(&self, object: &DynamicObject) -> PathBuf;

    /// Filters objects based on their GroupVersionKind and the object itself.
    /// Returns true if the object should be included, false otherwise.
    fn filter(&self, gvk: &GroupVersionKind, object: &DynamicObject) -> bool;

    /// Converts the provided DynamicObject into a vector of Representation
    /// with YAML object data and output path for the object.
    async fn representations(&self, object: &DynamicObject) -> anyhow::Result<Vec<Representation>> {
        log::debug!(
            "Collecting representation for {} {}/{}",
            object.types.clone().unwrap().kind,
            object.metadata.clone().namespace.unwrap_or_default(),
            object.metadata.clone().name.unwrap()
        );

        Ok(vec![Representation::new()
            .with_path(self.path(object))
            .with_data(serde_yaml::to_string(&object)?.as_str())])
    }

    /// Returns the Kubernetes API client for the resource type this scanner handles.
    fn get_api(&self) -> Api<DynamicObject>;

    /// Returns the TypeMeta for the API resource type this scanner handles.
    /// Used to set the TypeMeta on the returned objects in the list,
    /// as the API server does not provide this data in the response.
    fn get_type_meta(&self) -> TypeMeta;

    /// Lists Kubernetes objects of the type handled by this scanner, and set
    /// the get_type_meta() information on the objects. Objects are filtered
    /// before getting added to the result.
    async fn list(&self) -> anyhow::Result<Vec<DynamicObject>> {
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
            .map(|o| DynamicObject {
                types: Some(self.get_type_meta()),
                ..o
            })
            .filter(|o| {
                self.filter(
                    &o.types
                        .as_ref()
                        .unwrap()
                        .try_into()
                        .expect("incomplete TypeMeta provided"),
                    o,
                )
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
            .unwrap()
    }

    /// Retries collecting representations using an exponential backoff with jitter.
    /// This helps handle transient errors and spreading load.
    async fn write_with_retry(&self, object: &DynamicObject) -> anyhow::Result<()> {
        let representations = Retry::spawn(Self::delay(), || async {
            self.representations(object).await
        })
        .await?;

        let writer = self.get_writer();
        for repr in representations {
            writer
                .lock()
                .unwrap()
                .store(&self.get_secrets().strip(&repr))?
        }

        Ok(())
    }
}
