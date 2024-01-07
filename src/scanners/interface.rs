use anyhow;
use async_trait::async_trait;
use kube::Api;
use kube_core::params::ListParams;
use kube_core::{DynamicObject, GroupVersionKind, TypeMeta};
use serde_yaml;
use std::path::PathBuf;

use crate::gather::writer::Representation;

#[async_trait]
/// Collect defines a trait for collecting Kubernetes object representations.
pub trait Collect: Sync + Send {
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
        let data = self.get_api().list(&ListParams::default()).await?;

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
    async fn collect(&self) -> anyhow::Result<Vec<Representation>> {
        let mut representations = vec![];
        for obj in self.list().await? {
            representations.append(&mut self.representations(&obj).await?);
        }

        Ok(representations)
    }
}
