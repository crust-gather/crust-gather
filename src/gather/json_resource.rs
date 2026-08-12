//! A `Resource` backed by a raw JSON value.
//!
//! [`JsonResourceExt`] holds a [`Value`] (aliased to [`Value`]) and is intended
//! to represent a Kubernetes object whose shape is only known at runtime, encoded as JSON.
//!
//! Metadata is extracted from the `"metadata"` key on construction/deserialization and cached
//! for efficient access via the [`Resource`] trait methods.

use std::borrow::Cow;
use std::path::PathBuf;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::ResourceExt as _;
use kube::api::WatchEvent;
use kube::core::{ApiResource, DynamicResourceScope, Resource};
use serde::de::Deserializer;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::gather::reader::{NamedObject, ResultTable, Table};
use crate::gather::storage::Storage;
use crate::scanners::interface::{DELETED_ANNOTATION, UPDATED_ANNOTATION};

/// A `Resource` backed by a [raw JSON value](Value).
///
/// The `metadata` field is extracted from the JSON on deserialization for efficient access
/// by the [`Resource`] trait methods. Since this type is used only in the reader path,
/// mutations to `metadata` via [`meta_mut`](Resource::meta_mut) are not reflected back
/// into the JSON representation.
#[derive(Clone, Default, PartialEq)]
pub struct JsonResourceExt {
    /// The raw JSON representation of the Kubernetes object.
    pub json: Value,

    /// Cached metadata extracted from `json["metadata"]` during construction/deserialization.
    pub metadata: ObjectMeta,
}

impl std::fmt::Debug for JsonResourceExt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonResourceExt")
            .field("json", &self.json)
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl<'de> Deserialize<'de> for JsonResourceExt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let json = Value::deserialize(deserializer)?;
        Ok(Self::new(json))
    }
}

impl Serialize for JsonResourceExt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.json.serialize(serializer)
    }
}

impl JsonResourceExt {
    /// Creates a new [`JsonResourceExt`] wrapping a [`Value`], extracting the metadata
    /// from the `"metadata"` key if present.
    #[must_use]
    pub fn new(json: Value) -> Self {
        let metadata = match &json {
            Value::Object(map) => map
                .get("metadata")
                .and_then(|m| serde_json::from_value(m.clone()).ok())
                .unwrap_or_default(),
            _ => ObjectMeta::default(),
        };
        Self { json, metadata }
    }

    /// Build a table `WatchEvent` for this single object, leveraging cached
    /// metadata via `self.meta().annotations` (O(1)) instead of re-traversing
    /// the JSON tree through `annotations()` / `event()` chain.
    pub async fn table_watch_event(
        self,
        crd_path: Option<PathBuf>,
        list: NamedObject,
        storage: &Storage,
    ) -> anyhow::Result<WatchEvent<ResultTable>> {
        let annotations = self.annotations().clone();
        let table = Table::new(crd_path, list, vec![self], storage).await?;
        let result = table.to_result_table()?;
        let event = match annotations {
            annotations if annotations.contains_key(DELETED_ANNOTATION) => {
                WatchEvent::Deleted(result)
            }
            annotations if annotations.contains_key(UPDATED_ANNOTATION) => {
                WatchEvent::Modified(result)
            }
            _ => WatchEvent::Added(result),
        };
        Ok(event)
    }
}

impl Resource for JsonResourceExt {
    // The object's type information is only known at runtime, so the dynamic type is `ApiResource`.
    type DynamicType = ApiResource;
    // The scope is indeterminate for a raw JSON object.
    type Scope = DynamicResourceScope;

    fn kind(dt: &Self::DynamicType) -> Cow<'_, str> {
        dt.kind.as_str().into()
    }

    fn group(dt: &Self::DynamicType) -> Cow<'_, str> {
        dt.group.as_str().into()
    }

    fn version(dt: &Self::DynamicType) -> Cow<'_, str> {
        dt.version.as_str().into()
    }

    fn api_version(dt: &Self::DynamicType) -> Cow<'_, str> {
        dt.api_version.as_str().into()
    }

    fn plural(dt: &Self::DynamicType) -> Cow<'_, str> {
        dt.plural.as_str().into()
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}
