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
use serde_json::value::{RawValue, to_raw_value};
use snafu::Whatever;

use crate::gather::reader::{NamedObject, ResultTable, Table};
use crate::gather::storage::Storage;
use crate::scanners::interface::{DELETED_ANNOTATION, Extension, UPDATED_ANNOTATION};

/// State threaded into deserialize [`JsonResourceExt`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadState {
    /// Watch read — always materialize the full `serde_json::Value`.
    Watch,
    /// List/table read — materialize the full `serde_json::Value` when `bool` is `true`.
    SelectorSet(bool),
    /// Load/get read — always materialize the full `serde_json::Value`.
    TableView,
}

#[derive(Default)]
pub enum Format {
    #[default]
    ValueFirst,
    TextFirst,
    OnlyText,
}

impl Format {
    pub fn new(state: ReadState, extension: Extension) -> Self {
        match extension {
            Extension::Yaml => Self::ValueFirst,
            Extension::Json => match state {
                ReadState::Watch | ReadState::SelectorSet(true) | ReadState::TableView => {
                    Self::TextFirst
                }
                ReadState::SelectorSet(false) => Self::OnlyText,
            },
        }
    }
}

/// A `Resource` backed by a [raw JSON value](Value).
///
/// The `metadata` field is extracted from the JSON on deserialization for efficient access
/// by the [`Resource`] trait methods. Since this type is used only in the reader path,
/// mutations to `metadata` via [`meta_mut`](Resource::meta_mut) are not reflected back
/// into the JSON representation.
#[derive(Clone, Default, derive_more::PartialEq)]
pub struct JsonResource {
    /// Parsed JSON representation of the Kubernetes object.
    pub json: Value,

    /// Raw JSON text, for direct serving.
    #[partial_eq(skip)]
    pub text: Box<RawValue>,

    /// Cached metadata extracted from `json["metadata"]` during construction/deserialization.
    pub metadata: ObjectMeta,
}

impl std::fmt::Debug for JsonResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonResourceExt")
            .field("json", &self.json)
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl<'de> Deserialize<'de> for JsonResource {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::deserialize_state(&mut Default::default(), deserializer)
    }
}

impl Serialize for JsonResource {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.text.serialize(serializer)
    }
}

impl JsonResource {
    /// Deserialises a raw JSON/YAML object into a [`JsonResource`], threaded with a
    /// [`Format`] that selects between value-first, text-first, and text-only materialisation.
    pub fn deserialize_state<'de, D>(format: &mut Format, deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match format {
            Format::ValueFirst => {
                let json: Value = Value::deserialize(deserializer)?;
                let text = to_raw_value(&json).map_err(serde::de::Error::custom)?;
                Ok(Self::new(json, text))
            }
            Format::TextFirst => {
                let text: Box<RawValue> = Box::deserialize(deserializer)?;
                let json = serde_json::from_str(text.get()).map_err(serde::de::Error::custom)?;
                Ok(Self::new(json, text))
            }
            Format::OnlyText => Ok(Self::new(
                Default::default(),
                Box::deserialize(deserializer)?,
            )),
        }
    }

    /// Creates a new [`JsonResourceExt`] wrapping a [`Value`] and its raw text,
    /// extracting the metadata from the `"metadata"` key if present.
    #[must_use]
    pub fn new(json: Value, text: Box<RawValue>) -> Self {
        let metadata = match &json {
            Value::Object(map) => map
                .get("metadata")
                .and_then(|m| serde_json::from_value(m.clone()).ok())
                .unwrap_or_default(),
            _ => ObjectMeta::default(),
        };
        Self {
            json,
            text,
            metadata,
        }
    }

    /// Build a table `WatchEvent` for this single object, leveraging cached
    /// metadata via `self.meta().annotations` (O(1)) instead of re-traversing
    /// the JSON tree through `annotations()` / `event()` chain.
    pub async fn table_watch_event(
        self,
        crd_path: Option<PathBuf>,
        list: NamedObject,
        storage: &Storage,
    ) -> Result<WatchEvent<ResultTable>, Whatever> {
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

impl Resource for JsonResource {
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
