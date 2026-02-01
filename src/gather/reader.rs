use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    hash::Hash,
    io::{self, BufRead as _, Read},
    path::PathBuf,
    pin::pin,
    str::FromStr,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use anyhow::bail;
use chrono::{DateTime, Utc};
use futures::{StreamExt as _, TryStreamExt as _, future, stream};
use json_patch::{AddOperation, PatchOperation, ReplaceOperation, patch};
use jsonptr::PointerBuf;
use k8s_openapi::{
    apiextensions_apiserver::pkg::apis::apiextensions::v1::{
        CustomResourceColumnDefinition, CustomResourceDefinition, CustomResourceDefinitionSpec,
        CustomResourceDefinitionVersion,
    },
    serde_json::{self, json},
};
use kube::{
    ResourceExt,
    api::{GroupVersionResource, PartialObjectMetaExt as _, WatchEvent},
    client::{APIGroupDiscovery, APIGroupDiscoveryList, APIResourceDiscovery, APIVersionDiscovery},
    core::{DynamicObject, Resource, TypeMeta},
};
use oci_client::{Client, Reference, manifest::OciDescriptor, secrets::RegistryAuth};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json_path::JsonPath;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync,
};
use tracing::instrument;

use crate::{gather::storage::Storage, scanners::interface::{ADDED_ANNOTATION, DELETED_ANNOTATION, UPDATED_ANNOTATION}};

use super::{
    representation::{
        ArchivePath, Container, LogGroup, NamespaceName, NamespacedName, TypeMetaGetter,
    },
    selector::Selector,
    writer::Archive,
};

const ADDED_PATH: [&str; 3] = ["metadata", "annotations", ADDED_ANNOTATION];
const UPDATED_PATH: [&str; 3] = ["metadata", "annotations", UPDATED_ANNOTATION];
const DELETED_PATH: [&str; 3] = ["metadata", "annotations", DELETED_ANNOTATION];

static PREDEFINED_TABLES: OnceLock<BTreeMap<String, Vec<CustomResourceColumnDefinition>>> =
    OnceLock::new();

fn predefined_tables() -> &'static BTreeMap<String, Vec<CustomResourceColumnDefinition>> {
    PREDEFINED_TABLES.get_or_init(|| {
        let mut map = BTreeMap::new();
        map.insert(
            "events".into(),
            vec![
                CustomResourceColumnDefinition {
                    name: "lastTimestamp".into(),
                    json_path: ".lastTimestamp".into(),
                    ..Default::default()
                },
                CustomResourceColumnDefinition {
                    name: "type".into(),
                    json_path: ".type".into(),
                    ..Default::default()
                },
                CustomResourceColumnDefinition {
                    name: "reason".into(),
                    json_path: ".reason".into(),
                    ..Default::default()
                },
                CustomResourceColumnDefinition {
                    name: "object".into(),
                    json_path: ".metadata.name".into(),
                    ..Default::default()
                },
                CustomResourceColumnDefinition {
                    name: "message".into(),
                    json_path: ".message".into(),
                    ..Default::default()
                },
            ],
        );
        map
    })
}

#[derive(Deserialize, Clone)]
pub struct Destination {
    server: String,
}

impl Destination {
    pub fn get_server(&self) -> &str {
        &self.server
    }
}

#[derive(Deserialize, Clone)]
pub struct Get {
    server: String,
    namespace: Option<String>,
    name: String,
    group: Option<String>,
    version: String,
    kind: String,
}

impl Get {
    pub fn get_server(&self) -> &str {
        &self.server
    }
}

impl NamespacedName for &Get {
    fn name(&self) -> Option<String> {
        self.name.clone().into()
    }

    fn namespace(&self) -> Option<String> {
        self.namespace.clone()
    }
}

#[derive(Deserialize, Clone)]
pub struct Log {
    container: Container,
    previous: Option<bool>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct List {
    pub server: String,
    namespace: Option<String>,
    group: Option<String>,
    version: String,
    kind: String,
}

impl List {
    pub fn get_server(&self) -> &str {
        &self.server
    }
}

#[derive(Serialize, Deserialize)]
pub struct ObjectValueList {
    #[serde(flatten)]
    type_meta: TypeMeta,
    items: Vec<DynamicObject>,
}

impl ObjectValueList {
    pub fn new(list: NamedObject, items: Vec<DynamicObject>) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: list.named_resource.list_kind.clone(),
                api_version: list.to_type_meta().api_version,
            },
            items,
        }
    }
}

#[derive(Clone)]
pub struct Table {
    pub data: Vec<TablePath>,
    pub items: Vec<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TablePath {
    pub column: CustomResourceColumnDefinition,
    pub json_path: Option<JsonPath>,
}

impl TablePath {
    fn new(column: &CustomResourceColumnDefinition) -> Self {
        let json_path = format!("${}", column.json_path.replace(r"\.", r"."));
        let json_path = match JsonPath::parse(&json_path) {
            Ok(json_path) => Some(json_path),
            Err(e) => {
                tracing::debug!("unable to parse json path for {json_path}: {e:?}");
                None
            }
        };
        Self {
            column: column.clone(),
            json_path,
        }
    }

    fn to_definition(&self) -> serde_json::Value {
        json!({
            "name": self.column.name,
            "type": self.column.type_,
            "format": self.column.format.clone().unwrap_or_default(),
            "description": self.column.description.clone().unwrap_or_default(),
            "priority": self.column.priority.unwrap_or_default(),
        })
    }
}

impl Table {
    async fn new(
        crd_path: PathBuf,
        list: NamedObject,
        items: Vec<impl Serialize>,
        storage: &Storage,
    ) -> anyhow::Result<Self> {
        let mut data = vec![];

        data.extend(Table::table_entries(storage, crd_path, list).await?);
        let items: anyhow::Result<Vec<serde_json::Value>> = items
            .into_iter()
            .map(|i| serde_json::to_value(i).map_err(Into::into))
            .collect();
        Ok(Self {
            data,
            items: items?,
        })
    }

    async fn table_entries(
        storage: &Storage,
        crd_path: PathBuf,
        list: NamedObject,
    ) -> anyhow::Result<Vec<TablePath>> {
        let crd: CustomResourceDefinition = match crd_path.is_file() {
            true => {
                let mut file = vec![];
                storage.read(crd_path, &mut file).await?;
                serde_yaml::from_slice(&file)?
            }
            false => match predefined_tables().get(&list.named_resource.resource) {
                Some(columns) => CustomResourceDefinition {
                    spec: CustomResourceDefinitionSpec {
                        versions: vec![CustomResourceDefinitionVersion {
                            name: list.named_resource.version.clone(),
                            additional_printer_columns: Some(columns.clone()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    ..Default::default()
                },
                None => Default::default(),
            },
        };

        let crd_version = crd
            .spec
            .versions
            .iter()
            .find(|crd| crd.name == list.named_resource.version);

        let table_entries = crd_version
            .map(|version| version.additional_printer_columns.clone())
            .unwrap_or_default()
            .map(|columns| columns.iter().map(TablePath::new).collect())
            .unwrap_or_default();

        Ok(
            match predefined_tables().get(&list.named_resource.resource) {
                Some(_) => table_entries,
                None => {
                    let mut data = vec![TablePath {
                        column: CustomResourceColumnDefinition {
                            name: "Name".to_string(),
                            type_: "string".to_string(),
                            ..Default::default()
                        },
                        json_path: JsonPath::parse("$.metadata.name").ok(),
                    }];
                    data.extend(table_entries);
                    data
                }
            },
        )
    }

    fn to_row(&self, obj: impl Serialize) -> anyhow::Result<serde_json::Value> {
        let Table { data: rows, .. } = self;
        let obj = serde_json::to_value(obj)?;
        let cells: Vec<&serde_json::Value> = rows
            .iter()
            .filter_map(|r| {
                r.json_path
                    .as_ref()
                    .map(|json_path| json_path.query(&obj).first())
                    .unwrap_or_default()
            })
            .collect();

        Ok(json!({
            "cells": cells,
            "object": serde_json::from_value::<DynamicObject>(obj)?.metadata.into_response_partial::<DynamicObject>(),
        }))
    }

    fn definitions(&self) -> Vec<serde_json::Value> {
        self.data.iter().map(|r| r.to_definition()).collect()
    }

    fn rows(&self) -> anyhow::Result<Vec<serde_json::Value>> {
        let Table { items, .. } = self;
        items.iter().map(|i| self.to_row(i)).collect()
    }

    fn to_value(&self) -> anyhow::Result<serde_json::Value> {
        Ok(json!({
            "kind": "Table",
            "apiVersion": "meta.k8s.io/v1",
            "metadata": {
                "resourceVersion": "1"
            },
            "columnDefinitions": self.definitions(),
            "rows": self.rows()?,
        }))
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct Watch {
    pub watch: Option<bool>,
}

trait GatherObject: ResourceExt + Sized + Serialize {
    fn watch_event(self) -> WatchEvent<Self> {
        self.event()(self)
    }

    fn event<K>(&self) -> fn(K) -> WatchEvent<K> {
        match self.annotations() {
            annotations if annotations.contains_key(DELETED_ANNOTATION) => WatchEvent::Deleted::<K>,
            annotations if annotations.contains_key(UPDATED_ANNOTATION) => {
                WatchEvent::Modified::<K>
            }
            _ => WatchEvent::Added::<K>,
        }
    }

    fn last_sync_timestamp(&self) -> Option<DateTime<Utc>> {
        let a = self.annotations();
        match a
            .get(UPDATED_ANNOTATION)
            .or(a.get(DELETED_ANNOTATION))
            .or(a.get(ADDED_ANNOTATION))
        {
            Some(last_sync_timestamp) => {
                serde_json::from_str(&format!("\"{last_sync_timestamp}\"")).ok()
            }
            // Handling of pre-record feature versions
            None => Some(Default::default()),
        }
    }

    fn older(&self, before: DateTime<Utc>) -> bool {
        let passed = || Some(before >= self.last_sync_timestamp()?);
        passed().is_some_and(|is_true| is_true)
    }

    fn deleted(&self) -> bool {
        self.annotations().contains_key(DELETED_ANNOTATION)
    }

    async fn table_watch_event(
        &self,
        crd_path: PathBuf,
        list: NamedObject,
        storage: &Storage,
    ) -> anyhow::Result<serde_json::Value> {
        Ok(serde_json::to_value(self.event()(
            Table::new(crd_path, list, vec![&self], storage)
                .await?
                .to_value()?,
        ))?)
    }
}

impl<T: Resource + Serialize> GatherObject for T {}

// resource is the plural lowercase version of the resource name (exposed to the k8s api)         : configmaps
// singular is the singular lowercase version of the resource name (used to get retrieve data)    : configmap
// kind is the PascalCase version of the resource name (not used)                                 : ConfigMap
// list_kind is the PascalCase version of the resource name + List                                : ConfigMapList
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct NamedResource {
    group: Option<String>,
    version: String,
    resource: String,
    singular: String,
    list_kind: String,
}

impl From<GroupVersionResource> for NamedResource {
    fn from(val: GroupVersionResource) -> Self {
        // Doing best effort to convert arbitrary object plural to singular.
        let singular = if let Some(stripped) = val.resource.strip_suffix("uses") {
            format!("{stripped}us")
        } else if let Some(stripped) = val.resource.strip_suffix("sses") {
            format!("{stripped}ss")
        } else if let Some(stripped) = val.resource.strip_suffix("ies") {
            format!("{stripped}y")
        } else {
            val.resource.trim_end_matches('s').to_string()
        };

        // Doing best effort to convert arbitrary object list to a typed list.
        // Works best for core types, generating SecretList instead of just List.
        let mut kind = singular.clone();
        let list_kind = format!("{}{kind}List", kind.remove(0).to_uppercase(),);

        NamedResource {
            group: if val.group.is_empty() {
                None
            } else {
                Some(val.group)
            },
            version: val.version,
            resource: val.resource,
            singular,
            list_kind,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct NamedResources(HashMap<GroupVersionResource, NamedResource>);

impl NamedResources {
    async fn from_discovery_file(path: PathBuf, storage: &Storage) -> anyhow::Result<Self> {
        let mut object = vec![];
        storage.read(path.clone(), &mut object).await?;

        let discovery = serde_yaml::from_slice(&object)?;
        Ok(Self::from_discovery_groups(discovery))
    }

    fn from_discovery_groups(groups: APIGroupDiscoveryList) -> Self {
        let mut resources = Self::default();

        for api_group in groups.items {
            resources.insert_group(api_group);
        }

        resources
    }

    fn insert_group(&mut self, api_group: APIGroupDiscovery) -> Option<()> {
        self.insert_discovery_versions(&api_group.metadata?.name?, api_group.versions);
        Some(())
    }

    fn insert_discovery_versions(&mut self, group: &str, versions: Vec<APIVersionDiscovery>) {
        for api_version in versions {
            self.insert_version(group, api_version);
        }
    }

    fn insert_version(&mut self, group: &str, api_version: APIVersionDiscovery) -> Option<()> {
        self.insert_discovery_resources(group, &api_version.version?, api_version.resources);
        Some(())
    }

    fn insert_discovery_resources(
        &mut self,
        group: &str,
        version: &str,
        resources: Vec<APIResourceDiscovery>,
    ) {
        for res in resources {
            self.insert_resource(group, version, res);
        }
    }

    fn parse_discovery_resource(
        group: &str,
        version: &str,
        resource: APIResourceDiscovery,
    ) -> Option<NamedResource> {
        let gvk = resource.response_kind?;
        let kind = gvk.kind?;

        Some(NamedResource {
            group: Some(gvk.group.unwrap_or_else(|| group.to_string())),
            version: gvk.version.unwrap_or_else(|| version.to_string()),
            list_kind: format!("{kind}List"),
            resource: resource.resource?,
            singular: resource.singular_resource.unwrap_or_default(),
        })
    }

    fn get(&self, gvr: &GroupVersionResource) -> Option<&NamedResource> {
        self.0.get(gvr)
    }

    fn extend(&mut self, other: NamedResources) {
        self.0.extend(other.0);
    }

    fn insert_resource(
        &mut self,
        group: &str,
        version: &str,
        res: APIResourceDiscovery,
    ) -> Option<NamedResource> {
        let res = Self::parse_discovery_resource(group, version, res)?;
        tracing::debug!("{res:?}");

        self.0.insert(
            GroupVersionResource::gvr(
                res.group.as_deref().unwrap_or_default(),
                &res.version,
                &res.resource,
            ),
            res,
        )
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NamedObject {
    named_resource: NamedResource,
    namespace: Option<String>,
    name: Option<String>,
}

impl NamedObject {
    pub fn get_path(&self) -> ArchivePath {
        ArchivePath::new_path(self, self.to_type_meta())
    }

    pub fn get_crd_path(&self) -> Option<ArchivePath> {
        self.named_resource.group.as_ref().map(|group| {
            ArchivePath::new_path(
                NamespaceName::new(
                    Some(format!("{}.{}", self.named_resource.resource, group)),
                    None,
                ),
                TypeMeta::resource::<CustomResourceDefinition>(),
            )
        })
    }

    pub fn get_logs_path(&self, log: &Log) -> ArchivePath {
        ArchivePath::new_logs(
            self,
            self.to_type_meta(),
            match log.previous {
                Some(true) => LogGroup::Previous(log.container.clone()),
                _ => LogGroup::Current(log.container.clone()),
            },
        )
    }
}

impl TypeMetaGetter for NamedObject {
    fn to_type_meta(&self) -> TypeMeta {
        match &self.named_resource.group {
            Some(group) => TypeMeta {
                api_version: format!("{}/{}", group, self.named_resource.version),
                kind: self.named_resource.singular.clone(),
            },
            None => TypeMeta {
                api_version: self.named_resource.version.clone(),
                kind: self.named_resource.singular.clone(),
            },
        }
    }
}

impl NamespacedName for &NamedObject {
    fn name(&self) -> Option<String> {
        self.name.clone()
    }

    fn namespace(&self) -> Option<String> {
        self.namespace.clone()
    }
}

#[derive(Clone)]
pub struct ArchiveReader {
    archive: Archive,
    named_resources: Arc<NamedResources>,
}

impl ArchiveReader {
    pub async fn new(archive: Archive, storage: &Storage) -> Self {
        let mut named_resources = match NamedResources::from_discovery_file(
            archive.join(ArchivePath::Custom("apis.json".into())),
            storage,
        )
        .await
        {
            Ok(named_resources) => named_resources,
            Err(e) => {
                tracing::error!("Fail parsing apis.json : {e:?}");
                NamedResources::default()
            }
        };

        match NamedResources::from_discovery_file(
            archive.join(ArchivePath::Custom("api.json".into())),
            storage,
        )
        .await
        {
            Ok(nrs) => named_resources.extend(nrs),
            Err(e) => {
                tracing::error!("Fail parsing api.json : {e:?}");
            }
        };

        Self {
            archive,
            named_resources: Arc::new(named_resources),
        }
    }

    pub fn join(&self, path: ArchivePath) -> PathBuf {
        self.archive.join(path)
    }

    pub fn named_object_from_list(&self, list: List) -> NamedObject {
        let gvr = GroupVersionResource::gvr(
            &list.group.clone().unwrap_or_default(),
            &list.version,
            &list.kind,
        );
        let named_resource = self
            .named_resources
            .get(&gvr)
            .cloned()
            .unwrap_or(gvr.into());

        NamedObject {
            named_resource,
            namespace: list.namespace,
            name: None,
        }
    }

    pub fn named_object_from_get(&self, get: Get) -> NamedObject {
        let gvr = GroupVersionResource::gvr(
            &get.group.clone().unwrap_or_default(),
            &get.version,
            &get.kind,
        );
        let named_resource = self
            .named_resources
            .get(&gvr)
            .cloned()
            .unwrap_or(gvr.into());

        NamedObject {
            named_resource,
            namespace: get.namespace,
            name: Some(get.name),
        }
    }
}

#[derive(Clone)]
pub struct Reader {
    pub archive: ArchiveReader,
    diff: Duration,
    objects_state: Arc<Mutex<HashMap<PathBuf, DynamicObject>>>,
    next_patch_time: Arc<Mutex<Duration>>,
    storage: Storage,
}

impl Hash for Reader {
    fn hash<H: std::hash::Hasher>(&self, _: &mut H) {}
}

impl PartialEq for Reader {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for Reader {}

impl Reader {
    #[instrument(skip_all, err)]
    pub async fn new(
        archive: ArchiveReader,
        beginning: DateTime<Utc>,
        storage: Storage,
    ) -> anyhow::Result<Self> {
        let path = ArchivePath::Custom(PathBuf::from_str("collected.timestamp")?);
        let path = archive.join(path);
        let diff = match storage.exist(&path) {
            true => {
                let mut file = vec![];
                storage.read(path, &mut file).await?;
                let record_timestamp: DateTime<Utc> = serde_json::from_slice(&file)?;
                beginning.signed_duration_since(record_timestamp).to_std()?
            }
            false => Default::default(),
        };
        Ok(Self {
            archive,
            storage,
            diff,
            next_patch_time: Arc::new(Mutex::new(Duration::MAX)),
            objects_state: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    // Load a table representation for the object
    pub async fn load_table(
        &self,
        list: NamedObject,
        selector: Selector,
    ) -> anyhow::Result<serde_json::Value> {
        self.table(list, selector).await?.to_value()
    }

    fn archive_time(&self) -> DateTime<Utc> {
        Utc::now() - self.diff
    }

    pub fn pop_next_event_time(&self) -> Duration {
        let mut next_patch_time = self
            .next_patch_time
            .lock()
            .expect("next_patch_time lock poisoned");
        std::mem::replace(&mut *next_patch_time, Duration::MAX)
    }

    #[instrument(skip_all, fields(table = list.get_path().to_string()))]
    async fn table(&self, list: NamedObject, selector: Selector) -> anyhow::Result<Table> {
        tracing::trace!("Reading table...");

        Table::new(
            self.archive.join(list.get_crd_path().unwrap_or_default()),
            list.clone(),
            self.items(self.archive.join(list.get_path()), selector)
                .await?
                .filter(|obj| obj.older(self.archive_time()) && !obj.deleted())
                .collect(),
            &self.storage,
        )
        .await
    }

    // Watch events as a series of table representation for objects
    #[instrument(skip_all, fields(table = list.get_path().to_string()))]
    pub async fn watch_table_events(
        &self,
        list: NamedObject,
        selector: Selector,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        tracing::trace!("Watching table...");

        let mut events = vec![];
        for object in self
            .objects(list.get_path())
            .await?
            .filter(|obj| selector.matches(obj.labels()))
        {
            let crd_path = self.archive.join(list.get_crd_path().unwrap_or_default());
            let event = object
                .table_watch_event(crd_path, list.clone(), &self.storage)
                .await?;
            events.push(event)
        }

        Ok(events)
    }

    // Watch events as a series of json enoded objects
    #[instrument(skip_all, fields(object = list.get_path().to_string()))]
    pub async fn watch_events(
        &self,
        list: NamedObject,
        selector: Selector,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        tracing::trace!("Watching list...");

        self.objects(list.get_path())
            .await?
            .filter(|obj| selector.matches(obj.labels()))
            .map(|obj| obj.watch_event())
            .map(|ev| serde_json::to_value(ev).map_err(Into::into))
            .collect()
    }

    async fn objects(
        &self,
        path: ArchivePath,
    ) -> anyhow::Result<impl Iterator<Item = DynamicObject>> {
        let mut new_objects = HashMap::new();
        let objects = {
            let mut objects_state = self
                .objects_state
                .lock()
                .expect("objects_state lock poisoned");
            std::mem::take(&mut *objects_state)
        };
        let mut items = vec![];
        for path in self.storage.matching_paths(self.archive.join(path))? {
            match objects.get(&path) {
                Some(previous) if self.storage.exist(&path.with_extension("patch")) => {
                    new_objects.insert(path.clone(), previous.clone());
                    let versions = self
                        .interpolate(
                            previous,
                            path.with_extension("patch"),
                            previous.last_sync_timestamp().unwrap_or_default(),
                            self.archive_time(),
                        )
                        .await?;
                    for version in versions
                        .into_iter()
                        .filter(|obj| obj.older(self.archive_time()))
                    {
                        new_objects.insert(path.clone(), version.clone());
                        items.push(version);
                    }
                }
                Some(previous) => {
                    new_objects.insert(path, previous.clone());
                }
                None => {
                    for version in self
                        .versions(path.clone())
                        .await?
                        .into_iter()
                        .filter(|obj: &DynamicObject| obj.older(self.archive_time()))
                    {
                        new_objects.insert(path.clone(), version.clone());
                        items.push(version);
                    }
                }
            };
        }

        {
            let mut objects_state = self
                .objects_state
                .lock()
                .expect("objects_state lock poisoned");
            *objects_state = new_objects;
        }

        Ok(items.into_iter())
    }

    async fn items(
        &self,
        path: PathBuf,
        selector: Selector,
    ) -> anyhow::Result<impl Iterator<Item = DynamicObject>> {
        let items = Arc::new(sync::Mutex::new(vec![]));
        stream::iter(self.storage.matching_paths(path)?.into_iter())
            .map(|path| {
                let selector = &selector;
                let items = items.clone();
                async move {
                    let obj: DynamicObject = self.read(path).await?;
                    if selector.matches(obj.labels()) {
                        items.lock().await.push(obj);
                    }

                    anyhow::Result::Ok(())
                }
            })
            .boxed()
            .buffered(1024)
            .try_for_each(future::ok::<(), anyhow::Error>)
            .await?;

        Ok(items.lock().await.clone().into_iter())
    }

    #[instrument(skip_all, fields(path = path.to_string()))]
    pub async fn load_raw(&self, path: ArchivePath) -> anyhow::Result<String> {
        tracing::debug!("Reading file...");

        self.storage.read_raw(self.archive.join(path)).await
    }

    #[instrument(skip_all, fields(path = get.get_path().to_string()))]
    pub async fn load(&self, get: NamedObject) -> anyhow::Result<serde_json::Value> {
        tracing::debug!("Reading file...");

        let obj: DynamicObject = self.read(self.archive.join(get.get_path())).await?;
        if obj.deleted() {
            bail!("Object was deleted")
        }

        serde_json::to_value(obj).map_err(Into::into)
    }

    #[instrument(skip_all, fields(object = list.get_path().to_string()))]
    pub async fn list(
        &self,
        list: NamedObject,
        selector: Selector,
    ) -> anyhow::Result<serde_json::Value> {
        tracing::trace!("Reading list...");

        let path = self.archive.join(list.get_path());

        serde_json::to_value(ObjectValueList::new(
            list,
            self.items(path, selector)
                .await?
                .filter(|obj| obj.older(self.archive_time()) && !obj.deleted())
                .collect(),
        ))
        .map_err(Into::into)
    }

    pub async fn read<R: DeserializeOwned + Clone>(&self, path: PathBuf) -> anyhow::Result<R> {
        self.versions(path)
            .await?
            .last()
            .cloned()
            .ok_or(anyhow::anyhow!("failed to find object"))
    }

    // Collect a sequence of versions for the given object until clusters equivalent of Utc::now()
    async fn versions<R: DeserializeOwned>(&self, path: PathBuf) -> anyhow::Result<Vec<R>> {
        let mut object = vec![];
        self.storage.read(path.clone(), &mut object).await?;
        match self.storage.exist(&path.with_extension("patch")) {
            false => Ok(vec![serde_yaml::from_slice(&object)?]),
            true => {
                let original: serde_json::Value = serde_yaml::from_slice(&object)?;
                Some(original.clone())
                    .into_iter()
                    .chain(
                        self.interpolate(
                            &original,
                            path.with_extension("patch"),
                            Default::default(),
                            self.archive_time(),
                        )
                        .await?,
                    )
                    .map(|version| serde_json::from_value(version).map_err(Into::into))
                    .collect()
            }
        }
    }

    async fn read_lines(
        &self,
        filename: PathBuf,
    ) -> anyhow::Result<io::Lines<io::BufReader<impl io::Read>>> {
        let mut file = vec![];
        self.storage.read(filename, &mut file).await?;
        Ok(io::BufReader::new(io::Cursor::new(file)).lines())
    }

    // Goes through all json patches and applies them on the resource in order
    async fn interpolate<R: Serialize + DeserializeOwned>(
        &self,
        target: &R,
        patches_file: PathBuf,
        from: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> anyhow::Result<Vec<R>> {
        let mut target = serde_json::to_value(target)?;
        let mut versions = vec![];
        for list in self.read_lines(patches_file).await? {
            let patches: Vec<PatchOperation> = serde_json::from_str(&list?)?;
            let mut do_apply = false;
            for p in patches.clone() {
                match p {
                    PatchOperation::Replace(ReplaceOperation { path, value })
                    | PatchOperation::Add(AddOperation { path, value })
                        if path == PointerBuf::from_tokens(UPDATED_PATH)
                            || path == PointerBuf::from_tokens(ADDED_PATH)
                            || path == PointerBuf::from_tokens(DELETED_PATH) =>
                    {
                        let last_sync_timestamp: DateTime<Utc> = serde_json::from_value(value)?;
                        if last_sync_timestamp >= until {
                            let wait_duration = (last_sync_timestamp - until).to_std()?;
                            let mut next_patch_time = self
                                .next_patch_time
                                .lock()
                                .expect("next_patch_time lock poisoned");
                            *next_patch_time = (*next_patch_time).min(wait_duration);
                            return Ok(versions);
                        } else if last_sync_timestamp <= from {
                            break;
                        } else {
                            do_apply = true;
                        }
                    }
                    _ => (),
                };
            }

            if do_apply && !patches.is_empty() {
                patch(&mut target, &patches)?;
                versions.push(serde_json::from_value(target.clone())?)
            }
        }

        Ok(versions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn table_columns() {
        let list = NamedObject {
            named_resource: NamedResource {
                group: Some("my-group".to_string()),
                version: "v1".to_string(),
                resource: "my-kinds".to_string(),
                singular: "my-kind".to_string(),
                list_kind: "my-kindList".to_string(),
            },
            namespace: Some("my-namespace".to_string()),
            name: None,
        };
        let items = vec!["foo", "bar", "baz"];
        let tbl = Table::new(
            PathBuf::from("hello".to_string()),
            list,
            items,
            &Storage::FS,
        )
        .await;

        let expected_paths = vec![TablePath {
            column: CustomResourceColumnDefinition {
                name: "Name".to_string(),
                type_: "string".to_string(),
                ..Default::default()
            },
            json_path: JsonPath::parse("$.metadata.name").ok(),
        }];

        assert_eq!(expected_paths, tbl.unwrap().data);
    }

    #[tokio::test]
    async fn table_columns_known_kind() {
        let list = NamedObject {
            named_resource: NamedResource {
                group: Some("my-group".to_string()),
                version: "v1".to_string(),
                resource: "my-kinds".to_string(),
                singular: "type".to_string(),
                list_kind: "TypeList".to_string(),
            },
            namespace: Some("my-namespace".to_string()),
            name: None,
        };
        let items = vec!["foo", "bar", "baz"];
        let tbl = Table::new(
            PathBuf::from("hello".to_string()),
            list,
            items,
            &Storage::FS,
        )
        .await;

        let expected_paths = vec![TablePath {
            column: CustomResourceColumnDefinition {
                name: "Name".to_string(),
                type_: "string".to_string(),
                ..Default::default()
            },
            json_path: JsonPath::parse("$.metadata.name").ok(),
        }];

        assert_eq!(expected_paths, tbl.unwrap().data);
    }
}
