use std::{
    collections::HashMap,
    hash::Hash,
    io::{self, BufRead as _},
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::bail;
use chrono::{DateTime, Utc};
use derive_more::From;
use futures::{StreamExt as _, TryStreamExt as _, future, stream};
use json_patch::{AddOperation, PatchOperation, ReplaceOperation, patch};
use jsonptr::PointerBuf;
use k8s_openapi::{
    apiextensions_apiserver::pkg::apis::apiextensions::v1::{
        CustomResourceColumnDefinition, CustomResourceDefinition, CustomResourceDefinitionSpec,
        CustomResourceDefinitionVersion,
    },
    serde_json,
};
use kube::{
    ResourceExt,
    api::{
        GroupVersionResource, ObjectMeta, PartialObjectMeta, PartialObjectMetaExt as _, WatchEvent,
    },
    client::{APIGroupDiscovery, APIGroupDiscoveryList, APIResourceDiscovery, APIVersionDiscovery},
    core::{DynamicObject, Resource, TypeMeta},
};
use serde::{Deserialize, Serialize};
use serde_json_path::JsonPath;
use tokio::sync;
use tracing::instrument;

use crate::{
    gather::{json_resource::JsonResourceExt, storage::Storage},
    scanners::interface::{ADDED_ANNOTATION, DELETED_ANNOTATION, UPDATED_ANNOTATION},
};

use super::{
    printers::{
        AGE_CEL, ColumnDefinition, TablePath, TableRowDefinition, has_predefined_table,
        predefined_table,
    },
    representation::{
        ArchivePath, Container, LogGroup, NamespaceName, NamespacedName, TypeMetaGetter,
    },
    selector::Selector,
    writer::Archive,
};

const ADDED_PATH: [&str; 3] = ["metadata", "annotations", ADDED_ANNOTATION];
const UPDATED_PATH: [&str; 3] = ["metadata", "annotations", UPDATED_ANNOTATION];
const DELETED_PATH: [&str; 3] = ["metadata", "annotations", DELETED_ANNOTATION];

#[derive(Deserialize, Clone)]
pub struct Destination {
    server: String,
}

impl Destination {
    #[must_use]
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
    #[must_use]
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
    pub timestamps: Option<bool>,
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
    #[must_use]
    pub fn get_server(&self) -> &str {
        &self.server
    }
}

#[derive(Serialize, From)]
#[serde(untagged)]
pub enum ListResponse {
    List(ObjectValueList),
    Table(ResultTable),
}

#[derive(Serialize, From)]
#[serde(untagged)]
pub enum WatchResponse {
    Watch(WatchEvent<JsonResourceExt>),
    Table(WatchEvent<ResultTable>),
}

#[derive(Serialize, Deserialize)]
pub struct ObjectValueList {
    #[serde(flatten)]
    type_meta: TypeMeta,
    metadata: ObjectMeta,
    items: Vec<JsonResourceExt>,
}

impl ObjectValueList {
    #[must_use]
    pub fn new(list: NamedObject, items: Vec<JsonResourceExt>) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: list.named_resource.list_kind.clone(),
                api_version: list.to_type_meta().api_version,
            },
            metadata: ObjectMeta {
                resource_version: Some("1".into()),
                ..Default::default()
            },
            items,
        }
    }
}

#[derive(Clone)]
pub struct Table {
    pub data: Vec<TablePath>,
    pub items: Vec<JsonResourceExt>,
}

impl Table {
    #[instrument(skip_all, err)]
    pub(crate) async fn new(
        crd_path: Option<PathBuf>,
        list: NamedObject,
        items: Vec<JsonResourceExt>,
        storage: &Storage,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            data: Self::table_entries(storage, crd_path, list).await?,
            items,
        })
    }

    async fn table_entries(
        storage: &Storage,
        crd_path: Option<PathBuf>,
        list: NamedObject,
    ) -> anyhow::Result<Vec<TablePath>> {
        let crd: CustomResourceDefinition = if let Some(crd_path) = crd_path
            && storage.exist(&crd_path)
        {
            let mut file = vec![];
            storage.read(&crd_path, &mut file).await?;
            serde_saphyr::from_slice(&file)?
        } else {
            match predefined_table(&list.named_resource.resource) {
                Some(columns) => CustomResourceDefinition {
                    spec: CustomResourceDefinitionSpec {
                        versions: vec![CustomResourceDefinitionVersion {
                            name: list.named_resource.version.clone(),
                            additional_printer_columns: Some(
                                columns
                                    .iter()
                                    .map(|entry| entry.column.source.clone())
                                    .collect(),
                            ),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    ..Default::default()
                },
                None => CustomResourceDefinition::default(),
            }
        };

        let crd_version = crd
            .spec
            .versions
            .iter()
            .find(|crd| crd.name == list.named_resource.version);

        let table_entries: Vec<TablePath> = crd_version
            .map(|version| version.additional_printer_columns.clone())
            .unwrap_or_default()
            .map(|columns| {
                columns
                    .iter()
                    .map(|column| {
                        TablePath::new(&ColumnDefinition {
                            source: column.clone(),
                            ..Default::default()
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(match has_predefined_table(&list.named_resource.resource) {
            true => predefined_table(&list.named_resource.resource).unwrap_or_default(),
            false => {
                let mut data = vec![TablePath {
                    column: ColumnDefinition {
                        source: CustomResourceColumnDefinition {
                            name: "Name".to_string(),
                            type_: "string".to_string(),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    json_path: JsonPath::parse("$.metadata.name").ok(),
                }];
                data.extend(
                    table_entries
                        .into_iter()
                        .filter(|entry| !entry.column.source.name.eq_ignore_ascii_case("age")),
                );
                data.push(TablePath::new(&ColumnDefinition {
                    source: CustomResourceColumnDefinition {
                        name: "Age".to_string(),
                        type_: "string".to_string(),
                        ..Default::default()
                    },
                    cel: Some(AGE_CEL.to_string()),
                }));
                data
            }
        })
    }

    fn to_row(&self, obj: &JsonResourceExt) -> anyhow::Result<TableRow> {
        let Self { data: rows, .. } = self;
        let context = TablePath::build_context(&obj.json)
            .ok_or_else(|| anyhow::anyhow!("failed to build context"))?;
        let cells: Vec<serde_json::Value> = rows
            .iter()
            .map(|r| {
                r.render(&obj.json, &context)
                    .unwrap_or(serde_json::Value::Null)
            })
            .collect();

        Ok(TableRow {
            cells,
            object: obj.meta().clone().into_response_partial::<DynamicObject>(),
        })
    }

    fn definitions(&self) -> Vec<TableRowDefinition> {
        self.data
            .iter()
            .map(super::printers::TablePath::to_definition)
            .collect()
    }

    fn rows(&self) -> anyhow::Result<Vec<TableRow>> {
        let Self { items, .. } = self;
        items.iter().map(|i| self.to_row(i)).collect()
    }

    pub(crate) fn to_result_table(&self) -> anyhow::Result<ResultTable> {
        ResultTable::from_table(self)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableRow {
    pub cells: Vec<serde_json::Value>,
    pub object: PartialObjectMeta<DynamicObject>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultTable {
    pub kind: String,
    pub api_version: String,
    pub metadata: ObjectMeta,
    pub column_definitions: Vec<TableRowDefinition>,
    pub rows: Vec<TableRow>,
}

impl ResultTable {
    pub fn from_table(table: &Table) -> anyhow::Result<ResultTable> {
        Ok(ResultTable {
            kind: "Table".to_string(),
            api_version: "meta.k8s.io/v1".to_string(),
            metadata: ObjectMeta {
                resource_version: Some("1".to_string()),
                ..Default::default()
            },
            column_definitions: table.definitions(),
            rows: table.rows()?,
        })
    }

    fn to_result_table(&self) -> anyhow::Result<ResultTable> {
        ResultTable::from_table(self)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultTable {
    pub kind: String,
    pub api_version: String,
    pub metadata: ObjectMeta,
    pub column_definitions: Vec<serde_json::Value>,
    pub rows: Vec<serde_json::Value>,
}

impl ResultTable {
    pub fn from_table(table: &Table) -> anyhow::Result<ResultTable> {
        Ok(ResultTable {
            kind: "Table".to_string(),
            api_version: "meta.k8s.io/v1".to_string(),
            metadata: ObjectMeta {
                resource_version: Some("1".to_string()),
                ..Default::default()
            },
            column_definitions: table.definitions(),
            rows: table.rows()?,
        })
    }
}

#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Watch {
    pub watch: Option<bool>,
    pub allow_watch_bookmarks: Option<bool>,
    pub send_initial_events: Option<bool>,
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
            None => Some(DateTime::default()),
        }
    }

    fn older(&self, before: DateTime<Utc>) -> bool {
        let passed = || Some(before >= self.last_sync_timestamp()?);
        passed().is_some_and(|is_true| is_true)
    }

    fn deleted(&self) -> bool {
        self.annotations().contains_key(DELETED_ANNOTATION)
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

impl NamedResource {
    pub fn get_crd_path(&self) -> Option<ArchivePath> {
        self.group.as_ref().map(|group| {
            ArchivePath::new_path(
                NamespaceName::new(Some(format!("{}.{}", self.resource, group)), None),
                TypeMeta::resource::<CustomResourceDefinition>(),
            )
        })
    }
}

struct NamedResourcesState {
    archive: Arc<Archive>,
    storage: Arc<Storage>,
    served_crs_only: bool,
}

type DiscoveryResource = (String, String, APIResourceDiscovery);

impl NamedResourcesState {
    pub fn new(archive: Arc<Archive>, storage: Arc<Storage>, served_crs_only: bool) -> Self {
        Self {
            archive,
            storage,
            served_crs_only,
        }
    }

    async fn discovery_file(&self, path: ArchivePath) -> anyhow::Result<Vec<DiscoveryResource>> {
        let mut object = vec![];
        self.storage
            .read(&self.archive.join(path), &mut object)
            .await?;

        let discovery = serde_saphyr::from_slice(&object)?;
        Ok(self.discovery_groups(discovery))
    }

    fn discovery_groups(&self, groups: APIGroupDiscoveryList) -> Vec<DiscoveryResource> {
        groups
            .items
            .into_iter()
            .flat_map(|group| self.process_group(group))
            .collect()
    }

    fn process_group(&self, api_group: APIGroupDiscovery) -> Vec<DiscoveryResource> {
        let Some(metadata) = api_group.metadata else {
            return Vec::new();
        };
        let group = metadata.name.unwrap_or_default();
        self.process_discovery_versions(group, api_group.versions)
    }

    fn process_discovery_versions(
        &self,
        group: String,
        versions: Vec<APIVersionDiscovery>,
    ) -> Vec<DiscoveryResource> {
        versions
            .into_iter()
            .flat_map(|version| self.process_version(&group, version))
            .collect()
    }

    fn process_version(
        &self,
        group: &str,
        api_version: APIVersionDiscovery,
    ) -> Vec<DiscoveryResource> {
        let Some(version) = api_version.version else {
            return Vec::new();
        };
        self.process_discovery_resources(group, version, api_version.resources)
    }

    fn process_discovery_resources(
        &self,
        group: &str,
        version: String,
        resources: Vec<APIResourceDiscovery>,
    ) -> Vec<DiscoveryResource> {
        resources
            .into_iter()
            .map(|resource| (group.to_owned(), version.clone(), resource))
            .collect()
    }

    async fn parse_discovery_resource(
        &self,
        group: &str,
        version: &str,
        resource: APIResourceDiscovery,
    ) -> Option<NamedResource> {
        let gvk = resource.response_kind?;
        let kind = gvk.kind?;

        let resource = NamedResource {
            group: gvk
                .group
                .filter(|g| !g.is_empty())
                .or_else(|| Some(group.to_string()))
                .filter(|g| !g.is_empty()),
            version: gvk
                .version
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| version.to_string()),
            list_kind: format!("{kind}List"),
            resource: resource.resource?,
            singular: resource
                .singular_resource
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| kind.clone().to_lowercase()),
        };

        if !self.served_crs_only {
            return Some(resource);
        }

        self.only_served_stored_resource(resource).await
    }

    async fn only_served_stored_resource(&self, resource: NamedResource) -> Option<NamedResource> {
        let Some(crd_path) = resource.get_crd_path() else {
            return Some(resource);
        };

        let crd_path = self.archive.join(crd_path);
        if !self.storage.exist(&crd_path) {
            return Some(resource);
        }

        let crd: CustomResourceDefinition = {
            let mut file = vec![];
            self.storage.read(&crd_path, &mut file).await.ok()?;
            serde_saphyr::from_slice(&file).ok()?
        };

        crd.spec
            .versions
            .iter()
            .find(|crd| crd.name == resource.version && crd.served && crd.storage)?;

        Some(resource)
    }
}

#[derive(Clone, Default)]
struct NamedResources {
    resources: HashMap<GroupVersionResource, NamedResource>,
}

impl NamedResources {
    fn get(&self, gvr: &GroupVersionResource) -> Option<&NamedResource> {
        self.resources.get(gvr)
    }

    async fn insert_resources(
        &mut self,
        state: &NamedResourcesState,
        resources: Vec<DiscoveryResource>,
    ) {
        for (group, version, resource) in resources {
            self.insert_resource(state, &group, &version, resource)
                .await;
        }
    }

    async fn insert_resource(
        &mut self,
        state: &NamedResourcesState,
        group: &str,
        version: &str,
        res: APIResourceDiscovery,
    ) -> Option<NamedResource> {
        let res = state.parse_discovery_resource(group, version, res).await?;

        self.resources.insert(
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
    #[must_use]
    pub fn get_path(&self) -> ArchivePath {
        ArchivePath::new_path(self, self.to_type_meta())
    }

    #[must_use]
    pub fn get_crd_path(&self) -> Option<ArchivePath> {
        self.named_resource.get_crd_path()
    }

    #[must_use]
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
    archive: Arc<Archive>,
    named_resources: Arc<NamedResources>,
    buffer_size: usize,
}

impl ArchiveReader {
    pub async fn new(
        archive: Arc<Archive>,
        storage: Arc<Storage>,
        buffer_size: usize,
        served_crs_only: bool,
    ) -> Self {
        let state = NamedResourcesState::new(archive.clone(), storage, served_crs_only);
        let mut named_resources = NamedResources::default();

        match state
            .discovery_file(ArchivePath::Custom("apis.json".into()))
            .await
        {
            Ok(resources) => named_resources.insert_resources(&state, resources).await,
            Err(e) => {
                tracing::error!("Fail parsing apis.json : {e:?}");
            }
        }

        match state
            .discovery_file(ArchivePath::Custom("api.json".into()))
            .await
        {
            Ok(resources) => named_resources.insert_resources(&state, resources).await,
            Err(e) => {
                tracing::error!("Fail parsing api.json : {e:?}");
            }
        }

        Self {
            archive,
            named_resources: Arc::new(named_resources),
            buffer_size: buffer_size.max(1),
        }
    }

    #[must_use]
    pub fn join(&self, path: ArchivePath) -> PathBuf {
        self.archive.join(path)
    }

    /// Returns the archive's root path. Used to distinguish archives in hash/eq.
    #[must_use]
    pub fn path(&self) -> PathBuf {
        self.archive.path()
    }

    pub fn named_object_from_list(&self, list: List) -> anyhow::Result<NamedObject> {
        let gvr = GroupVersionResource::gvr(
            &list.group.clone().unwrap_or_default(),
            &list.version,
            &list.kind,
        );

        Ok(NamedObject {
            named_resource: self
                .named_resources
                .get(&gvr)
                .cloned()
                .ok_or(anyhow::anyhow!("Failed to find named resource for {gvr:?}"))?,
            namespace: list.namespace,
            name: None,
        })
    }

    pub fn named_object_from_get(&self, get: Get) -> anyhow::Result<NamedObject> {
        let gvr = GroupVersionResource::gvr(
            &get.group.clone().unwrap_or_default(),
            &get.version,
            &get.kind,
        );

        Ok(NamedObject {
            named_resource: self
                .named_resources
                .get(&gvr)
                .cloned()
                .ok_or(anyhow::anyhow!("Failed to find named resource for {gvr:?}"))?,
            namespace: get.namespace,
            name: Some(get.name),
        })
    }
}

#[derive(Clone)]
pub struct Reader {
    pub archive: ArchiveReader,
    diff: Duration,
    objects_state: Arc<Mutex<HashMap<PathBuf, JsonResourceExt>>>,
    next_patch_time: Arc<Mutex<Duration>>,
    storage: Arc<Storage>,
}

impl Hash for Reader {
    // Hash on the archive's path so that readers for different archives produce
    // different cache keys in `#[cached]` functions. Previously this was a no-op,
    // which made all Readers collide in the cache: the first archive's response
    // for a given path/list/get would be returned for every other archive too.
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.archive.path().hash(state);
    }
}

impl PartialEq for Reader {
    // Equal iff the underlying archive paths match. See `Hash` above for the
    // rationale; both must agree for the cache to correctly distinguish archives.
    fn eq(&self, other: &Self) -> bool {
        self.archive.path() == other.archive.path()
    }
}

impl Eq for Reader {}

impl Reader {
    #[instrument(skip_all, err)]
    pub async fn new(
        archive: ArchiveReader,
        beginning: DateTime<Utc>,
        storage: Arc<Storage>,
    ) -> anyhow::Result<Self> {
        let path = ArchivePath::Custom(PathBuf::from_str("collected.timestamp")?);
        let path = archive.join(path);
        let diff = match storage.exist(&path) {
            true => {
                let mut file = vec![];
                storage.read(&path, &mut file).await?;
                let record_timestamp: DateTime<Utc> = serde_json::from_slice(&file)?;
                beginning.signed_duration_since(record_timestamp).to_std()?
            }
            false => Duration::default(),
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
    ) -> anyhow::Result<ResultTable> {
        self.table(list, selector).await?.to_result_table()
    }

    fn archive_time(&self) -> DateTime<Utc> {
        Utc::now() - self.diff
    }

    #[must_use]
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
            list.get_crd_path().map(|crd| self.archive.join(crd)),
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
    ) -> anyhow::Result<Vec<WatchEvent<ResultTable>>> {
        tracing::trace!("Watching table...");

        let mut events = vec![];
        for object in self
            .objects(list.get_path())
            .await?
            .filter(|obj| selector.matches(obj.labels()))
        {
            let crd_path = list.get_crd_path().map(|crd| self.archive.join(crd));
            let event = object
                .table_watch_event(crd_path, list.clone(), &self.storage)
                .await?;
            events.push(event);
        }

        Ok(events)
    }

    // Watch events as a series of json enoded objects
    #[instrument(skip_all, fields(object = list.get_path().to_string()))]
    pub async fn watch_events(
        &self,
        list: NamedObject,
        selector: Selector,
    ) -> anyhow::Result<Vec<WatchEvent<JsonResourceExt>>> {
        tracing::trace!("Watching list...");

        Ok(self
            .objects(list.get_path())
            .await?
            .filter(|obj| selector.matches(obj.labels()))
            .map(GatherObject::watch_event)
            .collect())
    }

    async fn objects(
        &self,
        path: ArchivePath,
    ) -> anyhow::Result<impl Iterator<Item = JsonResourceExt>> {
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
                        .filter(|obj: &JsonResourceExt| obj.older(self.archive_time()))
                    {
                        new_objects.insert(path.clone(), version.clone());
                        items.push(version);
                    }
                }
            }
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
    ) -> anyhow::Result<impl Iterator<Item = JsonResourceExt>> {
        let items = Arc::new(sync::Mutex::new(vec![]));
        stream::iter(self.storage.matching_paths(path)?)
            .map(|path| {
                let selector = &selector;
                let items = items.clone();
                async move {
                    let obj: JsonResourceExt = self.read(path).await?;
                    if selector.matches(obj.labels()) {
                        items.lock().await.push(obj);
                    }

                    anyhow::Result::Ok(())
                }
            })
            .boxed()
            .buffered(self.archive.buffer_size)
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
    pub async fn load(&self, get: NamedObject) -> anyhow::Result<JsonResourceExt> {
        tracing::debug!("Reading file...");

        let obj: JsonResourceExt = self.read(self.archive.join(get.get_path())).await?;
        if obj.deleted() {
            bail!("Object was deleted")
        }

        Ok(obj)
    }

    #[instrument(skip_all, fields(object = list.get_path().to_string()))]
    pub async fn list(
        &self,
        list: NamedObject,
        selector: Selector,
    ) -> anyhow::Result<ObjectValueList> {
        tracing::trace!("Reading list...");

        let path = self.archive.join(list.get_path());

        Ok(ObjectValueList::new(
            list,
            self.items(path, selector)
                .await?
                .filter(|obj| obj.older(self.archive_time()) && !obj.deleted())
                .collect(),
        ))
    }

    pub async fn read(&self, path: PathBuf) -> anyhow::Result<JsonResourceExt> {
        self.versions(path)
            .await?
            .last()
            .cloned()
            .ok_or(anyhow::anyhow!("failed to find object"))
    }

    // Collect a sequence of versions for the given object until clusters equivalent of Utc::now()
    async fn versions(&self, path: PathBuf) -> anyhow::Result<Vec<JsonResourceExt>> {
        let mut object = vec![];
        self.storage.read(&path, &mut object).await?;
        match self.storage.exist(&path.with_extension("patch")) {
            false => Ok(vec![serde_saphyr::from_slice(&object)?]),
            true => {
                self.interpolate(
                    &serde_saphyr::from_slice(&object)?,
                    path.with_extension("patch"),
                    DateTime::default(),
                    self.archive_time(),
                )
                .await
            }
        }
    }

    async fn read_lines(
        &self,
        filename: PathBuf,
    ) -> anyhow::Result<io::Lines<io::BufReader<impl io::Read>>> {
        let mut file = vec![];
        self.storage.read(&filename, &mut file).await?;
        Ok(io::BufReader::new(io::Cursor::new(file)).lines())
    }

    // Goes through all json patches and applies them on the resource in order
    async fn interpolate(
        &self,
        target: &JsonResourceExt,
        patches_file: PathBuf,
        from: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> anyhow::Result<Vec<JsonResourceExt>> {
        let mut target = target.clone();
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
                        }
                        do_apply = true;
                    }
                    _ => (),
                }
            }

            if do_apply && !patches.is_empty() {
                patch(&mut target.json, &patches)?;
                versions.push(JsonResourceExt::new(target.json.clone()));
            }
        }

        Ok(versions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use serde_json::json;

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
        let items = vec![JsonResourceExt::default()];
        let tbl = Table::new(
            Some(PathBuf::from("hello".to_string())),
            list,
            items,
            &Storage::FS,
        )
        .await;

        let expected_paths = vec![
            TablePath {
                column: ColumnDefinition {
                    source: CustomResourceColumnDefinition {
                        name: "Name".to_string(),
                        type_: "string".to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                json_path: JsonPath::parse("$.metadata.name").ok(),
            },
            TablePath {
                column: ColumnDefinition {
                    source: CustomResourceColumnDefinition {
                        name: "Age".to_string(),
                        type_: "string".to_string(),
                        ..Default::default()
                    },
                    cel: Some(
                        "(now - timestamp(self.metadata.creationTimestamp)).age()".to_string(),
                    ),
                },
                json_path: None,
            },
        ];

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
        let items = vec![JsonResourceExt::default()];
        let tbl = Table::new(
            Some(PathBuf::from("hello".to_string())),
            list,
            items,
            &Storage::FS,
        )
        .await;

        let expected_paths = vec![
            TablePath {
                column: ColumnDefinition {
                    source: CustomResourceColumnDefinition {
                        name: "Name".to_string(),
                        type_: "string".to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                json_path: JsonPath::parse("$.metadata.name").ok(),
            },
            TablePath {
                column: ColumnDefinition {
                    source: CustomResourceColumnDefinition {
                        name: "Age".to_string(),
                        type_: "string".to_string(),
                        ..Default::default()
                    },
                    cel: Some(
                        "(now - timestamp(self.metadata.creationTimestamp)).age()".to_string(),
                    ),
                },
                json_path: None,
            },
        ];

        assert_eq!(expected_paths, tbl.unwrap().data);
    }

    #[tokio::test]
    async fn table_columns_pods() {
        let list = NamedObject {
            named_resource: NamedResource {
                group: None,
                version: "v1".to_string(),
                resource: "pods".to_string(),
                singular: "pod".to_string(),
                list_kind: "PodList".to_string(),
            },
            namespace: Some("my-namespace".to_string()),
            name: None,
        };
        let created = (Utc::now() - Duration::minutes(5)).to_rfc3339();
        let items = vec![JsonResourceExt::new(json!({
            "metadata": {
                "name": "pod-a",
                "namespace": "my-namespace",
                "creationTimestamp": created,
            },
            "spec": {
                "containers": [
                    {"name": "c1"},
                    {"name": "c2"}
                ]
            },
            "status": {
                "phase": "Running",
                "containerStatuses": [
                    {"ready": true, "restartCount": 1},
                    {"ready": false, "restartCount": 2}
                ],
                "initContainerStatuses": [
                    {"restartCount": 3}
                ]
            }
        }))];
        let tbl = Table::new(
            Some(PathBuf::from("hello".to_string())),
            list,
            items,
            &Storage::FS,
        )
        .await
        .unwrap();

        let columns: Vec<&str> = tbl
            .data
            .iter()
            .map(|entry| entry.column.source.name.as_str())
            .collect();
        assert_eq!(columns, vec!["Name", "Ready", "Status", "Restarts", "Age"]);

        let row = tbl.to_row(&tbl.items[0]).unwrap();
        let cells = &row.cells;
        assert_eq!(cells[0], json!("pod-a"));
        assert_eq!(cells[1], json!("1/2"));
        assert_eq!(cells[2], json!("Running"));
        assert_eq!(cells[3], json!(6));
        assert_eq!(cells[4], json!("5m"));
    }

    #[tokio::test]
    async fn table_columns_pods_without_status() {
        let list = NamedObject {
            named_resource: NamedResource {
                group: None,
                version: "v1".to_string(),
                resource: "pods".to_string(),
                singular: "pod".to_string(),
                list_kind: "PodList".to_string(),
            },
            namespace: Some("my-namespace".to_string()),
            name: None,
        };
        let created = (Utc::now() - Duration::minutes(5)).to_rfc3339();
        let items = vec![JsonResourceExt::new(json!({
            "metadata": {
                "name": "pod-b",
                "namespace": "my-namespace",
                "creationTimestamp": created,
            },
            "spec": {
                "containers": [
                    {"name": "c1"}
                ]
            }
        }))];
        let tbl = Table::new(
            Some(PathBuf::from("hello".to_string())),
            list,
            items,
            &Storage::FS,
        )
        .await
        .unwrap();

        let row = tbl.to_row(&tbl.items[0]).unwrap();
        let cells = &row.cells;
        assert_eq!(cells[0], json!("pod-b"));
        assert_eq!(cells[1], json!("0/1"));
        assert_eq!(cells[2], json!(""));
        assert_eq!(cells[3], json!(0));
        assert_eq!(cells[4], json!("5m"));
    }

    #[tokio::test]
    async fn table_columns_namespaces() {
        let list = NamedObject {
            named_resource: NamedResource {
                group: None,
                version: "v1".to_string(),
                resource: "namespaces".to_string(),
                singular: "namespace".to_string(),
                list_kind: "NamespaceList".to_string(),
            },
            namespace: None,
            name: None,
        };
        let created = (Utc::now() - Duration::hours(2)).to_rfc3339();
        let items = vec![JsonResourceExt::new(json!({
            "metadata": {
                "name": "ns-a",
                "creationTimestamp": created,
                "deletionTimestamp": Utc::now().to_rfc3339(),
            },
            "status": {
                "phase": "Active"
            }
        }))];
        let tbl = Table::new(
            Some(PathBuf::from("hello".to_string())),
            list,
            items,
            &Storage::FS,
        )
        .await
        .unwrap();

        let columns: Vec<&str> = tbl
            .data
            .iter()
            .map(|entry| entry.column.source.name.as_str())
            .collect();
        assert_eq!(columns, vec!["Name", "Status", "Age"]);

        let row = tbl.to_row(&tbl.items[0]).unwrap();
        let cells = &row.cells;
        assert_eq!(cells[0], json!("ns-a"));
        assert_eq!(cells[1], json!("Terminating"));
        assert_eq!(cells[2], json!("2h"));
    }

    #[tokio::test]
    async fn table_columns_deployments() {
        let list = NamedObject {
            named_resource: NamedResource {
                group: Some("apps".to_string()),
                version: "v1".to_string(),
                resource: "deployments".to_string(),
                singular: "deployment".to_string(),
                list_kind: "DeploymentList".to_string(),
            },
            namespace: Some("my-namespace".to_string()),
            name: None,
        };
        let created = (Utc::now() - Duration::days(3)).to_rfc3339();
        let items = vec![JsonResourceExt::new(json!({
            "metadata": {
                "name": "deploy-a",
                "namespace": "my-namespace",
                "creationTimestamp": created,
            },
            "spec": {
                "replicas": 3
            },
            "status": {
                "readyReplicas": 2,
                "updatedReplicas": 3,
                "availableReplicas": 2
            }
        }))];
        let tbl = Table::new(
            Some(PathBuf::from("hello".to_string())),
            list,
            items,
            &Storage::FS,
        )
        .await
        .unwrap();

        let columns: Vec<&str> = tbl
            .data
            .iter()
            .map(|entry| entry.column.source.name.as_str())
            .collect();
        assert_eq!(
            columns,
            vec!["Name", "Ready", "Up-to-date", "Available", "Age"]
        );

        let row = tbl.to_row(&tbl.items[0]).unwrap();
        let cells = &row.cells;
        assert_eq!(cells[0], json!("deploy-a"));
        assert_eq!(cells[1], json!("2/3"));
        assert_eq!(cells[2], json!(3));
        assert_eq!(cells[3], json!(2));
        assert_eq!(cells[4], json!("3d"));
    }

    #[tokio::test]
    async fn table_columns_services() {
        let list = NamedObject {
            named_resource: NamedResource {
                group: None,
                version: "v1".to_string(),
                resource: "services".to_string(),
                singular: "service".to_string(),
                list_kind: "ServiceList".to_string(),
            },
            namespace: Some("default".to_string()),
            name: None,
        };
        let created = (Utc::now() - Duration::days(5) - Duration::hours(9)).to_rfc3339();
        let items = vec![JsonResourceExt::new(json!({
            "metadata": {
                "name": "kubernetes",
                "namespace": "default",
                "creationTimestamp": created,
            },
            "spec": {
                "type": "ClusterIP",
                "clusterIP": "10.96.0.1",
                "ports": [
                    {"port": 443, "protocol": "TCP"}
                ]
            },
            "status": {}
        }))];
        let tbl = Table::new(
            Some(PathBuf::from("hello".to_string())),
            list,
            items,
            &Storage::FS,
        )
        .await
        .unwrap();

        let columns: Vec<&str> = tbl
            .data
            .iter()
            .map(|entry| entry.column.source.name.as_str())
            .collect();
        assert_eq!(
            columns,
            vec![
                "NAME",
                "TYPE",
                "CLUSTER-IP",
                "EXTERNAL-IP",
                "PORT(S)",
                "AGE"
            ]
        );

        let row = tbl.to_row(&tbl.items[0]).unwrap();
        let cells = &row.cells;
        assert_eq!(cells[0], json!("kubernetes"));
        assert_eq!(cells[1], json!("ClusterIP"));
        assert_eq!(cells[2], json!("10.96.0.1"));
        assert_eq!(cells[3], json!("<none>"));
        assert_eq!(cells[4], json!("443/TCP"));
        assert_eq!(cells[5], json!("5d9h"));
    }

    #[tokio::test]
    async fn table_columns_daemonsets() {
        let list = NamedObject {
            named_resource: NamedResource {
                group: Some("apps".to_string()),
                version: "v1".to_string(),
                resource: "daemonsets".to_string(),
                singular: "daemonset".to_string(),
                list_kind: "DaemonSetList".to_string(),
            },
            namespace: Some("kube-system".to_string()),
            name: None,
        };
        let created = (Utc::now() - Duration::days(10)).to_rfc3339();
        let items = vec![JsonResourceExt::new(json!({
            "metadata": {
                "name": "node-local-dns",
                "namespace": "kube-system",
                "creationTimestamp": created,
            },
            "spec": {
                "template": {
                    "spec": {
                        "nodeSelector": {
                            "kubernetes.io/os": "linux"
                        }
                    }
                }
            },
            "status": {
                "desiredNumberScheduled": 3,
                "currentNumberScheduled": 3,
                "numberReady": 3,
                "updatedNumberScheduled": 3,
                "numberAvailable": 3
            }
        }))];
        let tbl = Table::new(
            Some(PathBuf::from("hello".to_string())),
            list,
            items,
            &Storage::FS,
        )
        .await
        .unwrap();

        let columns: Vec<&str> = tbl
            .data
            .iter()
            .map(|entry| entry.column.source.name.as_str())
            .collect();
        assert_eq!(
            columns,
            vec![
                "NAME",
                "DESIRED",
                "CURRENT",
                "READY",
                "UP-TO-DATE",
                "AVAILABLE",
                "NODE SELECTOR",
                "AGE"
            ]
        );

        let row = tbl.to_row(&tbl.items[0]).unwrap();
        let cells = &row.cells;
        assert_eq!(cells[0], json!("node-local-dns"));
        assert_eq!(cells[1], json!(3));
        assert_eq!(cells[2], json!(3));
        assert_eq!(cells[3], json!(3));
        assert_eq!(cells[4], json!(3));
        assert_eq!(cells[5], json!(3));
        assert_eq!(cells[6], json!("kubernetes.io/os=linux"));
        assert_eq!(cells[7], json!("10d"));
    }

    #[tokio::test]
    async fn table_columns_validating_admission_policy_bindings() {
        let list = NamedObject {
            named_resource: NamedResource {
                group: Some("admissionregistration.k8s.io".to_string()),
                version: "v1".to_string(),
                resource: "validatingadmissionpolicybindings".to_string(),
                singular: "validatingadmissionpolicybinding".to_string(),
                list_kind: "ValidatingAdmissionPolicyBindingList".to_string(),
            },
            namespace: None,
            name: None,
        };
        let created = (Utc::now() - Duration::minutes(15)).to_rfc3339();
        let items = vec![JsonResourceExt::new(json!({
            "metadata": {
                "name": "binding-a",
                "creationTimestamp": created,
            },
            "spec": {
                "policyName": "require-team-label",
                "paramRef": {
                    "namespace": "default",
                    "name": "team-label-params"
                }
            }
        }))];
        let tbl = Table::new(
            Some(PathBuf::from("hello".to_string())),
            list,
            items,
            &Storage::FS,
        )
        .await
        .unwrap();

        let columns: Vec<&str> = tbl
            .data
            .iter()
            .map(|entry| entry.column.source.name.as_str())
            .collect();
        assert_eq!(columns, vec!["Name", "PolicyName", "ParamRef", "Age"]);

        let row = tbl.to_row(&tbl.items[0]).unwrap();
        let cells = &row.cells;
        assert_eq!(cells[0], json!("binding-a"));
        assert_eq!(cells[1], json!("require-team-label"));
        assert_eq!(cells[2], json!("default/team-label-params"));
        assert_eq!(cells[3], json!("15m"));
    }
}
