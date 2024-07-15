use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    fs::File,
    io::{self, BufRead as _, Read},
    path::PathBuf,
    str::FromStr,
    time::Duration,
};

use anyhow::{anyhow, bail};
use json_patch::{patch, AddOperation, PatchOperation, ReplaceOperation};
use jsonptr::Pointer;
use k8s_openapi::{
    apiextensions_apiserver::pkg::apis::apiextensions::v1::{
        CustomResourceColumnDefinition, CustomResourceDefinition,
    },
    chrono::{DateTime, Utc},
    serde_json::{self, json},
};
use kube::{
    api::{PartialObjectMetaExt as _, WatchEvent},
    core::{DynamicObject, Resource, TypeMeta},
    ResourceExt,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json_path::JsonPath;

use crate::scanners::interface::{ADDED_ANNOTATION, DELETED_ANNOTATION, UPDATED_ANNOTATION};

use super::{
    representation::{
        ArchivePath, Container, LogGroup, NamespaceName, NamespacedName, TypeMetaGetter,
    },
    selector::Selector,
    writer::Archive,
};

const ADDED_PATH: &[&str] = &["metadata", "annotations", ADDED_ANNOTATION];
const UPDATED_PATH: &[&str] = &["metadata", "annotations", UPDATED_ANNOTATION];
const DELETED_PATH: &[&str] = &["metadata", "annotations", DELETED_ANNOTATION];

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

    pub fn get_path(&self) -> ArchivePath {
        ArchivePath::new_path(self, self.to_type_meta())
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

impl TypeMetaGetter for Get {
    fn to_type_meta(&self) -> TypeMeta {
        match self.group.clone() {
            Some(group) => TypeMeta {
                api_version: format!("{}/{}", group, self.version),
                kind: self.kind.trim_end_matches('s').to_string(),
            },
            None => TypeMeta {
                api_version: self.version.clone(),
                kind: self.kind.trim_end_matches('s').to_string(),
            },
        }
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

    pub fn get_path(&self) -> ArchivePath {
        ArchivePath::new_path(self, self.to_type_meta())
    }

    pub fn get_crd_path(&self) -> Option<ArchivePath> {
        self.group.clone().map(|group| {
            ArchivePath::new_path(
                NamespaceName::new(Some(format!("{}.{}", self.kind, group)), None),
                TypeMeta::resource::<CustomResourceDefinition>(),
            )
        })
    }
}

impl TypeMetaGetter for List {
    fn to_type_meta(&self) -> TypeMeta {
        match self.group.clone() {
            Some(group) => TypeMeta {
                api_version: format!("{}/{}", group, self.version),
                kind: self.kind.trim_end_matches('s').to_string(),
            },
            None => TypeMeta {
                api_version: self.version.clone(),
                kind: self.kind.trim_end_matches('s').to_string(),
            },
        }
    }
}

impl NamespacedName for &List {
    fn name(&self) -> Option<String> {
        None
    }

    fn namespace(&self) -> Option<String> {
        self.namespace.clone()
    }
}

#[derive(Serialize, Deserialize)]
pub struct ObjectValueList {
    #[serde(flatten)]
    type_meta: TypeMeta,
    items: Vec<DynamicObject>,
}

impl ObjectValueList {
    pub fn new(list: List, items: Vec<DynamicObject>) -> Self {
        // Doing best effort to convert arbitrary object list to a typed list.
        // Works best for core types, generating SecretList instead of just List.
        let mut kind = list
            .kind
            .strip_suffix('s')
            .unwrap_or(list.kind.as_str())
            .to_string();
        Self {
            type_meta: TypeMeta {
                kind: format!("{}{kind}List", kind.remove(0).to_uppercase(),),
                api_version: "v1".to_string(),
            },
            items,
        }
    }
}

#[derive(Clone)]
pub struct Table(Vec<TablePath>, Vec<serde_json::Value>);

#[derive(Clone)]
struct TablePath {
    column: CustomResourceColumnDefinition,
    json_path: JsonPath,
}

impl TablePath {
    fn new(column: &CustomResourceColumnDefinition) -> anyhow::Result<Self> {
        let json_path = format!("${}", column.json_path.replace(r"\.", r"."));
        let json_path = JsonPath::parse(&json_path)
            .map_err(|e| anyhow!("unable to parse json path for {json_path}: {e:?}",))?;
        Ok(Self {
            column: column.clone(),
            json_path,
        })
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
    fn new(crd_path: PathBuf, version: &str, items: Vec<impl Serialize>) -> anyhow::Result<Self> {
        let mut data = vec![TablePath {
            column: CustomResourceColumnDefinition {
                name: "Name".to_string(),
                type_: "string".to_string(),
                ..Default::default()
            },
            json_path: JsonPath::parse("$.metadata.name").unwrap(),
        }];

        data.extend(Table::table_entries(crd_path, version)?);
        let items: anyhow::Result<Vec<serde_json::Value>> = items
            .into_iter()
            .map(|i| serde_json::to_value(i).map_err(Into::into))
            .collect();
        Ok(Self(data, items?))
    }

    fn table_entries(crd_path: PathBuf, version: &str) -> anyhow::Result<Vec<TablePath>> {
        let crd: CustomResourceDefinition = match crd_path.is_file() {
            true => serde_yaml::from_reader(File::open(crd_path)?)?,
            false => Default::default(),
        };

        let crd_version = crd.spec.versions.iter().find(|crd| crd.name == version);

        let table_entries = crd_version
            .map(|version| version.additional_printer_columns.clone())
            .unwrap_or_default()
            .map(|columns| columns.iter().map(TablePath::new).collect())
            .transpose()?
            .unwrap_or_default();

        Ok(table_entries)
    }

    fn to_row(&self, obj: impl Serialize) -> anyhow::Result<serde_json::Value> {
        let Table(rows, _) = self;
        let obj = serde_json::to_value(obj)?;
        let cells: Vec<&serde_json::Value> = rows
            .iter()
            .filter_map(|r| r.json_path.query(&obj).first())
            .collect();

        Ok(json!({
            "cells": cells,
            "object": serde_json::from_value::<DynamicObject>(obj)?.metadata.into_response_partial::<DynamicObject>(),
        }))
    }

    fn definitions(&self) -> Vec<serde_json::Value> {
        self.0.iter().map(|r| r.to_definition()).collect()
    }

    fn rows(&self) -> anyhow::Result<Vec<serde_json::Value>> {
        let Table(_, items) = self;
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

    fn table_watch_event(
        &self,
        crd_path: PathBuf,
        version: &str,
    ) -> anyhow::Result<serde_json::Value> {
        Ok(serde_json::to_value(self.event()(
            Table::new(crd_path, version, vec![&self])?.to_value()?,
        ))?)
    }
}

impl<T: Resource + Serialize> GatherObject for T {}

#[derive(Clone)]
pub struct Reader {
    archive: Archive,
    diff: Duration,
    objects_state: RefCell<HashMap<PathBuf, DynamicObject>>,
    next_patch_time: Cell<Duration>,
}

impl Reader {
    pub fn new(archive: Archive, beginning: DateTime<Utc>) -> anyhow::Result<Self> {
        let path = ArchivePath::Custom(PathBuf::from_str("collected.timestamp")?);
        let path = archive.join(path);
        Ok(Self {
            archive,
            diff: match path.exists() {
                true => {
                    let file = File::open(path)?;
                    let record_timestamp: DateTime<Utc> = serde_json::from_reader(file)?;
                    beginning.signed_duration_since(record_timestamp).to_std()?
                }
                false => Default::default(),
            },
            next_patch_time: Duration::MAX.into(),
            objects_state: Default::default(),
        })
    }

    // Load a table representation for the object
    pub fn load_table(&self, list: List, selector: Selector) -> anyhow::Result<serde_json::Value> {
        self.table(list, selector)?.to_value()
    }

    fn archive_time(&self) -> DateTime<Utc> {
        Utc::now() - self.diff
    }

    pub fn pop_next_event_time(&self) -> Duration {
        self.next_patch_time.replace(Duration::MAX)
    }

    fn table(&self, list: List, selector: Selector) -> anyhow::Result<Table> {
        log::trace!("Reading table {}...", list.get_path());

        Table::new(
            self.archive.join(list.get_crd_path().unwrap_or_default()),
            &list.version,
            self.items(self.archive.join(list.get_path()), selector)?
                .filter(|obj| obj.older(self.archive_time()) && !obj.deleted())
                .collect(),
        )
    }

    // Watch events as a series of table representation for objects
    pub fn watch_table_events(
        &self,
        list: List,
        selector: Selector,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        log::trace!("Watching table {}...", list.get_path());

        let mut events = vec![];
        for object in self
            .objects(list.get_path())?
            .filter(|obj| selector.filter(obj))
        {
            let crd_path = self.archive.join(list.get_crd_path().unwrap_or_default());
            let event = object.table_watch_event(crd_path, &list.version)?;
            events.push(event)
        }

        Ok(events)
    }

    // Watch events as a series of json enoded objects
    pub fn watch_events(
        &self,
        list: List,
        selector: Selector,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        log::trace!("Watching list {}...", list.get_path());

        self.objects(list.get_path())?
            .filter(|obj| selector.filter(obj))
            .map(|obj| obj.watch_event())
            .map(|ev| serde_json::to_value(ev).map_err(Into::into))
            .collect()
    }

    fn objects(&self, path: ArchivePath) -> anyhow::Result<impl Iterator<Item = DynamicObject>> {
        let mut new_objects = HashMap::new();
        let objects = self.objects_state.take();
        let path = self.archive.join(path);
        let paths = glob::glob(
            path.to_str()
                .map_or_else(|| bail!("Unable to convert path to string: {path:?}"), Ok)?,
        )?;
        let mut items = vec![];
        for path in paths {
            let path = path?;
            match objects.get(&path) {
                Some(previous) if path.with_extension("patch").exists() => {
                    new_objects.insert(path.clone(), previous.clone());
                    let versions = self.interpolate(
                        previous,
                        path.with_extension("patch"),
                        previous.last_sync_timestamp().unwrap_or_default(),
                        self.archive_time(),
                    )?;
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
                        .versions(path.clone())?
                        .into_iter()
                        .filter(|obj: &DynamicObject| obj.older(self.archive_time()))
                    {
                        new_objects.insert(path.clone(), version.clone());
                        items.push(version);
                    }
                }
            };
        }

        self.objects_state.replace(new_objects);

        Ok(items.into_iter())
    }

    fn items(
        &self,
        path: PathBuf,
        selector: Selector,
    ) -> anyhow::Result<impl Iterator<Item = DynamicObject>> {
        let paths = glob::glob(
            path.to_str()
                .map_or_else(|| bail!("Unable to convert path to string: {path:?}"), Ok)?,
        )?;
        let mut items = vec![];
        for path in paths {
            let obj = self.read(path?)?;
            if selector.filter(&obj) {
                items.push(obj);
            }
        }

        Ok(items.into_iter())
    }

    pub fn load_raw(&self, path: ArchivePath) -> anyhow::Result<String> {
        log::debug!("Reading file {}...", path);

        Reader::read_raw(self.archive.join(path))
    }

    pub fn load(&self, get: Get) -> anyhow::Result<serde_json::Value> {
        let path = get.get_path();
        log::debug!("Reading file {}...", path);

        let obj: DynamicObject = self.read(self.archive.join(path))?;
        if obj.deleted() {
            bail!("Object was deleted")
        }

        serde_json::to_value(obj).map_err(Into::into)
    }

    pub fn list(&self, list: List, selector: Selector) -> anyhow::Result<serde_json::Value> {
        log::trace!("Reading list {}...", list.get_path());

        let path = self.archive.join(list.get_path());

        serde_json::to_value(ObjectValueList::new(
            list,
            self.items(path, selector)?
                .filter(|obj| obj.older(self.archive_time()) && !obj.deleted())
                .collect(),
        ))
        .map_err(Into::into)
    }

    pub fn read<R: DeserializeOwned + Clone>(&self, path: PathBuf) -> anyhow::Result<R> {
        self.versions(path)?
            .last()
            .cloned()
            .ok_or(anyhow::anyhow!("failed to find object"))
    }

    // Collect a sequence of versions for the given object until clusters equivalent of Utc::now()
    fn versions<R: DeserializeOwned>(&self, path: PathBuf) -> anyhow::Result<Vec<R>> {
        let object = File::open(path.clone())?;
        match path.with_extension("patch").exists() {
            false => Ok(vec![serde_yaml::from_reader(object)?]),
            true => {
                let original: serde_json::Value = serde_yaml::from_reader(object)?;
                Some(original.clone())
                    .into_iter()
                    .chain(self.interpolate(
                        &original,
                        path.with_extension("patch"),
                        Default::default(),
                        self.archive_time(),
                    )?)
                    .map(|version| serde_json::from_value(version).map_err(Into::into))
                    .collect()
            }
        }
    }

    fn read_lines(filename: PathBuf) -> io::Result<io::Lines<io::BufReader<File>>> {
        let file = File::open(filename)?;
        Ok(io::BufReader::new(file).lines())
    }

    // Goes through all json patches and applies them on the resource in order
    fn interpolate<R: Serialize + DeserializeOwned>(
        &self,
        target: &R,
        patches_file: PathBuf,
        from: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> anyhow::Result<Vec<R>> {
        let mut target = serde_json::to_value(target)?;
        let mut versions = vec![];
        for list in Reader::read_lines(patches_file)? {
            let patches: Vec<PatchOperation> = serde_json::from_str(&list?)?;
            let mut do_apply = false;
            for p in patches.clone() {
                match p {
                    PatchOperation::Replace(ReplaceOperation { path, value })
                    | PatchOperation::Add(AddOperation { path, value })
                        if path == Pointer::new(UPDATED_PATH)
                            || path == Pointer::new(ADDED_PATH)
                            || path == Pointer::new(DELETED_PATH) =>
                    {
                        let last_sync_timestamp: DateTime<Utc> = serde_json::from_value(value)?;
                        if last_sync_timestamp >= until {
                            let wait_duration = (last_sync_timestamp - until).to_std()?;
                            self.next_patch_time
                                .replace(self.next_patch_time.take().min(wait_duration));
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

    fn read_raw(path: PathBuf) -> anyhow::Result<String> {
        let mut file = File::open(path)?;
        let mut data = String::new();
        File::read_to_string(&mut file, &mut data)?;
        Ok(data)
    }
}
