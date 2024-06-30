use std::{fs::File, io::Read, path::PathBuf};

use anyhow::bail;
use k8s_openapi::{
    apiextensions_apiserver::pkg::apis::apiextensions::v1::{
        CustomResourceColumnDefinition, CustomResourceDefinition, CustomResourceDefinitionVersion,
    },
    serde_json::{self, json},
};
use kube::core::{PartialObjectMeta, Resource, TypeMeta};
use kube_core::DynamicObject;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json_path::JsonPath;

use super::{
    representation::{ArchivePath, Container, LogGroup, NamespaceName},
    writer::Archive,
};

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
        ArchivePath::new_path(
            NamespaceName::new(Some(self.name.clone()), self.namespace.clone()),
            match self.group.clone() {
                Some(group) => TypeMeta {
                    api_version: format!("{}/{}", group, self.version),
                    kind: self.kind.trim_end_matches('s').to_string(),
                },
                None => TypeMeta {
                    api_version: self.version.clone(),
                    kind: self.kind.trim_end_matches('s').to_string(),
                },
            },
        )
    }

    pub fn get_logs_path(&self, log: &Log) -> ArchivePath {
        ArchivePath::new_logs(
            NamespaceName::new(Some(self.name.clone()), self.namespace.clone()),
            TypeMeta {
                api_version: self.version.clone(),
                kind: self.kind.trim_end_matches('s').to_string(),
            },
            match log.previous {
                Some(true) => LogGroup::Previous(log.container.clone()),
                _ => LogGroup::Current(log.container.clone()),
            },
        )
    }
}

#[derive(Deserialize, Clone)]
pub struct Log {
    container: Container,
    previous: Option<bool>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Selector {
    #[serde(rename = "labelSelector")]
    label_selector: Option<String>,
}

impl Selector {
    fn matches<R: Resource>(&self, res: R) -> Option<R> {
        match &self.label_selector {
            Some(selector) => selector
                .split(',')
                .map(|selector| Selector::matches_selector(&selector.to_string(), &res))
                .all(|matches| matches.is_some())
                .then_some(res),
            None => Some(res),
        }
    }

    fn matches_selector<R: Resource>(selector: &String, res: &R) -> Option<()> {
        match selector {
            selector if selector.contains("!=") => {
                let (key, value) = selector.split_once("!=")?;
                match &res.meta().labels {
                    Some(labels) => {
                        labels.get(key).map(|v| v == value).and_then(|_| None)?;
                        Some(())
                    }
                    None => Some(()),
                }
            }
            selector if selector.contains("=") => {
                let (key, value) = selector.split_once("=")?;
                res.meta()
                    .labels
                    .clone()
                    .map(|labels| {
                        let v = labels.get(key)?;
                        (v == value).then_some(())
                    })
                    .flatten()
            }
            key if key.starts_with('!') => res // todo: split off
                .meta()
                .labels
                .clone()
                .map(|labels| {
                    labels.get(&key.clone().split_off(1)).and_then(|_| None)?;
                    Some(())
                })
                .flatten(),
            key => res
                .meta()
                .labels
                .clone()
                .map(|labels| labels.get(key).and_then(|_| Some(())))
                .flatten(),
        }
    }
}

#[derive(Deserialize, Clone)]
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
        ArchivePath::new_path(
            NamespaceName::new(None, self.namespace.clone()),
            match self.group.clone() {
                Some(group) => TypeMeta {
                    api_version: format!("{}/{}", group, self.version),
                    kind: self.kind.trim_end_matches('s').to_string(),
                },
                None => TypeMeta {
                    api_version: self.version.clone(),
                    kind: self.kind.trim_end_matches('s').to_string(),
                },
            },
        )
    }

    pub fn get_crd_path(&self) -> Option<ArchivePath> {
        self.group.clone().map(|group| {
            ArchivePath::new_path(
                NamespaceName::new(Some(format!("{}.{}", self.kind, group)), None),
                TypeMeta {
                    api_version: CustomResourceDefinition::api_version(&()).to_string(),
                    kind: CustomResourceDefinition::kind(&()).to_string(),
                },
            )
        })
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
struct Table(Vec<TableEntry>);

#[derive(Clone)]
struct TableEntry {
    column: CustomResourceColumnDefinition,
    json_path: JsonPath,
}

impl TableEntry {
    fn new(column: &CustomResourceColumnDefinition) -> anyhow::Result<Self> {
        Ok(Self {
            column: column.clone(),
            json_path: JsonPath::parse(format!("${}", column.json_path).as_str())?,
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
    fn new(columns: Vec<TableEntry>) -> Self {
        let mut data = vec![TableEntry {
            column: CustomResourceColumnDefinition {
                name: "Name".to_string(),
                type_: "string".to_string(),
                ..Default::default()
            },
            json_path: JsonPath::parse("$.metadata.name").unwrap(),
        }];
        data.extend(columns.to_vec());
        Self(data)
    }

    fn to_row(&self, obj: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        let Table(rows) = self;
        let cells: Vec<&serde_json::Value> = rows
            .iter()
            .filter_map(|r| r.json_path.query(&obj).first())
            .collect();

        Ok(json!({
            "cells": cells,
            "object": serde_json::from_value::<PartialObjectMeta>(obj)?,
        }))
    }

    fn definitions(&self) -> Vec<serde_json::Value> {
        self.0.iter().map(|r| r.to_definition()).collect()
    }

    fn rows(&self, items: Vec<impl Serialize>) -> anyhow::Result<Vec<serde_json::Value>> {
        items
            .iter()
            .map(|i| self.to_row(serde_json::to_value(i)?))
            .collect()
    }
}

impl FromIterator<TableEntry> for Table {
    fn from_iter<T: IntoIterator<Item = TableEntry>>(iter: T) -> Self {
        Self::new(iter.into_iter().collect())
    }
}

#[derive(Clone)]
pub struct Reader(Archive);

impl Reader {
    pub fn new(archive: &Archive) -> Self {
        Self(archive.clone())
    }

    pub fn load_table(&self, list: List, selector: Selector) -> anyhow::Result<serde_json::Value> {
        let Reader(archive) = self;

        let crd_file = match list.get_crd_path() {
            Some(crd_path) => match File::open(archive.join(crd_path)) {
                Ok(crd_file) => serde_yaml::from_reader(crd_file)?,
                Err(_) => CustomResourceDefinition::default(),
            },
            None => CustomResourceDefinition::default(),
        };
        let mut items: Vec<DynamicObject> = vec![];

        let version = crd_file
            .spec
            .versions
            .iter()
            .find(|crd| crd.name == list.version);

        let columns: anyhow::Result<Table> = match version {
            Some(CustomResourceDefinitionVersion {
                additional_printer_columns: Some(columns),
                ..
            }) => columns.iter().map(TableEntry::new).collect(),
            _ => Ok(Table::new(vec![])),
        };

        let table = columns?;

        let paths = glob::glob(archive.join(list.get_path()).to_str().map_or_else(
            || {
                bail!(
                    "Unable to convert path to string: {:?}",
                    archive.join(list.get_path())
                )
            },
            Ok,
        )?)?;

        for path in paths {
            if let Some(obj) = selector.matches(Reader::read(path?)?) {
                items.push(obj);
            }
        }

        Ok(json!({
            "kind": "Table",
            "apiVersion": "meta.k8s.io/v1",
            "columnDefinitions": table.definitions(),
            "rows": table.rows(items)?,
        }))
    }

    pub fn load_raw(&self, path: ArchivePath) -> anyhow::Result<String> {
        log::debug!("Reading file {}...", path);

        let Reader(archive) = self;
        Reader::read_raw(archive.join(path))
    }

    pub fn load(&self, get: Get) -> anyhow::Result<serde_json::Value> {
        let path = get.get_path();
        log::debug!("Reading file {}...", path);

        let Reader(archive) = self;
        Reader::read(archive.join(path))
    }

    pub fn load_list(&self, list: List, selector: Selector) -> anyhow::Result<serde_json::Value> {
        log::debug!("Reading list {}...", list.get_path());

        let Reader(archive) = self;
        let path = archive.join(list.get_path());
        let paths = glob::glob(
            path.to_str()
                .map_or_else(|| bail!("Unable to convert path to string: {path:?}"), Ok)?,
        )?;
        let mut items = vec![];
        for path in paths {
            if let Some(obj) = selector.matches(Reader::read(path?)?) {
                items.push(obj);
            }
        }

        Ok(serde_json::to_value(ObjectValueList::new(list, items))?)
    }

    fn read<V: DeserializeOwned>(path: PathBuf) -> anyhow::Result<V> {
        Ok(serde_yaml::from_reader(File::open(path)?)?)
    }

    fn read_raw(path: PathBuf) -> anyhow::Result<String> {
        let mut file = File::open(path)?;
        let mut data = String::new();
        File::read_to_string(&mut file, &mut data)?;
        Ok(data)
    }
}
