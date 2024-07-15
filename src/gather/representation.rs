use std::{
    fmt::{self, Display},
    path::{Path, PathBuf},
};

use anyhow::bail;
use kube::{
    api::{ApiResource, ObjectMeta},
    core::TypeMeta,
};
use serde::Deserialize;

use crate::scanners::interface::ResourceThreadSafe;

pub trait NamespacedName {
    fn name(&self) -> Option<String>;
    fn namespace(&self) -> Option<String>;
}

#[derive(Default, Clone, Deserialize, Debug)]
pub struct NamespaceName {
    pub name: Option<String>,
    pub namespace: Option<String>,
}

impl NamespacedName for NamespaceName {
    fn name(&self) -> Option<String> {
        self.name.clone()
    }

    fn namespace(&self) -> Option<String> {
        self.namespace.clone()
    }
}

impl NamespacedName for &ObjectMeta {
    fn name(&self) -> Option<String> {
        self.name.to_owned()
    }

    fn namespace(&self) -> Option<String> {
        self.namespace.to_owned()
    }
}

impl NamespaceName {
    pub fn new(name: Option<String>, namespace: Option<String>) -> Self {
        Self { name, namespace }
    }
}

impl From<String> for NamespaceName {
    fn from(value: String) -> Self {
        match value.split_once('/') {
            Some(("", name)) => NamespaceName::new(Some(name.into()), None),
            Some((ns, "")) => NamespaceName::new(None, Some(ns.into())),
            Some((ns, name)) => NamespaceName::new(Some(name.into()), Some(ns.into())),
            None => NamespaceName::new(Some(value), None),
        }
    }
}

pub trait TypeMetaGetter {
    fn to_type_meta(&self) -> TypeMeta;
}

impl TypeMetaGetter for TypeMeta {
    fn to_type_meta(&self) -> TypeMeta {
        self.clone()
    }
}

impl TypeMetaGetter for ApiResource {
    fn to_type_meta(&self) -> TypeMeta {
        TypeMeta {
            api_version: self.api_version.clone(),
            kind: self.kind.clone(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Deserialize)]
pub struct Container(pub String);

impl From<String> for Container {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum LogGroup {
    Current(Container),
    Previous(Container),
    Node,
    NodePath,
}

impl fmt::Display for LogGroup {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Current(container) => write!(formatter, "{container:?}/current.log"),
            Self::Previous(container) => write!(formatter, "{container:?}/previous.log"),
            Self::Node => write!(formatter, "kubelet.log"),
            Self::NodePath => write!(formatter, "kubelet-log-path.log"),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ArchivePath {
    #[default]
    Empty,
    Cluster(PathBuf),
    Namespaced(PathBuf),
    NamespacedList(PathBuf),
    ClusterList(PathBuf),
    Logs(PathBuf),
    Custom(PathBuf),
}

impl ArchivePath {
    #[inline]
    /// Replaces invalid characters in a path with dashes, to make the path valid for GitHub artifacts.
    /// GitHub artifacts paths may not contain : * ? | characters. This replaces those characters with dashes.
    fn fix_github_artifacts_path(path: &str) -> String {
        path.replace([':', '*', '?', '|'], "-")
    }

    pub fn to_path<R: ResourceThreadSafe>(resource: &R, type_meta: TypeMeta) -> Self {
        ArchivePath::new_path(resource.meta(), type_meta)
    }

    pub fn new_path(namespace_name: impl NamespacedName, type_meta: TypeMeta) -> Self {
        let (api_version, kind) = (
            type_meta.api_version.to_lowercase().replace('/', "-"),
            type_meta.kind.to_lowercase(),
        );

        match (namespace_name.name(), namespace_name.namespace()) {
            (Some(name), Some(namespace)) => ArchivePath::Namespaced(
                format!("namespaces/{namespace}/{api_version}/{kind}/{name}.yaml").into(),
            ),
            (Some(name), None) => {
                ArchivePath::Cluster(format!("cluster/{api_version}/{kind}/{name}.yaml").into())
            }
            (None, Some(namespace)) => ArchivePath::NamespacedList(
                format!("namespaces/{namespace}/{api_version}/{kind}/*.yaml").into(),
            ),
            (None, None) => {
                ArchivePath::ClusterList(format!("**/{api_version}/{kind}/*.yaml").into())
            }
        }
    }

    pub fn new_logs(
        namespace_name: impl NamespacedName,
        type_meta: TypeMeta,
        logs: LogGroup,
    ) -> Self {
        match ArchivePath::new_path(namespace_name, type_meta) {
            ArchivePath::Namespaced(path) | ArchivePath::Cluster(path) => match logs {
                LogGroup::Current(Container(container)) => {
                    ArchivePath::Logs(path.with_extension("").join(container).join("current.log"))
                }
                LogGroup::Previous(Container(container)) => {
                    ArchivePath::Logs(path.with_extension("").join(container).join("previous.log"))
                }
                LogGroup::Node => ArchivePath::Logs(path.with_extension("").join("kubelet.log")),
                LogGroup::NodePath => {
                    ArchivePath::Logs(path.with_extension("").join("kubelet-log-path.log"))
                }
            },
            other => other,
        }
    }

    pub fn logs_path<R: ResourceThreadSafe>(
        resource: &R,
        type_meta: TypeMeta,
        logs: LogGroup,
    ) -> Self {
        ArchivePath::new_logs(resource.meta(), type_meta, logs)
    }

    pub fn parent(&self) -> Option<&Path> {
        match self {
            ArchivePath::Empty => None,
            ArchivePath::Cluster(path) => path.parent(),
            ArchivePath::Namespaced(path) => path.parent(),
            ArchivePath::Logs(path) => path.parent(),
            ArchivePath::Custom(path) => path.parent(),
            ArchivePath::NamespacedList(path) => Some(path.as_path()),
            ArchivePath::ClusterList(path) => Some(path.as_path()),
        }
    }
}

impl Display for ArchivePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArchivePath::Empty => write!(f, "<empty>"),
            ArchivePath::Cluster(path) => write!(f, "{}", path.display()),
            ArchivePath::Namespaced(path) => write!(f, "{}", path.display()),
            ArchivePath::Logs(path) => write!(f, "{}", path.display()),
            ArchivePath::Custom(path) => write!(f, "{}", path.display()),
            ArchivePath::NamespacedList(path) => write!(f, "{}", path.display()),
            ArchivePath::ClusterList(path) => write!(f, "{}", path.display()),
        }
    }
}

impl TryFrom<ArchivePath> for String {
    type Error = anyhow::Error;

    fn try_from(value: ArchivePath) -> Result<Self, Self::Error> {
        match value {
            ArchivePath::Empty => bail!("Path is empty"),
            ArchivePath::NamespacedList(path)
            | ArchivePath::ClusterList(path)
            | ArchivePath::Cluster(path)
            | ArchivePath::Namespaced(path)
            | ArchivePath::Logs(path)
            | ArchivePath::Custom(path) => match path.to_str() {
                Some(path) => Ok(ArchivePath::fix_github_artifacts_path(path)),
                None => bail!("Path is empty"),
            },
        }
    }
}

impl From<ArchivePath> for PathBuf {
    fn from(value: ArchivePath) -> Self {
        match value {
            ArchivePath::Empty => PathBuf::new(),
            ArchivePath::Cluster(path) => path,
            ArchivePath::Namespaced(path) => path,
            ArchivePath::NamespacedList(path) => path,
            ArchivePath::ClusterList(path) => path,
            ArchivePath::Logs(path) => path,
            ArchivePath::Custom(path) => path,
        }
    }
}

#[derive(Clone, Default, Debug)]
/// Representation holds the path and content for a serialized Kubernetes object.
pub struct Representation {
    path: ArchivePath,
    data: String,
}

impl Representation {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_data(self, data: &str) -> Self {
        Self {
            data: data.into(),
            ..self
        }
    }

    pub fn with_path(self, path: ArchivePath) -> Self {
        Self { path, ..self }
    }

    pub fn data(&self) -> &str {
        self.data.as_ref()
    }

    pub fn path(&self) -> ArchivePath {
        self.path.clone()
    }

    pub fn load_data(&self) -> Self {
        self.clone()
    }
}

#[cfg(test)]
mod tests {

    use k8s_openapi::api::core::v1::{Node, Pod};
    use kube::core::{ObjectMeta, Resource};

    use super::*;

    #[test]
    fn test_logs_path_current() {
        let resource = Pod {
            metadata: ObjectMeta {
                name: Some("name".into()),
                namespace: Some("namespace".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        let log_group = LogGroup::Current(Container("container".into()));

        let result = ArchivePath::logs_path(&resource, TypeMeta::resource::<Pod>(), log_group);

        assert_eq!(
            result,
            ArchivePath::Logs("namespaces/namespace/v1/pod/name/container/current.log".into())
        );
    }

    #[test]
    fn test_logs_path_previous() {
        let resource = Pod {
            metadata: ObjectMeta {
                name: Some("name".into()),
                namespace: Some("namespace".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        let log_group = LogGroup::Previous(Container("container".into()));

        let result = ArchivePath::logs_path(&resource, TypeMeta::resource::<Pod>(), log_group);

        assert_eq!(
            result,
            ArchivePath::Logs("namespaces/namespace/v1/pod/name/container/previous.log".into())
        );
    }

    #[test]
    fn test_node_logs() {
        let resource = Node {
            metadata: ObjectMeta {
                name: Some("name".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        let log_group = LogGroup::Node;

        let result = ArchivePath::logs_path(&resource, TypeMeta::resource::<Node>(), log_group);

        assert_eq!(
            result,
            ArchivePath::Logs("cluster/v1/node/name/kubelet.log".into())
        );
    }

    #[test]
    fn test_node_path_logs() {
        let resource = Node {
            metadata: ObjectMeta {
                name: Some("name".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        let log_group = LogGroup::NodePath;

        let result = ArchivePath::logs_path(&resource, TypeMeta::resource::<Node>(), log_group);

        assert_eq!(
            result,
            ArchivePath::Logs("cluster/v1/node/name/kubelet-log-path.log".into())
        );
    }

    #[test]
    fn test_cluster_list_path() {
        let resource = Node::default();
        let result = ArchivePath::new_path(resource.meta(), TypeMeta::resource::<Node>());

        assert_eq!(result, ArchivePath::ClusterList("**/v1/node/*.yaml".into()));
    }

    #[test]
    fn test_namespace_list_path() {
        let resource = Pod {
            metadata: ObjectMeta {
                namespace: Some("default".into()),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = ArchivePath::new_path(resource.meta(), TypeMeta::resource::<Pod>());

        assert_eq!(
            result,
            ArchivePath::NamespacedList("namespaces/default/v1/pod/*.yaml".into())
        );
    }
}
