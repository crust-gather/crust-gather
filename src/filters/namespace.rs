use kube::core::GroupVersionKind;
use serde::Deserialize;
use tracing::instrument;

use crate::scanners::interface::ResourceThreadSafe;

use super::filter::{Filter, FilterRegex, FilterType};

#[derive(Clone, Default, Deserialize, Debug)]
#[serde(try_from = "String")]
pub struct NamespaceInclude {
    namespace: FilterRegex,
}

/// Filters kubernetes namespaced resources based on whether their namespace is in the allowed
/// list. Returns true if the object's namespace is in the allowed namespaces,
/// or the namespace list is empty or the resource is cluster-scoped.
impl<R: ResourceThreadSafe> Filter<R> for NamespaceInclude {
    #[instrument(skip_all, fields(name = obj.name_any(), namespace = obj.namespace(), namespace_list = self.namespace.to_string()))]
    fn filter_object(&self, obj: &R, _: &GroupVersionKind) -> Option<bool> {
        let empty = obj.namespace().unwrap_or_default().is_empty();
        let included = self.namespace.matches(&obj.namespace().unwrap_or_default());
        match (empty, included) {
            (true, _) => None,
            (false, true) => Some(true),
            (false, false) => {
                tracing::debug!(
                    "NamespaceExclude filter excluded object as it is not present in the namespace list"
                );
                Some(false)
            }
        }
    }
}

impl TryFrom<String> for NamespaceInclude {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(Self {
            namespace: value.try_into()?,
        })
    }
}

impl From<Vec<NamespaceInclude>> for FilterType {
    fn from(val: Vec<NamespaceInclude>) -> Self {
        Self::NamespaceInclude(val)
    }
}

#[derive(Clone, Default, Deserialize, Debug)]
#[serde(try_from = "String")]
pub struct NamespaceExclude {
    namespace: FilterRegex,
}

impl TryFrom<String> for NamespaceExclude {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(Self {
            namespace: value.try_into()?,
        })
    }
}

impl From<Vec<NamespaceExclude>> for FilterType {
    fn from(val: Vec<NamespaceExclude>) -> Self {
        Self::NamespaceExclude(val)
    }
}

impl<R: ResourceThreadSafe> Filter<R> for NamespaceExclude {
    #[instrument(fields(name = obj.name_any(), namespace = obj.namespace(), namespace_list = self.namespace.to_string()))]
    fn filter_object(&self, obj: &R, _: &GroupVersionKind) -> Option<bool> {
        let empty = obj.namespace().unwrap_or_default().is_empty();
        let excluded = !self.namespace.matches(&obj.namespace().unwrap_or_default());
        match (empty, excluded) {
            (true, _) => None,
            (false, true) => Some(true),
            (false, false) => {
                tracing::debug!("NamespaceExclude filter excluded object as it is not present in the namespace list");
                Some(false)
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use k8s_openapi::api::core::v1::Pod;
    use kube::core::{ApiResource, DynamicObject, TypeMeta};

    use super::*;

    static POD: &str = r"---
    apiVersion: v1
    kind: Pod";

    #[test]
    fn test_include_namespace_filter() {
        let pod_tm: TypeMeta = serde_yaml::from_str(POD).unwrap();

        let filter = NamespaceInclude::try_from("default".to_string()).unwrap();

        let obj = DynamicObject::new("test", &ApiResource::erase::<Pod>(&())).within("default");
        assert_eq!(
            filter.filter_object(
                &obj,
                &GroupVersionKind::try_from(&pod_tm).expect("parse GVK")
            ),
            Some(true)
        );

        let filter = NamespaceInclude::try_from("default".to_string()).unwrap();

        let obj = DynamicObject::new("other", &ApiResource::erase::<Pod>(&())).within("other");
        assert_eq!(
            filter.filter_object(
                &obj,
                &GroupVersionKind::try_from(&pod_tm).expect("parse GVK")
            ),
            Some(false)
        );

        let obj = DynamicObject::new("other", &ApiResource::erase::<Pod>(&()));
        assert_eq!(
            filter.filter_object(
                &obj,
                &GroupVersionKind::try_from(&pod_tm).expect("parse GVK")
            ),
            None
        );
    }

    #[test]
    fn test_exclude_namespace() {
        let pod_tm: TypeMeta = serde_yaml::from_str(POD).unwrap();
        let filter = NamespaceExclude::try_from("default".to_string()).unwrap();

        let obj = DynamicObject::new("test", &ApiResource::erase::<Pod>(&())).within("default");
        assert_eq!(
            filter.filter_object(
                &obj,
                &GroupVersionKind::try_from(&pod_tm).expect("parse GVK")
            ),
            Some(false)
        );

        let filter = NamespaceExclude::try_from("default".to_string()).unwrap();

        let obj = DynamicObject::new("test", &ApiResource::erase::<Pod>(&())).within("other");
        assert_eq!(
            filter.filter_object(
                &obj,
                &GroupVersionKind::try_from(&pod_tm).expect("parse GVK")
            ),
            Some(true)
        );

        let obj = DynamicObject::new("other", &ApiResource::erase::<Pod>(&()));
        assert_eq!(
            filter.filter_object(
                &obj,
                &GroupVersionKind::try_from(&pod_tm).expect("parse GVK")
            ),
            None
        );
    }

    #[test]
    fn test_include_from_string() {
        let namespaces = NamespaceInclude::try_from("default".to_string()).unwrap();
        assert_eq!(namespaces.namespace.0.as_str(), "default");
    }

    #[test]
    fn test_exclude_from_string() {
        let namespaces = NamespaceExclude::try_from("default".to_string()).unwrap();
        assert_eq!(namespaces.namespace.0.as_str(), "default");
    }
}
