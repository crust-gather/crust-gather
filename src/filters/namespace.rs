use std::marker::PhantomData;

use kube::core::GroupVersionKind;
use rmcp::schemars;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    filters::filter::{Exclude, Include, Match},
    scanners::interface::ResourceThreadSafe,
};

use super::filter::{Filter, FilterRegex, FilterType};

/// Filters kubernetes namespaced resources based on whether their namespace is in the allowed
/// list. Returns true if the object's namespace is in the allowed namespaces,
/// or the namespace list is empty or the resource is cluster-scoped.
#[derive(Clone, Default, Serialize, Deserialize, Debug, schemars::JsonSchema)]
#[serde(try_from = "String")]
pub struct Namespace<M: Match> {
    namespace: FilterRegex,

    #[serde(skip)]
    #[schemars(skip)]
    matcher: PhantomData<M>,
}

impl<M: Match> TryFrom<&str> for Namespace<M> {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self {
            namespace: value.try_into()?,
            matcher: PhantomData,
        })
    }
}

impl<M: Match> TryFrom<String> for Namespace<M> {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl From<Vec<Namespace<Include>>> for FilterType {
    fn from(val: Vec<Namespace<Include>>) -> Self {
        Self::NamespaceInclude(val)
    }
}

impl From<Vec<Namespace<Exclude>>> for FilterType {
    fn from(val: Vec<Namespace<Exclude>>) -> Self {
        Self::NamespaceExclude(val)
    }
}

impl<R: ResourceThreadSafe, M> Filter<R> for Namespace<M>
where
    M: Match + Send + Sync,
{
    #[instrument(fields(name = obj.name_any(), namespace = obj.namespace(), namespace_list = self.namespace.to_string()))]
    fn filter_object(&self, obj: &R, _: &GroupVersionKind) -> Option<bool> {
        let empty = obj.namespace().unwrap_or_default().is_empty();
        let matches = M::matches(self.namespace.matches(&obj.namespace().unwrap_or_default()));
        match (empty, matches) {
            (true, _) => None,
            (false, true) => Some(true),
            (false, false) => {
                tracing::debug!(
                    "Namespace filter excluded object as it is not present in the expected namespace list"
                );
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

        let filter = Namespace::<Include>::try_from("default").unwrap();

        let obj = DynamicObject::new("test", &ApiResource::erase::<Pod>(&())).within("default");
        assert_eq!(
            filter.filter_object(
                &obj,
                &GroupVersionKind::try_from(&pod_tm).expect("parse GVK")
            ),
            Some(true)
        );

        let filter = Namespace::<Include>::try_from("default").unwrap();

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
        let filter = Namespace::<Exclude>::try_from("default").unwrap();

        let obj = DynamicObject::new("test", &ApiResource::erase::<Pod>(&())).within("default");
        assert_eq!(
            filter.filter_object(
                &obj,
                &GroupVersionKind::try_from(&pod_tm).expect("parse GVK")
            ),
            Some(false)
        );

        let filter = Namespace::<Exclude>::try_from("default").unwrap();

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
        let namespaces = Namespace::<Include>::try_from("default").unwrap();
        assert_eq!(namespaces.namespace.0.as_str(), "default");
    }

    #[test]
    fn test_exclude_from_string() {
        let namespaces = Namespace::<Exclude>::try_from("default").unwrap();
        assert_eq!(namespaces.namespace.0.as_str(), "default");
    }
}
