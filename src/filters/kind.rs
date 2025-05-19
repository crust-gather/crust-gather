use kube::core::GroupVersionKind;
use serde::Deserialize;
use tracing::instrument;

use crate::scanners::interface::ResourceThreadSafe;

use super::filter::{Filter, FilterRegex, FilterType};

#[derive(Clone, Default, Deserialize, Debug)]
#[serde(try_from = "String")]
pub struct KindInclude {
    kind: FilterRegex,
}

impl<R: ResourceThreadSafe> Filter<R> for KindInclude {
    #[instrument(skip_all, fields(group = gvk.group, kind = gvk.kind, kind_list = self.kind.to_string()))]
    fn filter_object(&self, _: &R, gvk: &GroupVersionKind) -> Option<bool> {
        let accepted = self.kind.matches(&gvk.kind);

        if !accepted {
            tracing::debug!(
                "KindInclude filter excluded object as it is not present in the kind list"
            );
        }

        Some(accepted)
    }
}

impl TryFrom<String> for KindInclude {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(Self {
            kind: value.try_into()?,
        })
    }
}

impl From<Vec<KindInclude>> for FilterType {
    fn from(val: Vec<KindInclude>) -> Self {
        Self::KindInclude(val)
    }
}

#[derive(Clone, Default, Deserialize, Debug)]
#[serde(try_from = "String")]
pub struct KindExclude {
    kinds: FilterRegex,
}

impl<R: ResourceThreadSafe> Filter<R> for KindExclude {
    #[instrument(skip_all, fields(group = gvk.group, kind = gvk.kind, exclude = self.kinds.to_string()))]
    fn filter_object(&self, _: &R, gvk: &GroupVersionKind) -> Option<bool> {
        let accepted = !self.kinds.matches(&gvk.kind);

        if !accepted {
            tracing::debug!(
                "KindExclude filter excluded object as it is present in the exclude kind list",
            );
        }

        Some(accepted)
    }
}

impl TryFrom<String> for KindExclude {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(Self {
            kinds: value.try_into()?,
        })
    }
}

impl From<Vec<KindExclude>> for FilterType {
    fn from(val: Vec<KindExclude>) -> Self {
        Self::KindExclude(val)
    }
}

#[cfg(test)]
mod tests {

    use k8s_openapi::{api::core::v1::Pod, Resource};
    use kube::core::{ApiResource, DynamicObject, TypeMeta};

    use super::*;

    static POD: &str = r"---
    apiVersion: v1
    kind: Pod";

    static DEPLOY: &str = r"---
    apiVersion: v1
    kind: Deployment
    ";

    #[test]
    fn test_kind_include_filter() {
        let filter = KindInclude::try_from(Pod::KIND.to_string()).expect("Parse KindInclude");
        let pod_tm: TypeMeta = serde_yaml::from_str(POD).unwrap();
        let deploy_tm: TypeMeta = serde_yaml::from_str(DEPLOY).unwrap();
        let obj: DynamicObject =
            DynamicObject::new("test", &ApiResource::erase::<Pod>(&())).within("default");

        assert_eq!(
            <KindInclude as Filter<DynamicObject>>::filter_object(
                &filter,
                &obj,
                &GroupVersionKind::try_from(pod_tm).expect("parse GVK")
            ),
            Some(true)
        );
        assert_eq!(
            <KindInclude as Filter<DynamicObject>>::filter_object(
                &filter,
                &obj,
                &GroupVersionKind::try_from(deploy_tm).expect("parse GVK")
            ),
            Some(false)
        );
    }

    #[test]
    fn test_from_string() {
        let filter = KindInclude::try_from("Pod".to_string()).expect("Parse KindInclude");
        assert_eq!(filter.kind.0.as_str(), "Pod");
    }

    #[test]
    fn test_kind_exclude_filter() {
        let pod_tm: TypeMeta = serde_yaml::from_str(POD).unwrap();
        let deploy_tm: TypeMeta = serde_yaml::from_str(DEPLOY).unwrap();
        let obj: DynamicObject =
            DynamicObject::new("test", &ApiResource::erase::<Pod>(&())).within("default");

        let filter = KindExclude::try_from("Pod".to_string()).expect("KindExclude");
        assert_eq!(
            <KindExclude as Filter<DynamicObject>>::filter_object(
                &filter,
                &obj,
                &GroupVersionKind::try_from(pod_tm).expect("parse GVK")
            ),
            Some(false)
        );
        assert_eq!(
            <KindExclude as Filter<DynamicObject>>::filter_object(
                &filter,
                &obj,
                &GroupVersionKind::try_from(deploy_tm).expect("parse GVK")
            ),
            Some(true)
        );
    }
}
