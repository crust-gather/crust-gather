use kube_core::{DynamicObject, GroupVersionKind};
use serde::Deserialize;

use super::filter::{Filter, FilterDefinition, FilterRegex, FilterType};

#[derive(Clone, Default, Deserialize)]
#[serde(try_from = "String")]
pub struct KindInclude {
    kind: FilterRegex,
}

impl Filter for KindInclude {
    fn filter_object(&self, _: &DynamicObject) -> bool {
        true
    }

    fn filter_api(&self, gvk: &GroupVersionKind) -> bool {
        let accepted = self.kind.matches(&gvk.kind);

        if !accepted {
            log::debug!(
                "KindInclude filter excluded {}/{} as it is not present in the kind list {}",
                gvk.group,
                gvk.kind,
                self.kind,
            )
        }

        accepted
    }
}

impl FilterDefinition for KindInclude {}

impl TryFrom<String> for KindInclude {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(KindInclude {
            kind: value.try_into()?,
        })
    }
}

impl Into<FilterType> for KindInclude {
    fn into(self) -> FilterType {
        FilterType::KindInclude(self)
    }
}

#[derive(Clone, Default, Deserialize)]
#[serde(try_from = "String")]
pub struct KindExclude {
    kinds: FilterRegex,
}

impl Filter for KindExclude {
    fn filter_api(&self, gvk: &GroupVersionKind) -> bool {
        let accepted = !self.kinds.matches(&gvk.kind);

        if !accepted {
            log::debug!(
                "KindExclude filter excluded {}/{} as it is present in the exclude kind list {}",
                gvk.group,
                gvk.kind,
                self.kinds
            )
        }

        accepted
    }

    fn filter_object(&self, _: &DynamicObject) -> bool {
        true
    }
}

impl FilterDefinition for KindExclude {}

impl TryFrom<String> for KindExclude {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(KindExclude {
            kinds: value.try_into()?,
        })
    }
}

impl Into<FilterType> for KindExclude {
    fn into(self) -> FilterType {
        FilterType::KindExclude(self)
    }
}

#[cfg(test)]
mod tests {

    use k8s_openapi::{api::core::v1::Pod, Resource};
    use kube_core::TypeMeta;

    use super::*;

    #[test]
    fn test_kind_include_filter() {
        let pod = r#"---
        apiVersion: v1
        kind: Pod"#;

        let deploy = r#"---
        apiVersion: v1
        kind: Deployment
        "#;

        let pod_tm: TypeMeta = serde_yaml::from_str(pod).unwrap();
        let deploy_tm: TypeMeta = serde_yaml::from_str(deploy).unwrap();

        let filter = KindInclude::try_from(Pod::KIND.to_string()).expect("Parse KindInclude");

        assert!(filter.filter_api(&GroupVersionKind::try_from(pod_tm.clone()).expect("parse GVK")));
        assert!(!filter.filter_api(&GroupVersionKind::try_from(deploy_tm).expect("parse GVK")));
    }

    #[test]
    fn test_from_string() {
        let filter = KindInclude::try_from("Pod".to_string()).expect("Parse KindInclude");
        assert_eq!(filter.kind.0.as_str(), "Pod");
    }

    #[test]
    fn test_kind_exclude_filter() {
        let pod = r#"---
        apiVersion: v1
        kind: Pod"#;

        let deploy = r#"---
        apiVersion: v1
        kind: Deployment
        "#;

        let pod_tm: TypeMeta = serde_yaml::from_str(pod).unwrap();
        let deploy_tm: TypeMeta = serde_yaml::from_str(deploy).unwrap();

        let pod = KindExclude::try_from("Pod".to_string()).expect("KindExclude");
        assert!(!pod.filter_api(&GroupVersionKind::try_from(pod_tm.clone()).expect("parse GVK")));
        assert!(pod.filter_api(&GroupVersionKind::try_from(deploy_tm.clone()).expect("parse GVK")));
    }
}
