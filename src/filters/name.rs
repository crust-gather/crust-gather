use kube::core::GroupVersionKind;
use rmcp::schemars;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::scanners::interface::ResourceThreadSafe;

use super::filter::{Filter, FilterRegex, FilterType};

#[derive(Clone, Default, Serialize, Deserialize, Debug, schemars::JsonSchema)]
#[serde(try_from = "String")]
pub struct NameInclude {
    name: FilterRegex,
}

impl<R: ResourceThreadSafe> Filter<R> for NameInclude {
    #[instrument(skip_all, fields(name = obj.name_any(), include = self.name.to_string()))]
    fn filter_object(&self, obj: &R, _: &GroupVersionKind) -> Option<bool> {
        Some(self.name.matches(&obj.name_any()))
    }
}

impl TryFrom<String> for NameInclude {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.try_into()?,
        })
    }
}

impl From<Vec<NameInclude>> for FilterType {
    fn from(val: Vec<NameInclude>) -> Self {
        Self::NameInclude(val)
    }
}

#[derive(Clone, Default, Serialize, Deserialize, Debug, schemars::JsonSchema)]
#[serde(try_from = "String")]
pub struct NameExclude {
    name: FilterRegex,
}

impl<R: ResourceThreadSafe> Filter<R> for NameExclude {
    #[instrument(skip_all, fields(name = obj.name_any(), exclude = self.name.to_string()))]
    fn filter_object(&self, obj: &R, _: &GroupVersionKind) -> Option<bool> {
        Some(!self.name.matches(&obj.name_any()))
    }
}

impl TryFrom<String> for NameExclude {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.try_into()?,
        })
    }
}

impl From<Vec<NameExclude>> for FilterType {
    fn from(val: Vec<NameExclude>) -> Self {
        Self::NameExclude(val)
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
    fn test_name_include_filter() {
        let pod_tm: TypeMeta = serde_yaml::from_str(POD).unwrap();
        let filter = NameInclude::try_from("test.*".to_string()).unwrap();
        let obj = DynamicObject::new("test-pod", &ApiResource::erase::<Pod>(&())).within("default");
        assert_eq!(
            filter.filter_object(
                &obj,
                &GroupVersionKind::try_from(&pod_tm).expect("parse GVK")
            ),
            Some(true)
        );
    }

    #[test]
    fn test_name_exclude_filter() {
        let pod_tm: TypeMeta = serde_yaml::from_str(POD).unwrap();
        let filter = NameExclude::try_from("secret.*".to_string()).unwrap();
        let obj =
            DynamicObject::new("public-pod", &ApiResource::erase::<Pod>(&())).within("default");
        assert_eq!(
            filter.filter_object(
                &obj,
                &GroupVersionKind::try_from(&pod_tm).expect("parse GVK")
            ),
            Some(true)
        );
    }
}
