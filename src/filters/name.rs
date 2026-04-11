use kube::core::GroupVersionKind;
use rmcp::schemars;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    filters::filter::{Exclude, Include, Match},
    scanners::interface::ResourceThreadSafe,
};

use super::filter::{Filter, FilterRegex, FilterType};

#[derive(Clone, Default, Serialize, Deserialize, Debug, schemars::JsonSchema)]
#[serde(try_from = "String")]
pub struct Name<M: Match> {
    name: FilterRegex,

    #[serde(skip)]
    #[schemars(skip)]
    matcher: std::marker::PhantomData<M>,
}

impl<R: ResourceThreadSafe, M> Filter<R> for Name<M>
where
    M: Match + Send + Sync,
{
    #[instrument(skip_all, fields(name = obj.name_any(), include = self.name.to_string()))]
    fn filter_object(&self, obj: &R, _: &GroupVersionKind) -> Option<bool> {
        Some(M::matches(self.name.matches(&obj.name_any())))
    }
}

impl<M: Match> TryFrom<&str> for Name<M> {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.try_into()?,
            matcher: std::marker::PhantomData,
        })
    }
}

impl<M: Match> TryFrom<String> for Name<M> {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl From<Vec<Name<Include>>> for FilterType {
    fn from(val: Vec<Name<Include>>) -> Self {
        Self::NameInclude(val)
    }
}

impl From<Vec<Name<Exclude>>> for FilterType {
    fn from(val: Vec<Name<Exclude>>) -> Self {
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
        let filter = Name::<Include>::try_from("test.*").unwrap();
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
        let filter = Name::<Exclude>::try_from("secret.*").unwrap();
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
