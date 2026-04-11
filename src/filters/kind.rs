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
pub struct Kind<M: Match> {
    kind: FilterRegex,

    #[serde(skip)]
    #[schemars(skip)]
    matcher: std::marker::PhantomData<M>,
}

impl<R: ResourceThreadSafe, M: Match> Filter<R> for Kind<M>
where
    M: Match + Send + Sync,
{
    #[instrument(skip_all, fields(group = gvk.group, kind = gvk.kind, kind_list = self.kind.to_string()))]
    fn filter_object(&self, _: &R, gvk: &GroupVersionKind) -> Option<bool> {
        let accepted = M::matches(self.kind.matches(&gvk.kind));

        if !accepted {
            tracing::debug!(
                "Kind filter excluded object as it is not present in the expected kind list"
            );
        }

        Some(accepted)
    }
}

impl<M: Match> TryFrom<&str> for Kind<M> {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self {
            kind: value.try_into()?,
            matcher: std::marker::PhantomData,
        })
    }
}

impl<M: Match> TryFrom<String> for Kind<M> {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl From<Vec<Kind<Include>>> for FilterType {
    fn from(val: Vec<Kind<Include>>) -> Self {
        Self::KindInclude(val)
    }
}

impl From<Vec<Kind<Exclude>>> for FilterType {
    fn from(val: Vec<Kind<Exclude>>) -> Self {
        Self::KindExclude(val)
    }
}

#[cfg(test)]
mod tests {

    use k8s_openapi::{Resource, api::core::v1::Pod};
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
        let filter = Kind::<Include>::try_from(Pod::KIND).expect("Parse KindInclude");
        let pod_tm: TypeMeta = serde_yaml::from_str(POD).unwrap();
        let deploy_tm: TypeMeta = serde_yaml::from_str(DEPLOY).unwrap();
        let obj: DynamicObject =
            DynamicObject::new("test", &ApiResource::erase::<Pod>(&())).within("default");

        assert_eq!(
            <Kind<Include> as Filter<DynamicObject>>::filter_object(
                &filter,
                &obj,
                &GroupVersionKind::try_from(pod_tm).expect("parse GVK")
            ),
            Some(true)
        );
        assert_eq!(
            <Kind<Include> as Filter<DynamicObject>>::filter_object(
                &filter,
                &obj,
                &GroupVersionKind::try_from(deploy_tm).expect("parse GVK")
            ),
            Some(false)
        );
    }

    #[test]
    fn test_from_string() {
        let filter = Kind::<Include>::try_from("Pod").expect("Parse KindInclude");
        assert_eq!(filter.kind.0.as_str(), "Pod");
    }

    #[test]
    fn test_kind_exclude_filter() {
        let pod_tm: TypeMeta = serde_yaml::from_str(POD).unwrap();
        let deploy_tm: TypeMeta = serde_yaml::from_str(DEPLOY).unwrap();
        let obj: DynamicObject =
            DynamicObject::new("test", &ApiResource::erase::<Pod>(&())).within("default");

        let filter = Kind::<Exclude>::try_from("Pod").expect("KindExclude");
        assert_eq!(
            <Kind<Exclude> as Filter<DynamicObject>>::filter_object(
                &filter,
                &obj,
                &GroupVersionKind::try_from(pod_tm).expect("parse GVK")
            ),
            Some(false)
        );
        assert_eq!(
            <Kind<Exclude> as Filter<DynamicObject>>::filter_object(
                &filter,
                &obj,
                &GroupVersionKind::try_from(deploy_tm).expect("parse GVK")
            ),
            Some(true)
        );
    }
}
