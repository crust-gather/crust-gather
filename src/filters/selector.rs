use std::{collections::BTreeMap, marker::PhantomData};

use kube::core::GroupVersionKind;
use rmcp::schemars;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    filters::filter::{Exclude, Include, Match},
    gather::selector::Expressions,
    scanners::interface::ResourceThreadSafe,
};

use super::filter::{Filter, FilterType};

#[derive(Clone, Default, Serialize, Deserialize, Debug, schemars::JsonSchema)]
#[serde(try_from = "String")]
pub struct Selector<M, G>
where
    G: SelectorSource + Send + Sync,
    M: Match + Send + Sync,
{
    selector: Expressions,

    #[serde(skip)]
    #[schemars(skip)]
    group: PhantomData<G>,

    #[serde(skip)]
    #[schemars(skip)]
    matcher: PhantomData<M>,
}

impl<R: ResourceThreadSafe, G, M> Filter<R> for Selector<M, G>
where
    G: SelectorSource + Send + Sync,
    M: Match + Send + Sync,
{
    #[instrument(skip_all, fields(name = obj.name_any(), include = self.selector.to_string()))]
    fn filter_object(&self, obj: &R, _: &GroupVersionKind) -> Option<bool> {
        Some(M::matches(self.selector.matches(G::select(obj))))
    }
}

impl<G, M> TryFrom<&str> for Selector<M, G>
where
    G: SelectorSource + Send + Sync,
    M: Match + Send + Sync,
{
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self {
            selector: value.try_into()?,
            group: PhantomData,
            matcher: PhantomData,
        })
    }
}

impl<G, M> TryFrom<String> for Selector<M, G>
where
    G: SelectorSource + Send + Sync,
    M: Match + Send + Sync,
{
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl From<Vec<Selector<Include, Labels>>> for FilterType {
    fn from(val: Vec<Selector<Include, Labels>>) -> Self {
        Self::LabelSelectorInclude(val)
    }
}

impl From<Vec<Selector<Include, Annotations>>> for FilterType {
    fn from(val: Vec<Selector<Include, Annotations>>) -> Self {
        Self::AnnotationSelectorInclude(val)
    }
}

impl From<Vec<Selector<Exclude, Labels>>> for FilterType {
    fn from(val: Vec<Selector<Exclude, Labels>>) -> Self {
        Self::LabelSelectorExclude(val)
    }
}

impl From<Vec<Selector<Exclude, Annotations>>> for FilterType {
    fn from(val: Vec<Selector<Exclude, Annotations>>) -> Self {
        Self::AnnotationSelectorExclude(val)
    }
}

#[derive(Clone, Default, Serialize, Deserialize, Debug, schemars::JsonSchema)]
pub struct Labels;

#[derive(Clone, Default, Serialize, Deserialize, Debug, schemars::JsonSchema)]
pub struct Annotations;

pub trait SelectorSource {
    fn select<R: ResourceThreadSafe>(obj: &R) -> &BTreeMap<String, String>;
}

impl SelectorSource for Labels {
    fn select<R: ResourceThreadSafe>(obj: &R) -> &BTreeMap<String, String> {
        obj.labels()
    }
}

impl SelectorSource for Annotations {
    fn select<R: ResourceThreadSafe>(obj: &R) -> &BTreeMap<String, String> {
        obj.annotations()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use k8s_openapi::api::core::v1::Pod;
    use kube::core::{ApiResource, DynamicObject, GroupVersionKind, TypeMeta};

    use crate::filters::filter::{Exclude, Include};

    use super::{Annotations, Filter, Labels, Selector};

    static POD: &str = r"---
    apiVersion: v1
    kind: Pod";

    fn pod_gvk() -> GroupVersionKind {
        let pod_tm: TypeMeta = serde_yaml::from_str(POD).unwrap();
        GroupVersionKind::try_from(&pod_tm).expect("parse GVK")
    }

    fn pod_with_metadata(labels: &[(&str, &str)], annotations: &[(&str, &str)]) -> DynamicObject {
        let mut obj =
            DynamicObject::new("test-pod", &ApiResource::erase::<Pod>(&())).within("default");

        obj.metadata.labels = Some(
            labels
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect::<BTreeMap<_, _>>(),
        );
        obj.metadata.annotations = Some(
            annotations
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect::<BTreeMap<_, _>>(),
        );
        obj
    }

    #[test]
    fn test_label_selector_include_filter() {
        let filter = Selector::<Include, Labels>::try_from("app=web").unwrap();
        let obj = pod_with_metadata(&[("app", "web")], &[]);

        assert_eq!(filter.filter_object(&obj, &pod_gvk()), Some(true));
    }

    #[test]
    fn test_label_selector_exclude_filter() {
        let filter = Selector::<Exclude, Labels>::try_from("app=web").unwrap();
        let obj = pod_with_metadata(&[("app", "web")], &[]);

        assert_eq!(filter.filter_object(&obj, &pod_gvk()), Some(false));
    }

    #[test]
    fn test_annotation_selector_include_filter() {
        let filter =
            Selector::<Include, Annotations>::try_from("team in (platform,infra)").unwrap();
        let obj = pod_with_metadata(&[], &[("team", "platform")]);

        assert_eq!(filter.filter_object(&obj, &pod_gvk()), Some(true));
    }

    #[test]
    fn test_annotation_selector_exclude_filter() {
        let filter =
            Selector::<Exclude, Annotations>::try_from("team in (platform,infra)").unwrap();
        let obj = pod_with_metadata(&[], &[("team", "platform")]);

        assert_eq!(filter.filter_object(&obj, &pod_gvk()), Some(false));
    }
}
