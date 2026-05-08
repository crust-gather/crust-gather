use std::{
    borrow::Cow,
    fmt::{Debug, Display},
};

use kube::core::{GroupVersionKind, Resource};
use regex::Regex;
use rmcp::schemars::{self, Schema};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::scanners::interface::ResourceThreadSafe;

use super::{
    group::Group,
    kind::Kind,
    name::Name,
    namespace::Namespace,
    selector::{Annotations, Labels, Selector},
};

pub trait Filter<R>: Sync + Send
where
    R: Resource + Serialize + DeserializeOwned,
    R: Clone + Sync + Send + Debug,
{
    fn filter_object(&self, _: &R, _: &GroupVersionKind) -> Option<bool> {
        None
    }

    fn filter(&self, gvk: &GroupVersionKind, obj: &R) -> bool {
        self.filter_object(obj, gvk).unwrap_or(true)
    }
}

impl<T, R, I> Filter<R> for I
where
    R: Resource + Serialize + DeserializeOwned,
    R: Clone + Sync + Send + Debug,
    T: Filter<R>,
    I: IntoIterator<Item = T> + Clone + Sync + Send,
{
    fn filter_object(&self, obj: &R, gvk: &GroupVersionKind) -> Option<bool> {
        let mut f = self
            .clone()
            .into_iter()
            .flat_map(|f| f.filter_object(obj, gvk))
            .peekable();

        f.peek()?;

        Some(f.any(|accepted| accepted))
    }
}

#[derive(Default, Debug)]
pub struct FilterList(pub Vec<FilterType>);

#[derive(Default)]
pub struct FilterGroup(pub Vec<FilterList>);

#[derive(Clone, Debug)]
pub enum FilterType {
    NamespaceExclude(Vec<Namespace<Exclude>>),
    NamespaceInclude(Vec<Namespace<Include>>),
    KindInclude(Vec<Kind<Include>>),
    KindExclude(Vec<Kind<Exclude>>),
    GroupInclude(Vec<Group<Include>>),
    GroupExclude(Vec<Group<Exclude>>),
    NameInclude(Vec<Name<Include>>),
    NameExclude(Vec<Name<Exclude>>),
    LabelSelectorInclude(Vec<Selector<Include, Labels>>),
    LabelSelectorExclude(Vec<Selector<Exclude, Labels>>),
    AnnotationSelectorInclude(Vec<Selector<Include, Annotations>>),
    AnnotationSelectorExclude(Vec<Selector<Exclude, Annotations>>),
}

impl From<&Self> for FilterType {
    fn from(val: &Self) -> Self {
        val.clone()
    }
}

impl<R: ResourceThreadSafe> Filter<R> for FilterGroup {
    fn filter_object(&self, obj: &R, gvk: &GroupVersionKind) -> Option<bool> {
        let mut filter = self
            .0
            .iter()
            .filter_map(|f| f.filter_object(obj, gvk))
            .peekable();

        filter.peek()?;

        Some(filter.any(|accepted| accepted))
    }
}

impl<R: ResourceThreadSafe> Filter<R> for FilterList {
    fn filter_object(&self, obj: &R, gvk: &GroupVersionKind) -> Option<bool> {
        self.0
            .iter()
            .filter_map(|f| match f {
                FilterType::NamespaceExclude(e) => Some(eval_exclude(e, obj, gvk)),
                FilterType::KindExclude(e) => Some(eval_exclude(e, obj, gvk)),
                FilterType::GroupExclude(e) => Some(eval_exclude(e, obj, gvk)),
                FilterType::NameExclude(e) => Some(eval_exclude(e, obj, gvk)),
                FilterType::LabelSelectorExclude(e) => Some(eval_exclude(e, obj, gvk)),
                FilterType::AnnotationSelectorExclude(e) => Some(eval_exclude(e, obj, gvk)),
                FilterType::NamespaceInclude(i) => i.filter_object(obj, gvk),
                FilterType::KindInclude(i) => i.filter_object(obj, gvk),
                FilterType::GroupInclude(i) => i.filter_object(obj, gvk),
                FilterType::NameInclude(i) => i.filter_object(obj, gvk),
                FilterType::LabelSelectorInclude(i) => i.filter_object(obj, gvk),
                FilterType::AnnotationSelectorInclude(i) => i.filter_object(obj, gvk),
            })
            .all(|allowed| allowed)
            .into()
    }
}

fn eval_exclude<R, F>(filters: &[F], obj: &R, gvk: &GroupVersionKind) -> bool
where
    F: Filter<R>,
    R: ResourceThreadSafe,
{
    filters
        .iter()
        .filter_map(|f| f.filter_object(obj, gvk))
        .all(|allowed| allowed)
}

#[derive(Clone, Deserialize, Debug)]
#[serde(try_from = "String")]
pub struct FilterRegex(pub Regex);

impl schemars::JsonSchema for FilterRegex {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("FilterRegex")
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> Schema {
        String::json_schema(generator)
    }
}

impl Default for FilterRegex {
    fn default() -> Self {
        Self(Regex::new("").unwrap())
    }
}

impl Serialize for FilterRegex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.0.as_str())
    }
}

impl FilterRegex {
    pub fn matches(&self, s: &str) -> bool {
        self.0.is_match(s)
    }
}

impl Display for FilterRegex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_str())
    }
}

impl TryFrom<&str> for FilterRegex {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Ok(Self(Regex::new(s)?))
    }
}

impl TryFrom<String> for FilterRegex {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

#[derive(Clone, Default, Serialize, Deserialize, Debug, schemars::JsonSchema)]
pub struct Include;

#[derive(Clone, Default, Serialize, Deserialize, Debug, schemars::JsonSchema)]
pub struct Exclude;

pub trait Match: Debug {
    fn matches(matches: bool) -> bool;
}

impl Match for Include {
    fn matches(matches: bool) -> bool {
        matches
    }
}

impl Match for Exclude {
    fn matches(matches: bool) -> bool {
        !matches
    }
}

#[cfg(test)]
mod tests {

    use k8s_openapi::api::core::v1::Pod;
    use kube::core::{ApiResource, DynamicObject, TypeMeta};

    use crate::filters::{
        namespace::Namespace,
        selector::{Labels, Selector},
    };

    use super::*;

    static POD: &str = r"---
    apiVersion: v1
    kind: Pod";

    #[test]
    fn filter_all_filters_allow() {
        let obj = DynamicObject::new("", &ApiResource::erase::<Pod>(&())).within("test");

        let pod_tm: TypeMeta = serde_saphyr::from_str(POD).unwrap();
        assert_eq!(
            FilterList(vec![FilterType::NamespaceInclude(vec![
                Namespace::<Include>::try_from("test").unwrap(),
                Namespace::<Include>::try_from("other").unwrap(),
                Namespace::<Include>::try_from("test").unwrap(),
            ]),])
            .filter_object(
                &obj,
                &GroupVersionKind::try_from(pod_tm.clone()).expect("parse GVK")
            ),
            Some(true)
        );

        assert_eq!(
            FilterList(vec![
                FilterType::NamespaceInclude(vec![Namespace::<Include>::try_from("test").unwrap()]),
                FilterType::NamespaceExclude(vec![Namespace::<Exclude>::try_from("test").unwrap()]),
            ])
            .filter_object(
                &obj,
                &GroupVersionKind::try_from(pod_tm.clone()).expect("parse GVK")
            ),
            Some(false)
        );

        assert_eq!(
            FilterList(vec![
                FilterType::NamespaceExclude(vec![
                    Namespace::<Exclude>::try_from("other").unwrap()
                ]),
                FilterType::NamespaceExclude(vec![Namespace::<Exclude>::try_from("test").unwrap()]),
            ])
            .filter_object(
                &obj,
                &GroupVersionKind::try_from(pod_tm.clone()).expect("parse GVK")
            ),
            Some(false)
        );

        assert_eq!(
            FilterList(vec![]).filter_object(
                &obj,
                &GroupVersionKind::try_from(pod_tm.clone()).expect("parse GVK")
            ),
            Some(true)
        );
    }

    #[test]
    fn repeated_excludes_are_combined_with_or() {
        let pod_tm: TypeMeta = serde_saphyr::from_str(POD).unwrap();

        let mut app_obj =
            DynamicObject::new("app", &ApiResource::erase::<Pod>(&())).within("default");
        app_obj.metadata.labels = Some(
            [(
                "app.kubernetes.io/name".to_string(),
                "crust-gather".to_string(),
            )]
            .into_iter()
            .collect(),
        );

        let mut name_obj =
            DynamicObject::new("name", &ApiResource::erase::<Pod>(&())).within("default");
        name_obj.metadata.labels = Some(
            [("name".to_string(), "crust-gather".to_string())]
                .into_iter()
                .collect(),
        );

        let filter = FilterList(vec![FilterType::LabelSelectorExclude(vec![
            Selector::<Exclude, Labels>::try_from("app.kubernetes.io/name=crust-gather").unwrap(),
            Selector::<Exclude, Labels>::try_from("name=crust-gather").unwrap(),
        ])]);

        let gvk = GroupVersionKind::try_from(pod_tm.clone()).expect("parse GVK");
        assert_eq!(filter.filter_object(&app_obj, &gvk), Some(false));
        assert_eq!(filter.filter_object(&name_obj, &gvk), Some(false));
    }

    #[test]
    fn test_matches() {
        let list = FilterRegex::try_from("foo|bar").unwrap();
        assert!(list.matches("foo"));
        assert!(list.matches("bar"));
        assert!(!list.matches("baz"));
    }
}
