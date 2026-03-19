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
    group::{GroupExclude, GroupInclude},
    kind::{KindExclude, KindInclude},
    name::{NameExclude, NameInclude},
    namespace::{NamespaceExclude, NamespaceInclude},
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
    NamespaceExclude(Vec<NamespaceExclude>),
    NamespaceInclude(Vec<NamespaceInclude>),
    KindInclude(Vec<KindInclude>),
    KindExclude(Vec<KindExclude>),
    GroupInclude(Vec<GroupInclude>),
    GroupExclude(Vec<GroupExclude>),
    NameInclude(Vec<NameInclude>),
    NameExclude(Vec<NameExclude>),
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
        let mut excludes = self
            .0
            .iter()
            .filter_map(|f| match f {
                FilterType::NamespaceExclude(e) => e.filter_object(obj, gvk),
                FilterType::KindExclude(e) => e.filter_object(obj, gvk),
                FilterType::GroupExclude(e) => e.filter_object(obj, gvk),
                FilterType::NameExclude(e) => e.filter_object(obj, gvk),
                FilterType::NamespaceInclude(_) => None,
                FilterType::KindInclude(_) => None,
                FilterType::GroupInclude(_) => None,
                FilterType::NameInclude(_) => None,
            })
            .peekable();

        if excludes.peek().is_some() && excludes.filter(|&e| !e).any(|allowed| !allowed) {
            return Some(false);
        }

        let mut includes = self.0.iter().filter_map(|f| match f {
            FilterType::NamespaceExclude(_) => None,
            FilterType::KindExclude(_) => None,
            FilterType::GroupExclude(_) => None,
            FilterType::NameExclude(_) => None,
            FilterType::NamespaceInclude(i) => i.filter_object(obj, gvk),
            FilterType::KindInclude(i) => i.filter_object(obj, gvk),
            FilterType::GroupInclude(i) => i.filter_object(obj, gvk),
            FilterType::NameInclude(i) => i.filter_object(obj, gvk),
        });

        Some(includes.all(|allowed| allowed))
    }
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

impl TryFrom<String> for FilterRegex {
    type Error = anyhow::Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Ok(Self(Regex::new(s.as_str())?))
    }
}

#[cfg(test)]
mod tests {

    use k8s_openapi::api::core::v1::Pod;
    use kube::core::{ApiResource, DynamicObject, TypeMeta};

    use crate::filters::namespace::{NamespaceExclude, NamespaceInclude};

    use super::*;

    static POD: &str = r"---
    apiVersion: v1
    kind: Pod";

    #[test]
    fn filter_all_filters_allow() {
        let obj = DynamicObject::new("", &ApiResource::erase::<Pod>(&())).within("test");

        let pod_tm: TypeMeta = serde_yaml::from_str(POD).unwrap();
        assert_eq!(
            FilterList(vec![FilterType::NamespaceInclude(vec![
                NamespaceInclude::try_from("test".to_string()).unwrap(),
                NamespaceInclude::try_from("other".to_string()).unwrap(),
                NamespaceInclude::try_from("test".to_string()).unwrap(),
            ]),])
            .filter_object(
                &obj,
                &GroupVersionKind::try_from(pod_tm.clone()).expect("parse GVK")
            ),
            Some(true)
        );

        assert_eq!(
            FilterList(vec![
                FilterType::NamespaceInclude(vec![
                    NamespaceInclude::try_from("test".to_string()).unwrap()
                ]),
                FilterType::NamespaceExclude(vec![
                    NamespaceExclude::try_from("test".to_string()).unwrap()
                ]),
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
                    NamespaceExclude::try_from("other".to_string()).unwrap()
                ]),
                FilterType::NamespaceExclude(vec![
                    NamespaceExclude::try_from("test".to_string()).unwrap()
                ]),
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
    fn test_matches() {
        let list = FilterRegex::try_from("foo|bar".to_string()).unwrap();
        assert!(list.matches("foo"));
        assert!(list.matches("bar"));
        assert!(!list.matches("baz"));
    }
}
