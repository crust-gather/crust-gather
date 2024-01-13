use std::fmt::{Debug, Display};

use kube_core::{GroupVersionKind, Resource};
use regex::Regex;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::scanners::interface::ResourceThreadSafe;

use super::{
    group::{GroupExclude, GroupInclude},
    kind::{KindExclude, KindInclude},
    namespace::{NamespaceExclude, NamespaceInclude},
};

pub trait Filter<R>: Sync + Send
where
    R: Resource,
    R: Serialize + DeserializeOwned,
    R: Clone + Sync + Send + Debug,
{
    fn filter_object(&self, _: &R) -> bool;
    fn filter_api(&self, _: &GroupVersionKind) -> bool;

    fn filter(&self, gvk: &GroupVersionKind, obj: &R) -> bool {
        self.filter_api(gvk) && self.filter_object(obj)
    }
}

pub(crate) trait FilterDefinition<R>:
    Filter<R> + TryFrom<String> + Into<FilterType> + Clone
where
    R: Resource,
    R: Serialize + DeserializeOwned,
    R: Clone + Sync + Send + Debug,
{
}

#[derive(Default)]
pub struct List(pub Vec<FilterType>);

#[derive(Clone)]
pub enum FilterType {
    NamespaceExclude(NamespaceExclude),
    NamespaceInclude(NamespaceInclude),
    KindInclude(KindInclude),
    KindExclude(KindExclude),
    GroupInclude(GroupInclude),
    GroupExclude(GroupExclude),
}

impl From<&FilterType> for FilterType {
    fn from(val: &FilterType) -> Self {
        val.clone()
    }
}

impl<R: ResourceThreadSafe> Filter<R> for List {
    fn filter_object(&self, obj: &R) -> bool {
        let no_excludes = self
            .0
            .iter()
            .map(|f| match f {
                FilterType::NamespaceExclude(f) => f.filter_object(obj),
                FilterType::KindExclude(f) => f.filter_object(obj),
                FilterType::GroupExclude(f) => f.filter_object(obj),
                FilterType::NamespaceInclude(_) => true,
                FilterType::KindInclude(_) => true,
                FilterType::GroupInclude(_) => true,
            })
            .all(|allowed| allowed);

        let mut includes = self.0.iter().map(|f| match f {
            FilterType::NamespaceInclude(f) => f.filter_object(obj),
            FilterType::KindInclude(f) => f.filter_object(obj),
            FilterType::GroupInclude(f) => f.filter_object(obj),
            FilterType::NamespaceExclude(_) => false,
            FilterType::KindExclude(_) => false,
            FilterType::GroupExclude(_) => false,
        });

        no_excludes && (includes.len() == 0 || includes.any(|allowed| allowed))
    }

    fn filter_api(&self, gvk: &GroupVersionKind) -> bool {
        let no_excludes = self
            .0
            .iter()
            .map(|f| match f {
                FilterType::NamespaceExclude(f) => Filter::<R>::filter_api(f, gvk),
                FilterType::KindExclude(f) => Filter::<R>::filter_api(f, gvk),
                FilterType::GroupExclude(f) => Filter::<R>::filter_api(f, gvk),
                FilterType::NamespaceInclude(_)
                | FilterType::KindInclude(_)
                | FilterType::GroupInclude(_) => true,
            })
            .all(|allowed| allowed);

        let mut includes = self.0.iter().map(|f| match f {
            FilterType::NamespaceInclude(f) => Filter::<R>::filter_api(f, gvk),
            FilterType::KindInclude(f) => Filter::<R>::filter_api(f, gvk),
            FilterType::GroupInclude(f) => Filter::<R>::filter_api(f, gvk),
            FilterType::NamespaceExclude(_)
            | FilterType::GroupExclude(_)
            | FilterType::KindExclude(_) => false,
        });

        no_excludes && (includes.len() == 0 || includes.any(|allowed| allowed))
    }
}

#[derive(Clone, Deserialize)]
#[serde(try_from = "String")]
pub struct FilterRegex(pub Regex);

impl Default for FilterRegex {
    fn default() -> Self {
        Self(Regex::new("").unwrap())
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
        Ok(FilterRegex(Regex::new(s.as_str())?))
    }
}

#[cfg(test)]
mod tests {

    use k8s_openapi::api::core::v1::Pod;
    use kube_core::{ApiResource, DynamicObject};

    use crate::filters::namespace::{NamespaceExclude, NamespaceInclude};

    use super::*;

    #[test]
    fn filter_all_filters_allow() {
        let obj = DynamicObject::new("", &ApiResource::erase::<Pod>(&())).within("test");

        assert!(List(vec![
            FilterType::NamespaceInclude(NamespaceInclude::try_from("test".to_string()).unwrap()),
            FilterType::NamespaceInclude(NamespaceInclude::try_from("other".to_string()).unwrap()),
            FilterType::NamespaceInclude(NamespaceInclude::try_from("test".to_string()).unwrap())
        ])
        .filter_object(&obj));

        assert!(!List(vec![
            FilterType::NamespaceInclude(NamespaceInclude::try_from("test".to_string()).unwrap()),
            FilterType::NamespaceExclude(NamespaceExclude::try_from("test".to_string()).unwrap()),
        ])
        .filter_object(&obj));

        assert!(!List(vec![
            FilterType::NamespaceExclude(NamespaceExclude::try_from("other".to_string()).unwrap()),
            FilterType::NamespaceExclude(NamespaceExclude::try_from("test".to_string()).unwrap()),
        ])
        .filter_object(&obj));

        assert!(List(vec![]).filter_object(&obj));
    }

    #[test]
    fn test_matches() {
        let list = FilterRegex::try_from("foo|bar".to_string()).unwrap();
        assert!(list.matches("foo"));
        assert!(list.matches("bar"));
        assert!(!list.matches("baz"));
    }
}
