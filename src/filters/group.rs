use std::fmt::Display;

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
pub struct GroupRegex {
    group: FilterRegex,
    kind: FilterRegex,
}

impl GroupRegex {
    pub fn matches(&self, gvk: &GroupVersionKind) -> bool {
        self.group.matches(&gvk.group) && self.kind.matches(&gvk.kind)
    }
}

#[derive(Clone, Default, Serialize, Deserialize, Debug, schemars::JsonSchema)]
#[serde(try_from = "String")]
pub struct Group<M: Match> {
    group: GroupRegex,

    #[serde(skip)]
    #[schemars(skip)]
    matcher: std::marker::PhantomData<M>,
}

impl<R: ResourceThreadSafe, M: Match> Filter<R> for Group<M>
where
    M: Match + Send + Sync,
{
    #[instrument(skip_all, fields(group = gvk.group, kind = gvk.kind, group_list = self.group.to_string()))]
    fn filter_object(&self, _: &R, gvk: &GroupVersionKind) -> Option<bool> {
        let accepted = M::matches(self.group.matches(gvk));

        if !accepted {
            tracing::debug!("Group filter excluded object as it is not in the matching group list",);
        }

        Some(accepted)
    }
}

impl TryFrom<&str> for GroupRegex {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let gksplit = s.splitn(2, '/').collect::<Vec<_>>();
        let (groups, kinds) = match *gksplit.as_slice() {
            ["", k] => ("^$", k), // empty group case
            [g, k] => (g, k),     // standard case
            [""] => ("^$", ".*"), // empty group and no kind case
            [g] => (g, ".*"),     // no kind case
            _ => unreachable!(),
        };

        Ok(Self {
            group: groups.try_into()?,
            kind: kinds.try_into()?,
        })
    }
}

impl TryFrom<String> for GroupRegex {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

/// Formats the group and kind for display.
/// Example: "<Group: ^$, Kind: Pod>
impl Display for GroupRegex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<Group: {}, Kind: {}>", self.group, self.kind)
    }
}

impl<M: Match> TryFrom<&str> for Group<M> {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self {
            group: value.try_into()?,
            matcher: std::marker::PhantomData,
        })
    }
}

impl<M: Match> TryFrom<String> for Group<M> {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl From<Vec<Group<Include>>> for FilterType {
    fn from(val: Vec<Group<Include>>) -> Self {
        Self::GroupInclude(val)
    }
}

impl From<Vec<Group<Exclude>>> for FilterType {
    fn from(val: Vec<Group<Exclude>>) -> Self {
        Self::GroupExclude(val)
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
    apiVersion: apps/v1
    kind: Deployment";

    static RS: &str = r"---
    apiVersion: apps/v1
    kind: ReplicaSet";

    #[test]
    fn test_group_include_filter() {
        let filter = Group::<Include>::try_from("apps/(Deployment|ReplicaSet)").unwrap();

        let pod_tm: TypeMeta = serde_yaml::from_str(POD).unwrap();
        let deploy_tm: TypeMeta = serde_yaml::from_str(DEPLOY).unwrap();
        let replicaset_tm: TypeMeta = serde_yaml::from_str(RS).unwrap();
        let obj: DynamicObject =
            DynamicObject::new("test", &ApiResource::erase::<Pod>(&())).within("default");

        assert_eq!(
            <Group<Include> as Filter<DynamicObject>>::filter_object(
                &filter,
                &obj,
                &GroupVersionKind::try_from(pod_tm).expect("parse GVK")
            ),
            Some(false)
        );
        assert_eq!(
            <Group<Include> as Filter<DynamicObject>>::filter_object(
                &filter,
                &obj,
                &GroupVersionKind::try_from(deploy_tm).expect("parse GVK")
            ),
            Some(true)
        );
        assert_eq!(
            <Group<Include> as Filter<DynamicObject>>::filter_object(
                &filter,
                &obj,
                &GroupVersionKind::try_from(replicaset_tm).expect("parse GVK")
            ),
            Some(true)
        );
    }

    #[test]
    fn test_from_string_list() {
        let filter = Group::<Include>::try_from("/Pod").unwrap();
        assert_eq!(filter.group.to_string(), "<Group: ^$, Kind: Pod>");

        let filter = Group::<Include>::try_from("apps").unwrap();
        assert_eq!(filter.group.to_string(), "<Group: apps, Kind: .*>");
    }

    #[test]
    fn test_group_exclude_filter() {
        let other = r"---
        apiVersion: test/v1
        kind: OtherType";

        let other_tm: TypeMeta = serde_yaml::from_str(other).unwrap();

        let pod_tm: TypeMeta = serde_yaml::from_str(POD).unwrap();
        let obj: DynamicObject =
            DynamicObject::new("test", &ApiResource::erase::<Pod>(&())).within("default");

        let exclude = Group::<Exclude>::try_from(Pod::GROUP).unwrap();
        assert_eq!(
            <Group<Exclude> as Filter<DynamicObject>>::filter_object(
                &exclude,
                &obj,
                &GroupVersionKind::try_from(pod_tm).expect("parse GVK")
            ),
            Some(false)
        );
        assert_eq!(
            <Group<Exclude> as Filter<DynamicObject>>::filter_object(
                &exclude,
                &obj,
                &GroupVersionKind::try_from(other_tm).expect("parse GVK")
            ),
            Some(true)
        );
    }

    #[test]
    fn test_try_from_include() {
        let filter = Group::<Include>::try_from("apps/Deployment|ReplicaSet").unwrap();
        assert_eq!(
            filter.group.to_string(),
            "<Group: apps, Kind: Deployment|ReplicaSet>"
        );

        let filter = Group::<Include>::try_from("").unwrap();
        assert_eq!(filter.group.to_string(), "<Group: ^$, Kind: .*>");
    }
}
