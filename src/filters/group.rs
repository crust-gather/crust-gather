use std::fmt::Display;

use kube_core::GroupVersionKind;
use serde::Deserialize;

use crate::scanners::interface::ResourceThreadSafe;

use super::filter::{Filter, FilterDefinition, FilterRegex, FilterType};

#[derive(Clone, Default, Deserialize)]
#[serde(try_from = "String")]
pub struct Group {
    group: FilterRegex,
    kind: FilterRegex,
}

impl Group {
    pub fn matches(&self, gvk: &GroupVersionKind) -> bool {
        self.group.matches(&gvk.group) && self.kind.matches(&gvk.kind)
    }
}

#[derive(Clone, Default, Deserialize)]
#[serde(try_from = "String")]
pub struct GroupInclude {
    group: Group,
}

impl<R: ResourceThreadSafe> Filter<R> for GroupInclude {
    fn filter_object(&self, _: &R) -> bool {
        true
    }

    fn filter_api(&self, gvk: &GroupVersionKind) -> bool {
        let accepted = self.group.matches(gvk);

        if !accepted {
            log::debug!(
                "GroupInclude filter excluded {}/{} as it is not present in the group list {}",
                gvk.group,
                gvk.kind,
                self.group,
            );
        }

        accepted
    }
}

impl TryFrom<String> for Group {
    type Error = anyhow::Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        let gksplit = s.splitn(2, '/').collect::<Vec<_>>();
        let (groups, kinds) = match *gksplit.as_slice() {
            ["", k] => ("^$", k), // empty group case
            [g, k] => (g, k),     // standard case
            [""] => ("^$", ".*"), // empty group and no kind case
            [g] => (g, ".*"),     // no kind case
            _ => unreachable!(),
        };

        Ok(Self {
            group: groups.to_string().try_into()?,
            kind: kinds.to_string().try_into()?,
        })
    }
}

/// Formats the group and kind for display.
/// Example: "<Group: ^$, Kind: Pod>
impl Display for Group {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<Group: {}, Kind: {}>", self.group, self.kind)
    }
}

impl<R: ResourceThreadSafe> FilterDefinition<R> for GroupInclude {}

impl TryFrom<String> for GroupInclude {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(Self {
            group: value.try_into()?,
        })
    }
}

impl From<GroupInclude> for FilterType {
    fn from(val: GroupInclude) -> Self {
        Self::GroupInclude(val)
    }
}

#[derive(Clone, Default, Deserialize)]
#[serde(try_from = "String")]
pub struct GroupExclude {
    group: Group,
}

impl<R: ResourceThreadSafe> Filter<R> for GroupExclude {
    fn filter_api(&self, gvk: &GroupVersionKind) -> bool {
        let accepted = !self.group.matches(gvk);

        if !accepted {
            log::debug!(
                "GroupExclude filter excluded {}/{} as it is present in the exclude group list {}",
                gvk.group,
                gvk.kind,
                self.group,
            );
        }

        accepted
    }

    fn filter_object(&self, _: &R) -> bool {
        true
    }
}

impl<R: ResourceThreadSafe> FilterDefinition<R> for GroupExclude {}

impl TryFrom<String> for GroupExclude {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(Self {
            group: value.try_into()?,
        })
    }
}

impl From<GroupExclude> for FilterType {
    fn from(val: GroupExclude) -> Self {
        Self::GroupExclude(val)
    }
}

#[cfg(test)]
mod tests {

    use k8s_openapi::{api::core::v1::Pod, Resource};
    use kube_core::{DynamicObject, TypeMeta};

    use super::*;

    #[test]
    fn test_group_include_filter() {
        let pod = r"---
        apiVersion: v1
        kind: Pod";

        let deploy = r"---
        apiVersion: apps/v1
        kind: Deployment";

        let rs = r"---
        apiVersion: apps/v1
        kind: ReplicaSet";

        let pod_tm: TypeMeta = serde_yaml::from_str(pod).unwrap();
        let deploy_tm: TypeMeta = serde_yaml::from_str(deploy).unwrap();
        let replicaset_tm: TypeMeta = serde_yaml::from_str(rs).unwrap();

        let filter = GroupInclude::try_from("apps/(Deployment|ReplicaSet)".to_string()).unwrap();

        assert!(!<GroupInclude as Filter<DynamicObject>>::filter_api(
            &filter,
            &GroupVersionKind::try_from(pod_tm).expect("parse GVK")
        ));
        assert!(<GroupInclude as Filter<DynamicObject>>::filter_api(
            &filter,
            &GroupVersionKind::try_from(deploy_tm).expect("parse GVK")
        ));
        assert!(<GroupInclude as Filter<DynamicObject>>::filter_api(
            &filter,
            &GroupVersionKind::try_from(replicaset_tm).expect("parse GVK")
        ));
    }

    #[test]
    fn test_from_string_list() {
        let filter = GroupInclude::try_from("/Pod".to_string()).unwrap();
        assert_eq!(filter.group.to_string(), "<Group: ^$, Kind: Pod>");

        let filter = GroupInclude::try_from("apps".to_string()).unwrap();
        assert_eq!(filter.group.to_string(), "<Group: apps, Kind: .*>");
    }

    #[test]
    fn test_group_exclude_filter() {
        let pod = r"---
        apiVersion: v1
        kind: Pod";

        let deploy = r"---
        apiVersion: test/v1
        kind: OtherType";

        let pod_tm: TypeMeta = serde_yaml::from_str(pod).unwrap();
        let deploy_tm: TypeMeta = serde_yaml::from_str(deploy).unwrap();

        let exclude = GroupExclude::try_from(Pod::GROUP.to_string()).unwrap();
        assert!(!<GroupExclude as Filter<DynamicObject>>::filter_api(
            &exclude,
            &GroupVersionKind::try_from(pod_tm).expect("parse GVK")
        ));
        assert!(<GroupExclude as Filter<DynamicObject>>::filter_api(
            &exclude,
            &GroupVersionKind::try_from(deploy_tm).expect("parse GVK")
        ));
    }

    #[test]
    fn test_try_from_include() {
        let filter = GroupInclude::try_from("apps/Deployment|ReplicaSet".to_string()).unwrap();
        assert_eq!(
            filter.group.to_string(),
            "<Group: apps, Kind: Deployment|ReplicaSet>"
        );

        let filter = GroupInclude::try_from(String::new()).unwrap();
        assert_eq!(filter.group.to_string(), "<Group: ^$, Kind: .*>");
    }
}
