use std::fmt::Display;

use anyhow::anyhow;
use serde::Deserialize;

#[derive(Clone, Default, Deserialize, Debug)]
pub struct UserLog {
    pub name: String,
    pub command: String,
}

impl TryFrom<String> for UserLog {
    type Error = anyhow::Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        let (name, command) = s.split_once(':').unwrap_or_default();
        if name.is_empty() && command.is_empty() {
            Err(anyhow!("Custom log should contain : delimiter"))?;
        }

        Ok(Self {
            name: name.into(),
            command: command.into(),
        })
    }
}

/// Formats the group and kind for display.
/// Example: "<File: ^$, Command: Pod>
impl Display for UserLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<File: {}, Command: {}>", self.name, self.command)
    }
}

// #[cfg(test)]
// mod tests {

//     use k8s_openapi::{api::core::v1::Pod, Resource};
//     use kube::core::{ApiResource, DynamicObject, TypeMeta};

//     use super::*;

//     static POD: &str = r"---
//     apiVersion: v1
//     kind: Pod";

//     static DEPLOY: &str = r"---
//     apiVersion: apps/v1
//     kind: Deployment";

//     static RS: &str = r"---
//     apiVersion: apps/v1
//     kind: ReplicaSet";

//     #[test]
//     fn test_group_include_filter() {
//         let filter = GroupInclude::try_from("apps/(Deployment|ReplicaSet)".to_string()).unwrap();

//         let pod_tm: TypeMeta = serde_yaml::from_str(POD).unwrap();
//         let deploy_tm: TypeMeta = serde_yaml::from_str(DEPLOY).unwrap();
//         let replicaset_tm: TypeMeta = serde_yaml::from_str(RS).unwrap();
//         let obj: DynamicObject =
//             DynamicObject::new("test", &ApiResource::erase::<Pod>(&())).within("default");

//         assert_eq!(
//             <GroupInclude as Filter<DynamicObject>>::filter_object(
//                 &filter,
//                 &obj,
//                 &GroupVersionKind::try_from(pod_tm).expect("parse GVK")
//             ),
//             Some(false)
//         );
//         assert_eq!(
//             <GroupInclude as Filter<DynamicObject>>::filter_object(
//                 &filter,
//                 &obj,
//                 &GroupVersionKind::try_from(deploy_tm).expect("parse GVK")
//             ),
//             Some(true)
//         );
//         assert_eq!(
//             <GroupInclude as Filter<DynamicObject>>::filter_object(
//                 &filter,
//                 &obj,
//                 &GroupVersionKind::try_from(replicaset_tm).expect("parse GVK")
//             ),
//             Some(true)
//         );
//     }

//     #[test]
//     fn test_from_string_list() {
//         let filter = GroupInclude::try_from("/Pod".to_string()).unwrap();
//         assert_eq!(filter.group.to_string(), "<Group: ^$, Kind: Pod>");

//         let filter = GroupInclude::try_from("apps".to_string()).unwrap();
//         assert_eq!(filter.group.to_string(), "<Group: apps, Kind: .*>");
//     }

//     #[test]
//     fn test_group_exclude_filter() {
//         let other = r"---
//         apiVersion: test/v1
//         kind: OtherType";

//         let other_tm: TypeMeta = serde_yaml::from_str(other).unwrap();

//         let pod_tm: TypeMeta = serde_yaml::from_str(POD).unwrap();
//         let obj: DynamicObject =
//             DynamicObject::new("test", &ApiResource::erase::<Pod>(&())).within("default");

//         let exclude = GroupExclude::try_from(Pod::GROUP.to_string()).unwrap();
//         assert_eq!(
//             <GroupExclude as Filter<DynamicObject>>::filter_object(
//                 &exclude,
//                 &obj,
//                 &GroupVersionKind::try_from(pod_tm).expect("parse GVK")
//             ),
//             Some(false)
//         );
//         assert_eq!(
//             <GroupExclude as Filter<DynamicObject>>::filter_object(
//                 &exclude,
//                 &obj,
//                 &GroupVersionKind::try_from(other_tm).expect("parse GVK")
//             ),
//             Some(true)
//         );
//     }

//     #[test]
//     fn test_try_from_include() {
//         let filter = GroupInclude::try_from("apps/Deployment|ReplicaSet".to_string()).unwrap();
//         assert_eq!(
//             filter.group.to_string(),
//             "<Group: apps, Kind: Deployment|ReplicaSet>"
//         );

//         let filter = GroupInclude::try_from(String::new()).unwrap();
//         assert_eq!(filter.group.to_string(), "<Group: ^$, Kind: .*>");
//     }
// }
