use async_trait::async_trait;
use kube::{Api, Client};
use kube_core::{ApiResource, DynamicObject, TypeMeta};
use std::path::PathBuf;

use crate::filter::Filter;

use super::interface::Collect;

pub struct Collectable {
    pub api: Api<DynamicObject>,
    pub filter: Box<dyn Filter>,
    resource: ApiResource,
}

impl Collectable {
    pub fn new(client: Client, resource: ApiResource, filter: Box<dyn Filter>) -> Self {
        Collectable {
            api: Api::all_with(client, &ApiResource::erase::<DynamicObject>(&resource)),
            filter,
            resource,
        }
    }
}

impl Into<Box<dyn Collect>> for Collectable {
    fn into(self) -> Box<dyn Collect> {
        Box::new(self)
    }
}

#[async_trait]
impl Collect for Collectable {
    fn path(self: &Self, obj: &DynamicObject) -> PathBuf {
        let obj = obj.clone();
        let (kind, namespace, name) = (
            obj.types.unwrap().kind.to_lowercase(),
            obj.metadata.namespace.unwrap_or_default(),
            obj.metadata.name.unwrap(),
        );

        match namespace.as_str() {
            "" => format!("crust-gather/cluster/{kind}/{name}.yaml"),
            _ => format!("crust-gather/namespaces/{namespace}/{kind}/{name}.yaml"),
        }
        .into()
    }

    fn get_type_meta(&self) -> TypeMeta {
        TypeMeta {
            kind: self.resource.kind.clone(),
            api_version: self.resource.api_version.clone(),
        }
    }

    fn get_api(&self) -> Api<DynamicObject> {
        self.api.clone()
    }

    fn filter(&self, obj: &DynamicObject) -> bool {
        self.filter.filter(obj)
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use k8s_openapi::{api::core::v1::Pod, serde_json};
    use kube::Api;
    use kube_core::{params::PostParams, ApiResource, DynamicObject};
    use tar::{Archive, Builder};

    use crate::{
        filter::NamespaceInclude,
        scanners::{generic::Collectable, interface::Collect},
        tests::kwok,
    };

    #[tokio::test]
    async fn collect_pod() {
        let test_env = kwok::TestEnvBuilder::default()
            .insecure_skip_tls_verify(true)
            .build();
        let filter = &NamespaceInclude {
            namespaces: vec!["default".into()],
        };

        let pod_api: Api<DynamicObject> =
            Api::default_namespaced_with(test_env.client().await, &ApiResource::erase::<Pod>(&()));

        pod_api
            .create(
                &PostParams::default(),
                &serde_json::from_value(serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "test",
                    },
                    "spec": {
                        "containers": [{
                          "name": "test",
                          "image": "test",
                        }],
                    }
                }))
                .expect("Serialized pod"),
            )
            .await
            .expect("Pod to be created");

        let repr = Collectable::new(
            test_env.client().await,
            ApiResource::erase::<Pod>(&()),
            filter.into(),
        )
        .collect()
        .await
        .expect("Succeed");

        let b = &mut Builder::new(vec![]);
        let repr = repr[0].clone();
        assert_eq!(
            repr.path,
            PathBuf::from("crust-gather/namespaces/default/pod/test.yaml")
        );
        let existing_pod: Pod = serde_yaml::from_str(repr.data()).unwrap();
        assert_eq!(existing_pod.spec.unwrap().containers[0].name, "test");
        assert!(repr.write(b).is_ok());
        let mut a = Archive::new(b.get_ref().as_slice());
        for e in a.entries().unwrap() {
            let e = e.unwrap();
            let p = e.path().unwrap();
            assert_eq!(
                p.to_str().unwrap(),
                "crust-gather/namespaces/default/pod/test.yaml"
            );
        }
    }
}
