use async_trait::async_trait;
use kube::{Api, Client};
use kube_core::{ApiResource, DynamicObject, GroupVersionKind, TypeMeta};
use std::{path::PathBuf, sync::Arc};

use crate::filters::filter::Filter;

use super::interface::Collect;

pub struct Collectable {
    pub api: Api<DynamicObject>,
    pub filter: Arc<dyn Filter>,
    resource: ApiResource,
}

/// Constructs a new Collectable instance.
///
/// # Arguments
///
/// * `client` - The Kubernetes client to use for API calls.
/// * `resource` - The Kubernetes discovery resource to collect.
/// * `filter` - The filter to apply when collecting objects of the resource.
impl Collectable {
    pub fn new(client: Client, resource: ApiResource, filter: Arc<dyn Filter>) -> Self {
        Collectable {
            api: Api::all_with(client, &resource),
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
/// Implements the Collect trait for Collectable.
///
/// This allows Collectable to be collected into an archive under the
/// PathBuf destination returned by the path method.
impl Collect for Collectable {
    /// Constructs the path for storing the collected Kubernetes object.
    ///
    /// The path is constructed differently for cluster-scoped vs namespaced objects.
    /// Cluster-scoped objects are stored under `cluster/{kind}/{name}.yaml`.
    /// Namespaced objects are stored under `namespaces/{namespace}/{kind}/{name}.yaml`.
    ///
    /// Example output: `crust-gather/namespaces/default/pod/nginx-deployment-549849849849849849849
    fn path(self: &Self, obj: &DynamicObject) -> PathBuf {
        let obj = obj.clone();
        let (kind, namespace, name) = (
            obj.types.unwrap().kind.to_lowercase(),
            obj.metadata.namespace.unwrap_or_default(),
            obj.metadata.name.unwrap(),
        );

        // Constructs the path for the collected object, cluster-scoped or namespaced.
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

    fn filter(&self, gvk: &GroupVersionKind, obj: &DynamicObject) -> bool {
        self.filter.filter(gvk, obj)
    }
}

#[cfg(test)]
mod test {
    use std::{path::PathBuf, sync::Arc};

    use k8s_openapi::{api::core::v1::Pod, serde_json};
    use kube::Api;
    use kube_core::{params::PostParams, ApiResource, DynamicObject};

    use crate::{
        filters::namespace::NamespaceInclude,
        scanners::{generic::Collectable, interface::Collect},
        tests::kwok,
    };

    #[tokio::test]
    async fn collect_pod() {
        let test_env = kwok::TestEnvBuilder::default()
            .insecure_skip_tls_verify(true)
            .build();

        let filter = NamespaceInclude::try_from("default".to_string()).unwrap();

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
            Arc::new(filter),
        )
        .collect()
        .await
        .expect("Succeed");

        let repr = repr[0].clone();
        assert_eq!(
            repr.path,
            PathBuf::from("crust-gather/namespaces/default/pod/test.yaml")
        );

        let existing_pod: Pod = serde_yaml::from_str(repr.data()).unwrap();
        assert_eq!(existing_pod.spec.unwrap().containers[0].name, "test");
    }
}
