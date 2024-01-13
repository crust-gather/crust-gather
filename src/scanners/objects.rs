use crate::{
    filters::filter::Filter,
    gather::{
        config::{Config, Secrets},
        writer::Writer,
    },
};
use async_trait::async_trait;

use kube::Api;
use kube_core::{ApiResource, GroupVersionKind, Resource, TypeMeta};

use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use super::interface::{Collect, ResourceReq, ResourceThreadSafe};

#[derive(Clone)]
pub struct Objects<R: Resource> {
    pub api: Api<R>,
    pub filter: Arc<dyn Filter<R>>,
    pub resource: ApiResource,
    secrets: Secrets,
    writer: Arc<Mutex<Writer>>,
}

impl<R: ResourceThreadSafe> Debug for Objects<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Object")
            .field("resource", &self.resource.kind)
            .finish()
    }
}

impl<R> Objects<R>
where
    R: Resource<DynamicType = ApiResource>,
    R: ResourceReq,
{
    pub fn new(config: Config, resource: ApiResource) -> Self {
        Objects {
            api: Api::all_with(config.client, &resource),
            filter: config.filter,
            writer: config.writer,
            secrets: config.secrets,
            resource,
        }
    }
}

impl<R> Objects<R>
where
    R: Resource<DynamicType = ()>,
    R: ResourceReq,
{
    pub fn new_typed(config: Config, resource: ApiResource) -> Self {
        Objects {
            api: Api::all(config.client),
            filter: config.filter,
            writer: config.writer,
            secrets: config.secrets,
            resource,
        }
    }
}

#[async_trait]
/// Collects default representations for Kubernetes API objects of any type.
impl<R: ResourceThreadSafe> Collect<R> for Objects<R> {
    fn get_secrets(&self) -> Secrets {
        self.secrets.clone()
    }

    fn get_writer(&self) -> Arc<Mutex<Writer>> {
        self.writer.clone()
    }

    fn filter(&self, obj: &R) -> anyhow::Result<bool> {
        Ok(self
            .filter
            .filter(&GroupVersionKind::try_from(self.get_type_meta())?, obj))
    }

    fn get_api(&self) -> Api<R> {
        log::info!(
            "Collecting {} {} resources",
            self.resource.group,
            self.resource.kind
        );
        self.api.clone()
    }

    fn get_type_meta(&self) -> TypeMeta {
        TypeMeta {
            kind: self.resource.kind.clone(),
            api_version: self.resource.api_version.clone(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use k8s_openapi::{
        api::core::v1::{Namespace, Pod},
        serde_json,
    };
    use kube::Api;
    use kube_core::{params::PostParams, ApiResource, DynamicObject};
    use serial_test::serial;
    use tokio_retry::strategy::FixedInterval;
    use tokio_retry::Retry;

    use crate::{
        filters::{filter::List, namespace::NamespaceInclude},
        gather::{
            config::Config,
            writer::{Archive, Encoding, Writer},
        },
        scanners::{interface::Collect, objects::Objects},
        tests::kwok,
    };
    use tokio::time::timeout;

    use std::time::Duration;

    #[tokio::test]
    #[serial]
    async fn collect_pod() {
        let test_env = kwok::TestEnvBuilder::default()
            .insecure_skip_tls_verify(true)
            .build();

        let filter = NamespaceInclude::try_from("default".to_string()).unwrap();

        let pod_api: Api<Pod> = Api::default_namespaced(test_env.client().await);
        timeout(
            Duration::new(10, 0),
            Retry::spawn(FixedInterval::new(Duration::from_secs(1)), || async {
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
                        .expect("Serialize"),
                    )
                    .await
            }),
        )
        .await
        .expect("Timeout")
        .unwrap();

        let api: Api<DynamicObject> =
            Api::default_namespaced_with(test_env.client().await, &ApiResource::erase::<Pod>(&()));
        let pod = api.get("test").await.unwrap();
        let repr = Objects::new(
            Config::new(
                test_env.client().await,
                List(vec![filter.into()]),
                Writer::new(&Archive::new("crust-gather".into()), &Encoding::Path)
                    .expect("failed to create builder"),
                Default::default(),
                "1m".to_string().try_into().unwrap(),
            ),
            ApiResource::erase::<Pod>(&()),
        )
        .representations(&pod)
        .await
        .expect("Succeed");

        let repr = &repr[0];

        let existing_pod: Pod = serde_yaml::from_str(repr.data()).unwrap();
        assert_eq!(existing_pod.spec.unwrap().containers[0].name, "test");
    }

    #[tokio::test]
    #[serial]
    async fn test_path_cluster_scoped() {
        let test_env = kwok::TestEnvBuilder::default()
            .insecure_skip_tls_verify(true)
            .build();

        let obj = DynamicObject::new("test", &ApiResource::erase::<Namespace>(&()));

        let collectable = Objects::new(
            Config::new(
                test_env.client().await,
                List(vec![]),
                Writer::new(&Archive::new("crust-gather".into()), &Encoding::Path)
                    .expect("failed to create builder"),
                Default::default(),
                "1m".to_string().try_into().unwrap(),
            ),
            ApiResource::erase::<Namespace>(&()),
        );

        let expected = PathBuf::from("cluster/v1/namespace/test.yaml");
        let actual = collectable.path(&obj);

        assert_eq!(expected, actual);
    }

    #[tokio::test]
    #[serial]
    async fn test_path_namespaced() {
        let test_env = kwok::TestEnvBuilder::default()
            .insecure_skip_tls_verify(true)
            .build();
        let obj = DynamicObject::new("test", &ApiResource::erase::<Pod>(&())).within("default");

        let collectable = Objects::new(
            Config::new(
                test_env.client().await,
                List(vec![]),
                Writer::new(&Archive::new("crust-gather".into()), &Encoding::Path)
                    .expect("failed to create builder"),
                Default::default(),
                "1m".to_string().try_into().unwrap(),
            ),
            ApiResource::erase::<Pod>(&()),
        );

        let expected = PathBuf::from("namespaces/default/v1/pod/test.yaml");
        let actual = collectable.path(&obj);

        assert_eq!(expected, actual);
    }
}
