use crate::{
    filters::filter::Filter,
    gather::{
        config::{Config, Secrets},
        representation::TypeMetaGetter,
        writer::Writer,
    },
};
use async_trait::async_trait;

use kube::core::{ApiResource, GroupVersionKind, Resource};
use kube::Api;
use tracing::instrument;

use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use super::interface::{Collect, CollectError, ResourceReq, ResourceThreadSafe};

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
            .finish_non_exhaustive()
    }
}

impl<R> Objects<R>
where
    R: Resource<DynamicType = ApiResource> + ResourceReq,
{
    pub fn new(config: Config, resource: ApiResource) -> Self {
        Self {
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
    R: ResourceThreadSafe,
    R::DynamicType: Default,
{
    pub fn new_typed(config: Config) -> Self {
        Self {
            api: Api::all(config.client),
            filter: config.filter,
            writer: config.writer,
            secrets: config.secrets,
            resource: ApiResource::erase::<R>(&Default::default()),
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

    #[instrument(skip_all, fields(kind = self.resource().to_type_meta().kind, apiVersion = self.resource().to_type_meta().api_version), err)]
    fn filter(&self, obj: &R) -> Result<bool, CollectError> {
        Ok(self.filter.filter(
            &GroupVersionKind::try_from(self.resource().to_type_meta())
                .map_err(CollectError::GroupVersion)?,
            obj,
        ))
    }

    #[instrument(skip_all, fields(
        kind = self.resource().to_type_meta().kind,
        apiVersion = self.resource().to_type_meta().api_version,
    ))]
    fn get_api(&self) -> Api<R> {
        tracing::debug!("Collecting resources");
        self.api.clone()
    }

    #[allow(refining_impl_trait)]
    fn resource(&self) -> ApiResource {
        self.resource.clone()
    }
}

#[cfg(test)]
mod test {

    use k8s_openapi::{
        api::core::v1::{Namespace, Pod},
        serde_json,
    };
    use kube::{config::Kubeconfig, Api, Client};
    use kube::{
        config::KubeConfigOptions,
        core::{params::PostParams, ApiResource, DynamicObject},
    };
    use serial_test::serial;
    use tokio_retry::strategy::FixedInterval;
    use tokio_retry::Retry;

    use crate::{
        filters::{
            filter::{FilterGroup, FilterList},
            namespace::NamespaceInclude,
        },
        gather::{
            config::{Config, GatherMode},
            representation::ArchivePath,
            writer::{Archive, Encoding, Writer},
        },
        scanners::{interface::Collect, objects::Objects},
    };
    use tokio::time::timeout;

    use std::time::Duration;

    #[tokio::test]
    #[serial]
    async fn collect_pod() {
        let test_env = envtest::Environment::default().create().expect("cluster");
        let config: Kubeconfig = test_env.kubeconfig().expect("kubeconfig");
        let client: Client = config.try_into().expect("client");

        let filter = NamespaceInclude::try_from("default".to_string()).unwrap();

        let pod_api: Api<Pod> = Api::default_namespaced(client.clone());
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

        let api: Api<DynamicObject> = Api::default_namespaced_with(
            client.clone(),
            &ApiResource::erase::<Pod>(&()),
        );
        let pod = api.get("test").await.unwrap();
        let repr = Objects::new(
            Config::new(
                client,
                FilterGroup(vec![FilterList(vec![vec![filter].into()])]),
                Writer::new(&Archive::new("crust-gather".into()), &Encoding::Path)
                    .expect("failed to create builder"),
                Default::default(),
                GatherMode::Collect,
                Default::default(),
                "1m".to_string().try_into().unwrap(),
                Default::default(),
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
        let test_env = envtest::Environment::default().create().expect("cluster");
        let config: Kubeconfig = test_env.kubeconfig().expect("kubeconfig");
        let client = config.try_into().expect("client");

        let obj = DynamicObject::new("test", &ApiResource::erase::<Namespace>(&()));

        let collectable = Objects::new(
            Config::new(
                client,
                FilterGroup(vec![FilterList(vec![])]),
                Writer::new(&Archive::new("crust-gather".into()), &Encoding::Path)
                    .expect("failed to create builder"),
                Default::default(),
                GatherMode::Collect,
                Default::default(),
                "1m".to_string().try_into().unwrap(),
                Default::default(),
            ),
            ApiResource::erase::<Namespace>(&()),
        );

        let expected = ArchivePath::Cluster("cluster/v1/namespace/test.yaml".into());
        let actual = collectable.path(&obj);

        assert_eq!(expected, actual);
    }

    #[tokio::test]
    #[serial]
    async fn test_path_namespaced() {
        let test_env = envtest::Environment::default().create().expect("cluster");
        let config: Kubeconfig = test_env.kubeconfig().expect("kubeconfig");
        let client = config.try_into().expect("client");

        let obj = DynamicObject::new("test", &ApiResource::erase::<Pod>(&())).within("default");

        let collectable = Objects::new(
            Config::new(
                client,
                FilterGroup(vec![FilterList(vec![])]),
                Writer::new(&Archive::new("crust-gather".into()), &Encoding::Path)
                    .expect("failed to create builder"),
                Default::default(),
                GatherMode::Collect,
                Default::default(),
                "1m".to_string().try_into().unwrap(),
                Default::default(),
            ),
            ApiResource::erase::<Pod>(&()),
        );

        let expected = ArchivePath::Namespaced("namespaces/default/v1/pod/test.yaml".into());
        let actual = collectable.path(&obj);

        assert_eq!(expected, actual);
    }
}
