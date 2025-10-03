use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use kube::core::{ApiResource, DynamicObject, ResourceExt};
use kube::Api;
use tracing::instrument;

use crate::gather::{
    config::{Config, Secrets},
    representation::{Representation, TypeMetaGetter},
    writer::Writer,
};

use super::{
    interface::{Collect, CollectError},
    objects::Objects,
};

#[derive(Clone, Debug)]
pub struct Dynamic {
    pub collectable: Objects<DynamicObject>,
}

impl Dynamic {
    pub fn new(config: Config, resource: ApiResource) -> Self {
        Self {
            collectable: Objects::new(config, resource),
        }
    }
}

#[async_trait]
impl Collect<DynamicObject> for Dynamic {
    fn get_secrets(&self) -> Secrets {
        self.collectable.get_secrets()
    }

    fn get_writer(&self) -> Arc<Mutex<Writer>> {
        self.collectable.get_writer()
    }

    fn filter(&self, obj: &DynamicObject) -> Result<bool, CollectError> {
        self.collectable.filter(obj)
    }

    /// Converts the provided DynamicObject into a vector of Representation
    /// with YAML object data and output path for the object.
    #[instrument(skip_all, fields(
        kind = self.resource().to_type_meta().kind,
        apiVersion = self.resource().to_type_meta().api_version,
        name = object.name_any(),
        namespace = object.namespace(),
    ), err)]
    async fn representations(&self, object: &DynamicObject) -> anyhow::Result<Vec<Representation>> {
        tracing::debug!("Collecting representations");

        Ok(vec![Representation::new()
            .with_path(self.path(object))
            .with_data(&serde_yaml::to_string(&DynamicObject {
                types: Some(self.resource().to_type_meta()),
                ..object.clone()
            })?)])
    }

    fn get_api(&self) -> Api<DynamicObject> {
        self.collectable.get_api()
    }

    #[allow(refining_impl_trait)]
    fn resource(&self) -> ApiResource {
        self.collectable.resource()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::time::Duration;

    use k8s_openapi::{api::core::v1::Pod, serde_json};
    use kube::config::{KubeConfigOptions, Kubeconfig};
    use kube::core::{params::PostParams, ApiResource};
    use kube::Client;
    use serde::Deserialize;
    use serial_test::serial;
    use tempdir::TempDir;
    use tokio::time::timeout;
    use tokio_retry::{strategy::FixedInterval, Retry};

    use crate::gather::config::GatherMode;
    use crate::{
        filters::{
            filter::{FilterGroup, FilterList},
            namespace::NamespaceInclude,
        },
        gather::{
            config::Config,
            writer::{Archive, Encoding, Writer},
        },
        scanners::{interface::Collect, objects::Objects},
    };

    use super::*;

    #[derive(Deserialize, Debug)]
    #[allow(dead_code)]
    struct NoDuplicate(
        #[serde(with = "::serde_with::rust::maps_duplicate_key_is_error")]
        pub  HashMap<String, Option<serde_json::Value>>,
    );

    #[tokio::test]
    #[serial]
    async fn collect_dynamic_object() {
        let test_env = envtest::Environment::default().create().expect("cluster");
        let filter = NamespaceInclude::try_from("default".to_string()).unwrap();

        let config: Kubeconfig = test_env.kubeconfig().unwrap();
        let api: Api<DynamicObject> = Api::default_namespaced_with(
            config.clone().try_into().expect("client"),
            &ApiResource::erase::<Pod>(&()),
        );

        timeout(
            Duration::new(10, 0),
            Retry::spawn(FixedInterval::new(Duration::from_secs(1)), || async {
                api.create(
                    &PostParams::default(),
                    &serde_json::from_value(serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {},
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
        .expect("Pod to be created");

        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let file_path = tmp_dir.path().join("crust-gather-test");
        let dynamic = Dynamic {
            collectable: Objects::new(
                Config::new(
                    config.try_into().expect("client"),
                    FilterGroup(vec![FilterList(vec![vec![filter].into()])]),
                    Writer::new(&Archive::new(file_path), &Encoding::Path)
                        .expect("failed to create builder"),
                    Default::default(),
                    GatherMode::Collect,
                    Default::default(),
                    "1m".to_string().try_into().unwrap(),
                    Default::default(),
                    Default::default(),
                ),
                ApiResource::erase::<Pod>(&()),
            ),
        };

        let objects = dynamic.list().await.expect("list");
        let repr = dynamic.representations(&objects[0]).await.expect("Succeed");

        assert!(!repr[0].data().is_empty());
        serde_yaml::from_str::<NoDuplicate>(repr[0].data()).expect("Success");
    }
}
