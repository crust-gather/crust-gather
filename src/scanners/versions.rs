use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::{Api, Resource};
use kube_core::{ApiResource, TypeMeta};
use serde::Serialize;

use crate::gather::{
    config::{Config, Secrets},
    representation::{ArchivePath, Representation},
    writer::Writer,
};

use super::{interface::Collect, objects::Objects};

#[derive(Clone, Debug, Serialize)]
struct Version {
    name: String,
    namespace: String,
    container: String,
    version: String,
}

#[derive(Clone, Debug)]
pub struct Versions {
    pub collectable: Objects<Pod>,
}

impl Versions {
    pub fn new(config: Config) -> Self {
        Self {
            collectable: Objects::new_typed(config, ApiResource::erase::<Pod>(&())),
        }
    }
}

#[async_trait]
impl Collect<Pod> for Versions {
    fn get_secrets(&self) -> Secrets {
        self.collectable.get_secrets()
    }

    fn get_writer(&self) -> Arc<Mutex<Writer>> {
        self.collectable.get_writer()
    }

    fn filter(&self, _: &Pod) -> anyhow::Result<bool> {
        Ok(true)
    }

    async fn collect(&self) -> anyhow::Result<()> {
        let data = self
            .list()
            .await?
            .iter()
            .filter_map(|pod| pod.spec.clone().map(|s| (pod.meta(), s)))
            .flat_map(|(meta, spec)| {
                spec.containers.into_iter().map(|container| Version {
                    name: meta.name.clone().unwrap_or_default(),
                    namespace: meta.namespace.clone().unwrap_or_default(),
                    container: container.name.clone(),
                    version: container.image.clone().unwrap_or_default(),
                })
            })
            .collect::<Vec<_>>();

        self.get_writer().lock().unwrap().store(
            &Representation::new()
                .with_path(ArchivePath::Custom("app-versions.yaml".into()))
                .with_data(serde_yaml::to_string(&data)?.as_str()),
        )
    }

    fn get_api(&self) -> Api<Pod> {
        self.collectable.get_api()
    }

    fn get_type_meta(&self) -> TypeMeta {
        self.collectable.get_type_meta()
    }
}

#[cfg(test)]
mod tests {

    use std::{fs::File, io::Write, time::Duration};

    use k8s_openapi::{api::core::v1::Pod, serde_json};
    use kube::{api::PostParams, Api};
    use serial_test::serial;
    use tempdir::TempDir;
    use tokio::{fs, time::timeout};
    use tokio_retry::{strategy::FixedInterval, Retry};

    use crate::{cli::GatherCommands, tests::kwok};

    #[tokio::test]
    #[serial]
    async fn test_collect_versions() {
        let test_env = kwok::TestEnvBuilder::default()
            .insecure_skip_tls_verify(true)
            .build();

        let kubeconfig = serde_yaml::to_string(&test_env.kubeconfig()).unwrap();
        fs::write(test_env.kubeconfig_path(), kubeconfig)
            .await
            .unwrap();

        let tmp_dir = TempDir::new("collect").expect("failed to create temp dir");

        let mut valid = File::create(tmp_dir.path().join("valid.yaml")).unwrap();
        let valid_config = format!(
            r"
            settings:
              file: {}
              kubeconfig: {}
            ",
            tmp_dir.path().join("collect").to_str().unwrap(),
            test_env.kubeconfig_path().to_str().unwrap(),
        );
        valid.write_all(valid_config.as_bytes()).unwrap();

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

        let commands = GatherCommands::try_from(
            tmp_dir
                .path()
                .join("valid.yaml")
                .to_str()
                .unwrap()
                .to_string(),
        )
        .unwrap();

        let config = commands.load().await.unwrap();

        config.collect().await.unwrap();
        assert!(tmp_dir
            .path()
            .join("collect")
            .join("app-versions.yaml")
            .is_file());
        assert_eq!(
            fs::read_to_string(tmp_dir.path().join("collect").join("app-versions.yaml"))
                .await
                .unwrap(),
            r"- name: test
  namespace: default
  container: test
  version: test
"
            .to_string()
        )
    }
}
