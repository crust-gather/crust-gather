use std::{
    fmt::{self, Debug},
    sync::{Arc, Mutex},
};

use anyhow::bail;
use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube_core::{
    subresource::LogParams, ApiResource, ErrorResponse, Resource, ResourceExt, TypeMeta,
};

use crate::gather::{
    config::{Config, Secrets},
    writer::{Representation, Writer},
};

use super::{generic::Objects, interface::Collect};

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum LogGroup {
    Current,
    Previous,
}

impl fmt::Display for LogGroup {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LogGroup::Current => write!(formatter, "current.log"),
            LogGroup::Previous => write!(formatter, "previous.log"),
        }
    }
}

impl From<LogGroup> for LogParams {
    fn from(val: LogGroup) -> Self {
        LogParams {
            previous: val == LogGroup::Previous,
            ..Default::default()
        }
    }
}

/// Logs collects container logs for pods. It contains a Collectable for
/// querying pods and a LogGroup to specify whether to collect current or
/// previous logs.
#[derive(Clone)]
pub struct Logs {
    pub collectable: Objects<Pod>,
    pub group: LogGroup,
}

impl Debug for Logs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Logs").field("group", &self.group).finish()
    }
}

impl Logs {
    pub fn new(config: Config, group: LogGroup) -> Self {
        Logs {
            collectable: Objects::new_typed(config, ApiResource::erase::<Pod>(&())),
            group,
        }
    }
}

#[async_trait]
impl Collect<Pod> for Logs {
    fn get_secrets(&self) -> Secrets {
        self.collectable.get_secrets()
    }

    fn get_writer(&self) -> Arc<Mutex<Writer>> {
        self.collectable.get_writer()
    }

    fn filter(&self, obj: &Pod) -> anyhow::Result<bool> {
        self.collectable.filter(obj)
    }

    /// Collects container logs representations.
    async fn representations(&self, pod: &Pod) -> anyhow::Result<Vec<Representation>> {
        log::info!(
            "Collecting {} logs for {}/{}",
            self.group,
            pod.namespace().unwrap_or_default(),
            pod.name_any(),
        );

        let api: Api<Pod> = Api::namespaced(
            self.get_api().into(),
            pod.namespace().unwrap_or_default().as_ref(),
        );
        let mut representations = vec![];

        for container in pod.spec.clone().unwrap().containers {
            let logs = match api
                .logs(
                    pod.name_any().as_str(),
                    &LogParams {
                        container: Some(container.name.clone()),
                        ..self.group.into()
                    },
                )
                .await
            {
                Ok(logs) => logs,
                // If a 400 error occurs, returns the current representations, as that indicates no logs exist.
                Err(kube::Error::Api(ErrorResponse { code: 400, .. })) => {
                    log::info!("No {} logs found for pod {}", self.group, pod.name_any());
                    return Ok(representations);
                }
                Err(e) => {
                    log::error!("Failed to collect logs: {:?}", e);
                    bail!(e)
                }
            };

            let container_path = format!("{}/{}/{}", pod.name_any(), container.name, self.group);
            representations.push(
                Representation::new()
                    .with_path(Collect::path(self, pod).with_file_name(container_path))
                    .with_data(logs.as_str()),
            )
        }

        Ok(representations)
    }

    fn get_api(&self) -> Api<Pod> {
        Collect::get_api(&self.collectable)
    }

    fn get_type_meta(&self) -> TypeMeta {
        TypeMeta {
            api_version: Pod::api_version(&()).into(),
            kind: Pod::kind(&()).into(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use k8s_openapi::{api::core::v1::Pod, serde_json};
    use kube::Api;
    use kube_core::{params::PostParams, ApiResource};
    use serial_test::serial;
    use tempdir::TempDir;
    use tokio::time::timeout;
    use tokio_retry::{strategy::FixedInterval, Retry};

    use crate::{
        filters::{filter::List, namespace::NamespaceInclude},
        gather::{
            config::Config,
            writer::{Archive, Encoding, Writer},
        },
        scanners::{generic::Objects, interface::Collect},
        tests::kwok,
    };

    use super::{LogGroup, Logs};

    #[tokio::test]
    #[serial]
    async fn collect_logs() {
        let test_env = kwok::TestEnvBuilder::default()
            .insecure_skip_tls_verify(true)
            .build();
        let filter = NamespaceInclude::try_from("default".to_string()).unwrap();

        let pod_api: Api<Pod> = Api::default_namespaced(test_env.client().await);

        let pod = timeout(
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
        .expect("Pod to be created");

        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let file_path = tmp_dir.path().join("crust-gather-test");
        let repr = Logs {
            collectable: Objects::new_typed(
                Config::new(
                    test_env.client().await,
                    List(vec![filter.into()]),
                    Writer::new(&Archive::new(file_path), &Encoding::Path)
                        .expect("failed to create builder"),
                    Default::default(),
                    "1m".to_string().try_into().unwrap(),
                ),
                ApiResource::erase::<Pod>(&()),
            ),
            group: LogGroup::Current,
        }
        .representations(&pod)
        .await
        .expect("Succeed");

        let repr = repr[0].clone();
        assert_eq!(repr.data(), "");
    }
}
