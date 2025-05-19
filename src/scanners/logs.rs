use std::{
    fmt::{self, Debug, Display},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::{
    api::TypeMeta,
    core::{subresource::LogParams, ApiResource, ErrorResponse, ResourceExt},
};
use thiserror::Error;
use tracing::instrument;

use crate::gather::{
    config::{Config, Secrets},
    representation::{ArchivePath, Container, LogGroup, Representation},
    writer::Writer,
};

use super::{
    interface::{Collect, CollectError},
    objects::Objects,
};

/// Failure to collect logs
#[derive(Debug, Error)]
#[error("Failed to collect logs: {0:?}")]
pub struct LogsError(kube::Error);

#[derive(Clone, PartialEq)]
pub enum LogSelection {
    Current,
    Previous,
}

impl Display for LogSelection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogSelection::Current => write!(f, "current.log"),
            LogSelection::Previous => write!(f, "previous.log"),
        }
    }
}

impl From<LogSelection> for LogParams {
    fn from(val: LogSelection) -> Self {
        Self {
            previous: val == LogSelection::Previous,
            ..Default::default()
        }
    }
}

/// Logs collects container logs for pods. It contains a Collectable for
/// querying pods and a `LogGroup` to specify whether to collect current or
/// previous logs.
#[derive(Clone)]
pub struct Logs {
    pub collectable: Objects<Pod>,
    pub group: LogSelection,
}

impl Debug for Logs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.group.fmt(f)
    }
}

impl Logs {
    pub fn new(config: Config, group: LogSelection) -> Self {
        Self {
            collectable: Objects::new_typed(config),
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

    fn filter(&self, obj: &Pod) -> Result<bool, CollectError> {
        self.collectable.filter(obj)
    }

    /// Collects container logs representations.
    #[instrument(skip_all, fields(name = pod.name_any(), namespace = pod.namespace(), group=self.group.to_string()), err)]
    async fn representations(&self, pod: &Pod) -> anyhow::Result<Vec<Representation>> {
        tracing::debug!("Collecting logs");

        let mut representations = vec![];

        for container in pod.spec.clone().unwrap().containers {
            let logs = match Api::<Pod>::namespaced(
                self.get_api().into(),
                pod.namespace().unwrap_or_default().as_ref(),
            )
            .logs(
                pod.name_any().as_str(),
                &LogParams {
                    container: Some(container.name.clone()),
                    since_time: Some(Default::default()),
                    ..self.group.clone().into()
                },
            )
            .await
            {
                Ok(logs) => Ok(logs),
                // If a 400 error occurs, returns the current representations, as that indicates no logs exist.
                Err(kube::Error::Api(ErrorResponse { code: 400, .. })) => {
                    tracing::info!("No logs found");
                    return Ok(representations);
                }
                e => e,
            }
            .map_err(LogsError)?;

            representations.push(
                Representation::new()
                    .with_path(ArchivePath::logs_path(
                        pod,
                        TypeMeta::resource::<Pod>(),
                        match self.group {
                            LogSelection::Current => LogGroup::Current(Container(container.name)),
                            LogSelection::Previous => LogGroup::Previous(Container(container.name)),
                        },
                    ))
                    .with_data(logs.as_str()),
            );
        }

        Ok(representations)
    }

    fn get_api(&self) -> Api<Pod> {
        self.collectable.get_api()
    }

    #[allow(refining_impl_trait)]
    fn resource(&self) -> ApiResource {
        self.collectable.resource()
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use k8s_openapi::{api::core::v1::Pod, serde_json};
    use kube::core::params::PostParams;
    use kube::Api;
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
        scanners::{interface::Collect, logs::LogSelection, objects::Objects},
        tests::kwok,
    };

    use super::Logs;

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
            collectable: Objects::new_typed(Config::new(
                test_env.client().await,
                FilterGroup(vec![FilterList(vec![vec![filter].into()])]),
                Writer::new(&Archive::new(file_path), &Encoding::Path)
                    .expect("failed to create builder"),
                Default::default(),
                GatherMode::Collect,
                Default::default(),
                "1m".to_string().try_into().unwrap(),
            )),
            group: LogSelection::Current,
        }
        .representations(&pod)
        .await
        .expect("Succeed");

        let repr = repr[0].clone();
        assert_eq!(repr.data(), "");
    }
}
