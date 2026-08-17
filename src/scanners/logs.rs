use std::{
    fmt::{self, Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use k8s_openapi::{api::core::v1::Pod, jiff::Timestamp};
use kube::Api;
use kube::{
    api::TypeMeta,
    core::{ApiResource, ResourceExt, subresource::LogParams},
};
use thiserror::Error;
use tokio::sync::Mutex;
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

#[derive(Clone, PartialEq, Eq)]
pub enum LogSelection {
    Current,
    Previous,
}

impl Display for LogSelection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Current => write!(f, "current.log"),
            Self::Previous => write!(f, "previous.log"),
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
    pub skip_logs_collection: bool,
}

impl Debug for Logs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.group.fmt(f)
    }
}

impl Logs {
    #[must_use]
    pub fn new(config: Config, group: LogSelection) -> Self {
        Self {
            skip_logs_collection: config.skip_logs_collection,
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
        if self.skip_logs_collection {
            return Ok(false);
        }
        self.collectable.filter(obj)
    }

    fn extension(&self) -> &str {
        &self.collectable.extension
    }

    /// Collects container logs representations.
    #[instrument(skip_all, fields(name = pod.name_any(), namespace = pod.namespace(), group=self.group.to_string()), err)]
    async fn representations(&self, pod: &Pod) -> anyhow::Result<Vec<Representation>> {
        tracing::debug!("Collecting logs");

        let mut representations = vec![];

        let Some(spec) = pod.spec.as_ref() else {
            return Ok(representations);
        };

        for container in &spec.containers {
            let container_name = container.name.clone();
            let logs = match Api::<Pod>::namespaced(
                self.get_api().into(),
                pod.namespace().unwrap_or_default().as_ref(),
            )
            .logs(
                pod.name_any().as_str(),
                &LogParams {
                    container: Some(container_name.clone()),
                    since_time: Some(Timestamp::default()),
                    timestamps: true,
                    ..self.group.clone().into()
                },
            )
            .await
            {
                Ok(logs) => Ok(logs),
                // If a 400 error occurs, returns the current representations, as that indicates no logs exist.
                Err(kube::Error::Api(status)) if status.code == 400 => {
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
                            LogSelection::Current => LogGroup::Current(Container(container_name)),
                            LogSelection::Previous => LogGroup::Previous(Container(container_name)),
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
    use std::sync::Arc;
    use std::time::Duration;

    use backon::{ConstantBuilder, Retryable};
    use k8s_openapi::{api::core::v1::Pod, serde_json};
    use kube::Api;
    use kube::core::params::PostParams;
    use tempfile::TempDir;
    use tokio::time::timeout;

    use crate::cli::{DEFAULT_OCI_BUFFER_SIZE, DebugPod};
    use crate::filters::filter::Include;
    use crate::gather::config::{GatherMode, Secrets};
    use crate::{
        filters::{
            filter::{FilterGroup, FilterList},
            namespace::Namespace,
        },
        gather::{
            config::Config,
            writer::{Archive, Encoding, Writer},
        },
        scanners::{interface::Collect, logs::LogSelection, objects::Objects},
    };

    use super::Logs;

    #[tokio::test]
    async fn collect_logs() {
        let test_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster");
        let filter = Namespace::<Include>::try_from("default").unwrap();

        let pod_api: Api<Pod> = Api::default_namespaced(test_env.client().expect("client"));

        let pod = timeout(
            Duration::new(10, 0),
            (|| async {
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
            })
            .retry(ConstantBuilder::default().with_delay(Duration::from_secs(1))),
        )
        .await
        .expect("Timeout")
        .expect("Pod to be created");

        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let file_path = tmp_dir.path().join("crust-gather-test");
        let repr = Logs {
            skip_logs_collection: false,
            collectable: Objects::new_typed(Config {
                skip_logs_collection: false,
                client: test_env.client().expect("client"),
                filter: Arc::new(FilterGroup(vec![FilterList(vec![vec![filter].into()])])),
                writer: Writer::new(
                    &Archive::new(file_path),
                    &Encoding::Path,
                    None,
                    None,
                    DEFAULT_OCI_BUFFER_SIZE,
                )
                .await
                .expect("failed to create builder")
                .into(),
                secrets: Secrets::default(),
                mode: GatherMode::Collect,
                additional_logs: Vec::default(),
                duration: "1m".try_into().unwrap(),
                systemd_units: Vec::default(),
                debug_pod: DebugPod::default(),
                disable_additional_logs: false,
                extension: String::new(),
            }),
            group: LogSelection::Current,
        }
        .representations(&pod)
        .await
        .expect("Succeed");

        let repr = repr[0].clone();
        assert_eq!(repr.data(), "");
    }
}
