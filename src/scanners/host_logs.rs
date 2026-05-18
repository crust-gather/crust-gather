use std::{
    fmt::{self, Debug},
    ops::Deref,
    sync::Arc,
};

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, executor::block_on};
use k8s_openapi::{
    api::core::v1::{
        Container, ContainerState, ContainerStateTerminated, ContainerStatus, HostPathVolumeSource,
        Node, Pod, PodSpec, PodStatus, Toleration, Volume, VolumeMount,
    },
    apimachinery::pkg::apis::meta::v1::{Status, Time},
    jiff::Timestamp,
    serde_json,
};
use kube::Api;
use kube::{
    api::TypeMeta,
    core::{
        ApiResource, DynamicObject, ObjectMeta, ResourceExt, WatchEvent,
        params::{DeleteParams, WatchParams},
        subresource::AttachParams,
    },
};
use scopeguard::defer;
use serde::{Serialize, Serializer};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::Mutex;
use tracing::instrument;

use crate::{
    cli::DebugPod,
    gather::{
        config::{Config, Secrets},
        representation::{self, ArchivePath, CustomLog, LogGroup, Representation},
        writer::Writer,
    },
};

use super::{
    interface::{Collect, CollectError},
    objects::Objects,
};

/// Failure of debug pod
#[derive(Debug, Error)]
pub enum DebugPodError {
    #[error("Failed to create pod: {0:?}")]
    Create(kube::Error),

    #[error("Failed to get pod: {0:?}")]
    Get(kube::Error),
}

#[derive(Clone)]
pub struct HostLogs {
    pub disabled: bool,
    pub collectable: Objects<Node>,
    pub logs: Vec<CustomLog>,
    pub debug_pod: DebugPod,
}

#[derive(Serialize)]
pub struct HostLogsRepresentation {
    pub command: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub output: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub errors: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    #[serde(serialize_with = "serialize_status_without_metadata")]
    pub status: Option<Status>,
}

fn serialize_status_without_metadata<S>(
    status: &Option<Status>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let Some(status) = status else {
        return serializer.serialize_none();
    };

    let mut value = serde_json::to_value(status).map_err(serde::ser::Error::custom)?;
    if let serde_json::Value::Object(ref mut object) = value {
        object.remove("metadata");
        object.remove("kind");
        object.remove("apiVersion");
    }

    value.serialize(serializer)
}

impl Debug for HostLogs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostLogs").finish()
    }
}

impl From<Config> for HostLogs {
    fn from(value: Config) -> Self {
        let mut logs = value.additional_logs.clone();
        for systemd_unit in value.systemd_units.iter() {
            logs.push(CustomLog {
                path: systemd_unit.to_string(),
                command: format!(
                    "chroot /host /bin/sh <<\"EOT\"\njournalctl -u {systemd_unit}\nEOT"
                ),
            });
        }

        Self {
            logs,
            debug_pod: value.debug_pod.clone(),
            disabled: value.disable_additional_logs,
            collectable: Objects::new_typed(value),
        }
    }
}

#[async_trait]
impl Collect<Node> for HostLogs {
    fn get_secrets(&self) -> Secrets {
        self.collectable.get_secrets()
    }

    fn get_writer(&self) -> Arc<Mutex<Writer>> {
        self.collectable.get_writer()
    }

    fn filter(&self, obj: &Node) -> Result<bool, CollectError> {
        self.collectable.filter(obj)
    }

    /// Collects container logs representations.
    #[instrument(skip_all, fields(node = node.name_any()), err)]
    async fn representations(&self, node: &Node) -> anyhow::Result<Vec<Representation>> {
        if self.disabled {
            tracing::warn!("Host logs collection disabled, skipping...");
            return Ok(vec![]);
        }

        tracing::info!("Collecting host logs");

        let node_name = node.name_any();

        let pod = Self::get_template_pod(&self.debug_pod, node_name.clone());
        defer! {
            let _ = block_on(self.delete(&pod));
        }
        self.get_or_create(pod.clone()).await?;

        let mut representations = vec![];
        let logs = self.logs.iter().map(|l| l.path.clone()).collect();
        representations.push(Self::pod_representation(&pod, logs)?);

        for log in self.logs.deref() {
            let logs = self.collect_logs(&pod, log).await?;
            representations.extend(logs);
        }

        Ok(representations)
    }

    fn get_api(&self) -> Api<Node> {
        self.collectable.get_api()
    }

    #[allow(refining_impl_trait)]
    fn resource(&self) -> ApiResource {
        self.collectable.resource()
    }
}

impl HostLogs {
    fn pod_representation(
        pod: &Pod,
        log_containers: Vec<String>,
    ) -> anyhow::Result<Representation> {
        let mut archive_pod = pod.clone();
        if let Some(spec) = archive_pod.spec.as_mut() {
            let template = spec.containers.first().cloned().unwrap_or_default();
            spec.containers = vec![];
            for container in &log_containers {
                spec.containers.push(Container {
                    name: container.clone(),
                    ..template.clone()
                });
            }
        }

        if let Some(status) = archive_pod.status.as_mut() {
            status.phase = Some("Succeeded".to_string());
            status.conditions = Some(vec![]);
            let mut container_statuses = vec![];
            for container in log_containers {
                container_statuses.push(ContainerStatus {
                    name: container,
                    ready: true,
                    state: Some(ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            exit_code: 0,
                            reason: Some("Completed".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
            }
            status.container_statuses = Some(container_statuses);
        }

        let obj = serde_json::to_value(&archive_pod)?;
        let mut data: DynamicObject = serde_json::from_value(obj)?;
        data.types = Some(data.types.unwrap_or(TypeMeta::resource::<Pod>()));

        Ok(Representation::new()
            .with_path(ArchivePath::to_path(pod, TypeMeta::resource::<Pod>()))
            .with_data(&serde_saphyr::to_string(&data)?))
    }

    async fn read_stream<R>(reader: Option<R>) -> anyhow::Result<String>
    where
        R: AsyncRead + Unpin,
    {
        let Some(mut reader) = reader else {
            return Ok(String::new());
        };

        let mut output = String::new();
        reader.read_to_string(&mut output).await?;
        Ok(output)
    }

    fn pod_ready(pod: &Pod) -> bool {
        pod.status
            .as_ref()
            .and_then(|status| status.container_statuses.as_ref())
            .is_some_and(|statuses| statuses.iter().any(|status| status.ready))
    }

    fn get_template_pod(debug_pod: &DebugPod, node_name: String) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(node_name.to_string()),
                namespace: Some(debug_pod.namespace.clone().unwrap_or("default".to_string())),
                creation_timestamp: Some(Time(Timestamp::now())),
                ..Default::default()
            },
            spec: Some(PodSpec {
                node_name: Some(node_name),
                restart_policy: Some("Never".into()),
                dns_policy: Some("ClusterFirst".into()),
                enable_service_links: Some(true),
                host_ipc: Some(true),
                host_network: Some(true),
                host_pid: Some(true),
                automount_service_account_token: Some(false),
                tolerations: Some(vec![
                    Toleration {
                        operator: Some("Exists".into()),
                        ..Default::default()
                    },
                    Toleration {
                        operator: Some("Exists".into()),
                        key: Some("NoSchedule".into()),
                        ..Default::default()
                    },
                ]),
                containers: vec![Container {
                    name: "debug".into(),
                    stdin: Some(true),
                    tty: Some(true),
                    image: debug_pod.image.clone().or(Some("busybox".into())),
                    image_pull_policy: Some("IfNotPresent".into()),
                    volume_mounts: Some(vec![VolumeMount {
                        name: "host-root".into(),
                        mount_path: "/host".into(),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }],
                volumes: Some(vec![Volume {
                    name: "host-root".into(),
                    host_path: Some(HostPathVolumeSource {
                        path: "/".into(),
                        type_: Some(String::new()),
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            status: Some(PodStatus {
                ..Default::default()
            }),
        }
    }

    #[instrument(skip_all, fields(pod_name = pod.name_any()), err)]
    async fn get_or_create(&self, pod: Pod) -> anyhow::Result<()> {
        let api = Api::namespaced(
            self.get_api().into(),
            &pod.namespace().unwrap_or("default".to_string()),
        );

        let found = api
            .get_opt(pod.name_any().as_str())
            .await
            .map_err(DebugPodError::Get)?;

        if found.is_none() {
            tracing::info!("Creating user logs debug pod");
            api.create(&Default::default(), &pod)
                .await
                .map_err(DebugPodError::Create)?;
        };

        Ok(())
    }

    #[instrument(skip_all, fields(pod_name = pod.name_any(), namespace = self.debug_pod.namespace), err)]
    async fn delete(&self, pod: &Pod) -> anyhow::Result<()> {
        let api: Api<Pod> = Api::namespaced(
            self.get_api().into(),
            &self
                .debug_pod
                .namespace
                .clone()
                .unwrap_or("default".to_string()),
        );
        api.delete(&pod.name_any(), &DeleteParams::default().grace_period(0))
            .await?;
        Ok(())
    }

    #[instrument(skip_all, fields(pod_name = pod.name_any()), err)]
    async fn collect_logs(
        &self,
        pod: &Pod,
        custom_log: &CustomLog,
    ) -> anyhow::Result<Vec<Representation>> {
        tracing::info!("Waiting for pod to be running");

        let wp = WatchParams::default()
            .fields(format!("metadata.name={pod_name}", pod_name = pod.name_any()).as_str())
            .timeout(10);

        let api = Api::namespaced(
            self.get_api().into(),
            &pod.namespace().unwrap_or("default".to_string()),
        );
        let mut stream = api.watch(&wp, "0").await?.boxed();
        while let Some(event) = stream.try_next().await? {
            if let WatchEvent::Added(pod) | WatchEvent::Modified(pod) = event
                && Self::pod_ready(&pod)
            {
                break;
            };
        }

        Ok(vec![
            self.get_representation(
                pod,
                &custom_log.command,
                Self::custom_log_archive_path(pod, custom_log),
            )
            .await?,
        ])
    }

    fn custom_log_archive_path(pod: &Pod, custom_log: &CustomLog) -> ArchivePath {
        ArchivePath::logs_path(
            pod,
            TypeMeta::resource::<Pod>(),
            LogGroup::Current(representation::Container(custom_log.path.clone())),
        )
    }

    #[instrument(skip_all, fields(node = path.to_string()))]
    async fn get_representation(
        &self,
        pod: &Pod,
        command: &str,
        path: ArchivePath,
    ) -> anyhow::Result<Representation> {
        let api: Api<Pod> = Api::namespaced(
            self.get_api().into(),
            &pod.namespace().unwrap_or("default".to_string()),
        );
        let args = vec!["sh", "-c", command];

        let mut attached = api
            .exec(
                &pod.name_any(),
                args.clone(),
                &AttachParams::default().stdout(true).stderr(true).tty(false),
            )
            .await?;

        let result = HostLogsRepresentation {
            command: command.to_string(),
            output: Self::read_stream(attached.stdout()).await?,
            errors: Self::read_stream(attached.stderr()).await?,
            status: match attached.take_status() {
                Some(status) => status.await,
                _ => None,
            },
        };
        attached.join().await?;

        Ok(Representation::new()
            .with_path(path)
            .with_data(&serde_saphyr::to_string(&result)?))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn pod_representation_uses_regular_pod_archive_path() {
        let pod = HostLogs::get_template_pod(
            &DebugPod {
                namespace: Some("default".into()),
                ..Default::default()
            },
            "worker-1".into(),
        );

        let representation =
            HostLogs::pod_representation(&pod, vec!["kubelet-log-path".to_string()]).unwrap();
        let dynamic_pod: DynamicObject = serde_saphyr::from_str(representation.data()).unwrap();
        let archived_pod: Pod = serde_saphyr::from_str(representation.data()).unwrap();

        assert_eq!(
            representation.path(),
            ArchivePath::Namespaced("namespaces/default/v1/pod/worker-1.yaml".into())
        );
        assert!(pod.metadata.creation_timestamp.is_some());
        assert_eq!(dynamic_pod.types.unwrap(), TypeMeta::resource::<Pod>());
        assert_eq!(
            archived_pod
                .spec
                .unwrap()
                .containers
                .iter()
                .map(|container| container.name.as_str())
                .collect::<Vec<_>>(),
            vec!["kubelet-log-path"]
        );
        let pod_spec = pod.spec.unwrap();
        assert_eq!(pod_spec.automount_service_account_token, Some(false));
        let debug_container = pod_spec.containers.into_iter().next().unwrap();
        assert_eq!(debug_container.command, None);
        assert_eq!(debug_container.stdin, Some(true));
        assert_eq!(debug_container.tty, Some(true));
    }

    #[test]
    fn pod_ready_waits_for_ready_container() {
        let mut pod = Pod {
            status: Some(PodStatus {
                container_statuses: Some(vec![ContainerStatus {
                    name: "debug".into(),
                    ready: true,
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(HostLogs::pod_ready(&pod));

        pod.status.as_mut().unwrap().container_statuses = Some(vec![ContainerStatus {
            name: "debug".into(),
            ready: false,
            ..Default::default()
        }]);
        assert!(!HostLogs::pod_ready(&pod));
    }

    #[test]
    fn user_log_archive_path_uses_virtual_pod_container_log() {
        let pod = HostLogs::get_template_pod(
            &DebugPod {
                namespace: Some("default".into()),
                ..Default::default()
            },
            "worker-1".into(),
        );
        let custom_log = CustomLog {
            path: "journal".into(),
            command: "journalctl -xe".into(),
        };

        assert_eq!(
            HostLogs::custom_log_archive_path(&pod, &custom_log),
            ArchivePath::Logs("namespaces/default/v1/pod/worker-1/journal/current.log".into())
        );
    }
}
