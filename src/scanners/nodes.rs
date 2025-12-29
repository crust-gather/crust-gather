use std::{
    fmt::{self, Debug},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::{
    Container, HostPathVolumeSource, Node, Pod, PodSpec, Toleration, Volume, VolumeMount,
};
use kube::Api;
use kube::{
    api::TypeMeta,
    core::{
        ApiResource, ObjectMeta, ResourceExt, WatchEvent,
        params::{DeleteParams, WatchParams},
        subresource::AttachParams,
    },
};
use thiserror::Error;
use tracing::instrument;

use crate::{
    cli::DebugPod,
    gather::{
        config::{Config, Secrets},
        representation::{ArchivePath, LogGroup, Representation},
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
pub struct Nodes {
    pub collectable: Objects<Node>,
    systemd_units: Vec<String>,
    pub debug_pod: DebugPod,
}

impl Debug for Nodes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Nodes").finish()
    }
}

impl From<Config> for Nodes {
    fn from(value: Config) -> Self {
        Self {
            systemd_units: value.systemd_units.clone(),
            debug_pod: value.debug_pod.clone(),
            collectable: Objects::new_typed(value),
        }
    }
}

#[async_trait]
impl Collect<Node> for Nodes {
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
        tracing::info!("Collecting node logs");

        let node_name = node.name_any();
        let pod = Self::get_template_pod(&self.debug_pod, "node-debug".into(), node_name);
        let pod_name = pod.name_any();
        self.get_or_create(pod).await?;

        self.collect_logs(node, pod_name).await
    }

    fn get_api(&self) -> Api<Node> {
        self.collectable.get_api()
    }

    #[allow(refining_impl_trait)]
    fn resource(&self) -> ApiResource {
        self.collectable.resource()
    }
}

impl Nodes {
    #[instrument(skip_all, fields(pod_name = pod.name_any()), err)]
    async fn get_or_create(&self, pod: Pod) -> anyhow::Result<()> {
        let api = Api::default_namespaced(self.get_api().into());

        let found = api
            .get_opt(pod.name_any().as_str())
            .await
            .map_err(DebugPodError::Get)?;

        if found.is_none() {
            tracing::info!("Creating debug pod");
            api.create(&Default::default(), &pod)
                .await
                .map_err(DebugPodError::Create)?;
        };

        Ok(())
    }

    #[instrument(skip_all, fields(pod_name = pod_name), err)]
    async fn collect_logs(
        &self,
        node: &Node,
        pod_name: String,
    ) -> anyhow::Result<Vec<Representation>> {
        tracing::info!("Waiting for pod to be running");

        let wp = WatchParams::default()
            .fields(format!("metadata.name={pod_name}").as_str())
            .timeout(10);

        let api = Api::default_namespaced(self.get_api().into());
        let mut stream = api.watch(&wp, "0").await?.boxed();
        while let Some(WatchEvent::Modified(Pod { status, .. })) = stream.try_next().await? {
            let s = status.as_ref().expect("status exists on pod");
            if s.phase.clone().unwrap_or_default() == "Running" {
                tracing::info!("Attaching to pod");
                break;
            }
        }

        let mut representations = vec![
            self.get_representation(
                pod_name.as_str(),
                vec!["sh", "-c", "cat /host/var/log/kubelet.log"],
                ArchivePath::logs_path(node, TypeMeta::resource::<Node>(), LogGroup::KubeletLegacy),
            )
            .await?,
        ];

        for systemd_unit in self.systemd_units.iter() {
            representations.push(
                self.get_representation(
                    pod_name.as_str(),
                    vec![
                        "sh",
                        "-c",
                        &format!(
                            "chroot /host /bin/sh <<\"EOT\"\njournalctl -u {systemd_unit}\n\"EOT\""
                        ),
                    ],
                    ArchivePath::logs_path(
                        node,
                        TypeMeta::resource::<Node>(),
                        LogGroup::Kubelet(systemd_unit.clone()),
                    ),
                )
                .await?,
            );
        }

        api.delete(&pod_name, &DeleteParams::default().grace_period(0))
            .await?;

        Ok(representations.into_iter().flatten().collect())
    }

    #[instrument(skip_all, fields(node = path.to_string()))]
    async fn get_representation(
        &self,
        pod_name: &str,
        args: Vec<&str>,
        path: ArchivePath,
    ) -> anyhow::Result<Option<Representation>> {
        let api: Api<Pod> = Api::default_namespaced(self.get_api().into());

        let mut attached = api
            .exec(pod_name, args, &AttachParams::default().stderr(false))
            .await?;

        let stdout = tokio_util::io::ReaderStream::new(attached.stdout().unwrap());
        let out = stdout
            .filter_map(|r| async { r.ok().and_then(|v| String::from_utf8(v.to_vec()).ok()) })
            .collect::<Vec<_>>()
            .await
            .join("");
        attached.join().await.unwrap();

        match out.as_str() {
            "" => {
                tracing::debug!("Node debug output is unavailable.");
                Ok(None)
            }
            data => Ok(Some(Representation::new().with_path(path).with_data(data))),
        }
    }

    pub fn get_template_pod(debug_pod: &DebugPod, log_path: String, node_name: String) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(format!("{log_path}-{node_name}")),
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
            ..Default::default()
        }
    }
}
