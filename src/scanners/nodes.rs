use std::{
    fmt::{self, Debug},
    sync::{Arc, Mutex},
};

use anyhow::bail;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::{
    Container, HostPathVolumeSource, Node, Pod, PodSpec, Toleration, Volume, VolumeMount,
};
use kube::Api;
use kube_core::{
    params::{DeleteParams, WatchParams},
    subresource::AttachParams,
    ApiResource, ErrorResponse, ObjectMeta, ResourceExt, TypeMeta, WatchEvent,
};

use crate::gather::{
    config::{Config, Secrets},
    representation::{ArchivePath, LogGroup, Representation},
    writer::Writer,
};

use super::{interface::Collect, objects::Objects};

#[derive(Clone)]
pub struct Nodes {
    pub collectable: Objects<Node>,
}

impl Debug for Nodes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Nodes").finish()
    }
}

impl From<Config> for Nodes {
    fn from(value: Config) -> Self {
        Self {
            collectable: Objects::new_typed(value, ApiResource::erase::<Node>(&())),
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

    fn filter(&self, obj: &Node) -> anyhow::Result<bool> {
        self.collectable.filter(obj)
    }

    /// Collects container logs representations.
    async fn representations(&self, node: &Node) -> anyhow::Result<Vec<Representation>> {
        let node_name = node.name_any();
        log::info!("Collecting node logs for {}", node.name_any());

        let pod = Self::get_template_pod(node_name);
        let pod_name = pod.name_any();
        let api: Api<Pod> = Api::default_namespaced(self.get_api().into());

        log::info!("Creating debug pod {pod_name}");
        match api.get(pod_name.as_str()).await {
            Ok(pod) => pod,
            Err(kube::Error::Api(ErrorResponse { code: 404, .. })) => {
                match api.create(&Default::default(), &pod).await {
                    Ok(pod) => pod,
                    Err(e) => {
                        log::error!("Failed to create pod {pod_name}: {e}");
                        bail!(e)
                    }
                }
            }
            Err(e) => {
                log::error!("Failed to get pod {pod_name}: {e}");
                bail!(e)
            }
        };

        log::info!("Waiting for pod {pod_name} to be running");
        let wp = WatchParams::default()
            .fields(format!("metadata.name={pod_name}").as_str())
            .timeout(10);
        let mut stream = api.watch(&wp, "0").await?.boxed();
        while let Some(WatchEvent::Modified(Pod { status, .. })) = stream.try_next().await? {
            let s = status.as_ref().expect("status exists on pod");
            if s.phase.clone().unwrap_or_default() == "Running" {
                log::info!("Attaching to {pod_name}");
                break;
            }
        }

        let representations = vec![
            self.get_representation(
                pod_name.as_str(),
                vec![
                    "sh",
                    "-c",
                    "chroot /host /bin/sh <<\"EOT\"\njournalctl -u kubelet\n\"EOT\"",
                ],
                ArchivePath::logs_path(node, self.get_type_meta(), LogGroup::Node),
            )
            .await?,
            self.get_representation(
                pod_name.as_str(),
                vec!["sh", "-c", "cat /host/var/log/kubelet.log"],
                ArchivePath::logs_path(node, self.get_type_meta(), LogGroup::NodePath),
            )
            .await?,
        ];

        api.delete(&pod_name, &DeleteParams::default().grace_period(0))
            .await?;

        Ok(representations.into_iter().flatten().collect())
    }

    fn get_api(&self) -> Api<Node> {
        self.collectable.get_api()
    }

    fn get_type_meta(&self) -> TypeMeta {
        self.collectable.get_type_meta()
    }
}

impl Nodes {
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
                log::debug!("Node {path} is unavailable.");
                Ok(None)
            }
            data => Ok(Some(Representation::new().with_path(path).with_data(data))),
        }
    }

    fn get_template_pod(node_name: String) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(format!("node-debug-{node_name}")),
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
                tolerations: Some(vec![Toleration {
                    operator: Some("Exists".into()),
                    ..Default::default()
                }]),
                containers: vec![Container {
                    name: "debug".into(),
                    stdin: Some(true),
                    tty: Some(true),
                    image: Some("busybox".into()),
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
