use std::{
    fmt::{self, Debug},
    path::PathBuf,
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
    ApiResource, DynamicObject, ErrorResponse, GroupVersionKind, ObjectMeta, Resource, TypeMeta,
    WatchEvent,
};

use crate::gather::{
    config::{Config, Secrets},
    writer::{Representation, Writer},
};

use super::{generic::Object, interface::Collect};

#[derive(Clone)]
pub struct Nodes {
    pub collectable: Object,
}

impl Debug for Nodes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Nodes").finish()
    }
}

impl From<Config> for Nodes {
    fn from(value: Config) -> Self {
        Nodes {
            collectable: Object::new(value, ApiResource::erase::<Node>(&())),
        }
    }
}

#[async_trait]
impl Collect for Nodes {
    fn get_secrets(&self) -> Secrets {
        self.collectable.get_secrets()
    }

    fn get_writer(&self) -> Arc<Mutex<Writer>> {
        self.collectable.get_writer()
    }

    /// Returns the path for the pod object. This will be the root path for the logs to store in.
    fn path(&self, obj: &DynamicObject) -> PathBuf {
        self.collectable.path(obj)
    }

    fn filter(&self, gvk: &GroupVersionKind, obj: &DynamicObject) -> bool {
        self.collectable.filter(gvk, obj)
    }

    /// Collects container logs representations.
    async fn representations(&self, obj: &DynamicObject) -> anyhow::Result<Vec<Representation>> {
        log::info!(
            "Collecting node logs for {}",
            obj.metadata.clone().name.unwrap()
        );

        let node: Node = obj.clone().try_parse().unwrap();
        let node_name = node.meta().name.clone().unwrap();
        let pod_name = format!("node-debug-{node_name}");
        let pod = Pod {
            metadata: ObjectMeta {
                name: Some(pod_name.clone()),
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
                        type_: Some("".into()),
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
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
        while let Some(status) = stream.try_next().await? {
            match status {
                WatchEvent::Added(_) => {
                    log::info!("Added debug pod {pod_name}");
                }
                WatchEvent::Modified(o) => {
                    let s = o.status.as_ref().expect("status exists on pod");
                    if s.phase.clone().unwrap_or_default() == "Running" {
                        log::info!("Attaching to {pod_name}");
                        break;
                    }
                }
                _ => {}
            }
        }

        let representations = vec![
            self.get_representation(
                api.clone(),
                pod_name.as_str(),
                vec![
                    "sh",
                    "-c",
                    "chroot /host /bin/sh <<\"EOT\"\njournalctl -u kubelet\n\"EOT\"",
                ],
                self.path(obj).with_extension("").join("kubelet.log"),
            )
            .await?,
            self.get_representation(
                api.clone(),
                pod_name.as_str(),
                vec!["sh", "-c", "cat /host/var/log/kubelet.log"],
                self.path(obj)
                    .with_extension("")
                    .join("kubelet-log-path.log"),
            )
            .await?,
        ];

        api.delete(&pod_name, &DeleteParams::default().grace_period(0))
            .await?;

        Ok(representations)
    }

    fn get_api(&self) -> Api<DynamicObject> {
        self.collectable.get_api()
    }

    fn get_type_meta(&self) -> TypeMeta {
        TypeMeta {
            api_version: Node::api_version(&()).into(),
            kind: Node::kind(&()).into(),
        }
    }
}

impl Nodes {
    async fn get_representation(
        &self,
        api: Api<Pod>,
        pod_name: &str,
        args: Vec<&str>,
        path: PathBuf,
    ) -> anyhow::Result<Representation> {
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

        Ok(Representation::new()
            .with_path(path)
            .with_data(out.as_str()))
    }
}
