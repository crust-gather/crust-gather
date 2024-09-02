use std::{
    fmt::{self, Debug},
    ops::Deref,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::Api;
use kube::{
    api::TypeMeta,
    core::{
        params::{DeleteParams, WatchParams},
        subresource::AttachParams,
        ApiResource, ResourceExt, WatchEvent,
    },
};
use tracing::instrument;

use crate::{
    gather::{
        config::{Config, Secrets},
        representation::{ArchivePath, CustomLog, LogGroup, Representation},
        writer::Writer,
    },
    scanners::nodes::Nodes,
};

use super::{
    interface::{Collect, CollectError},
    nodes::DebugPodError,
    objects::Objects,
};

#[derive(Clone)]
pub struct UserLogs {
    pub collectable: Objects<Node>,
    pub logs: Vec<CustomLog>,
}

impl Debug for UserLogs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UserLogs").finish()
    }
}

impl From<Config> for UserLogs {
    fn from(value: Config) -> Self {
        Self {
            logs: value.additional_logs.clone(),
            collectable: Objects::new_typed(value),
        }
    }
}

#[async_trait]
impl Collect<Node> for UserLogs {
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
        tracing::info!("Collecting user logs");

        let node_name = node.name_any();

        let mut pods = vec![];
        for log in self.logs.deref() {
            let pod = Nodes::get_template_pod(log.path.clone(), node_name.clone());
            pods.push(pod.clone());
            self.get_or_create(pod).await?;
        }

        let mut representations = vec![];
        for pod in pods.deref() {
            let logs = self.collect_logs(node, pod.name_any()).await?;
            representations.extend(logs);
        }

        let api: Api<Pod> = Api::default_namespaced(self.get_api().into());
        for pod in pods.deref() {
            api.delete(&pod.name_any(), &DeleteParams::default().grace_period(0))
                .await?;
        }

        Ok(representations)
    }

    fn get_api(&self) -> Api<Node> {
        self.collectable.get_api()
    }

    fn resource(&self) -> ApiResource {
        self.collectable.resource()
    }
}

impl UserLogs {
    #[instrument(skip_all, fields(pod_name = pod.name_any()), err)]
    async fn get_or_create(&self, pod: Pod) -> anyhow::Result<()> {
        let api = Api::default_namespaced(self.get_api().into());

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

        let mut representations = vec![];

        for custom_log in self.logs.deref() {
            let representation = self
                .get_representation(
                    pod_name.as_str(),
                    custom_log.command.split_ascii_whitespace().collect(),
                    ArchivePath::logs_path(
                        node,
                        TypeMeta::resource::<Node>(),
                        LogGroup::Custom(custom_log.clone()),
                    ),
                )
                .await?;

            representations.push(representation);
        }

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
                tracing::debug!("User log output is empty.");
                Ok(None)
            }
            data => Ok(Some(Representation::new().with_path(path).with_data(data))),
        }
    }
}
