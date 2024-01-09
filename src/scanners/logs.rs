use std::{fmt, path::PathBuf, sync::Arc};

use anyhow::bail;
use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::{Api, Client};
use kube_core::{
    subresource::LogParams, ApiResource, DynamicObject, ErrorResponse, GroupVersionKind, TypeMeta, Resource,
};

use crate::{filters::filter::Filter, gather::writer::Representation};

use super::{generic::Object, interface::Collect};

#[derive(Clone, Copy, PartialEq)]
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

impl Into<LogParams> for LogGroup {
    fn into(self) -> LogParams {
        LogParams {
            previous: self == LogGroup::Previous,
            ..Default::default()
        }
    }
}

/// Logs collects container logs for pods. It contains a Collectable for
/// querying pods and a LogGroup to specify whether to collect current or
/// previous logs.
pub struct Logs {
    pub collectable: Object,
    pub group: LogGroup,
}

impl Logs {
    pub fn new(client: Client, filter: Arc<dyn Filter>, group: LogGroup) -> Self {
        Logs {
            collectable: Object::new(client, ApiResource::erase::<Pod>(&()), filter),
            group,
        }
    }
}

#[async_trait]
/// Implements the Collect trait for Logs. This collects container logs for Kubernetes pods.
///
/// The path() method returns the path for the pod object.
///
/// The get_type_meta() method returns the TypeMeta for pods.
///
/// The get_api() method returns the API client for pods.
///
/// The filter() method filters pods based on the configured filter.
///
/// The representations() method collects the container logs for the given pod. It parses the pod,
/// gets the API client for that pod, and then for each container it calls the logs API to get the
/// logs. It returns a vector of Representation objects containing the log data.
impl Collect for Logs {
    /// Returns the path for the pod object. This will be the root path for the logs to store in.
    fn path(self: &Self, obj: &DynamicObject) -> PathBuf {
        self.collectable.path(obj)
    }

    fn get_type_meta(&self) -> TypeMeta {
        TypeMeta {
            api_version: Pod::api_version(&()).into(),
            kind: Pod::kind(&()).into(),
        }
    }

    fn get_api(&self) -> Api<DynamicObject> {
        self.collectable.get_api()
    }

    fn filter(&self, gvk: &GroupVersionKind, obj: &DynamicObject) -> bool {
        self.collectable.filter(gvk, obj)
    }

    /// Collects container logs representations.
    async fn representations(&self, obj: &DynamicObject) -> anyhow::Result<Vec<Representation>> {
        let pod: Pod = obj.clone().try_parse()?;
        let api: Api<Pod> = Api::namespaced(
            self.get_api().into(),
            pod.metadata.clone().namespace.unwrap().as_ref(),
        );
        let mut representations = vec![];

        for container in pod.spec.unwrap().containers {
            let meta = pod.metadata.clone();
            let logs = match api
                .logs(
                    meta.clone().name.unwrap().as_str(),
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
                    log::info!(
                        "No {} logs found for pod {}",
                        self.group,
                        meta.name.unwrap()
                    );
                    return Ok(representations);
                }
                Err(e) => bail!(e),
            };

            let container_path =
                format!("{}/{}/{}", meta.name.unwrap(), container.name, self.group);
            representations.push(
                Representation::new()
                    .with_path(self.path(obj).with_file_name(container_path))
                    .with_data(logs.as_str()),
            )
        }

        Ok(representations)
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Duration};

    use k8s_openapi::{api::core::v1::Pod, serde_json};
    use kube::Api;
    use kube_core::{params::PostParams, ApiResource, DynamicObject};
    use tokio::time::timeout;
    use tokio_retry::{Retry, strategy::FixedInterval};

    use crate::{
        filters::namespace::NamespaceInclude,
        scanners::{generic::Object, interface::Collect},
        tests::kwok,
    };

    use super::{LogGroup, Logs};

    #[tokio::test]
    async fn collect_logs() {
        let test_env = kwok::TestEnvBuilder::default()
            .insecure_skip_tls_verify(true)
            .build().await;
        let filter = NamespaceInclude::try_from("default".to_string()).unwrap();

        let pod_api: Api<DynamicObject> =
            Api::default_namespaced_with(test_env.client().await, &ApiResource::erase::<Pod>(&()));

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

        let repr = Logs {
            collectable: Object::new(
                test_env.client().await,
                ApiResource::erase::<Pod>(&()),
                Arc::new(filter),
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
