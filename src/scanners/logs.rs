use std::{fmt, path::PathBuf};

use anyhow::bail;
use async_trait::async_trait;
use k8s_openapi::{api::core::v1::Pod, Resource};
use kube::{Api, Client};
use kube_core::{subresource::LogParams, ApiResource, DynamicObject, ErrorResponse, TypeMeta};

use crate::filter::Filter;

use super::{
    generic::Collectable,
    interface::{Collect, Representation},
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LogGroup {
    Current,
    Previous,
}

impl fmt::Display for LogGroup {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "{}.log", format!("{:?}", self).to_lowercase())
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

pub struct Logs {
    pub collectable: Collectable,
    pub group: LogGroup,
}

impl Logs {
    pub fn new(client: Client, filter: Box<dyn Filter>, group: LogGroup) -> Self {
        Logs {
            collectable: Collectable::new(client, ApiResource::erase::<Pod>(&()), filter),
            group,
        }
    }
}

impl Into<Box<dyn Collect>> for Logs {
    fn into(self) -> Box<dyn Collect> {
        Box::new(self)
    }
}

#[async_trait]
impl Collect for Logs {
    fn path(self: &Self, obj: &DynamicObject) -> PathBuf {
        self.collectable.path(obj)
    }

    fn get_type_meta(&self) -> TypeMeta {
        TypeMeta {
            api_version: Pod::API_VERSION.into(),
            kind: Pod::KIND.into(),
        }
    }

    fn get_api(&self) -> Api<DynamicObject> {
        self.collectable.get_api()
    }

    fn filter(&self, obj: &DynamicObject) -> bool {
        self.collectable.filter(obj)
    }

    async fn representations(&self, obj: &DynamicObject) -> anyhow::Result<Vec<Representation>> {
        let pod: Pod = obj.clone().try_parse()?;
        let api: Api<Pod> = Api::namespaced(
            self.get_api().into(),
            pod.metadata.clone().namespace.unwrap().as_ref(),
        );
        let mut representations: Vec<Representation> = vec![];

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
                Err(kube::Error::Api(ErrorResponse { code: 400, .. })) => {
                    return Ok(representations)
                }
                Err(e) => bail!(e),
            };

            let container_path =
                format!("{}/{}/{}", meta.name.unwrap(), container.name, self.group);
            representations.push(
                Representation::new()
                    .with_path(self.path(obj).with_file_name(container_path))
                    .with_data(logs),
            )
        }

        Ok(representations)
    }
}

#[cfg(test)]
mod test {
    use k8s_openapi::{api::core::v1::Pod, serde_json};
    use kube::Api;
    use kube_core::{params::PostParams, ApiResource, DynamicObject};
    use tar::{Archive, Builder};

    use crate::{
        filter::NamespaceInclude,
        scanners::{generic::Collectable, interface::Collect},
        tests::kwok,
    };

    use super::{LogGroup, Logs};

    #[tokio::test]
    async fn collect_logs() {
        let test_env = kwok::TestEnvBuilder::default()
            .insecure_skip_tls_verify(true)
            .build();
        let filter = &NamespaceInclude {
            namespaces: vec!["default".into()],
        };

        let pod_api: Api<DynamicObject> =
            Api::default_namespaced_with(test_env.client().await, &ApiResource::erase::<Pod>(&()));

        let pod = pod_api
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
                .expect("Serialized pod"),
            )
            .await
            .expect("Pod to be created");

        let repr = Logs {
            collectable: Collectable::new(
                test_env.client().await,
                ApiResource::erase::<Pod>(&()),
                filter.into(),
            ),
            group: LogGroup::Current,
        }
        .representations(&pod)
        .await
        .expect("Succeed");

        let b = &mut Builder::new(vec![]);
        let repr = repr[0].clone();
        assert_eq!(repr.data(), "");
        assert!(repr.write(b).is_ok());
        let mut a = Archive::new(b.get_ref().as_slice());
        for e in a.entries().unwrap() {
            let e = e.unwrap();
            let p = e.path().unwrap();
            assert_eq!(
                p.to_str().unwrap(),
                "crust-gather/namespaces/default/pod/test/test/current.log"
            );
        }
    }
}
