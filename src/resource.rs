use anyhow;
use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::{discovery, Api, Client};
use kube_core::discovery::verbs::LIST;
use kube_core::discovery::{ApiCapabilities, Scope};
use kube_core::params::ListParams;
use kube_core::subresource::LogParams;
use kube_core::{ApiResource, DynamicObject};
use serde_yaml;
use std::{fs, path};

#[derive(Debug)]
pub struct Namespaced {
    api: Api<DynamicObject>,
    resource: ApiResource,
}

#[derive(Debug)]
pub struct ClusterScoped {
    api: Api<DynamicObject>,
    resource: ApiResource,
}

#[derive(Debug)]
pub struct Pods {
    api: Api<Pod>,
    namespaced: Namespaced,
}

struct Collector {
    client: Client,
    namespace: String,
    capabilities: ApiCapabilities,
    resource: ApiResource,
}

#[async_trait]
trait Processable: Sync + Send {
    fn path(&self, obj: &DynamicObject) -> String;

    fn repr(&self, obj: &DynamicObject) -> anyhow::Result<String> {
        Ok(serde_yaml::to_string(&obj)?)
    }

    fn write(&self, obj: &DynamicObject) -> anyhow::Result<()> {
        let path = self.path(&obj);
        let path = path::Path::new(&path);
        fs::create_dir_all(path.parent().unwrap())?;
        fs::write(path, self.repr(&obj)?)?;
        Ok(())
    }

    fn get_api(&self) -> Api<DynamicObject>;

    async fn list(&mut self) -> anyhow::Result<Vec<DynamicObject>> {
        let data = self.get_api().list(&ListParams::default()).await?;
        Ok(data.items)
    }

    async fn process(self: &mut Self) -> anyhow::Result<()> {
        for obj in self.list().await? {
            self.write(&obj)?;
        }

        Ok(())
    }
}

impl Namespaced {
    fn new(api: Api<DynamicObject>, resource: ApiResource) -> Namespaced {
        Namespaced {
            resource: resource.clone(),
            api: api,
        }
    }
}

impl ClusterScoped {
    fn new(api: Api<DynamicObject>, resource: ApiResource) -> ClusterScoped {
        ClusterScoped {
            resource: resource.clone(),
            api: api,
        }
    }
}

impl Pods {
    fn new(api: Api<Pod>, resource: ApiResource, namespace: String) -> Pods {
        Pods {
            api: api.clone(),
            namespaced: Namespaced::new(
                Api::namespaced_with(
                    api.into_client(),
                    &namespace,
                    &resource,
                ),
                resource,
            ),
        }
    }
}

#[async_trait]
impl Processable for ClusterScoped {
    fn path(self: &Self, obj: &DynamicObject) -> String {
        format!(
            "./cluster/{}/{}.yaml",
            self.resource.kind,
            obj.metadata.name.clone().unwrap()
        )
    }

    fn get_api(&self) -> Api<DynamicObject> {
        self.api.clone()
    }
}

#[async_trait]
impl Processable for Namespaced {
    fn path(self: &Self, obj: &DynamicObject) -> String {
        format!(
            "./{}/{}/{}.yaml",
            obj.metadata.namespace.clone().unwrap(),
            self.resource.kind,
            obj.metadata.name.clone().unwrap()
        )
    }

    fn get_api(&self) -> Api<DynamicObject> {
        self.api.clone()
    }
}

#[async_trait]
impl Processable for Pods {
    fn path(self: &Self, obj: &DynamicObject) -> String {
        format!(
            "./{}/{}/{}.log",
            obj.metadata.namespace.clone().unwrap(),
            self.namespaced.resource.kind,
            obj.metadata.name.clone().unwrap()
        )
    }

    fn get_api(&self) -> Api<DynamicObject> {
        self.namespaced.get_api()
    }

    async fn process(self: &mut Self) -> anyhow::Result<()> {
        self.namespaced.process().await?;

        for obj in self.list().await? {
            let pod: Pod = obj.clone().try_parse()?;
            for container in pod.spec.unwrap().containers {
                let logs = self
                    .api
                    .logs(
                        pod.metadata.name.clone().unwrap().as_str(),
                        &LogParams {
                            container: Some(container.name.clone()),
                            ..Default::default()
                        },
                    )
                    .await?;
                let path = self.path(&obj);
                let path = path::Path::new(&path);
                fs::create_dir_all(path.parent().unwrap())?;
                fs::write(path, logs)?;

                let previous = match self
                    .api
                    .logs(
                        pod.metadata.name.clone().unwrap().as_str(),
                        &LogParams {
                            container: Some(container.name),
                            previous: true,
                            ..Default::default()
                        },
                    )
                    .await {
                        Ok(previous) => previous,
                        Err(_) => return Ok(()),
                    };
                let path = self.path(&obj);
                let path = path::Path::new(&path);
                fs::create_dir_all(path.parent().unwrap())?;
                fs::write(path, previous)?;
            }
        }

        Ok(())
    }
}

impl From<Collector> for Box<dyn Processable> {
    fn from(c: Collector) -> Self {
        match c.resource.kind.as_str() {
            "Pod" => Box::new(Pods::new(
                Api::namespaced(c.client.clone(), c.namespace.clone().as_str()),
                c.resource,
                c.namespace,
            )),
            _ => match c.capabilities.scope {
                Scope::Namespaced => Box::new(Namespaced::new(
                    Api::namespaced_with(c.client.clone(), c.namespace.as_str(), &c.resource),
                    c.resource,
                )),
                Scope::Cluster => Box::new(ClusterScoped::new(
                    Api::all_with(c.client.clone(), &c.resource),
                    c.resource,
                )),
            },
        }
    }
}

pub async fn collect(namespace: &String) -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let discovery = discovery::Discovery::new(client.clone()).run().await?;
    let groups = discovery.groups();

    let groups: Vec<Box<dyn Processable>> = groups
        .map(|g| g.recommended_resources())
        .flatten()
        .filter_map(|r| r.1.supports_operation(LIST).then_some(r))
        .map(|(resouce, capabilities)| {
            Collector {
                namespace: namespace.clone(),
                client: client.clone(),
                capabilities: capabilities,
                resource: resouce,
            }
            .into()
        })
        .collect();

    for mut group in groups {
        group.process().await?;
    }

    Ok(())
}
