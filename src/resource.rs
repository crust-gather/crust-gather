use std::fs::File;
use std::path::Path;

use anyhow;
use flate2::write::GzEncoder;
use flate2::Compression;
use k8s_openapi::api::core::v1::{Event, Pod};
use kube::{discovery, Client};
use kube_core::discovery::verbs::LIST;
use kube_core::ApiResource;
use tar::Builder;

use crate::filter::NamespaceInclude;
use crate::scanners::events::Events;
use crate::scanners::generic::Collectable;
use crate::scanners::interface::Collect;
use crate::scanners::logs::{LogGroup, Logs};

enum Group {
    Logs(ApiResource),
    Events(ApiResource),
    DynamicObject(ApiResource),
}

impl Into<Group> for ApiResource {
    fn into(self) -> Group {
        match self {
            r if r == ApiResource::erase::<Event>(&()) => Group::Events(r),
            r if r == ApiResource::erase::<Pod>(&()) => Group::Logs(r),
            r => Group::DynamicObject(r),
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub client: Client,
    pub filter: NamespaceInclude,
}

impl Group {
    fn to_collectable(self, config: Config) -> Vec<Box<dyn Collect>> {
        let (client, filter) = (config.client, config.filter);
        match self {
            Group::Logs(resource) => vec![
                Logs::new(client.clone(), filter.clone().into(), LogGroup::Current).into(),
                Logs::new(client.clone(), filter.clone().into(), LogGroup::Previous).into(),
                Collectable::new(client, resource, filter.into()).into(),
            ],
            Group::Events(resource) => vec![
                Events::new(client.clone(), filter.clone().into()).into(),
                Collectable::new(client, resource, filter.into()).into(),
            ],
            Group::DynamicObject(resource) => {
                vec![Collectable::new(client, resource, filter.into()).into()]
            }
        }
    }
}

impl Config {
    pub async fn collect(self) -> anyhow::Result<()> {
        let discovery = discovery::Discovery::new(self.client.clone()).run().await?;

        let groups: Vec<Box<dyn Collect>> = discovery
            .groups()
            .map(|g| g.recommended_resources())
            .flatten()
            .filter_map(|r| r.1.supports_operation(LIST).then_some(r.0.into()))
            .map(|group: Group| group.to_collectable(self.clone()))
            .flatten()
            .collect();

        let path = Path::new("./crust-gather.tar.gz");
        let file = File::create(&path).unwrap();
        let enc = GzEncoder::new(file, Compression::default());
        let ar = &mut Builder::new(enc);

        for mut group in groups {
            for repr in group.collect().await? {
                repr.write(ar)?;
            }
        }

        Ok(())
    }
}
