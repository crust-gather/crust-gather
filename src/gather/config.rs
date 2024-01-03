use std::sync::Arc;

use anyhow;
use k8s_openapi::api::core::v1::{Event, Pod};
use kube::{discovery, Client};
use kube_core::discovery::verbs::LIST;
use kube_core::ApiResource;

use crate::filters::filter::{Filter, List};
use crate::scanners::events::Events;
use crate::scanners::generic::Collectable;
use crate::scanners::interface::Collect;
use crate::scanners::logs::{LogGroup, Logs};

use super::writer::Writer;

pub struct GatherConfig {
    pub client: Client,
    pub filter: Arc<List>,
    pub writer: Writer,
    pub secrets: Vec<String>,
}

impl GatherConfig {
    /// Collect representations for resources from discovery to the specified archive file.
    pub async fn collect(&mut self) -> anyhow::Result<()> {
        let discovery = discovery::Discovery::new(self.client.clone()).run().await?;

        let groups: Vec<Box<dyn Collect>> = discovery
            .groups()
            .map(|g| g.recommended_resources())
            .flatten()
            .filter_map(|r| r.1.supports_operation(LIST).then_some(r.0.into()))
            .map(|group: Group| group.to_collectable(self.client.clone(), self.filter.clone()))
            .flatten()
            .collect();

        for mut group in groups {
            for repr in group.collect().await? {
                self.writer.add(repr, &self.secrets)?;
            }
        }

        Ok(self.writer.finish()?)
    }
}

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

impl Group {
    fn to_collectable(self, client: Client, filter: Arc<dyn Filter>) -> Vec<Box<dyn Collect>> {
        match self {
            Group::Logs(resource) => vec![
                Logs::new(client.clone(), filter.clone(), LogGroup::Current).into(),
                Logs::new(client.clone(), filter.clone(), LogGroup::Previous).into(),
                Collectable::new(client, resource, filter).into(),
            ],
            Group::Events(resource) => vec![
                Events::new(client.clone(), filter.clone()).into(),
                Collectable::new(client, resource, filter).into(),
            ],
            Group::DynamicObject(resource) => {
                vec![Collectable::new(client, resource, filter).into()]
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use tempdir::TempDir;

    use crate::{
        filters::namespace::NamespaceInclude, tests::kwok, gather::writer::{Encoding, Archive},
    };

    use super::*;

    #[tokio::test]
    async fn test_gzip_collect() {
        let test_env = kwok::TestEnvBuilder::default()
            .insecure_skip_tls_verify(true)
            .build();

        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let file_path = tmp_dir.path().join("crust-gather-test.tar.gz");
        let mut config = GatherConfig {
            client: test_env.client().await,
            filter: Arc::new(List(vec![NamespaceInclude::try_from("default".to_string())
                .unwrap()
                .into()])),
            writer: Writer::new(&Archive::new(file_path), &Encoding::Gzip)
                .expect("failed to create builder"),
            secrets: vec![],
        };

        let result = config.collect().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_zip_collect() {
        let test_env = kwok::TestEnvBuilder::default()
            .insecure_skip_tls_verify(true)
            .build();

        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let file_path = tmp_dir.path().join("crust-gather-test.zip");
        let mut config = GatherConfig {
            client: test_env.client().await,
            filter: Arc::new(List(vec![])),
            writer: Writer::new(&Archive::new(file_path), &Encoding::Zip)
                .expect("failed to create builder"),
            secrets: vec![],
        };

        let result = config.collect().await;
        assert!(result.is_ok());
    }
}
