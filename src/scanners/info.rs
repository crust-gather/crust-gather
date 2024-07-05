use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use http::Request;
use k8s_openapi::{api::core::v1::Node, chrono::Utc};
use kube::core::{ApiResource, TypeMeta};
use kube::Api;

use crate::gather::{
    config::{Config, Secrets},
    representation::{ArchivePath, Representation},
    writer::Writer,
};

use super::{interface::Collect, objects::Objects};

#[derive(Clone, Debug)]
pub struct Info {
    pub collectable: Objects<Node>,
}

impl Info {
    pub fn new(config: Config) -> Self {
        Self {
            collectable: Objects::new_typed(config, ApiResource::erase::<Node>(&())),
        }
    }
}

#[async_trait]
impl Collect<Node> for Info {
    fn get_secrets(&self) -> Secrets {
        Secrets::default()
    }

    fn get_writer(&self) -> Arc<Mutex<Writer>> {
        self.collectable.get_writer()
    }

    fn filter(&self, _: &Node) -> anyhow::Result<bool> {
        Ok(true)
    }

    async fn collect(&self) -> anyhow::Result<()> {
        let c = self.get_api().into_client();

        let version = serde_yaml::to_string(&c.apiserver_version().await?)?;
        let api_versions = c
            .request_text(
                Request::builder()
                    .uri("/api")
                    .header(
                        "Accept",
                        "application/json;g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList",
                    )
                    .body(vec![])?,
            )
            .await?;
        let apis_versions = c
            .request_text(
                Request::builder()
                    .uri("/apis")
                    .header(
                        "Accept",
                        "application/json;g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList",
                    )
                    .body(vec![])?,
            )
            .await?;

        let reprs = vec![
            Representation::new()
                .with_path(ArchivePath::Custom("version.yaml".into()))
                .with_data(version.as_str()),
            Representation::new()
                .with_path(ArchivePath::Custom("api.json".into()))
                .with_data(api_versions.as_str()),
            Representation::new()
                .with_path(ArchivePath::Custom("apis.json".into()))
                .with_data(apis_versions.as_str()),
            Representation::new()
                .with_path(ArchivePath::Custom("collected.timestamp".into()))
                .with_data(&Utc::now().to_string()),
        ];

        for repr in reprs {
            self.get_writer().lock().unwrap().store(&repr)?;
        }

        Ok(())
    }

    fn get_api(&self) -> Api<Node> {
        self.collectable.get_api()
    }

    fn get_type_meta(&self) -> TypeMeta {
        self.collectable.get_type_meta()
    }
}
