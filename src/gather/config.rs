use std::env;
use std::fmt::Display;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{self, bail};
use duration_string::DurationString;
use futures::future::join_all;
use k8s_openapi::api::core::v1::{Event, Node, Pod};
use kube::{discovery, Client};
use kube_core::discovery::verbs::LIST;
use kube_core::ApiResource;
use serde::Deserialize;
use tokio::time::timeout;

use crate::filters::filter::List;
use crate::scanners::events::Events;
use crate::scanners::generic::Object;
use crate::scanners::interface::Collect;
use crate::scanners::logs::{LogGroup, Logs};
use crate::scanners::nodes::Nodes;

use super::writer::{Representation, Writer};

#[derive(Default, Clone)]
pub struct Secrets(pub Vec<String>);

impl Secrets {
    /// Replaces any secrets in representation data with ***.
    pub fn strip(&self, repr: &Representation) -> Representation {
        let mut data = repr.data().to_string();
        for secret in &self.0 {
            data = data.replace(secret.as_str(), "***");
        }

        repr.clone().with_data(data.as_str())
    }
}

impl From<Vec<String>> for Secrets {
    /// Gets a list of secret environment variable values to exclude from the collected artifacts.
    fn from(val: Vec<String>) -> Self {
        Secrets(
            val.iter()
                .map(|s| env::var(s).unwrap_or_default())
                .filter(|s| !s.is_empty())
                .collect(),
        )
    }
}

#[derive(Clone, Deserialize, Copy)]
pub struct RunDuration(DurationString);

impl TryFrom<String> for RunDuration {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(RunDuration(match DurationString::try_from(value) {
            Ok(duration) => duration,
            Err(error) => bail!(error),
        }))
    }
}

impl Default for RunDuration {
    fn default() -> Self {
        Self(DurationString::from(Duration::new(60, 0)))
    }
}

impl Display for RunDuration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone)]
pub struct Config {
    pub client: Client,
    pub filter: Arc<List>,
    pub writer: Arc<Mutex<Writer>>,
    pub secrets: Secrets,
    duration: RunDuration,
}

impl Config {
    pub fn new(
        client: Client,
        filter: List,
        writer: Writer,
        secrets: Secrets,
        duration: RunDuration,
    ) -> Config {
        Config {
            client,
            filter: Arc::new(filter),
            secrets,
            duration,
            writer: writer.into(),
        }
    }
}

impl Config {
    /// Collect representations for resources from discovery to the specified archive file.
    pub async fn collect(&self) -> anyhow::Result<()> {
        log::info!("Collecting resources...");

        let collectables = discovery::Discovery::new(self.client.clone())
            .run()
            .await?
            .groups()
            .flat_map(|g| g.recommended_resources())
            .filter_map(|r| r.1.supports_operation(LIST).then_some(r.0.into()))
            .flat_map(|group: Group| group.into_collectable(self.clone()))
            .collect();

        match timeout(
            self.duration.0.into(),
            self.iterate_until_completion(collectables),
        )
        .await
        {
            Ok(_) => (),
            Err(e) => log::error!("{e}"),
        }

        self.writer.lock().unwrap().finish()
    }

    async fn iterate_until_completion(&self, collectables: Vec<Collectable>) {
        join_all(collectables.iter().map(|c| async { c.collect().await })).await;
    }
}

enum Group {
    Nodes(ApiResource),
    Logs(ApiResource),
    Events(ApiResource),
    DynamicObject(ApiResource),
}

impl From<ApiResource> for Group {
    fn from(val: ApiResource) -> Self {
        match val {
            r if r == ApiResource::erase::<Event>(&()) => Group::Events(r),
            r if r == ApiResource::erase::<Pod>(&()) => Group::Logs(r),
            r if r == ApiResource::erase::<Node>(&()) => Group::Nodes(r),
            r => Group::DynamicObject(r),
        }
    }
}

#[derive(Debug, Clone)]
enum Collectable {
    Object(Object),
    Logs(Logs),
    Events(Events),
    Nodes(Nodes),
}

impl Collectable {
    async fn collect(&self) {
        match self {
            Collectable::Object(o) => o.collect_retry(),
            Collectable::Logs(l) => l.collect_retry(),
            Collectable::Events(e) => e.collect_retry(),
            Collectable::Nodes(n) => n.collect_retry(),
        }
        .await
    }
}

impl Group {
    fn into_collectable(self, gather: Config) -> Vec<Collectable> {
        match self {
            Group::Nodes(resource) => vec![
                Collectable::Nodes(Nodes::from(gather.clone())),
                Collectable::Object(Object::new(gather, resource)),
            ],
            Group::Logs(resource) => vec![
                Collectable::Logs(Logs::new(gather.clone(), LogGroup::Current)),
                Collectable::Logs(Logs::new(gather.clone(), LogGroup::Previous)),
                Collectable::Object(Object::new(gather, resource)),
            ],
            Group::Events(resource) => vec![
                Collectable::Events(Events::from(gather.clone())),
                Collectable::Object(Object::new(gather, resource)),
            ],
            Group::DynamicObject(resource) => {
                vec![Collectable::Object(Object::new(gather, resource))]
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use serial_test::serial;
    use tempdir::TempDir;

    use crate::{
        filters::namespace::NamespaceInclude,
        gather::writer::{Archive, Encoding},
        tests::kwok,
    };

    use super::*;

    #[test]
    fn test_secrets_empty() {
        let secrets: Secrets = vec![].into();

        assert!(secrets.0.is_empty());
    }

    #[test]
    fn test_secrets_populated() {
        env::set_var("FOO", "foo");
        env::set_var("BAR", "bar");

        let secrets: Secrets = vec!["FOO".into(), "BAR".into(), "OTHER".into()].into();

        assert_eq!(secrets.0, vec!["foo", "bar"]);
    }

    #[test]
    fn test_strip_secrets() {
        env::set_var("KEY", "password");

        let data = "omit password string".to_string();
        let secrets: Secrets = vec!["KEY".to_string()].into();
        let result = secrets.strip(&Representation::new().with_data(data.as_str()));

        assert_eq!(result.data(), "omit *** string");
    }

    #[tokio::test]
    #[serial]
    async fn test_gzip_collect() {
        let test_env = kwok::TestEnvBuilder::default()
            .insecure_skip_tls_verify(true)
            .build();

        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let file_path = tmp_dir.path().join("crust-gather-test.zip");
        let config = Config {
            client: test_env.client().await,
            filter: Arc::new(List(vec![NamespaceInclude::try_from(
                "default".to_string(),
            )
            .unwrap()
            .into()])),
            writer: Writer::new(&Archive::new(file_path), &Encoding::Zip)
                .expect("failed to create builder")
                .into(),
            secrets: Default::default(),
            duration: "1m".to_string().try_into().unwrap(),
        };

        let result = config.collect().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[serial]
    async fn test_zip_collect() {
        let test_env = kwok::TestEnvBuilder::default()
            .insecure_skip_tls_verify(true)
            .build();

        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let file_path = tmp_dir.path().join("crust-gather-test.tar.gz");
        let config = Config {
            client: test_env.client().await,
            filter: Arc::new(List(vec![])),
            writer: Writer::new(&Archive::new(file_path), &Encoding::Gzip)
                .expect("failed to create builder")
                .into(),
            duration: "1m".to_string().try_into().unwrap(),
            secrets: Default::default(),
        };

        let result = config.collect().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[serial]
    async fn test_path_collect() {
        let test_env = kwok::TestEnvBuilder::default()
            .insecure_skip_tls_verify(true)
            .build();

        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let file_path = tmp_dir.path().join("crust-gather-test");
        let config = Config {
            client: test_env.client().await,
            filter: Arc::new(List(vec![])),
            writer: Writer::new(&Archive::new(file_path), &Encoding::Path)
                .expect("failed to create builder")
                .into(),
            duration: "1m".to_string().try_into().unwrap(),
            secrets: Default::default(),
        };

        let result = config.collect().await;
        assert!(result.is_ok());
    }
}
