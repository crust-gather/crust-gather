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

use crate::filters::filter::FilterGroup;
use crate::scanners::dynamic::Dynamic;
use crate::scanners::events::Events;
use crate::scanners::info::Info;
use crate::scanners::interface::Collect;
use crate::scanners::logs::{LogSelection, Logs};
use crate::scanners::nodes::Nodes;

use super::representation::Representation;
use super::writer::Writer;

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
        Self(
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
        Ok(Self(match DurationString::try_from(value) {
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
    pub filter: Arc<FilterGroup>,
    pub writer: Arc<Mutex<Writer>>,
    pub secrets: Secrets,
    duration: RunDuration,
}

impl Config {
    pub fn new(
        client: Client,
        filter: FilterGroup,
        writer: Writer,
        secrets: Secrets,
        duration: RunDuration,
    ) -> Self {
        Self {
            client,
            filter: Arc::new(filter),
            secrets,
            duration,
            writer: writer.into(),
        }
    }

    /// Collect representations for resources from discovery to the specified archive file.
    pub async fn collect(&self) -> anyhow::Result<()> {
        log::info!("Collecting resources...");

        match timeout(
            self.duration.0.into(),
            self.iterate_until_completion(
                discovery::Discovery::new(self.client.clone())
                    .run()
                    .await?
                    .groups()
                    .flat_map(kube::discovery::ApiGroup::recommended_resources)
                    .filter_map(|r| r.1.supports_operation(LIST).then_some(r.0.into()))
                    .flat_map(|group: Group| group.into_collectable(self.clone()))
                    .collect(),
            ),
        )
        .await
        {
            Ok(()) => (),
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
    Dynamic(ApiResource),
}

impl From<ApiResource> for Group {
    fn from(val: ApiResource) -> Self {
        match val {
            r if r == ApiResource::erase::<Event>(&()) => Self::Events(r),
            r if r == ApiResource::erase::<Pod>(&()) => Self::Logs(r),
            r if r == ApiResource::erase::<Node>(&()) => Self::Nodes(r),
            r => Self::Dynamic(r),
        }
    }
}

#[derive(Debug, Clone)]
enum Collectable {
    Dynamic(Dynamic),
    Logs(Logs),
    Events(Events),
    Nodes(Nodes),
    Info(Info),
}

impl Collectable {
    async fn collect(&self) {
        match self {
            Self::Dynamic(o) => o.collect_retry(),
            Self::Logs(l) => l.collect_retry(),
            Self::Events(e) => e.collect_retry(),
            Self::Nodes(n) => n.collect_retry(),
            Self::Info(i) => i.collect_retry(),
        }
        .await;
    }
}

impl Group {
    fn into_collectable(self, gather: Config) -> Vec<Collectable> {
        match self {
            Self::Nodes(resource) => vec![
                Collectable::Nodes(Nodes::from(gather.clone())),
                Collectable::Info(Info::new(gather.clone())),
                Collectable::Dynamic(Dynamic::new(gather, resource)),
            ],
            Self::Logs(resource) => vec![
                Collectable::Logs(Logs::new(gather.clone(), LogSelection::Current)),
                Collectable::Logs(Logs::new(gather.clone(), LogSelection::Previous)),
                Collectable::Dynamic(Dynamic::new(gather, resource)),
            ],
            Self::Events(resource) => vec![
                Collectable::Events(Events::from(gather.clone())),
                Collectable::Dynamic(Dynamic::new(gather, resource)),
            ],
            Self::Dynamic(resource) => {
                vec![Collectable::Dynamic(Dynamic::new(gather, resource))]
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use serial_test::serial;
    use tempdir::TempDir;

    use crate::{
        filters::filter::FilterList,
        gather::writer::{Archive, Encoding},
        tests::kwok,
    };

    #[cfg(feature = "archive")]
    use crate::filters::namespace::NamespaceInclude;

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
    #[cfg(feature = "archive")]
    #[serial]
    async fn test_gzip_collect() {
        let test_env = kwok::TestEnvBuilder::default()
            .insecure_skip_tls_verify(true)
            .build();

        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let file_path = tmp_dir.path().join("crust-gather-test.zip");
        let f = NamespaceInclude::try_from("default".to_string()).unwrap();
        let config = Config {
            client: test_env.client().await,
            filter: Arc::new(FilterGroup(vec![FilterList(vec![vec![f].into()])])),
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
    #[cfg(feature = "archive")]
    #[serial]
    async fn test_zip_collect() {
        let test_env = kwok::TestEnvBuilder::default()
            .insecure_skip_tls_verify(true)
            .build();

        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let file_path = tmp_dir.path().join("crust-gather-test.tar.gz");
        let config = Config {
            client: test_env.client().await,
            filter: Arc::new(FilterGroup(vec![FilterList(vec![])])),
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
            filter: Arc::new(FilterGroup(vec![FilterList(vec![])])),
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
