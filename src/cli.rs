use std::{env, fs::File, sync::Arc};

use clap::{arg, command, ArgAction, Parser, Subcommand};
use k8s_openapi::serde::Deserialize;
use kube::Client;

use crate::{
    filters::{
        filter::{FilterType, List},
        group::{GroupExclude, GroupInclude},
        kind::{KindExclude, KindInclude},
        namespace::{NamespaceExclude, NamespaceInclude},
    },
    gather::{
        config::GatherConfig,
        writer::{Archive, Encoding, KubeconfigFile, Writer},
    },
};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Controls the verbosity of the output.
    ///
    /// OneOf: OFF, ERROR, WARN, INFO, DEBUG, TRACE
    #[arg(short, long, default_value = "INFO", global = true)]
    verbosity: log::LevelFilter,

    #[command(subcommand)]
    pub command: Commands,
}

impl Cli {
    pub fn init(&self) {
        env_logger::builder().filter_level(self.verbosity).init()
    }
}

#[derive(Subcommand, Clone)]
pub enum Commands {
    /// Collect resources from the Kubernetes cluster using the provided
    /// configuration options like namespaces, kinds, etc to include/exclude.
    Collect {
        #[command(flatten)]
        config: GatherCommands,
    },
    /// Parse the gather configuration from a file.
    CollectFromConfig {
        /// Parse the gather configuration from a file.
        ///
        /// Example file:
        // ```yaml
        /// include_namespace:
        /// - default
        /// include_kind:
        /// - Pod
        /// secret:
        /// - FOO
        /// - BAR
        /// kubeconfig: ~/.kube/my_kubeconfig
        /// ```
        #[arg(short, long,
            value_parser = |arg: &str| -> anyhow::Result<GatherCommands> {Ok(GatherCommands::try_from(arg.to_string())?)},)]
        config: GatherCommands,
    },
}

#[derive(Parser, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatherCommands {
    /// Namespace to include in the resource collection.
    ///
    /// By default all cluster namespaces are getting collected.
    /// The namespace argument accept namespace name as a regular string or as a regex string.
    ///
    /// --include-namespace specified multiple times allows to include multiple namespaces.
    ///
    /// Example:
    ///     --include-namespace=default --include-namespace=kube-.*
    #[arg(long, value_name = "NAMESPACE",
            value_parser = |arg: &str| -> anyhow::Result<NamespaceInclude> {Ok(NamespaceInclude::try_from(arg.to_string())?)},
            action = ArgAction::Append )]
    #[serde(default)]
    include_namespace: Vec<NamespaceInclude>,

    /// Namespace to exclude from the resource collection.
    ///
    /// By default all cluster namespaces are getting collected.
    /// The namespace argument accept namespace name as a regular string or as a regex string.
    ///
    /// --exclude-namespace specified multiple times allows to exclude multiple namespaces.
    ///
    /// Example:
    ///     --exclude-namespace=default --exclude-namespace=kube-.*
    #[arg(long, value_name = "NAMESPACE",
            value_parser = |arg: &str| -> anyhow::Result<NamespaceExclude> {Ok(NamespaceExclude::try_from(arg.to_string())?)},
            action = ArgAction::Append )]
    #[serde(default)]
    exclude_namespace: Vec<NamespaceExclude>,

    /// Resource kind to include in the resource collection.
    ///
    /// By default all cluster resource kinds are getting collected.
    /// Kind can be specified as a regular string or as a regex string.
    ///
    /// --include-kind specified multiple times allows to include multiple kinds.
    ///
    /// Example:
    ///     --include-kind=Pod --include-kind=Deployment|ReplicaSet
    #[arg(long, value_name = "KIND",
            value_parser = |arg: &str| -> anyhow::Result<KindInclude> {Ok(KindInclude::try_from(arg.to_string())?)},
            action = ArgAction::Append )]
    #[serde(default)]
    include_kind: Vec<KindInclude>,

    /// Resource kind to exclude from the resource collection.
    ///
    /// By default all cluster resource kinds are getting collected.
    /// Kind can be specified as a regular string or as a regex string.
    ///
    /// --exclude-kind specified multiple times allows to exclude multiple kinds.
    ///
    /// Example:
    ///     --exclude-kind=Pod --exclude-kind=Deployment|ReplicaSet
    #[arg(long, value_name = "KIND",
            value_parser = |arg: &str| -> anyhow::Result<KindExclude> {Ok(KindExclude::try_from(arg.to_string())?)},
            action = ArgAction::Append )]
    #[serde(default)]
    exclude_kind: Vec<KindExclude>,

    /// API group/kind to include in the resource collection.
    ///
    /// By default all cluster resource groups and kinds are getting collected.
    /// Either group or group with kind could be specified, separated by '/': <group>/<kind>
    ///
    /// --include-group specified multiple times to include multiple group/kinds.
    /// Both <group> and <kind> could are separate strings, or regex strings.
    ///
    /// Example:
    ///     --include-group=/Node
    ///     --include-group=apps/Deployment|ReplicaSet
    #[arg(long, value_name = "GROUP_KIND",
            value_parser = |arg: &str| -> anyhow::Result<GroupInclude> {Ok(GroupInclude::try_from(arg.to_string())?)},
            action = ArgAction::Append )]
    #[serde(default)]
    include_group: Vec<GroupInclude>,

    /// API groups/kind to exclude from the resource collection.
    ///
    /// By default all cluster resource groups and kinds are getting collected.
    /// Either group or group with kind could be specified, separated by '/': <group>/<kind>
    ///
    /// --exclude-group specified multiple times to exclude multiple group/kinds.
    /// Both <group> and <kind> could are separate strings, or regex strings.
    ///
    /// Example:
    ///     --exclude-group=/Node
    ///     --exclude-group=apps/Deployment|ReplicaSet
    #[arg(long, value_name = "GROUP_KIND",
            value_parser = |arg: &str| -> anyhow::Result<GroupExclude> {Ok(GroupExclude::try_from(arg.to_string())?)},
            action = ArgAction::Append )]
    #[serde(default)]
    exclude_group: Vec<GroupExclude>,

    /// Path to a kubeconfig file.
    /// If not provided, will attempt to use the default config for the environemnt.
    ///
    /// Example:
    ///     --kubeconfig=./kubeconfig
    #[arg(short, long, value_name = "PATH",
        value_parser = |arg: &str| -> anyhow::Result<KubeconfigFile> {Ok(KubeconfigFile::try_from(arg.to_string())?)})]
    kubeconfig: Option<KubeconfigFile>,

    /// The output file path.
    /// Defaults to a new archive with name "crust-gather".
    ///
    /// Example:
    ///     --file=./artifacts
    #[arg(short, long, value_name = "PATH", default_value_t = Default::default())]
    #[serde(default)]
    file: Archive,

    /// Encoding for the output file.
    /// The default encoding is GZip.
    /// The available options are:
    /// - gzip: GZip encoded tar.
    /// - zip: ZIP encoded.
    ///
    /// Example:
    ///     --encoding=zip
    #[arg(short, long)]
    #[serde(default)]
    encoding: Encoding,

    /// Secret environment variable name with data to exclude in the collected artifacts.
    /// Can be specified multiple times to exclude multiple values.
    ///
    /// Example:
    ///     --secret=MY_ENV_SECRET_DATA --secret=SOME_OTHER_SECRET_DATA
    #[arg(short, long = "secret", action = ArgAction::Append)]
    #[serde(default)]
    secrets: Vec<String>,
}

impl Into<GatherCommands> for &Commands {
    fn into(self) -> GatherCommands {
        match self {
            Commands::Collect { config } => config.clone(),
            Commands::CollectFromConfig { config } => config.clone(),
        }
    }
}

impl TryFrom<String> for GatherCommands {
    type Error = anyhow::Error;

    fn try_from(file: String) -> Result<Self, Self::Error> {
        Ok(serde_yaml::from_reader(File::open(file)?)?)
    }
}

impl GatherCommands {
    pub async fn load(&self) -> anyhow::Result<GatherConfig> {
        log::info!("Initializing client");

        Ok(GatherConfig {
            client: self.client().await?,
            filter: self.into(),
            writer: self.try_into()?,
            secrets: self.secrets(),
        })
    }

    /// Gets a list of secret environment variable values to exclude from the collected artifacts.
    fn secrets(&self) -> Vec<String> {
        self.secrets
            .iter()
            .map(|s| env::var(s).unwrap_or_default())
            .collect()
    }

    async fn client(&self) -> anyhow::Result<Client> {
        Ok(match &self.kubeconfig {
            Some(kubeconfig) => kubeconfig.client().await?,
            None => Client::try_default().await?,
        })
    }
}

impl Commands {
    pub async fn load(&self) -> anyhow::Result<GatherConfig> {
        Ok(Into::<GatherCommands>::into(self).load().await?)
    }
}

impl TryInto<Writer> for &GatherCommands {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Writer, Self::Error> {
        Writer::new(&self.file, &self.encoding)
    }
}

impl From<&GatherCommands> for Arc<List> {
    fn from(val: &GatherCommands) -> Self {
        Arc::new(List({
            let data: Vec<Vec<FilterType>> = vec![
                val.include_namespace
                    .iter()
                    .map(Clone::clone)
                    .map(Into::into)
                    .collect(),
                val.exclude_namespace
                    .iter()
                    .map(Clone::clone)
                    .map(Into::into)
                    .collect(),
                val.include_kind
                    .iter()
                    .map(Clone::clone)
                    .map(Into::into)
                    .collect(),
                val.exclude_kind
                    .iter()
                    .map(Clone::clone)
                    .map(Into::into)
                    .collect(),
                val.include_group
                    .iter()
                    .map(Clone::clone)
                    .map(Into::into)
                    .collect(),
                val.exclude_group
                    .iter()
                    .map(Clone::clone)
                    .map(Into::into)
                    .collect(),
            ];

            data.iter()
                .flatten()
                .map(Clone::clone)
                .map(Into::into)
                .collect()
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use k8s_openapi::api::core::v1::Namespace;
    use kube::Api;
    use kube_core::params::ListParams;
    use tempdir::TempDir;
    use tokio::fs;

    use crate::tests::kwok;

    use super::*;

    #[test]
    fn test_secrets_empty() {
        let commands = GatherCommands::default();
        let secrets = commands.secrets();

        assert!(secrets.is_empty());
    }

    #[test]
    fn test_secrets_populated() {
        env::set_var("FOO", "foo");
        env::set_var("BAR", "bar");

        let commands = GatherCommands {
            secrets: vec!["FOO".into(), "BAR".into()],
            ..Default::default()
        };

        let secrets = commands.secrets();

        assert_eq!(secrets, vec!["foo", "bar"]);
    }

    #[tokio::test]
    async fn test_client_from_kubeconfig() {
        let test_env = kwok::TestEnvBuilder::default()
            .insecure_skip_tls_verify(true)
            .build();

        let kubeconfig = serde_yaml::to_string(&test_env.kubeconfig()).unwrap();
        fs::write(test_env.kubeconfig_path(), kubeconfig)
            .await
            .unwrap();

        let kubeconfig =
            KubeconfigFile::try_from(test_env.kubeconfig_path().to_str().unwrap().to_string())
                .unwrap();

        let commands = GatherCommands {
            kubeconfig: Some(kubeconfig),
            ..Default::default()
        };

        let client = commands.client().await.unwrap();

        let ns_api: Api<Namespace> = Api::all(client);
        ns_api.list(&ListParams::default()).await.unwrap();
    }

    #[tokio::test]
    async fn test_client_from_default() {
        let test_env = kwok::TestEnvBuilder::default()
            .insecure_skip_tls_verify(true)
            .build();

        env::set_var("KUBECONFIG", test_env.kubeconfig_path().to_str().unwrap());

        let kubeconfig = serde_yaml::to_string(&test_env.kubeconfig()).unwrap();
        fs::write(test_env.kubeconfig_path(), kubeconfig)
            .await
            .unwrap();

        let commands = GatherCommands::default();
        let client = commands.client().await.unwrap();

        let ns_api: Api<Namespace> = Api::all(client);
        ns_api.list(&ListParams::default()).await.unwrap();
    }

    #[test]
    fn test_from_str() {
        let tmp_dir = TempDir::new("config").expect("failed to create temp dir");

        let mut valid = File::create(tmp_dir.path().join("valid.yaml")).unwrap();
        let valid_config = r#"
        include_namespace:
            - default
            - kube-system
        "#;
        valid.write_all(valid_config.as_bytes()).unwrap();

        let mut invalid = File::create(tmp_dir.path().join("invalid.yaml")).unwrap();
        let invalid_config = r#"
        include_namespace:
            - default
            - kube-system
        something: unknown
        "#;
        invalid.write_all(invalid_config.as_bytes()).unwrap();

        let result = GatherCommands::try_from(
            tmp_dir
                .path()
                .join("valid.yaml")
                .to_str()
                .unwrap()
                .to_string(),
        );
        assert!(result.is_ok());
        let result = &result.unwrap();
        assert_eq!(result.include_namespace.len(), 2);

        let result = GatherCommands::try_from(
            tmp_dir
                .path()
                .join("invalid.yaml")
                .to_str()
                .unwrap()
                .to_string(),
        );
        assert!(result.is_err());
    }
}
