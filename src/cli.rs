use std::fs::File;

use anyhow::anyhow;
use clap::{ArgAction, Parser, Subcommand, arg, command};
use k8s_openapi::serde::Deserialize;
use kube::{Client, config::Kubeconfig};
use tracing::level_filters::LevelFilter;

use crate::{
    filters::{
        filter::{FilterGroup, FilterList, FilterType},
        group::{GroupExclude, GroupInclude},
        kind::{KindExclude, KindInclude},
        log::UserLog,
        namespace::{NamespaceExclude, NamespaceInclude},
    },
    gather::{
        config::{
            Config, ConfigFromConfigMap, GatherMode, KubeconfigFile, KubeconfigSecretLabel,
            KubeconfigSecretNamespaceName, RunDuration, Secrets, SecretsFile,
        },
        server::Server,
        writer::{Archive, Encoding, Writer},
    },
};

use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt as _;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Controls the verbosity of the output.
    ///
    /// OneOf: OFF, ERROR, WARN, INFO, DEBUG, TRACE
    #[arg(short, long, default_value = "INFO", global = true)]
    verbosity: LevelFilter,

    #[command(subcommand)]
    pub command: Commands,
}

impl Cli {
    pub fn init(&self) {
        tracing_subscriber::registry()
            .with(fmt::layer())
            .with(EnvFilter::from_default_env().add_directive(self.verbosity.into()))
            .init();
    }
}

#[derive(Subcommand, Clone)]
pub enum Commands {
    /// Collect resources from the Kubernetes cluster using the provided
    /// filtering options like namespaces, kinds, etc to include/exclude.
    Collect {
        #[command(flatten)]
        config: GatherCommands,
    },

    /// Collect resources from the Kubernetes cluster using the provided config file
    CollectFromConfig {
        #[command(flatten)]
        source: ConfigSource,

        #[command(flatten)]
        overrides: GatherSettings,
    },

    /// Record resource changes over time from the Kubernetes cluster using the provided
    /// filtering options like namespaces, kinds, etc to include/exclude.
    Record {
        #[command(flatten)]
        config: GatherCommands,
    },

    /// Record resource changes over time from the Kubernetes cluster using the provided config file
    RecordFromConfig {
        #[command(flatten)]
        source: ConfigSource,

        #[command(flatten)]
        overrides: GatherSettings,
    },

    /// Start the API server on the archive.
    Serve {
        /// Start the API server with the provided configuration.
        #[command(flatten)]
        serve: Server,
    },
}

impl Commands {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            Commands::Collect { config } => {
                Into::<GatherCommands>::into(config)
                    .load()
                    .await?
                    .collect()
                    .await
            }
            Commands::CollectFromConfig { source, overrides } => {
                source
                    .gather(overrides.origin_client().await?)
                    .await?
                    .merge(overrides)
                    .load()
                    .await?
                    .collect()
                    .await
            }
            Commands::Serve { serve } => serve.get_api()?.serve().await.map_err(|e| anyhow!(e)),
            Commands::Record { config } => {
                let config = GatherCommands {
                    mode: GatherMode::Record,
                    ..config
                };
                Into::<GatherCommands>::into(config)
                    .load()
                    .await?
                    .collect()
                    .await
            }
            Commands::RecordFromConfig { source, overrides } => {
                let config = source
                    .gather(overrides.origin_client().await?)
                    .await?
                    .merge(overrides);
                let config = GatherCommands {
                    mode: GatherMode::Record,
                    ..config
                };
                config.load().await?.collect().await
            }
        }
    }
}

#[derive(Parser, Clone, Deserialize)]
#[group(required = true, multiple = false)]
#[serde(deny_unknown_fields)]
pub struct ConfigSource {
    /// Parse the gather configuration from a file.
    ///
    /// Example file:
    ///
    /// filters:
    /// - include_namespace:
    ///   - default
    ///   include_kind:
    ///   - Pod
    /// settings:
    ///   secret:
    ///   - FOO
    ///   - BAR
    ///
    /// Example:
    ///     --config=config.yaml
    #[arg(short, long, verbatim_doc_comment,
            value_parser = |arg: &str| -> anyhow::Result<GatherCommands> {Ok(GatherCommands::try_from(arg.to_string())?)},)]
    config: Option<GatherCommands>,

    /// Parse the gather configuration from an in-cluster config map specified by a name.
    ///
    /// Example content:
    ///
    /// apiVersion: v1
    /// kind: ConfigMap
    /// metadata:
    ///   name: crust-gather-config
    /// data:
    ///   config: |
    ///     filters:
    ///     - include_namespace:
    ///       - default
    ///       include_kind:
    ///       - Pod
    ///     settings:
    ///       secret:
    ///       - FOO
    ///       - BAR
    ///
    /// Example:
    ///     --config-map=crust-gather-config
    #[arg(long, verbatim_doc_comment)]
    config_map: Option<ConfigFromConfigMap>,
}

impl ConfigSource {
    pub async fn gather(&self, client: Client) -> anyhow::Result<GatherCommands> {
        match self {
            Self {
                config: Some(config),
                ..
            } => Ok(config.clone()),
            Self {
                config_map: Some(config_map),
                ..
            } => config_map.get_config(client).await,
            Self { config: None, .. } => unreachable!("No config source is not allowed by parser"),
        }
    }
}

#[derive(Parser, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatherCommands {
    #[serde(default)]
    #[clap(skip)]
    mode: GatherMode,

    #[command(flatten)]
    #[serde(flatten)]
    #[serde(default)]
    additional_logs: AdditionalLogs,

    #[serde(default)]
    #[clap(skip)]
    filters: Vec<Filters>,

    #[command(flatten)]
    #[serde(default)]
    filter: Option<Filters>,

    #[command(flatten)]
    #[serde(default)]
    settings: GatherSettings,
}

impl GatherCommands {
    pub fn merge(&self, other: GatherSettings) -> Self {
        Self {
            mode: self.mode.clone(),
            filter: self.filter.clone(),
            filters: self.filters.clone(),
            settings: self.settings.merge(other),
            additional_logs: self.additional_logs.clone(),
        }
    }
}

impl GatherSettings {
    pub fn merge(&self, other: Self) -> Self {
        Self {
            kubeconfig: other.kubeconfig.or(self.kubeconfig.clone()),
            kubeconfig_secret: other.kubeconfig_secret.or(self.kubeconfig_secret.clone()),
            insecure_skip_tls_verify: other
                .insecure_skip_tls_verify
                .or(self.insecure_skip_tls_verify),
            file: other.file.or(self.file.clone()),
            encoding: other.encoding.or(self.encoding.clone()),
            secrets: if other.secrets.is_empty() {
                self.secrets.clone()
            } else {
                other.secrets
            },
            secrets_file: other.secrets_file.or(self.secrets_file.clone()),
            duration: other.duration.or(self.duration),
            systemd_units: if other.systemd_units.is_empty() {
                self.systemd_units.clone()
            } else {
                other.systemd_units
            },
            debug_pod: self.debug_pod.merge(other.debug_pod),
        }
    }
}

#[derive(Parser, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatherSettings {
    /// Path to a kubeconfig file.
    /// If not provided, will attempt to use the default config for the environemnt.
    ///
    /// Example:
    ///     --kubeconfig=./kubeconfig
    #[arg(short, long, value_name = "PATH",
        value_parser = |arg: &str| -> anyhow::Result<KubeconfigFile> {Ok(KubeconfigFile::try_from(arg.to_string())?)})]
    kubeconfig: Option<KubeconfigFile>,

    /// Collect kubeconfig from a secret.
    #[command(flatten)]
    kubeconfig_secret: Option<KubeconfigFromSecret>,

    /// Pass an insecure flag to kubeconfig file.
    /// If not provided, defaults to false and kubeconfig will be uses as-is.
    ///
    /// Example:
    ///     --insecure-skip-tls-verify
    #[arg(short, long)]
    #[serde(default)]
    insecure_skip_tls_verify: Option<bool>,

    /// The output file path.
    /// Defaults to a new archive with name "crust-gather".
    ///
    /// Example:
    ///     --file=./artifacts
    #[arg(short, long, value_name = "PATH")]
    #[serde(default)]
    file: Option<Archive>,

    /// Encoding for the output file.
    /// By default there is no encoding and data is written to the filesystem.
    /// The available options are:
    /// - gzip: GZip encoded tar.
    /// - zip: ZIP encoded.
    ///
    /// Example:
    ///     --encoding=zip
    #[arg(short, long, value_enum)]
    #[serde(default)]
    encoding: Option<Encoding>,

    /// Secret environment variable name with data to exclude in the collected artifacts.
    /// Can be specified multiple times to exclude multiple values.
    ///
    /// Example:
    ///     --secret=MY_ENV_SECRET_DATA --secret=SOME_OTHER_SECRET_DATA
    #[arg(short, long = "secret", action = ArgAction::Append)]
    #[serde(default)]
    secrets: Vec<String>,

    /// Secret file name with secret data to exclude in the collected artifacts.
    /// Can be supplied only once.
    ///
    /// Example content of secrets.txt:
    /// 10.244.0.0
    /// 172.18.0.3
    /// password123
    /// aws-access-key
    ///
    /// Example:
    ///     --secrets-file=secrets.txt
    #[arg(long = "secrets-file", value_name = "PATH", verbatim_doc_comment,
        value_parser = |arg: &str| -> anyhow::Result<SecretsFile> {Ok(SecretsFile::try_from(arg.to_string())?)})]
    #[serde(default)]
    secrets_file: Option<SecretsFile>,

    /// The duration to run the collection for.
    /// Defaults to 60 seconds.
    ///
    /// Example:
    ///     --duration=2m
    #[arg(short, long, value_name = "DURATION",
        value_parser = |arg: &str| -> anyhow::Result<RunDuration> {Ok(RunDuration::try_from(arg.to_string())?)})]
    #[serde(default)]
    duration: Option<RunDuration>,

    /// Name of the kubelet systemd unit.
    ///
    /// Defaults to kubelet.
    ///
    /// Example:
    ///     --systemd-unit=rke2-server
    ///     --systemd-unit=rke2-agent
    #[arg(long = "systemd-unit", value_name = "SYSTEMD_UNIT", default_value = "kubelet", action = ArgAction::Append )]
    #[serde(default)]
    systemd_units: Vec<String>,

    /// Collect settings to configure the pod which collect logs on nodes.
    #[command(flatten)]
    #[serde(default)]
    debug_pod: DebugPod,
}

impl GatherSettings {
    pub async fn client(&self) -> anyhow::Result<Client> {
        let origin = self.origin_client().await?;
        match &self.kubeconfig_secret {
            Some(secret) => {
                let kubeconfigs = secret.get_config(origin).await?;
                for kubeconfig in kubeconfigs {
                    match KubeconfigFile(kubeconfig)
                        .client(self.insecure_skip_tls_verify.unwrap_or_default())
                        .await
                    {
                        Ok(client) => return Ok(client),
                        Err(_) => continue,
                    };
                }

                Err(anyhow::anyhow!(
                    "No kubeconfig found matching selector {secret:?}",
                ))
            }
            None => Ok(origin),
        }
    }

    pub async fn origin_client(&self) -> anyhow::Result<Client> {
        tracing::info!("Initializing client...");

        match &self.kubeconfig {
            Some(kubeconfig) => {
                kubeconfig
                    .client(self.insecure_skip_tls_verify.unwrap_or_default())
                    .await
            }
            None => KubeconfigFile::infer(self.insecure_skip_tls_verify.unwrap_or_default()).await,
        }
    }
}

#[derive(Parser, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DebugPod {
    /// The image to use to collect logs on nodes.
    /// Defaults to busybox.
    ///
    /// Example:
    ///     --debug-pod-image=busybox:1.37.0
    #[arg(long("debug-pod-image"), value_name = "IMAGE")]
    #[serde(default)]
    pub image: Option<String>,
}

impl DebugPod {
    fn merge(&self, other: Self) -> Self {
        Self {
            image: other.image.or(self.image.clone()),
        }
    }
}

#[derive(Parser, Clone, Deserialize, Debug)]
#[group(required = false, multiple = false)]
#[serde(deny_unknown_fields)]
pub struct KubeconfigFromSecret {
    /// Collect kubeconfig from a first secret matching the label.
    /// Allows you to specify a label selector, and use the current cluster as a proxy to the
    /// one, where a snapshot will be collected.
    /// If not provided, --kubeconfig flag takes precedence.
    ///
    /// Example:
    ///     --kubeconfig-secret-label=cluster.x-k8s.io/cluster-name=docker
    #[arg(long, value_name = "key=value")]
    #[serde(default)]
    kubeconfig_secret_label: Option<KubeconfigSecretLabel>,

    /// Collect kubeconfig from a secret.
    /// Secret can be specified by namespace/name or just by name.
    /// If not provided, --kubeconfig flag takes precedence.
    ///
    /// Example:
    ///     --kubeconfig-secret-name=default/cluster-kubeconfig
    #[arg(long, value_name = "NAMESPACE/NAME")]
    #[serde(default)]
    kubeconfig_secret_name: Option<KubeconfigSecretNamespaceName>,
}

impl KubeconfigFromSecret {
    async fn get_config(&self, client: Client) -> anyhow::Result<Vec<Kubeconfig>> {
        let (label, name) = (&self.kubeconfig_secret_label, &self.kubeconfig_secret_name);
        match (label, name) {
            (None, None) => Ok(vec![]),
            (Some(label), _) => label.get_config(client).await,
            (_, Some(name)) => name.get_config(client).await,
        }
    }
}

#[derive(Parser, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdditionalLogs {
    /// Additional logs to include into collection.
    ///
    /// This follows similar pattern to kubelet logs collection. Provided command is executed in a privileged pod
    /// which allows to collect logs from the workloads running directly on the node, and store them in the specified
    /// file. In the CLI invocation the name of the file and command needs to be separated by ':'.
    ///
    /// --additional-logs specified multiple times allows to collect multiple logs files.
    ///
    /// Example:
    ///     --additional-logs="my-binary.log:sh -c cat /host/var/log/my-binary.log"
    #[arg(long, alias("additional-logs"), value_name = "FILE:COMMAND",
            value_parser = |arg: &str| -> anyhow::Result<UserLog> {Ok(UserLog::try_from(arg.to_string())?)},
            action = ArgAction::Append )]
    #[serde(default)]
    logs: Vec<UserLog>,

    /// Additional logs to include into collection.
    ///
    /// This follows similar pattern to kubelet logs collection. Provided command is executed in a privileged pod
    /// which allows to collect logs from the workloads running directly on the node, and store them in the specified
    /// file.
    ///
    /// Example config file:
    /// additional_logs:
    /// - name: test.txt
    ///   command: echo "hi"
    /// - name: test2.txt
    ///   command: echo "hi"
    #[clap(skip)]
    #[serde(default)]
    additional_logs: Vec<UserLog>,
}

#[derive(Parser, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Filters {
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
    #[arg(long, value_name = "GROUP_KIND", verbatim_doc_comment,
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
    #[arg(long, value_name = "GROUP_KIND", verbatim_doc_comment,
            value_parser = |arg: &str| -> anyhow::Result<GroupExclude> {Ok(GroupExclude::try_from(arg.to_string())?)},
            action = ArgAction::Append )]
    #[serde(default)]
    exclude_group: Vec<GroupExclude>,
}

impl TryFrom<String> for GatherCommands {
    type Error = anyhow::Error;

    fn try_from(file: String) -> Result<Self, Self::Error> {
        Ok(serde_yaml::from_reader(File::open(file)?)?)
    }
}

impl GatherCommands {
    pub async fn load(&self) -> anyhow::Result<Config> {
        let env_secrets: Secrets = self.settings.secrets.clone().into();
        let mut secrets: Secrets = match self.settings.secrets_file.clone() {
            Some(file) => file.clone().try_into()?,
            None => vec![].into(),
        };

        secrets.0.extend(env_secrets.0.into_iter());

        Ok(Config::new(
            self.client().await?,
            self.into(),
            (&self.settings).try_into()?,
            secrets,
            self.mode.clone(),
            self.additional_logs
                .logs
                .clone()
                .into_iter()
                .chain(self.additional_logs.additional_logs.clone().into_iter())
                .map(Into::into)
                .collect(),
            self.settings.duration.unwrap_or_default(),
            self.settings.systemd_units.clone(),
            self.settings.debug_pod.clone(),
        ))
    }

    async fn client(&self) -> anyhow::Result<Client> {
        self.settings.client().await
    }
}

impl TryInto<Writer> for &GatherSettings {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Writer, Self::Error> {
        Writer::new(
            &self.file.clone().unwrap_or_default(),
            self.encoding.as_ref().map_or(&Encoding::Path, |e| e),
        )
    }
}

impl From<&GatherCommands> for FilterGroup {
    fn from(val: &GatherCommands) -> FilterGroup {
        FilterGroup(match &val.filter {
            Some(filter) => vec![filter.into()],
            None => val.filters.iter().map(Into::into).collect(),
        })
    }
}

impl From<&Filters> for FilterList {
    fn from(filter: &Filters) -> Self {
        let data: Vec<FilterType> = vec![
            filter.include_namespace.clone().into(),
            filter.exclude_namespace.clone().into(),
            filter.include_kind.clone().into(),
            filter.exclude_kind.clone().into(),
            filter.include_group.clone().into(),
            filter.exclude_group.clone().into(),
        ];

        Self(data.iter().map(Clone::clone).collect())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::{collections::BTreeMap, env, io::Write};

    use k8s_openapi::api::core::v1::{ConfigMap, Namespace, Secret};
    use k8s_openapi::serde_json;

    use kube::core::{ObjectMeta, params::ListParams};
    use kube::{Api, api::PostParams};
    use serial_test::serial;
    use tempdir::TempDir;
    use tokio::fs;

    use super::*;

    fn temp_kubeconfig() -> PathBuf {
        let mut dir = env::temp_dir();
        dir.push(xid::new().to_string());
        dir
    }

    #[tokio::test]
    #[serial]
    async fn test_client_from_kubeconfig() {
        let test_env = envtest::Environment::default().create().expect("cluster");

        let config: Kubeconfig = test_env.kubeconfig().unwrap();
        let kubeconfig_path = temp_kubeconfig();
        fs::write(
            kubeconfig_path.clone(),
            serde_yaml::to_string(&config).unwrap(),
        )
        .await
        .unwrap();

        let kubeconfig =
            KubeconfigFile::try_from(kubeconfig_path.to_str().unwrap().to_string()).unwrap();

        let commands = GatherCommands {
            settings: GatherSettings {
                kubeconfig: Some(kubeconfig),
                ..Default::default()
            },
            ..Default::default()
        };

        let client = commands.client().await.unwrap();

        let ns_api: Api<Namespace> = Api::all(client);
        ns_api.list(&ListParams::default()).await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_insecure_client_from_kubeconfig() {
        let test_env = envtest::Environment::default().create().expect("cluster");

        let config: Kubeconfig = test_env.kubeconfig().unwrap();
        let kubeconfig_path = temp_kubeconfig();
        fs::write(
            kubeconfig_path.clone(),
            serde_yaml::to_string(&config).unwrap(),
        )
        .await
        .unwrap();

        let kubeconfig =
            KubeconfigFile::try_from(kubeconfig_path.to_str().unwrap().to_string()).unwrap();

        let commands = GatherCommands {
            settings: GatherSettings {
                insecure_skip_tls_verify: Some(true),
                kubeconfig: Some(kubeconfig),
                ..Default::default()
            },
            ..Default::default()
        };

        let client = commands.client().await.unwrap();

        let ns_api: Api<Namespace> = Api::all(client);
        ns_api.list(&ListParams::default()).await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_client_from_default() {
        let test_env = envtest::Environment::default().create().expect("cluster");

        let config: Kubeconfig = test_env.kubeconfig().unwrap();
        let kubeconfig_path = temp_kubeconfig();
        fs::write(
            kubeconfig_path.clone(),
            serde_yaml::to_string(&config).unwrap(),
        )
        .await
        .unwrap();

        unsafe {
            env::set_var("KUBECONFIG", kubeconfig_path.clone().to_str().unwrap());
        }

        let commands = GatherCommands::default();
        let client = commands.client().await.unwrap();

        let ns_api: Api<Namespace> = Api::all(client);
        ns_api.list(&ListParams::default()).await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_insecure_client_from_default() {
        let test_env = envtest::Environment::default().create().expect("cluster");
        let config = test_env.kubeconfig().unwrap();

        let commands = GatherCommands {
            settings: GatherSettings {
                insecure_skip_tls_verify: Some(true),
                kubeconfig: Some(KubeconfigFile(config)),
                ..Default::default()
            },
            ..GatherCommands::default()
        };
        let client = commands.client().await.unwrap();

        let ns_api: Api<Namespace> = Api::all(client);
        ns_api.list(&ListParams::default()).await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_collect() {
        let test_env = envtest::Environment::default().create().expect("cluster");
        let config = test_env.kubeconfig().unwrap();

        let tmp_dir = TempDir::new("collect").expect("failed to create temp dir");

        let commands = GatherCommands {
            settings: GatherSettings {
                insecure_skip_tls_verify: Some(true),
                file: Some(tmp_dir.path().join("collect").to_str().unwrap().into()),
                kubeconfig: Some(KubeconfigFile(config)),
                ..Default::default()
            },
            ..GatherCommands::default()
        };
        let config = commands.load().await.unwrap();

        assert!(config.collect().await.is_ok());
        assert!(tmp_dir.path().join("collect").join("cluster").is_dir());
        assert!(tmp_dir.path().join("collect").join("namespaces").is_dir());
        assert!(
            tmp_dir
                .path()
                .join("collect")
                .join("namespaces")
                .join("default")
                .is_dir()
        );
        assert!(
            tmp_dir
                .path()
                .join("collect")
                .join("namespaces")
                .join("kube-system")
                .is_dir()
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_collect_from_config() {
        let test_env = envtest::Environment::default().create().expect("cluster");
        let config = test_env.kubeconfig().unwrap();

        let tmp_dir = TempDir::new("config").expect("failed to create temp dir");

        let config_path = tmp_dir.path().join("valid.yaml");
        let mut valid = File::create(config_path.clone()).unwrap();
        let valid_config = r"
            filters:
            - include_namespace:
                - default
            ";
        valid.write_all(valid_config.as_bytes()).unwrap();

        let tmp_dir = TempDir::new("collect").expect("failed to create temp dir");

        let config_path = config_path.to_str();
        let commands = Commands::CollectFromConfig {
            source: ConfigSource {
                config: Some(GatherCommands::try_from(String::from(config_path.unwrap())).unwrap()),
                config_map: None,
            },
            overrides: GatherSettings {
                insecure_skip_tls_verify: Some(true),
                kubeconfig: Some(KubeconfigFile(config)),
                file: Some(tmp_dir.path().join("collect").to_str().unwrap().into()),
                ..Default::default()
            },
        };

        assert!(commands.run().await.is_ok());
        assert!(tmp_dir.path().join("collect").join("cluster").is_dir());
        assert!(tmp_dir.path().join("collect").join("namespaces").is_dir());
        assert!(
            tmp_dir
                .path()
                .join("collect")
                .join("namespaces")
                .join("default")
                .is_dir()
        );
        assert!(
            !tmp_dir
                .path()
                .join("collect")
                .join("namespaces")
                .join("kube-system")
                .is_dir()
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_collect_from_config_map() {
        let test_env = envtest::Environment::default().create().expect("cluster");
        let config: Kubeconfig = test_env.kubeconfig().unwrap();
        let client: Client = config.clone().try_into().unwrap();

        let valid_config = r"
            filters:
            - include_namespace:
                - default
            ";
        let cm_api: Api<ConfigMap> = Api::namespaced(client, "default");
        cm_api
            .create(
                &PostParams::default(),
                &ConfigMap {
                    metadata: ObjectMeta {
                        name: Some("crust-gather-config".into()),
                        ..Default::default()
                    },
                    data: Some(BTreeMap::from([("config".into(), valid_config.into())])),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let tmp_dir = TempDir::new("collect").expect("failed to create temp dir");

        let commands = Commands::CollectFromConfig {
            source: ConfigSource {
                config: None,
                config_map: Some(ConfigFromConfigMap("crust-gather-config".into())),
            },
            overrides: GatherSettings {
                insecure_skip_tls_verify: Some(true),
                kubeconfig: Some(KubeconfigFile(config)),
                file: Some(tmp_dir.path().join("collect").to_str().unwrap().into()),
                ..Default::default()
            },
        };

        assert!(commands.run().await.is_ok());
        assert!(tmp_dir.path().join("collect").join("cluster").is_dir());
        assert!(tmp_dir.path().join("collect").join("namespaces").is_dir());
        assert!(
            tmp_dir
                .path()
                .join("collect")
                .join("namespaces")
                .join("default")
                .is_dir()
        );
        assert!(
            !tmp_dir
                .path()
                .join("collect")
                .join("namespaces")
                .join("kube-system")
                .is_dir()
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_collect_kubeconfig_from_secret() {
        let test_env = envtest::Environment::default()
            .create()
            .expect("cluster one");
        let config: Kubeconfig = test_env.kubeconfig().unwrap();
        let client: Client = config.clone().try_into().unwrap();

        let other_env = envtest::Environment::default()
            .create()
            .expect("cluster two");

        let other_kubeconfig: serde_json::Value = other_env.kubeconfig().unwrap();
        let other_kubeconfig = serde_yaml::to_string(&other_kubeconfig).unwrap();
        fs::write(temp_kubeconfig(), other_kubeconfig.clone())
            .await
            .unwrap();

        let valid_config = r"
        filters:
        - include_namespace:
            - default
        ";
        let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), "default");
        cm_api
            .create(
                &PostParams::default(),
                &ConfigMap {
                    metadata: ObjectMeta {
                        name: Some("crust-gather-config".into()),
                        ..Default::default()
                    },
                    data: Some(BTreeMap::from([("config".into(), valid_config.into())])),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let secret_api: Api<Secret> = Api::namespaced(client.clone(), "default");
        secret_api
            .create(
                &PostParams::default(),
                &Secret {
                    metadata: ObjectMeta {
                        name: Some("kubeconfig-secret".into()),
                        labels: Some(BTreeMap::from([("kubeconfig".into(), "true".into())])),
                        ..Default::default()
                    },
                    string_data: Some(BTreeMap::from([("config".into(), other_kubeconfig)])),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let tmp_dir = TempDir::new("collect").expect("failed to create temp dir");

        let commands = Commands::CollectFromConfig {
            source: ConfigSource {
                config: None,
                config_map: Some(ConfigFromConfigMap("crust-gather-config".into())),
            },
            overrides: GatherSettings {
                kubeconfig_secret: Some(KubeconfigFromSecret {
                    kubeconfig_secret_label: Some(KubeconfigSecretLabel("kubeconfig=true".into())),
                    kubeconfig_secret_name: None,
                }),
                insecure_skip_tls_verify: Some(true),
                kubeconfig: Some(KubeconfigFile(config.clone())),
                file: Some(tmp_dir.path().join("collect").to_str().unwrap().into()),
                ..Default::default()
            },
        };

        assert!(commands.run().await.is_ok());
        assert!(tmp_dir.path().join("collect").join("cluster").is_dir());
        assert!(tmp_dir.path().join("collect").join("namespaces").is_dir());
        assert!(
            tmp_dir
                .path()
                .join("collect")
                .join("namespaces")
                .join("default")
                .is_dir()
        );
        assert!(
            !tmp_dir
                .path()
                .join("collect")
                .join("namespaces")
                .join("default")
                .join("configmap")
                .is_dir()
        );
        assert!(
            !tmp_dir
                .path()
                .join("collect")
                .join("namespaces")
                .join("kube-system")
                .is_dir()
        );

        let commands = Commands::CollectFromConfig {
            source: ConfigSource {
                config: None,
                config_map: Some(ConfigFromConfigMap("crust-gather-config".into())),
            },
            overrides: GatherSettings {
                insecure_skip_tls_verify: Some(true),
                kubeconfig: Some(KubeconfigFile(config)),
                file: Some(
                    tmp_dir
                        .path()
                        .join("collect-origin")
                        .to_str()
                        .unwrap()
                        .into(),
                ),
                ..Default::default()
            },
        };

        assert!(commands.run().await.is_ok());
        assert!(
            tmp_dir
                .path()
                .join("collect-origin")
                .join("cluster")
                .is_dir()
        );
        assert!(
            tmp_dir
                .path()
                .join("collect-origin")
                .join("namespaces")
                .is_dir()
        );
        assert!(
            tmp_dir
                .path()
                .join("collect-origin")
                .join("namespaces")
                .join("default")
                .is_dir()
        );
        assert!(
            !tmp_dir
                .path()
                .join("collect-origin")
                .join("namespaces")
                .join("kube-system")
                .is_dir()
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_collect_kubeconfig_from_secret_by_name() {
        let test_env = envtest::Environment::default()
            .create()
            .expect("cluster one");
        let config: Kubeconfig = test_env.kubeconfig().unwrap();
        let client: Client = config.clone().try_into().unwrap();

        let other_env = envtest::Environment::default()
            .create()
            .expect("cluster two");

        let other_kubeconfig: serde_json::Value = other_env.kubeconfig().unwrap();
        let other_kubeconfig = serde_yaml::to_string(&other_kubeconfig).unwrap();
        fs::write(temp_kubeconfig(), other_kubeconfig.clone())
            .await
            .unwrap();

        let secret_api: Api<Secret> = Api::namespaced(client.clone(), "default");
        secret_api
            .create(
                &PostParams::default(),
                &Secret {
                    metadata: ObjectMeta {
                        name: Some("kubeconfig-secret".into()),
                        ..Default::default()
                    },
                    string_data: Some(BTreeMap::from([("config".into(), other_kubeconfig)])),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let tmp_dir = TempDir::new("collect").expect("failed to create temp dir");

        let commands = Commands::Collect {
            config: GatherCommands {
                settings: GatherSettings {
                    kubeconfig_secret: Some(KubeconfigFromSecret {
                        kubeconfig_secret_label: None,
                        kubeconfig_secret_name: Some(KubeconfigSecretNamespaceName::from(
                            "default/kubeconfig-secret".to_string(),
                        )),
                    }),
                    kubeconfig: Some(KubeconfigFile(config)),
                    insecure_skip_tls_verify: Some(true),
                    file: Some(tmp_dir.path().join("collect").to_str().unwrap().into()),
                    ..Default::default()
                },
                ..Default::default()
            },
        };

        assert!(commands.run().await.is_ok());
        assert!(tmp_dir.path().join("collect").join("cluster").is_dir());
        assert!(tmp_dir.path().join("collect").join("namespaces").is_dir());
        assert!(
            tmp_dir
                .path()
                .join("collect")
                .join("namespaces")
                .join("default")
                .is_dir()
        );
        assert!(
            tmp_dir
                .path()
                .join("collect")
                .join("namespaces")
                .join("kube-system")
                .is_dir()
        );
    }

    #[test]
    fn test_from_str() {
        let tmp_dir = TempDir::new("config").expect("failed to create temp dir");

        let mut valid = File::create(tmp_dir.path().join("valid.yaml")).unwrap();
        let valid_config = r"
        filters:
        - include_namespace:
            - default
            - kube-system
        settings:
          debug_pod:
            image: busybox:1.37.0
        ";
        valid.write_all(valid_config.as_bytes()).unwrap();

        let mut invalid = File::create(tmp_dir.path().join("invalid.yaml")).unwrap();
        let invalid_config = r"
        filters:
        - include_namespace:
            - default
            - kube-system
        something: unknown
        ";
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
        assert!(result.filters.len() == 1);
        assert!(result.filters[0].include_namespace.len() == 2);

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
