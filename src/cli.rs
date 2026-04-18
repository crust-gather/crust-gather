use std::{
    fs::{self, File},
    sync::Arc,
};

use anyhow::anyhow;
use clap::{ArgAction, Parser, Subcommand};
use k8s_openapi::serde::{Deserialize, Serialize};
use kube::{
    Client,
    config::{Kubeconfig, KubeconfigError},
};
use oci_client::{
    Reference,
    client::{self, ClientConfig, ClientProtocol},
    secrets::RegistryAuth,
};
use rmcp::schemars;
use tracing::level_filters::LevelFilter;

use crate::{
    filters::{
        filter::{Exclude, FilterGroup, FilterList, FilterType, Include},
        group::Group,
        kind::Kind,
        name::Name,
        namespace::Namespace,
        selector::{Annotations, Labels, Selector},
    },
    gather::{
        config::{
            Config, ConfigFromConfigMap, GatherMode, KubeconfigFile, KubeconfigSecretLabel,
            KubeconfigSecretNamespaceName, RunDuration, Secrets, SecretsFile,
        },
        log::UserLog,
        server::Server,
        writer::{Archive, Encoding, Writer},
    },
    mcp_server,
};

pub const DEFAULT_OCI_BUFFER_SIZE: usize = 32;

use tracing_subscriber::Layer;
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
        let fmt_layer = if matches!(self.command, Commands::Mcp) {
            // The MCP transport owns stdout, so logs must go to stderr in this mode.
            fmt::layer().with_writer(std::io::stderr).boxed()
        } else {
            fmt::layer().boxed()
        };

        tracing_subscriber::registry()
            .with(fmt_layer)
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

    /// Start the MCP server over stdio.
    Mcp,
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
            Commands::Serve { serve } => {
                serve.get_api().await?.serve().await.map_err(|e| anyhow!(e))
            }
            Commands::Mcp => mcp_server::run().await,
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
            value_parser = |arg: &str| -> anyhow::Result<GatherCommands> {Ok(GatherCommands::try_from(arg)?)},)]
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
            oci: other.oci.merge(self.oci.clone()),
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
        value_parser = |arg: &str| -> anyhow::Result<KubeconfigFile> {Ok(KubeconfigFile::try_from(arg)?)})]
    pub kubeconfig: Option<KubeconfigFile>,

    /// Collect kubeconfig from a secret.
    #[command(flatten)]
    pub kubeconfig_secret: Option<KubeconfigFromSecret>,

    /// Pass an insecure flag to kubeconfig file.
    /// If not provided, defaults to false and kubeconfig will be uses as-is.
    ///
    /// Example:
    ///     --insecure-skip-tls-verify
    #[arg(short, long)]
    #[serde(default)]
    pub insecure_skip_tls_verify: Option<bool>,

    /// The output file path.
    /// Defaults to a new archive with name "crust-gather".
    ///
    /// Example:
    ///     --file=./artifacts
    #[arg(short, long, value_name = "PATH")]
    #[serde(default)]
    pub file: Option<Archive>,

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
    #[arg(conflicts_with = "reference")]
    pub encoding: Option<Encoding>,

    /// OCI destination for crust gather collection. Stores cluster state in the provided image reference
    /// with optional authentication.
    #[clap(flatten)]
    #[serde(flatten)]
    #[serde(default)]
    pub oci: OCISettings,

    /// Secret environment variable name with data to exclude in the collected artifacts.
    /// Can be specified multiple times to exclude multiple values.
    ///
    /// Example:
    ///     --secret=MY_ENV_SECRET_DATA --secret=SOME_OTHER_SECRET_DATA
    #[arg(short, long = "secret", action = ArgAction::Append)]
    #[serde(default)]
    pub secrets: Vec<String>,

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
        value_parser = |arg: &str| -> anyhow::Result<SecretsFile> {Ok(SecretsFile::try_from(arg)?)})]
    #[serde(default)]
    pub secrets_file: Option<SecretsFile>,

    /// The duration to run the collection for.
    /// Defaults to 60 seconds.
    ///
    /// Example:
    ///     --duration=2m
    #[arg(short, long, value_name = "DURATION",
        value_parser = |arg: &str| -> anyhow::Result<RunDuration> {Ok(RunDuration::try_from(arg)?)})]
    #[serde(default)]
    pub duration: Option<RunDuration>,

    /// Name of the kubelet systemd unit.
    ///
    /// Defaults to kubelet.
    ///
    /// Example:
    ///     --systemd-unit=rke2-server
    ///     --systemd-unit=rke2-agent
    #[arg(long = "systemd-unit", value_name = "SYSTEMD_UNIT", default_value = "kubelet", action = ArgAction::Append )]
    #[serde(default)]
    pub systemd_units: Vec<String>,

    /// Collect settings to configure the pod which collect logs on nodes.
    #[command(flatten)]
    #[serde(default)]
    pub debug_pod: DebugPod,
}

/// OCI Registry storage options.
#[derive(Parser, Clone, Default, Deserialize, Debug, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OCISettings {
    /// Token to use for the registry authentication
    #[arg(short, long, env = "OCI_AUTH_TOKEN")]
    #[arg(conflicts_with = "regular")]
    #[serde(default)]
    pub token: Option<String>,

    /// Username and password for the registry authentication
    #[command(flatten)]
    #[serde(flatten)]
    #[serde(default)]
    pub regular: Option<UsernamePassword>,

    /// Use HTTP to interact with the registry
    #[arg(long)]
    #[serde(default)]
    pub insecure: bool,

    /// CA file to use for registry communication
    #[arg(short, long, value_parser = |arg: &str| -> anyhow::Result<Certificate> {Ok(arg.try_into()?)})]
    #[serde(default)]
    pub ca_file: Option<Certificate>,

    /// OCI Image reference
    #[arg(short, long, value_parser = |arg: &str| -> anyhow::Result<OCIReference> {Ok(arg.try_into()?)})]
    #[serde(default)]
    pub reference: Option<OCIReference>,

    /// Maximum number of OCI layers processed concurrently
    #[arg(short, long, value_parser = |arg: &str| -> anyhow::Result<usize> {
        let value = arg.parse::<u64>()?;
        anyhow::ensure!(value >= 1, "buffer size must be at least 1");
        Ok(value.try_into()?)
    }, default_value_t = DEFAULT_OCI_BUFFER_SIZE)]
    #[serde(default = "default_oci_buffer_size")]
    pub buffer_size: usize,
}

#[derive(
    Clone, Hash, PartialEq, Eq, Debug, Serialize, Deserialize, schemars::JsonSchema, Default,
)]
pub struct OCIReference {
    pub registry: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mirror_registry: Option<String>,
    pub repository: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
}

impl From<OCIReference> for Reference {
    fn from(reference: OCIReference) -> Self {
        let mut r = if let Some(tag) = reference.tag {
            Reference::with_tag(reference.registry, reference.repository, tag)
        } else {
            Reference::with_digest(
                reference.registry,
                reference.repository,
                reference.digest.unwrap_or_default(),
            )
        };

        if let Some(mirror) = reference.mirror_registry {
            r.set_mirror_registry(mirror)
        }

        r
    }
}

impl From<Reference> for OCIReference {
    fn from(reference: Reference) -> Self {
        Self {
            registry: reference.registry().to_string(),
            mirror_registry: reference.namespace().map(ToString::to_string),
            repository: reference.repository().to_string(),
            tag: reference.tag().map(ToString::to_string),
            digest: reference.digest().map(ToString::to_string),
        }
    }
}

impl TryFrom<&str> for OCIReference {
    type Error = anyhow::Error;

    fn try_from(reference: &str) -> Result<Self, Self::Error> {
        let reference: Reference = reference.parse()?;
        Ok(reference.into())
    }
}

/// Username/password authentication payload.
#[derive(Parser, Clone, Default, Deserialize, Debug, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[group(id = "regular", conflicts_with = "token")]
pub struct UsernamePassword {
    /// Username to use for the registry authentication
    #[arg(short, long, env = "OCI_AUTH_USERNAME", required = false)]
    pub username: String,

    /// Password to use for the registry authentication
    #[arg(short, long, env = "OCI_AUTH_PASSWORD", required = false)]
    pub password: String,
}

/// A x509 certificate
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct Certificate {
    pub data: Vec<u8>,
}

impl TryFrom<&str> for Certificate {
    type Error = anyhow::Error;

    fn try_from(ca_file: &str) -> Result<Self, Self::Error> {
        Ok(Self {
            data: fs::read_to_string(ca_file)?.as_bytes().to_vec(),
        })
    }
}

impl OCISettings {
    fn merge(&self, other: Self) -> Self {
        Self {
            token: self.token.clone().or(other.token),
            regular: self.regular.clone().or(other.regular),
            insecure: self.insecure || other.insecure,
            ca_file: self.ca_file.clone().or(other.ca_file),
            reference: self.reference.clone().or(other.reference),
            buffer_size: if self.buffer_size == DEFAULT_OCI_BUFFER_SIZE {
                other.buffer_size
            } else {
                self.buffer_size
            },
        }
    }

    pub fn to_client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::default();
        if self.insecure {
            config.protocol = ClientProtocol::Http;
        };

        if let Some(cert) = self.ca_file.as_ref() {
            config.extra_root_certificates.push(client::Certificate {
                encoding: client::CertificateEncoding::Pem,
                data: cert.data.clone(),
            });
        }

        config.max_concurrent_upload = self.buffer_size;

        config
    }

    pub fn to_auth(&self) -> RegistryAuth {
        let mut auth = RegistryAuth::Anonymous;
        if let Some(token) = self.token.as_ref() {
            auth = RegistryAuth::Bearer(token.clone())
        } else if let Some(up) = self.regular.as_ref() {
            auth = RegistryAuth::Basic(up.username.clone(), up.password.clone())
        }

        auth
    }
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

        let client = match &self.kubeconfig {
            Some(kubeconfig) => {
                kubeconfig
                    .client(self.insecure_skip_tls_verify.unwrap_or_default())
                    .await
            }
            None => KubeconfigFile::infer(self.insecure_skip_tls_verify.unwrap_or_default()).await,
        };

        // Follow the in-cluster scenario
        if let Err(kube::Error::InferKubeconfig(e)) = &client
            && matches!(e, KubeconfigError::ReadConfig(..))
        {
            return Client::try_default()
                .await
                .map_err(|_| anyhow::anyhow!("Failed to infer default client: {e}"));
        }

        client.map_err(|e| anyhow::anyhow!("Failed to initialize client from kubeconfig: {e}"))
    }

    pub async fn to_writer(&self) -> anyhow::Result<Writer> {
        let encoding = if let Some(reference) = self.oci.reference.as_ref() {
            &Encoding::Oci(reference.clone().into())
        } else if let Some(encoding) = self.encoding.as_ref() {
            encoding
        } else {
            &Encoding::Path
        };

        let client_config = if self.oci.reference.as_ref().is_some() {
            Some(self.oci.to_client_config())
        } else {
            None
        };

        let auth = if self.oci.reference.as_ref().is_some() {
            Some(self.oci.to_auth())
        } else {
            None
        };

        Writer::new(
            &self.file.clone().unwrap_or_default(),
            encoding,
            client_config,
            auth,
            self.oci.buffer_size,
        )
        .await
    }
}

const fn default_oci_buffer_size() -> usize {
    DEFAULT_OCI_BUFFER_SIZE
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
            value_parser = |arg: &str| -> anyhow::Result<UserLog> {Ok(UserLog::try_from(arg)?)},
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

#[derive(Parser, Clone, Default, Serialize, Deserialize, Debug, schemars::JsonSchema)]
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
            value_parser = |arg: &str| -> anyhow::Result<Namespace<Include>> {Ok(Namespace::<Include>::try_from(arg)?)},
            action = ArgAction::Append )]
    #[serde(default)]
    pub include_namespace: Vec<Namespace<Include>>,

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
            value_parser = |arg: &str| -> anyhow::Result<Namespace<Exclude>> {Ok(Namespace::<Exclude>::try_from(arg)?)},
            action = ArgAction::Append )]
    #[serde(default)]
    pub exclude_namespace: Vec<Namespace<Exclude>>,

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
            value_parser = |arg: &str| -> anyhow::Result<Kind<Include>> {Ok(Kind::<Include>::try_from(arg)?)},
            action = ArgAction::Append )]
    #[serde(default)]
    pub include_kind: Vec<Kind<Include>>,

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
            value_parser = |arg: &str| -> anyhow::Result<Kind<Exclude>> {Ok(Kind::<Exclude>::try_from(arg)?)},
            action = ArgAction::Append )]
    #[serde(default)]
    pub exclude_kind: Vec<Kind<Exclude>>,

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
            value_parser = |arg: &str| -> anyhow::Result<Group<Include>> {Ok(Group::<Include>::try_from(arg)?)},
            action = ArgAction::Append )]
    #[serde(default)]
    pub include_group: Vec<Group<Include>>,

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
            value_parser = |arg: &str| -> anyhow::Result<Group<Exclude>> {Ok(Group::<Exclude>::try_from(arg)?)},
            action = ArgAction::Append )]
    #[serde(default)]
    pub exclude_group: Vec<Group<Exclude>>,

    /// Resource name to include in the resource collection.
    ///
    /// By default all resource names are collected.
    /// Name can be specified as a regular string or as a regex string.
    ///
    /// Example:
    ///     --include-name=my-pod --include-name=frontend-.*
    #[arg(long, value_name = "NAME",
            value_parser = |arg: &str| -> anyhow::Result<Name<Include>> {Ok(Name::<Include>::try_from(arg)?)},
            action = ArgAction::Append )]
    #[serde(default)]
    pub include_name: Vec<Name<Include>>,

    /// Resource name to exclude from the resource collection.
    ///
    /// By default all resource names are collected.
    /// Name can be specified as a regular string or as a regex string.
    ///
    /// Example:
    ///     --exclude-name=my-secret --exclude-name=internal-.*
    #[arg(long, value_name = "NAME",
            value_parser = |arg: &str| -> anyhow::Result<Name<Exclude>> {Ok(Name::<Exclude>::try_from(arg)?)},
            action = ArgAction::Append )]
    #[serde(default)]
    pub exclude_name: Vec<Name<Exclude>>,

    /// Label selector to include in the resource collection.
    ///
    /// By default resources with any labels are collected.
    /// Label selector supports Kubernetes selector expressions.
    ///
    /// --include-labels specified multiple times allows to include multiple label selectors, which are combined with OR operator.
    ///
    /// Example:
    ///     --include-labels=app=frontend --include-labels='environment notin (prod,staging),tier!=web'
    #[arg(long, value_name = "LABEL_SELECTOR",
            value_parser = |arg: &str| -> anyhow::Result<Selector<Include, Labels>> {Ok(Selector::<Include, Labels>::try_from(arg)?)},
            action = ArgAction::Append )]
    #[serde(default)]
    pub include_labels: Vec<Selector<Include, Labels>>,

    /// Label selector to exclude from the resource collection.
    ///
    /// By default resources with any labels are collected.
    /// Label selector supports Kubernetes selector expressions.
    ///
    /// --exclude-labels specified multiple times allows to exclude multiple label selectors, which are combined with OR operator.
    ///
    /// Example:
    ///     --exclude-labels=app=internal --exclude-labels='environment in (prod,staging),tier==web'
    #[arg(long, value_name = "LABEL_SELECTOR",
            value_parser = |arg: &str| -> anyhow::Result<Selector<Exclude, Labels>> {Ok(Selector::<Exclude, Labels>::try_from(arg)?)},
            action = ArgAction::Append )]
    #[serde(default)]
    pub exclude_labels: Vec<Selector<Exclude, Labels>>,

    /// Annotation selector to include in the resource collection.
    ///
    /// By default resources with any annotations are collected.
    /// Annotation selector supports Kubernetes selector expressions.
    ///
    /// --include-annotations specified multiple times allows to include multiple annotation selectors, which are combined with OR operator.
    ///
    /// Example:
    ///     --include-annotations=app=frontend --include-annotations='environment notin (prod,staging),tier!=web'
    #[arg(long, value_name = "ANNOTATION_SELECTOR",
            value_parser = |arg: &str| -> anyhow::Result<Selector<Include, Annotations>> {Ok(Selector::<Include, Annotations>::try_from(arg)?)},
            action = ArgAction::Append )]
    #[serde(default)]
    pub include_annotations: Vec<Selector<Include, Annotations>>,

    /// Annotation selector to exclude from the resource collection.
    ///
    /// By default resources with any annotations are collected.
    /// Annotation selector supports Kubernetes selector expressions.
    ///
    /// --exclude-annotations specified multiple times allows to exclude multiple annotation selectors, which are combined with OR operator.
    ///
    /// Example:
    ///     --exclude-annotations=app=internal --exclude-annotations='environment in (prod,staging),tier==web'
    #[arg(long, value_name = "ANNOTATION_SELECTOR",
            value_parser = |arg: &str| -> anyhow::Result<Selector<Exclude, Annotations>> {Ok(Selector::<Exclude, Annotations>::try_from(arg)?)},
            action = ArgAction::Append )]
    #[serde(default)]
    pub exclude_annotations: Vec<Selector<Exclude, Annotations>>,
}

impl TryFrom<&str> for GatherCommands {
    type Error = anyhow::Error;

    fn try_from(file: &str) -> Result<Self, Self::Error> {
        Ok(serde_yaml::from_reader(File::open(file)?)?)
    }
}

impl GatherCommands {
    pub fn new(mode: GatherMode, filter: Option<Filters>, settings: GatherSettings) -> Self {
        Self {
            mode,
            additional_logs: AdditionalLogs::default(),
            filters: vec![],
            filter,
            settings,
        }
    }

    pub async fn load(&self) -> anyhow::Result<Config> {
        let env_secrets: Secrets = self.settings.secrets.clone().into();
        let mut secrets: Secrets = match self.settings.secrets_file.clone() {
            Some(file) => file.clone().try_into()?,
            None => vec![].into(),
        };

        secrets.0.extend(env_secrets.0.into_iter());

        let writer: Writer = self.settings.to_writer().await?;

        Ok(Config {
            client: self.client().await?,
            filter: Arc::new(self.into()),
            writer: writer.into(),
            secrets,
            mode: self.mode.clone(),
            additional_logs: self
                .additional_logs
                .logs
                .clone()
                .into_iter()
                .chain(self.additional_logs.additional_logs.clone().into_iter())
                .map(Into::into)
                .collect(),
            duration: self.settings.duration.unwrap_or_default(),
            systemd_units: self.settings.systemd_units.clone(),
            debug_pod: self.settings.debug_pod.clone(),
        })
    }

    async fn client(&self) -> anyhow::Result<Client> {
        self.settings.client().await
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
            filter.include_name.clone().into(),
            filter.exclude_name.clone().into(),
            filter.include_labels.clone().into(),
            filter.exclude_labels.clone().into(),
            filter.include_annotations.clone().into(),
            filter.exclude_annotations.clone().into(),
        ];

        Self(data.iter().map(Clone::clone).collect())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::{collections::BTreeMap, env, io::Write};

    use k8s_openapi::api::core::v1::{ConfigMap, Namespace, Secret};
    use kube::core::{ObjectMeta, params::ListParams};
    use kube::{Api, api::PostParams};
    use serde_json::Value;
    use tempfile::TempDir;
    use tokio::fs;

    use super::*;

    fn temp_kubeconfig() -> PathBuf {
        let mut dir = env::temp_dir();
        dir.push(xid::new().to_string());
        dir
    }

    #[test]
    fn test_filter_list_matches_filters_field_count() {
        let filters = Filters::default();
        let serialized = serde_json::to_value(&filters).unwrap();
        let Value::Object(fields) = serialized else {
            panic!("filters should serialize as an object");
        };

        assert_eq!(FilterList::from(&filters).0.len(), fields.len());
    }

    #[tokio::test]
    async fn test_client_from_kubeconfig() {
        let test_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster");

        let config = test_env.kubeconfig().unwrap();
        let kubeconfig_path = temp_kubeconfig();
        fs::write(
            kubeconfig_path.clone(),
            serde_yaml::to_string(&config).unwrap(),
        )
        .await
        .unwrap();

        let kubeconfig = KubeconfigFile::try_from(kubeconfig_path.to_str().unwrap()).unwrap();

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
    async fn test_insecure_client_from_kubeconfig() {
        let test_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster");

        let config = test_env.kubeconfig().unwrap();
        let kubeconfig_path = temp_kubeconfig();
        fs::write(
            kubeconfig_path.clone(),
            serde_yaml::to_string(&config).unwrap(),
        )
        .await
        .unwrap();

        let kubeconfig = KubeconfigFile::try_from(kubeconfig_path.to_str().unwrap()).unwrap();

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
    async fn test_client_from_default() {
        let test_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster");

        let config = test_env.kubeconfig().unwrap();
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
    async fn test_insecure_client_from_default() {
        let test_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster");
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
    async fn test_collect() {
        let test_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster");
        let config = test_env.kubeconfig().unwrap();

        let tmp_dir = TempDir::new().expect("failed to create temp dir");

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
    async fn test_collect_from_config() {
        let test_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster");
        let config = test_env.kubeconfig().unwrap();

        let tmp_dir = TempDir::new().expect("failed to create temp dir");

        let config_path = tmp_dir.path().join("valid.yaml");
        let mut valid = File::create(config_path.clone()).unwrap();
        let valid_config = r"
            filters:
            - include_namespace:
                - default
            ";
        valid.write_all(valid_config.as_bytes()).unwrap();

        let tmp_dir = TempDir::new().expect("failed to create temp dir");

        let config_path = config_path.to_str();
        let commands = Commands::CollectFromConfig {
            source: ConfigSource {
                config: Some(GatherCommands::try_from(config_path.unwrap()).unwrap()),
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
    async fn test_collect_from_config_map() {
        let test_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster");
        let config = test_env.kubeconfig().unwrap();
        let client = test_env.client().expect("client");

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

        let tmp_dir = TempDir::new().expect("failed to create temp dir");

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
    async fn test_collect_kubeconfig_from_secret() {
        let test_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster one");
        let config = test_env.kubeconfig().unwrap();
        let client = test_env.client().expect("client");

        let other_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster two");

        let other_kubeconfig = other_env.kubeconfig().unwrap();
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

        let tmp_dir = TempDir::new().expect("failed to create temp dir");

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
    async fn test_collect_kubeconfig_from_secret_by_name() {
        let test_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster one");
        let config = test_env.kubeconfig().unwrap();
        let client = test_env.client().expect("client");

        let other_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster two");

        let other_kubeconfig = other_env.kubeconfig().unwrap();
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

        let tmp_dir = TempDir::new().expect("failed to create temp dir");

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
        let tmp_dir = TempDir::new().expect("failed to create temp dir");

        let mut valid = File::create(tmp_dir.path().join("valid.yaml")).unwrap();
        let valid_config = r"
        filters:
        - include_namespace:
            - default
            - kube-system
          exclude_namespace:
            - kube-public
          include_kind:
            - Pod
          exclude_kind:
            - Secret
          include_group:
            - apps/Deployment
          exclude_group:
            - /Node
          include_name:
            - frontend-.*
          exclude_name:
            - secret-.*
          include_labels:
            - app=frontend
          exclude_labels:
            - tier=internal
          include_annotations:
            - owner in (platform,infra)
          exclude_annotations:
            - debug=true
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

        let result = GatherCommands::try_from(tmp_dir.path().join("valid.yaml").to_str().unwrap());
        assert!(result.is_ok());
        let result = &result.unwrap();
        assert!(result.filters.len() == 1);
        assert!(result.filters[0].include_namespace.len() == 2);
        assert!(result.filters[0].exclude_namespace.len() == 1);
        assert!(result.filters[0].include_kind.len() == 1);
        assert!(result.filters[0].exclude_kind.len() == 1);
        assert!(result.filters[0].include_group.len() == 1);
        assert!(result.filters[0].exclude_group.len() == 1);
        assert!(result.filters[0].include_name.len() == 1);
        assert!(result.filters[0].exclude_name.len() == 1);
        assert!(result.filters[0].include_labels.len() == 1);
        assert!(result.filters[0].exclude_labels.len() == 1);
        assert!(result.filters[0].include_annotations.len() == 1);
        assert!(result.filters[0].exclude_annotations.len() == 1);

        let result =
            GatherCommands::try_from(tmp_dir.path().join("invalid.yaml").to_str().unwrap());
        assert!(result.is_err());
    }
}
