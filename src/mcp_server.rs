use std::{sync::Arc, time::Instant};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use oci_client::{Reference, secrets::RegistryAuth};
use rmcp::{
    ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::*,
    schemars, tool, tool_handler, tool_router,
};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use tokio::{
    sync::{Mutex, oneshot},
    task::JoinHandle,
};

use crate::{
    cli::{Filters, GatherCommands, GatherSettings, OCIReference, OCISettings},
    gather::{
        config::{GatherMode, KubeconfigFile, RunDuration, SecretsFile},
        server::{Api, Socket},
        writer::{Archive, ArchiveSearch, Encoding},
    },
};

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct RedactionOptions {
    #[serde(default)]
    secret_values: Vec<String>,
    secrets_file: Option<String>,
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct CollectArchiveRequest {
    kubeconfig: Option<String>,
    context: Option<String>,
    archive_path: Option<String>,
    selectors: Option<Filters>,
    redaction: Option<RedactionOptions>,
    #[serde(default)]
    insecure_skip_tls_verify: bool,
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct CollectOciRequest {
    kubeconfig: Option<String>,
    context: Option<String>,
    image_reference: String,
    archive_path: Option<String>,
    duration: Option<String>,
    selectors: Option<Filters>,
    redaction: Option<RedactionOptions>,
    registry_auth: Option<OCISettings>,
    #[serde(default)]
    insecure_skip_tls_verify: bool,
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct ServeArchiveRequest {
    archive_path: String,
    socket: Option<String>,
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct ServeOciRequest {
    image_reference: String,
    socket: Option<String>,
    registry_auth: Option<OCISettings>,
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct StopServeRequest {
    #[serde(default)]
    graceful: bool,
}

#[derive(Debug, Serialize)]
struct CommandResponse {
    operation: &'static str,
    status: &'static str,
    summary: String,
    details: CommandDetails,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum CommandDetails {
    Collect(CollectResult),
    Serve(ServeResult),
    ServeStatus(ServeStatusResult),
    ServeExit(ServeExit),
}

#[derive(Debug, Serialize)]
struct CollectResult {
    destination: String,
    kubeconfig: Option<String>,
    context: Option<String>,
    elapsed_ms: u128,
    selectors: Filters,
    redaction: NormalizedRedaction,
}

#[derive(Debug, Serialize)]
struct ServeResult {
    source: ServeDescriptor,
    kubeconfig: String,
    kubectl_hint: String,
    started_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct ServeStatusResult {
    state: &'static str,
    active: Option<ServeResult>,
    last_exit: Option<ServeExit>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServeDescriptor {
    Archive {
        path: String,
        socket: String,
    },
    Oci {
        image_reference: String,
        socket: String,
        insecure_registry: bool,
        has_basic_auth: bool,
        has_token_auth: bool,
        has_custom_ca: bool,
    },
}

#[derive(Debug, Clone, Default, Serialize)]
struct NormalizedRedaction {
    secret_value_count: usize,
    secrets_file: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ServeExit {
    source: ServeDescriptor,
    finished_at: DateTime<Utc>,
    clean_shutdown: bool,
    error: Option<String>,
}

struct RunningServer {
    source: ServeDescriptor,
    kubeconfig: String,
    started_at: DateTime<Utc>,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<()>>,
    _temp_kubeconfig: NamedTempFile,
}

#[derive(Default)]
struct ServeRuntime {
    current: Option<RunningServer>,
    last_exit: Option<ServeExit>,
}

pub struct CrustGatherMcp {
    tool_router: ToolRouter<CrustGatherMcp>,
    runtime: Arc<Mutex<ServeRuntime>>,
}

const KUBECTL_HINT: &str = concat!(
    "Use this file via KUBECONFIG=<kubeconfig> kubectl ... ",
    "or kubectl --kubeconfig=<kubeconfig> ..."
);

const SERVER_INSTRUCTIONS: &str = concat!(
    "Use collect_archive to create a local snapshot artifact, or collect_oci to collect and ",
    "publish a snapshot to an OCI image. Both collection tools accept selectors to narrow scope ",
    "and redaction settings such as secret_values or secrets_file so sensitive values can be ",
    "excluded before saving or upload. Use serve_archive or serve_oci to expose a collected ",
    "snapshot as a read-only Kubernetes-like API. The serve tools always create a temporary ",
    "client kubeconfig for the active snapshot. In serve responses and serving_status, ",
    "details.source describes what is being served, details.kubeconfig is the client kubeconfig ",
    "to reuse, details.kubectl_hint is a lightweight kubectl usage hint, and details.started_at ",
    "is the server start timestamp. Prefer following kubectl_hint instead of rebuilding ",
    "connection details manually, and read kubeconfig file content if you need to do it ",
    "differently. Use stop_serving to shut down the background serve task."
);

#[tool_router]
impl CrustGatherMcp {
    #[tool(description = "Collect a crust-gather archive from a kubeconfig and optional context.")]
    async fn collect_archive(
        &self,
        Parameters(request): Parameters<CollectArchiveRequest>,
    ) -> String {
        render_response(self.execute_collect_archive(request).await)
    }

    #[tool(
        description = "Collect a crust-gather archive and push it to an OCI registry. \
                       Recommended to use ttl.sh registry with a short tag lifetime."
    )]
    async fn collect_oci(&self, Parameters(request): Parameters<CollectOciRequest>) -> String {
        render_response(self.execute_collect_oci(request).await)
    }

    #[tool(
        description = "Start serving a crust-gather archive in the background and return a \
                       temporary kubeconfig for clients."
    )]
    async fn serve_archive(&self, Parameters(request): Parameters<ServeArchiveRequest>) -> String {
        render_response(self.start_archive_server(request).await)
    }

    #[tool(
        description = "Start serving a crust-gather OCI source in the background and return a \
                       temporary kubeconfig for clients. The server may have a brief cold-start \
                       delay while OCI content is fetched and prepared."
    )]
    async fn serve_oci(&self, Parameters(request): Parameters<ServeOciRequest>) -> String {
        render_response(self.start_oci_server(request).await)
    }

    #[tool(
        description = "Report whether the MCP server is currently serving an archive or OCI \
                       source, including the temporary kubeconfig path for clients when active."
    )]
    async fn serving_status(&self) -> String {
        render_response(self.serving_status_response().await)
    }

    #[tool(description = "Stop the currently running archive or OCI serve task.")]
    async fn stop_serving(&self, Parameters(request): Parameters<StopServeRequest>) -> String {
        render_response(self.stop_serving_response(request.graceful).await)
    }
}

impl Default for CrustGatherMcp {
    fn default() -> Self {
        Self {
            tool_router: Self::tool_router(),
            runtime: Arc::new(Mutex::new(ServeRuntime::default())),
        }
    }
}

impl CrustGatherMcp {
    async fn execute_collect_archive(
        &self,
        request: CollectArchiveRequest,
    ) -> Result<CommandResponse> {
        let start = Instant::now();
        let kubeconfig = normalize_optional_string(request.kubeconfig)?;
        let context = normalize_optional_string(request.context)?;
        let archive_path = normalize_optional_string(request.archive_path)?
            .unwrap_or_else(|| Archive::default().to_string());
        let redaction = prepare_redaction(request.redaction)?;

        let settings = GatherSettings {
            kubeconfig: resolve_kubeconfig(kubeconfig.clone(), context.clone())?,
            insecure_skip_tls_verify: Some(request.insecure_skip_tls_verify),
            file: Some(Archive::from(archive_path.as_str())),
            secrets_file: redaction.secrets_file.clone(),
            encoding: Some(Encoding::Path),
            ..Default::default()
        };

        GatherCommands::new(GatherMode::Collect, request.selectors.clone(), settings)
            .load()
            .await?
            .collect()
            .await?;

        Ok(CommandResponse {
            operation: "collect_archive",
            status: "completed",
            summary: format!("Collected archive into {}", archive_path),
            details: CommandDetails::Collect(CollectResult {
                destination: archive_path,
                kubeconfig,
                context,
                elapsed_ms: start.elapsed().as_millis(),
                selectors: request.selectors.unwrap_or_default(),
                redaction: redaction.normalized,
            }),
        })
    }

    async fn execute_collect_oci(&self, request: CollectOciRequest) -> Result<CommandResponse> {
        let start = Instant::now();
        let kubeconfig = normalize_optional_string(request.kubeconfig)?;
        let context = normalize_optional_string(request.context)?;
        let image_reference = parse_reference(&request.image_reference)?;
        let archive_path = normalize_optional_string(request.archive_path)?
            .unwrap_or_else(|| Archive::default().to_string());
        let duration = parse_duration(request.duration)?;
        let redaction = prepare_redaction(request.redaction)?;
        let mut registry = request.registry_auth.unwrap_or_default();
        registry.reference = Some(image_reference.clone());
        let image_reference_value: Reference = image_reference.clone().into();

        let settings = GatherSettings {
            kubeconfig: resolve_kubeconfig(kubeconfig.clone(), context.clone())?,
            insecure_skip_tls_verify: Some(request.insecure_skip_tls_verify),
            file: Some(Archive::from(archive_path.as_str())),
            secrets_file: redaction.secrets_file.clone(),
            encoding: Some(Encoding::Oci(image_reference_value.clone())),
            oci: registry,
            duration,
            ..Default::default()
        };

        GatherCommands::new(GatherMode::Collect, request.selectors.clone(), settings)
            .load()
            .await?
            .collect()
            .await?;

        Ok(CommandResponse {
            operation: "collect_oci",
            status: "completed",
            summary: format!(
                "Collected archive and pushed it to {}",
                image_reference_value
            ),
            details: CommandDetails::Collect(CollectResult {
                destination: image_reference_value.to_string(),
                kubeconfig,
                context,
                elapsed_ms: start.elapsed().as_millis(),
                selectors: request.selectors.unwrap_or_default(),
                redaction: redaction.normalized,
            }),
        })
    }

    async fn start_archive_server(&self, request: ServeArchiveRequest) -> Result<CommandResponse> {
        self.reconcile_runtime().await;

        let archive_path = normalize_required_string(request.archive_path, "archive_path")?;
        let socket =
            normalize_optional_string(request.socket)?.unwrap_or_else(|| "0.0.0.0:9095".into());
        let socket_value = parse_socket(&socket)?;
        let archives = resolve_archives(&archive_path);
        let temp_kubeconfig = NamedTempFile::new()?;
        let client_kubeconfig = temp_kubeconfig.path().display().to_string();
        let api = Api::new(
            archives,
            Socket::try_from(socket.clone())?,
            Some(temp_kubeconfig.path().to_path_buf()),
        )
        .await?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let descriptor = ServeDescriptor::Archive {
            path: archive_path,
            socket: socket_value.to_string(),
        };
        let started_at = Utc::now();
        let task = tokio::spawn(async move { api.serve_with_shutdown(shutdown_rx).await });

        let serve_result = ServeResult {
            source: descriptor.clone(),
            kubeconfig: client_kubeconfig.clone(),
            kubectl_hint: KUBECTL_HINT.into(),
            started_at,
        };

        let mut runtime = self.runtime.lock().await;
        if runtime.current.is_some() {
            anyhow::bail!("a serve task is already running");
        }

        runtime.current = Some(RunningServer {
            source: descriptor,
            kubeconfig: client_kubeconfig,
            started_at,
            shutdown: Some(shutdown_tx),
            task,
            _temp_kubeconfig: temp_kubeconfig,
        });

        Ok(CommandResponse {
            operation: "serve_archive",
            status: "started",
            summary: format!("Serving archive on {}", socket_value),
            details: CommandDetails::Serve(serve_result),
        })
    }

    async fn start_oci_server(&self, request: ServeOciRequest) -> Result<CommandResponse> {
        self.reconcile_runtime().await;

        let image_reference = parse_reference(&request.image_reference)?;
        let socket =
            normalize_optional_string(request.socket)?.unwrap_or_else(|| "0.0.0.0:9095".into());
        let socket_value = parse_socket(&socket)?;
        let mut registry = request.registry_auth.unwrap_or_default();
        registry.reference = Some(image_reference.clone());
        let image_reference_value: Reference = image_reference.clone().into();
        let temp_kubeconfig = NamedTempFile::new()?;
        let client_kubeconfig = temp_kubeconfig.path().display().to_string();
        let descriptor = ServeDescriptor::Oci {
            image_reference: image_reference_value.to_string(),
            socket: socket_value.to_string(),
            insecure_registry: registry.insecure,
            has_basic_auth: matches!(registry.to_auth(), RegistryAuth::Basic(..)),
            has_token_auth: matches!(registry.to_auth(), RegistryAuth::Bearer(..)),
            has_custom_ca: registry.ca_file.is_some(),
        };
        let api = Api::new_oci(
            registry,
            Socket::try_from(socket.clone())?,
            Some(temp_kubeconfig.path().to_path_buf()),
        )
        .await?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let started_at = Utc::now();
        let task = tokio::spawn(async move { api.serve_with_shutdown(shutdown_rx).await });

        let serve_result = ServeResult {
            source: descriptor.clone(),
            kubeconfig: client_kubeconfig.clone(),
            kubectl_hint: KUBECTL_HINT.into(),
            started_at,
        };

        let mut runtime = self.runtime.lock().await;
        if runtime.current.is_some() {
            anyhow::bail!("a serve task is already running");
        }
        runtime.current = Some(RunningServer {
            source: descriptor,
            kubeconfig: client_kubeconfig,
            started_at,
            shutdown: Some(shutdown_tx),
            task,
            _temp_kubeconfig: temp_kubeconfig,
        });

        Ok(CommandResponse {
            operation: "serve_oci",
            status: "started",
            summary: format!("Serving OCI source on {}", socket_value),
            details: CommandDetails::Serve(serve_result),
        })
    }

    async fn serving_status_response(&self) -> Result<CommandResponse> {
        self.reconcile_runtime().await;

        let runtime = self.runtime.lock().await;
        let active = runtime.current.as_ref().map(|server| ServeResult {
            source: server.source.clone(),
            kubeconfig: server.kubeconfig.clone(),
            kubectl_hint: KUBECTL_HINT.into(),
            started_at: server.started_at,
        });
        let state = if active.is_some() { "running" } else { "idle" };

        Ok(CommandResponse {
            operation: "serving_status",
            status: "completed",
            summary: format!("Serve runtime is {state}"),
            details: CommandDetails::ServeStatus(ServeStatusResult {
                state,
                active,
                last_exit: runtime.last_exit.clone(),
            }),
        })
    }

    async fn stop_serving_response(&self, _graceful: bool) -> Result<CommandResponse> {
        self.reconcile_runtime().await;

        let running = {
            let mut runtime = self.runtime.lock().await;
            runtime.current.take()
        };

        let Some(mut running) = running else {
            return Ok(CommandResponse {
                operation: "stop_serving",
                status: "completed",
                summary: "No serve task is running".to_string(),
                details: CommandDetails::ServeStatus(ServeStatusResult {
                    state: "idle",
                    active: None,
                    last_exit: None,
                }),
            });
        };

        if let Some(shutdown) = running.shutdown.take() {
            let _ = shutdown.send(());
        }

        let exit = finalize_running_server(running, true).await;
        let mut runtime = self.runtime.lock().await;
        runtime.last_exit = Some(exit.clone());

        Ok(CommandResponse {
            operation: "stop_serving",
            status: "completed",
            summary: "Stopped serve task".to_string(),
            details: CommandDetails::ServeExit(exit),
        })
    }

    async fn reconcile_runtime(&self) {
        let finished = {
            let runtime = self.runtime.lock().await;
            runtime
                .current
                .as_ref()
                .map(|server| server.task.is_finished())
                .unwrap_or(false)
        };

        if !finished {
            return;
        }

        let running = {
            let mut runtime = self.runtime.lock().await;
            runtime.current.take()
        };

        if let Some(running) = running {
            let exit = finalize_running_server(running, false).await;
            let mut runtime = self.runtime.lock().await;
            runtime.last_exit = Some(exit);
        }
    }
}

#[tool_handler]
impl ServerHandler for CrustGatherMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions(SERVER_INSTRUCTIONS)
    }
}

pub async fn run() -> Result<()> {
    let transport = (tokio::io::stdin(), tokio::io::stdout());
    let service = CrustGatherMcp::default().serve(transport).await?;
    service.waiting().await?;
    Ok(())
}

fn render_response(response: Result<CommandResponse>) -> String {
    match response {
        Ok(response) => serde_json::to_string_pretty(&response)
            .unwrap_or_else(|error| format!("failed to serialize response: {error}")),
        Err(error) => serde_json::json!({
            "status": "error",
            "message": error.to_string(),
        })
        .to_string(),
    }
}

async fn finalize_running_server(running: RunningServer, clean_shutdown: bool) -> ServeExit {
    let error = match running.task.await {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error.to_string()),
        Err(error) => Some(error.to_string()),
    };

    ServeExit {
        source: running.source,
        finished_at: Utc::now(),
        clean_shutdown: clean_shutdown && error.is_none(),
        error,
    }
}

fn prepare_redaction(redaction: Option<RedactionOptions>) -> Result<PreparedRedaction> {
    let Some(redaction) = redaction else {
        return Ok(PreparedRedaction::default());
    };

    let mut secrets = redaction
        .secret_values
        .into_iter()
        .map(|value| normalize_required_string(value, "redaction.secret_values[]"))
        .collect::<Result<Vec<_>>>()?;

    let secrets_file = normalize_optional_string(redaction.secrets_file)?;
    if let Some(path) = secrets_file.clone() {
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read redaction file: {path}"))?;
        secrets.extend(
            content
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .map(ToOwned::to_owned),
        );
    }

    if secrets.is_empty() {
        return Ok(PreparedRedaction {
            normalized: NormalizedRedaction {
                secret_value_count: 0,
                secrets_file,
            },
            ..Default::default()
        });
    }

    let mut temp = NamedTempFile::new()?;
    for secret in &secrets {
        use std::io::Write as _;
        writeln!(temp, "{secret}")?;
    }

    Ok(PreparedRedaction {
        secrets_file: Some(SecretsFile::try_from(temp.path().display().to_string())?),
        _temp_file: Some(temp),
        normalized: NormalizedRedaction {
            secret_value_count: secrets.len(),
            secrets_file,
        },
    })
}

fn normalize_optional_string(value: Option<String>) -> Result<Option<String>> {
    value
        .map(|value| normalize_required_string(value, "string"))
        .transpose()
}

fn normalize_required_string(value: String, field: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        anyhow::bail!("{field} cannot be empty");
    }

    Ok(trimmed.to_string())
}

fn resolve_kubeconfig(
    kubeconfig: Option<String>,
    context: Option<String>,
) -> Result<Option<KubeconfigFile>> {
    match (kubeconfig, context) {
        (None, None) => Ok(None),
        (Some(path), context) => Ok(Some(
            KubeconfigFile::try_from(path)?.with_context(context.as_deref())?,
        )),
        (None, Some(context)) => Ok(Some(
            KubeconfigFile::infer_file()?.with_context(Some(context.as_str()))?,
        )),
    }
}

fn resolve_archives(path: &str) -> Vec<Archive> {
    let search = ArchiveSearch::from(path);
    let archives: Vec<Archive> = search.into();
    if archives.is_empty() {
        vec![Archive::from(path)]
    } else {
        archives
    }
}

fn parse_reference(reference: &str) -> Result<OCIReference> {
    OCIReference::try_from(reference.to_string())
        .with_context(|| format!("invalid OCI image reference: {reference}"))
}

fn parse_duration(duration: Option<String>) -> Result<Option<RunDuration>> {
    duration
        .map(|duration| {
            let duration = normalize_required_string(duration, "duration")?;
            RunDuration::try_from(duration)
                .with_context(|| "invalid duration, expected values like '60s' or '2m'")
        })
        .transpose()
}

fn parse_socket(socket: &str) -> Result<std::net::SocketAddr> {
    socket
        .parse()
        .with_context(|| format!("invalid socket address: {socket}"))
}

#[derive(Default)]
struct PreparedRedaction {
    secrets_file: Option<SecretsFile>,
    _temp_file: Option<NamedTempFile>,
    normalized: NormalizedRedaction,
}
