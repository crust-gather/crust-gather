use std::{
    collections::HashMap, fmt::Display, fs::File, net::SocketAddr, ops::Deref, path::PathBuf,
    sync::Arc, time::Duration,
};

use actix_web::{
    App, HttpResponse, HttpServer, Responder, error, get,
    http::{
        KeepAlive,
        header::{Accept, CONTENT_TYPE, QualityItem, VARY},
    },
    post,
    web::{self, Bytes, Header, Path, Query},
};
use anyhow::Context as _;
use async_stream::stream;
use cached::proc_macro::cached;
use chrono::{DateTime, Utc};
use clap::Parser;
use k8s_openapi::serde_json::{self, json};
use kube::{
    config::{Cluster, Context, Kubeconfig, NamedAuthInfo, NamedCluster, NamedContext},
    core::discovery::v2,
};
use oci_client::Client;
use serde::Deserialize;
use tokio::time::{Instant, sleep};

use crate::{
    cli::OCISettings,
    gather::{
        reader::{ArchiveReader, Destination, Get, List, Log, NamedObject, Reader, Watch}, storage::{OCIState, Storage}, writer::Archive
    },
};

use super::{representation::ArchivePath, selector::Selector, writer::ArchiveSearch};

#[derive(Clone, Deserialize)]
pub struct Socket(SocketAddr);

impl Default for Socket {
    fn default() -> Self {
        Self("0.0.0.0:9095".parse().unwrap())
    }
}

impl Display for Socket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Parser, Clone, Default, Deserialize)]
pub struct Server {
    /// Path to a kubeconfig file.
    /// If not provided, will attempt to use the default config for the environment.
    /// This can be defined by KUBECONFIG env variable.
    ///
    /// Example:
    ///     --kubeconfig=./kubeconfig
    #[arg(short, long, value_name = "KUBECONFIG")]
    kubeconfig: Option<PathBuf>,

    /// The input archive path. Will be used as a recursive search directory for
    /// snapshot locations.
    ///
    /// Defaults to a new archive with name "crust-gather".
    ///
    /// Example:
    ///     --archive=./artifacts
    #[arg(short, long, value_name = "PATH", default_value_t = Default::default())]
    #[serde(default)]
    #[arg(conflicts_with = "reference")]
    archive: ArchiveSearch,

    /// OCI source for crust gather archive serving. Reads cluster state directly from the provided image reference
    /// with optional authentication.
    #[clap(flatten)]
    #[serde(flatten)]
    #[serde(default)]
    oci: OCISettings,

    /// The socket address to bind the HTTP server to.
    ///
    /// Defaults to 0.0.0.0:8080.
    ///
    /// Example:
    ///     --socket=192.168.1.100:8088
    #[arg(short, long, value_name = "SOCKET", default_value_t = Default::default(),
        value_parser = |arg: &str| -> anyhow::Result<Socket> {Ok(Socket(arg.to_string().parse()?))})]
    #[serde(default)]
    socket: Socket,
}

impl Server {
    pub async fn get_api(&self) -> anyhow::Result<Api> {
        let archives: Vec<_> = self.archive.clone().into();
        if self.oci.reference.is_some() {
            Api::new_oci(
                self.oci.clone(),
                self.socket.clone(),
                self.kubeconfig.clone(),
            )
            .await
        } else {
            Api::new(archives, self.socket.clone(), self.kubeconfig.clone()).await
        }
    }
}

pub struct Api {
    state: ApiState,
    socket: SocketAddr,
}

#[derive(Clone)]
struct ApiState {
    archives: HashMap<String, ArchiveReader>,
    kubeconfig_path: PathBuf,
    previous_context: Option<String>,
    serve_time: DateTime<Utc>,
    storage: Storage,
}

impl ApiState {
    pub async fn to_reader(&self, archive: ArchiveReader) -> anyhow::Result<Reader> {
        Reader::new(archive, self.serve_time, self.storage.clone())
            .await
            .context("failed to open storage reader")
    }
}

impl Api {
    pub async fn new(
        archives: impl IntoIterator<Item = Archive> + Clone,
        socket: Socket,
        kubeconfig: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        let Socket(socket) = socket;

        let kubeconfig_path = kubeconfig.unwrap_or(std::path::PathBuf::from(
            std::env::var("KUBECONFIG")
                .unwrap_or(format!("{}/.kube/config", std::env::var("HOME")?).to_string()),
        ));

        let mut config = match File::open(&kubeconfig_path) {
            Ok(file) => serde_yaml::from_reader(file)?,
            _ => Kubeconfig::default(),
        };

        let previous_context = config.current_context;
        config.current_context = None;

        let config = archives
            .clone()
            .into_iter()
            .map(|a| Api::prepare_kubeconfig(a.name().to_string_lossy().to_string(), socket))
            .try_fold(config, Kubeconfig::merge)?;

        serde_yaml::to_writer(File::create(&kubeconfig_path)?, &config)?;

        let mut readers = HashMap::new();
        for archive in archives {
            readers.insert(
                archive.name().to_string_lossy().to_string(),
                ArchiveReader::new(archive, &Storage::FS).await,
            );
        }

        Ok(Self {
            state: ApiState {
                archives: readers,
                kubeconfig_path,
                previous_context,
                serve_time: Utc::now(),
                storage: Storage::FS,
            },
            socket,
        })
    }

    pub async fn new_oci(
        oci: OCISettings,
        socket: Socket,
        kubeconfig: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        let Some(reference) = &oci.reference else {
            anyhow::bail!("missing reference");
        };

        let Socket(socket) = socket;

        let kubeconfig_path = kubeconfig.unwrap_or(std::path::PathBuf::from(
            std::env::var("KUBECONFIG")
                .unwrap_or(format!("{}/.kube/config", std::env::var("HOME")?).to_string()),
        ));

        let mut config = match File::open(&kubeconfig_path) {
            Result::Ok(file) => serde_yaml::from_reader(file)?,
            _ => Kubeconfig::default(),
        };

        let previous_context = config.current_context;
        config.current_context = None;

        let config = Api::prepare_kubeconfig(reference.repository().to_string(), socket);

        serde_yaml::to_writer(File::create(&kubeconfig_path)?, &config)?;

        let client = Client::new(oci.to_client_config());
        let auth = oci.to_auth();
        let (manifest, _) = client.pull_image_manifest(reference, &auth).await?;
        let mut index = HashMap::new();

        for layer in manifest.layers {
            let Some(annotations) = layer.annotations.clone() else {
                anyhow::bail!("manifest layer contains no org.opencontainers.image.title annoation")
            };
            let path = &annotations["org.opencontainers.image.title"];
            index.insert(PathBuf::from(path), layer);
        }

        let index = Arc::new(index);

        let storage = Storage::new(Some(OCIState {
            reference: reference.clone(),
            client,
            index,
            auth,
        }));

        let search = ArchiveSearch::default();
        let mut archives = HashMap::new();
        archives.insert(
            reference.repository().to_string(),
            ArchiveReader::new(Archive::new(search.path()), &storage).await,
        );

        Ok(Self {
            state: ApiState {
                archives,
                kubeconfig_path,
                previous_context,
                serve_time: Utc::now(),
                storage,
            },
            socket,
        })
    }

    fn prepare_kubeconfig(name: String, socket: SocketAddr) -> Kubeconfig {
        Kubeconfig {
            current_context: Some(name.clone()),
            auth_infos: vec![NamedAuthInfo {
                name: name.clone(),
                ..Default::default()
            }],
            contexts: vec![NamedContext {
                name: name.clone(),
                context: Some(Context {
                    cluster: name.clone(),
                    user: name.clone().into(),
                    ..Default::default()
                }),
            }],
            clusters: vec![NamedCluster {
                name: name.clone(),
                cluster: Some(Cluster {
                    server: Some(format!("http://{socket}/{name}")),
                    ..Default::default()
                }),
            }],
            ..Default::default()
        }
    }

    fn clean_kubeconfig(state: ApiState) -> anyhow::Result<()> {
        let mut config = Kubeconfig::read_from(&state.kubeconfig_path)?;

        let contexts = state.archives.into_keys().collect::<Vec<_>>();

        config.contexts.retain(|c| !contexts.contains(&c.name));
        config.clusters.retain(|c| !contexts.contains(&c.name));
        config.auth_infos.retain(|ai| !contexts.contains(&ai.name));

        config.current_context = match config
            .contexts
            .iter()
            .find(|c| Some(c.name.clone()) == state.previous_context)
        {
            Some(context) => Some(context.name.clone()),
            None => config.contexts.first().map(|c| c.name.clone()),
        };

        serde_yaml::to_writer(File::create(state.kubeconfig_path)?, &config)?;

        Ok(())
    }

    pub async fn serve(self) -> anyhow::Result<()> {
        let state = self.state.clone();

        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(self.state.clone()))
                .service(version)
                .service(ssr_stub)
                .service(api)
                .service(apis)
                .service(api_list)
                .service(apis_list)
                .service(api_namespaced_list)
                .service(apis_namespaced_list)
                .service(cluster_get)
                .service(cluster_apis_get)
                .service(namespaced_get)
                .service(namespaced_apis_get)
                .service(logs_get)
        })
        .keep_alive(KeepAlive::Timeout(Duration::from_secs(30)))
        .client_disconnect_timeout(Duration::from_secs(5))
        .bind_auto_h2c(self.socket)?
        .run()
        .await?;

        Self::clean_kubeconfig(state)?;

        Ok(())
    }
}

#[get("{server}/version")]
async fn version(
    server: Path<Destination>,
    state: web::Data<ApiState>,
) -> actix_web::Result<impl Responder> {
    let archive = state
        .archives
        .get(server.get_server())
        .ok_or(error::ErrorNotFound(anyhow::anyhow!("Server not found")))?;
    let reader = state
        .to_reader(archive.clone())
        .await
        .map_err(error::ErrorServiceUnavailable)?;
    let path = load_raw(reader, ArchivePath::Custom("version.yaml".into()))
        .await
        .map_err(error::ErrorNotFound)?;
    let version: serde_yaml::Value =
        serde_yaml::from_str(&path).map_err(error::ErrorUnprocessableEntity)?;

    Ok(web::Json(version))
}

#[post("{server}/apis/authorization.k8s.io/v1/selfsubjectaccessreviews")]
async fn ssr_stub() -> impl Responder {
    web::Json(json!({
        "kind": "SelfSubjectAccessReview",
        "apiVersion": "authorization.k8s.io/v1",
        "spec": {
            "resourceAttributes": {
                "verb": "*",
                "resource": "*"
            }
        },
        "status": {
            "allowed": true
        }
    }))
}

#[get("{server}/api")]
async fn api(
    server: Path<Destination>,
    state: web::Data<ApiState>,
) -> actix_web::Result<impl Responder> {
    let archive = state
        .archives
        .get(server.get_server())
        .ok_or(error::ErrorNotFound(anyhow::anyhow!("Server not found")))?;

    let latest_discovery_version = v2::ACCEPT_AGGREGATED_DISCOVERY_V2
        .split(',')
        .next()
        .unwrap_or(v2::ACCEPT_AGGREGATED_DISCOVERY_V2)
        .trim();

    let reader = state
        .to_reader(archive.clone())
        .await
        .map_err(error::ErrorServiceUnavailable)?;

    Ok(load_raw(reader, ArchivePath::Custom("api.json".into()))
        .await
        .map_err(error::ErrorNotFound)?
        .customize()
        .insert_header((CONTENT_TYPE, latest_discovery_version))
        .insert_header((VARY, "Accept"))
        .insert_header(("X-Varied-Accept", v2::ACCEPT_AGGREGATED_DISCOVERY_V2)))
}

#[get("{server}/apis")]
async fn apis(
    server: Path<Destination>,
    state: web::Data<ApiState>,
) -> actix_web::Result<impl Responder> {
    let archive = state
        .archives
        .get(server.get_server())
        .ok_or(error::ErrorNotFound(anyhow::anyhow!("Server not found")))?;
    let latest_discovery_version = v2::ACCEPT_AGGREGATED_DISCOVERY_V2
        .split(',')
        .next()
        .unwrap_or(v2::ACCEPT_AGGREGATED_DISCOVERY_V2)
        .trim();

    let reader = state
        .to_reader(archive.clone())
        .await
        .map_err(error::ErrorServiceUnavailable)?;

    Ok(load_raw(reader, ArchivePath::Custom("apis.json".into()))
        .await
        .map_err(error::ErrorNotFound)?
        .customize()
        .insert_header((CONTENT_TYPE, latest_discovery_version))
        .insert_header((VARY, "Accept"))
        .insert_header(("X-Varied-Accept", v2::ACCEPT_AGGREGATED_DISCOVERY_V2)))
}

#[get("{server}/api/{version}/{kind}")]
async fn api_list(
    accept: Header<Accept>,
    list: Path<List>,
    query: Query<Selector>,
    watch: Query<Watch>,
    state: web::Data<ApiState>,
) -> actix_web::Result<impl Responder> {
    match watch.watch {
        Some(true) => watch_response(accept, list, query, state).await,
        None | Some(false) => list_response(accept, list, query, state).await,
    }
}

#[get("{server}/apis/{group}/{version}/{kind}")]
async fn apis_list(
    accept: Header<Accept>,
    list: Path<List>,
    query: Query<Selector>,
    watch: Query<Watch>,
    state: web::Data<ApiState>,
) -> actix_web::Result<impl Responder> {
    match watch.watch {
        Some(true) => watch_response(accept, list, query, state).await,
        None | Some(false) => list_response(accept, list, query, state).await,
    }
}

#[get("{server}/api/{version}/namespaces/{namespace}/{kind}")]
async fn api_namespaced_list(
    accept: Header<Accept>,
    list: Path<List>,
    query: Query<Selector>,
    watch: Query<Watch>,
    state: web::Data<ApiState>,
) -> actix_web::Result<impl Responder> {
    match watch.watch {
        Some(true) => watch_response(accept, list, query, state).await,
        None | Some(false) => list_response(accept, list, query, state).await,
    }
}

fn publish(val: serde_json::Value) -> Result<Bytes, anyhow::Error> {
    Ok(Bytes::copy_from_slice(&serde_json::to_vec(&val)?))
}

#[get("{server}/apis/{group}/{version}/namespaces/{namespace}/{kind}")]
async fn apis_namespaced_list(
    accept: Header<Accept>,
    list: Path<List>,
    query: Query<Selector>,
    watch: Query<Watch>,
    state: web::Data<ApiState>,
) -> actix_web::Result<impl Responder> {
    match watch.watch {
        Some(true) => watch_response(accept, list, query, state).await,
        None | Some(false) => list_response(accept, list, query, state).await,
    }
}

async fn list_items(
    accept: Header<Accept>,
    list: Path<List>,
    query: Query<Selector>,
    state: web::Data<ApiState>,
) -> anyhow::Result<serde_json::Value> {
    let archive = state
        .archives
        .get(list.get_server())
        .ok_or(anyhow::anyhow!("Server not found"))?;
    let reader = state.to_reader(archive.clone()).await?;
    let list = archive.named_object_from_list(list.clone());
    let selector = query.0;
    Ok(match accept.0.as_slice() {
        [QualityItem { item, .. }, ..] if item.to_string().contains("as=Table") => {
            load_table(reader, list, selector).await?
        }
        _ => load_list(reader, list, selector).await?,
    })
}

async fn watch_events(
    accept: Header<Accept>,
    list: NamedObject,
    query: Query<Selector>,
    reader: &Reader,
) -> anyhow::Result<Vec<serde_json::Value>> {
    let selector = query.0;
    Ok(match accept.0.as_slice() {
        [QualityItem { item, .. }, ..] if item.to_string().contains("as=Table") => {
            watch_table_events(reader.clone(), list, selector).await?
        }
        _ => watch_events_cached(reader.clone(), list, selector).await?,
    })
}

// Regular responder with list of objects
async fn list_response(
    accept: Header<Accept>,
    list: Path<List>,
    query: Query<Selector>,
    state: web::Data<ApiState>,
) -> actix_web::Result<HttpResponse> {
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(serde_json::to_string(
            &list_items(accept, list, query, state)
                .await
                .map_err(error::ErrorNotFound)?,
        )?))
}

// Streaming responder with watch events
async fn watch_response(
    accept: Header<Accept>,
    list: Path<List>,
    query: Query<Selector>,
    state: web::Data<ApiState>,
) -> actix_web::Result<HttpResponse> {
    let archive = state
        .archives
        .get(list.get_server())
        .ok_or(anyhow::anyhow!("Server not found"))
        .map_err(error::ErrorNotFound)?;
    let reader = state
        .to_reader(archive.clone())
        .await
        .map_err(error::ErrorServiceUnavailable)?;
    let list = archive.named_object_from_list(list.clone());
    let mut refresh = Instant::now();
    let s = stream! {
        loop {
            for event in watch_events(accept.clone(), list.clone(), query.clone(), &reader).await? {
                yield publish(event);
            }
            let next_event_time = reader.pop_next_event_time();
            if next_event_time == Duration::MAX {
                break;
            }
            sleep(next_event_time.checked_sub(refresh.elapsed()).unwrap_or_default()).await;
            refresh = Instant::now();
        }
    };

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .streaming(s))
}

#[get("{server}/api/{version}/{kind}/{name}")]
async fn cluster_get(
    get: Path<Get>,
    state: web::Data<ApiState>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(
        get_item(get, state).await.map_err(error::ErrorNotFound)?,
    ))
}

#[get("{server}/apis/{group}/{version}/{kind}/{name}")]
async fn cluster_apis_get(
    get: Path<Get>,
    state: web::Data<ApiState>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(
        get_item(get, state).await.map_err(error::ErrorNotFound)?,
    ))
}

#[get("{server}/api/{version}/namespaces/{namespace}/{kind}/{name}")]
async fn namespaced_get(
    get: Path<Get>,
    state: web::Data<ApiState>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(
        get_item(get, state).await.map_err(error::ErrorNotFound)?,
    ))
}

#[get("{server}/apis/{group}/{version}/namespaces/{namespace}/{kind}/{name}")]
async fn namespaced_apis_get(
    get: Path<Get>,
    state: web::Data<ApiState>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(
        get_item(get, state).await.map_err(error::ErrorNotFound)?,
    ))
}

#[get("{server}/api/{version}/namespaces/{namespace}/{kind}/{name}/log")]
async fn logs_get(
    get: Path<Get>,
    query: Query<Log>,
    state: web::Data<ApiState>,
) -> actix_web::Result<impl Responder> {
    let archive = state
        .archives
        .get(get.get_server())
        .ok_or(anyhow::anyhow!("Server not found"))
        .map_err(error::ErrorNotFound)?;
    let reader = state
        .to_reader(archive.clone())
        .await
        .map_err(error::ErrorServiceUnavailable)?;
    let get = archive.named_object_from_get(get.clone());
    load_raw(reader, get.get_logs_path(query.deref()))
        .await
        .map_err(error::ErrorNotFound)
}

async fn get_item(get: Path<Get>, state: web::Data<ApiState>) -> anyhow::Result<serde_json::Value> {
    let archive = state
        .archives
        .get(get.get_server())
        .ok_or(anyhow::anyhow!("Server not found"))?;
    let reader = state.to_reader(archive.clone()).await?;
    let get = archive.named_object_from_get(get.clone());
    load(reader, get).await
}

#[cached(result)]
async fn load_raw(reader: Reader, path: ArchivePath) -> anyhow::Result<String> {
    reader.load_raw(path).await
}

#[cached(result)]
async fn load(reader: Reader, get: NamedObject) -> anyhow::Result<serde_json::Value> {
    reader.load(get).await
}

#[cached(result)]
async fn load_list(
    reader: Reader,
    list: NamedObject,
    selector: Selector,
) -> anyhow::Result<serde_json::Value> {
    reader.list(list, selector).await
}

#[cached(result)]
async fn load_table(
    reader: Reader,
    list: NamedObject,
    selector: Selector,
) -> anyhow::Result<serde_json::Value> {
    reader.load_table(list, selector).await
}

#[cached(result)]
async fn watch_table_events(
    reader: Reader,
    list: NamedObject,
    selector: Selector,
) -> anyhow::Result<Vec<serde_json::Value>> {
    reader.watch_table_events(list, selector).await
}

#[cached(result)]
async fn watch_events_cached(
    reader: Reader,
    list: NamedObject,
    selector: Selector,
) -> anyhow::Result<Vec<serde_json::Value>> {
    reader.watch_events(list, selector).await
}
