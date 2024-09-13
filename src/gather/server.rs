use std::{
    collections::HashMap, fmt::Display, fs::File, net::SocketAddr, ops::Deref, path::PathBuf,
    time::Duration,
};

use actix_web::{
    error, get,
    http::{
        header::{Accept, QualityItem, CONTENT_TYPE},
        KeepAlive,
    },
    post,
    web::{self, Bytes, Header, Path, Query},
    App, HttpResponse, HttpServer, Responder,
};
use async_stream::stream;
use clap::Parser;
use k8s_openapi::{
    chrono::{DateTime, Utc},
    serde_json::{self, json},
};
use kube::config::{Cluster, Context, Kubeconfig, NamedAuthInfo, NamedCluster, NamedContext};
use serde::Deserialize;
use tokio::time::{sleep, Instant};

use crate::gather::{
    reader::{Destination, Get, List, Log, Reader, Watch},
    writer::Archive,
};

use super::{representation::ArchivePath, selector::Selector, writer::ArchiveSearch};

#[derive(Clone, Deserialize)]
pub struct Socket(SocketAddr);

impl Default for Socket {
    fn default() -> Self {
        Self("0.0.0.0:8080".parse().unwrap())
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
    archive: ArchiveSearch,

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
    pub fn get_api(&self) -> anyhow::Result<Api> {
        let archives: Vec<_> = self.archive.clone().into();

        Api::new(archives, self.socket.clone(), self.kubeconfig.clone())
    }
}

pub struct Api {
    state: ApiState,
    socket: SocketAddr,
}

#[derive(Clone)]
struct ApiState {
    archives: HashMap<String, Archive>,
    serve_time: DateTime<Utc>,
}

impl Api {
    pub fn new(
        archives: impl IntoIterator<Item = Archive> + Clone,
        socket: Socket,
        kubeconfig: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        let Socket(socket) = socket;

        let config = archives
            .clone()
            .into_iter()
            .map(|a| Api::prepare_kubeconfig(&a, socket))
            .try_fold(Kubeconfig::default(), Kubeconfig::merge)?;

        let kubeconfig_path = kubeconfig.unwrap_or(std::path::PathBuf::from(
            std::env::var("KUBECONFIG")
                .unwrap_or(format!("{}/.kube/config", std::env::var("HOME")?).to_string()),
        ));

        serde_yaml::to_writer(File::create(kubeconfig_path)?, &config)?;

        let mut readers = HashMap::new();
        for archive in archives {
            readers.insert(archive.name().to_string_lossy().to_string(), archive);
        }

        Ok(Self {
            state: ApiState {
                archives: readers,
                serve_time: Utc::now(),
            },
            socket,
        })
    }

    fn prepare_kubeconfig(archive: &Archive, socket: SocketAddr) -> Kubeconfig {
        let name = archive.name().to_string_lossy().to_string();
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
                    user: name.clone(),
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

    pub async fn serve(self) -> std::io::Result<()> {
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
        .await
    }
}

#[get("{server}/version")]
async fn version(
    server: Path<Destination>,
    state: web::Data<ApiState>,
) -> actix_web::Result<impl Responder> {
    let version: serde_yaml::Value = serde_yaml::from_str({
        let archive = state
            .archives
            .get(server.get_server())
            .ok_or(error::ErrorNotFound(anyhow::anyhow!("Server not found")))?;
        Reader::new(archive.clone(), state.serve_time)
            .map_err(error::ErrorNotFound)?
            .load_raw(ArchivePath::Custom("version.yaml".into()))
            .map_err(error::ErrorNotFound)?
            .as_str()
    })
    .map_err(error::ErrorUnprocessableEntity)?;

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
    Ok(Reader::new(archive.clone(), state.serve_time)
        .map_err(error::ErrorNotFound)?
        .load_raw(ArchivePath::Custom("api.json".into()))
        .map_err(error::ErrorNotFound)?
        .customize()
        .insert_header((
            CONTENT_TYPE,
            "application/json;g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList",
        )))
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
    Ok(Reader::new(archive.clone(), state.serve_time)
        .map_err(error::ErrorNotFound)?
        .load_raw(ArchivePath::Custom("apis.json".into()))
        .map_err(error::ErrorNotFound)?
        .customize()
        .insert_header((
            CONTENT_TYPE,
            "application/json;g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList",
        )))
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
        Some(true) => watch_response(accept, list, query, state),
        None | Some(false) => list_response(accept, list, query, state),
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
        Some(true) => watch_response(accept, list, query, state),
        None | Some(false) => list_response(accept, list, query, state),
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
        Some(true) => watch_response(accept, list, query, state),
        None | Some(false) => list_response(accept, list, query, state),
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
        Some(true) => watch_response(accept, list, query, state),
        None | Some(false) => list_response(accept, list, query, state),
    }
}

fn list_items(
    accept: Header<Accept>,
    list: Path<List>,
    query: Query<Selector>,
    state: web::Data<ApiState>,
) -> anyhow::Result<serde_json::Value> {
    let archive = state
        .archives
        .get(list.get_server())
        .ok_or(anyhow::anyhow!("Server not found"))?;
    let reader = Reader::new(archive.clone(), state.serve_time)?;
    Ok(match accept.0.as_slice() {
        [QualityItem { item, .. }, ..] if item.to_string().contains("as=Table") => {
            reader.load_table(list.clone(), query.0)?
        }
        _ => reader.list(list.clone(), query.0)?,
    })
}

fn watch_events(
    accept: Header<Accept>,
    list: List,
    query: Query<Selector>,
    reader: &Reader,
) -> anyhow::Result<Vec<serde_json::Value>> {
    Ok(match accept.0.as_slice() {
        [QualityItem { item, .. }, ..] if item.to_string().contains("as=Table") => {
            reader.watch_table_events(list.clone(), query.0)?
        }
        _ => reader.watch_events(list.clone(), query.0)?,
    })
}

// Regular responder with list of objects
fn list_response(
    accept: Header<Accept>,
    list: Path<List>,
    query: Query<Selector>,
    state: web::Data<ApiState>,
) -> actix_web::Result<HttpResponse> {
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(serde_json::to_string(
            &list_items(accept, list, query, state).map_err(error::ErrorNotFound)?,
        )?))
}

// Streaming responder with watch events
fn watch_response(
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
    let reader = Reader::new(archive.clone(), state.serve_time).map_err(error::ErrorNotFound)?;
    let mut refresh = Instant::now();
    let s = stream! {
        loop {
            for event in watch_events(accept.clone(), list.clone(), query.clone(), &reader)? {
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
        get_item(get, state).map_err(error::ErrorNotFound)?,
    ))
}

#[get("{server}/apis/{group}/{version}/{kind}/{name}")]
async fn cluster_apis_get(
    get: Path<Get>,
    state: web::Data<ApiState>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(
        get_item(get, state).map_err(error::ErrorNotFound)?,
    ))
}

#[get("{server}/api/{version}/namespaces/{namespace}/{kind}/{name}")]
async fn namespaced_get(
    get: Path<Get>,
    state: web::Data<ApiState>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(
        get_item(get, state).map_err(error::ErrorNotFound)?,
    ))
}

#[get("{server}/apis/{group}/{version}/namespaces/{namespace}/{kind}/{name}")]
async fn namespaced_apis_get(
    get: Path<Get>,
    state: web::Data<ApiState>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(
        get_item(get, state).map_err(error::ErrorNotFound)?,
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
    let reader = Reader::new(archive.clone(), state.serve_time).map_err(error::ErrorNotFound)?;
    reader
        .load_raw(get.get_logs_path(query.deref()))
        .map_err(error::ErrorNotFound)
}

fn get_item(get: Path<Get>, state: web::Data<ApiState>) -> anyhow::Result<serde_json::Value> {
    let archive = state
        .archives
        .get(get.get_server())
        .ok_or(anyhow::anyhow!("Server not found"))?;
    let reader = Reader::new(archive.clone(), state.serve_time)?;
    reader.load(get.clone())
}
