use std::{
    collections::HashMap, fmt::Display, fs::File, net::SocketAddr, ops::Deref, path::PathBuf,
};

use actix_web::{
    error, get,
    http::header::{Accept, QualityItem, CONTENT_TYPE},
    post,
    web::{self, Header, Path, Query},
    App, HttpServer, Responder,
};
use clap::Parser;
use k8s_openapi::serde_json::{self, json};
use kube::config::{Cluster, Context, Kubeconfig, NamedAuthInfo, NamedCluster, NamedContext};
use serde::Deserialize;

use crate::gather::{
    reader::{Destination, Get, List, Log, Reader},
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
    /// If not provided, will attempt to use the default config for the environemnt.
    /// This can be defined by KUBECONFIG env variable.
    ///
    /// Example:
    ///     --kubeconfig=./kubeconfig
    #[arg(short, long, value_name = "KUBECONFIG")]
    kubeconfig: Option<PathBuf>,

    /// The input archive path. Will be used as a recursive search direcory for
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
    readers: HashMap<String, Reader>,
    socket: SocketAddr,
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

        let archives = archives
            .into_iter()
            .map(|a| (a.name().to_string_lossy().to_string(), Reader::new(&a)));

        Ok(Self {
            readers: archives.collect(),
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
                .app_data(web::Data::new(self.readers.clone()))
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
        .bind(self.socket)?
        .run()
        .await
    }
}

#[get("{server}/version")]
async fn version(
    server: Path<Destination>,
    reader: web::Data<HashMap<String, Reader>>,
) -> actix_web::Result<impl Responder> {
    let version: serde_yaml::Value = serde_yaml::from_str(
        reader
            .get(server.get_server())
            .ok_or(error::ErrorNotFound(anyhow::anyhow!("Server not found")))?
            .load_raw(ArchivePath::Custom("version.yaml".into()))
            .map_err(error::ErrorNotFound)?
            .as_str(),
    )
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
    reader: web::Data<HashMap<String, Reader>>,
) -> actix_web::Result<impl Responder> {
    Ok(reader
        .get(server.get_server())
        .ok_or(error::ErrorNotFound(anyhow::anyhow!("Server not found")))?
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
    reader: web::Data<HashMap<String, Reader>>,
) -> actix_web::Result<impl Responder> {
    Ok(reader
        .get(server.get_server())
        .ok_or(error::ErrorNotFound(anyhow::anyhow!("Server not found")))?
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
    reader: web::Data<HashMap<String, Reader>>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(
        list_items(accept, list, query, reader).map_err(error::ErrorNotFound)?,
    ))
}

#[get("{server}/apis/{group}/{version}/{kind}")]
async fn apis_list(
    accept: Header<Accept>,
    list: Path<List>,
    query: Query<Selector>,
    reader: web::Data<HashMap<String, Reader>>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(
        list_items(accept, list, query, reader).map_err(error::ErrorNotFound)?,
    ))
}

#[get("{server}/api/{version}/namespaces/{namespace}/{kind}")]
async fn api_namespaced_list(
    accept: Header<Accept>,
    list: Path<List>,
    query: Query<Selector>,
    reader: web::Data<HashMap<String, Reader>>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(
        list_items(accept, list, query, reader).map_err(error::ErrorNotFound)?,
    ))
}

#[get("{server}/apis/{group}/{version}/namespaces/{namespace}/{kind}")]
async fn apis_namespaced_list(
    accept: Header<Accept>,
    list: Path<List>,
    query: Query<Selector>,
    reader: web::Data<HashMap<String, Reader>>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(
        list_items(accept, list, query, reader).map_err(error::ErrorNotFound)?,
    ))
}

fn list_items(
    accept: Header<Accept>,
    list: Path<List>,
    query: Query<Selector>,
    reader: web::Data<HashMap<String, Reader>>,
) -> anyhow::Result<serde_json::Value> {
    let server = reader
        .get(list.get_server())
        .ok_or(anyhow::anyhow!("Server not found"))?;
    Ok(match accept.0.as_slice() {
        [QualityItem { item, .. }, ..] if item.to_string().contains("as=Table") => {
            server.load_table(list.clone(), query.0)?
        }
        _ => server.load_list(list.clone(), query.0)?,
    })
}

#[get("{server}/api/{version}/{kind}/{name}")]
async fn cluster_get(
    get: Path<Get>,
    reader: web::Data<HashMap<String, Reader>>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(
        get_item(get, reader).map_err(error::ErrorNotFound)?,
    ))
}

#[get("{server}/apis/{group}/{version}/{kind}/{name}")]
async fn cluster_apis_get(
    get: Path<Get>,
    reader: web::Data<HashMap<String, Reader>>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(
        get_item(get, reader).map_err(error::ErrorNotFound)?,
    ))
}

#[get("{server}/api/{version}/namespaces/{namespace}/{kind}/{name}")]
async fn namespaced_get(
    get: Path<Get>,
    reader: web::Data<HashMap<String, Reader>>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(
        get_item(get, reader).map_err(error::ErrorNotFound)?,
    ))
}

#[get("{server}/apis/{group}/{version}/namespaces/{namespace}/{kind}/{name}")]
async fn namespaced_apis_get(
    get: Path<Get>,
    reader: web::Data<HashMap<String, Reader>>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(
        get_item(get, reader).map_err(error::ErrorNotFound)?,
    ))
}

#[get("{server}/api/{version}/namespaces/{namespace}/{kind}/{name}/log")]
async fn logs_get(
    get: Path<Get>,
    query: Query<Log>,
    reader: web::Data<HashMap<String, Reader>>,
) -> actix_web::Result<impl Responder> {
    reader
        .get(get.get_server())
        .ok_or(error::ErrorNotFound(anyhow::anyhow!("Server not found")))?
        .load_raw(get.get_logs_path(query.deref()))
        .map_err(error::ErrorNotFound)
}

fn get_item(
    get: Path<Get>,
    reader: web::Data<HashMap<String, Reader>>,
) -> anyhow::Result<serde_json::Value> {
    reader
        .get(get.get_server())
        .ok_or(anyhow::anyhow!("Server not found"))?
        .load(get.clone())
}
