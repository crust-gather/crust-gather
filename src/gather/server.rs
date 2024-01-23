use std::{fmt::Display, fs::File, net::SocketAddr, ops::Deref, path::PathBuf};

use actix_web::{
    error, get,
    http::header::{Accept, QualityItem, CONTENT_TYPE},
    post,
    web::{self, Header, Path, Query},
    App, HttpServer, Responder,
};
use clap::Parser;
use k8s_openapi::serde_json::{self, json};
use kube::config::Kubeconfig;
use serde::Deserialize;

use crate::gather::{
    reader::{Get, List, Log, Reader},
    writer::Archive,
};

use super::representation::ArchivePath;

#[derive(Clone, Deserialize)]
pub struct Socket(SocketAddr);

impl Default for Socket {
    fn default() -> Self {
        Self("127.0.0.1:8080".parse().unwrap())
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

    /// The input archive path.
    /// Defaults to a new archive with name "crust-gather".
    ///
    /// Example:
    ///     --file=./artifacts
    #[arg(short, long, value_name = "PATH", default_value_t = Default::default())]
    #[serde(default)]
    archive: Archive,

    /// The socket address to bind the HTTP server to.
    /// Defaults to 127.0.0.1:8080.
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
        Api::new(
            self.archive.clone(),
            self.socket.clone(),
            self.kubeconfig.clone(),
        )
    }
}

pub struct Api {
    reader: Reader,
    socket: SocketAddr,
}

impl Api {
    pub fn new(
        archive: Archive,
        socket: Socket,
        kubeconfig: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        let Socket(socket) = socket;

        Api::place_kubeconfig(kubeconfig, socket)?;

        Ok(Self {
            reader: Reader::new(&archive)?,
            socket,
        })
    }

    fn place_kubeconfig(
        kubeconfig_path: Option<PathBuf>,
        socket: SocketAddr,
    ) -> anyhow::Result<()> {
        let kubeconfig_path = match kubeconfig_path {
            Some(kubeconfig) => kubeconfig,
            None => std::path::PathBuf::from(std::env::var("KUBECONFIG")?),
        };

        let kubeconfig: Kubeconfig = serde_json::from_value(json!({
            "apiVersion": "v1",
            "clusters": [{
                "cluster": {
                    "server": socket.to_string(),
                },
                "name": "snapshot",
            }],
            "contexts": [{
                "context": {
                    "cluster": "snapshot",
                    "user": "snapshot",
                },
                "name": "snapshot",
            }],
            "current-context": "snapshot",
            "users": [{
                "name": "snapshot",
            }],
        }))?;

        Ok(serde_yaml::to_writer(
            File::create(kubeconfig_path)?,
            &kubeconfig,
        )?)
    }

    pub async fn serve(self) -> std::io::Result<()> {
        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(self.reader.clone()))
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

#[get("version")]
async fn version(reader: web::Data<Reader>) -> actix_web::Result<impl Responder> {
    let version: serde_yaml::Value = serde_yaml::from_str(
        reader
            .load_raw(ArchivePath::Custom("version.yaml".into()))
            .map_err(error::ErrorNotFound)?
            .as_str(),
    )
    .map_err(error::ErrorUnprocessableEntity)?;

    Ok(web::Json(version))
}

#[post("apis/authorization.k8s.io/v1/selfsubjectaccessreviews")]
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

#[get("api")]
async fn api(reader: web::Data<Reader>) -> actix_web::Result<impl Responder> {
    Ok(reader
        .load_raw(ArchivePath::Custom("api.json".into()))
        .map_err(error::ErrorNotFound)?
        .customize()
        .insert_header((
            CONTENT_TYPE,
            "application/json;g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList",
        )))
}

#[get("apis")]
async fn apis(reader: web::Data<Reader>) -> actix_web::Result<impl Responder> {
    Ok(reader
        .load_raw(ArchivePath::Custom("apis.json".into()))
        .map_err(error::ErrorNotFound)?
        .customize()
        .insert_header((
            CONTENT_TYPE,
            "application/json;g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList",
        )))
}

#[get("api/{version}/{kind}")]
async fn api_list(
    accept: Header<Accept>,
    list: Path<List>,
    reader: web::Data<Reader>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(match accept.0.as_slice() {
        [QualityItem { item, .. }, ..] if item.to_string().contains("as=Table") => reader
            .load_table(list.clone())
            .map_err(error::ErrorNotFound)?,
        _ => reader
            .load_list(list.clone())
            .map_err(error::ErrorNotFound)?,
    }))
}

#[get("apis/{group}/{version}/{kind}")]
async fn apis_list(
    accept: Header<Accept>,
    list: Path<List>,
    reader: web::Data<Reader>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(match accept.0.as_slice() {
        [QualityItem { item, .. }, ..] if item.to_string().contains("as=Table") => reader
            .load_table(list.clone())
            .map_err(error::ErrorNotFound)?,
        _ => reader
            .load_list(list.clone())
            .map_err(error::ErrorNotFound)?,
    }))
}

#[get("api/{version}/namespaces/{namespace}/{kind}")]
async fn api_namespaced_list(
    accept: Header<Accept>,
    list: Path<List>,
    reader: web::Data<Reader>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(match accept.0.as_slice() {
        [QualityItem { item, .. }, ..] if item.to_string().contains("as=Table") => reader
            .load_table(list.clone())
            .map_err(error::ErrorNotFound)?,
        _ => reader
            .load_list(list.clone())
            .map_err(error::ErrorNotFound)?,
    }))
}

#[get("apis/{group}/{version}/namespaces/{namespace}/{kind}")]
async fn apis_namespaced_list(
    accept: Header<Accept>,
    list: Path<List>,
    reader: web::Data<Reader>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(match accept.0.as_slice() {
        [QualityItem { item, .. }, ..] if item.to_string().contains("as=Table") => reader
            .load_table(list.clone())
            .map_err(error::ErrorNotFound)?,
        _ => reader
            .load_list(list.clone())
            .map_err(error::ErrorNotFound)?,
    }))
}

#[get("api/{version}/{kind}/{name}")]
async fn cluster_get(
    get: Path<Get>,
    reader: web::Data<Reader>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(
        reader.load(get.clone()).map_err(error::ErrorNotFound)?,
    ))
}

#[get("apis/{group}/{version}/{kind}/{name}")]
async fn cluster_apis_get(
    get: Path<Get>,
    reader: web::Data<Reader>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(
        reader.load(get.clone()).map_err(error::ErrorNotFound)?,
    ))
}

#[get("api/{version}/namespaces/{namespace}/{kind}/{name}")]
async fn namespaced_get(
    get: Path<Get>,
    reader: web::Data<Reader>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(
        reader.load(get.clone()).map_err(error::ErrorNotFound)?,
    ))
}

#[get("apis/{group}/{version}/namespaces/{namespace}/{kind}/{name}")]
async fn namespaced_apis_get(
    get: Path<Get>,
    reader: web::Data<Reader>,
) -> actix_web::Result<impl Responder> {
    Ok(web::Json(
        reader.load(get.clone()).map_err(error::ErrorNotFound)?,
    ))
}

#[get("api/{version}/namespaces/{namespace}/{kind}/{name}/log")]
async fn logs_get(
    get: Path<Get>,
    query: Query<Log>,
    reader: web::Data<Reader>,
) -> actix_web::Result<impl Responder> {
    reader
        .load_raw(get.get_logs_path(query.deref()))
        .map_err(error::ErrorNotFound)
}
