use anyhow::Context;
use backon::{ExponentialBuilder, Retryable};
use base64::{Engine as _, prelude::BASE64_STANDARD};
use flate2::{Compression, write::GzEncoder};

use chrono::Utc;
use futures::{
    StreamExt as _, TryStreamExt as _,
    future::{self},
    stream, try_join,
};
use json_patch::diff;
use k8s_openapi::serde_json;
use oci_client::{
    Client, Reference,
    client::{ClientConfig, Config},
    errors::OciDistributionError,
    manifest::{IMAGE_LAYER_MEDIA_TYPE, OCI_IMAGE_MEDIA_TYPE, OciDescriptor, OciImageManifest},
    secrets::RegistryAuth,
};
use serde::{Deserialize, Serialize};
use sha2::Digest as _;
use std::{
    borrow::Cow,
    collections::BTreeMap,
    ffi::OsStr,
    fmt::Display,
    fs::{DirBuilder, File},
    io::{Read as _, Write as _},
    ops::Deref,
    os::unix::fs::MetadataExt,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tar::{Builder, Header};
use tokio::sync::Mutex;
use tracing::{debug, info, instrument};
use walkdir::WalkDir;
use zip::{ZipWriter, result::ZipError, write::SimpleFileOptions};

use crate::cli::DEFAULT_OCI_BUFFER_SIZE;
use crate::gather::{
    reader::{ArchiveReader, Reader},
    storage::Storage,
};

use super::representation::{ArchivePath, Representation};

#[derive(Clone, Deserialize)]
pub struct ArchiveSearch(PathBuf);

impl ArchiveSearch {
    pub fn path(&self) -> PathBuf {
        self.0.clone()
    }
}

impl Default for ArchiveSearch {
    fn default() -> Self {
        Self("crust-gather".into())
    }
}

impl From<ArchiveSearch> for PathBuf {
    fn from(val: ArchiveSearch) -> Self {
        val.0
    }
}

impl Display for ArchiveSearch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.display())
    }
}

impl From<&str> for ArchiveSearch {
    fn from(value: &str) -> Self {
        Self(PathBuf::from(value))
    }
}

impl From<ArchiveSearch> for Vec<Archive> {
    fn from(value: ArchiveSearch) -> Self {
        WalkDir::new(value.0)
            .max_depth(5)
            .same_file_system(true)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|f| f.file_name() == "version.yaml")
            .map(|f| {
                f.path()
                    .parent()
                    .unwrap_or(PathBuf::from("snapshot").as_path())
                    .to_path_buf()
            })
            .map(Into::into)
            .collect()
    }
}

#[derive(Clone, Deserialize)]
pub struct Archive(PathBuf);

/// Creates a new Archive instance with the given path.
impl Archive {
    pub fn new(path: PathBuf) -> Self {
        Self(path)
    }

    pub fn path(&self) -> PathBuf {
        self.0.clone()
    }

    pub fn name(&self) -> &OsStr {
        self.0
            .components()
            .next_back()
            .map(|c| c.as_os_str())
            .unwrap_or(OsStr::new("snapshot"))
    }

    pub fn join(&self, path: ArchivePath) -> PathBuf {
        match path {
            ArchivePath::Empty => self.path(),
            ArchivePath::Cluster(path) => self.path().join(path),
            ArchivePath::Namespaced(path) => self.path().join(path),
            ArchivePath::NamespacedList(path) => self.path().join(path),
            ArchivePath::ClusterList(path) => self.path().join(path),
            ArchivePath::Logs(path) => self.path().join(path),
            ArchivePath::Custom(path) => self.path().join(path),
        }
    }
}

impl Default for Archive {
    fn default() -> Self {
        Self("crust-gather".into())
    }
}

impl From<Archive> for PathBuf {
    fn from(val: Archive) -> Self {
        val.0
    }
}

impl From<PathBuf> for Archive {
    fn from(val: PathBuf) -> Archive {
        Archive(val)
    }
}

impl Display for Archive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.display())
    }
}

impl From<&str> for Archive {
    fn from(value: &str) -> Self {
        Self(PathBuf::from(value))
    }
}

#[derive(Clone, Default, Deserialize)]
/// The Encoding enum represents the supported archive encoding formats.
/// - Path indicates no encoding.
/// - Gzip indicates gzip compression should be used.
/// - Zip indicates zip compression should be used.
pub enum Encoding {
    #[default]
    Path,
    Gzip,
    Zip,
    Oci(Reference),
}

impl From<&str> for Encoding {
    fn from(value: &str) -> Self {
        match value {
            "zip" => Self::Zip,
            "gzip" => Self::Gzip,
            _ => Self::Path,
        }
    }
}

/// The Writer enum represents the different archive writer implementations.
/// Gzip uses the gzip compression format.
/// Zip uses the zip compression format.
/// Oci uses the remote image reference as a destination.
pub enum Writer {
    Path(Archive),
    Gzip(Archive, Box<Builder<GzEncoder<File>>>),
    Zip(Archive, Box<ZipWriter<File>>),
    Oci(OCIState),
}

// OCIState holds current OCI writer destination state
pub struct OCIState {
    archive: Archive,
    client: Client,
    image_ref: Box<Reference>,
    auth: RegistryAuth,
    buffer_size: usize,
}

// YamlPath contains a full path in the yaml file in archive
// and a range of bytes to extract yaml from a list
#[derive(Serialize, Deserialize)]
pub struct YamlPath {
    pub path: PathBuf,
    pub from: usize,
    pub to: usize,
}

impl From<Writer> for Arc<Mutex<Writer>> {
    fn from(val: Writer) -> Self {
        Self::new(Mutex::new(val))
    }
}

impl Writer {
    /// Finish zip archive
    pub fn finish_zip(self) -> anyhow::Result<()> {
        let Self::Zip(_, builder) = self else {
            return anyhow::Result::Ok(());
        };

        builder.finish()?;
        Ok(())
    }

    /// Finish gzip archive
    pub fn finish_gzip(&mut self) -> anyhow::Result<()> {
        let Self::Gzip(_, builder) = self else {
            return anyhow::Result::Ok(());
        };

        Ok(builder.finish()?)
    }

    /// Finish writing the archive, finalizing any compression and flushing buffers.
    pub async fn finish_oci(&self) -> anyhow::Result<()> {
        if let Writer::Oci(ocistate) = self {
            return ocistate.publish_image().await;
        }

        Ok(())
    }

    /// Adds a representation data to the archive under the representation path
    #[instrument(skip_all, fields(repr = repr.path().to_string()))]
    pub async fn store(&mut self, repr: &Representation) -> anyhow::Result<()> {
        tracing::debug!("Writing...");

        let archive_path: String = repr.path().try_into()?;
        let data = repr.data();

        match self {
            Self::Path(Archive(archive))
            | Self::Oci(OCIState {
                archive: Archive(archive),
                ..
            }) => {
                let file = archive.join(archive_path);
                if !file.exists() {
                    DirBuilder::new()
                        .recursive(true)
                        .create(file.parent().unwrap())?;
                    let mut file = File::create(file)?;
                    file.write_all(data.as_bytes())?;
                }
            }
            Self::Gzip(Archive(archive), builder) => {
                let mut header = Header::new_gnu();
                header.set_size(data.len() as u64 + 1);
                header.set_cksum();
                header.set_mode(0o644);

                let root_prefix = archive.file_stem().unwrap();
                let file = PathBuf::from(root_prefix).join(archive_path);
                builder.append_data(&mut header, file, data.as_bytes())?;
            }
            Self::Zip(Archive(archive), writer) => {
                let path = repr.path();
                let path = path.parent().unwrap().to_str().unwrap();
                writer
                    .add_directory(path, SimpleFileOptions::default())
                    .or_else(|err| match err {
                        ZipError::InvalidArchive(Cow::Borrowed("Duplicate filename")) => Ok(()),
                        other => Err(other),
                    })?;

                let root_prefix = archive.file_stem().unwrap();
                let file = PathBuf::from(root_prefix).join(archive_path);
                let file = file.to_str().unwrap();
                writer
                    .start_file(file, SimpleFileOptions::default())
                    .or_else(|err| match err {
                        ZipError::InvalidArchive(Cow::Borrowed("Duplicate filename")) => Ok(()),
                        other => Err(other),
                    })?;
                writer.write_all(data.as_bytes())?;
            }
        }
        Ok(())
    }

    /// Adds a representation data to the archive under the representation path
    #[instrument(skip_all, fields(repr = repr.path().to_string()))]
    pub async fn sync(&mut self, repr: &Representation) -> anyhow::Result<()> {
        tracing::debug!("Writing...");

        let archive_path: String = repr.path().try_into()?;

        match self {
            Self::Path(archive) => {
                let file_path = archive.0.join(archive_path);

                // generate diff and write
                let original = Reader::new(
                    ArchiveReader::new(archive.clone(), &Storage::FS, DEFAULT_OCI_BUFFER_SIZE)
                        .await,
                    Utc::now(),
                    Storage::FS,
                )
                .await?
                .read(file_path.clone())
                .await?;
                let updated = serde_saphyr::from_str(repr.data())?;
                let patch = &diff(&original, &updated);
                if !patch.deref().is_empty() {
                    let mut patches = File::options()
                        .create(true)
                        .append(true)
                        .open(file_path.with_extension("patch"))?;
                    serde_json::to_writer(patches.try_clone()?, patch)?;
                    patches.write_all(b"\n")?;
                }
                self.store(repr).await?;
            }
            Self::Gzip(Archive(_archive), _builder) => {
                unimplemented!();
            }
            Self::Zip(Archive(_archive), _writer) => {
                unimplemented!();
            }
            Self::Oci(..) => {
                unimplemented!();
            }
        }
        Ok(())
    }

    /// Creates a new `Writer` for the given `Archive` and `Encoding`.
    pub async fn new(
        archive: &Archive,
        encoding: &Encoding,
        client_config: Option<ClientConfig>,
        auth: Option<RegistryAuth>,
        buffer_size: usize,
    ) -> anyhow::Result<Self> {
        let buffer_size = buffer_size.max(1);
        match archive.0.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => {
                DirBuilder::new().recursive(true).create(parent)?;
            }
            Some(_) | None => (),
        };

        Ok(match encoding {
            Encoding::Path => Self::Path(archive.clone()),
            Encoding::Gzip => Self::Gzip(
                archive.clone(),
                Box::new(Builder::new(GzEncoder::new(
                    File::create(archive.0.with_extension("tar.gz"))?,
                    Compression::default(),
                ))),
            ),
            Encoding::Zip => Self::Zip(
                archive.clone(),
                Box::new(ZipWriter::new(File::create(
                    archive.0.with_extension("zip"),
                )?)),
            ),
            Encoding::Oci(image_ref) => Self::Oci(OCIState {
                archive: archive.clone(),
                client: Client::new(client_config.unwrap_or_default()),
                image_ref: image_ref.clone().into(),
                auth: auth.unwrap_or(RegistryAuth::Anonymous),
                buffer_size,
            }),
        })
    }
}

impl OCIState {
    #[instrument(skip_all, err)]
    async fn publish_image(&self) -> anyhow::Result<()> {
        info!("Pushing image: {:?}", self.image_ref);
        self.client
            .store_auth_if_needed(self.image_ref.resolve_registry(), &self.auth)
            .await;

        let config = Config::new(b"{}".to_vec(), OCI_IMAGE_MEDIA_TYPE.to_string(), None);
        let paths = glob::glob(&format!(
            "{}/**/*",
            self.archive
                .path()
                .to_str()
                .ok_or(anyhow::anyhow!("archive path is not convertable to string"))?,
        ))?;

        let resource_paths = Arc::new(Mutex::new(Default::default()));
        let non_resource_paths: Vec<PathBuf> = stream::iter(paths.into_iter())
            .filter_map(|path| Self::prepare_resource_layer(path, resource_paths.clone()))
            .collect()
            .await;

        // Upload layers
        let layers = Arc::new(Mutex::new(vec![]));
        let raw_layers = stream::iter(non_resource_paths.into_iter())
            .map(|path| self.push_oci_archive_layer(path, layers.clone()))
            .buffer_unordered(self.buffer_size)
            .try_for_each(future::ok::<(), anyhow::Error>);

        let resource_layer_entries = { resource_paths.lock().await.clone() };
        let resources = stream::iter(resource_layer_entries)
            .filter_map(|(p, yamls)| {
                future::ready(
                    Self::combined_oci_archive_layer(&yamls)
                        .ok()
                        .map(|data| (p + ".yaml", data)),
                )
            })
            .map(|(p, data)| self.push_oci_layer(p, data, layers.clone()))
            .buffer_unordered(self.buffer_size)
            .try_for_each(future::ok::<(), anyhow::Error>);

        let yamls = Self::prepare_index(resource_paths.lock().await.values().cloned().collect())?;
        let yamls =
            serde_saphyr::to_string(&yamls).context("unable to collect yamls index file")?;

        let index_layer = self.push_oci_layer("index.yaml".to_string(), yamls, layers.clone());

        try_join!(resources, raw_layers, index_layer).context("failed to upload OCI layers")?;

        let mut manifest = OciImageManifest::default();
        manifest.config.media_type = config.media_type.to_string();
        manifest.config.size = config.data.len() as i64;
        manifest.config.digest =
            format!("sha256:{}", hex::encode(sha2::Sha256::digest(&config.data)));
        manifest.layers = layers.lock().await.clone();
        manifest.layers.sort_by(|a, b| a.digest.cmp(&b.digest));

        info!("Pushing config: {}", self.image_ref);
        let push = || {
            self.client.push_blob(
                &self.image_ref,
                config.data.clone(),
                &manifest.config.digest,
            )
        };
        push.retry(
            ExponentialBuilder::default()
                .with_max_times(20)
                .with_max_delay(Duration::from_secs(10))
                .with_jitter(),
        )
        .when(|e| matches!(e, OciDistributionError::ServerError { code, .. } if *code == 429 ))
        .await?;

        info!("Pushing manifest: {}", self.image_ref);
        let manifest = &manifest.into();
        let push = || self.client.push_manifest(&self.image_ref, manifest);
        push.retry(
            ExponentialBuilder::default()
                .with_max_times(20)
                .with_max_delay(Duration::from_secs(10))
                .with_jitter(),
        )
        .when(|e| matches!(e, OciDistributionError::ServerError { code, .. } if *code == 429 ))
        .await?;

        Ok(())
    }

    fn prepare_index(yamls: Vec<Vec<PathBuf>>) -> anyhow::Result<Vec<YamlPath>> {
        let mut list = vec![];
        for yaml_list in yamls {
            let mut index = 0;
            for yaml in yaml_list {
                let path = yaml.to_string_lossy();
                let file = File::open(&yaml).context(format!("failed to open file {path}"))?;
                let value: serde_json::Value = serde_saphyr::from_reader(file)
                    .context(format!("failed to read file {path}"))?;
                let data = serde_saphyr::to_string(&value)
                    .context(format!("failed to serialize file {path}"))?;

                list.push(YamlPath {
                    path: yaml,
                    from: index,
                    to: {
                        index += data.len();
                        index
                    },
                });
                index += 4
            }
        }

        Ok(list)
    }

    async fn prepare_resource_layer(
        path: glob::GlobResult,
        resource_layers: Arc<Mutex<BTreeMap<String, Vec<PathBuf>>>>,
    ) -> Option<PathBuf> {
        let path = path.ok()?;

        if path.is_dir() {
            return Some(path);
        }

        let Some(parent) = path.parent() else {
            return Some(path);
        };

        let Some(ext) = path.extension() else {
            return Some(path);
        };

        if ext != "yaml" || path.to_string_lossy().ends_with("version.yaml") {
            return Some(path);
        }

        let parent = parent.to_str()?;

        debug!("Inserting path {:?} with parent {:?}", path, parent);
        resource_layers
            .lock()
            .await
            .entry(parent.to_string())
            .or_default()
            .push(path);

        None
    }

    async fn push_oci_layer(
        &self,
        archive_path: String,
        mut data: String,
        layers: Arc<Mutex<Vec<OciDescriptor>>>,
    ) -> anyhow::Result<()> {
        if data.is_empty() {
            // That could only happen for empty logs, so we publish an empty json instead
            // as ghcr doesn't allow empty layers
            data = "{}".to_string();
        };
        let size = data.len() as i64;
        let digest = format!(
            "sha256:{}",
            hex::encode(sha2::Sha256::digest(data.as_bytes()))
        );
        {
            layers.lock().await.push(OciDescriptor {
                urls: None,
                media_type: IMAGE_LAYER_MEDIA_TYPE.to_string(),
                digest: digest.clone(),
                size,
                annotations: Some(
                    [(
                        "org.opencontainers.image.title".to_string(),
                        archive_path.to_string(),
                    )]
                    .into(),
                ),
            });
        }

        info!("Pushing layer: {:?}", archive_path);
        let push = || {
            self.client
                .push_blob(&self.image_ref, data.clone(), &digest)
        };
        push.retry(
            ExponentialBuilder::default()
                .with_max_times(20)
                .with_max_delay(Duration::from_secs(10))
                .with_jitter(),
        )
        .when(|e| matches!(e, OciDistributionError::ServerError { code, .. } if *code == 429 ))
        .notify(|e, dur| {
            debug!("Pushing layer: {archive_path:?} - retry after {dur:?} due to {e:?}")
        })
        .await
        .context(format!("failed to push layer for file {archive_path}"))?;

        anyhow::Result::Ok(())
    }

    #[instrument(skip_all, err)]
    fn combined_oci_archive_layer(yamls: &Vec<PathBuf>) -> anyhow::Result<String> {
        let mut files: Vec<serde_json::Value> = vec![];
        for yaml in yamls {
            let path = yaml.to_string_lossy();
            let file = File::open(yaml).context(format!("failed to open file {path}"))?;
            files.push(
                serde_saphyr::from_reader(file).context(format!("failed to read file {path}"))?,
            );
        }

        let mut enc = GzEncoder::new(vec![], Compression::best());
        let data = serde_saphyr::to_string_multiple(&files)
            .context("failed to serialize a list of yamls")?;
        enc.write_all(data.as_bytes())?;

        Ok(BASE64_STANDARD.encode(enc.finish()?))
    }

    async fn push_oci_archive_layer(
        &self,
        path: PathBuf,
        layers: Arc<Mutex<Vec<OciDescriptor>>>,
    ) -> anyhow::Result<()> {
        if path.is_dir() {
            return anyhow::Result::Ok(());
        }
        let archive_path = path.clone();
        let archive_path = archive_path
            .to_str()
            .ok_or(anyhow::anyhow!("file path is not convertable to string"))?;
        let mut file = File::open(path).context(format!("failed to open file {archive_path}"))?;
        let mut data = String::new();
        File::read_to_string(&mut file, &mut data)
            .context(format!("failed to read file {archive_path}"))?;

        self.push_oci_layer(archive_path.to_string(), data, layers)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        fs::{self},
    };

    use tempfile::TempDir;

    use crate::{
        cli::DEFAULT_OCI_BUFFER_SIZE,
        gather::{config::Secrets, representation::ArchivePath, writer::Representation},
    };

    use super::{Archive, Encoding, Writer};

    #[tokio::test]
    async fn test_new_gzip() {
        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test.tar.gz");
        let archive = Archive::new(archive);
        let result = Writer::new(
            &archive,
            &Encoding::Gzip,
            None,
            None,
            DEFAULT_OCI_BUFFER_SIZE,
        );

        assert!(result.await.is_ok());
    }

    #[tokio::test]
    async fn test_new_zip() {
        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test.zip");
        let archive = Archive::new(archive);
        let result = Writer::new(
            &archive,
            &Encoding::Zip,
            None,
            None,
            DEFAULT_OCI_BUFFER_SIZE,
        );

        assert!(result.await.is_ok());
    }

    #[tokio::test]
    async fn test_add_gzip() {
        use crate::gather::representation::ArchivePath;

        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test");
        let mut writer = Writer::new(
            &Archive::new(archive.clone()),
            &Encoding::Gzip,
            None,
            None,
            DEFAULT_OCI_BUFFER_SIZE,
        )
        .await
        .unwrap();

        let repr = Representation::new()
            .with_data("content")
            .with_path(ArchivePath::Custom("test.txt".into()));

        assert!(writer.store(&repr).await.is_ok());
        assert!(writer.finish_gzip().is_ok());
        assert!(archive.with_file_name("test.tar.gz").exists());
    }

    #[tokio::test]
    async fn test_add_zip() {
        use std::{
            fs::File,
            io::{Read, Seek},
        };

        use crate::gather::representation::ArchivePath;

        unsafe {
            env::set_var("SECRET", "secret");
        }

        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test.zip");
        let mut writer = Writer::new(
            &Archive::new(archive.clone()),
            &Encoding::Zip,
            None,
            None,
            DEFAULT_OCI_BUFFER_SIZE,
        )
        .await
        .unwrap();

        let repr = Representation::new()
            .with_path(ArchivePath::Custom("test.txt".into()))
            .with_data("content with secret");

        let secret: Secrets = vec!["SECRET".into()].into();
        assert!(writer.store(&secret.strip(&repr)).await.is_ok());
        assert!(writer.finish_zip().is_ok());
        assert!(archive.exists());

        fn check_zip_contents(reader: impl Read + Seek) {
            let mut zip = zip::ZipArchive::new(reader).unwrap();
            let mut file = zip.by_name("test/test.txt").unwrap();

            let mut data = String::new();
            file.read_to_string(&mut data).unwrap();
            assert_eq!(data, "content with xxx");
        }

        check_zip_contents(File::open(archive).unwrap());
    }

    #[tokio::test]
    async fn test_add_path() {
        unsafe { env::set_var("SECRET", "secret") };

        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let archive = tmp_dir.path().join("cluster1/collected");
        let mut writer = Writer::new(
            &Archive::new(archive.clone()),
            &Encoding::Path,
            None,
            None,
            DEFAULT_OCI_BUFFER_SIZE,
        )
        .await
        .unwrap();

        let repr = Representation::new()
            .with_data("content with secret")
            .with_path(ArchivePath::Namespaced("test.txt".into()));

        let secret: Secrets = vec!["SECRET".into()].into();
        assert!(writer.store(&secret.strip(&repr)).await.is_ok());
        assert!(archive.exists());
        assert!(archive.join("test.txt").exists());
        let data = fs::read_to_string(archive.join("test.txt")).unwrap();
        assert_eq!(data, "content with xxx");
    }

    #[tokio::test]
    async fn test_try_into_nested_file_success() {
        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let tmp_dir = tmp_dir.path();
        Writer::new(
            &Archive::new(tmp_dir.join("nested/output.zip")),
            &Encoding::Zip,
            None,
            None,
            DEFAULT_OCI_BUFFER_SIZE,
        )
        .await
        .unwrap();

        assert!(tmp_dir.join("nested/output.zip").exists());
    }

    #[tokio::test]
    async fn test_try_into_writer_empty_path() {
        assert!(
            Writer::new(
                &Archive::new("".into()),
                &Encoding::Zip,
                None,
                None,
                DEFAULT_OCI_BUFFER_SIZE,
            )
            .await
            .is_err()
        );
    }
}
