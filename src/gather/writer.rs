use std::{
    fmt::Display,
    fs::{DirBuilder, File},
    io::Write as _,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use flate2::{write::GzEncoder, Compression};
use kube::{
    config::{self, KubeConfigOptions},
    Client,
};
use serde::Deserialize;
use tar::{Builder, Header};
use zip::{write::FileOptions, ZipWriter};

#[derive(Clone, Deserialize)]
pub struct Archive(PathBuf);

/// Creates a new Archive instance with the given path.
impl Archive {
    pub fn new(path: PathBuf) -> Self {
        Self(path)
    }
}

impl Default for Archive {
    fn default() -> Self {
        Self("crust-gather".into())
    }
}

impl Into<PathBuf> for Archive {
    fn into(self) -> PathBuf {
        self.0
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

#[derive(Clone, Deserialize)]
/// KubeconfigFile wraps a Kubeconfig struct used to instantiate a Kubernetes client.
pub struct KubeconfigFile(config::Kubeconfig);

impl KubeconfigFile {
    /// Creates a new Kubernetes client from the KubeconfigFile.
    pub async fn client(&self) -> anyhow::Result<Client> {
        Ok(Client::try_from(
            kube::Config::from_custom_kubeconfig(self.into(), &KubeConfigOptions::default())
                .await?,
        )?)
    }
}

impl TryFrom<String> for KubeconfigFile {
    type Error = anyhow::Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Ok(Self(serde_yaml::from_reader(File::open(s)?)?))
    }
}

impl Into<config::Kubeconfig> for &KubeconfigFile {
    fn into(self) -> config::Kubeconfig {
        self.0.clone()
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
}

impl From<&str> for Encoding {
    fn from(value: &str) -> Self {
        match value {
            "zip" => Encoding::Zip,
            "gzip" => Encoding::Gzip,
            _ => Encoding::Path,
        }
    }
}

#[derive(Clone, Default, Debug)]
/// Representation holds the path and content for a serialized Kubernetes object.
pub struct Representation {
    path: PathBuf,
    data: String,
}

impl Representation {
    pub fn new() -> Self {
        Representation {
            ..Default::default()
        }
    }

    pub fn with_data(self, data: &str) -> Self {
        Self {
            data: data.into(),
            ..self
        }
    }

    pub fn with_path(self, path: PathBuf) -> Self {
        Self { path: path, ..self }
    }

    pub fn data(&self) -> &str {
        self.data.as_ref()
    }
}

/// The Writer enum represents the different archive writer implementations.
/// Gzip uses the gzip compression format.
/// Zip uses the zip compression format.
pub enum Writer {
    Path(Archive),
    Gzip(Archive, Builder<GzEncoder<File>>),
    Zip(Archive, ZipWriter<File>),
}

impl Into<Arc<Mutex<Writer>>> for Writer {
    fn into(self) -> Arc<Mutex<Writer>> {
        Arc::new(Mutex::new(self))
    }
}

/// Replaces invalid characters in a path with dashes, to make the path valid for GitHub artifacts.
/// GitHub artifacts paths may not contain : * ? | characters. This replaces those characters with dashes.
fn fix_github_artifacts_path(path: &str) -> String {
    path.replace(":", "-")
        .replace("*", "-")
        .replace("?", "-")
        .replace("|", "-")
}

impl Writer {
    /// Finish writing the archive, finalizing any compression and flushing buffers.
    pub fn finish(&mut self) -> anyhow::Result<()> {
        Ok(match self {
            Writer::Path(_) => (),
            Writer::Gzip(_, builder) => builder.finish()?,
            Writer::Zip(_, writer) => match writer.finish() {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            }?,
        })
    }

    /// Adds a representation data to the archive under the representation path
    pub fn store(&mut self, repr: &Representation) -> anyhow::Result<()> {
        log::debug!("Writing {}...", repr.path.to_str().unwrap());

        let archive_path = fix_github_artifacts_path(repr.path.to_str().unwrap());
        let data = repr.data.clone();

        match self {
            Writer::Path(Archive(archive)) => {
                let file = archive.join(archive_path);
                DirBuilder::new()
                    .recursive(true)
                    .create(file.parent().unwrap())?;
                let mut file = File::create(file)?;
                file.write_all(data.as_bytes())?;
            }
            Writer::Gzip(Archive(archive), builder) => {
                let mut header = Header::new_gnu();
                header.set_size(data.len() as u64 + 1);
                header.set_cksum();
                header.set_mode(0o644);

                let root_prefix = archive.file_stem().unwrap();
                let file = PathBuf::from(root_prefix).join(archive_path);
                builder.append_data(&mut header, file, data.as_bytes())?
            }
            Writer::Zip(Archive(archive), writer) => {
                let path = repr.path.parent().unwrap().to_str().unwrap();
                writer.add_directory(path, FileOptions::default())?;

                let root_prefix = archive.file_stem().unwrap();
                let file = PathBuf::from(root_prefix).join(archive_path);
                let file = file.to_str().unwrap();
                writer.start_file(file, FileOptions::default())?;
                writer.write(data.as_bytes())?;
            }
        }
        Ok(())
    }

    /// Creates a new `Writer` for the given `Archive` and `Encoding`.
    pub fn new(archive: &Archive, encoding: &Encoding) -> anyhow::Result<Self> {
        match archive.0.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => {
                DirBuilder::new().recursive(true).create(parent)?
            }
            Some(_) | None => (),
        };

        Ok(match encoding {
            Encoding::Path => Writer::Path(archive.clone()),
            Encoding::Gzip => Writer::Gzip(
                archive.clone(),
                Builder::new(GzEncoder::new(
                    File::create(archive.0.with_extension("tar.gz"))?,
                    Compression::default(),
                )),
            ),
            Encoding::Zip => Writer::Zip(
                archive.clone(),
                ZipWriter::new(File::create(archive.0.with_extension("zip"))?),
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{env, fs::{self, File}, io::{Read, Seek}};

    use tempdir::TempDir;

    use crate::gather::{gather::Secrets, writer::Representation};

    use super::{Archive, Encoding, Writer};

    #[test]
    fn test_new_gzip() {
        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test.tar.gz");
        let result = Writer::new(&Archive::new(archive.clone()), &Encoding::Gzip);

        assert!(result.is_ok());
    }

    #[test]
    fn test_new_zip() {
        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test.zip");
        let result = Writer::new(&Archive::new(archive.clone()), &Encoding::Zip);

        assert!(result.is_ok());
    }

    #[test]
    fn test_add_gzip() {
        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test");
        let mut writer = Writer::new(&Archive::new(archive.clone()), &Encoding::Gzip).unwrap();

        let repr = Representation {
            path: "test.txt".into(),
            data: "content".into(),
        };

        assert!(writer.store(&repr).is_ok());
        assert!(writer.finish().is_ok());
        assert!(archive.with_file_name("test.tar.gz").exists());
    }

    #[test]
    fn test_add_zip() {
        env::set_var("SECRET", "secret");

        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test.zip");
        let mut writer = Writer::new(&Archive::new(archive.clone()), &Encoding::Zip).unwrap();

        let repr = Representation {
            path: "test.txt".into(),
            data: "content with secret".into(),
        };

        let secret: Secrets = vec!["SECRET".into()].into();
        assert!(writer.store(&secret.strip(&repr)).is_ok());
        assert!(writer.finish().is_ok());
        assert!(archive.exists());

        fn check_zip_contents(reader: impl Read + Seek) {
            let mut zip = zip::ZipArchive::new(reader).unwrap();
            let mut file = zip.by_name("test/test.txt").unwrap();

            let mut data = String::new();
            file.read_to_string(&mut data).unwrap();
            assert_eq!(data, "content with ***");
        }

        check_zip_contents(File::open(archive).unwrap());
    }

    #[test]
    fn test_add_path() {
        env::set_var("SECRET", "secret");

        let tmp_dir = TempDir::new("path").expect("failed to create temp dir");
        let archive = tmp_dir.path().join("cluster1/collected");
        let mut writer = Writer::new(&Archive::new(archive.clone()), &Encoding::Path).unwrap();

        let repr = Representation {
            path: "test.txt".into(),
            data: "content with secret".into(),
        };

        let secret: Secrets = vec!["SECRET".into()].into();
        assert!(writer.store(&secret.strip(&repr)).is_ok());
        assert!(writer.finish().is_ok());
        assert!(archive.clone().exists());
        assert!(archive.join("test.txt").exists());
        let data = fs::read_to_string(archive.join("test.txt")).unwrap();
        assert_eq!(data, "content with ***")
    }

    #[test]
    fn test_try_into_nested_file_success() {
        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let tmp_dir = tmp_dir.path();
        let mut writer = Writer::new(
            &Archive::new(tmp_dir.join("nested/output.zip")),
            &Encoding::Zip,
        )
        .unwrap();

        writer.finish().unwrap();

        assert!(tmp_dir.join("nested/output.zip").exists());
    }

    #[test]
    fn test_try_into_writer_empty_path() {
        assert!(Writer::new(&Archive::new("".into()), &Encoding::Zip).is_err());
    }
}
