use std::{fmt::Display, fs::File, io::Write, path::PathBuf};

use flate2::{write::GzEncoder, Compression};
use kube::{
    config::{self, KubeConfigOptions},
    Client,
};
use serde::Deserialize;
use tar::{Builder, Header};
use zip::{write::FileOptions, ZipWriter};

use crate::scanners::interface::Representation;

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
/// Gzip indicates gzip compression should be used.
/// Zip indicates zip compression should be used.
pub enum Encoding {
    #[default]
    Gzip,
    Zip,
}

impl From<&str> for Encoding {
    fn from(value: &str) -> Self {
        match value {
            "zip" => Encoding::Zip,
            _ => Encoding::Gzip,
        }
    }
}

/// The Writer enum represents the different archive writer implementations.
/// Gzip uses the gzip compression format.
/// Zip uses the zip compression format.
pub enum Writer {
    Gzip(Builder<GzEncoder<File>>),
    Zip(ZipWriter<File>),
}

impl Writer {
    /// Replaces any secrets in representation data with ***.
    fn strip_secrets(&self, mut data: String, secrets: &Vec<String>) -> String {
        for secret in secrets {
            data = data.replace(secret.as_str(), "***");
        }

        data
    }

    /// Finish writing the archive, finalizing any compression and flushing buffers.
    pub fn finish(&mut self) -> anyhow::Result<()> {
        Ok(match self {
            Writer::Gzip(builder) => builder.finish()?,
            Writer::Zip(writer) => match writer.finish() {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            }?,
        })
    }

    /// Adds a representation data to the archive under the representation path
    pub fn add(&mut self, repr: Representation, secrets: &Vec<String>) -> anyhow::Result<()> {
        log::debug!("Writing {}...", repr.path.to_str().unwrap());

        let data = self.strip_secrets(repr.data().to_string(), secrets);

        match self {
            Writer::Gzip(builder) => {
                let mut header = Header::new_gnu();
                header.set_size(data.as_bytes().len() as u64);
                header.set_cksum();
                header.set_mode(0o644);

                builder.append_data(&mut header, repr.path.clone(), data.as_bytes())?
            }
            Writer::Zip(writer) => {
                let path = repr.path.parent().unwrap().to_str().unwrap();
                writer.add_directory(path, FileOptions::default())?;

                let file = repr.path.to_str().unwrap();
                writer.start_file(file, FileOptions::default())?;
                writer.write(data.as_bytes())?;
            }
        }
        Ok(())
    }

    /// Creates a new `Writer` for the given `Archive` and `Encoding`.
    pub fn new(archive: &Archive, encoding: &Encoding) -> anyhow::Result<Self> {
        Ok(match encoding {
            Encoding::Gzip => Writer::Gzip(Builder::new(GzEncoder::new(
                File::create(archive.0.with_extension("tar.gz"))?,
                Compression::default(),
            ))),
            Encoding::Zip => Writer::Zip(ZipWriter::new(File::create(
                archive.0.with_extension("zip"),
            )?)),
        })
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use crate::scanners::interface::Representation;

    use super::{Archive, Encoding, Writer};

    #[test]
    fn test_strip_secrets() {
        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test.tar.gz");
        let writer = Writer::new(&Archive::new(archive), &Encoding::Gzip).unwrap();

        let secrets = vec!["password".to_string()];
        let data = "omit password string".to_string();

        let result = writer.strip_secrets(data, &secrets);

        assert_eq!(result, "omit *** string");
    }

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

        assert!(writer.add(repr, &vec![]).is_ok());
        assert!(writer.finish().is_ok());
        assert!(archive.with_file_name("test.tar.gz").exists())
    }

    #[test]
    fn test_add_zip() {
        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test.zip");
        let mut writer = Writer::new(&Archive::new(archive.clone()), &Encoding::Zip).unwrap();

        let repr = Representation {
            path: "test.txt".into(),
            data: "content".into(),
        };

        assert!(writer.add(repr, &vec![]).is_ok());
        assert!(writer.finish().is_ok());
        assert!(archive.exists())
    }
}
