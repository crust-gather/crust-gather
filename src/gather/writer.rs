#[cfg(feature = "archive")]
use flate2::{write::GzEncoder, Compression};

use serde::Deserialize;
use std::{
    fmt::Display,
    fs::{DirBuilder, File},
    io::Write as _,
    path::PathBuf,
    sync::{Arc, Mutex},
};
#[cfg(feature = "archive")]
use tar::{Builder, Header};
#[cfg(feature = "archive")]
use zip::{write::FileOptions, ZipWriter};

use super::representation::{ArchivePath, Representation};

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
    #[cfg(feature = "archive")]
    Gzip,
    #[cfg(feature = "archive")]
    Zip,
}

impl From<&str> for Encoding {
    fn from(value: &str) -> Self {
        match value {
            #[cfg(feature = "archive")]
            "zip" => Self::Zip,
            #[cfg(feature = "archive")]
            "gzip" => Self::Gzip,
            _ => Self::Path,
        }
    }
}

/// The Writer enum represents the different archive writer implementations.
/// Gzip uses the gzip compression format.
/// Zip uses the zip compression format.
pub enum Writer {
    Path(Archive),
    #[cfg(feature = "archive")]
    Gzip(Archive, Builder<GzEncoder<File>>),
    #[cfg(feature = "archive")]
    Zip(Archive, ZipWriter<File>),
}

impl From<Writer> for Arc<Mutex<Writer>> {
    fn from(val: Writer) -> Self {
        Self::new(Mutex::new(val))
    }
}

impl Writer {
    /// Finish writing the archive, finalizing any compression and flushing buffers.
    pub fn finish(&mut self) -> anyhow::Result<()> {
        match self {
            Self::Path(_) => (),
            #[cfg(feature = "archive")]
            Self::Gzip(_, builder) => builder.finish()?,
            #[cfg(feature = "archive")]
            Self::Zip(_, writer) => match writer.finish() {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            }?,
        };
        Ok(())
    }

    /// Adds a representation data to the archive under the representation path
    pub fn store(&mut self, repr: &Representation) -> anyhow::Result<()> {
        log::debug!("Writing {}...", repr.path());

        let archive_path: String = repr.path().try_into()?;
        let data = repr.data();

        match self {
            Self::Path(Archive(archive)) => {
                let file = archive.join(archive_path);
                DirBuilder::new()
                    .recursive(true)
                    .create(file.parent().unwrap())?;
                let mut file = File::create(file)?;
                file.write_all(data.as_bytes())?;
            }
            #[cfg(feature = "archive")]
            Self::Gzip(Archive(archive), builder) => {
                let mut header = Header::new_gnu();
                header.set_size(data.len() as u64 + 1);
                header.set_cksum();
                header.set_mode(0o644);

                let root_prefix = archive.file_stem().unwrap();
                let file = PathBuf::from(root_prefix).join(archive_path);
                builder.append_data(&mut header, file, data.as_bytes())?;
            }
            #[cfg(feature = "archive")]
            Self::Zip(Archive(archive), writer) => {
                let path = repr.path();
                let path = path.parent().unwrap().to_str().unwrap();
                writer.add_directory(path, FileOptions::default())?;

                let root_prefix = archive.file_stem().unwrap();
                let file = PathBuf::from(root_prefix).join(archive_path);
                let file = file.to_str().unwrap();
                writer.start_file(file, FileOptions::default())?;
                writer.write_all(data.as_bytes())?;
            }
        }
        Ok(())
    }

    /// Creates a new `Writer` for the given `Archive` and `Encoding`.
    pub fn new(archive: &Archive, encoding: &Encoding) -> anyhow::Result<Self> {
        match archive.0.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => {
                DirBuilder::new().recursive(true).create(parent)?;
            }
            Some(_) | None => (),
        };

        Ok(match encoding {
            Encoding::Path => Self::Path(archive.clone()),
            #[cfg(feature = "archive")]
            Encoding::Gzip => Self::Gzip(
                archive.clone(),
                Builder::new(GzEncoder::new(
                    File::create(archive.0.with_extension("tar.gz"))?,
                    Compression::default(),
                )),
            ),
            #[cfg(feature = "archive")]
            Encoding::Zip => Self::Zip(
                archive.clone(),
                ZipWriter::new(File::create(archive.0.with_extension("zip"))?),
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        fs::{self},
    };

    use tempdir::TempDir;

    use crate::gather::{config::Secrets, representation::ArchivePath, writer::Representation};

    use super::{Archive, Encoding, Writer};

    #[test]
    #[cfg(feature = "archive")]
    fn test_new_gzip() {
        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test.tar.gz");
        let result = Writer::new(&Archive::new(archive), &Encoding::Gzip);

        assert!(result.is_ok());
    }

    #[test]
    #[cfg(feature = "archive")]
    fn test_new_zip() {
        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test.zip");
        let result = Writer::new(&Archive::new(archive), &Encoding::Zip);

        assert!(result.is_ok());
    }

    #[test]
    #[cfg(feature = "archive")]
    fn test_add_gzip() {
        use crate::gather::representation::ArchivePath;

        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test");
        let mut writer = Writer::new(&Archive::new(archive.clone()), &Encoding::Gzip).unwrap();

        let repr = Representation::new()
            .with_data("content".into())
            .with_path(ArchivePath::Custom("test.txt".into()));

        assert!(writer.store(&repr).is_ok());
        assert!(writer.finish().is_ok());
        assert!(archive.with_file_name("test.tar.gz").exists());
    }

    #[test]
    #[cfg(feature = "archive")]
    fn test_add_zip() {
        use std::{
            fs::File,
            io::{Read, Seek},
        };

        use crate::gather::representation::ArchivePath;

        env::set_var("SECRET", "secret");

        let tmp_dir = TempDir::new("archive").expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test.zip");
        let mut writer = Writer::new(&Archive::new(archive.clone()), &Encoding::Zip).unwrap();

        let repr = Representation::new()
            .with_path(ArchivePath::Custom("test.txt".into()))
            .with_data("content with secret".into());

        let secret: Secrets = vec!["SECRET".into()].into();
        assert!(writer.store(&secret.strip(&repr)).is_ok());
        assert!(writer.finish().is_ok());
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

    #[test]
    fn test_add_path() {
        env::set_var("SECRET", "secret");

        let tmp_dir = TempDir::new("path").expect("failed to create temp dir");
        let archive = tmp_dir.path().join("cluster1/collected");
        let mut writer = Writer::new(&Archive::new(archive.clone()), &Encoding::Path).unwrap();

        let repr = Representation::new()
            .with_data("content with secret")
            .with_path(ArchivePath::Namespaced("test.txt".into()));

        let secret: Secrets = vec!["SECRET".into()].into();
        assert!(writer.store(&secret.strip(&repr)).is_ok());
        assert!(writer.finish().is_ok());
        assert!(archive.exists());
        assert!(archive.join("test.txt").exists());
        let data = fs::read_to_string(archive.join("test.txt")).unwrap();
        assert_eq!(data, "content with xxx");
    }

    #[test]
    #[cfg(feature = "archive")]
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
    #[cfg(feature = "archive")]
    fn test_try_into_writer_empty_path() {
        assert!(Writer::new(&Archive::new("".into()), &Encoding::Zip).is_err());
    }
}
