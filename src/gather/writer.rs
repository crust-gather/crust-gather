use flate2::{Compression, write::GzEncoder};

use json_patch::diff;
use k8s_openapi::{chrono::Utc, serde_json};
use serde::Deserialize;
use std::{
    borrow::Cow,
    ffi::OsStr,
    fmt::Display,
    fs::{DirBuilder, File},
    io::Write as _,
    ops::Deref,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tar::{Builder, Header};
use tracing::instrument;
use walkdir::WalkDir;
use zip::{ZipWriter, result::ZipError, write::SimpleFileOptions};

use crate::gather::reader::Reader;

use super::representation::{ArchivePath, Representation};

#[derive(Clone, Deserialize)]
pub struct ArchiveSearch(PathBuf);

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
pub enum Writer {
    Path(Archive),
    Gzip(Archive, Box<Builder<GzEncoder<File>>>),
    Zip(Archive, Box<ZipWriter<File>>),
}

impl From<Writer> for Arc<Mutex<Writer>> {
    fn from(val: Writer) -> Self {
        Self::new(Mutex::new(val))
    }
}

impl Writer {
    /// Finish writing the archive, finalizing any compression and flushing buffers.
    pub fn finish(self) -> anyhow::Result<()> {
        match self {
            Self::Path(_) => (),
            Self::Gzip(_, mut builder) => {
                builder.finish()?;
            }
            Self::Zip(_, writer) => {
                writer.finish()?;
            }
        };
        Ok(())
    }

    /// Adds a representation data to the archive under the representation path
    #[instrument(skip_all, fields(repr = repr.path().to_string()))]
    pub fn store(&mut self, repr: &Representation) -> anyhow::Result<()> {
        tracing::debug!("Writing...");

        let archive_path: String = repr.path().try_into()?;
        let data = repr.data();

        match self {
            Self::Path(Archive(archive)) => {
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
    pub fn sync(&mut self, repr: &Representation) -> anyhow::Result<()> {
        tracing::debug!("Writing...");

        let archive_path: String = repr.path().try_into()?;

        match self {
            Self::Path(archive) => {
                let file_path = archive.0.join(archive_path);

                // generate diff and write
                let original =
                    Reader::new(archive.clone().into(), Utc::now())?.read(file_path.clone())?;
                let updated = serde_yaml::from_str(repr.data())?;
                let patch = &diff(&original, &updated);
                if !patch.deref().is_empty() {
                    let mut patches = File::options()
                        .create(true)
                        .append(true)
                        .open(file_path.with_extension("patch"))?;
                    serde_json::to_writer(patches.try_clone()?, patch)?;
                    patches.write_all(b"\n")?;
                }
                self.store(repr)?;
            }
            Self::Gzip(Archive(_archive), _builder) => {
                unimplemented!();
            }
            Self::Zip(Archive(_archive), _writer) => {
                unimplemented!();
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
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        fs::{self},
    };

    use tempfile::TempDir;

    use crate::gather::{config::Secrets, representation::ArchivePath, writer::Representation};

    use super::{Archive, Encoding, Writer};

    #[test]
    fn test_new_gzip() {
        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test.tar.gz");
        let result = Writer::new(&Archive::new(archive), &Encoding::Gzip);

        assert!(result.is_ok());
    }

    #[test]
    fn test_new_zip() {
        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test.zip");
        let result = Writer::new(&Archive::new(archive), &Encoding::Zip);

        assert!(result.is_ok());
    }

    #[test]
    fn test_add_gzip() {
        use crate::gather::representation::ArchivePath;

        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test");
        let mut writer = Writer::new(&Archive::new(archive.clone()), &Encoding::Gzip).unwrap();

        let repr = Representation::new()
            .with_data("content")
            .with_path(ArchivePath::Custom("test.txt".into()));

        assert!(writer.store(&repr).is_ok());
        assert!(writer.finish().is_ok());
        assert!(archive.with_file_name("test.tar.gz").exists());
    }

    #[test]
    fn test_add_zip() {
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
        let mut writer = Writer::new(&Archive::new(archive.clone()), &Encoding::Zip).unwrap();

        let repr = Representation::new()
            .with_path(ArchivePath::Custom("test.txt".into()))
            .with_data("content with secret");

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
        unsafe { env::set_var("SECRET", "secret") };

        let tmp_dir = TempDir::new().expect("failed to create temp dir");
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
    fn test_try_into_nested_file_success() {
        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let tmp_dir = tmp_dir.path();
        let writer = Writer::new(
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
