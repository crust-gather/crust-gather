use std::{collections::HashMap, io::Read as _, path::PathBuf, pin::pin, sync::Arc};

use anyhow::bail;
use base64::{Engine as _, prelude::BASE64_STANDARD};
use cached::cached;
use derive_more::Deref;
use flate2::read::GzDecoder;
use oci_client::{Client, Reference, manifest::OciDescriptor, secrets::RegistryAuth};
use tokio::io::{AsyncWrite, AsyncWriteExt as _};

use crate::gather::json_resource::{JsonResource, ReadState};
use crate::gather::writer::ManifestConfig;
use crate::scanners::interface::Extension;

#[derive(Clone)]
pub enum Storage {
    FS,
    OCI(Arc<OCIState>),
}

#[derive(Clone)]
pub struct OCIState {
    pub reference: Reference,
    pub auth: RegistryAuth,
    pub client: Client,
    pub config: ManifestConfig,
    pub index: Arc<HashMap<PathBuf, Descriptor>>,
}

#[derive(Clone, Deref)]
pub enum Descriptor {
    OciDescriptor(OciDescriptor),
    ListOciDescriptor(#[deref] OciDescriptor, usize, usize),
}

impl Storage {
    #[must_use]
    pub fn new(oci: Option<OCIState>) -> Self {
        match oci {
            Some(oci) => Self::OCI(Arc::new(oci)),
            None => Self::FS,
        }
    }

    pub async fn read_raw(&self, path: PathBuf) -> anyhow::Result<String> {
        match self {
            Self::FS => Ok(tokio::fs::read_to_string(&path).await?),
            Self::OCI(oci_state) => Ok(oci_state.read_raw(path).await?),
        }
    }

    pub async fn read<W: AsyncWrite>(&self, path: &PathBuf, out: W) -> anyhow::Result<usize> {
        match self {
            Self::FS => {
                let data = tokio::fs::read(path).await?;
                let mut out = pin!(out);
                out.write_all(&data).await?;
                Ok(data.len())
            }
            Self::OCI(oci_state) => Ok(oci_state.read(path, out).await?),
        }
    }

    #[must_use]
    pub fn exist(&self, path: &PathBuf) -> bool {
        match self {
            Self::FS => path.exists(),
            Self::OCI(ocistate) => ocistate.index.contains_key(path),
        }
    }

    pub fn matching_paths(&self, path: PathBuf) -> anyhow::Result<Vec<PathBuf>> {
        let mut paths = vec![];
        let path = path
            .to_str()
            .map_or_else(|| bail!("Unable to convert path to string: {path:?}"), Ok)?;
        match self {
            Self::FS => {
                for path in glob::glob(path)? {
                    paths.push(path?);
                }
            }
            Self::OCI(ocistate) => {
                let pattern = glob::Pattern::new(path)?;
                for path in ocistate.index.keys() {
                    if pattern.matches(
                        path.to_str().map_or_else(
                            || bail!("Unable to convert path to string: {path:?}"),
                            Ok,
                        )?,
                    ) {
                        paths.push(path.clone());
                    }
                }
            }
        }
        Ok(paths)
    }

    pub fn extension(&self) -> Extension {
        match self {
            Storage::FS => Extension::Yaml,
            Storage::OCI(ocistate) => ocistate.config.extension,
        }
    }

    /// Reads bytes. FS uses YAML. OCI uses JSON when the flag is on.
    pub fn from_slice<T: serde::de::DeserializeOwned + Send>(
        &self,
        data: Vec<u8>,
    ) -> anyhow::Result<T> {
        // The Extension is taken from OCI state (when present) or defaults to Yaml (FS),
        // via `Self::extension()`.
        self.extension().from_slice(&data)
    }

    pub fn object(&self, data: Vec<u8>, state: ReadState) -> anyhow::Result<JsonResource> {
        let ext = self.extension();
        ext.object(&data, state)
    }
}

impl OCIState {
    async fn read_raw(&self, path: PathBuf) -> anyhow::Result<String> {
        let layer = self
            .index
            .get(&path)
            .ok_or_else(|| anyhow::anyhow!("missing OCI layer entry for path: {path:?}"))?;
        let mut data = Vec::with_capacity(layer.size.try_into()?);
        self.pull_blob(layer, &mut data).await?;
        Ok(String::from_utf8(data)?)
    }

    async fn read<W: AsyncWrite>(&self, path: &PathBuf, out: W) -> anyhow::Result<usize> {
        let layer = self
            .index
            .get(path)
            .ok_or_else(|| anyhow::anyhow!("missing OCI layer entry for path: {path:?}"))?;

        let size = self.pull_blob(layer, out).await?;
        Ok(size)
    }

    async fn pull_blob<W: AsyncWrite>(
        &self,
        descriptor: &Descriptor,
        out: W,
    ) -> anyhow::Result<usize> {
        let data = pull_blob_cached(
            &self.client,
            &self.reference,
            &self.auth,
            descriptor,
            self.config.compressed || matches!(descriptor, Descriptor::ListOciDescriptor(..)),
        )
        .await?;
        let mut out = pin!(out);

        let Descriptor::ListOciDescriptor(_, from, to) = descriptor else {
            out.write_all(&data).await?;
            return Ok(data.len());
        };

        out.write_all(&data[*from..*to]).await?;
        Ok(*to - *from)
    }
}

#[cached(
    result = true,
    key = "String",
    sync_writes = "by_key",
    convert = r#"{ format!("{}@{}", reference, descriptor.digest) }"#
)]
pub(crate) async fn pull_blob_cached(
    client: &Client,
    reference: &Reference,
    auth: &RegistryAuth,
    descriptor: &OciDescriptor,
    encoded: bool,
) -> anyhow::Result<Vec<u8>> {
    client
        .store_auth_if_needed(reference.registry(), auth)
        .await;
    let mut out = Vec::with_capacity(descriptor.size.try_into()?);
    client.pull_blob(reference, &descriptor, &mut out).await?;

    if !encoded {
        return Ok(out);
    }

    let data = BASE64_STANDARD.decode(out)?;
    let mut dec = GzDecoder::new(data.as_slice());

    let mut objects = vec![];
    dec.read_to_end(&mut objects)?;
    Ok(objects)
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::serde_json;

    #[test]
    fn test_manifest_config_default() {
        let data: Vec<u8> = b"{\"compressed\": true}".to_vec();
        let config: ManifestConfig = serde_json::from_slice(&data).unwrap();
        assert!(config.compressed);
        assert_eq!(config.extension, Extension::Yaml);
    }

    #[test]
    fn test_manifest_config_new() {
        // New archives set the extension field directly.
        let data: Vec<u8> = b"{\"compressed\": true, \"extension\": \"json\"}".to_vec();
        let config: ManifestConfig = serde_json::from_slice(&data).unwrap();
        assert!(config.compressed);
        assert_eq!(config.extension, Extension::Json);
    }
}
