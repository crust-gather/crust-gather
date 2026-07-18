use std::{
    collections::HashMap, fs::File, io::Read as _, ops::Deref as _, path::PathBuf, pin::pin,
    sync::Arc,
};

use anyhow::bail;
use base64::{Engine as _, prelude::BASE64_STANDARD};
use cached::cached;
use derive_more::Deref;
use flate2::read::GzDecoder;
use oci_client::{Client, Reference, manifest::OciDescriptor, secrets::RegistryAuth};
use tokio::io::{AsyncWrite, AsyncWriteExt as _};

use crate::gather::writer::ManifestConfig;

#[derive(Clone)]
pub enum Storage {
    FS,
    OCI(Box<OCIState>),
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
    pub fn new(oci: Option<OCIState>) -> Self {
        match oci {
            Some(oci) => Storage::OCI(Box::new(oci)),
            None => Storage::FS,
        }
    }

    pub async fn read_raw(&self, path: PathBuf) -> anyhow::Result<String> {
        match self {
            Storage::FS => {
                let mut file = File::open(path)?;
                let mut data = String::new();
                File::read_to_string(&mut file, &mut data)?;
                Ok(data)
            }
            Storage::OCI(oci_state) => Ok(oci_state.read_raw(path).await?),
        }
    }

    pub async fn read<W: AsyncWrite>(&self, path: PathBuf, out: W) -> anyhow::Result<usize> {
        match self {
            Storage::FS => {
                let mut file = File::open(path)?;
                let mut data = String::new();
                File::read_to_string(&mut file, &mut data)?;
                let mut out = pin!(out);
                Ok(out.write(data.as_bytes()).await?)
            }
            Storage::OCI(oci_state) => Ok(oci_state.read(path, out).await?),
        }
    }

    pub fn exist(&self, path: &PathBuf) -> bool {
        match self {
            Storage::FS => path.exists(),
            Storage::OCI(ocistate) => ocistate.index.contains_key(path),
        }
    }

    pub fn matching_paths(&self, path: PathBuf) -> anyhow::Result<Vec<PathBuf>> {
        let mut paths = vec![];
        let path = path
            .to_str()
            .map_or_else(|| bail!("Unable to convert path to string: {path:?}"), Ok)?;
        match self {
            Storage::FS => {
                for path in glob::glob(path)? {
                    paths.push(path?);
                }
            }
            Storage::OCI(ocistate) => {
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
        };
        Ok(paths)
    }
}

impl OCIState {
    async fn read_raw(&self, path: PathBuf) -> anyhow::Result<String> {
        let layer = self
            .index
            .get(&path)
            .ok_or_else(|| anyhow::anyhow!("missing OCI layer entry for path: {path:?}"))?;
        let mut data = Vec::with_capacity(layer.size as usize);
        self.pull_blob(layer, &mut data).await?;
        Ok(String::from_utf8(data)?)
    }

    async fn read<W: AsyncWrite>(&self, path: PathBuf, out: W) -> anyhow::Result<usize> {
        let layer = self
            .index
            .get(&path)
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
            descriptor.deref(),
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
    let mut out = Vec::with_capacity(descriptor.size as usize);
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
