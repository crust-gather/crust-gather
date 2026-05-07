use std::{
    collections::HashMap, fs::File, io::Read as _, ops::Deref as _, path::PathBuf, pin::pin,
    sync::Arc,
};

use anyhow::bail;
use base64::{Engine as _, prelude::BASE64_STANDARD};
use cached::proc_macro::cached;
use derive_more::Deref;
use flate2::read::GzDecoder;
use oci_client::{Client, Reference, manifest::OciDescriptor, secrets::RegistryAuth};
use tokio::io::{AsyncWrite, AsyncWriteExt as _};

#[derive(Clone)]
pub enum Storage {
    FS,
    OCI(OCIState),
}

#[derive(Clone)]
pub struct OCIState {
    pub reference: Reference,
    pub auth: RegistryAuth,
    pub client: Client,
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
            Some(oci) => Storage::OCI(oci),
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
        Ok(String::from_utf8(self.pull_blob(layer.deref()).await?)?)
    }

    async fn read<W: AsyncWrite>(&self, path: PathBuf, out: W) -> anyhow::Result<usize> {
        let mut out = pin!(out);
        let layer = self
            .index
            .get(&path)
            .ok_or_else(|| anyhow::anyhow!("missing OCI layer entry for path: {path:?}"))?;

        match layer {
            Descriptor::OciDescriptor(oci_descriptor) => {
                let data = self.pull_blob(oci_descriptor).await?;
                out.write_all(&data).await?;
                Ok(data.len())
            }
            Descriptor::ListOciDescriptor(oci_descriptor, from, to) => {
                let data = self.pull_blob(oci_descriptor).await?;
                let data = BASE64_STANDARD.decode(data)?;
                let mut dec = GzDecoder::new(data.as_slice());

                let mut objects = vec![];
                dec.read_to_end(&mut objects)?;

                let buf = &objects[*from..*to];
                out.write_all(buf).await?;

                Ok(buf.len())
            }
        }
    }

    async fn pull_blob(&self, descriptor: &OciDescriptor) -> anyhow::Result<Vec<u8>> {
        pull_blob_cached(
            self.client.clone(),
            self.reference.clone(),
            self.auth.clone(),
            descriptor.clone(),
        )
        .await
    }
}

#[cached(
    result = true,
    key = "String",
    convert = r#"{ format!("{}@{}", reference, descriptor.digest) }"#
)]
async fn pull_blob_cached(
    client: Client,
    reference: Reference,
    auth: RegistryAuth,
    descriptor: OciDescriptor,
) -> anyhow::Result<Vec<u8>> {
    client
        .store_auth_if_needed(reference.registry(), &auth)
        .await;
    let mut out = Vec::with_capacity(descriptor.size as usize);
    client.pull_blob(&reference, &descriptor, &mut out).await?;
    Ok(out)
}
