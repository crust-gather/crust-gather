use std::{collections::HashMap, fs::File, io::Read as _, path::PathBuf, pin::pin, sync::Arc};

use anyhow::bail;
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
    pub index: Arc<HashMap<PathBuf, OciDescriptor>>,
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
            Storage::OCI(OCIState {
                reference,
                client,
                index,
                auth,
            }) => {
                let layer = index
                    .get(&path)
                    .ok_or_else(|| anyhow::anyhow!("missing OCI layer entry for path: {path:?}"))?;
                Ok(Storage::read_raw_oci(client, reference, layer, auth).await?)
            }
        }
    }

    async fn read_raw_oci(
        client: &Client,
        reference: &Reference,
        layer: &OciDescriptor,
        auth: &RegistryAuth,
    ) -> anyhow::Result<String> {
        client
            .store_auth_if_needed(reference.registry(), auth)
            .await;
        let mut data = vec![];
        client.pull_blob(reference, layer, &mut data).await?;
        Ok(String::from_utf8(data)?)
    }

    pub async fn read<W: AsyncWrite>(&self, path: PathBuf, out: W) -> anyhow::Result<usize> {
        let mut out = pin!(out);
        match self {
            Storage::FS => {
                let mut file = File::open(path)?;
                let mut data = String::new();
                File::read_to_string(&mut file, &mut data)?;
                Ok(out.write(data.as_bytes()).await?)
            }
            Storage::OCI(OCIState {
                reference,
                client,
                index,
                auth,
            }) => {
                let layer = index
                    .get(&path)
                    .ok_or_else(|| anyhow::anyhow!("missing OCI layer entry for path: {path:?}"))?;
                client
                    .store_auth_if_needed(reference.registry(), auth)
                    .await;
                client.pull_blob(reference, layer, &mut out).await?;
                Ok(layer.size as usize)
            }
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
