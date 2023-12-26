use anyhow;
use async_trait::async_trait;
use kube::Api;
use kube_core::params::ListParams;
use kube_core::{DynamicObject, TypeMeta};
use serde_yaml;
use std::io::Write;
use std::path::PathBuf;
use tar::Builder;

#[async_trait]
pub trait Collect: Sync + Send {
    fn path(&self, obj: &DynamicObject) -> PathBuf;

    fn filter(&self, obj: &DynamicObject) -> bool;

    async fn representations(&self, obj: &DynamicObject) -> anyhow::Result<Vec<Representation>> {
        Ok(vec![Representation::new()
            .with_path(self.path(obj))
            .with_data(serde_yaml::to_string(&obj)?)])
    }

    fn get_api(&self) -> Api<DynamicObject>;

    fn get_type_meta(&self) -> TypeMeta;

    async fn list(&mut self) -> anyhow::Result<Vec<DynamicObject>> {
        let data = self.get_api().list(&ListParams::default()).await?;

        Ok(data
            .items
            .into_iter()
            .map(|o| DynamicObject {
                types: Some(self.get_type_meta()),
                ..o
            })
            .filter(|o| self.filter(o))
            .collect())
    }

    async fn collect(self: &mut Self) -> anyhow::Result<Vec<Representation>> {
        let mut representations = vec![];
        for obj in self.list().await? {
            representations.append(&mut self.representations(&obj).await?);
        }

        Ok(representations)
    }
}

#[derive(Clone, Default)]
pub struct Representation {
    pub path: PathBuf,
    pub data: String,
}

impl Representation {
    pub fn new() -> Self {
        Representation {
            ..Default::default()
        }
    }

    pub fn with_data(self: Self, data: String) -> Self {
        Self { data: data, ..self }
    }

    pub fn with_path(self: Self, path: PathBuf) -> Self {
        Self { path: path, ..self }
    }

    pub fn write<T: Write>(self: Self, builder: &mut Builder<T>) -> anyhow::Result<()> {
        use tar::Header;

        let mut header = Header::new_gnu();
        header.set_size(self.data.as_bytes().len() as u64);
        header.set_cksum();
        header.set_mode(0o755);

        builder.append_data(&mut header, self.path, self.data.as_bytes())?;

        Ok(())
    }

    pub fn data(&self) -> &str {
        self.data.as_ref()
    }
}
