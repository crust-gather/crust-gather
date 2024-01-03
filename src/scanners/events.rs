use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use build_html::{Html, HtmlContainer, TableCell, TableRow};
use k8s_openapi::{
    api::core::v1::Event,
    apimachinery::pkg::apis::meta::v1::Time,
    chrono::{DateTime, Utc},
    Resource,
};
use kube::{Api, Client};
use kube_core::{ApiResource, DynamicObject, GroupVersionKind, TypeMeta};

use crate::filters::filter::Filter;

use super::{
    generic::Collectable,
    interface::{Collect, Representation},
};

pub struct Events {
    pub collectable: Collectable,
}

impl Events {
    pub fn new(client: Client, filter: Arc<dyn Filter>) -> Self {
        Events {
            collectable: Collectable::new(client, ApiResource::erase::<Event>(&()), filter),
        }
    }
}
impl Into<Box<dyn Collect>> for Events {
    fn into(self) -> Box<dyn Collect> {
        Box::new(self)
    }
}

#[async_trait]
impl Collect for Events {
    fn path(self: &Self, _: &DynamicObject) -> PathBuf {
        "crust-gather/event-filter.html".into()
    }

    fn get_type_meta(&self) -> TypeMeta {
        TypeMeta {
            api_version: Event::API_VERSION.into(),
            kind: Event::KIND.into(),
        }
    }

    fn get_api(&self) -> Api<DynamicObject> {
        self.collectable.get_api()
    }

    fn filter(&self, gvk: &GroupVersionKind, obj: &DynamicObject) -> bool {
        self.collectable.filter(gvk, obj)
    }

    /// Generates an HTML table representations for an Event object.
    async fn representations(&self, obj: &DynamicObject) -> anyhow::Result<Vec<Representation>> {
        let event: Event = obj.clone().try_parse()?;
        let mut representations: Vec<Representation> = vec![];
        let row = TableRow::new()
            .with_cell(TableCell::default().with_raw({
                let (creation, first, last) = (
                    event.metadata.creation_timestamp.unwrap_or(Time(DateTime::<Utc>::MIN_UTC)).0,
                    event.first_timestamp.unwrap_or(Time(DateTime::<Utc>::MIN_UTC)).0,
                    event.last_timestamp.unwrap_or(Time(DateTime::<Utc>::MIN_UTC)).0
                );
                let count = event.count.ok_or(0).unwrap().to_string();
                format!("<time datetime=\"{creation}\" title=\"First Seen: {first}\">{last}</time> <small>(x{count})</small>")}))
            .with_cell(TableCell::default().with_paragraph_attr(
                event.metadata.namespace.unwrap_or_default(),
                [("class", "truncated")],
            ))
            .with_cell(
                TableCell::default().with_paragraph_attr(
                    event
                        .source
                        .unwrap_or_default()
                        .component
                        .unwrap_or_default(),
                    [("class", "truncated")],
                ),
            )
            .with_cell(TableCell::default().with_paragraph_attr(
                event.involved_object.name.unwrap_or_default(),
                [("class", "truncated")],
            ))
            .with_cell(
                TableCell::default()
                    .with_attributes([
                        match event.reason.clone().unwrap_or_default().to_lowercase() {
                            r if r.contains("fail")
                                || r.contains("error")
                                || r.contains("kill")
                                || r.contains("backoff") =>
                            {
                                ("class", "text-danger")
                            }
                            r if r.contains("notready")
                                || r.contains("unhealthy")
                                || r.contains("missing") =>
                            {
                                ("class", "text-warning")
                            }
                            _ => ("class", "text-muted"),
                        },
                    ])
                    .with_paragraph(event.reason.unwrap_or_default()),
            )
            .with_cell(
                TableCell::default()
                    .with_attributes([("data-formatter", "messageForm")])
                    .with_raw(event.message.unwrap_or_default()),
            );

        representations.push(
            Representation::new()
                .with_path(self.path(obj))
                .with_data(row.to_html_string().as_str()),
        );

        Ok(representations)
    }

    async fn collect(self: &mut Self) -> anyhow::Result<Vec<Representation>> {
        let mut data = String::from("");
        for obj in self.list().await? {
            for repr in &mut self.representations(&obj).await? {
                data.push_str(repr.data())
            }
        }

        Ok(vec![Representation::new()
            .with_path(self.path(&DynamicObject::new("", &ApiResource::erase::<Event>(&()))))
            .with_data(
                format!(include_str!("templates/event-filter.html"), data).as_str(),
            )])
    }
}
