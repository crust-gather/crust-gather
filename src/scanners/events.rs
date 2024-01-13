use async_trait::async_trait;
use build_html::{Html, HtmlContainer, TableCell, TableRow};
use k8s_openapi::{
    api::core::v1::Event,
    apimachinery::pkg::apis::meta::v1::Time,
    chrono::{DateTime, Utc},
};
use kube::Api;
use kube_core::{ApiResource, TypeMeta};
use std::{
    fmt::Debug,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::gather::{
    config::{Config, Secrets},
    writer::{Representation, Writer},
};

use super::{interface::Collect, objects::Objects};

#[derive(Clone)]
pub struct Events {
    pub collectable: Objects<Event>,
}

impl Debug for Events {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Events").finish()
    }
}

impl From<Config> for Events {
    fn from(value: Config) -> Self {
        Events {
            collectable: Objects::new_typed(value, ApiResource::erase::<Event>(&())),
        }
    }
}

#[async_trait]
impl Collect<Event> for Events {
    fn get_secrets(&self) -> Secrets {
        self.collectable.get_secrets()
    }

    fn get_writer(&self) -> Arc<Mutex<Writer>> {
        self.collectable.get_writer()
    }

    fn path(&self, _: &Event) -> PathBuf {
        "event-filter.html".into()
    }

    fn filter(&self, obj: &Event) -> anyhow::Result<bool> {
        self.collectable.filter(obj)
    }

    /// Generates an HTML table representations for an Event object.
    async fn representations(&self, event: &Event) -> anyhow::Result<Vec<Representation>> {
        log::info!("Collecting events");

        let mut representations = vec![];
        let row = TableRow::new()
            .with_cell(TableCell::default().with_raw({
                let (creation, first, last) = (
                    event.metadata.creation_timestamp.clone().unwrap_or(Time(DateTime::<Utc>::MIN_UTC)).0,
                    event.first_timestamp.clone().unwrap_or(Time(DateTime::<Utc>::MIN_UTC)).0,
                    event.last_timestamp.clone().unwrap_or(Time(DateTime::<Utc>::MIN_UTC)).0
                );
                let count = event.count.unwrap_or(1).to_string();
                format!("<time datetime=\"{creation}\" title=\"First Seen: {first}\">{last}</time> <small>(x{count})</small>")}))
            .with_cell(TableCell::default().with_paragraph_attr(
                event.metadata.namespace.clone().unwrap_or_default(),
                [("class", "truncated")],
            ))
            .with_cell(
                TableCell::default().with_paragraph_attr(
                    event
                        .source.clone()
                        .unwrap_or_default()
                        .component
                        .unwrap_or_default(),
                    [("class", "truncated")],
                ),
            )
            .with_cell(TableCell::default().with_paragraph_attr(
                event.involved_object.name.clone().unwrap_or_default(),
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
                    .with_paragraph(event.reason.clone().unwrap_or_default()),
            )
            .with_cell(
                TableCell::default()
                    .with_attributes([("data-formatter", "messageForm")])
                    .with_raw(event.message.clone().unwrap_or_default()),
            );

        representations.push(
            Representation::new()
                .with_path(self.path(event))
                .with_data(row.to_html_string().as_str()),
        );

        Ok(representations)
    }

    fn get_api(&self) -> Api<Event> {
        self.collectable.get_api()
    }

    fn get_type_meta(&self) -> TypeMeta {
        self.collectable.get_type_meta()
    }

    async fn collect(&self) -> anyhow::Result<()> {
        let mut data = String::from("");
        for obj in self.list().await? {
            for repr in self.representations(&obj).await? {
                data.push_str(repr.data())
            }
        }

        self.get_writer().lock().unwrap().store(
            &Representation::new()
                .with_path(self.path(&Event::default()))
                .with_data(format!(include_str!("templates/event-filter.html"), data).as_str()),
        )?;

        Ok(())
    }
}
