use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};

use cel::{
    Context, FunctionContext, Program, Value,
    extractors::This,
    objects::{Key, KeyRef, TryIntoValue as _},
};
use chrono::Utc;
use k8s_openapi::{
    apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceColumnDefinition,
    apimachinery::pkg::apis::meta::v1::{Condition, LabelSelector},
    serde_json::{self, json},
};
use kube::core::Selector;
use kube_cel::register_all;
use serde_json_path::JsonPath;

static PREDEFINED_TABLES: OnceLock<BTreeMap<String, Vec<ColumnDefinition>>> = OnceLock::new();
pub const AGE_CEL: &str = "(now - timestamp(self.metadata.creationTimestamp)).age()";

#[derive(Clone, Debug, PartialEq, Default)]
pub struct TablePath {
    pub column: ColumnDefinition,
    pub json_path: Option<JsonPath>,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct ColumnDefinition {
    pub source: CustomResourceColumnDefinition,
    pub cel: Option<String>,
}

impl ColumnDefinition {
    pub fn json(name: &str, json_path: &str) -> Self {
        Self {
            source: CustomResourceColumnDefinition {
                name: name.into(),
                json_path: json_path.into(),
                ..Default::default()
            },
            cel: None,
        }
    }

    pub fn cel(name: &str, cel: impl Into<String>) -> Self {
        Self {
            source: CustomResourceColumnDefinition {
                name: name.into(),
                ..Default::default()
            },
            cel: Some(cel.into()),
        }
    }
}

pub fn predefined_table(resource: &str) -> Option<Vec<TablePath>> {
    let columns = predefined_tables().get(resource)?;
    Some(columns.iter().map(TablePath::new).collect())
}

pub fn has_predefined_table(resource: &str) -> bool {
    predefined_tables().contains_key(resource)
}

fn predefined_tables() -> &'static BTreeMap<String, Vec<ColumnDefinition>> {
    PREDEFINED_TABLES.get_or_init(|| {
        let mut map = BTreeMap::new();
        map.insert(
            "events".into(),
            vec![
                ColumnDefinition::cel("lastTimestamp", "(now - timestamp(self.lastTimestamp)).age()"),
                ColumnDefinition::json("type", ".type"),
                ColumnDefinition::json("reason", ".reason"),
                ColumnDefinition::json("object", ".metadata.name"),
                ColumnDefinition::json("message", ".message"),
            ],
        );
        map.insert(
            "pods".into(),
            vec![
                ColumnDefinition::json("Name", ".metadata.name"),
                ColumnDefinition::cel(
                    "Ready",
                        r#""%s/%s".format([
                            self.get("status").get("containerStatuses").or([])
                                .filter(status, status.ready)
                                .size(),
                            self.get("status").get("containerStatuses").or(
                                self.get("spec").get("containers").or([]))
                                .size()
                            ])"#
                ),
                ColumnDefinition::cel(
                    "Status",
                        r#"has(self.metadata.deletionTimestamp)
                            ? 'Terminating'
                            : self.get("status").get("reason").or("") != "" ? self.get("status").get("reason") : self.get("status").get("phase").or("").string()"#
                ),
                ColumnDefinition::cel(
                    "Restarts",
                        r#"self.get("status").get("initContainerStatuses").or([]).map(status, status.get("restartCount").or(0).int()).sum()
                            +
                            self.get("status").get("containerStatuses").or([]).map(status, status.get("restartCount").or(0).int()).sum()"#
                ),
                ColumnDefinition::cel("Age", AGE_CEL),
            ],
        );
        map.insert(
            "namespaces".into(),
            vec![
                ColumnDefinition::json("Name", ".metadata.name"),
                ColumnDefinition::cel(
                    "Status",
                        r#"has(self.metadata.deletionTimestamp)
                            ? 'Terminating'
                            : self.get("status").get("phase").or("")"#
                ),
                ColumnDefinition::cel("Age", AGE_CEL),
            ],
        );
        map.insert(
            "deployments".into(),
            vec![
                ColumnDefinition::json("Name", ".metadata.name"),
                ColumnDefinition::cel(
                    "Ready",
                        r#""%s/%s".format([
                            self.get("status").get("readyReplicas").or(0),
                            self.get("spec").get("replicas").or(1)
                        ])"#,
                ),
                ColumnDefinition::cel("Up-to-date", r#"self.get("status").get("updatedReplicas").or(0)"#),
                ColumnDefinition::cel("Available", r#"self.get("status").get("availableReplicas").or(0)"#),
                ColumnDefinition::cel("Age", AGE_CEL),
            ],
        );
        map.insert(
            "jobs".into(),
            vec![
                ColumnDefinition::json("Name", ".metadata.name"),
                ColumnDefinition::cel(
                    "Status",
                    r#"self.get("status").get("conditions").or([]).condition('Complete', 'True') != null ? 'Complete'
                        : self.get("status").get("conditions").or([]).condition('Failed', 'True') != null ? 'Failed'
                        : has(self.metadata.deletionTimestamp) ? 'Terminating'
                        : self.get("status").get("conditions").or([]).condition('Suspended', 'True') != null ? 'Suspended'
                        : self.get("status").get("conditions").or([]).condition('FailureTarget', 'True') != null  ? 'FailureTarget'
                        : self.get("status").get("conditions").or([]).condition('SuccessCriteriaMet', 'True') != null ? 'SuccessCriteriaMet'
                        : 'Running'"#,
                ),
                ColumnDefinition::cel(
                    "Completions",
                    r#"has(self.spec.completions)
                        ? string(self.get("status").get("succeeded").or(0)) + '/' + string(self.spec.completions)
                        : (
                            self.get("spec").get("parallelism").or(0) > 1
                                ? string(self.get("status").get("succeeded").or(0)) + '/1 of ' + string(self.get("spec").get("parallelism").or(0))
                                : string(self.get("status").get("succeeded").or(0)) + '/1'
                        )"#,
                ),
                ColumnDefinition::cel(
                    "Duration",
                    r#"!has(self.status.startTime) ? '0s' : age((has(self.status.completionTime) ? timestamp(self.status.completionTime) : now) - timestamp(self.status.startTime))"#,
                ),
                ColumnDefinition::cel("Age", AGE_CEL),
            ],
        );
        map.insert(
            "cronjobs".into(),
            vec![
                ColumnDefinition::json("Name", ".metadata.name"),
                ColumnDefinition::cel(
                    "Timezone",
                    r#"self.spec.get("timeZone").or('<none>')"#,
                ),
                ColumnDefinition::cel(
                    "Suspend",
                    r#"self.spec.get("suspend").or('<unset>')"#,
                ),
                ColumnDefinition::cel("Active", r#"self.get("status").get("active").or([]).size()"#),
                ColumnDefinition::cel(
                    "Last Schedule",
                    r#"has(self.status.lastScheduleTime) ? age(now - timestamp(self.status.lastScheduleTime)) : '<none>'"#,
                ),
                ColumnDefinition::cel("Age", AGE_CEL),
            ],
        );
        map.insert(
            "services".into(),
            vec![
                ColumnDefinition::json("NAME", ".metadata.name"),
                ColumnDefinition::json("TYPE", ".spec.type"),
                ColumnDefinition::json("CLUSTER-IP", ".spec.clusterIP"),
                ColumnDefinition::cel(
                    "EXTERNAL-IP",
                        r#"
                        self.get("status").get("loadBalancer").get("ingress").or([]).size() > 0
                            ? self.get("status").get("loadBalancer").get("ingress")
                                .map(ingress, ingress.get("ip").or(ingress.get("hostname")))
                                .join(',')
                            : (
                                self.spec.get("externalIPs").or([]).size() > 0
                                    ? self.spec.get("externalIPs").or([]).join(',')
                                    : '<none>'
                                )"#
                ),
                ColumnDefinition::cel(
                    "PORT(S)",
                        r#"has(self.spec.ports)
                            ? self.spec.ports
                                .map(
                                    port,
                                    string(port.port)
                                    + (has(port.nodePort) ? ':' + string(port.nodePort) : '')
                                    + '/'
                                    + (has(port.protocol) ? port.protocol : 'TCP')
                                )
                                .join(',')
                            : ''"#
                ),
                ColumnDefinition::cel("AGE", AGE_CEL),
            ],
        );
        map.insert(
            "daemonsets".into(),
            vec![
                ColumnDefinition::json("NAME", ".metadata.name"),
                ColumnDefinition::cel("DESIRED", r#"self.get("status").get("desiredNumberScheduled").or(0)"#),
                ColumnDefinition::cel("CURRENT", r#"self.get("status").get("currentNumberScheduled").or(0)"#),
                ColumnDefinition::cel("READY", r#"self.get("status").get("numberReady").or(0)"#),
                ColumnDefinition::cel("UP-TO-DATE", r#"self.get("status").get("updatedNumberScheduled").or(0)"#),
                ColumnDefinition::cel("AVAILABLE", r#"self.get("status").get("numberAvailable").or(0)"#),
                ColumnDefinition::cel(
                    "NODE SELECTOR",
                    r#"self.spec.template.spec.get("nodeSelector").or({}).size() > 0 ? self.spec.template.spec.nodeSelector.selector() : '<none>'"#,
                ),
                ColumnDefinition::cel("AGE", AGE_CEL),
            ],
        );
        map.insert(
            "configmaps".into(),
            vec![
                ColumnDefinition::json("Name", ".metadata.name"),
                ColumnDefinition::cel("Data", r#"self.get("data").or({}).size() + self.get("binaryData").or({}).size()"#),
                ColumnDefinition::cel("Age", AGE_CEL),
            ],
        );
        map.insert(
            "secrets".into(),
            vec![
                ColumnDefinition::json("Name", ".metadata.name"),
                ColumnDefinition::json("Type", ".type"),
                ColumnDefinition::cel("Data", r#"self.get("data").or({}).size()"#),
                ColumnDefinition::cel("Age", AGE_CEL),
            ],
        );
        map.insert(
            "serviceaccounts".into(),
            vec![
                ColumnDefinition::json("Name", ".metadata.name"),
                ColumnDefinition::cel("Age", AGE_CEL),
            ],
        );
        map.insert(
            "networkpolicies".into(),
            vec![
                ColumnDefinition::json("Name", ".metadata.name"),
                ColumnDefinition::cel("Pod-Selector", "self.spec.podSelector.labelSelector()"),
                ColumnDefinition::cel("Age", AGE_CEL),
            ],
        );
        map.insert(
            "poddisruptionbudgets".into(),
            vec![
                ColumnDefinition::json("Name", ".metadata.name"),
                ColumnDefinition::cel("Min Available", r#"self.spec.get("minAvailable").or('N/A')"#),
                ColumnDefinition::cel("Max Unavailable", r#"self.spec.get("maxUnavailable").or('N/A')"#),
                ColumnDefinition::cel("Allowed Disruptions", r#"self.get("status").get("disruptionsAllowed").or(0)"#),
                ColumnDefinition::cel("Age", AGE_CEL),
            ],
        );
        map.insert(
            "rolebindings".into(),
            vec![
                ColumnDefinition::json("Name", ".metadata.name"),
                ColumnDefinition::cel("Role", "self.roleRef.kind + '/' + self.roleRef.name"),
                ColumnDefinition::cel("Age", AGE_CEL),
            ],
        );
        map.insert(
            "clusterrolebindings".into(),
            vec![
                ColumnDefinition::json("Name", ".metadata.name"),
                ColumnDefinition::cel("Role", "self.roleRef.kind + '/' + self.roleRef.name"),
                ColumnDefinition::cel("Age", AGE_CEL),
            ],
        );
        map.insert(
            "leases".into(),
            vec![
                ColumnDefinition::json("Name", ".metadata.name"),
                ColumnDefinition::cel("Holder", r#"self.get("spec").get("holderIdentity").or('')"#),
                ColumnDefinition::cel("Age", AGE_CEL),
            ],
        );
        map.insert(
            "mutatingwebhookconfigurations".into(),
            vec![
                ColumnDefinition::json("Name", ".metadata.name"),
                ColumnDefinition::cel("Webhooks", r#"self.get("webhooks").or([]).size()"#),
                ColumnDefinition::cel("Age", AGE_CEL),
            ],
        );
        map.insert(
            "validatingwebhookconfigurations".into(),
            vec![
                ColumnDefinition::json("Name", ".metadata.name"),
                ColumnDefinition::cel("Webhooks", r#"self.get("webhooks").or([]).size()"#),
                ColumnDefinition::cel("Age", AGE_CEL),
            ],
        );
        map.insert(
            "validatingadmissionpolicies".into(),
            vec![
                ColumnDefinition::json("Name", ".metadata.name"),
                ColumnDefinition::cel("Validations", r#"self.get("spec").get("validations").or([]).size()"#),
                ColumnDefinition::cel(
                    "ParamKind",
                    r#"self.get("spec").get("paramKind").get("kind").or('') == '' ? '<unset>' : self.get("spec").get("paramKind").get("apiVersion").or('') + '/' + self.get("spec").get("paramKind").get("kind").or('')"#,
                ),
                ColumnDefinition::cel("Age", AGE_CEL),
            ],
        );
        map.insert(
            "validatingadmissionpolicybindings".into(),
            vec![
                ColumnDefinition::json("Name", ".metadata.name"),
                ColumnDefinition::cel("PolicyName", r#"self.get("spec").get("policyName").or('')"#),
                ColumnDefinition::cel(
                    "ParamRef",
                        r#"self.get("spec").get("paramRef").or({}).size() == 0
                            ? '<unset>'
                            : (
                                self.get("spec").get("paramRef").get("name").or('') != ''
                                    ? (
                                        self.get("spec").get("paramRef").get("namespace").or('') != ''
                                            ? self.get("spec").get("paramRef").get("namespace").or('')
                                                + '/'
                                                + self.get("spec").get("paramRef").get("name").or('')
                                            : '*/' + self.get("spec").get("paramRef").get("name").or('')
                                    )
                                    : (
                                        self.get("spec").get("paramRef").get("selector").or({}).size() > 0
                                            ? self.get("spec").get("paramRef").get("selector").labelSelector()
                                            : '<unset>'
                                    )
                            )"#
                ),
                ColumnDefinition::cel("Age", AGE_CEL),
            ],
        );
        map.insert(
            "mutatingadmissionpolicies".into(),
            vec![
                ColumnDefinition::json("Name", ".metadata.name"),
                ColumnDefinition::cel("Mutations", r#"self.get("spec").get("mutations").or([]).size()"#),
                ColumnDefinition::cel(
                    "ParamKind",
                    r#"self.get("spec").get("paramKind").get("kind").or('') == '' ? '<unset>' : self.get("spec").get("paramKind").get("apiVersion").or('') + '/' + self.get("spec").get("paramKind").get("kind").or('')"#,
                ),
                ColumnDefinition::cel("Age", AGE_CEL),
            ],
        );
        map.insert(
            "mutatingadmissionpolicybindings".into(),
            vec![
                ColumnDefinition::json("Name", ".metadata.name"),
                ColumnDefinition::cel(
                    "ParamRef",
                        r#"self.get("spec").get("paramRef").or({}).size() == 0
                            ? '<unset>'
                            : (
                                self.get("spec").get("paramRef").get("name").or('') != ''
                                    ? (
                                        self.get("spec").get("paramRef").get("namespace").or('') != ''
                                            ? self.get("spec").get("paramRef").get("namespace").or('')
                                                + '/'
                                                + self.get("spec").get("paramRef").get("name").or('')
                                            : '*/' + self.get("spec").get("paramRef").get("name").or('')
                                    )
                                    : (
                                        self.get("spec").get("paramRef").get("selector").or({}).size() > 0
                                            ? self.get("spec").get("paramRef").get("selector").labelSelector()
                                            : '<unset>'
                                    )
                            )"#
                ),
                ColumnDefinition::cel("Age", AGE_CEL),
            ],
        );
        map
    })
}

impl TablePath {
    pub fn new(column: &ColumnDefinition) -> Self {
        let json_path = if column.cel.is_some() || column.source.json_path.is_empty() {
            None
        } else {
            let json_path = format!("${}", column.source.json_path.replace(r"\.", "."));
            match JsonPath::parse(&json_path) {
                Ok(json_path) => Some(json_path),
                Err(e) => {
                    tracing::debug!("unable to parse json path for {json_path}: {e:?}");
                    None
                }
            }
        };
        Self {
            column: column.clone(),
            json_path,
        }
    }

    pub fn to_definition(&self) -> serde_json::Value {
        let mut definition = serde_json::Map::from_iter([
            ("name".into(), json!(self.column.source.name)),
            (
                "format".into(),
                json!(self.column.source.format.clone().unwrap_or_default()),
            ),
            (
                "description".into(),
                json!(self.column.source.description.clone().unwrap_or_default()),
            ),
            (
                "priority".into(),
                json!(self.column.source.priority.unwrap_or_default()),
            ),
        ]);
        if !self.column.source.type_.is_empty() {
            definition.insert("type".into(), json!(self.column.source.type_));
        }

        serde_json::Value::Object(definition)
    }

    pub fn render(&self, obj: &serde_json::Value) -> Option<serde_json::Value> {
        let Some(cel) = &self.column.cel else {
            return self
                .json_path
                .as_ref()
                .and_then(|json_path| json_path.query(obj).first().cloned());
        };

        let program = Program::compile(cel).unwrap();
        let mut context = Context::default();

        register_all(&mut context);

        context.add_function("selector", Self::cel_selector_string);
        context.add_function("labelSelector", Self::cel_label_selector_string);
        context.add_function("condition", Self::cel_condition);
        context.add_function("get", Self::cel_get);
        context.add_function("or", Self::cel_or);
        context.add_function("age", Self::cel_age);

        context.add_variable("self", obj).ok()?;
        context
            .add_variable("now", Value::Timestamp(Utc::now().fixed_offset()))
            .ok()?;

        let value = match program.execute(&context) {
            Ok(value) => value,
            Err(error) => {
                tracing::error!(
                    "failed to execute CEL for column {}: {cel}: {error}",
                    self.column.source.name
                );

                return None;
            }
        };

        Self::cel_value_to_json(value)
    }

    fn cel_value_to_json(value: Value) -> Option<serde_json::Value> {
        match value {
            Value::Map(map) => {
                let mut json = serde_json::Map::new();
                for (key, value) in map.map.iter() {
                    let Key::String(key) = key else {
                        return None;
                    };

                    json.insert(key.to_string(), Self::cel_value_to_json(value.clone())?);
                }

                Some(serde_json::Value::Object(json))
            }
            Value::List(values) => values
                .iter()
                .cloned()
                .map(Self::cel_value_to_json)
                .collect::<Option<Vec<_>>>()
                .map(serde_json::Value::Array),
            Value::Int(value) => Some(json!(value)),
            Value::UInt(value) => Some(json!(value)),
            Value::Float(value) => Some(json!(value)),
            Value::String(value) => Some(json!(value)),
            Value::Bool(value) => Some(json!(value)),
            Value::Timestamp(value) => Some(json!(value.to_rfc3339())),
            Value::Duration(value) => Some(json!(value.num_seconds())),
            Value::Null => Some(serde_json::Value::Null),
            _ => None,
        }
    }

    fn cel_selector_string(
        ftx: &FunctionContext,
        This(this): This<Value>,
    ) -> Result<Value, cel::ExecutionError> {
        let Value::Map(map) = this else {
            return Err(ftx.error(format!(
                "cannot format selector from non-map value: {this:?}"
            )));
        };

        let mut labels = BTreeMap::new();
        for (key, value) in map.map.iter() {
            let Key::String(key) = key else {
                return Err(ftx.error(format!(
                    "cannot format selector from non-string key: {key:?}"
                )));
            };
            let Value::String(value) = value else {
                return Err(ftx.error(format!(
                    "cannot format selector from non-string value for key {key}: {value:?}"
                )));
            };

            labels.insert(key.to_string(), value.to_string());
        }

        Ok(Value::String(
            Selector::from_iter(labels).to_string().into(),
        ))
    }

    fn cel_label_selector_string(
        ftx: &FunctionContext,
        This(this): This<Value>,
    ) -> Result<Value, cel::ExecutionError> {
        let json = Self::cel_value_to_json(this).ok_or_else(|| {
            ftx.error("cannot convert label selector to json-compatible object".to_string())
        })?;
        let selector: LabelSelector = serde_json::from_value(json)
            .map_err(|error| ftx.error(format!("cannot parse label selector: {error}")))?;
        let selector = Selector::try_from(selector)
            .map_err(|error| ftx.error(format!("cannot format label selector: {error}")))?;
        let selector = selector.to_string();

        Ok(Value::String(
            if selector.is_empty() {
                "<none>"
            } else {
                &selector
            }
            .to_string()
            .into(),
        ))
    }

    fn cel_condition(
        ftx: &FunctionContext,
        This(this): This<Value>,
        type_: Arc<String>,
        status: Arc<String>,
    ) -> Result<Value, cel::ExecutionError> {
        let json = Self::cel_value_to_json(this).ok_or_else(|| {
            ftx.error("cannot convert list of conditions to json-compatible object".to_string())
        })?;
        let conditions: Vec<Condition> = serde_json::from_value(json)
            .map_err(|error| ftx.error(format!("cannot parse conditions: {error}")))?;

        let Some(condition) = conditions
            .iter()
            .find(|c| c.type_ == *type_ && c.status == *status)
        else {
            return Ok(Value::Null);
        };

        let condition = serde_json::to_value(condition)
            .map_err(|e| ftx.error(format!("unable to parse condition into value: {e}")))?;

        condition
            .try_into_value()
            .map_err(|e| ftx.error(format!("unable to parse condition into CEL value: {e}")))
    }

    fn cel_age(
        ftx: &FunctionContext,
        This(this): This<Value>,
    ) -> Result<Value, cel::ExecutionError> {
        let Value::Duration(duration) = this else {
            return Err(ftx.error(format!("cannot format age from {this:?}")));
        };

        Ok(Value::String(Self::format_age(duration).into()))
    }

    fn cel_or(This(this): This<Value>, default: Value) -> Result<Value, cel::ExecutionError> {
        match this {
            Value::Null | Value::Bool(false) => Ok(default),
            value => Ok(value),
        }
    }

    fn cel_get(This(this): This<Value>, key: Value) -> Result<Value, cel::ExecutionError> {
        let Value::Map(map) = this else {
            return Ok(Value::Null);
        };

        let Ok(key) = KeyRef::try_from(&key) else {
            return Ok(Value::Null);
        };

        Ok(map.get(&key).cloned().unwrap_or(Value::Null))
    }

    fn format_age(duration: chrono::Duration) -> String {
        let seconds = duration.num_seconds().max(0);
        if seconds < 60 {
            format!("{seconds}s")
        } else {
            let minutes = seconds / 60;
            if minutes < 60 {
                format!("{minutes}m")
            } else {
                let hours = minutes / 60;
                if hours < 24 {
                    format!("{hours}h")
                } else {
                    let days = hours / 24;
                    let hours = hours % 24;
                    if hours > 0 {
                        format!("{days}d{hours}h")
                    } else {
                        format!("{days}d")
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cel::{Context, Program, Value};
    use serde_json::json;

    use crate::gather::printers::predefined_table;

    use super::TablePath;

    fn render_selector(
        value: serde_json::Value,
    ) -> Result<Option<serde_json::Value>, cel::ExecutionError> {
        let program = Program::compile("self.selector()").unwrap();
        let mut context = Context::default();
        context.add_function("selector", TablePath::cel_selector_string);
        context.add_variable("self", value).unwrap();
        let value = program.execute(&context)?;
        Ok(TablePath::cel_value_to_json(value))
    }

    fn render_label_selector(
        value: serde_json::Value,
    ) -> Result<Option<serde_json::Value>, cel::ExecutionError> {
        let program = Program::compile("self.labelSelector()").unwrap();
        let mut context = Context::default();
        context.add_function("labelSelector", TablePath::cel_label_selector_string);
        context.add_variable("self", value).unwrap();
        let value = program.execute(&context)?;
        Ok(TablePath::cel_value_to_json(value))
    }

    fn evaluate(expr: &str, self_value: serde_json::Value, default: serde_json::Value) -> Value {
        let program = Program::compile(expr).unwrap();
        let mut context = Context::default();
        context.add_function("get", TablePath::cel_get);
        context.add_function("or", TablePath::cel_or);
        context.add_variable("self", self_value).unwrap();
        context.add_variable("default", default).unwrap();
        program.execute(&context).unwrap()
    }

    fn render_condition(
        value: serde_json::Value,
        type_: &str,
        status: &str,
    ) -> Result<Value, cel::ExecutionError> {
        let program =
            Program::compile(&format!(r#"self.condition("{type_}", "{status}")"#)).unwrap();
        let mut context = Context::default();
        context.add_function("condition", TablePath::cel_condition);
        context.add_variable("self", value).unwrap();
        program.execute(&context)
    }

    #[test]
    fn selector_string_formats_match_labels() {
        assert_eq!(
            Some(json!("app=api,tier=backend")),
            render_selector(json!({
                "app": "api",
                "tier": "backend",
            }))
            .unwrap()
        );
    }

    #[test]
    fn selector_string_returns_error_for_non_string_values() {
        assert!(
            render_selector(json!({
                "app": 1,
            }))
            .is_err()
        );
    }

    #[test]
    fn selector_string_returns_error_for_non_map_values() {
        assert!(render_selector(json!("app=api")).is_err());
    }

    #[test]
    fn label_selector_string_formats_match_labels_and_expressions() {
        assert_eq!(
            Some(json!("app=api,tier in (backend,worker)")),
            render_label_selector(json!({
                "matchLabels": {
                    "app": "api"
                },
                "matchExpressions": [
                    {
                        "key": "tier",
                        "operator": "In",
                        "values": ["backend", "worker"]
                    }
                ]
            }))
            .unwrap()
        );
    }

    #[test]
    fn label_selector_string_returns_none_marker_for_empty_selector() {
        assert_eq!(
            Some(json!("<none>")),
            render_label_selector(json!({})).unwrap()
        );
    }

    #[test]
    fn condition_returns_matching_condition() {
        assert_eq!(
            Some(json!({
                "lastTransitionTime": "2026-04-14T12:00:00Z",
                "message": "",
                "reason": "",
                "type": "Ready",
                "status": "True",
            })),
            TablePath::cel_value_to_json(
                render_condition(
                    json!([
                        {
                            "lastTransitionTime": "2026-04-14T12:00:00Z",
                            "type": "Ready",
                            "status": "True",
                        },
                        {
                            "lastTransitionTime": "2026-04-14T12:01:00Z",
                            "type": "Succeeded",
                            "status": "False",
                        }
                    ]),
                    "Ready",
                    "True",
                )
                .unwrap(),
            )
        );
    }

    #[test]
    fn or_returns_default_for_null() {
        assert_eq!(
            Value::Int(3.into()),
            evaluate(
                "self.or(default).size()",
                serde_json::Value::Null,
                json!([1, 2, 3])
            )
        );
    }

    #[test]
    fn or_returns_default_for_false() {
        assert_eq!(
            Value::String(Arc::new(String::from("fallback"))),
            evaluate("self.or(default)", json!(false), json!("fallback"))
        );
    }

    #[test]
    fn or_keeps_truthy_values() {
        assert_eq!(
            Value::Int(2.into()),
            evaluate("self.or(default).size()", json!([1, 2]), json!([]))
        );
        assert_eq!(
            Value::Bool(true),
            evaluate("self.or(default)", json!(true), json!(false))
        );
        assert_eq!(
            Value::Int(0.into()),
            evaluate("self.or(default)", json!(0), json!(42))
        );
    }

    #[test]
    fn get_returns_nested_value_when_present() {
        assert_eq!(
            Value::Int(3.into()),
            evaluate(
                r#"self.get("status").get("restartCount").or(default)"#,
                json!({"status": {"restartCount": 3}}),
                json!(0),
            )
        );
    }

    #[test]
    fn get_returns_null_for_missing_paths() {
        assert_eq!(
            Value::Int(0.into()),
            evaluate(
                r#"self.get("status").get("restartCount").or(default)"#,
                json!({}),
                json!(0),
            )
        );
    }

    #[test]
    fn job_duration_renders_without_completion_time() {
        let duration_column = predefined_table("jobs")
            .unwrap()
            .into_iter()
            .find(|column| column.column.source.name == "Duration")
            .unwrap();

        let rendered = duration_column.render(&json!({
            "status": {
                "startTime": "2026-04-14T08:00:56Z"
            }
        }));

        assert!(rendered.is_some());
    }
}
