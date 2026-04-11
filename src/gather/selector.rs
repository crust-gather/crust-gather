use std::{borrow::Cow, collections::BTreeMap, ops::Deref};

use derive_more::Deref;
use kube::core::{Expression, SelectorExt};
use rmcp::schemars::{self, Schema};
use serde::{Deserialize, Serialize};

use tracing::instrument;

#[derive(Deserialize, Clone, Debug, Hash, PartialEq, Eq)]
pub struct Selector {
    #[serde(rename = "labelSelector")]
    label_selector: Option<String>,
}

impl Selector {
    pub fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        let Some(selector) = self.label_selector.as_deref() else {
            return true;
        };

        Expressions::try_from(selector)
            .map(|expr| expr.matches(labels))
            .unwrap_or_default()
    }
}

#[derive(Deref, Clone, Default, Serialize, Deserialize, Debug)]
pub struct Expressions(unselector::Expressions);

impl schemars::JsonSchema for Expressions {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        Cow::Borrowed("Expressions")
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> Schema {
        String::json_schema(generator)
    }
}

impl Expressions {
    pub fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        self.deref().clone().into_iter().all(|expression| {
            match expression.deref().clone() {
                unselector::Expression::In(key, btree_set) => Expression::In(key, btree_set),
                unselector::Expression::NotIn(key, btree_set) => Expression::NotIn(key, btree_set),
                unselector::Expression::Equal(key, value) => Expression::Equal(key, value),
                unselector::Expression::NotEqual(key, value) => Expression::NotEqual(key, value),
                unselector::Expression::Exists(key) => Expression::Exists(key),
                unselector::Expression::DoesNotExist(key) => Expression::DoesNotExist(key),
            }
            .matches(labels)
        })
    }
}

impl TryFrom<&str> for Expressions {
    type Error = unselector::ParseError;

    #[instrument(err)]
    fn try_from(selector: &str) -> unselector::Result<Self> {
        Ok(Expressions(selector.try_into()?))
    }
}
