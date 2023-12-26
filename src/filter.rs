use kube_core::DynamicObject;

pub trait Filter: Sync + Send {
    fn filter(&self, obj: &DynamicObject) -> bool;
}

#[derive(Clone, Default)]
pub struct NamespaceInclude {
    pub namespaces: Vec<String>,
}

impl Filter for NamespaceInclude {
    fn filter(&self, obj: &DynamicObject) -> bool {
        return self.namespaces.is_empty()
            || obj
                .metadata
                .namespace
                .clone()
                .unwrap_or_default()
                .is_empty()
            || self
                .namespaces
                .contains(&obj.metadata.namespace.clone().unwrap_or_default());
    }
}

impl From<&NamespaceInclude> for Box<dyn Filter> {
    fn from(f: &NamespaceInclude) -> Self {
        Box::new(f.clone())
    }
}

impl From<NamespaceInclude> for Box<dyn Filter> {
    fn from(f: NamespaceInclude) -> Self {
        Box::new(f.clone())
    }
}

impl From<String> for NamespaceInclude {
    fn from(s: String) -> Self {
        NamespaceInclude {
            namespaces: s.split(",").map(String::from).collect(),
        }
    }
}
