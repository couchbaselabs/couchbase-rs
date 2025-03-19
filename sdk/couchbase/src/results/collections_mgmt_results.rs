use crate::collection_manager::MaxExpiryValue;

#[derive(Default, Debug, Clone)]
pub struct ScopeSpec {
    pub(crate) name: String,
    pub(crate) collections: Vec<CollectionSpec>,
}

impl ScopeSpec {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn collections(&self) -> &[CollectionSpec] {
        &self.collections
    }
}

#[derive(Debug, Clone)]
pub struct CollectionSpec {
    pub(crate) name: String,
    pub(crate) scope_name: String,
    pub(crate) max_expiry: MaxExpiryValue,
    pub(crate) history: bool,
}

impl CollectionSpec {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn scope_name(&self) -> &str {
        &self.scope_name
    }

    pub fn max_expiry(&self) -> MaxExpiryValue {
        self.max_expiry
    }

    pub fn history(&self) -> bool {
        self.history
    }
}
