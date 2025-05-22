#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAllScopesOptions {}

impl GetAllScopesOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CreateScopeOptions {}

impl CreateScopeOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropScopeOptions {}

impl DropScopeOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CreateCollectionOptions {}

impl CreateCollectionOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpdateCollectionOptions {}

impl UpdateCollectionOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropCollectionOptions {}

impl DropCollectionOptions {
    pub fn new() -> Self {
        Default::default()
    }
}
