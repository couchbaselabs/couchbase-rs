#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CreateScopeResponse {
    pub manifest_uid: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeleteScopeResponse {
    pub manifest_uid: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CreateCollectionResponse {
    pub manifest_uid: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct UpdateCollectionResponse {
    pub manifest_uid: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeleteCollectionResponse {
    pub manifest_uid: String,
}
