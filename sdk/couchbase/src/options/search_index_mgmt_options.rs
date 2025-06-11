#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetIndexOptions {}

impl GetIndexOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAllIndexesOptions {}

impl GetAllIndexesOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpsertIndexOptions {}

impl UpsertIndexOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DeleteIndexOptions {}

impl DeleteIndexOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct AnalyzeDocumentOptions {}

impl AnalyzeDocumentOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetIndexedDocumentsCountOptions {}

impl GetIndexedDocumentsCountOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct PauseIngestOptions {}

impl PauseIngestOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct ResumeIngestOptions {}

impl ResumeIngestOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct AllowQueryingOptions {}

impl AllowQueryingOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DisallowQueryingOptions {}

impl DisallowQueryingOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct FreezePlanOptions {}

impl FreezePlanOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UnfreezePlanOptions {}

impl UnfreezePlanOptions {
    pub fn new() -> Self {
        Self::default()
    }
}
