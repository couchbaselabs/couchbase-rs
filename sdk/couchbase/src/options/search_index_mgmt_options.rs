#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetSearchIndexOptions {}

impl GetSearchIndexOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAllSearchIndexesOptions {}

impl GetAllSearchIndexesOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpsertSearchIndexOptions {}

impl UpsertSearchIndexOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropSearchIndexOptions {}

impl DropSearchIndexOptions {
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
pub struct PauseIngestSearchIndexOptions {}

impl PauseIngestSearchIndexOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct ResumeIngestSearchIndexOptions {}

impl ResumeIngestSearchIndexOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct AllowQueryingSearchIndexOptions {}

impl AllowQueryingSearchIndexOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DisallowQueryingSearchIndexOptions {}

impl DisallowQueryingSearchIndexOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct FreezePlanSearchIndexOptions {}

impl FreezePlanSearchIndexOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UnfreezePlanSearchIndexOptions {}

impl UnfreezePlanSearchIndexOptions {
    pub fn new() -> Self {
        Self::default()
    }
}
