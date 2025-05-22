#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAllBucketsOptions {}

impl GetAllBucketsOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetBucketOptions {}

impl GetBucketOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CreateBucketOptions {}

impl CreateBucketOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpdateBucketOptions {}

impl UpdateBucketOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DeleteBucketOptions {}

impl DeleteBucketOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct FlushBucketOptions {}

impl FlushBucketOptions {
    pub fn new() -> Self {
        Default::default()
    }
}
