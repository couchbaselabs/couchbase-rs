use crate::clients::bucket_mgmt_client::BucketMgmtClient;
use crate::error;
use crate::management::buckets::bucket_settings::BucketSettings;
use crate::options::bucket_mgmt_options::*;
use std::sync::Arc;

#[derive(Clone)]
pub struct BucketManager {
    client: Arc<BucketMgmtClient>,
}

impl BucketManager {
    pub(crate) fn new(client: Arc<BucketMgmtClient>) -> Self {
        Self { client }
    }

    pub async fn get_all_buckets(
        &self,
        opts: impl Into<Option<GetAllBucketsOptions>>,
    ) -> error::Result<Vec<BucketSettings>> {
        self.client
            .get_all_buckets(opts.into().unwrap_or(GetAllBucketsOptions::default()))
            .await
    }

    pub async fn get_bucket(
        &self,
        bucket_name: impl Into<String>,
        opts: impl Into<Option<GetBucketOptions>>,
    ) -> error::Result<BucketSettings> {
        self.client
            .get_bucket(
                bucket_name.into(),
                opts.into().unwrap_or(GetBucketOptions::default()),
            )
            .await
    }

    pub async fn create_bucket(
        &self,
        settings: BucketSettings,
        opts: impl Into<Option<CreateBucketOptions>>,
    ) -> error::Result<()> {
        self.client
            .create_bucket(
                settings,
                opts.into().unwrap_or(CreateBucketOptions::default()),
            )
            .await
    }

    pub async fn update_bucket(
        &self,
        settings: BucketSettings,
        opts: impl Into<Option<UpdateBucketOptions>>,
    ) -> error::Result<()> {
        self.client
            .update_bucket(
                settings,
                opts.into().unwrap_or(UpdateBucketOptions::default()),
            )
            .await
    }

    pub async fn delete_bucket(
        &self,
        bucket_name: impl Into<String>,
        opts: impl Into<Option<DeleteBucketOptions>>,
    ) -> error::Result<()> {
        self.client
            .delete_bucket(
                bucket_name.into(),
                opts.into().unwrap_or(DeleteBucketOptions::default()),
            )
            .await
    }

    pub async fn flush_bucket(
        &self,
        bucket_name: impl Into<String>,
        opts: impl Into<Option<FlushBucketOptions>>,
    ) -> error::Result<()> {
        self.client
            .flush_bucket(
                bucket_name.into(),
                opts.into().unwrap_or(FlushBucketOptions::default()),
            )
            .await
    }
}
