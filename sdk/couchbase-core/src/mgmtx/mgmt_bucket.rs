use crate::httpx::client::Client;
use crate::mgmtx::bucket_settings::{encode_bucket_settings, BucketDef};
use crate::mgmtx::bucket_settings_json::BucketSettingsJson;
use crate::mgmtx::error;
use crate::mgmtx::mgmt::{parse_response_json, Management};
use crate::mgmtx::options::{
    CreateBucketOptions, DeleteBucketOptions, FlushBucketOptions, GetAllBucketsOptions,
    GetBucketOptions, UpdateBucketOptions,
};
use bytes::Bytes;
use http::Method;

impl<C: Client> Management<C> {
    pub async fn get_all_buckets(
        &self,
        opts: &GetAllBucketsOptions<'_>,
    ) -> error::Result<Vec<BucketDef>> {
        let resp = self
            .execute(
                Method::GET,
                "/pools/default/buckets",
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        let json_buckets: Vec<BucketSettingsJson> = parse_response_json(resp).await?;
        let mut buckets = Vec::with_capacity(json_buckets.len());
        for bucket in json_buckets {
            buckets.push(bucket.into());
        }

        Ok(buckets)
    }

    pub async fn get_bucket(&self, opts: &GetBucketOptions<'_>) -> error::Result<BucketDef> {
        let resp = self
            .execute(
                Method::GET,
                format!("/pools/default/buckets/{}", opts.bucket_name),
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        let bucket: BucketSettingsJson = parse_response_json(resp).await?;

        Ok(bucket.into())
    }

    pub async fn create_bucket(&self, opts: &CreateBucketOptions<'_>) -> error::Result<()> {
        let body = {
            // Serializer is not Send so we need to drop it before making the request.
            let mut form = url::form_urlencoded::Serializer::new(String::new());
            form.append_pair("name", opts.bucket_name);
            encode_bucket_settings(&mut form, opts.bucket_settings);

            Bytes::from(form.finish())
        };

        let resp = self
            .execute(
                Method::POST,
                "/pools/default/buckets",
                "application/x-www-form-urlencoded",
                opts.on_behalf_of_info.cloned(),
                None,
                Some(body),
            )
            .await?;

        if resp.status() != 202 {
            return Err(Self::decode_common_error(resp).await);
        }

        Ok(())
    }

    pub async fn update_bucket(&self, opts: &UpdateBucketOptions<'_>) -> error::Result<()> {
        let body = {
            // Serializer is not Send so we need to drop it before making the request.
            let mut form = url::form_urlencoded::Serializer::new(String::new());
            encode_bucket_settings(&mut form, opts.bucket_settings);

            Bytes::from(form.finish())
        };

        let resp = self
            .execute(
                Method::POST,
                format!("/pools/default/buckets/{}", opts.bucket_name),
                "application/x-www-form-urlencoded",
                opts.on_behalf_of_info.cloned(),
                None,
                Some(body),
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        Ok(())
    }

    pub async fn delete_bucket(&self, opts: &DeleteBucketOptions<'_>) -> error::Result<()> {
        let resp = self
            .execute(
                Method::DELETE,
                format!("/pools/default/buckets/{}", opts.bucket_name),
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            let e = Self::decode_common_error(resp).await;
            match e.kind() {
                error::ErrorKind::Server(e) => {
                    // A delayed operation is considered a success for deletion, since
                    // bucket management is already eventually consistent anyways.
                    if e.kind() == &error::ServerErrorKind::OperationDelayed {
                        return Ok(());
                    }
                }
                _ => return Err(e),
            }
        }

        Ok(())
    }

    pub async fn flush_bucket(&self, opts: &FlushBucketOptions<'_>) -> error::Result<()> {
        let resp = self
            .execute(
                Method::POST,
                format!(
                    "/pools/default/buckets/{}/controller/doFlush",
                    opts.bucket_name
                ),
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        Ok(())
    }
}
