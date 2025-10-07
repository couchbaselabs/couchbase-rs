/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use couchbase::error;
use couchbase::management::buckets::bucket_manager::BucketManager;
use couchbase::management::buckets::bucket_settings::*;
use couchbase::management::collections::collection_manager::*;
use couchbase::management::users::user::*;
use couchbase::management::users::user_manager::UserManager;
use couchbase::options::bucket_mgmt_options::*;
use couchbase::options::collection_mgmt_options::*;
use couchbase::options::user_mgmt_options::*;
use couchbase::results::collections_mgmt_results::ScopeSpec;
use tokio::time::{timeout, Duration};

#[derive(Clone)]
pub struct TestBucketManager {
    inner: BucketManager,
}

impl std::ops::Deref for TestBucketManager {
    type Target = BucketManager;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TestBucketManager {
    pub fn new(inner: BucketManager) -> Self {
        Self { inner }
    }

    pub async fn get_all_buckets(
        &self,
        opts: impl Into<Option<GetAllBucketsOptions>>,
    ) -> error::Result<Vec<BucketSettings>> {
        timeout(Duration::from_secs(20), self.inner.get_all_buckets(opts))
            .await
            .unwrap()
    }

    pub async fn get_bucket(
        &self,
        bucket_name: impl Into<String>,
        opts: impl Into<Option<GetBucketOptions>>,
    ) -> error::Result<BucketSettings> {
        timeout(
            Duration::from_secs(20),
            self.inner.get_bucket(bucket_name, opts),
        )
        .await
        .unwrap()
    }

    pub async fn create_bucket(
        &self,
        settings: BucketSettings,
        opts: impl Into<Option<CreateBucketOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.create_bucket(settings, opts),
        )
        .await
        .unwrap()
    }

    pub async fn update_bucket(
        &self,
        settings: BucketSettings,
        opts: impl Into<Option<UpdateBucketOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.update_bucket(settings, opts),
        )
        .await
        .unwrap()
    }

    pub async fn drop_bucket(
        &self,
        bucket_name: impl Into<String>,
        opts: impl Into<Option<DropBucketOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.drop_bucket(bucket_name, opts),
        )
        .await
        .unwrap()
    }

    pub async fn flush_bucket(
        &self,
        bucket_name: impl Into<String>,
        opts: impl Into<Option<FlushBucketOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.flush_bucket(bucket_name, opts),
        )
        .await
        .unwrap()
    }
}

#[derive(Clone)]
pub struct TestUserManager {
    inner: UserManager,
}

impl std::ops::Deref for TestUserManager {
    type Target = UserManager;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TestUserManager {
    pub fn new(inner: UserManager) -> Self {
        Self { inner }
    }

    pub async fn get_all_users(
        &self,
        opts: impl Into<Option<GetAllUsersOptions>>,
    ) -> error::Result<Vec<UserAndMetadata>> {
        timeout(Duration::from_secs(20), self.inner.get_all_users(opts))
            .await
            .unwrap()
    }

    pub async fn get_user(
        &self,
        username: impl Into<String>,
        opts: impl Into<Option<GetUserOptions>>,
    ) -> error::Result<UserAndMetadata> {
        timeout(Duration::from_secs(20), self.inner.get_user(username, opts))
            .await
            .unwrap()
    }

    pub async fn upsert_user(
        &self,
        settings: User,
        opts: impl Into<Option<UpsertUserOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.upsert_user(settings, opts),
        )
        .await
        .unwrap()
    }

    pub async fn drop_user(
        &self,
        username: impl Into<String>,
        opts: impl Into<Option<DropUserOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.drop_user(username, opts),
        )
        .await
        .unwrap()
    }

    pub async fn get_roles(
        &self,
        opts: impl Into<Option<GetRolesOptions>>,
    ) -> error::Result<Vec<RoleAndDescription>> {
        timeout(Duration::from_secs(20), self.inner.get_roles(opts))
            .await
            .unwrap()
    }

    pub async fn get_group(
        &self,
        group_name: impl Into<String>,
        opts: impl Into<Option<GetGroupOptions>>,
    ) -> error::Result<Group> {
        timeout(
            Duration::from_secs(20),
            self.inner.get_group(group_name, opts),
        )
        .await
        .unwrap()
    }

    pub async fn get_all_groups(
        &self,
        opts: impl Into<Option<GetAllGroupsOptions>>,
    ) -> error::Result<Vec<Group>> {
        timeout(Duration::from_secs(20), self.inner.get_all_groups(opts))
            .await
            .unwrap()
    }

    pub async fn upsert_group(
        &self,
        group: Group,
        opts: impl Into<Option<UpsertGroupOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.upsert_group(group, opts),
        )
        .await
        .unwrap()
    }

    pub async fn drop_group(
        &self,
        group_name: impl Into<String>,
        opts: impl Into<Option<DropGroupOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.drop_group(group_name, opts),
        )
        .await
        .unwrap()
    }

    pub async fn change_password(
        &self,
        password: impl Into<String>,
        opts: impl Into<Option<ChangePasswordOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.change_password(password, opts),
        )
        .await
        .unwrap()
    }
}

#[derive(Clone)]
pub struct TestCollectionManager {
    inner: CollectionManager,
}

impl std::ops::Deref for TestCollectionManager {
    type Target = CollectionManager;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TestCollectionManager {
    pub fn new(inner: CollectionManager) -> Self {
        Self { inner }
    }

    pub async fn create_scope(
        &self,
        scope_name: impl Into<String>,
        opts: impl Into<Option<CreateScopeOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.create_scope(scope_name, opts),
        )
        .await
        .unwrap()
    }

    pub async fn drop_scope(
        &self,
        scope_name: impl Into<String>,
        opts: impl Into<Option<DropScopeOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.drop_scope(scope_name, opts),
        )
        .await
        .unwrap()
    }

    pub async fn create_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        settings: CreateCollectionSettings,
        opts: impl Into<Option<CreateCollectionOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner
                .create_collection(scope_name, collection_name, settings, opts),
        )
        .await
        .unwrap()
    }

    pub async fn update_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        settings: UpdateCollectionSettings,
        opts: impl Into<Option<UpdateCollectionOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner
                .update_collection(scope_name, collection_name, settings, opts),
        )
        .await
        .unwrap()
    }

    pub async fn drop_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        opts: impl Into<Option<DropCollectionOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner
                .drop_collection(scope_name, collection_name, opts),
        )
        .await
        .unwrap()
    }

    pub async fn get_all_scopes(
        &self,
        opts: impl Into<Option<GetAllScopesOptions>>,
    ) -> error::Result<Vec<ScopeSpec>> {
        timeout(Duration::from_secs(20), self.inner.get_all_scopes(opts))
            .await
            .unwrap()
    }
}
