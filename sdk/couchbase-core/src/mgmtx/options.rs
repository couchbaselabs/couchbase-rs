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

use crate::httpx::client::Client;
use crate::httpx::request::OnBehalfOfInfo;
use crate::mgmtx::bucket_settings::BucketSettings;
use crate::mgmtx::node_target::NodeTarget;
use crate::mgmtx::user::{Group, User};
use std::sync::Arc;

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct GetCollectionManifestOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct CreateScopeOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct DeleteScopeOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct CreateCollectionOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub max_ttl: Option<i32>,
    pub history_enabled: Option<bool>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct UpdateCollectionOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub max_ttl: Option<i32>,
    pub history_enabled: Option<bool>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct DeleteCollectionOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,
    pub collection_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct GetTerseClusterConfigOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct GetTerseBucketConfigOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnsureManifestPollOptions<C: Client> {
    pub client: Arc<C>,
    pub targets: Vec<NodeTarget>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct GetAllBucketsOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct GetBucketOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct CreateBucketOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub bucket_settings: &'a BucketSettings,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct UpdateBucketOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub bucket_settings: &'a BucketSettings,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct DeleteBucketOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct FlushBucketOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnsureBucketPollOptions<C: Client> {
    pub client: Arc<C>,
    pub targets: Vec<NodeTarget>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct GetUserOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub username: &'a str,
    pub auth_domain: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct GetAllUsersOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub auth_domain: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct UpsertUserOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub user: &'a User,
    pub auth_domain: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct DeleteUserOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub username: &'a str,
    pub auth_domain: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct GetRolesOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct GetGroupOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub group_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct GetAllGroupsOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct UpsertGroupOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub group: &'a Group,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct DeleteGroupOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub group_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct ChangePasswordOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub new_password: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnsureUserPollOptions<C: Client> {
    pub client: Arc<C>,
    pub targets: Vec<NodeTarget>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnsureGroupPollOptions<C: Client> {
    pub client: Arc<C>,
    pub targets: Vec<NodeTarget>,
}
