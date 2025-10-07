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

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetUserOptions {
    pub auth_domain: Option<String>,
}

impl GetUserOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn auth_domain(mut self, auth_domain: impl Into<String>) -> Self {
        self.auth_domain = Some(auth_domain.into());
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAllUsersOptions {
    pub auth_domain: Option<String>,
}

impl GetAllUsersOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn auth_domain(mut self, auth_domain: impl Into<String>) -> Self {
        self.auth_domain = Some(auth_domain.into());
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpsertUserOptions {
    pub auth_domain: Option<String>,
}

impl UpsertUserOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn auth_domain(mut self, auth_domain: impl Into<String>) -> Self {
        self.auth_domain = Some(auth_domain.into());
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropUserOptions {
    pub auth_domain: Option<String>,
}

impl DropUserOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn auth_domain(mut self, auth_domain: impl Into<String>) -> Self {
        self.auth_domain = Some(auth_domain.into());
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetRolesOptions {}

impl GetRolesOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetGroupOptions {}

impl GetGroupOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAllGroupsOptions {}

impl GetAllGroupsOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpsertGroupOptions {}

impl UpsertGroupOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropGroupOptions {}

impl DropGroupOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct ChangePasswordOptions {}

impl ChangePasswordOptions {
    pub fn new() -> Self {
        Self::default()
    }
}
