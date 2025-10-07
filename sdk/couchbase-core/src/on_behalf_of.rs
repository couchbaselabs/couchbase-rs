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

use crate::{error, httpx};

#[derive(Clone, PartialEq, Eq, Debug)]
#[non_exhaustive]
pub struct OnBehalfOfInfo {
    pub(crate) username: String,
    pub(crate) password_or_domain: Option<OboPasswordOrDomain>,
}

impl OnBehalfOfInfo {
    pub fn new(username: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password_or_domain: None,
        }
    }

    pub fn password_or_domain(mut self, password_or_domain: OboPasswordOrDomain) -> Self {
        self.password_or_domain = Some(password_or_domain);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OboPasswordOrDomain {
    Password(String),
    Domain(String),
}

impl TryFrom<OnBehalfOfInfo> for httpx::request::OnBehalfOfInfo {
    type Error = error::Error;

    fn try_from(info: OnBehalfOfInfo) -> Result<Self, Self::Error> {
        let password_or_domain = info.password_or_domain.ok_or_else(|| {
            error::Error::new_message_error("OnBehalfOfInfo must have a password or domain set")
        })?;

        Ok(httpx::request::OnBehalfOfInfo {
            username: info.username,
            password_or_domain: password_or_domain.into(),
        })
    }
}

impl From<OboPasswordOrDomain> for httpx::request::OboPasswordOrDomain {
    fn from(info: OboPasswordOrDomain) -> Self {
        match info {
            OboPasswordOrDomain::Password(password) => {
                httpx::request::OboPasswordOrDomain::Password(password)
            }
            OboPasswordOrDomain::Domain(domain) => {
                httpx::request::OboPasswordOrDomain::Domain(domain)
            }
        }
    }
}
