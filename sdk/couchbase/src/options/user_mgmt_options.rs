use couchbase_core::retry::RetryStrategy;
use std::sync::Arc;

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetUserOptions {
    pub auth_domain: Option<String>,
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl GetUserOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn auth_domain(mut self, auth_domain: impl Into<String>) -> Self {
        self.auth_domain = Some(auth_domain.into());
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAllUsersOptions {
    pub auth_domain: Option<String>,
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl GetAllUsersOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn auth_domain(mut self, auth_domain: impl Into<String>) -> Self {
        self.auth_domain = Some(auth_domain.into());
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpsertUserOptions {
    pub auth_domain: Option<String>,
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl UpsertUserOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn auth_domain(mut self, auth_domain: impl Into<String>) -> Self {
        self.auth_domain = Some(auth_domain.into());
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropUserOptions {
    pub auth_domain: Option<String>,
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl DropUserOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn auth_domain(mut self, auth_domain: impl Into<String>) -> Self {
        self.auth_domain = Some(auth_domain.into());
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetRolesOptions {
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl GetRolesOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetGroupOptions {
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl GetGroupOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAllGroupsOptions {
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl GetAllGroupsOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpsertGroupOptions {
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl UpsertGroupOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropGroupOptions {
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl DropGroupOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct ChangePasswordOptions {
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl ChangePasswordOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}
