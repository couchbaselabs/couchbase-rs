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
