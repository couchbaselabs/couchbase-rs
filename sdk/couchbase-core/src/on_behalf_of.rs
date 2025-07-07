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
