use super::*;
use crate::api::analytics_options::AnalyticsOptions;
use crate::api::cluster::ServiceType;
use crate::io::request::*;
use crate::{CouchbaseError, CouchbaseResult, ErrorContext, GenericManagementResult};
use futures::channel::oneshot;
use futures::StreamExt;
use serde_derive::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;
#[derive(Debug, Clone, Deserialize)]
pub struct AnalyticsDataset {
    #[serde(rename = "DatasetName")]
    name: String,
    #[serde(rename = "DataverseName")]
    dataverse_name: String,
    #[serde(rename = "LinkName")]
    link_name: String,
    #[serde(rename = "BucketName")]
    bucket_name: String,
}

impl AnalyticsDataset {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn dataverse_name(&self) -> &str {
        &self.dataverse_name
    }
    pub fn link_name(&self) -> &str {
        &self.link_name
    }
    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AnalyticsIndex {
    #[serde(rename = "DatasetName")]
    name: String,
    #[serde(rename = "DataverseName")]
    dataverse_name: String,
    #[serde(rename = "DatasetName")]
    dataset_name: String,
    #[serde(rename = "IsPrimary")]
    is_primary: bool,
}

impl AnalyticsIndex {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn dataverse_name(&self) -> &str {
        &self.dataverse_name
    }
    pub fn dataset_name(&self) -> &str {
        &self.dataset_name
    }
    pub fn is_primary(&self) -> bool {
        self.is_primary
    }
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
#[non_exhaustive]
pub enum AnalyticsLinkType {
    CouchbaseRemote,
    S3External,
    #[cfg(feature = "volatile")]
    AzureBlobExternal,
}

impl Display for AnalyticsLinkType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let alias = match *self {
            AnalyticsLinkType::CouchbaseRemote => "couchbase",
            AnalyticsLinkType::S3External => "s3",
            #[cfg(feature = "volatile")]
            AnalyticsLinkType::AzureBlobExternal => "azureblob",
        };

        write!(f, "{}", alias)
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum AnalyticsLink {
    CouchbaseRemote(CouchbaseRemoteAnalyticsLink),
    S3External(S3ExternalAnalyticsLink),
    #[cfg(feature = "volatile")]
    AzureBlobExternal(AzureBlobExternalAnalyticsLink),
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum AnalyticsEncryptionLevel {
    None,
    Half,
    Full,
}

impl From<String> for AnalyticsEncryptionLevel {
    fn from(val: String) -> Self {
        match val.as_str() {
            "none" => AnalyticsEncryptionLevel::None,
            "half" => AnalyticsEncryptionLevel::Half,
            "full" => AnalyticsEncryptionLevel::Full,
            _ => AnalyticsEncryptionLevel::None,
        }
    }
}

impl Display for AnalyticsEncryptionLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let alias = match *self {
            AnalyticsEncryptionLevel::None => "none",
            AnalyticsEncryptionLevel::Half => "half",
            AnalyticsEncryptionLevel::Full => "full",
        };

        write!(f, "{}", alias)
    }
}

#[derive(Debug, Clone)]
pub struct CouchbaseAnalyticsEncryptionSettingsBuilder {
    level: AnalyticsEncryptionLevel,
    certificate: Option<String>,
    client_certificate: Option<String>,
    client_key: Option<String>,
}

impl CouchbaseAnalyticsEncryptionSettingsBuilder {
    pub fn new(level: AnalyticsEncryptionLevel) -> Self {
        Self {
            level,
            certificate: None,
            client_certificate: None,
            client_key: None,
        }
    }

    pub fn certificate(mut self, certificate: impl Into<String>) -> Self {
        self.certificate = Some(certificate.into());
        self
    }

    pub fn client_certificate(mut self, certificate: impl Into<String>) -> Self {
        self.client_certificate = Some(certificate.into());
        self
    }

    pub fn client_key(mut self, key: impl Into<String>) -> Self {
        self.client_key = Some(key.into());
        self
    }

    pub fn build(self) -> CouchbaseAnalyticsEncryptionSettings {
        CouchbaseAnalyticsEncryptionSettings {
            encryption_level: self.level,
            certificate: self.certificate,
            client_certificate: self.client_certificate,
            client_key: self.client_key,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CouchbaseAnalyticsEncryptionSettings {
    encryption_level: AnalyticsEncryptionLevel,
    certificate: Option<String>,
    client_certificate: Option<String>,
    client_key: Option<String>,
}

impl Default for CouchbaseAnalyticsEncryptionSettings {
    fn default() -> Self {
        Self {
            encryption_level: AnalyticsEncryptionLevel::None,
            certificate: None,
            client_certificate: None,
            client_key: None,
        }
    }
}

impl CouchbaseAnalyticsEncryptionSettings {
    pub fn encryption_level(&self) -> AnalyticsEncryptionLevel {
        self.encryption_level
    }
    pub fn certificate(&self) -> Option<&String> {
        self.certificate.as_ref()
    }
    pub fn client_certificate(&self) -> Option<&String> {
        self.client_certificate.as_ref()
    }
    pub fn set_encryption_level(&mut self, level: AnalyticsEncryptionLevel) {
        self.encryption_level = level;
    }
    pub fn set_certificate(&mut self, certificate: impl Into<Option<String>>) {
        self.certificate = certificate.into();
    }
    pub fn set_client_certificate(&mut self, certificate: impl Into<Option<String>>) {
        self.client_certificate = certificate.into();
    }
    pub fn set_client_key(&mut self, key: impl Into<Option<String>>) {
        self.client_key = key.into();
    }
}

#[derive(Debug, Clone)]
pub struct CouchbaseRemoteAnalyticsLinkBuilder {
    name: String,
    dataverse_name: String,
    hostname: String,
    username: Option<String>,
    password: Option<String>,
    encryption: Option<CouchbaseAnalyticsEncryptionSettings>,
}

impl CouchbaseRemoteAnalyticsLinkBuilder {
    pub fn new(
        name: impl Into<String>,
        dataverse_name: impl Into<String>,
        hostname: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            dataverse_name: dataverse_name.into(),
            hostname: hostname.into(),
            username: None,
            password: None,
            encryption: None,
        }
    }

    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    pub fn encryption(mut self, encryption: CouchbaseAnalyticsEncryptionSettings) -> Self {
        self.encryption = Some(encryption);
        self
    }

    pub fn build(self) -> CouchbaseRemoteAnalyticsLink {
        CouchbaseRemoteAnalyticsLink {
            name: self.name,
            dataverse_name: self.dataverse_name,
            hostname: self.hostname,
            username: self.username,
            password: self.password,
            encryption: self.encryption,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CouchbaseRemoteAnalyticsLink {
    name: String,
    dataverse_name: String,
    hostname: String,
    username: Option<String>,
    password: Option<String>,
    encryption: Option<CouchbaseAnalyticsEncryptionSettings>,
}

impl CouchbaseRemoteAnalyticsLink {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn dataverse_name(&self) -> &str {
        &self.dataverse_name
    }
    pub fn hostname(&self) -> &str {
        &self.hostname
    }
    pub fn username(&self) -> Option<&String> {
        self.username.as_ref()
    }
    pub fn encryption(&self) -> Option<&CouchbaseAnalyticsEncryptionSettings> {
        self.encryption.as_ref()
    }
    pub fn set_hostname(&mut self, hostname: impl Into<String>) {
        self.hostname = hostname.into();
    }
    pub fn set_username(&mut self, username: impl Into<Option<String>>) {
        self.username = username.into();
    }
    pub fn set_password(&mut self, password: impl Into<Option<String>>) {
        self.password = password.into();
    }
    pub fn set_encryption(
        &mut self,
        encryption: impl Into<Option<CouchbaseAnalyticsEncryptionSettings>>,
    ) {
        self.encryption = encryption.into();
    }

    fn validate(&self) -> CouchbaseResult<()> {
        if self.name.is_empty() {
            return Err(CouchbaseError::InvalidArgument {
                ctx: ErrorContext::from(("name", "Name cannot be empty")),
            });
        }
        if self.dataverse_name.is_empty() {
            return Err(CouchbaseError::InvalidArgument {
                ctx: ErrorContext::from(("dataverse_name", "Dataverse name cannot be empty")),
            });
        }
        if self.hostname.is_empty() {
            return Err(CouchbaseError::InvalidArgument {
                ctx: ErrorContext::from(("hostname", "Hostname cannot be empty")),
            });
        }
        if let Some(e) = &self.encryption {
            match e.encryption_level {
                AnalyticsEncryptionLevel::Full => {
                    if e.certificate.is_none() {
                        return Err(CouchbaseError::InvalidArgument {
                            ctx: ErrorContext::from((
                                "certificate",
                                "Certificate must be set when full encryption is used",
                            )),
                        });
                    }

                    let basic_creds_bad = self.username.is_none() || self.password.is_none();
                    let cert_creds_bad = e.client_certificate.is_none() || e.client_key.is_none();
                    if basic_creds_bad && cert_creds_bad {
                        return Err(CouchbaseError::InvalidArgument {
                            ctx: ErrorContext::from(("credentials", "Either username and password or client certificate and client key must be provided")),
                        });
                    }
                }
                _ => {
                    if self.username.is_none() {
                        return Err(CouchbaseError::InvalidArgument {
                            ctx: ErrorContext::from((
                                "username",
                                "Username must be provided when encryption is not set to full",
                            )),
                        });
                    }
                    if self.password.is_none() {
                        return Err(CouchbaseError::InvalidArgument {
                            ctx: ErrorContext::from((
                                "password",
                                "Password must be provided when encryption is not set to full",
                            )),
                        });
                    }
                }
            }
        } else {
            if self.username.is_none() {
                return Err(CouchbaseError::InvalidArgument {
                    ctx: ErrorContext::from((
                        "username",
                        "Username must be provided when encryption is not used",
                    )),
                });
            }
            if self.password.is_none() {
                return Err(CouchbaseError::InvalidArgument {
                    ctx: ErrorContext::from((
                        "password",
                        "Password must be provided when encryption is not used",
                    )),
                });
            }
        }
        Ok(())
    }
    fn encode(&self) -> CouchbaseResult<Vec<(&str, String)>> {
        let mut form = vec![
            ("hostname", self.hostname.clone()),
            ("type", String::from("couchbase")),
        ];

        if self.dataverse_name.contains('/') {
            form.push(("name", self.name.clone()));
            form.push(("dataverse", self.dataverse_name.clone()));
        }
        if let Some(u) = &self.username {
            form.push(("username", u.clone()))
        }
        if let Some(p) = &self.password {
            form.push(("password", p.clone()))
        }
        if let Some(e) = &self.encryption {
            form.push(("encryption", e.encryption_level.to_string()));
            if let Some(c) = &e.certificate {
                form.push(("certificate", c.clone()));
            }
            if let Some(c) = &e.client_certificate {
                form.push(("clientCertificate", c.clone()));
            }
            if let Some(k) = &e.client_key {
                form.push(("clientKey", k.clone()));
            }
        }

        Ok(form)
    }
}

impl From<Value> for CouchbaseRemoteAnalyticsLink {
    fn from(v: Value) -> Self {
        let dataverse_name = if let Some(d) = v.get("dataverse") {
            d.to_string()
        } else {
            v["scope"].to_string()
        };
        let username = v.get("username").map(|u| u.to_string());
        let encryption = match v.get("encryption") {
            Some(level) => {
                let certificate = v.get("certificate").map(|val| val.to_string());
                let client_certificate = v.get("clientCertificate").map(|val| val.to_string());
                Some(CouchbaseAnalyticsEncryptionSettings {
                    encryption_level: AnalyticsEncryptionLevel::from(level.to_string()),
                    certificate,
                    client_certificate,
                    client_key: None,
                })
            }
            None => None,
        };

        CouchbaseRemoteAnalyticsLink {
            name: v["name"].to_string(),
            dataverse_name,
            hostname: v["activeHostname"].to_string(),
            username,
            password: None,
            encryption,
        }
    }
}

#[derive(Debug, Clone)]
pub struct S3ExternalAnalyticsLinkBuilder {
    name: String,
    dataverse_name: String,
    access_key_id: String,
    secret_access_key: String,
    region: String,
    session_token: Option<String>,
    service_endpoint: Option<String>,
}

impl S3ExternalAnalyticsLinkBuilder {
    pub fn new(
        name: impl Into<String>,
        dataverse_name: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            dataverse_name: dataverse_name.into(),
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            region: region.into(),
            session_token: None,
            service_endpoint: None,
        }
    }

    pub fn session_token(mut self, token: impl Into<String>) -> Self {
        self.session_token = Some(token.into());
        self
    }

    pub fn service_endpoint(mut self, service_endpoint: impl Into<String>) -> Self {
        self.service_endpoint = Some(service_endpoint.into());
        self
    }

    pub fn build(self) -> S3ExternalAnalyticsLink {
        S3ExternalAnalyticsLink {
            name: self.name,
            dataverse_name: self.dataverse_name,
            access_key_id: self.access_key_id,
            secret_access_key: self.secret_access_key,
            region: self.region,
            session_token: self.session_token,
            service_endpoint: self.service_endpoint,
        }
    }
}
#[derive(Debug, Clone)]
pub struct S3ExternalAnalyticsLink {
    name: String,
    dataverse_name: String,
    access_key_id: String,
    secret_access_key: String,
    region: String,
    session_token: Option<String>,
    service_endpoint: Option<String>,
}

impl From<Value> for S3ExternalAnalyticsLink {
    fn from(v: Value) -> Self {
        let dataverse_name = if let Some(d) = v.get("dataverse") {
            d.to_string()
        } else {
            v["scope"].to_string()
        };
        let service_endpoint = v.get("serviceEndpoint").map(|u| u.to_string());

        S3ExternalAnalyticsLink {
            name: v["name"].to_string(),
            dataverse_name,
            access_key_id: v["accessKeyId"].to_string(),
            secret_access_key: "".to_string(),
            region: v["region"].to_string(),
            session_token: None,
            service_endpoint,
        }
    }
}

impl S3ExternalAnalyticsLink {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn dataverse_name(&self) -> &str {
        &self.dataverse_name
    }
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }
    pub fn region(&self) -> &str {
        &self.region
    }
    pub fn service_endpoint(&self) -> Option<&String> {
        self.service_endpoint.as_ref()
    }
    pub fn set_access_key_id(&mut self, access_key_id: impl Into<String>) {
        self.access_key_id = access_key_id.into();
    }
    pub fn set_secret_access_key(&mut self, secret_access_key: impl Into<String>) {
        self.secret_access_key = secret_access_key.into();
    }
    pub fn set_region(&mut self, region: impl Into<String>) {
        self.region = region.into();
    }
    pub fn set_session_token(&mut self, session_token: impl Into<Option<String>>) {
        self.session_token = session_token.into();
    }
    pub fn set_service_endpoint(&mut self, service_endpoint: impl Into<Option<String>>) {
        self.service_endpoint = service_endpoint.into();
    }

    fn validate(&self) -> CouchbaseResult<()> {
        if self.name.is_empty() {
            return Err(CouchbaseError::InvalidArgument {
                ctx: ErrorContext::from(("name", "Name must be provided")),
            });
        }
        if self.dataverse_name.is_empty() {
            return Err(CouchbaseError::InvalidArgument {
                ctx: ErrorContext::from(("dataverse_name", "Dataverse name must be provided")),
            });
        }
        if self.access_key_id.is_empty() {
            return Err(CouchbaseError::InvalidArgument {
                ctx: ErrorContext::from(("access_key_id", "Access key ID must be provided")),
            });
        }
        if self.secret_access_key.is_empty() {
            return Err(CouchbaseError::InvalidArgument {
                ctx: ErrorContext::from((
                    "secret_access_key",
                    "Secret access key must be provided",
                )),
            });
        }
        if self.region.is_empty() {
            return Err(CouchbaseError::InvalidArgument {
                ctx: ErrorContext::from(("region", "Region must be provided")),
            });
        }
        Ok(())
    }
    fn encode(&self) -> CouchbaseResult<Vec<(&str, String)>> {
        let mut form = vec![
            ("accessKeyId", self.access_key_id.clone()),
            ("secretAccessKey", self.secret_access_key.clone()),
            ("region", self.region.clone()),
            ("type", String::from("s3")),
        ];

        if self.dataverse_name.contains('/') {
            form.push(("name", self.name.clone()));
            form.push(("dataverse", self.dataverse_name.clone()));
        }
        if let Some(se) = &self.service_endpoint {
            form.push(("serviceEndpoint", se.clone()))
        }
        if let Some(st) = &self.session_token {
            form.push(("sessionToken", st.clone()))
        }

        Ok(form)
    }
}

#[derive(Debug, Clone)]
#[cfg(feature = "volatile")]
pub struct AzureBlobExternalAnalyticsLinkBuilder {
    name: String,
    dataverse_name: String,
    connection_string: Option<String>,
    account_name: Option<String>,
    account_key: Option<String>,
    shared_access_signature: Option<String>,
    blob_endpoint: Option<String>,
    endpoint_suffix: Option<String>,
}

#[cfg(feature = "volatile")]
impl AzureBlobExternalAnalyticsLinkBuilder {
    pub fn new(name: impl Into<String>, dataverse_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            dataverse_name: dataverse_name.into(),
            connection_string: None,
            account_name: None,
            account_key: None,
            shared_access_signature: None,
            blob_endpoint: None,
            endpoint_suffix: None,
        }
    }

    pub fn connection_string(mut self, connection_string: impl Into<String>) -> Self {
        self.connection_string = Some(connection_string);
        self
    }
    pub fn account_name(mut self, account_name: impl Into<String>) -> Self {
        self.account_name = Some(account_name);
        self
    }
    pub fn account_key(mut self, account_key: impl Into<String>) -> Self {
        self.account_key = Some(account_key);
        self
    }
    pub fn shared_access_signature(mut self, signature: impl Into<String>) -> Self {
        self.shared_access_signature = Some(signature);
        self
    }
    pub fn blob_endpoint(mut self, blob_endpoint: impl Into<String>) -> Self {
        self.blob_endpoint = Some(blob_endpoint);
        self
    }
    pub fn endpoint_suffix(mut self, endpoint_suffix: impl Into<String>) -> Self {
        self.endpoint_suffix = Some(endpoint_suffix);
        self
    }
    pub fn build(self) -> AzureBlobExternalAnalyticsLink {
        AzureBlobExternalAnalyticsLink {
            name: self.name,
            dataverse_name: self.dataverse_name,
            connection_string: self.connection_string,
            account_name: self.account_name,
            account_key: self.account_key,
            shared_access_signature: self.shared_access_signature,
            blob_endpoint: self.blob_endpoint,
            endpoint_suffix: self.endpoint_suffix,
        }
    }
}

#[derive(Debug, Clone)]
#[cfg(feature = "volatile")]
pub struct AzureBlobExternalAnalyticsLink {
    name: String,
    dataverse_name: String,
    connection_string: Option<String>,
    account_name: Option<String>,
    account_key: Option<String>,
    shared_access_signature: Option<String>,
    blob_endpoint: Option<String>,
    endpoint_suffix: Option<String>,
}

#[cfg(feature = "volatile")]
impl From<Value> for AzureBlobExternalAnalyticsLink {
    fn from(v: Value) -> Self {
        let account_name = if let Some(u) = v.get("accountName") {
            Some(u.to_string())
        } else {
            None
        };
        let blob_endpoint = if let Some(u) = v.get("blobEndpoint") {
            Some(u.to_string())
        } else {
            None
        };
        let endpoint_suffix = if let Some(u) = v.get("endpointSuffix") {
            Some(u.to_string())
        } else {
            None
        };

        AzureBlobExternalAnalyticsLink {
            name: v["name"].to_string(),
            dataverse_name: v["scope"].to_string(),
            connection_string: None,
            account_name,
            account_key: None,
            shared_access_signature: None,
            blob_endpoint,
            endpoint_suffix,
        }
    }
}

#[cfg(feature = "volatile")]
impl AzureBlobExternalAnalyticsLink {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn dataverse_name(&self) -> &str {
        &self.dataverse_name
    }
    pub fn account_name(&self) -> Option<&String> {
        self.account_name.as_ref()
    }
    pub fn blob_endpoint(&self) -> Option<&String> {
        self.blob_endpoint.as_ref()
    }
    pub fn endpoint_suffix(&self) -> Option<&String> {
        self.endpoint_suffix.as_ref()
    }
    pub fn set_connection_string(&mut self, connection_string: impl Into<Option<String>>) {
        self.connection_string = connection_string.into();
    }
    pub fn set_account_name(&mut self, name: impl Into<Option<String>>) {
        self.account_name = name.into();
    }
    pub fn set_account_key(&mut self, account_key: impl Into<Option<String>>) {
        self.account_key = account_key.into();
    }
    pub fn set_shared_access_signature(&mut self, signature: impl Into<Option<String>>) {
        self.shared_access_signature = signature.into();
    }
    pub fn set_blob_endpoint(&mut self, blob_endpoint: impl Into<Option<String>>) {
        self.blob_endpoint = blob_endpoint.into();
    }
    pub fn set_endpoint_suffix(&mut self, endpoint_suffix: impl Into<Option<String>>) {
        self.endpoint_suffix = endpoint_suffix.into();
    }

    fn validate(&self) -> CouchbaseResult<()> {
        if self.name.is_empty() {
            return Err(CouchbaseError::InvalidArgument {
                ctx: ErrorContext::from(("name", "Field cannot be empty")),
            });
        }
        if self.dataverse_name.is_empty() {
            return Err(CouchbaseError::InvalidArgument {
                ctx: ErrorContext::from(("name", "Dataverse name cannot be empty")),
            });
        }
        let account_name_key_ok = self.account_name.is_some() && self.account_key.is_some();
        let account_name_sig_ok =
            self.account_name.is_some() && self.shared_access_signature.is_some();
        if self.connection_string.is_none() && !account_name_key_ok && !account_name_sig_ok {
            return Err(CouchbaseError::InvalidArgument {
                ctx: ErrorContext::from(("credentials", "Must provide one of account name and account key or account name and shared access signature or connection string")),
            });
        }

        Ok(())
    }
    fn encode(&self) -> CouchbaseResult<Vec<(&str, String)>> {
        let mut form = vec![("type", String::from("azureblob"))];

        if self.dataverse_name.contains('/') {
            form.push(("name", self.name.clone()));
            form.push(("dataverse", self.dataverse_name.clone()));
        }
        if let Some(val) = &self.connection_string {
            form.push(("connectionString", val.clone()))
        }
        if let Some(val) = &self.account_name {
            form.push(("accountName", val.clone()))
        }
        if let Some(val) = &self.account_key {
            form.push(("accountKey", val.clone()))
        }
        if let Some(val) = &self.shared_access_signature {
            form.push(("sharedAccessSignature", val.clone()))
        }
        if let Some(val) = &self.blob_endpoint {
            form.push(("blobEndpoint", val.clone()))
        }
        if let Some(val) = &self.endpoint_suffix {
            form.push(("endpointSuffix", val.clone()))
        }

        Ok(form)
    }
}

pub struct AnalyticsIndexManager {
    core: Arc<Core>,
}

impl AnalyticsIndexManager {
    pub(crate) fn new(core: Arc<Core>) -> Self {
        Self { core }
    }

    pub async fn create_dataverse(
        &self,
        name: impl Into<String>,
        opts: impl Into<Option<CreateAnalyticsDataverseOptions>>,
    ) -> CouchbaseResult<()> {
        let opts = unwrap_or_default!(opts.into());
        let ignore_if_exists = if opts.ignore_if_exists.unwrap_or(false) {
            String::from("IF NOT EXISTS")
        } else {
            String::from("")
        };

        let statement = format!(
            "CREATE DATAVERSE {} {}",
            self.uncompound_name(name.into()),
            ignore_if_exists
        );

        let mut req_opts = AnalyticsOptions::default();
        if let Some(d) = opts.timeout {
            req_opts = req_opts.timeout(d);
        }

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Analytics(AnalyticsRequest {
            statement,
            options: req_opts,
            sender,
            scope: None,
        }));
        receiver.await.unwrap().map(|_| ())
    }

    pub async fn drop_dataverse(
        &self,
        name: impl Into<String>,
        opts: impl Into<Option<DropAnalyticsDataverseOptions>>,
    ) -> CouchbaseResult<()> {
        let opts = unwrap_or_default!(opts.into());
        let ignore_if_not_exists = if opts.ignore_if_not_exists.unwrap_or(false) {
            String::from("IF EXISTS")
        } else {
            String::from("")
        };

        let statement = format!(
            "DROP DATAVERSE {} {}",
            self.uncompound_name(name.into()),
            ignore_if_not_exists
        );

        let mut req_opts = AnalyticsOptions::default();
        if let Some(d) = opts.timeout {
            req_opts = req_opts.timeout(d);
        }

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Analytics(AnalyticsRequest {
            statement,
            options: req_opts,
            sender,
            scope: None,
        }));
        receiver.await.unwrap().map(|_| ())
    }

    pub async fn create_dataset(
        &self,
        dataset_name: impl Into<String>,
        bucket_name: impl Into<String>,
        opts: impl Into<Option<CreateAnalyticsDatasetOptions>>,
    ) -> CouchbaseResult<()> {
        let opts = unwrap_or_default!(opts.into());
        let ignore_if_exists = if opts.ignore_if_exists.unwrap_or(false) {
            String::from("IF NOT EXISTS")
        } else {
            String::from("")
        };

        let clause = match opts.condition {
            Some(c) => c,
            None => String::from(""),
        };

        let dataset = match opts.dataverse_name {
            Some(d) => format!("{}.`{}`", self.uncompound_name(d), dataset_name.into()),
            None => format!("`{}`", dataset_name.into()),
        };

        let statement = format!(
            "CREATE DATASET {} {} ON `{}` {}",
            ignore_if_exists,
            dataset,
            bucket_name.into(),
            clause
        );

        let mut req_opts = AnalyticsOptions::default();
        if let Some(d) = opts.timeout {
            req_opts = req_opts.timeout(d);
        }

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Analytics(AnalyticsRequest {
            statement,
            options: req_opts,
            sender,
            scope: None,
        }));
        receiver.await.unwrap().map(|_| ())
    }

    pub async fn drop_dataset(
        &self,
        dataset_name: impl Into<String>,
        opts: impl Into<Option<DropAnalyticsDatasetOptions>>,
    ) -> CouchbaseResult<()> {
        let opts = unwrap_or_default!(opts.into());
        let ignore_if_not_exists = if opts.ignore_if_not_exists.unwrap_or(false) {
            String::from("IF EXISTS")
        } else {
            String::from("")
        };

        let dataset = match opts.dataverse_name {
            Some(d) => format!("{}.`{}`", self.uncompound_name(d), dataset_name.into()),
            None => format!("`{}`", dataset_name.into()),
        };

        let statement = format!("DROP DATASET {} {}", dataset, ignore_if_not_exists);

        let mut req_opts = AnalyticsOptions::default();
        if let Some(d) = opts.timeout {
            req_opts = req_opts.timeout(d);
        }

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Analytics(AnalyticsRequest {
            statement,
            options: req_opts,
            sender,
            scope: None,
        }));
        receiver.await.unwrap().map(|_| ())
    }

    pub async fn get_all_datasets(
        &self,
        opts: impl Into<Option<GetAllAnalyticsDatasetsOptions>>,
    ) -> CouchbaseResult<impl IntoIterator<Item = AnalyticsDataset>> {
        let opts = unwrap_or_default!(opts.into());
        let statement =
            "SELECT d.* FROM Metadata.`Dataset` d WHERE d.DataverseName <> \"Metadata\"";

        let mut req_opts = AnalyticsOptions::default();
        if let Some(d) = opts.timeout {
            req_opts = req_opts.timeout(d);
        }

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Analytics(AnalyticsRequest {
            statement: statement.into(),
            options: req_opts,
            sender,
            scope: None,
        }));

        let mut result = receiver.await.unwrap()?;

        let mut datasets = vec![];
        let mut rows = result.rows::<AnalyticsDataset>();
        while let Some(index) = rows.next().await {
            datasets.push(index?);
        }

        Ok(datasets)
    }

    pub async fn create_index(
        &self,
        index_name: impl Into<String>,
        dataset_name: impl Into<String>,
        fields: HashMap<String, String>,
        opts: impl Into<Option<CreateAnalyticsIndexOptions>>,
    ) -> CouchbaseResult<()> {
        let opts = unwrap_or_default!(opts.into());
        let ignore_if_exists = if opts.ignore_if_exists.unwrap_or(false) {
            String::from("IF NOT EXISTS")
        } else {
            String::from("")
        };

        let dataset = match opts.dataverse_name {
            Some(d) => format!("{}.`{}`", self.uncompound_name(d), dataset_name.into()),
            None => format!("`{}`", dataset_name.into()),
        };

        let mut fields_list = vec![];
        for item in fields {
            fields_list.push(format!("{}:{}", item.0, item.1));
        }

        let statement = format!(
            "CREATE INDEX `{}` {} ON {} ({})",
            index_name.into(),
            ignore_if_exists,
            dataset,
            fields_list.join(",")
        );

        let mut req_opts = AnalyticsOptions::default();
        if let Some(d) = opts.timeout {
            req_opts = req_opts.timeout(d);
        }

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Analytics(AnalyticsRequest {
            statement,
            options: req_opts,
            sender,
            scope: None,
        }));
        receiver.await.unwrap().map(|_| ())
    }

    pub async fn drop_index(
        &self,
        index_name: impl Into<String>,
        dataset_name: impl Into<String>,
        opts: impl Into<Option<DropAnalyticsIndexOptions>>,
    ) -> CouchbaseResult<()> {
        let opts = unwrap_or_default!(opts.into());
        let ignore_if_not_exists = if opts.ignore_if_not_exists.unwrap_or(false) {
            String::from("IF EXISTS")
        } else {
            String::from("")
        };

        let dataset = match opts.dataverse_name {
            Some(d) => format!("{}.`{}`", self.uncompound_name(d), dataset_name.into()),
            None => format!("`{}`", dataset_name.into()),
        };

        let statement = format!(
            "DROP INDEX `{}` {} {}",
            index_name.into(),
            dataset,
            ignore_if_not_exists,
        );

        let mut req_opts = AnalyticsOptions::default();
        if let Some(d) = opts.timeout {
            req_opts = req_opts.timeout(d);
        }

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Analytics(AnalyticsRequest {
            statement,
            options: req_opts,
            sender,
            scope: None,
        }));
        receiver.await.unwrap().map(|_| ())
    }

    pub async fn get_all_indexes(
        &self,
        opts: impl Into<Option<GetAllAnalyticsDatasetsOptions>>,
    ) -> CouchbaseResult<impl IntoIterator<Item = AnalyticsIndex>> {
        let opts = unwrap_or_default!(opts.into());
        let statement = "SELECT d.* FROM Metadata.`Index` d WHERE d.DataverseName <> \"Metadata\"";

        let mut req_opts = AnalyticsOptions::default();
        if let Some(d) = opts.timeout {
            req_opts = req_opts.timeout(d);
        }

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Analytics(AnalyticsRequest {
            statement: statement.into(),
            options: req_opts,
            sender,
            scope: None,
        }));

        let mut result = receiver.await.unwrap()?;

        let mut indexes = vec![];
        let mut rows = result.rows::<AnalyticsIndex>();
        while let Some(index) = rows.next().await {
            indexes.push(index?);
        }

        Ok(indexes)
    }

    pub async fn connect_link(
        &self,
        opts: impl Into<Option<ConnectAnalyticsLinkOptions>>,
    ) -> CouchbaseResult<()> {
        let opts = unwrap_or_default!(opts.into());
        let mut link_name = match opts.link_name {
            Some(l) => l,
            None => String::from("Local"),
        };
        link_name = match opts.dataverse_name {
            Some(d) => format!("{}.`{}`", self.uncompound_name(d), link_name),
            None => link_name,
        };

        let statement = format!("CONNECT LINK {}", link_name,);

        let mut req_opts = AnalyticsOptions::default();
        if let Some(d) = opts.timeout {
            req_opts = req_opts.timeout(d);
        }

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Analytics(AnalyticsRequest {
            statement,
            options: req_opts,
            sender,
            scope: None,
        }));
        receiver.await.unwrap().map(|_| ())
    }

    pub async fn disconnect_link(
        &self,
        opts: impl Into<Option<DisconnectAnalyticsLinkOptions>>,
    ) -> CouchbaseResult<()> {
        let opts = unwrap_or_default!(opts.into());
        let mut link_name = match opts.link_name {
            Some(l) => l,
            None => String::from("Local"),
        };
        link_name = match opts.dataverse_name {
            Some(d) => format!("{}.`{}`", self.uncompound_name(d), link_name),
            None => link_name,
        };

        let statement = format!("DISCONNECT LINK {}", link_name,);

        let mut req_opts = AnalyticsOptions::default();
        if let Some(d) = opts.timeout {
            req_opts = req_opts.timeout(d);
        }

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Analytics(AnalyticsRequest {
            statement,
            options: req_opts,
            sender,
            scope: None,
        }));
        receiver.await.unwrap().map(|_| ())
    }

    pub async fn get_pending_mutations(
        &self,
        opts: impl Into<Option<GetAllAnalyticsDatasetsOptions>>,
    ) -> CouchbaseResult<HashMap<String, HashMap<String, i64>>> {
        let opts = unwrap_or_default!(opts.into());
        let (sender, receiver) = oneshot::channel();
        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: String::from("/analytics/node/agg/stats/remaining"),
                method: String::from("get"),
                payload: None,
                content_type: None,
                timeout: opts.timeout,
                service_type: Some(ServiceType::Analytics),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        let content: HashMap<String, HashMap<String, i64>> = match result.http_status() {
            200 => serde_json::from_slice(result.payload_or_error()?)
                .map_err(CouchbaseError::decoding_failure_from_serde),
            _ => Err(CouchbaseError::GenericHTTP {
                ctx: Default::default(),
                status: result.http_status(),
                message: String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
            }),
        }?;

        Ok(content)
    }

    pub async fn create_link(
        &self,
        link: AnalyticsLink,
        opts: impl Into<Option<CreateAnalyticsLinkOptions>>,
    ) -> CouchbaseResult<()> {
        let opts = unwrap_or_default!(opts.into());
        let (endpoint, form) = match link {
            AnalyticsLink::CouchbaseRemote(l) => {
                l.validate()?;
                let form = serde_urlencoded::to_string(l.encode()?).map_err(|e| {
                    CouchbaseError::EncodingFailure {
                        source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
                        ctx: ErrorContext::default(),
                    }
                })?;
                (self.endpoint_for_link(l.dataverse_name, l.name), form)
            }
            AnalyticsLink::S3External(l) => {
                l.validate()?;
                let form = serde_urlencoded::to_string(l.encode()?).map_err(|e| {
                    CouchbaseError::EncodingFailure {
                        source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
                        ctx: ErrorContext::default(),
                    }
                })?;
                (self.endpoint_for_link(l.dataverse_name, l.name), form)
            }
            #[cfg(feature = "volatile")]
            AnalyticsLink::AzureBlobExternal(l) => {
                l.validate()?;
                let form = serde_urlencoded::to_string(l.encode()?).map_err(|e| {
                    CouchbaseError::EncodingFailure {
                        source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
                        ctx: ErrorContext::default(),
                    }
                })?;
                (self.endpoint_for_link(l.dataverse_name, l.name), form)
            }
        };

        let (sender, receiver) = oneshot::channel();
        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: endpoint,
                method: String::from("post"),
                payload: Some(form),
                content_type: Some(String::from("application/x-www-form-urlencoded")),
                timeout: opts.timeout,
                service_type: Some(ServiceType::Analytics),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            200 => Ok(()),
            _ => Err(CouchbaseError::GenericHTTP {
                ctx: Default::default(),
                status: result.http_status(),
                message: String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
            }),
        }
    }

    pub async fn replace_link(
        &self,
        link: AnalyticsLink,
        opts: impl Into<Option<ReplaceAnalyticsLinkOptions>>,
    ) -> CouchbaseResult<()> {
        let opts = unwrap_or_default!(opts.into());
        let (endpoint, form) = match link {
            AnalyticsLink::CouchbaseRemote(l) => {
                l.validate()?;
                let form = serde_urlencoded::to_string(l.encode()?)?;
                (self.endpoint_for_link(l.dataverse_name, l.name), form)
            }
            AnalyticsLink::S3External(l) => {
                l.validate()?;
                let form = serde_urlencoded::to_string(l.encode()?)?;
                (self.endpoint_for_link(l.dataverse_name, l.name), form)
            }
            #[cfg(feature = "volatile")]
            AnalyticsLink::AzureBlobExternal(l) => {
                l.validate()?;
                let form = serde_urlencoded::to_string(l.encode()?)?;
                (self.endpoint_for_link(l.dataverse_name, l.name), form)
            }
        };

        let (sender, receiver) = oneshot::channel();
        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: endpoint,
                method: String::from("put"),
                payload: Some(form),
                content_type: Some(String::from("application/x-www-form-urlencoded")),
                timeout: opts.timeout,
                service_type: Some(ServiceType::Analytics),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            200 => Ok(()),
            _ => Err(CouchbaseError::GenericHTTP {
                ctx: Default::default(),
                status: result.http_status(),
                message: String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
            }),
        }
    }

    pub async fn drop_link(
        &self,
        link_name: impl Into<String>,
        dataverse_name: impl Into<String>,
        opts: impl Into<Option<DropAnalyticsLinkOptions>>,
    ) -> CouchbaseResult<()> {
        let opts = unwrap_or_default!(opts.into());
        let dataverse_name = dataverse_name.into();
        let link_name = link_name.into();
        let payload = if dataverse_name.contains('/') {
            Some(serde_urlencoded::to_string(vec![
                ("name", link_name.clone()),
                ("dataverse", dataverse_name.clone()),
            ])?)
        } else {
            None
        };
        let (sender, receiver) = oneshot::channel();
        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: self.endpoint_for_link(dataverse_name, link_name),
                method: String::from("delete"),
                payload,
                content_type: None,
                timeout: opts.timeout,
                service_type: Some(ServiceType::Analytics),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            200 => Ok(()),
            _ => Err(CouchbaseError::GenericHTTP {
                ctx: Default::default(),
                status: result.http_status(),
                message: String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
            }),
        }
    }

    pub async fn get_links(
        &self,
        opts: impl Into<Option<GetAllAnalyticsLinksOptions>>,
    ) -> CouchbaseResult<impl IntoIterator<Item = AnalyticsLink>> {
        let opts = unwrap_or_default!(opts.into());
        if opts.name.is_some() && opts.dataverse.is_none() {
            return Err(CouchbaseError::InvalidArgument {
                ctx: ErrorContext::from(("", "Dataverse must be set if name is set")),
            });
        }

        let mut endpoint = String::from("/analytics/link");
        let mut query_string = vec![];
        if let Some(d) = opts.dataverse {
            if d.contains('/') {
                endpoint = format!(
                    "{}/{}",
                    endpoint,
                    urlencoding::encode(d.as_str()).to_string()
                );
                if let Some(n) = opts.name {
                    endpoint = format!("{}/{}", endpoint, n);
                }
            } else {
                query_string.push(format!("dataverse={}", d));
                if let Some(n) = opts.name {
                    query_string.push(format!("name={}", n));
                }
            }
        }
        if let Some(l) = opts.link_type {
            query_string.push(format!("type={}", l));
        }
        if !query_string.is_empty() {
            endpoint = format!("{}?{}", endpoint, query_string.join("&"));
        }

        let (sender, receiver) = oneshot::channel();
        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: endpoint,
                method: String::from("get"),
                payload: None,
                content_type: None,
                timeout: opts.timeout,
                service_type: Some(ServiceType::Analytics),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;
        let content: Vec<Value> = match result.http_status() {
            200 => serde_json::from_slice(result.payload_or_error()?)
                .map_err(CouchbaseError::decoding_failure_from_serde),
            _ => Err(CouchbaseError::GenericHTTP {
                ctx: Default::default(),
                status: result.http_status(),
                message: String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
            }),
        }?;

        let mut links = vec![];
        for link_value in content {
            let link_type = link_value["type"]
                .as_str()
                .ok_or_else(|| CouchbaseError::Generic {
                    ctx: ErrorContext::from(("type", "Field missing or not string value")),
                })?;
            let link = match link_type {
                "s3" => Some(AnalyticsLink::S3External(S3ExternalAnalyticsLink::from(
                    link_value,
                ))),
                "couchbase" => Some(AnalyticsLink::CouchbaseRemote(
                    CouchbaseRemoteAnalyticsLink::from(link_value),
                )),
                #[cfg(feature = "volatile")]
                "azureblob" => Some(AnalyticsLink::AzureBlobExternal(
                    AzureBlobExternalAnalyticsLink::from(link_value),
                )),
                _ => {
                    // TODO: something better
                    None
                }
            };

            if let Some(l) = link {
                links.push(l);
            }
        }

        Ok(links)
    }

    fn uncompound_name(&self, name: String) -> String {
        let pieces: Vec<String> = name.split('/').map(String::from).collect();
        format!("`{}`", pieces.join("`.`"))
    }

    fn endpoint_for_link(&self, dataverse_name: String, name: String) -> String {
        if dataverse_name.contains('/') {
            format!(
                "/analytics/link/{}/{}",
                urlencoding::encode(dataverse_name.as_str()).to_string(),
                name
            )
        } else {
            String::from("/analytics/link")
        }
    }
}

#[derive(Debug, Default)]
pub struct CreateAnalyticsDataverseOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) ignore_if_exists: Option<bool>,
}

impl CreateAnalyticsDataverseOptions {
    timeout!();

    pub fn ignore_if_exists(mut self, ignore: bool) -> Self {
        self.ignore_if_exists = Some(ignore);
        self
    }
}

#[derive(Debug, Default)]
pub struct DropAnalyticsDataverseOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) ignore_if_not_exists: Option<bool>,
}

impl DropAnalyticsDataverseOptions {
    timeout!();

    pub fn ignore_if_not_exists(mut self, ignore: bool) -> Self {
        self.ignore_if_not_exists = Some(ignore);
        self
    }
}

#[derive(Debug, Default)]
pub struct CreateAnalyticsDatasetOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) ignore_if_exists: Option<bool>,
    pub(crate) condition: Option<String>,
    pub(crate) dataverse_name: Option<String>,
}

impl CreateAnalyticsDatasetOptions {
    timeout!();

    pub fn ignore_if_exists(mut self, ignore: bool) -> Self {
        self.ignore_if_exists = Some(ignore);
        self
    }

    pub fn condition(mut self, condition: impl Into<String>) -> Self {
        self.condition = Some(condition.into());
        self
    }

    pub fn dataverse_name(mut self, dataverse_name: impl Into<String>) -> Self {
        self.dataverse_name = Some(dataverse_name.into());
        self
    }
}

#[derive(Debug, Default)]
pub struct DropAnalyticsDatasetOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) ignore_if_not_exists: Option<bool>,
    pub(crate) dataverse_name: Option<String>,
}

impl DropAnalyticsDatasetOptions {
    timeout!();

    pub fn ignore_if_not_exists(mut self, ignore: bool) -> Self {
        self.ignore_if_not_exists = Some(ignore);
        self
    }

    pub fn dataverse_name(mut self, dataverse_name: impl Into<String>) -> Self {
        self.dataverse_name = Some(dataverse_name.into());
        self
    }
}

#[derive(Debug, Default)]
pub struct GetAllAnalyticsDatasetsOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetAllAnalyticsDatasetsOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct CreateAnalyticsIndexOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) ignore_if_exists: Option<bool>,
    pub(crate) dataverse_name: Option<String>,
}

impl CreateAnalyticsIndexOptions {
    timeout!();

    pub fn ignore_if_exists(mut self, ignore: bool) -> Self {
        self.ignore_if_exists = Some(ignore);
        self
    }

    pub fn dataverse_name(mut self, dataverse_name: impl Into<String>) -> Self {
        self.dataverse_name = Some(dataverse_name.into());
        self
    }
}

#[derive(Debug, Default)]
pub struct DropAnalyticsIndexOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) ignore_if_not_exists: Option<bool>,
    pub(crate) dataverse_name: Option<String>,
}

impl DropAnalyticsIndexOptions {
    timeout!();

    pub fn ignore_if_not_exists(mut self, ignore: bool) -> Self {
        self.ignore_if_not_exists = Some(ignore);
        self
    }

    pub fn dataverse_name(mut self, dataverse_name: impl Into<String>) -> Self {
        self.dataverse_name = Some(dataverse_name.into());
        self
    }
}

#[derive(Debug, Default)]
pub struct GetAllAnalyticsIndexesOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetAllAnalyticsIndexesOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct ConnectAnalyticsLinkOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) dataverse_name: Option<String>,
    pub(crate) link_name: Option<String>,
    pub(crate) force: Option<bool>,
}

impl ConnectAnalyticsLinkOptions {
    timeout!();

    pub fn dataverse_name(mut self, dataverse_name: impl Into<String>) -> Self {
        self.dataverse_name = Some(dataverse_name.into());
        self
    }

    pub fn link_name(mut self, link_name: impl Into<String>) -> Self {
        self.link_name = Some(link_name.into());
        self
    }

    pub fn force(mut self, force: bool) -> Self {
        self.force = Some(force);
        self
    }
}

#[derive(Debug, Default)]
pub struct DisconnectAnalyticsLinkOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) dataverse_name: Option<String>,
    pub(crate) link_name: Option<String>,
}

impl DisconnectAnalyticsLinkOptions {
    timeout!();

    pub fn dataverse_name(mut self, dataverse_name: impl Into<String>) -> Self {
        self.dataverse_name = Some(dataverse_name.into());
        self
    }

    pub fn link_name(mut self, link_name: impl Into<String>) -> Self {
        self.link_name = Some(link_name.into());
        self
    }
}

#[derive(Debug, Default)]
pub struct CreateAnalyticsLinkOptions {
    pub(crate) timeout: Option<Duration>,
}

impl CreateAnalyticsLinkOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct ReplaceAnalyticsLinkOptions {
    pub(crate) timeout: Option<Duration>,
}

impl ReplaceAnalyticsLinkOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct DropAnalyticsLinkOptions {
    pub(crate) timeout: Option<Duration>,
}

impl DropAnalyticsLinkOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct GetAllAnalyticsLinksOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) dataverse: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) link_type: Option<AnalyticsLinkType>,
}

impl GetAllAnalyticsLinksOptions {
    timeout!();

    pub fn dataverse(mut self, dataverse: impl Into<String>) -> Self {
        self.dataverse = Some(dataverse.into());
        self
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn link_type(mut self, link_type: AnalyticsLinkType) -> Self {
        self.link_type = Some(link_type);
        self
    }
}
