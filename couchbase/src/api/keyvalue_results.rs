use crate::{CouchbaseError, CouchbaseResult, ErrorContext, MutationToken, ServiceType};
use chrono::NaiveDateTime;
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

pub struct GetResult {
    pub(crate) content: Vec<u8>,
    pub(crate) cas: u64,
    pub(crate) flags: u32,
    pub(crate) expiry_time: Option<NaiveDateTime>,
}

impl GetResult {
    pub fn new(content: Vec<u8>, cas: u64, flags: u32) -> Self {
        Self {
            content,
            cas,
            flags,
            expiry_time: None,
        }
    }

    pub(crate) fn set_expiry_time(&mut self, expiry: NaiveDateTime) {
        self.expiry_time = Some(expiry);
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn content<'a, T>(&'a self) -> CouchbaseResult<T>
    where
        T: serde::Deserialize<'a>,
    {
        match serde_json::from_slice(self.content.as_slice()) {
            Ok(v) => Ok(v),
            Err(e) => Err(CouchbaseError::DecodingFailure {
                ctx: ErrorContext::default(),
                source: e.into(),
            }),
        }
    }

    // TODO: Pretty unconvinced that this returns the correct type, forcing users to use chrono here.
    pub fn expiry_time(&self) -> Option<&NaiveDateTime> {
        self.expiry_time.as_ref()
    }
}

impl fmt::Debug for GetResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let content = match std::str::from_utf8(&self.content) {
            Ok(c) => c,
            Err(_e) => "<Not Valid/Printable UTF-8>",
        };
        write!(
            f,
            "GetResult {{ cas: 0x{:x}, flags: 0x{:x}, content: {} }}",
            self.cas, self.flags, content
        )
    }
}

pub struct GetReplicaResult {
    content: Vec<u8>,
    cas: u64,
    flags: u32,
    is_replica: bool,
}

impl GetReplicaResult {
    pub fn new(content: Vec<u8>, cas: u64, flags: u32, is_replica: bool) -> Self {
        Self {
            content,
            cas,
            flags,
            is_replica,
        }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn content<'a, T>(&'a self) -> CouchbaseResult<T>
    where
        T: serde::Deserialize<'a>,
    {
        match serde_json::from_slice(self.content.as_slice()) {
            Ok(v) => Ok(v),
            Err(e) => Err(CouchbaseError::DecodingFailure {
                ctx: ErrorContext::default(),
                source: e.into(),
            }),
        }
    }

    pub fn is_replica(&self) -> bool {
        self.is_replica
    }
}

impl fmt::Debug for GetReplicaResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let content = match std::str::from_utf8(&self.content) {
            Ok(c) => c,
            Err(_e) => "<Not Valid/Printable UTF-8>",
        };
        write!(
            f,
            "GetReplicaResult {{ cas: 0x{:x}, flags: 0x{:x}, is_replica: {}, content: {} }}",
            self.cas, self.flags, self.is_replica, content
        )
    }
}

pub struct ExistsResult {
    cas: Option<u64>,
    exists: bool,
}

impl ExistsResult {
    pub fn new(exists: bool, cas: Option<u64>) -> Self {
        Self { exists, cas }
    }

    pub fn exists(&self) -> bool {
        self.exists
    }

    pub fn cas(&self) -> Option<u64> {
        self.cas
    }
}

impl fmt::Debug for ExistsResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ExistsResult {{ exists: {:?}, cas: {:?} }}",
            self.exists,
            self.cas.map(|c| format!("0x{:x}", c))
        )
    }
}

pub struct MutationResult {
    cas: u64,
    mutation_token: Option<MutationToken>,
}

impl MutationResult {
    pub fn new(cas: u64, mutation_token: Option<MutationToken>) -> Self {
        Self {
            cas,
            mutation_token,
        }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn mutation_token(&self) -> Option<&MutationToken> {
        self.mutation_token.as_ref()
    }
}

impl fmt::Debug for MutationResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MutationResult {{ cas: 0x{:x}, mutation_token: {:?} }}",
            self.cas, self.mutation_token
        )
    }
}

pub struct CounterResult {
    cas: u64,
    mutation_token: Option<MutationToken>,
    content: u64,
}

impl CounterResult {
    pub fn new(cas: u64, mutation_token: Option<MutationToken>, content: u64) -> Self {
        Self {
            cas,
            mutation_token,
            content,
        }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn mutation_token(&self) -> Option<&MutationToken> {
        self.mutation_token.as_ref()
    }

    pub fn content(&self) -> u64 {
        self.content
    }
}

impl fmt::Debug for CounterResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CounterResult {{ cas: 0x{:x}, mutation_token: {:?},  content: {:?}}}",
            self.cas, self.mutation_token, self.content
        )
    }
}

#[derive(Debug)]
pub struct PingResult {
    id: String,
    services: HashMap<ServiceType, Vec<EndpointPingReport>>,
}

impl PingResult {
    pub(crate) fn new(id: String, services: HashMap<ServiceType, Vec<EndpointPingReport>>) -> Self {
        Self { id, services }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn endpoints(&self) -> &HashMap<ServiceType, Vec<EndpointPingReport>> {
        &self.services
    }
}

#[derive(Debug)]
pub struct EndpointPingReport {
    local: Option<String>,
    remote: Option<String>,
    status: PingState,
    error: Option<String>,
    latency: Duration,
    scope: Option<String>,
    id: String,
    typ: ServiceType,
}

impl EndpointPingReport {
    pub(crate) fn new(
        local: Option<String>,
        remote: Option<String>,
        status: PingState,
        error: Option<String>,
        latency: Duration,
        scope: Option<String>,
        id: String,
        typ: ServiceType,
    ) -> Self {
        Self {
            local,
            remote,
            status,
            error,
            latency,
            scope,
            id,
            typ,
        }
    }

    pub fn service_type(&self) -> ServiceType {
        self.typ
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn local(&self) -> Option<&String> {
        self.local.as_ref()
    }

    pub fn remote(&self) -> Option<&String> {
        self.remote.as_ref()
    }

    pub fn state(&self) -> PingState {
        self.status
    }

    pub fn error(&self) -> Option<&String> {
        self.error.as_ref()
    }

    pub fn namespace(&self) -> Option<&String> {
        self.scope.as_ref()
    }

    pub fn latency(&self) -> Duration {
        self.latency
    }
}

#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub enum PingState {
    OK,
    Timeout,
    Error,
    Invalid,
}

impl fmt::Display for PingState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
