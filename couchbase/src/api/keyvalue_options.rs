use std::time::Duration;

#[derive(Debug, Default)]
pub struct GetOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) with_expiry: bool,
}

impl GetOptions {
    timeout!();
    pub fn with_expiry(mut self, with: bool) -> Self {
        self.with_expiry = with;
        self
    }
}

#[derive(Debug, Default)]
pub struct GetAndTouchOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetAndTouchOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct GetAndLockOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetAndLockOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct UpsertOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) expiry: Option<Duration>,
    pub(crate) preserve_expiry: bool,
}

impl UpsertOptions {
    timeout!();
    expiry!();
    preserve_expiry!();
}

#[derive(Debug, Default)]
pub struct InsertOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) expiry: Option<Duration>,
}

impl InsertOptions {
    timeout!();
    expiry!();
}

#[derive(Debug, Default)]
pub struct ReplaceOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) cas: Option<u64>,
    pub(crate) expiry: Option<Duration>,
    pub(crate) preserve_expiry: bool,
}

impl ReplaceOptions {
    timeout!();
    expiry!();
    preserve_expiry!();

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }
}

#[derive(Debug, Default)]
pub struct RemoveOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) cas: Option<u64>,
}

impl RemoveOptions {
    timeout!();

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }
}

#[derive(Debug, Default)]
pub struct ExistsOptions {
    pub(crate) timeout: Option<Duration>,
}

impl ExistsOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct AppendOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) cas: Option<u64>,
}

impl AppendOptions {
    timeout!();

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }
}

#[derive(Debug, Default)]
pub struct PrependOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) cas: Option<u64>,
    pub(crate) expiry: Option<Duration>,
}

impl PrependOptions {
    timeout!();

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }
}

#[derive(Debug, Default)]
pub struct IncrementOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) cas: Option<u64>,
    pub(crate) expiry: Option<Duration>,
    pub(crate) delta: Option<u64>,
}

impl IncrementOptions {
    timeout!();
    expiry!();

    pub fn delta(mut self, delta: u64) -> Self {
        self.delta = Some(delta);
        self
    }

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }
}

#[derive(Debug, Default)]
pub struct DecrementOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) cas: Option<u64>,
    pub(crate) expiry: Option<Duration>,
    pub(crate) delta: Option<u64>,
}

impl DecrementOptions {
    timeout!();
    expiry!();

    pub fn delta(mut self, delta: u64) -> Self {
        self.delta = Some(delta);
        self
    }

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }
}

#[derive(Debug, Default)]
pub(crate) struct CounterOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) cas: Option<u64>,
    pub(crate) expiry: Option<Duration>,
    pub(crate) delta: i64,
}

#[derive(Debug, Default)]
pub struct PingOptions {
    pub(crate) report_id: Option<String>,
}

impl PingOptions {
    pub fn report_id(mut self, report_id: String) -> Self {
        self.report_id = Some(report_id);
        self
    }
}

#[derive(Debug, Default)]
pub struct UnlockOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) cas: Option<u64>,
}

impl UnlockOptions {
    timeout!();

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }
}
