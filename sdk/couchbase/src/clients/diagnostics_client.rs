use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::error;
use crate::options::diagnostic_options::{DiagnosticsOptions, PingOptions, WaitUntilReadyOptions};
use crate::results::diagnostics::{DiagnosticsResult, PingReport};

#[derive(Clone)]
pub(crate) struct DiagnosticsClient {
    backend: DiagnosticsClientBackend,
}

impl DiagnosticsClient {
    pub fn new(backend: DiagnosticsClientBackend) -> Self {
        Self { backend }
    }

    pub async fn diagnostics(
        &self,
        opts: Option<DiagnosticsOptions>,
    ) -> error::Result<DiagnosticsResult> {
        match &self.backend {
            DiagnosticsClientBackend::CouchbaseDiagnosticsClientBackend(backend) => {
                backend.diagnostics(opts).await
            }
            DiagnosticsClientBackend::Couchbase2DiagnosticsClientBackend(backend) => {
                backend.diagnostics(opts).await
            }
        }
    }

    pub async fn ping(&self, opts: Option<PingOptions>) -> error::Result<PingReport> {
        match &self.backend {
            DiagnosticsClientBackend::CouchbaseDiagnosticsClientBackend(backend) => {
                backend.ping(opts).await
            }
            DiagnosticsClientBackend::Couchbase2DiagnosticsClientBackend(backend) => {
                backend.ping(opts).await
            }
        }
    }

    pub async fn wait_until_ready(&self, opts: Option<WaitUntilReadyOptions>) -> error::Result<()> {
        match &self.backend {
            DiagnosticsClientBackend::CouchbaseDiagnosticsClientBackend(backend) => {
                backend.wait_until_ready(opts).await
            }
            DiagnosticsClientBackend::Couchbase2DiagnosticsClientBackend(backend) => {
                backend.wait_until_ready(opts).await
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum DiagnosticsClientBackend {
    CouchbaseDiagnosticsClientBackend(CouchbaseDiagnosticsClient),
    Couchbase2DiagnosticsClientBackend(Couchbase2DiagnosticsClient),
}

#[derive(Clone)]
pub(crate) struct CouchbaseDiagnosticsClient {
    agent_provider: CouchbaseAgentProvider,
}

impl CouchbaseDiagnosticsClient {
    pub fn new(agent_provider: CouchbaseAgentProvider) -> Self {
        Self { agent_provider }
    }

    async fn diagnostics(
        &self,
        opts: Option<DiagnosticsOptions>,
    ) -> error::Result<DiagnosticsResult> {
        let agent = self.agent_provider.get_agent().await;

        let core_opts = if let Some(opts) = opts {
            couchbase_core::options::diagnostics::DiagnosticsOptions::from(opts)
        } else {
            couchbase_core::options::diagnostics::DiagnosticsOptions::new()
        };

        let report = agent.diagnostics(&core_opts).await?;

        Ok(DiagnosticsResult::from(report))
    }

    async fn ping(&self, opts: Option<PingOptions>) -> error::Result<PingReport> {
        let agent = self.agent_provider.get_agent().await;

        let core_opts = if let Some(opts) = opts {
            couchbase_core::options::ping::PingOptions::from(opts)
        } else {
            couchbase_core::options::ping::PingOptions::new()
        };

        let report = agent.ping(&core_opts).await?;

        Ok(PingReport::from(report))
    }

    async fn wait_until_ready(&self, opts: Option<WaitUntilReadyOptions>) -> error::Result<()> {
        let agent = self.agent_provider.get_agent().await;

        let core_opts = if let Some(opts) = opts {
            couchbase_core::options::waituntilready::WaitUntilReadyOptions::from(opts)
        } else {
            couchbase_core::options::waituntilready::WaitUntilReadyOptions::new()
        };

        Ok(agent.wait_until_ready(&core_opts).await?)
    }
}

#[derive(Clone)]
pub(crate) struct Couchbase2DiagnosticsClient {}

impl Couchbase2DiagnosticsClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    async fn diagnostics(
        &self,
        _opts: Option<DiagnosticsOptions>,
    ) -> error::Result<DiagnosticsResult> {
        unimplemented!()
    }

    async fn ping(&self, _opts: Option<PingOptions>) -> error::Result<PingReport> {
        unimplemented!()
    }

    async fn wait_until_ready(&self, _opts: Option<WaitUntilReadyOptions>) -> error::Result<()> {
        unimplemented!()
    }
}
