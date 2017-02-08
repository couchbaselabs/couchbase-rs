#[derive(Debug)]
pub enum N1qlResult {
    Row(String),
    Meta(N1qlMeta),
}

#[derive(Debug, Deserialize)]
pub struct N1qlMeta {
    #[serde(rename = "requestID")]
    request_id: String,
    #[serde(rename = "clientContextID")]
    client_context_id: Option<String>,
    #[serde(rename = "status")]
    status: String,
    #[serde(rename = "metrics")]
    metrics: N1qlMetrics,
}

impl N1qlMeta {
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn status(&self) -> &str {
        &self.status
    }

    pub fn metrics(&self) -> &N1qlMetrics {
        &self.metrics
    }
}

#[derive(Debug, Deserialize)]
pub struct N1qlMetrics {
    #[serde(rename = "elapsedTime")]
    elapsed_time: String,
    #[serde(rename = "executionTime")]
    execution_time: String,
    #[serde(rename = "resultCount")]
    result_count: u64,
    #[serde(rename = "resultSize")]
    result_size: u64,
    #[serde(default)]
    #[serde(rename = "errorCount")]
    error_count: u64,
}

impl N1qlMetrics {
    pub fn elapsed_time(&self) -> &str {
        &self.elapsed_time
    }

    pub fn execution_time(&self) -> &str {
        &self.execution_time
    }

    pub fn result_count(&self) -> u64 {
        self.result_count
    }

    pub fn result_size(&self) -> u64 {
        self.result_size
    }
}
