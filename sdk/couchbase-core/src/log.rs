use log::kv::{ToValue, Value};
use std::fmt::Display;
use uuid::Uuid;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LogContext {
    pub parent_context: Option<Box<LogContext>>,
    pub parent_component_type: String,
    pub parent_component_id: String,
    pub component_id: String,
}

impl Display for LogContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(component_id={} {})",
            self.component_id,
            self.build_parent_context_message()
        )
    }
}

impl ToValue for LogContext {
    fn to_value(&self) -> Value {
        Value::from_display(self)
    }
}

impl LogContext {
    fn build_parent_context_message(&self) -> String {
        let mut base = format!(
            "{}={}",
            self.parent_component_type, self.parent_component_id
        );

        if let Some(parent) = self.parent_context.as_ref() {
            base.push(' ');
            base.push_str(&parent.build_parent_context_message());
        }

        base
    }

    pub fn new_logger_id() -> String {
        let mut id = Uuid::new_v4().to_string();
        id.truncate(6);
        id
    }
}

pub trait LogContextAware {
    fn log_context(&self) -> &LogContext;
}

#[derive(Debug, Clone)]
pub(crate) enum LogContextOrAgentId {
    LogContext(LogContext),
    AgentId(String),
}

impl Display for LogContextOrAgentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogContextOrAgentId::LogContext(lc) => write!(f, "{}", lc),
            LogContextOrAgentId::AgentId(id) => write!(f, "{}", id),
        }
    }
}

impl ToValue for LogContextOrAgentId {
    fn to_value(&self) -> Value {
        Value::from_display(self)
    }
}

impl LogContextOrAgentId {
    pub fn logger_id(&self) -> &str {
        match self {
            LogContextOrAgentId::LogContext(lc) => &lc.component_id,
            LogContextOrAgentId::AgentId(id) => id,
        }
    }
}

impl From<LogContextOrAgentId> for Option<Box<LogContext>> {
    fn from(lc: LogContextOrAgentId) -> Self {
        match lc {
            LogContextOrAgentId::LogContext(lc) => Some(Box::new(lc)),
            LogContextOrAgentId::AgentId(_) => None,
        }
    }
}

impl From<&LogContextOrAgentId> for Option<Box<LogContext>> {
    fn from(lc: &LogContextOrAgentId) -> Self {
        match lc {
            LogContextOrAgentId::LogContext(lc) => Some(Box::new(lc.clone())),
            LogContextOrAgentId::AgentId(_) => None,
        }
    }
}
