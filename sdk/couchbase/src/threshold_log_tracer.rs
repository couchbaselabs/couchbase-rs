use crate::tracing::{
    SPAN_ATTRIB_LOCAL_ID_KEY, SPAN_ATTRIB_NET_HOST_NAME_KEY, SPAN_ATTRIB_NET_HOST_PORT_KEY,
    SPAN_ATTRIB_NET_PEER_NAME_KEY, SPAN_ATTRIB_NET_PEER_PORT_KEY, SPAN_ATTRIB_OPERATION_ID_KEY,
    SPAN_ATTRIB_OPERATION_KEY, SPAN_ATTRIB_SERVER_DURATION_KEY, SPAN_ATTRIB_SERVICE_KEY,
    SPAN_NAME_DISPATCH_TO_SERVER, SPAN_NAME_REQUEST_ENCODING,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::json;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::fmt::{Debug, Display};
use std::fs::metadata;
use tokio::sync::mpsc::{unbounded_channel, Receiver, UnboundedReceiver, UnboundedSender};
use tokio::time;
use tokio::time::{Duration, Instant};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Record};
use tracing::{Event, Id, Metadata, Span, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::{LookupSpan, SpanRef};
use tracing_subscriber::Layer;

#[derive(Debug, Clone, Default)]
struct SocketAddr {
    ip: Option<String>,
    port: Option<String>,
}

impl Serialize for SocketAddr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let ip = self.ip.as_deref().unwrap_or("");
        let port = self.port.as_deref().unwrap_or("");
        serializer.serialize_str(&format!("{ip}:{port}"))
    }
}

#[derive(Debug, Clone, Serialize)]
struct SpanInfo {
    #[serde(skip_serializing)]
    total_start: Option<Instant>,
    #[serde(skip_serializing)]
    encoding_start: Option<Instant>,
    #[serde(skip_serializing)]
    last_dispatch_start: Option<Instant>,
    #[serde(skip_serializing)]
    service_name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    total_duration_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    encode_duration_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_dispatch_duration_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_dispatch_duration_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_server_duration_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_server_duration_us: Option<u64>,
    operation_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_local_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    operation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_local_socket: Option<SocketAddr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_remote_socket: Option<SocketAddr>,
}

impl Eq for SpanInfo {}

impl PartialEq<Self> for SpanInfo {
    fn eq(&self, other: &Self) -> bool {
        self.total_duration_us == other.total_duration_us
    }
}

impl PartialOrd<Self> for SpanInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SpanInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        self.total_duration_us.cmp(&other.total_duration_us)
    }
}

impl Visit for SpanInfo {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == SPAN_ATTRIB_SERVER_DURATION_KEY {
            self.last_server_duration_us = Some(value);
            self.total_server_duration_us =
                Some(self.total_server_duration_us.unwrap_or_default() + value);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            SPAN_ATTRIB_OPERATION_KEY => self.operation_name = value.to_owned(),
            SPAN_ATTRIB_LOCAL_ID_KEY => self.last_local_id = Some(value.to_owned()),
            SPAN_ATTRIB_OPERATION_ID_KEY => self.operation_id = Some(value.to_owned()),
            SPAN_ATTRIB_SERVICE_KEY => self.service_name = Some(value.to_owned()),
            SPAN_ATTRIB_NET_HOST_NAME_KEY => {
                let socket = self.last_local_socket.get_or_insert(SocketAddr::default());
                socket.ip = Some(value.to_owned());
            }
            SPAN_ATTRIB_NET_HOST_PORT_KEY => {
                let socket = self.last_local_socket.get_or_insert(SocketAddr::default());
                socket.port = Some(value.to_owned());
            }
            SPAN_ATTRIB_NET_PEER_NAME_KEY => {
                let socket = self.last_remote_socket.get_or_insert(SocketAddr::default());
                socket.ip = Some(value.to_owned());
            }
            SPAN_ATTRIB_NET_PEER_PORT_KEY => {
                let socket = self.last_remote_socket.get_or_insert(SocketAddr::default());
                socket.port = Some(value.to_owned());
            }
            _ => {}
        };
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn Debug) {
        // do nothing
    }
}

impl SpanInfo {
    pub fn new(operation_name: String) -> Self {
        Self {
            total_start: None,
            encoding_start: None,
            last_dispatch_start: None,
            service_name: None,
            total_duration_us: None,
            encode_duration_us: None,
            last_dispatch_duration_us: None,
            total_dispatch_duration_us: None,
            last_server_duration_us: None,
            total_server_duration_us: None,
            operation_name,
            last_local_id: None,
            operation_id: None,
            last_local_socket: None,
            last_remote_socket: None,
        }
    }

    fn is_over_threshold(&self, thresholds: &LoggingThresholds) -> bool {
        match self.service_name.as_deref() {
            Some("kv") => self
                .total_duration_us
                .is_some_and(|d| d > thresholds.kv_threshold_us),
            Some("query") => self
                .total_duration_us
                .is_some_and(|d| d > thresholds.query_threshold_us),
            Some("search") => self
                .total_duration_us
                .is_some_and(|d| d > thresholds.search_threshold_us),
            Some("analytics") => self
                .total_duration_us
                .is_some_and(|d| d > thresholds.analytics_threshold_us),
            Some("management") => self
                .total_duration_us
                .is_some_and(|d| d > thresholds.management_threshold_us),
            _ => false,
        }
    }
}

pub struct ThresholdLoggingLayer {
    thresholds: LoggingThresholds,
    sample_size: usize,

    sender: UnboundedSender<SpanInfo>,
}

impl ThresholdLoggingLayer {
    pub fn new(options: Option<ThresholdLoggingOptions>) -> Self {
        let options = options.unwrap_or_default();

        let thresholds = LoggingThresholds {
            kv_threshold_us: options
                .kv_threshold
                .unwrap_or(Duration::from_millis(500))
                .as_micros() as u64,
            query_threshold_us: options
                .query_threshold
                .unwrap_or(Duration::from_secs(1))
                .as_micros() as u64,
            search_threshold_us: options
                .search_threshold
                .unwrap_or(Duration::from_secs(1))
                .as_micros() as u64,
            analytics_threshold_us: options
                .analytics_threshold
                .unwrap_or(Duration::from_secs(1))
                .as_micros() as u64,
            management_threshold_us: options
                .management_threshold
                .unwrap_or(Duration::from_secs(1))
                .as_micros() as u64,
        };

        let (sender, receiver) = unbounded_channel();

        let mut layer = Self {
            thresholds,
            sample_size: options.sample_size.unwrap_or(10),
            sender,
        };

        tokio::spawn(Self::logger_task(
            receiver,
            options.emit_interval.unwrap_or(Duration::from_secs(10)),
            layer.sample_size,
        ));

        layer
    }

    async fn logger_task(
        mut receiver: UnboundedReceiver<SpanInfo>,
        emit_interval: Duration,
        sample_size: usize,
    ) {
        let start = Instant::now() + emit_interval;
        let mut interval = time::interval_at(start, emit_interval);
        let mut groups: HashMap<String, BinaryHeap<SpanInfo>> = HashMap::with_capacity(5);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let log_data = Self::collect_log_data(&mut groups, sample_size);
                    Self::process_and_emit_logs(log_data);
                    groups.clear();
                },
                event = receiver.recv() => {
                    match event {
                        Some(span_info) => {
                            if let Some(service_name) = &span_info.service_name {
                                groups
                                    .entry(service_name.to_string())
                                    .or_default()
                                    .push(span_info);
                                }
                            },
                        None => break,
                    }
                }
            }
        }
    }

    fn collect_log_data(
        groups: &mut HashMap<String, BinaryHeap<SpanInfo>>,
        sample_size: usize,
    ) -> HashMap<String, (usize, Vec<SpanInfo>)> {
        let mut result = HashMap::new();

        for (service_type, heap) in groups.iter_mut() {
            let total_count = heap.len();
            let mut top_requests = Vec::with_capacity(sample_size);

            for _ in 0..sample_size {
                if let Some(span_info) = heap.pop() {
                    top_requests.push(span_info);
                }
            }

            result.insert(service_type.clone(), (total_count, top_requests));
        }
        result
    }

    fn process_and_emit_logs(log_data: HashMap<String, (usize, Vec<SpanInfo>)>) {
        let log_output: HashMap<_, _> = log_data
            .into_iter()
            .map(|(service_type, (total_count, top_requests))| {
                (
                    service_type,
                    json!({
                        "total_count": total_count,
                        "top_requests": top_requests,
                    }),
                )
            })
            .collect();

        if !log_output.is_empty() {
            match serde_json::to_string(&log_output) {
                Ok(log_output_str) => {
                    tracing::warn!("Operations over threshold: {}", log_output_str)
                }
                Err(_) => tracing::error!("Failed to serialize threshold log output"),
            }
        }
    }

    fn span_is_couchbase(target: &str) -> bool {
        ["couchbase::", "couchbase_core::"]
            .iter()
            .any(|prefix| target.starts_with(prefix))
    }

    fn root_couchbase_span<'a, S>(id: &Id, ctx: &'a Context<'_, S>) -> Option<SpanRef<'a, S>>
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        if let Some(scope) = ctx.span_scope(id) {
            // Skip the current span
            let iter = scope.skip(1);
            // Iterate up the tree to find the root couchbase span, if any
            for parent_span in iter {
                if !Self::span_is_couchbase(parent_span.metadata().target()) {
                    return None;
                }
                if parent_span.extensions().get::<SpanInfo>().is_some() {
                    return Some(parent_span);
                }
            }
        }
        None
    }
}

impl<S> Layer<S> for ThresholdLoggingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let now = Instant::now();
        if let Some(span) = ctx.span(id) {
            if !ThresholdLoggingLayer::span_is_couchbase(span.metadata().target()) {
                return;
            }

            match Self::root_couchbase_span(id, &ctx) {
                Some(root) => {
                    // Span is inner e.g. encoding or dispatch, so fetch SpanInfo from root
                    if let Some(mut span_info) = root.extensions_mut().get_mut::<SpanInfo>() {
                        attrs.record(span_info);
                        match span.name() {
                            SPAN_NAME_REQUEST_ENCODING => {
                                span_info.encoding_start = Some(now);
                            }
                            SPAN_NAME_DISPATCH_TO_SERVER => {
                                span_info.last_dispatch_start = Some(now);
                            }
                            _ => {}
                        }
                    }
                }
                None => {
                    // Span is root, so create a new SpanInfo and attach it
                    let mut info = SpanInfo::new(span.name().to_string());
                    attrs.record(&mut info);
                    if info.total_start.is_none() {
                        info.total_start = Some(now);
                    }

                    let mut extensions = span.extensions_mut();
                    extensions.insert::<SpanInfo>(info);
                }
            }
        }
    }

    fn on_record(&self, span: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        if let Some(span_ref) = ctx.span(span) {
            if !Self::span_is_couchbase(span_ref.metadata().target()) {
                return;
            }

            match Self::root_couchbase_span(span, &ctx) {
                Some(root) => {
                    if let Some(outer_info) = root.extensions_mut().get_mut::<SpanInfo>() {
                        values.record(outer_info);
                    }
                }
                None => {
                    tracing::warn!("Warning: on_record on couchbase span without a root");
                }
            }
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let now = Instant::now();
        if let Some(span) = ctx.span(&id) {
            if !Self::span_is_couchbase(span.metadata().target()) {
                return;
            }

            match Self::root_couchbase_span(&id, &ctx) {
                Some(root) => {
                    // Span is inner e.g. encoding or dispatch, so get root SpanInfo and add durations
                    if let Some(outer_info) = root.extensions_mut().get_mut::<SpanInfo>() {
                        match span.name() {
                            SPAN_NAME_REQUEST_ENCODING => {
                                outer_info.encode_duration_us = outer_info
                                    .encoding_start
                                    .map(|start| (now - start).as_micros() as u64);
                            }
                            SPAN_NAME_DISPATCH_TO_SERVER => {
                                if let Some(last_dispatch_start) = outer_info.last_dispatch_start {
                                    let dispatch_duration =
                                        (now - last_dispatch_start).as_micros() as u64;
                                    outer_info.last_dispatch_duration_us = Some(dispatch_duration);
                                    outer_info.total_dispatch_duration_us = Some(
                                        outer_info.total_dispatch_duration_us.unwrap_or_default()
                                            + dispatch_duration,
                                    );
                                }
                            }
                            _ => {}
                        }
                    }
                }
                None => {
                    // Span is root, so finalize and emit
                    if let Some(mut outer_info) = span.extensions_mut().remove::<SpanInfo>() {
                        outer_info.total_duration_us = outer_info
                            .total_start
                            .map(|start| (now - start).as_micros() as u64);
                        if outer_info.is_over_threshold(&self.thresholds) {
                            let _ = self.sender.send(outer_info);
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct LoggingThresholds {
    kv_threshold_us: u64,
    query_threshold_us: u64,
    search_threshold_us: u64,
    analytics_threshold_us: u64,
    management_threshold_us: u64,
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct ThresholdLoggingOptions {
    pub(crate) emit_interval: Option<Duration>,
    pub(crate) kv_threshold: Option<Duration>,
    pub(crate) query_threshold: Option<Duration>,
    pub(crate) search_threshold: Option<Duration>,
    pub(crate) analytics_threshold: Option<Duration>,
    pub(crate) management_threshold: Option<Duration>,
    pub(crate) sample_size: Option<usize>,
}

impl ThresholdLoggingOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn emit_interval(mut self, emit_interval: Duration) -> Self {
        self.emit_interval = Some(emit_interval);
        self
    }

    pub fn kv_threshold(mut self, kv_threshold: Duration) -> Self {
        self.kv_threshold = Some(kv_threshold);
        self
    }

    pub fn query_threshold(mut self, query_threshold: Duration) -> Self {
        self.query_threshold = Some(query_threshold);
        self
    }

    pub fn search_threshold(mut self, search_threshold: Duration) -> Self {
        self.search_threshold = Some(search_threshold);
        self
    }

    pub fn analytics_threshold(mut self, analytics_threshold: Duration) -> Self {
        self.analytics_threshold = Some(analytics_threshold);
        self
    }

    pub fn management_threshold(mut self, management_threshold: Duration) -> Self {
        self.management_threshold = Some(management_threshold);
        self
    }

    pub fn sample_size(mut self, sample_size: usize) -> Self {
        self.sample_size = Some(sample_size);
        self
    }
}
