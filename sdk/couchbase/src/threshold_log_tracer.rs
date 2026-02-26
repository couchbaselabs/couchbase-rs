/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use crate::tracing::{
    SPAN_ATTRIB_LOCAL_ID_KEY, SPAN_ATTRIB_NET_PEER_ADDRESS_KEY, SPAN_ATTRIB_NET_PEER_PORT_KEY,
    SPAN_ATTRIB_OPERATION_ID_KEY, SPAN_ATTRIB_OPERATION_KEY, SPAN_ATTRIB_SERVER_DURATION_KEY,
    SPAN_ATTRIB_SERVICE_KEY, SPAN_NAME_DISPATCH_TO_SERVER, SPAN_NAME_REQUEST_ENCODING,
};

const COUCHBASE_TARGET_PREFIX: &str = "couchbase::tracing";

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
            SPAN_ATTRIB_NET_PEER_ADDRESS_KEY => {
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

        tokio::runtime::Handle::try_current()
            .expect("ThresholdLoggingLayer::new must be called within a Tokio runtime.")
            .spawn(Self::logger_task(
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
        target == COUCHBASE_TARGET_PREFIX
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
                    if let Some(outer_info) = span_ref.extensions_mut().get_mut::<SpanInfo>() {
                        values.record(outer_info);
                    }
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
    management_threshold_us: u64,
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct ThresholdLoggingOptions {
    pub(crate) emit_interval: Option<Duration>,
    pub(crate) kv_threshold: Option<Duration>,
    pub(crate) query_threshold: Option<Duration>,
    pub(crate) search_threshold: Option<Duration>,
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

    pub fn management_threshold(mut self, management_threshold: Duration) -> Self {
        self.management_threshold = Some(management_threshold);
        self
    }

    pub fn sample_size(mut self, sample_size: usize) -> Self {
        self.sample_size = Some(sample_size);
        self
    }
}

#[cfg(test)]
impl ThresholdLoggingLayer {
    fn new_with_sender(thresholds: LoggingThresholds, sender: UnboundedSender<SpanInfo>) -> Self {
        Self {
            thresholds,
            sample_size: 10,
            sender,
        }
    }

    fn sender_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BinaryHeap;
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc::unbounded_channel;
    use tracing_subscriber::layer::SubscriberExt;

    #[derive(Clone, Default)]
    struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for CaptureWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
        type Writer = Self;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn span_info_ordered_by_total_duration() {
        let mut low = SpanInfo::new("get".to_string());
        low.total_duration_us = Some(100);

        let mut high = SpanInfo::new("get".to_string());
        high.total_duration_us = Some(999);

        assert!(high > low);
    }

    #[test]
    fn span_info_equal_when_same_duration() {
        let mut a = SpanInfo::new("get".to_string());
        a.total_duration_us = Some(500);

        let mut b = SpanInfo::new("query".to_string());
        b.total_duration_us = Some(500);

        assert_eq!(a, b);
    }

    #[test]
    fn span_info_binary_heap_pops_largest_first() {
        let mut heap = BinaryHeap::new();
        for duration in [300u64, 100, 500, 200, 400] {
            let mut s = SpanInfo::new("get".to_string());
            s.total_duration_us = Some(duration);
            heap.push(s);
        }
        let popped = heap.pop().unwrap();
        assert_eq!(popped.total_duration_us, Some(500));
    }

    fn make_thresholds() -> LoggingThresholds {
        LoggingThresholds {
            kv_threshold_us: 500_000,
            query_threshold_us: 1_000_000,
            search_threshold_us: 1_000_000,
            management_threshold_us: 1_000_000,
        }
    }

    #[test]
    fn span_over_threshold_is_reported() {
        let mut span = SpanInfo::new("get".to_string());
        span.service_name = Some("kv".to_string());
        span.total_duration_us = Some(600_000);
        assert!(span.is_over_threshold(&make_thresholds()));
    }

    #[test]
    fn span_under_threshold_is_not_reported() {
        let mut span = SpanInfo::new("get".to_string());
        span.service_name = Some("kv".to_string());
        span.total_duration_us = Some(400_000);
        assert!(!span.is_over_threshold(&make_thresholds()));
    }

    #[test]
    fn span_at_exact_threshold_is_not_reported() {
        let mut span = SpanInfo::new("get".to_string());
        span.service_name = Some("kv".to_string());
        span.total_duration_us = Some(500_000);
        assert!(!span.is_over_threshold(&make_thresholds()));
    }

    #[test]
    fn span_with_unknown_service_is_not_reported() {
        let mut span = SpanInfo::new("op".to_string());
        span.service_name = Some("analytics".to_string());
        span.total_duration_us = Some(u64::MAX);
        assert!(!span.is_over_threshold(&make_thresholds()));
    }

    #[test]
    fn span_with_no_service_is_not_reported() {
        let mut span = SpanInfo::new("op".to_string());
        span.total_duration_us = Some(u64::MAX);
        assert!(!span.is_over_threshold(&make_thresholds()));
    }

    #[test]
    fn process_and_emit_logs_outputs_correct_json() {
        let mut span1 = SpanInfo::new("get".to_string());
        span1.service_name = Some("kv".to_string());
        span1.total_duration_us = Some(750_000);
        span1.last_server_duration_us = Some(50_000);

        let mut span2 = SpanInfo::new("get".to_string());
        span2.service_name = Some("kv".to_string());
        span2.total_duration_us = Some(600_000);

        let log_data = HashMap::from([("kv".to_string(), (5usize, vec![span1, span2]))]);

        let writer = CaptureWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(writer.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            ThresholdLoggingLayer::process_and_emit_logs(log_data);
        });

        let output = String::from_utf8(writer.0.lock().unwrap().clone()).unwrap();
        assert!(
            output.contains("Operations over threshold"),
            "output should contain the log prefix, got: {output}"
        );
        assert!(
            output.contains("\"kv\""),
            "service name should appear in output"
        );
        assert!(
            output.contains("total_count"),
            "total_count should appear in output"
        );
        assert!(
            output.contains("top_requests"),
            "top_requests should appear in output"
        );
        assert!(
            output.contains("\"total_count\":5"),
            "total_count value 5 should appear in output"
        );
        assert!(
            output.contains("total_duration_us"),
            "total_duration_us should appear in top_requests entries"
        );
    }

    #[test]
    fn process_and_emit_logs_does_not_log_when_empty() {
        let writer = CaptureWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(writer.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            ThresholdLoggingLayer::process_and_emit_logs(HashMap::new());
        });

        let output = String::from_utf8(writer.0.lock().unwrap().clone()).unwrap();
        assert!(
            output.is_empty(),
            "no output should be emitted when log_data is empty, got: {output}"
        );
    }

    #[test]
    fn collect_log_data_returns_top_n_per_service() {
        let mut groups: HashMap<String, BinaryHeap<SpanInfo>> = HashMap::new();
        let heap = groups.entry("kv".to_string()).or_default();
        for i in 0..15u64 {
            let mut s = SpanInfo::new("get".to_string());
            s.service_name = Some("kv".to_string());
            s.total_duration_us = Some(i * 1_000);
            heap.push(s);
        }

        let result = ThresholdLoggingLayer::collect_log_data(&mut groups, 10);
        let (total_count, top) = result.get("kv").unwrap();

        assert_eq!(*total_count, 15);
        assert_eq!(top.len(), 10);
        assert_eq!(top[0].total_duration_us, Some(14_000));
    }

    #[test]
    fn collect_log_data_fewer_than_n_returns_all() {
        let mut groups: HashMap<String, BinaryHeap<SpanInfo>> = HashMap::new();
        let heap = groups.entry("query".to_string()).or_default();
        for i in 0..3u64 {
            let mut s = SpanInfo::new("query".to_string());
            s.total_duration_us = Some(i * 1_000);
            heap.push(s);
        }

        let result = ThresholdLoggingLayer::collect_log_data(&mut groups, 10);
        let (total_count, top) = result.get("query").unwrap();

        assert_eq!(*total_count, 3);
        assert_eq!(top.len(), 3);
    }

    #[test]
    fn span_is_couchbase_accepts_couchbase_tracing_prefix() {
        assert!(ThresholdLoggingLayer::span_is_couchbase(
            "couchbase::tracing"
        ));
    }

    #[test]
    fn span_is_couchbase_rejects_non_couchbase_tracing_prefix() {
        assert!(!ThresholdLoggingLayer::span_is_couchbase(
            "couchbase::cluster"
        ));
    }

    #[test]
    fn span_is_couchbase_rejects_other_crates() {
        assert!(!ThresholdLoggingLayer::span_is_couchbase("tracing::span"));
        assert!(!ThresholdLoggingLayer::span_is_couchbase("tokio::task"));
        assert!(!ThresholdLoggingLayer::span_is_couchbase(
            "my_app::couchbase_wrapper"
        ));
    }

    fn zero_thresholds() -> LoggingThresholds {
        LoggingThresholds {
            kv_threshold_us: 0,
            query_threshold_us: 0,
            search_threshold_us: 0,
            management_threshold_us: 0,
        }
    }

    #[tokio::test]
    async fn layer_records_service_set_via_span_record_after_creation() {
        let (tx, mut rx) = unbounded_channel::<SpanInfo>();
        let layer = ThresholdLoggingLayer::new_with_sender(zero_thresholds(), tx);
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::trace_span!(
                target: "couchbase::tracing",
                "get",
                db.operation.name = "get",
                couchbase.service = tracing::field::Empty,
            );
            span.record("couchbase.service", "kv");
            let _guard = span.enter();
        });

        let info = rx
            .try_recv()
            .expect("span should be sent — service recorded via span.record() must reach SpanInfo");
        assert_eq!(
            info.service_name.as_deref(),
            Some("kv"),
            "service_name should be populated even when set via span.record() after creation"
        );
    }

    #[tokio::test]
    async fn layer_records_root_span_service_and_operation() {
        let (tx, mut rx) = unbounded_channel::<SpanInfo>();
        let layer = ThresholdLoggingLayer::new_with_sender(zero_thresholds(), tx);
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::trace_span!(
                target: "couchbase::tracing",
                "get",
                db.operation.name = "get",
                couchbase.service = "kv",
            );
            let _guard = span.enter();
        });

        let info = rx
            .try_recv()
            .expect("span should be sent when over threshold");
        assert_eq!(info.service_name.as_deref(), Some("kv"));
        assert_eq!(info.operation_name, "get");
        assert!(
            info.total_duration_us.is_some(),
            "total_duration_us should be populated on close"
        );
    }

    #[tokio::test]
    async fn layer_records_fields_from_span_attributes() {
        let (tx, mut rx) = unbounded_channel::<SpanInfo>();
        let layer = ThresholdLoggingLayer::new_with_sender(zero_thresholds(), tx);
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::trace_span!(
                target: "couchbase::tracing",
                "get",
                db.operation.name = "get",
                couchbase.service = "kv",
                network.peer.address = "10.0.0.1",
                network.peer.port = "11210",
                couchbase.operation_id = "op-42",
                couchbase.local_id = "local-7",
            );
            let _guard = span.enter();
        });

        let info = rx
            .try_recv()
            .expect("span should be sent when over threshold");
        let socket = info
            .last_remote_socket
            .expect("remote socket should be populated");
        assert_eq!(socket.ip.as_deref(), Some("10.0.0.1"));
        assert_eq!(socket.port.as_deref(), Some("11210"));
        assert_eq!(info.operation_id.as_deref(), Some("op-42"));
        assert_eq!(info.last_local_id.as_deref(), Some("local-7"));
    }

    #[tokio::test]
    async fn layer_records_encoding_sub_span_duration() {
        let (tx, mut rx) = unbounded_channel::<SpanInfo>();
        let layer = ThresholdLoggingLayer::new_with_sender(zero_thresholds(), tx);
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let root = tracing::trace_span!(
                target: "couchbase::tracing",
                "get",
                db.operation.name = "get",
                couchbase.service = "kv",
            );
            let _root_guard = root.enter();

            let encoding = tracing::trace_span!(target: "couchbase::tracing", "request_encoding");
            let _enc_guard = encoding.enter();
            std::thread::sleep(std::time::Duration::from_millis(1));
        });

        let info = rx
            .try_recv()
            .expect("span should be sent when over threshold");
        assert!(
            info.encode_duration_us.is_some(),
            "encode_duration_us should be set after request_encoding span closes"
        );
        assert!(
            info.encode_duration_us.unwrap() >= 1_000,
            "encoding span included a 1 ms sleep so duration >= 1000 µs"
        );
    }

    #[tokio::test]
    async fn layer_records_dispatch_sub_span_duration_and_server_duration() {
        let (tx, mut rx) = unbounded_channel::<SpanInfo>();
        let layer = ThresholdLoggingLayer::new_with_sender(zero_thresholds(), tx);
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let root = tracing::trace_span!(
                target: "couchbase::tracing",
                "get",
                db.operation.name = "get",
                couchbase.service = "kv",
            );
            let _root_guard = root.enter();

            let dispatch = tracing::trace_span!(
                target: "couchbase::tracing",
                "dispatch_to_server",
                couchbase.server_duration = tracing::field::Empty,
            );
            let dispatch_guard = dispatch.enter();
            dispatch.record("couchbase.server_duration", 500u64);
            std::thread::sleep(std::time::Duration::from_millis(1));
            drop(dispatch_guard);
        });

        let info = rx
            .try_recv()
            .expect("span should be sent when over threshold");
        assert!(
            info.last_dispatch_duration_us.is_some(),
            "last_dispatch_duration_us should be set after dispatch_to_server closes"
        );
        assert!(
            info.total_dispatch_duration_us.is_some(),
            "total_dispatch_duration_us should be set"
        );
        assert_eq!(
            info.last_dispatch_duration_us, info.total_dispatch_duration_us,
            "with a single dispatch, last == total"
        );
        assert_eq!(info.last_server_duration_us, Some(500));
        assert_eq!(info.total_server_duration_us, Some(500));
    }

    #[tokio::test]
    async fn layer_accumulates_multiple_dispatch_and_server_durations() {
        let (tx, mut rx) = unbounded_channel::<SpanInfo>();
        let layer = ThresholdLoggingLayer::new_with_sender(zero_thresholds(), tx);
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let root = tracing::trace_span!(
                target: "couchbase::tracing",
                "get",
                db.operation.name = "get",
                couchbase.service = "kv",
            );
            let _root_guard = root.enter();

            {
                let d = tracing::trace_span!(
                    target: "couchbase::tracing",
                    "dispatch_to_server",
                    couchbase.server_duration = tracing::field::Empty,
                );
                let _g = d.enter();
                d.record("couchbase.server_duration", 200u64);
            }
            {
                let d = tracing::trace_span!(
                    target: "couchbase::tracing",
                    "dispatch_to_server",
                    couchbase.server_duration = tracing::field::Empty,
                );
                let _g = d.enter();
                d.record("couchbase.server_duration", 300u64);
            }
        });

        let info = rx
            .try_recv()
            .expect("span should be sent when over threshold");
        assert_eq!(
            info.total_server_duration_us,
            Some(500),
            "total server duration should be 200 + 300 = 500"
        );
        assert_eq!(
            info.last_server_duration_us,
            Some(300),
            "last server duration should be from the second dispatch"
        );
    }

    #[test]
    #[should_panic(expected = "ThresholdLoggingLayer::new must be called within a Tokio runtime")]
    fn new_outside_tokio_runtime_panics() {
        ThresholdLoggingLayer::new(None);
    }
}
