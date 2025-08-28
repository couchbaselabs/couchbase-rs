use crate::memdx::ops_crud::decode_res_ext_frames;
use crate::memdx::packet::ResponsePacket;
use crate::options::orphan_reporter::{OrphanReporterConfig, OrphanSinkFn};
use log::{debug, trace, warn};
use serde_json::json;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::mem;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{interval_at, Instant, MissedTickBehavior};

#[derive(Debug, Clone)]
pub struct OrphanContext {
    pub client_id: String,
    pub local_addr: SocketAddr,
    pub peer_addr: SocketAddr,
}

#[derive(Debug, Eq)]
struct OrphanLogItem {
    pub connection_id: String,
    pub operation_id: String,
    pub remote_socket: String,
    pub local_socket: String,
    pub server_duration: Duration,
    pub total_server_duration: Duration,
    pub operation_name: String,
}

impl Display for OrphanLogItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let obj = json!({
            "last_server_duration_us": self.server_duration.as_micros() as u64,
            "total_server_duration_us": self.total_server_duration.as_micros() as u64,
            "operation_name": self.operation_name,
            "last_local_id": self.connection_id,
            "operation_id": self.operation_id,
            "last_local_socket": self.local_socket,
            "last_remote_socket": self.remote_socket,
        });
        write!(f, "{}", obj)
    }
}

struct OrphanLogJsonEntry {
    count: u64,
    top_items: Vec<OrphanLogItem>,
}

impl Display for OrphanLogJsonEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let bodies: Vec<String> = self.top_items.iter().map(|item| item.to_string()).collect();
        write!(
            f,
            r#"{{"total_count":{},"top_requests":[{}]}}"#,
            self.count,
            bodies.join(",")
        )
    }
}

struct OrphanLogService(HashMap<String, OrphanLogJsonEntry>);

impl Display for OrphanLogService {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        for (svc, entry) in &self.0 {
            parts.push(format!(r#""{}":{}"#, svc, entry));
        }
        write!(f, "{{{}}}", parts.join(","))
    }
}

// Once we have total_duration added we will order on that
impl PartialEq for OrphanLogItem {
    fn eq(&self, other: &Self) -> bool {
        self.total_server_duration == other.total_server_duration
    }
}

impl Ord for OrphanLogItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.total_server_duration.cmp(&other.total_server_duration)
    }
}

impl PartialOrd for OrphanLogItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct State {
    heap: BinaryHeap<Reverse<OrphanLogItem>>,
    total_count: u64,
}

// Orphan reporter - Currently only handles/receives KV orphans
pub struct OrphanReporter {
    state: Arc<Mutex<State>>,
    sample_size: usize,
    reporter_interval: Duration,
    log_sink: Option<Arc<OrphanSinkFn>>,
}

impl OrphanReporter {
    pub fn new(config: OrphanReporterConfig) -> Self {
        let state = Arc::new(Mutex::new(State {
            heap: BinaryHeap::with_capacity(config.sample_size),
            total_count: 0,
        }));

        let log_sink = config.log_sink.clone();
        let state_clone = state.clone();

        tokio::spawn(async move {
            trace!(
                "OrphanReporter started: reporter_interval={:?}, sample_size={}",
                config.reporter_interval,
                config.sample_size
            );
            let start = Instant::now() + config.reporter_interval;
            let mut tick = interval_at(start, config.reporter_interval);
            tick.set_missed_tick_behavior(MissedTickBehavior::Burst);

            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        let (heap, total_count) = {
                            let mut st = state_clone.lock().unwrap();
                            if st.total_count == 0 {
                                continue;
                            }
                            let count = st.total_count;
                            st.total_count = 0;
                            (mem::take(&mut st.heap), count)
                        };

                        let obj = Self::create_log_object("kv".to_string(), heap, total_count);

                        let msg = format!("Orphaned responses observed: {}", obj);
                        if let Some(ref sink) = log_sink {
                            sink(&msg);
                        } else {
                            debug!("{}", msg);
                        }
                    }
                }
            }
        });
        Self {
            state,
            sample_size: config.sample_size,
            reporter_interval: config.reporter_interval,
            log_sink: config.log_sink,
        }
    }

    pub fn get_handle(&self) -> Arc<dyn Fn(ResponsePacket, OrphanContext) + Send + Sync> {
        let state = self.state.clone();
        let sample_size = self.sample_size;

        Arc::new(move |msg: ResponsePacket, ctx: OrphanContext| {
            let server_dur = msg
                .framing_extras
                .as_deref()
                .and_then(|f| decode_res_ext_frames(f).ok().flatten())
                .unwrap_or_default();

            let item = OrphanLogItem {
                connection_id: ctx.client_id,
                operation_id: format!("0x{:x}", msg.opaque),
                remote_socket: ctx.peer_addr.to_string(),
                local_socket: ctx.local_addr.to_string(),
                server_duration: server_dur,
                total_server_duration: server_dur,
                operation_name: format!("{:?}", msg.op_code),
            };

            let mut st = state.lock().unwrap();
            st.total_count += 1;

            if st.heap.len() < sample_size {
                st.heap.push(Reverse(item));
            } else if let Some(Reverse(ref min)) = st.heap.peek() {
                if item.total_server_duration > min.total_server_duration {
                    st.heap.pop();
                    st.heap.push(Reverse(item));
                }
            }
        })
    }

    fn create_log_object(
        service: String,
        heap_items: BinaryHeap<Reverse<OrphanLogItem>>,
        count: u64,
    ) -> OrphanLogService {
        let items: Vec<OrphanLogItem> = heap_items
            .into_sorted_vec()
            .into_iter()
            .map(|Reverse(item)| item)
            .collect();
        let entry = OrphanLogJsonEntry {
            count,
            top_items: items,
        };
        let mut services = HashMap::new();
        services.insert(service, entry);
        OrphanLogService(services)
    }

    pub fn with_sink_fn(mut self, sink: Arc<OrphanSinkFn>) -> Self {
        self.log_sink = Some(sink);
        self
    }
}
