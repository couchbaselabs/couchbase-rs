use crate::memdx::ops_crud::decode_res_ext_frames;
use crate::memdx::packet::ResponsePacket;
use crate::options::agent::OrphanReporterConfig;
use log::{debug, trace, warn};
use serde_json::json;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::mem;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
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

// Orphan reporter - Currently only handles/receives KV orphans
pub struct OrphanReporter {
    enabled: bool,
    sender: Option<mpsc::Sender<OrphanLogItem>>,
}

impl OrphanReporter {
    pub fn new(config: OrphanReporterConfig) -> Self {
        if !config.enabled {
            return Self {
                enabled: config.enabled,
                sender: None,
            };
        }
        let (tx, mut rx) = mpsc::channel::<OrphanLogItem>(100);

        tokio::spawn(async move {
            trace!(
                "OrphanReporter started: enabled={}, reporter_interval={:?}, sample_size={}",
                config.enabled,
                config.reporter_interval,
                config.sample_size
            );
            let mut total_count: u64 = 0;
            let mut buffer: BinaryHeap<Reverse<OrphanLogItem>> =
                BinaryHeap::with_capacity(config.sample_size);

            let start = Instant::now() + config.reporter_interval;
            let mut tick = interval_at(start, config.reporter_interval);
            tick.set_missed_tick_behavior(MissedTickBehavior::Burst);

            loop {
                tokio::select! {
                    maybe_msg = rx.recv() => {
                        match maybe_msg {
                            Some(entry) => {
                                total_count += 1;
                                if buffer.len() < config.sample_size {
                                    buffer.push(Reverse(entry));
                                } else if let Some(Reverse(min_item)) = buffer.peek() {
                                    if entry.total_server_duration > min_item.total_server_duration {
                                        buffer.pop();
                                        buffer.push(Reverse(entry));
                                    }
                                }
                            }
                            None => {
                                if !buffer.is_empty() {
                                    debug!("Orphaned responses observed: {}", Self::create_log_object("kv".to_string(), mem::take(&mut buffer), total_count));
                                }
                                trace!("OrphanReporter shutdown");
                                break;
                            }
                        }
                    }
                    _ = tick.tick() => {
                        if !buffer.is_empty() {
                            debug!("Orphaned responses observed: {}", Self::create_log_object("kv".to_string(), mem::take(&mut buffer), total_count));
                            total_count = 0
                        }
                    }
                }
            }
        });
        Self {
            enabled: config.enabled,
            sender: Some(tx),
        }
    }

    pub fn get_handle(&self) -> Arc<dyn Fn(ResponsePacket, OrphanContext) + Send + Sync> {
        if self.enabled {
            let tx = self.sender.clone();
            Arc::new(move |msg: ResponsePacket, ctx: OrphanContext| {
                let tx = tx.clone();

                let server_dur = msg
                    .framing_extras
                    .as_deref()
                    .and_then(|f| decode_res_ext_frames(f).ok().flatten())
                    .unwrap_or_default();

                let entry = OrphanLogItem {
                    connection_id: ctx.client_id,
                    operation_id: format!("0x{:x}", msg.opaque),
                    remote_socket: ctx.peer_addr.to_string(),
                    local_socket: ctx.local_addr.to_string(),
                    server_duration: server_dur,
                    total_server_duration: server_dur,
                    operation_name: format!("{:?}", msg.op_code),
                };
                if let Some(tx) = tx {
                    if let Err(e) = tx.try_send(entry) {
                        trace!("OrphanReporter: failed to send entry: {e}");
                    }
                } else {
                    trace!("OrphanReporter: sender dropped; dropping entry");
                }
            })
        } else {
            Arc::new(|_, _| ())
        }
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
}
