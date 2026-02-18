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

use hdrhistogram::Histogram;
use serde_json::json;
use std::collections::HashMap;
use std::fmt::Debug;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time;
use tokio::time::{Duration, Instant};

struct OpHistogram {
    histogram: Histogram<u64>,
    total_count: u64,
}

impl OpHistogram {
    fn new() -> Self {
        Self {
            histogram: Histogram::new(3).unwrap(),
            total_count: 0,
        }
    }

    fn record(&mut self, value: u64) {
        let _ = self.histogram.record(value);
        self.total_count += 1;
    }
}

type Service = String;
type Operation = String;

type MetricsMap = HashMap<Service, HashMap<Operation, OpHistogram>>;

use tracing::field::{Field, Visit};
use tracing::Subscriber;
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

#[derive(Default)]
struct MetricEventVisitor {
    service: Option<String>,
    operation: Option<String>,
    duration_us: Option<u64>,
}

impl Visit for MetricEventVisitor {
    fn record_f64(&mut self, field: &Field, value: f64) {
        if field.name() == "histogram.db.client.operation.duration"
            && value.is_finite()
            && value >= 0.0
        {
            let micros = (value * 1_000_000.0) as u64;
            self.duration_us = Some(micros);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "couchbase.service" => self.service = Some(value.to_string()),
            "db.operation.name" => self.operation = Some(value.to_string()),
            _ => {}
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn Debug) {
        // do nothing
    }
}

struct MetricEvent {
    service: String,
    operation: String,
    duration_us: u64,
}

pub struct LoggingMeterLayer {
    sender: UnboundedSender<MetricEvent>,
}

impl LoggingMeterLayer {
    pub fn new(options: Option<LoggingMeterOptions>) -> Self {
        let options = options.unwrap_or_default();
        let emit_interval = options.emit_interval.unwrap_or(Duration::from_secs(600));

        let (tx, rx) = unbounded_channel();

        tokio::spawn(Self::logger_task(rx, emit_interval));

        Self { sender: tx }
    }

    async fn logger_task(mut receiver: UnboundedReceiver<MetricEvent>, emit_interval: Duration) {
        let mut metrics: MetricsMap = HashMap::new();

        let start = Instant::now() + emit_interval;
        let mut interval = time::interval_at(start, emit_interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    Self::emit(&metrics, emit_interval);
                    metrics.clear();
                }
                Some(event) = receiver.recv() => {
                    let svc = metrics.entry(event.service).or_default();
                    let op = svc.entry(event.operation).or_insert_with(OpHistogram::new);
                    op.record(event.duration_us);
                }
                else => break,
            }
        }
    }

    fn emit(metrics: &MetricsMap, emit_interval: Duration) {
        if metrics.is_empty() {
            return;
        }

        let operations = metrics
            .iter()
            .map(|(service, ops)| {
                let ops_json = ops
                    .iter()
                    .map(|(op, hist)| {
                        let h = &hist.histogram;

                        (
                            op.clone(),
                            json!({
                                "total_count": hist.total_count,
                                "percentiles_us": {
                                    "50.0": h.value_at_quantile(0.50),
                                    "90.0": h.value_at_quantile(0.90),
                                    "99.0": h.value_at_quantile(0.99),
                                    "99.9": h.value_at_quantile(0.999),
                                    "100.0": h.max(),
                                }
                            }),
                        )
                    })
                    .collect::<serde_json::Map<_, _>>();

                (service.clone(), json!(ops_json))
            })
            .collect::<serde_json::Map<_, _>>();

        let output = json!({
            "meta": {
                "emit_interval_s": emit_interval.as_secs(),
            },
            "operations": operations,
        });

        match serde_json::to_string(&output) {
            Ok(s) => tracing::info!("LoggingMeter {}", s),
            Err(_) => tracing::error!("Failed to serialize LoggingMeter output"),
        }
    }
}

impl<S> Layer<S> for LoggingMeterLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != "couchbase.metrics" {
            return;
        }

        let mut visitor = MetricEventVisitor::default();
        event.record(&mut visitor);

        let (service, operation, duration) =
            match (visitor.service, visitor.operation, visitor.duration_us) {
                (Some(service), Some(op), Some(dur)) => (service, op, dur),
                _ => return,
            };

        let _ = self.sender.send(MetricEvent {
            service,
            operation,
            duration_us: duration,
        });
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct LoggingMeterOptions {
    pub emit_interval: Option<Duration>,
}

impl LoggingMeterOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn emit_interval(mut self, interval: Duration) -> Self {
        self.emit_interval = Some(interval);
        self
    }
}
