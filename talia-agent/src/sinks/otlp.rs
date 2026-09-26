//! OTLP [`Sink`](talia_core::pipeline::Sink): exports pipeline samples to an
//! OpenTelemetry collector.
//!
//! Every OpenTelemetry-specific decision lives here: the meter provider, the
//! instrument set, and the mapping from neutral sample names to instruments.
//! Providers only produce neutral [`Sample`](talia_core::pipeline::Sample)s
//! and never see OTel types.

use anyhow::Context;
use anyhow::Result;
use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;
use opentelemetry::metrics::Gauge;
use opentelemetry_otlp::MetricExporter;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::metrics::Temporality;
use talia_core::pipeline::Sample;
use talia_core::pipeline::SampleValue;
use talia_core::pipeline::Sink;

use crate::identity::AgentIdentity;
use crate::modules::cpu::CPU_UTILIZATION_SAMPLE;
use crate::modules::disk_io::DISK_ERRORS_SAMPLE;
use crate::modules::disk_io::DISK_IN_FLIGHT_BYTES_SAMPLE;
use crate::modules::disk_io::DISK_IO_SAMPLE;
use crate::modules::disk_io::DISK_LATENCY_SAMPLE;
use crate::modules::disk_io::DISK_OPERATIONS_SAMPLE;
use crate::modules::disk_io::DISK_QUEUE_DEPTH_SAMPLE;
use crate::modules::memory::MEMORY_USAGE_SAMPLE;
use crate::modules::memory::MEMORY_UTILIZATION_SAMPLE;
use crate::modules::memory::SWAP_IO_SAMPLE;
use crate::modules::memory::SWAP_USAGE_SAMPLE;
use crate::modules::memory::SWAP_UTILIZATION_SAMPLE;
use crate::modules::network::NETWORK_IO_SAMPLE;
use crate::modules::storage::FILESYSTEM_LIMIT_SAMPLE;
use crate::modules::storage::FILESYSTEM_USAGE_SAMPLE;
use crate::modules::storage::FILESYSTEM_UTILIZATION_SAMPLE;

const SERVICE_NAME: &str = "talia-agent";
const SERVICE_NAMESPACE: &str = "talia";
const CONFIG_VERSION_ATTRIBUTE: &str = "talia.config.version";
const AGENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Exports pipeline samples to OTLP.
///
/// Holds the OpenTelemetry meter provider (kept alive for the sink's
/// lifetime) and every instrument the agent writes to.
pub struct OtlpSink {
    _provider: SdkMeterProvider,
    filesystem_usage: Gauge<u64>,
    filesystem_limit: Gauge<u64>,
    filesystem_utilization: Gauge<f64>,
    memory_usage: Gauge<u64>,
    memory_utilization: Gauge<f64>,
    swap_usage: Gauge<u64>,
    swap_utilization: Gauge<f64>,
    swap_io: Counter<u64>,
    cpu_utilization: Gauge<f64>,
    network_io: Counter<u64>,
    disk_io: Counter<u64>,
    disk_operations: Counter<u64>,
    disk_errors: Counter<u64>,
    disk_latency: Gauge<f64>,
    disk_queue_depth: Gauge<i64>,
    disk_in_flight_bytes: Gauge<i64>,
}

impl OtlpSink {
    /// Builds the OTLP exporter, meter provider, and instruments, and
    /// installs the provider as the global one.
    pub fn new(otlp_endpoint: &str, identity: &AgentIdentity) -> Result<Self> {
        let exporter = MetricExporter::builder()
            .with_tonic()
            .with_endpoint(otlp_endpoint.to_string())
            .with_temporality(Temporality::Cumulative)
            .build()
            .context("failed to build OTLP metric exporter")?;
        let resource = metrics_resource(identity);
        let provider = SdkMeterProvider::builder()
            .with_resource(resource)
            .with_periodic_exporter(exporter)
            .build();
        opentelemetry::global::set_meter_provider(provider.clone());
        let meter = opentelemetry::global::meter("talia");
        Ok(Self {
            _provider: provider,
            filesystem_usage: meter
                .u64_gauge("system.filesystem.usage")
                .with_unit("By")
                .with_description("Filesystem bytes by state.")
                .build(),
            filesystem_limit: meter
                .u64_gauge("system.filesystem.limit")
                .with_unit("By")
                .with_description("Total filesystem capacity.")
                .build(),
            filesystem_utilization: meter
                .f64_gauge("system.filesystem.utilization")
                .with_unit("1")
                .with_description("Fraction of filesystem bytes used.")
                .build(),
            memory_usage: meter
                .u64_gauge("system.memory.usage")
                .with_unit("By")
                .with_description("Host memory bytes by state.")
                .build(),
            memory_utilization: meter
                .f64_gauge("system.memory.utilization")
                .with_unit("1")
                .with_description("Fraction of host memory that is not available.")
                .build(),
            swap_usage: meter
                .u64_gauge("system.linux.memory.swap.usage")
                .with_unit("By")
                .with_description("Host swap bytes by state.")
                .build(),
            swap_utilization: meter
                .f64_gauge("system.linux.memory.swap.utilization")
                .with_unit("1")
                .with_description("Fraction of host swap currently used.")
                .build(),
            swap_io: meter
                .u64_counter("system.linux.memory.swap.io")
                .with_unit("By")
                .with_description("Host swap bytes moved in or out during the interval.")
                .build(),
            cpu_utilization: meter
                .f64_gauge("system.cpu.utilization")
                .with_unit("1")
                .with_description(
                    "Per-CPU utilization ratios from Talia eBPF scheduler accounting.",
                )
                .build(),
            network_io: meter
                .u64_counter("system.network.io")
                .with_unit("By")
                .with_description("Host-wide network bytes from Talia eBPF packet accounting.")
                .build(),
            disk_io: meter
                .u64_counter("system.disk.io")
                .with_unit("By")
                .with_description("Host-wide disk I/O bytes from Talia eBPF block accounting.")
                .build(),
            disk_operations: meter
                .u64_counter("system.disk.operations")
                .with_unit("{operation}")
                .with_description("Host-wide disk I/O operations from Talia eBPF block accounting.")
                .build(),
            disk_errors: meter
                .u64_counter("system.disk.errors")
                .with_unit("{error}")
                .with_description("Host-wide disk I/O errors from Talia eBPF block accounting.")
                .build(),
            disk_latency: meter
                .f64_gauge("system.disk.io.latency")
                .with_unit("ms")
                .with_description("Average disk I/O issue-to-completion latency.")
                .build(),
            disk_queue_depth: meter
                .i64_gauge("system.disk.io.queue_depth")
                .with_unit("{operation}")
                .with_description("Current in-flight disk I/O operations.")
                .build(),
            disk_in_flight_bytes: meter
                .i64_gauge("system.disk.io.in_flight")
                .with_unit("By")
                .with_description("Current in-flight disk I/O bytes.")
                .build(),
        })
    }
}

impl Sink for OtlpSink {
    fn name(&self) -> &'static str {
        "otlp"
    }

    /// Emits one neutral pipeline [`Sample`] into the OTLP instruments.
    ///
    /// The config version attribute is attached here so providers stay
    /// transport-agnostic.
    fn emit(&self, sample: &Sample, config_version: &str) {
        let attributes = sample_attributes(config_version, sample);
        match (sample.name.as_str(), sample.value) {
            (FILESYSTEM_LIMIT_SAMPLE, SampleValue::GaugeU64(bytes)) => {
                self.filesystem_limit.record(bytes, &attributes);
            },
            (FILESYSTEM_USAGE_SAMPLE, SampleValue::GaugeU64(bytes)) => {
                self.filesystem_usage.record(bytes, &attributes);
            },
            (FILESYSTEM_UTILIZATION_SAMPLE, SampleValue::GaugeF64(ratio)) => {
                self.filesystem_utilization.record(ratio, &attributes);
            },
            (MEMORY_USAGE_SAMPLE, SampleValue::GaugeU64(bytes)) => {
                self.memory_usage.record(bytes, &attributes);
            },
            (MEMORY_UTILIZATION_SAMPLE, SampleValue::GaugeF64(ratio)) => {
                self.memory_utilization.record(ratio, &attributes);
            },
            (SWAP_USAGE_SAMPLE, SampleValue::GaugeU64(bytes)) => {
                self.swap_usage.record(bytes, &attributes);
            },
            (SWAP_UTILIZATION_SAMPLE, SampleValue::GaugeF64(ratio)) => {
                self.swap_utilization.record(ratio, &attributes);
            },
            (SWAP_IO_SAMPLE, SampleValue::Counter(bytes)) => {
                self.swap_io.add(bytes, &attributes);
            },
            (CPU_UTILIZATION_SAMPLE, SampleValue::GaugeF64(ratio)) => {
                self.cpu_utilization.record(ratio, &attributes);
            },
            (NETWORK_IO_SAMPLE, SampleValue::Counter(bytes)) => {
                self.network_io.add(bytes, &attributes);
            },
            (DISK_IO_SAMPLE, SampleValue::Counter(bytes)) => {
                self.disk_io.add(bytes, &attributes);
            },
            (DISK_OPERATIONS_SAMPLE, SampleValue::Counter(operations)) => {
                self.disk_operations.add(operations, &attributes);
            },
            (DISK_ERRORS_SAMPLE, SampleValue::Counter(errors)) => {
                self.disk_errors.add(errors, &attributes);
            },
            (DISK_LATENCY_SAMPLE, SampleValue::GaugeF64(latency_ms)) => {
                self.disk_latency.record(latency_ms, &attributes);
            },
            (DISK_QUEUE_DEPTH_SAMPLE, SampleValue::GaugeI64(operations)) => {
                self.disk_queue_depth.record(operations, &attributes);
            },
            (DISK_IN_FLIGHT_BYTES_SAMPLE, SampleValue::GaugeI64(bytes)) => {
                self.disk_in_flight_bytes.record(bytes, &attributes);
            },
            (name, _) => {
                tracing::warn!(sample_name = %name, "talia_unknown_sample_dropped");
            },
        }
    }
}

impl Drop for OtlpSink {
    fn drop(&mut self) {
        let _ = self._provider.force_flush();
        let _ = self._provider.shutdown();
    }
}

fn metrics_resource(identity: &AgentIdentity) -> Resource {
    Resource::builder()
        .with_service_name(SERVICE_NAME)
        .with_attributes([
            KeyValue::new("service.namespace", SERVICE_NAMESPACE),
            KeyValue::new("service.version", AGENT_VERSION),
            KeyValue::new("host.name", identity.hostname.clone()),
        ])
        .build()
}

/// Builds the OTLP attributes for one neutral pipeline [`Sample`]: the
/// sample's own dimensions plus Talia's config version, so providers stay
/// transport-agnostic.
fn sample_attributes(config_version: &str, sample: &Sample) -> Vec<KeyValue> {
    let mut attributes: Vec<KeyValue> = sample
        .attributes
        .iter()
        .map(|(key, value)| KeyValue::new(key.clone(), value.clone()))
        .collect();
    attributes.push(KeyValue::new(
        CONFIG_VERSION_ATTRIBUTE,
        config_version.to_string(),
    ));
    attributes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_resource_identifies_the_talia_service() {
        // Given: a Talia agent running on a named fleet host.
        let identity = AgentIdentity {
            agent_id: "agent-1".to_string(),
            hostname: "host-1".to_string(),
            boot_id: None,
        };

        // When: the sink builds the resource attached to every metric.
        let resource = metrics_resource(&identity);

        // Then: standard OTel attributes identify the service and its namespace.
        assert_eq!(
            resource
                .get(&opentelemetry::Key::new("service.name"))
                .map(|value| value.to_string()),
            Some("talia-agent".to_string())
        );
        assert_eq!(
            resource
                .get(&opentelemetry::Key::new("service.namespace"))
                .map(|value| value.to_string()),
            Some("talia".to_string())
        );
        assert_eq!(
            resource
                .get(&opentelemetry::Key::new("host.name"))
                .map(|value| value.to_string()),
            Some("host-1".to_string())
        );
    }

    #[test]
    fn metric_attributes_use_the_talia_config_version_key() {
        // Given: a runtime configuration version and a pipeline sample.
        let config_version = "config-v1";
        let sample = Sample {
            name: MEMORY_USAGE_SAMPLE.to_string(),
            value: SampleValue::GaugeU64(42),
            attributes: std::collections::BTreeMap::from([(
                "system.memory.state".to_string(),
                "used".to_string(),
            )]),
            timestamp: std::time::SystemTime::now(),
        };

        // When: the sink builds the OTLP attributes for the sample.
        let attributes = sample_attributes(config_version, &sample);

        // Then: the sample dimensions are kept and Talia's neutral config
        // version key is attached.
        assert_eq!(attributes.len(), 2);
        let version = attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == "talia.config.version")
            .expect("config version attribute present");
        assert_eq!(version.value.to_string(), config_version);
    }
}
