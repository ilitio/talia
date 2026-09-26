//! Runs the Talia host monitoring agent.

#![deny(warnings)]
#![allow(
    clippy::disallowed_methods,
    reason = "the CLI reads documented TALIA_* exporter and secret variables at startup"
)]

use std::env;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use futures_util::SinkExt;
use futures_util::StreamExt;
use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;
use opentelemetry::metrics::Gauge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::metrics::Temporality;
use reqwest::StatusCode;
use talia_agent::identity;
use talia_agent::modules::cpu::CpuCollector;
use talia_agent::modules::cpu::CpuSnapshot;
use talia_agent::modules::disk_io::DiskIoCollector;
use talia_agent::modules::disk_io::DiskIoSample;
use talia_agent::modules::memory::MemoryCollector;
use talia_agent::modules::memory::MemorySample;
use talia_agent::modules::network::NetworkCollector;
use talia_agent::modules::network::NetworkSample;
use talia_agent::modules::storage::FILESYSTEM_LIMIT_SAMPLE;
use talia_agent::modules::storage::FILESYSTEM_USAGE_SAMPLE;
use talia_agent::modules::storage::FILESYSTEM_UTILIZATION_SAMPLE;
use talia_agent::modules::storage::StorageProvider;
use talia_core::config::AgentBootstrapConfig;
use talia_core::config::AgentRuntimeConfig;
use talia_core::control::AGENT_CONFIG_TOKEN_HEADER;
use talia_core::control::AGENT_ID_HEADER;
use talia_core::control::AgentControlMessage;
use talia_core::control::CONFIG_SESSION_ID_HEADER;
use talia_core::control::ServerControlMessage;
use talia_core::pipeline::Provider;
use talia_core::pipeline::Sample;
use talia_core::pipeline::SampleValue;
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tracing_subscriber::EnvFilter;

const AGENT_VERSION: &str = env!("CARGO_PKG_VERSION");
const LAST_KNOWN_CONFIG_FILE: &str = "last-config.json";
const SERVICE_NAME: &str = "talia-agent";
const SERVICE_NAMESPACE: &str = "talia";
const CONFIG_VERSION_ATTRIBUTE: &str = "talia.config.version";

#[derive(Parser)]
#[command(about = "Talia host monitoring agent")]
struct Args {
    #[arg(long, env = "TALIA_AGENT_CONFIG")]
    config: Option<PathBuf>,
    #[arg(long, default_value = "info")]
    log_filter: String,
}

#[derive(Clone)]
struct AgentIdentity {
    agent_id: String,
    hostname: String,
    boot_id: Option<String>,
}

#[derive(Clone)]
struct ControlClientConfig {
    http_url: String,
    ws_url: String,
    token: String,
    agent_config_token: Option<String>,
}

type ConfigSessionId = Arc<RwLock<Option<String>>>;

struct Metrics {
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

impl Metrics {
    fn new(otlp_endpoint: &str, identity: &AgentIdentity) -> Result<Self> {
        let exporter = opentelemetry_otlp::MetricExporter::builder()
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

    /// Records one neutral pipeline [`Sample`] into the OTLP instruments.
    ///
    /// The config version attribute is attached here so providers stay
    /// transport-agnostic.
    fn record_sample(&self, config_version: &str, sample: &Sample) {
        let mut attributes: Vec<KeyValue> = sample
            .attributes
            .iter()
            .map(|(key, value)| KeyValue::new(key.clone(), value.clone()))
            .collect();
        attributes.push(KeyValue::new(
            CONFIG_VERSION_ATTRIBUTE,
            config_version.to_string(),
        ));
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
            (name, _) => {
                tracing::warn!(sample_name = %name, "talia_unknown_sample_dropped");
            },
        }
    }

    fn record_cpu(&self, config_version: &str, sample: &CpuSnapshot) {
        let cpu = sample.cpu.to_string();
        self.record_cpu_state(config_version, &cpu, "idle", sample.idle_ratio());
        self.record_cpu_state(config_version, &cpu, "user", sample.user_ratio());
        self.record_cpu_state(config_version, &cpu, "system", sample.system_ratio());
    }

    fn record_memory(&self, config_version: &str, sample: &MemorySample) {
        self.record_memory_state(config_version, "total", sample.total_bytes);
        self.record_memory_state(config_version, "used", sample.used_bytes);
        self.record_memory_state(config_version, "available", sample.available_bytes);
        self.record_memory_state(config_version, "free", sample.free_bytes);
        self.record_memory_state(config_version, "cached", sample.cached_bytes);
        self.memory_utilization
            .record(sample.used_ratio, &memory_attributes(config_version));

        self.record_swap_state(config_version, "total", sample.swap_total_bytes);
        self.record_swap_state(config_version, "used", sample.swap_used_bytes);
        self.record_swap_state(config_version, "free", sample.swap_free_bytes);
        self.swap_utilization
            .record(sample.swap_used_ratio, &memory_attributes(config_version));

        self.record_swap_io(config_version, "in", sample.swap_in_bytes);
        self.record_swap_io(config_version, "out", sample.swap_out_bytes);
    }

    fn record_memory_state(&self, config_version: &str, state: &str, bytes: u64) {
        let mut attributes = memory_attributes(config_version);
        attributes.push(KeyValue::new("system.memory.state", state.to_string()));
        self.memory_usage.record(bytes, &attributes);
    }

    fn record_swap_state(&self, config_version: &str, state: &str, bytes: u64) {
        let mut attributes = memory_attributes(config_version);
        attributes.push(KeyValue::new(
            "system.linux.memory.swap.state",
            state.to_string(),
        ));
        self.swap_usage.record(bytes, &attributes);
    }

    fn record_swap_io(&self, config_version: &str, direction: &str, bytes: u64) {
        let mut attributes = memory_attributes(config_version);
        attributes.push(KeyValue::new(
            "system.linux.memory.swap.direction",
            direction.to_string(),
        ));
        self.swap_io.add(bytes, &attributes);
    }

    fn record_cpu_state(&self, config_version: &str, cpu: &str, state: &str, ratio: f64) {
        let mut attributes = cpu_attributes(config_version, cpu);
        attributes.push(KeyValue::new("system.cpu.state", state.to_string()));
        self.cpu_utilization.record(ratio, &attributes);
    }

    fn record_network(&self, config_version: &str, sample: &NetworkSample) {
        self.network_io
            .add(sample.bytes, &network_attributes(config_version, sample));
    }

    fn record_disk_io(&self, config_version: &str, sample: &DiskIoSample) {
        let attributes = disk_io_attributes(config_version, sample);
        self.disk_io.add(sample.bytes, &attributes);
        self.disk_operations.add(sample.operations, &attributes);
        self.disk_errors.add(sample.errors, &attributes);
        self.disk_queue_depth
            .record(sample.in_flight_operations, &attributes);
        self.disk_in_flight_bytes
            .record(sample.in_flight_bytes, &attributes);
        if let Some(latency) = sample.average_latency {
            self.disk_latency
                .record(latency.as_secs_f64() * 1_000.0, &attributes);
        }
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

impl Drop for Metrics {
    fn drop(&mut self) {
        let _ = self._provider.force_flush();
        let _ = self._provider.shutdown();
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("talia-agent failed: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(args.log_filter))
        .init();

    let mut bootstrap = match &args.config {
        Some(path) => AgentBootstrapConfig::load(path)
            .with_context(|| format!("failed to load agent config {}", path.display()))?,
        None => AgentBootstrapConfig::default(),
    };
    if let Some(token) = optional_env_secret("TALIA_CONTROL_TOKEN")? {
        bootstrap.control_token = Some(token);
    }
    if let Some(token) = optional_env_secret("TALIA_AGENT_CONFIG_TOKEN")? {
        bootstrap.agent_config_token = Some(token);
    }
    bootstrap
        .validate()
        .context("agent bootstrap config is invalid")?;
    bootstrap
        .fallback_runtime_config()
        .validate()
        .context("local fallback runtime config is invalid")?;

    let identity = AgentIdentity {
        agent_id: identity::load_or_create_agent_id(&bootstrap.state_dir)
            .context("failed to load or create Talia agent id")?,
        hostname: identity::hostname(),
        boot_id: identity::read_boot_id(),
    };
    let runtime_config = load_last_known_config(&bootstrap.state_dir)
        .unwrap_or_else(|| bootstrap.fallback_runtime_config());
    runtime_config
        .validate()
        .context("initial runtime config is invalid")?;
    let shared_config = Arc::new(RwLock::new(runtime_config));
    let config_session_id = Arc::new(RwLock::new(None));
    let metrics = Arc::new(Metrics::new(&bootstrap.otlp_endpoint, &identity)?);
    let http_client = reqwest::Client::new();

    let control = bootstrap
        .control_token
        .as_ref()
        .filter(|token| !token.trim().is_empty())
        .map(|token| ControlClientConfig {
            http_url: bootstrap.control_http_url.clone(),
            ws_url: bootstrap.control_ws_url.clone(),
            token: token.clone(),
            agent_config_token: bootstrap.agent_config_token.clone(),
        });

    if let Some(control) = control.clone() {
        tokio::spawn(control_websocket_loop(
            http_client.clone(),
            control.clone(),
            identity.clone(),
            Arc::clone(&shared_config),
            Arc::clone(&config_session_id),
            bootstrap.state_dir.clone(),
        ));
        tokio::spawn(config_poll_loop(
            http_client,
            control,
            identity.clone(),
            Arc::clone(&shared_config),
            Arc::clone(&config_session_id),
            bootstrap.state_dir.clone(),
        ));
    } else {
        tracing::warn!("talia_control_token_missing_using_local_config_only");
    }

    tokio::spawn(cpu_loop(Arc::clone(&shared_config), Arc::clone(&metrics)));
    tokio::spawn(network_loop(
        Arc::clone(&shared_config),
        Arc::clone(&metrics),
    ));
    tokio::spawn(disk_io_loop(
        Arc::clone(&shared_config),
        Arc::clone(&metrics),
    ));
    tokio::spawn(memory_loop(
        Arc::clone(&shared_config),
        Arc::clone(&metrics),
    ));

    tokio::select! {
        result = storage_loop(Arc::clone(&shared_config), Arc::clone(&metrics)) => result,
        signal = tokio::signal::ctrl_c() => {
            signal.context("failed to listen for shutdown signal")?;
            tracing::info!("talia_agent_shutdown_signal");
            Ok(())
        }
    }
}

async fn memory_loop(
    shared_config: Arc<RwLock<AgentRuntimeConfig>>,
    metrics: Arc<Metrics>,
) -> Result<()> {
    let mut collector = MemoryCollector::new();
    loop {
        let config = shared_config.read().await.clone();
        if config.memory.enabled {
            match collector.collect() {
                Ok(sample) => {
                    metrics.record_memory(&config.version, &sample);
                    tracing::info!(
                        config_version = %config.version,
                        used_ratio = sample.used_ratio,
                        swap_used_ratio = sample.swap_used_ratio,
                        swap_in_bytes = sample.swap_in_bytes,
                        swap_out_bytes = sample.swap_out_bytes,
                        "talia_memory_collected"
                    );
                },
                Err(error) => {
                    tracing::warn!(error = %error, "talia_memory_collection_failed");
                },
            }
        }
        tokio::time::sleep(Duration::from_secs(config.memory.interval_seconds)).await;
    }
}

async fn disk_io_loop(shared_config: Arc<RwLock<AgentRuntimeConfig>>, metrics: Arc<Metrics>) {
    let mut collector = None;
    let mut disabled_logged = false;

    loop {
        let config = shared_config.read().await.clone();
        let interval = Duration::from_secs(config.disk_io.interval_seconds);
        if !config.disk_io.enabled {
            if collector.take().is_some() {
                tracing::info!("talia_disk_io_collector_stopped");
            }
            if !disabled_logged {
                tracing::info!("talia_disk_io_collector_disabled");
                disabled_logged = true;
            }
            tokio::time::sleep(interval).await;
            continue;
        }
        disabled_logged = false;
        if collector.is_none() {
            tracing::info!("talia_disk_io_collector_starting");
            match DiskIoCollector::load() {
                Ok(loaded) => {
                    collector = Some(loaded);
                    tracing::info!("talia_disk_io_collector_started");
                },
                Err(error) => {
                    tracing::warn!(error = %error, "talia_disk_io_collector_start_failed");
                    tokio::time::sleep(interval).await;
                    continue;
                },
            }
        }

        tokio::time::sleep(interval).await;
        let Some(collector) = collector.as_mut() else {
            continue;
        };
        match collector.collect(interval) {
            Ok(snapshot) => {
                for sample in &snapshot.samples {
                    metrics.record_disk_io(&config.version, sample);
                    tracing::debug!(
                        direction = sample.direction.as_str(),
                        bytes = sample.bytes,
                        operations = sample.operations,
                        errors = sample.errors,
                        average_latency_ms = sample
                            .average_latency
                            .map(|latency| latency.as_secs_f64() * 1_000.0),
                        in_flight_operations = sample.in_flight_operations,
                        in_flight_bytes = sample.in_flight_bytes,
                        "talia_disk_io_collected"
                    );
                }
            },
            Err(error) => {
                tracing::warn!(error = %error, "talia_disk_io_collection_failed");
            },
        }
    }
}

async fn network_loop(shared_config: Arc<RwLock<AgentRuntimeConfig>>, metrics: Arc<Metrics>) {
    let mut collector = None;
    let mut disabled_logged = false;

    loop {
        let config = shared_config.read().await.clone();
        let interval = Duration::from_secs(config.network.interval_seconds);
        if !config.network.enabled {
            if collector.take().is_some() {
                tracing::info!("talia_network_collector_stopped");
            }
            if !disabled_logged {
                tracing::info!("talia_network_collector_disabled");
                disabled_logged = true;
            }
            tokio::time::sleep(interval).await;
            continue;
        }
        disabled_logged = false;
        if collector.is_none() {
            tracing::info!("talia_network_collector_starting");
            match NetworkCollector::load() {
                Ok(loaded) => {
                    collector = Some(loaded);
                    tracing::info!("talia_network_collector_started");
                },
                Err(error) => {
                    tracing::warn!(error = %error, "talia_network_collector_start_failed");
                    tokio::time::sleep(interval).await;
                    continue;
                },
            }
        }

        tokio::time::sleep(interval).await;
        let Some(collector) = collector.as_mut() else {
            continue;
        };
        match collector.collect(interval) {
            Ok(snapshot) => {
                for sample in &snapshot.samples {
                    metrics.record_network(&config.version, sample);
                    tracing::debug!(
                        direction = sample.direction.as_str(),
                        bytes = sample.bytes,
                        packets = sample.packets,
                        "talia_network_collected"
                    );
                }
            },
            Err(error) => {
                tracing::warn!(error = %error, "talia_network_collection_failed");
            },
        }
    }
}

async fn cpu_loop(shared_config: Arc<RwLock<AgentRuntimeConfig>>, metrics: Arc<Metrics>) {
    let mut collector = None;
    let mut disabled_logged = false;

    loop {
        let config = shared_config.read().await.clone();
        let interval = Duration::from_secs(config.cpu.interval_seconds);
        if !config.cpu.enabled {
            if collector.take().is_some() {
                tracing::info!("talia_cpu_collector_stopped");
            }
            if !disabled_logged {
                tracing::info!("talia_cpu_collector_disabled");
                disabled_logged = true;
            }
            tokio::time::sleep(interval).await;
            continue;
        }
        disabled_logged = false;
        if collector.is_none() {
            tracing::info!("talia_cpu_collector_starting");
            match CpuCollector::load() {
                Ok(loaded) => {
                    collector = Some(loaded);
                    tracing::info!("talia_cpu_collector_started");
                },
                Err(error) => {
                    tracing::warn!(error = %error, "talia_cpu_collector_start_failed");
                    tokio::time::sleep(interval).await;
                    continue;
                },
            }
        }

        tokio::time::sleep(interval).await;
        let Some(collector) = collector.as_mut() else {
            continue;
        };
        match collector.collect(interval) {
            Ok(samples) => {
                for sample in samples {
                    metrics.record_cpu(&config.version, &sample);
                    tracing::debug!(
                        cpu = sample.cpu,
                        idle_ratio = sample.idle_ratio(),
                        user_ratio = sample.user_ratio(),
                        system_ratio = sample.system_ratio(),
                        "talia_cpu_collected"
                    );
                }
            },
            Err(error) => {
                tracing::warn!(error = %error, "talia_cpu_collection_failed");
            },
        }
    }
}

async fn storage_loop(
    shared_config: Arc<RwLock<AgentRuntimeConfig>>,
    metrics: Arc<Metrics>,
) -> Result<()> {
    let mut provider: Box<dyn Provider> = Box::new(StorageProvider::new(Vec::new()));
    let mut configured_mounts: Vec<String> = Vec::new();
    loop {
        let config = shared_config.read().await.clone();
        if config.storage.enabled {
            if config.storage.mounts != configured_mounts {
                configured_mounts = config.storage.mounts.clone();
                provider = Box::new(StorageProvider::new(configured_mounts.clone()));
            }
            match provider.collect() {
                Ok(samples) => {
                    for sample in &samples {
                        metrics.record_sample(&config.version, sample);
                    }
                    tracing::info!(
                        config_version = %config.version,
                        sample_count = samples.len(),
                        "talia_storage_collected"
                    );
                },
                Err(error) => {
                    tracing::warn!(error = %error, "talia_storage_collection_failed");
                },
            }
        }
        tokio::time::sleep(Duration::from_secs(config.storage.interval_seconds)).await;
    }
}

async fn config_poll_loop(
    client: reqwest::Client,
    control: ControlClientConfig,
    identity: AgentIdentity,
    shared_config: Arc<RwLock<AgentRuntimeConfig>>,
    config_session_id: ConfigSessionId,
    state_dir: PathBuf,
) {
    loop {
        let interval = {
            let config = shared_config.read().await;
            jittered_interval(
                config.agent.config_poll_interval_seconds,
                &identity.agent_id,
            )
        };
        tokio::time::sleep(interval).await;
        refresh_remote_config_with_current_session(
            "poll",
            &client,
            &control,
            &identity,
            &shared_config,
            &config_session_id,
            &state_dir,
        )
        .await;
    }
}

async fn control_websocket_loop(
    client: reqwest::Client,
    control: ControlClientConfig,
    identity: AgentIdentity,
    shared_config: Arc<RwLock<AgentRuntimeConfig>>,
    config_session_id: ConfigSessionId,
    state_dir: PathBuf,
) {
    let mut backoff = Duration::from_secs(5);
    loop {
        match run_control_websocket(
            &client,
            &control,
            &identity,
            &shared_config,
            &config_session_id,
            &state_dir,
        )
        .await
        {
            Ok(()) => {
                tracing::warn!("talia_control_websocket_closed");
                backoff = Duration::from_secs(5);
            },
            Err(error) => {
                tracing::warn!(error = %error, "talia_control_websocket_failed");
            },
        }
        *config_session_id.write().await = None;
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(60));
    }
}

async fn run_control_websocket(
    client: &reqwest::Client,
    control: &ControlClientConfig,
    identity: &AgentIdentity,
    shared_config: &Arc<RwLock<AgentRuntimeConfig>>,
    config_session_id: &ConfigSessionId,
    state_dir: &Path,
) -> Result<()> {
    let mut request = control
        .ws_url
        .as_str()
        .into_client_request()
        .context("failed to build control WebSocket request")?;
    let authorization = HeaderValue::from_str(&format!("Bearer {}", control.token))
        .context("failed to build control authorization header")?;
    request.headers_mut().insert("authorization", authorization);
    if let Some(token) = control
        .agent_config_token
        .as_deref()
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        let token =
            HeaderValue::from_str(token).context("failed to build control config token header")?;
        request
            .headers_mut()
            .insert(AGENT_CONFIG_TOKEN_HEADER, token);
    }
    let (stream, _) = connect_async(request)
        .await
        .context("failed to connect control WebSocket")?;
    tracing::info!("talia_control_websocket_connected");
    let (mut writer, mut reader) = stream.split();
    let hello = AgentControlMessage::Hello {
        agent_id: identity.agent_id.clone(),
        hostname: identity.hostname.clone(),
        agent_version: AGENT_VERSION.to_string(),
        boot_id: identity.boot_id.clone(),
    };
    writer
        .send(Message::Text(serde_json::to_string(&hello)?.into()))
        .await
        .context("failed to send control hello")?;

    send_control_heartbeat(&mut writer, identity, shared_config).await?;
    loop {
        let heartbeat_delay = {
            let config = shared_config.read().await;
            tokio::time::sleep(Duration::from_secs(config.agent.heartbeat_interval_seconds))
        };
        tokio::pin!(heartbeat_delay);
        tokio::select! {
            _ = &mut heartbeat_delay => {
                send_control_heartbeat(&mut writer, identity, shared_config).await?;
            }
            message = reader.next() => {
                let Some(message) = message else {
                    return Ok(());
                };
                let message = message.context("failed to read control WebSocket message")?;
                if let Message::Text(text) = message {
                    handle_server_message(
                        client,
                        control,
                        identity,
                        shared_config,
                        config_session_id,
                        state_dir,
                        text.as_str(),
                    )
                    .await?;
                }
            }
        }
    }
}

async fn send_control_heartbeat(
    writer: &mut futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    identity: &AgentIdentity,
    shared_config: &Arc<RwLock<AgentRuntimeConfig>>,
) -> Result<()> {
    let config = shared_config.read().await.clone();
    let heartbeat_message = AgentControlMessage::Heartbeat {
        agent_id: identity.agent_id.clone(),
        hostname: identity.hostname.clone(),
        agent_version: AGENT_VERSION.to_string(),
        config_version: config.version.clone(),
        enabled_collectors: enabled_collectors(&config),
    };
    writer
        .send(Message::Text(
            serde_json::to_string(&heartbeat_message)?.into(),
        ))
        .await
        .context("failed to send control heartbeat")
}

async fn handle_server_message(
    client: &reqwest::Client,
    control: &ControlClientConfig,
    identity: &AgentIdentity,
    shared_config: &Arc<RwLock<AgentRuntimeConfig>>,
    config_session_id: &ConfigSessionId,
    state_dir: &Path,
    text: &str,
) -> Result<()> {
    match serde_json::from_str::<ServerControlMessage>(text)
        .context("failed to parse server control message")?
    {
        ServerControlMessage::HelloAck {
            server_time_unix_seconds,
            config_session_id: server_config_session_id,
        } => {
            tracing::info!(server_time_unix_seconds, "talia_control_hello_ack");
            *config_session_id.write().await = Some(server_config_session_id.clone());
            refresh_remote_config(
                "hello_ack",
                client,
                control,
                identity,
                shared_config,
                &server_config_session_id,
                state_dir,
            )
            .await;
        },
        ServerControlMessage::ConfigChanged { version } => {
            tracing::info!(version, "talia_control_config_changed");
            refresh_remote_config_with_current_session(
                "websocket",
                client,
                control,
                identity,
                shared_config,
                config_session_id,
                state_dir,
            )
            .await;
        },
    }
    Ok(())
}

async fn refresh_remote_config_with_current_session(
    reason: &str,
    client: &reqwest::Client,
    control: &ControlClientConfig,
    identity: &AgentIdentity,
    shared_config: &Arc<RwLock<AgentRuntimeConfig>>,
    config_session_id: &ConfigSessionId,
    state_dir: &Path,
) {
    let Some(config_session_id) = config_session_id.read().await.clone() else {
        tracing::debug!(reason, "talia_remote_config_session_missing");
        return;
    };
    refresh_remote_config(
        reason,
        client,
        control,
        identity,
        shared_config,
        &config_session_id,
        state_dir,
    )
    .await;
}

async fn refresh_remote_config(
    reason: &str,
    client: &reqwest::Client,
    control: &ControlClientConfig,
    identity: &AgentIdentity,
    shared_config: &Arc<RwLock<AgentRuntimeConfig>>,
    config_session_id: &str,
    state_dir: &Path,
) {
    match fetch_remote_config(client, control, identity, config_session_id).await {
        Ok(config) => {
            if let Err(error) = config.validate() {
                tracing::warn!(reason, error = %error, "talia_remote_config_invalid");
                return;
            }
            if let Err(error) = save_last_known_config(state_dir, &config) {
                tracing::warn!(reason, error = %error, "talia_last_config_save_failed");
            }
            let version = config.version.clone();
            *shared_config.write().await = config;
            tracing::info!(reason, version, "talia_remote_config_applied");
        },
        Err(error) => {
            tracing::warn!(reason, error = %error, "talia_remote_config_fetch_failed");
        },
    }
}

async fn fetch_remote_config(
    client: &reqwest::Client,
    control: &ControlClientConfig,
    identity: &AgentIdentity,
    config_session_id: &str,
) -> Result<AgentRuntimeConfig> {
    let url = format!("{}/agent/config", control.http_url.trim_end_matches('/'));
    let mut request = client
        .get(url)
        .bearer_auth(&control.token)
        .header(AGENT_ID_HEADER, identity.agent_id.as_str())
        .header(CONFIG_SESSION_ID_HEADER, config_session_id);
    if let Some(token) = control
        .agent_config_token
        .as_deref()
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        request = request.header(AGENT_CONFIG_TOKEN_HEADER, token);
    }
    let response = request
        .send()
        .await
        .map_err(reqwest::Error::without_url)
        .context("config request failed")?;
    if response.status() == StatusCode::UNAUTHORIZED {
        anyhow::bail!("control server rejected bearer token");
    }
    let response = response
        .error_for_status()
        .map_err(reqwest::Error::without_url)
        .context("control server returned config error")?;
    response
        .json::<AgentRuntimeConfig>()
        .await
        .map_err(reqwest::Error::without_url)
        .context("failed to decode control config response")
}

fn cpu_attributes(config_version: &str, cpu: &str) -> Vec<KeyValue> {
    vec![
        KeyValue::new("system.cpu.logical_number", cpu.to_string()),
        KeyValue::new(CONFIG_VERSION_ATTRIBUTE, config_version.to_string()),
    ]
}

fn memory_attributes(config_version: &str) -> Vec<KeyValue> {
    vec![KeyValue::new(
        CONFIG_VERSION_ATTRIBUTE,
        config_version.to_string(),
    )]
}

fn network_attributes(config_version: &str, sample: &NetworkSample) -> Vec<KeyValue> {
    vec![
        KeyValue::new("network.io.direction", sample.direction.as_str()),
        KeyValue::new(CONFIG_VERSION_ATTRIBUTE, config_version.to_string()),
    ]
}

fn disk_io_attributes(config_version: &str, sample: &DiskIoSample) -> Vec<KeyValue> {
    vec![
        KeyValue::new("disk.io.direction", sample.direction.as_str()),
        KeyValue::new(CONFIG_VERSION_ATTRIBUTE, config_version.to_string()),
    ]
}

fn enabled_collectors(config: &AgentRuntimeConfig) -> Vec<String> {
    let mut collectors = Vec::new();
    if config.storage.enabled {
        collectors.push("storage".to_string());
    }
    if config.memory.enabled {
        collectors.push("memory".to_string());
    }
    if config.cpu.enabled {
        collectors.push("cpu".to_string());
    }
    if config.network.enabled {
        collectors.push("network".to_string());
    }
    if config.disk_io.enabled {
        collectors.push("disk_io".to_string());
    }
    collectors
}

fn optional_env_secret(name: &'static str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).with_context(|| format!("{name} must be valid unicode")),
    }
}

fn last_known_config_path(state_dir: &Path) -> PathBuf {
    state_dir.join(LAST_KNOWN_CONFIG_FILE)
}

fn load_last_known_config(state_dir: &Path) -> Option<AgentRuntimeConfig> {
    let path = last_known_config_path(state_dir);
    let contents = fs::read_to_string(path).ok()?;
    let config: AgentRuntimeConfig = serde_json::from_str(&contents).ok()?;
    config.validate().ok()?;
    Some(config)
}

fn save_last_known_config(state_dir: &Path, config: &AgentRuntimeConfig) -> Result<()> {
    fs::create_dir_all(state_dir)
        .with_context(|| format!("failed to create state directory {}", state_dir.display()))?;
    fs::write(
        last_known_config_path(state_dir),
        serde_json::to_vec_pretty(config).context("failed to serialize last known config")?,
    )
    .context("failed to write last known config")
}

fn jittered_interval(base_seconds: u64, agent_id: &str) -> Duration {
    let spread = base_seconds / 10;
    if spread == 0 {
        return Duration::from_secs(base_seconds);
    }
    let hash = fnv1a64(agent_id.as_bytes());
    let offset = hash % (spread * 2 + 1);
    Duration::from_secs(base_seconds - spread + offset)
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
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

        // When: the agent builds the resource attached to every metric.
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
        // Given: a runtime configuration version.
        let config_version = "config-v1";

        // When: the agent builds attributes shared by memory metrics.
        let attributes = memory_attributes(config_version);

        // Then: the emitted attribute uses Talia's neutral key and value.
        assert_eq!(attributes.len(), 1);
        assert_eq!(attributes[0].key.as_str(), "talia.config.version");
        assert_eq!(attributes[0].value.to_string(), config_version);
    }

    #[test]
    fn agent_cli_does_not_accept_secret_flags() {
        // Given: the scenario for agent cli does not accept secret flags is prepared.
        // When: the behavior under test runs.
        use clap::CommandFactory;
        let command = Args::command();
        let long_flags = command
            .get_arguments()
            .filter_map(|argument| argument.get_long())
            .map(str::to_string)
            .collect::<Vec<_>>();

        // Then: the assertions confirm that agent cli does not accept secret flags.
        assert!(!long_flags.contains(&"control-token".to_string()));
        assert!(!long_flags.contains(&"agent-config-token".to_string()));
    }

    #[test]
    fn jitter_stays_within_ten_percent() {
        // Given: the scenario for jitter stays within ten percent is prepared.
        let base = 900;

        // When: the behavior under test runs.
        let interval = jittered_interval(base, "agent-1");

        // Then: the assertions confirm that jitter stays within ten percent.
        assert!(interval >= Duration::from_secs(810));
        assert!(interval <= Duration::from_secs(990));
    }

    #[test]
    fn enabled_collectors_reports_storage_only_when_enabled() {
        // Given: the scenario for enabled collectors reports storage only when enabled is prepared.
        let mut config = AgentRuntimeConfig::default();
        config.storage.enabled = false;
        config.memory.enabled = false;
        config.cpu.enabled = false;

        // When: the behavior under test runs.
        let disabled = enabled_collectors(&config);
        config.storage.enabled = true;
        config.memory.enabled = false;
        let enabled = enabled_collectors(&config);

        // Then: the assertions confirm that enabled collectors reports storage only when enabled.
        assert!(disabled.is_empty());
        assert_eq!(enabled, ["storage"]);
    }

    #[test]
    fn enabled_collectors_reports_cpu_when_enabled() {
        // Given: the scenario for enabled collectors reports cpu when enabled is prepared.
        let mut config = AgentRuntimeConfig::default();
        config.storage.enabled = false;
        config.memory.enabled = false;
        config.cpu.enabled = true;

        // When: the behavior under test runs.
        let enabled = enabled_collectors(&config);

        // Then: the assertions confirm that enabled collectors reports cpu when enabled.
        assert_eq!(enabled, ["cpu"]);
    }

    #[test]
    fn enabled_collectors_reports_memory_when_enabled() {
        // Given: the scenario for enabled collectors reports memory when enabled is prepared.
        let mut config = AgentRuntimeConfig::default();
        config.storage.enabled = false;
        config.memory.enabled = true;

        // When: the behavior under test runs.
        let enabled = enabled_collectors(&config);

        // Then: the assertions confirm that enabled collectors reports memory when enabled.
        assert_eq!(enabled, ["memory"]);
    }

    #[test]
    fn enabled_collectors_reports_network_when_enabled() {
        // Given: the scenario for enabled collectors reports network when enabled is prepared.
        let mut config = AgentRuntimeConfig::default();
        config.storage.enabled = false;
        config.memory.enabled = false;
        config.network.enabled = true;

        // When: the behavior under test runs.
        let enabled = enabled_collectors(&config);

        // Then: the assertions confirm that enabled collectors reports network when enabled.
        assert_eq!(enabled, ["network"]);
    }

    #[test]
    fn enabled_collectors_reports_disk_io_when_enabled() {
        // Given: the scenario for enabled collectors reports disk io when enabled is prepared.
        let mut config = AgentRuntimeConfig::default();
        config.storage.enabled = false;
        config.memory.enabled = false;
        config.disk_io.enabled = true;

        // When: the behavior under test runs.
        let enabled = enabled_collectors(&config);

        // Then: the assertions confirm that enabled collectors reports disk io when enabled.
        assert_eq!(enabled, ["disk_io"]);
    }
}
