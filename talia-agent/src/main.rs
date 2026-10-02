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
use clap::Subcommand;
use futures_util::SinkExt;
use futures_util::StreamExt;
use reqwest::StatusCode;
use talia_agent::identity;
use talia_agent::identity::AgentIdentity;
use talia_agent::processors::NameFilter;
use talia_agent::runner;
use talia_agent::sinks::OtlpSink;
use talia_agent::sinks::StdoutSink;
use talia_core::config::AgentBootstrapConfig;
use talia_core::config::AgentRuntimeConfig;
use talia_core::control::AGENT_CONFIG_TOKEN_HEADER;
use talia_core::control::AGENT_ID_HEADER;
use talia_core::control::AgentControlMessage;
use talia_core::control::CONFIG_SESSION_ID_HEADER;
use talia_core::control::ServerControlMessage;
use talia_core::pipeline::Processor;
use talia_core::pipeline::Sink;
use talia_core::pipeline::process_sample;
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tracing_subscriber::EnvFilter;

const AGENT_VERSION: &str = env!("CARGO_PKG_VERSION");
const LAST_KNOWN_CONFIG_FILE: &str = "last-config.json";

#[derive(Parser)]
#[command(
    about = "Talia host monitoring agent",
    version,
    propagate_version = true
)]
struct Args {
    /// Local TOML file for agent bootstrap and fallback provider settings.
    #[arg(long, global = true, env = "TALIA_AGENT_CONFIG")]
    config: Option<PathBuf>,
    #[arg(long, default_value = "info")]
    log_filter: String,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the agent service (default when omitted).
    Run,
    /// Work with data providers.
    Providers {
        #[command(subcommand)]
        action: ProvidersAction,
    },
}

#[derive(Subcommand)]
enum ProvidersAction {
    /// List available provider names.
    List,
    /// Print local fallback provider settings as TOML (all when omitted).
    Show {
        /// Provider name, e.g. `tcp`.
        provider: Option<String>,
        /// Override a provider setting, e.g. `--set interval_seconds=5`.
        #[arg(long, value_name = "KEY=VALUE")]
        set: Vec<String>,
    },
    /// Collect one provider's samples to stdout for a fixed duration.
    Query {
        /// Provider name (see `providers list`).
        provider: String,
        /// How long to collect, e.g. `30s`, `5m`.
        #[arg(long, value_parser = humantime::parse_duration)]
        r#for: Duration,
        /// Collection interval; defaults to the provider's local setting.
        #[arg(long, value_parser = humantime::parse_duration)]
        interval: Option<Duration>,
        /// Only keep samples whose name starts with one of these prefixes.
        #[arg(long)]
        keep: Vec<String>,
        /// Override a provider setting, e.g. `--set pod_discovery.cri_socket=all`.
        #[arg(long, value_name = "KEY=VALUE")]
        set: Vec<String>,
    },
}

#[derive(Clone)]
struct ControlClientConfig {
    http_url: String,
    ws_url: String,
    token: String,
    agent_config_token: Option<String>,
}

type ConfigSessionId = Arc<RwLock<Option<String>>>;

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

    // Provider CLI modes use local fallback settings without starting the service.
    match &args.command {
        Some(Command::Providers {
            action: ProvidersAction::List,
        }) => {
            for spec in runner::collector_specs() {
                println!("{}", spec.name);
            }
            return Ok(());
        },
        Some(Command::Providers {
            action: ProvidersAction::Show { provider, set },
        }) => return show_provider_config(provider.as_deref(), args.config.as_deref(), set),
        Some(Command::Providers {
            action:
                ProvidersAction::Query {
                    provider,
                    r#for,
                    interval,
                    keep,
                    set,
                },
        }) => {
            let config = with_provider_overrides(
                load_local_runtime_config(args.config.as_deref())?,
                provider,
                set,
            )?;
            return query_provider(provider, *r#for, *interval, keep.clone(), &config).await;
        },
        Some(Command::Run) | None => {},
    }

    let mut bootstrap = load_bootstrap_config(args.config.as_deref())?;
    if let Some(token) = optional_env_secret("TALIA_CONTROL_TOKEN")? {
        bootstrap.control_token = Some(token);
    }
    if let Some(token) = optional_env_secret("TALIA_AGENT_CONFIG_TOKEN")? {
        bootstrap.agent_config_token = Some(token);
    }
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
    let sink: Arc<dyn Sink> = Arc::new(OtlpSink::new(&bootstrap.otlp_endpoint, &identity)?);
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

    let sinks: Vec<Arc<dyn Sink>> = vec![sink];
    // No processors are configured yet; the pipeline still runs every sample
    // through the (empty) processor chain.
    let processors: Vec<Arc<dyn Processor>> = Vec::new();
    for spec in runner::collector_specs() {
        tokio::spawn(runner::run_collector(
            spec,
            processors.clone(),
            sinks.clone(),
            Arc::clone(&shared_config),
        ));
    }

    tokio::signal::ctrl_c()
        .await
        .context("failed to listen for shutdown signal")?;
    tracing::info!("talia_agent_shutdown_signal");
    Ok(())
}

fn load_bootstrap_config(path: Option<&Path>) -> Result<AgentBootstrapConfig> {
    let bootstrap = match path {
        Some(path) => AgentBootstrapConfig::load(path)
            .with_context(|| format!("failed to load agent config {}", path.display()))?,
        None => AgentBootstrapConfig::default(),
    };
    bootstrap
        .validate()
        .context("agent bootstrap config is invalid")?;
    Ok(bootstrap)
}

fn load_local_runtime_config(path: Option<&Path>) -> Result<AgentRuntimeConfig> {
    Ok(load_bootstrap_config(path)?.fallback_runtime_config())
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

fn enabled_collectors(config: &AgentRuntimeConfig) -> Vec<String> {
    runner::collector_specs()
        .iter()
        .filter(|spec| (spec.schedule)(config).0)
        .map(|spec| spec.name.to_string())
        .collect()
}

/// Looks up a collector spec by provider name.
fn collector_spec(name: &str) -> Result<runner::CollectorSpec> {
    runner::collector_specs()
        .into_iter()
        .find(|spec| spec.name == name)
        .with_context(|| {
            let known: Vec<_> = runner::collector_specs()
                .iter()
                .map(|spec| spec.name)
                .collect();
            format!(
                "unknown provider '{name}'; known providers: {}",
                known.join(", ")
            )
        })
}

/// Applies CLI settings only to the selected provider (or its shared inputs).
fn with_provider_overrides(
    config: AgentRuntimeConfig,
    provider: &str,
    overrides: &[String],
) -> Result<AgentRuntimeConfig> {
    collector_spec(provider)?;
    if overrides.is_empty() {
        return Ok(config);
    }
    let mut value = serde_json::to_value(config).context("failed to serialize runtime config")?;
    for assignment in overrides {
        let (key, raw) = assignment
            .split_once('=')
            .with_context(|| format!("expected KEY=VALUE after --set, got '{assignment}'"))?;
        let key = key.trim();
        let raw = raw.trim();
        anyhow::ensure!(
            !key.is_empty() && !raw.is_empty(),
            "invalid --set '{assignment}'"
        );

        let mut fields: Vec<&str> = key.split('.').collect();
        let first = fields.first().copied();
        let section =
            if first == Some(provider) || (provider == "tcp" && first == Some("pod_discovery")) {
                fields.remove(0)
            } else {
                provider
            };
        anyhow::ensure!(
            !fields.is_empty() && fields.iter().all(|field| !field.is_empty()),
            "invalid setting name '{key}'"
        );
        let mut target = value
            .get_mut(section)
            .with_context(|| format!("unknown provider config section '{section}'"))?;
        for field in fields {
            let table = target
                .as_object_mut()
                .with_context(|| format!("setting '{key}' is not a table"))?;
            let available = table.keys().cloned().collect::<Vec<_>>().join(", ");
            target = table.get_mut(field).with_context(|| {
                format!(
                    "unknown setting '{key}' for provider '{provider}'; available in [{section}]: {available}"
                )
            })?;
        }
        *target = if target.is_string() {
            let string = if raw.starts_with('"') {
                serde_json::from_str::<String>(raw)
                    .with_context(|| format!("invalid quoted value for '{key}'"))?
            } else {
                raw.to_string()
            };
            serde_json::Value::String(string)
        } else {
            serde_json::from_str(raw)
                .with_context(|| format!("invalid value for '{key}': expected a JSON literal"))?
        };
    }
    let config: AgentRuntimeConfig =
        serde_json::from_value(value).context("invalid provider settings from --set")?;
    config
        .validate()
        .context("provider settings from --set are invalid")?;
    Ok(config)
}

/// Prints provider settings from the local bootstrap file, including defaults.
fn show_provider_config(name: Option<&str>, path: Option<&Path>, set: &[String]) -> Result<()> {
    anyhow::ensure!(
        name.is_some() || set.is_empty(),
        "--set requires a provider name"
    );
    let mut config = load_local_runtime_config(path)?;
    if let Some(name) = name {
        config = with_provider_overrides(config, name, set)?;
    }
    let names: Vec<&str> = match name {
        Some(name) => vec![collector_spec(name)?.name],
        None => runner::collector_specs()
            .iter()
            .map(|spec| spec.name)
            .collect(),
    };
    match path {
        Some(path) => println!(
            "# Local fallback settings from {} (including defaults); a running agent may use cached or remote settings.",
            path.display()
        ),
        None => println!(
            "# Built-in local fallback settings; a running agent may use cached or remote settings."
        ),
    }
    if !set.is_empty() {
        println!("# Command-line --set values applied.");
    }
    for name in names {
        let body = match name {
            "storage" => toml::to_string_pretty(&config.storage)?,
            "memory" => toml::to_string_pretty(&config.memory)?,
            "cpu" => toml::to_string_pretty(&config.cpu)?,
            "network" => toml::to_string_pretty(&config.network)?,
            "disk_io" => toml::to_string_pretty(&config.disk_io)?,
            "tcp" => toml::to_string_pretty(&config.tcp)?,
            _ => unreachable!("collector names are validated above"),
        };
        println!("\n[{name}]\n{}", body.trim_end());
        if name == "tcp" {
            let discovery = toml::to_string_pretty(&config.pod_discovery)?;
            println!("\n[pod_discovery]\n{}", discovery.trim_end());
        }
    }
    Ok(())
}

/// Collects one provider's samples to stdout for the given duration,
/// bpftrace-style.
async fn query_provider(
    name: &str,
    duration: Duration,
    interval: Option<Duration>,
    keep: Vec<String>,
    config: &AgentRuntimeConfig,
) -> Result<()> {
    let mut spec = collector_spec(name)?;
    let interval = interval.unwrap_or_else(|| (spec.schedule)(config).1);
    anyhow::ensure!(
        !interval.is_zero(),
        "query interval must be greater than zero"
    );
    let mut provider =
        (spec.factory)(interval).with_context(|| format!("failed to load provider '{name}'"))?;
    provider.reconfigure(config);
    let processors: Vec<Arc<dyn Processor>> = if keep.is_empty() {
        Vec::new()
    } else {
        vec![Arc::new(NameFilter::new(keep))]
    };
    let sink = StdoutSink::new();
    let start = std::time::Instant::now();
    let mut collected = false;
    let mut last_error = None;
    loop {
        if spec.sleep_before_collect {
            tokio::time::sleep(interval).await;
        }
        provider.set_window(interval);
        match provider.collect() {
            Ok(samples) => {
                collected = true;
                for sample in samples {
                    if let Some(sample) = process_sample(&processors, sample) {
                        sink.emit(&sample, "query");
                    }
                }
            },
            Err(error) => {
                tracing::warn!(provider = name, error = %error, "talia_query_collection_failed");
                last_error = Some(error.to_string());
            },
        }
        if start.elapsed() >= duration {
            break;
        }
        if !spec.sleep_before_collect {
            tokio::time::sleep(interval.min(duration.saturating_sub(start.elapsed()))).await;
        }
    }
    if !collected {
        anyhow::bail!(
            "provider '{name}' did not collect successfully: {}",
            last_error.unwrap_or_else(|| "no collection was attempted".to_string())
        );
    }
    Ok(())
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
