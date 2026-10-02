use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use futures_util::SinkExt;
use futures_util::StreamExt;
use reqwest::StatusCode;
use talia_agent::identity::AgentIdentity;
use talia_agent::runner;
use talia_core::config::AgentRuntimeConfig;
use talia_core::control::AGENT_CONFIG_TOKEN_HEADER;
use talia_core::control::AGENT_ID_HEADER;
use talia_core::control::AgentControlMessage;
use talia_core::control::CONFIG_SESSION_ID_HEADER;
use talia_core::control::ServerControlMessage;
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

use crate::local_config::save_last_known_config;

const AGENT_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone)]
pub(crate) struct ControlClientConfig {
    pub(crate) http_url: String,
    pub(crate) ws_url: String,
    pub(crate) token: String,
    pub(crate) agent_config_token: Option<String>,
}

pub(crate) type ConfigSessionId = Arc<RwLock<Option<String>>>;

pub(crate) async fn config_poll_loop(
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

pub(crate) async fn control_websocket_loop(
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
