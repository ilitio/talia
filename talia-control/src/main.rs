//! Runs the Talia monitoring control server for agent configuration and commands.

#![deny(warnings)]
#![allow(
    clippy::disallowed_methods,
    reason = "the CLI reads documented TALIA_* secret variables directly at startup"
)]

mod openapi;

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use anyhow::Result;
use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::extract::WebSocketUpgrade;
use axum::extract::ws::Message;
use axum::extract::ws::WebSocket;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::routing::post;
use clap::Parser;
use futures_util::SinkExt;
use futures_util::StreamExt;
use serde::Serialize;
use talia_core::config::ControlConfigFile;
use talia_core::config::config_source_version;
use talia_core::control::AGENT_CONFIG_TOKEN_HEADER;
use talia_core::control::AGENT_ID_HEADER;
use talia_core::control::AgentControlMessage;
use talia_core::control::CONFIG_SESSION_ID_HEADER;
use talia_core::control::ServerControlMessage;
use tokio::sync::RwLock;
use tokio::sync::broadcast;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[derive(Parser)]
#[command(about = "Talia monitoring control server")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:3501")]
    listen: SocketAddr,
    #[arg(long, env = "TALIA_CONTROL_CONFIG")]
    config: Option<PathBuf>,
    #[arg(long, default_value = "info")]
    log_filter: String,
}

#[derive(Clone)]
struct AppState {
    agent_token: Arc<str>,
    admin_token: Arc<str>,
    config_path: Option<PathBuf>,
    config: Arc<RwLock<LoadedControlConfig>>,
    notifications: broadcast::Sender<ServerControlMessage>,
    agents: Arc<RwLock<BTreeMap<String, ConnectedAgent>>>,
}

#[derive(Clone, Debug)]
struct LoadedControlConfig {
    source_version: String,
    config: ControlConfigFile,
}

#[derive(Clone, Debug, Serialize, utoipa::ToSchema)]
struct ConnectedAgent {
    agent_id: String,
    hostname: String,
    agent_version: String,
    boot_id: Option<String>,
    config_version: Option<String>,
    enabled_collectors: Vec<String>,
    connected_at_unix_seconds: u64,
    last_seen_unix_seconds: u64,
    #[serde(skip_serializing)]
    connection_id: String,
}

struct AgentHello {
    agent_id: String,
    hostname: String,
    agent_version: String,
    boot_id: Option<String>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
struct ReloadResponse {
    version: String,
    notified_agents: usize,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("talia-control failed: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let args = Args::parse();
    let (agent_token, admin_token) = validate_control_tokens(
        required_env_secret("TALIA_CONTROL_TOKEN")?,
        required_env_secret("TALIA_ADMIN_TOKEN")?,
    )?;
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(args.log_filter))
        .init();
    let config =
        load_control_config(args.config.as_deref()).context("failed to load control config")?;
    let (notifications, _) = broadcast::channel(128);
    let state = AppState {
        agent_token: Arc::from(agent_token),
        admin_token: Arc::from(admin_token),
        config_path: args.config,
        config: Arc::new(RwLock::new(config)),
        notifications,
        agents: Arc::new(RwLock::new(BTreeMap::new())),
    };
    let app = app(state);
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("failed to bind {}", args.listen))?;
    tracing::info!(listen = %args.listen, "talia_control_listening");
    axum::serve(listener, app)
        .await
        .context("talia control server failed")
}

fn app(state: AppState) -> Router {
    Router::new()
        .route("/openapi.json", get(openapi::openapi_json))
        .route("/health", get(health))
        .route("/agent/config", get(agent_config))
        .route("/agent/control", get(agent_control))
        .route("/admin/agents", get(admin_agents))
        .route("/admin/config/reload", post(admin_config_reload))
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

async fn agent_config(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !authorized(&headers, &state.agent_token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(agent_id) = header_value(&headers, AGENT_ID_HEADER).map(ToString::to_string) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(config_session_id) =
        header_value(&headers, CONFIG_SESSION_ID_HEADER).map(ToString::to_string)
    else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let agent_config_token =
        header_value(&headers, AGENT_CONFIG_TOKEN_HEADER).map(ToString::to_string);
    let Some(agent) = connected_agent_for_config(&state, &agent_id, &config_session_id).await
    else {
        tracing::warn!("talia_config_agent_identity_mismatch");
        return StatusCode::FORBIDDEN.into_response();
    };
    let config = state.config.read().await;
    if !agent_has_config_credential(
        &config.config,
        &agent.agent_id,
        &agent.hostname,
        agent_config_token.as_deref(),
    ) {
        tracing::warn!("talia_config_agent_credential_missing");
        return StatusCode::FORBIDDEN.into_response();
    }
    match config.config.runtime_config_for(&agent.hostname) {
        Ok(runtime) => {
            tracing::info!(
                config_version = %runtime.version,
                "talia_config_served"
            );
            Json(runtime).into_response()
        },
        Err(error) => {
            tracing::warn!(
                error = %error,
                "talia_config_invalid"
            );
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        },
    }
}

async fn connected_agent_for_config(
    state: &AppState,
    agent_id: &str,
    config_session_id: &str,
) -> Option<ConnectedAgent> {
    state
        .agents
        .read()
        .await
        .get(agent_id)
        .filter(|agent| agent.connection_id == config_session_id)
        .cloned()
}

async fn agent_control(
    State(state): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    if !authorized(&headers, &state.agent_token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let agent_config_token =
        header_value(&headers, AGENT_CONFIG_TOKEN_HEADER).map(ToString::to_string);
    ws.on_upgrade(move |socket| async move {
        if let Err(error) = handle_agent_socket(socket, state, agent_config_token).await {
            tracing::warn!(error = %error, "talia_agent_socket_failed");
        }
    })
}

async fn admin_agents(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !authorized(&headers, &state.admin_token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let agents = state
        .agents
        .read()
        .await
        .values()
        .cloned()
        .collect::<Vec<_>>();
    Json(agents).into_response()
}

async fn admin_config_reload(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !authorized(&headers, &state.admin_token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match load_control_config(state.config_path.as_deref()) {
        Ok(config) => {
            let version = config.source_version.clone();
            *state.config.write().await = config;
            let notified_agents = state.agents.read().await.len();
            let _ = state
                .notifications
                .send(ServerControlMessage::ConfigChanged {
                    version: version.clone(),
                });
            tracing::info!(version, notified_agents, "talia_control_config_reloaded");
            Json(ReloadResponse {
                version,
                notified_agents,
            })
            .into_response()
        },
        Err(error) => {
            tracing::warn!(error = %error, "talia_control_config_reload_failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        },
    }
}

async fn handle_agent_socket(
    socket: WebSocket,
    state: AppState,
    agent_config_token: Option<String>,
) -> Result<()> {
    let connection_id = Uuid::new_v4().to_string();
    let mut current_agent_id: Option<String> = None;
    let result = run_agent_socket(
        socket,
        &state,
        &connection_id,
        &mut current_agent_id,
        agent_config_token.as_deref(),
    )
    .await;
    remove_agent(&state, &connection_id, &current_agent_id).await;
    result
}

async fn run_agent_socket(
    socket: WebSocket,
    state: &AppState,
    connection_id: &str,
    current_agent_id: &mut Option<String>,
    agent_config_token: Option<&str>,
) -> Result<()> {
    let mut notifications = state.notifications.subscribe();
    let (mut sender, mut receiver) = socket.split();
    loop {
        tokio::select! {
            notification = notifications.recv() => {
                match notification {
                    Ok(message) => {
                        sender
                            .send(Message::Text(serde_json::to_string(&message)?.into()))
                            .await
                            .context("failed to send control notification")?;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "talia_control_notifications_lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
            message = receiver.next() => {
                let Some(message) = message else {
                    return Ok(());
                };
                let message = message.context("failed to read WebSocket message")?;
                match message {
                    Message::Text(text) => {
                        handle_agent_message(
                            state,
                            &mut sender,
                            connection_id,
                            current_agent_id,
                            agent_config_token,
                            text.as_str(),
                        )
                        .await?;
                    }
                    Message::Close(_) => {
                        return Ok(());
                    }
                    Message::Ping(_) | Message::Pong(_) | Message::Binary(_) => {}
                }
            }
        }
    }
}

async fn handle_agent_message(
    state: &AppState,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    connection_id: &str,
    current_agent_id: &mut Option<String>,
    agent_config_token: Option<&str>,
    text: &str,
) -> Result<()> {
    match serde_json::from_str::<AgentControlMessage>(text)
        .context("failed to parse agent control message")?
    {
        AgentControlMessage::Hello {
            agent_id,
            hostname,
            agent_version,
            boot_id,
        } => {
            let Some(now) = register_agent_connection(
                state,
                connection_id,
                current_agent_id,
                agent_config_token,
                AgentHello {
                    agent_id,
                    hostname,
                    agent_version,
                    boot_id,
                },
            )
            .await
            else {
                sender
                    .close()
                    .await
                    .context("failed to close rejected agent socket")?;
                return Ok(());
            };
            sender
                .send(Message::Text(
                    serde_json::to_string(&ServerControlMessage::HelloAck {
                        server_time_unix_seconds: now,
                        config_session_id: connection_id.to_string(),
                    })?
                    .into(),
                ))
                .await
                .context("failed to send hello ack")?;
        },
        AgentControlMessage::Heartbeat {
            agent_id,
            hostname,
            agent_version,
            config_version,
            enabled_collectors,
        } => {
            if current_agent_id.as_deref() != Some(agent_id.as_str()) {
                tracing::warn!(connection_id, "talia_unbound_agent_heartbeat_rejected");
                return Ok(());
            }
            let now = unix_now();
            let mut agents = state.agents.write().await;
            if let Some(agent) = agents.get_mut(&agent_id) {
                if agent.connection_id != connection_id {
                    tracing::warn!(connection_id, "talia_stale_agent_heartbeat_ignored");
                    return Ok(());
                }
                agent.hostname = hostname;
                agent.agent_version = agent_version;
                agent.config_version = Some(config_version);
                agent.enabled_collectors = enabled_collectors;
                agent.last_seen_unix_seconds = now;
            } else {
                tracing::warn!(connection_id, "talia_unknown_agent_heartbeat_rejected");
                return Ok(());
            }
            tracing::info!(connection_id, "talia_agent_heartbeat");
        },
    }
    Ok(())
}

async fn register_agent_connection(
    state: &AppState,
    connection_id: &str,
    current_agent_id: &mut Option<String>,
    agent_config_token: Option<&str>,
    hello: AgentHello,
) -> Option<u64> {
    if !agent_hello_authorized(state, &hello, agent_config_token).await {
        tracing::warn!(connection_id, "talia_agent_hello_credential_missing");
        return None;
    }

    let mut agents = state.agents.write().await;
    if agents
        .get(&hello.agent_id)
        .is_some_and(|agent| agent.connection_id != connection_id)
    {
        tracing::warn!(connection_id, "talia_duplicate_agent_hello_rejected");
        return None;
    }

    if let Some(previous_agent_id) = current_agent_id.as_deref()
        && previous_agent_id != hello.agent_id
        && agents
            .get(previous_agent_id)
            .is_some_and(|agent| agent.connection_id == connection_id)
    {
        agents.remove(previous_agent_id);
        tracing::info!(connection_id, "talia_agent_disconnected");
    }

    let now = unix_now();
    *current_agent_id = Some(hello.agent_id.clone());
    agents.insert(
        hello.agent_id.clone(),
        ConnectedAgent {
            agent_id: hello.agent_id,
            hostname: hello.hostname.clone(),
            agent_version: hello.agent_version,
            boot_id: hello.boot_id,
            config_version: None,
            enabled_collectors: Vec::new(),
            connected_at_unix_seconds: now,
            last_seen_unix_seconds: now,
            connection_id: connection_id.to_string(),
        },
    );
    tracing::info!(connection_id, "talia_agent_connected");
    Some(now)
}

async fn agent_hello_authorized(
    state: &AppState,
    hello: &AgentHello,
    agent_config_token: Option<&str>,
) -> bool {
    let config = state.config.read().await;
    agent_has_config_credential(
        &config.config,
        &hello.agent_id,
        &hello.hostname,
        agent_config_token,
    )
}

fn agent_has_config_credential(
    config: &ControlConfigFile,
    agent_id: &str,
    hostname: &str,
    agent_config_token: Option<&str>,
) -> bool {
    config.agent_is_enrolled_for_hostname(agent_id, hostname)
        && agent_config_token
            .is_some_and(|token| config.agent_config_token_matches(agent_id, token))
}

async fn remove_agent(state: &AppState, connection_id: &str, current_agent_id: &Option<String>) {
    if let Some(agent_id) = current_agent_id {
        let mut agents = state.agents.write().await;
        let should_remove = agents
            .get(agent_id)
            .is_some_and(|agent| agent.connection_id == connection_id);
        if should_remove {
            agents.remove(agent_id);
            tracing::info!(connection_id, "talia_agent_disconnected");
        }
    }
}

fn load_control_config(path: Option<&Path>) -> Result<LoadedControlConfig> {
    match path {
        Some(path) => {
            let contents = fs::read_to_string(path)
                .with_context(|| format!("failed to read control config {}", path.display()))?;
            let config: ControlConfigFile = toml::from_str(&contents)
                .with_context(|| format!("failed to parse control config {}", path.display()))?;
            validate_runtime_configs(&config)?;
            Ok(LoadedControlConfig {
                source_version: config_source_version(&contents),
                config,
            })
        },
        None => {
            let config = ControlConfigFile::default();
            validate_runtime_configs(&config)?;
            Ok(LoadedControlConfig {
                source_version: config_source_version(""),
                config,
            })
        },
    }
}

fn validate_runtime_configs(config: &ControlConfigFile) -> Result<()> {
    config
        .runtime_config_for("")
        .context("invalid default runtime config")?;
    for hostname in config.hosts.keys() {
        config
            .runtime_config_for(hostname)
            .with_context(|| format!("invalid runtime config for host {hostname}"))?;
    }
    Ok(())
}

fn authorized(headers: &HeaderMap, token: &str) -> bool {
    let Some(value) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    value
        .strip_prefix("Bearer ")
        .is_some_and(|candidate| talia_core::secret::matches(token, candidate))
}

fn header_value<'a>(headers: &'a HeaderMap, name: &'static str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn required_env_secret(name: &'static str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} must be set"))
}

fn validate_control_tokens(agent_token: String, admin_token: String) -> Result<(String, String)> {
    let agent_token = agent_token.trim().to_string();
    let admin_token = admin_token.trim().to_string();
    if agent_token.is_empty() {
        anyhow::bail!("TALIA_CONTROL_TOKEN must not be empty");
    }
    if admin_token.is_empty() {
        anyhow::bail!("TALIA_ADMIN_TOKEN must not be empty");
    }
    if agent_token == admin_token {
        anyhow::bail!("TALIA_ADMIN_TOKEN must be distinct from TALIA_CONTROL_TOKEN");
    }
    Ok((agent_token, admin_token))
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body;
    use axum::http::HeaderValue;
    use axum::http::Request;
    use talia_core::config::AgentRuntimeConfig;
    use tower::ServiceExt;

    #[tokio::test]
    async fn openapi_endpoint_serves_contract() {
        // Given: the production router and an HTTP request for its OpenAPI document.
        let request = Request::builder()
            .uri("/openapi.json")
            .body(body::Body::empty())
            .expect("OpenAPI request should build");

        // When: the request is served through the router.
        let response = app(test_state(""))
            .oneshot(request)
            .await
            .expect("OpenAPI request should succeed");
        let status = response.status();
        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("OpenAPI response body should be readable");
        let document: serde_json::Value =
            serde_json::from_slice(&body).expect("OpenAPI response should be JSON");

        // Then: the endpoint returns the Talia control contract.
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            document.pointer("/info/title"),
            Some(&serde_json::json!("Talia Control API"))
        );
        assert!(document.pointer("/paths/~1agent~1control/get").is_some());
    }

    #[test]
    fn control_cli_does_not_accept_secret_flags() {
        // Given: the scenario for control cli does not accept secret flags is prepared.
        // When: the behavior under test runs.
        use clap::CommandFactory;
        let command = Args::command();
        let long_flags = command
            .get_arguments()
            .filter_map(|argument| argument.get_long())
            .map(str::to_string)
            .collect::<Vec<_>>();

        // Then: the assertions confirm that control cli does not accept secret flags.
        assert!(!long_flags.contains(&"agent-token".to_string()));
        assert!(!long_flags.contains(&"admin-token".to_string()));
    }

    #[test]
    fn control_tokens_reject_empty_values() {
        // Given: the scenario for control tokens reject empty values is prepared.
        // When: the behavior under test runs.
        // Then: the assertions confirm that control tokens reject empty values.
        assert!(validate_control_tokens(String::new(), "admin".to_string()).is_err());
        assert!(validate_control_tokens("agent".to_string(), "   ".to_string()).is_err());
    }

    #[test]
    fn control_tokens_must_be_distinct() {
        // Given: the scenario for control tokens must be distinct is prepared.
        // When: the behavior under test runs.
        let result = validate_control_tokens("same".to_string(), "same".to_string());

        // Then: the assertions confirm that control tokens must be distinct.
        assert!(result.is_err());
    }

    #[test]
    fn empty_authorization_token_never_matches() {
        // Given: the scenario for empty authorization token never matches is prepared.
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer "));

        // When: the behavior under test runs.
        // Then: the assertions confirm that empty authorization token never matches.
        assert!(!authorized(&headers, ""));
    }

    #[test]
    fn authorization_token_matches_only_exact_bearer_value() {
        // Given: several authorization headers around the configured token.
        let headers = [
            bearer("control-secret"),
            bearer("control-secre"),
            bearer("control-secret-extra"),
            authorization_header("Basic control-secret"),
            HeaderMap::new(),
        ];

        // When: each request is authorized.
        let results = headers.map(|headers| authorized(&headers, "control-secret"));

        // Then: only the exact bearer token is accepted.
        assert_eq!(results, [true, false, false, false, false]);
    }

    #[tokio::test]
    async fn agent_config_serves_enrolled_agent_default_config() {
        // Given: the scenario for agent config serves enrolled agent default config is prepared.
        let state = test_state(
            r#"
            [agents."agent-1"]
            hostname = "host-1"
            config_token = "agent-secret"

            [defaults.storage]
            interval_seconds = 45
            mounts = ["/"]
            "#,
        );
        connect_test_agent(&state, "agent-1", "host-1").await;

        // When: the behavior under test runs.
        let response = agent_config(
            State(state),
            agent_headers("agent-1", "test-connection", Some("agent-secret")),
        )
        .await;

        // Then: the assertions confirm that agent config serves enrolled agent default config.
        assert_eq!(response.status(), StatusCode::OK);
        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let runtime: AgentRuntimeConfig =
            serde_json::from_slice(&body).expect("runtime config should deserialize");
        assert_eq!(runtime.storage.interval_seconds, 45);
    }

    #[tokio::test]
    async fn agent_config_rejects_unenrolled_agent_default_config() {
        // Given: the scenario for agent config rejects unenrolled agent default config is prepared.
        let state = test_state(
            r#"
            [defaults.storage]
            interval_seconds = 45
            mounts = ["/"]
            "#,
        );
        connect_test_agent(&state, "agent-1", "host-1").await;

        // When: the behavior under test runs.
        let response = agent_config(
            State(state),
            agent_headers("agent-1", "test-connection", Some("agent-secret")),
        )
        .await;

        // Then: the assertions confirm that agent config rejects unenrolled agent default config.
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn agent_config_serves_connected_agent_host_override() {
        // Given: the scenario for agent config serves connected agent host override is prepared.
        let state = test_state(
            r#"
            [agents."agent-1"]
            hostname = "prod-vps-1"
            config_token = "agent-secret"

            [defaults.storage]
            interval_seconds = 60
            mounts = ["/"]

            [hosts."prod-vps-1".storage]
            interval_seconds = 30
            mounts = ["/", "/var/lib/docker"]
            "#,
        );
        connect_test_agent(&state, "agent-1", "prod-vps-1").await;

        // When: the behavior under test runs.
        let response = agent_config(
            State(state),
            agent_headers("agent-1", "test-connection", Some("agent-secret")),
        )
        .await;

        // Then: the assertions confirm that agent config serves connected agent host override.
        assert_eq!(response.status(), StatusCode::OK);
        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let runtime: AgentRuntimeConfig =
            serde_json::from_slice(&body).expect("runtime config should deserialize");
        assert_eq!(runtime.storage.interval_seconds, 30);
        assert_eq!(runtime.storage.mounts, ["/", "/var/lib/docker"]);
    }

    #[tokio::test]
    async fn agent_config_rejects_session_mismatch() {
        // Given: the scenario for agent config rejects session mismatch is prepared.
        let state = test_state(
            r#"
            [agents."agent-1"]
            hostname = "prod-vps-1"
            config_token = "agent-secret"

            [hosts."prod-vps-1".storage]
            interval_seconds = 30
            mounts = ["/", "/var/lib/docker"]
            "#,
        );
        connect_test_agent(&state, "agent-1", "prod-vps-1").await;

        // When: the behavior under test runs.
        let response = agent_config(
            State(state),
            agent_headers("agent-1", "other-connection", Some("agent-secret")),
        )
        .await;

        // Then: the assertions confirm that agent config rejects session mismatch.
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn agent_config_rejects_unenrolled_host_override() {
        // Given: the scenario for agent config rejects unenrolled host override is prepared.
        let state = test_state(
            r#"
            [hosts."prod-vps-1".storage]
            interval_seconds = 30
            mounts = ["/", "/var/lib/docker"]
            "#,
        );
        connect_test_agent(&state, "agent-1", "prod-vps-1").await;

        // When: the behavior under test runs.
        let response = agent_config(
            State(state),
            agent_headers("agent-1", "test-connection", Some("agent-secret")),
        )
        .await;

        // Then: the assertions confirm that agent config rejects unenrolled host override.
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn agent_config_rejects_missing_config_token_for_host_override() {
        // Given: the scenario for agent config rejects missing config token for host override is prepared.
        let state = test_state(
            r#"
            [agents."agent-1"]
            hostname = "prod-vps-1"
            config_token = "agent-secret"

            [hosts."prod-vps-1".storage]
            interval_seconds = 30
            mounts = ["/", "/var/lib/docker"]
            "#,
        );
        connect_test_agent(&state, "agent-1", "prod-vps-1").await;

        // When: the behavior under test runs.
        let response = agent_config(
            State(state),
            agent_headers("agent-1", "test-connection", None),
        )
        .await;

        // Then: the assertions confirm that agent config rejects missing config token for host override.
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn agent_config_rejects_wrong_config_token_for_host_override() {
        // Given: the scenario for agent config rejects wrong config token for host override is prepared.
        let state = test_state(
            r#"
            [agents."agent-1"]
            hostname = "prod-vps-1"
            config_token = "agent-secret"

            [hosts."prod-vps-1".storage]
            interval_seconds = 30
            mounts = ["/", "/var/lib/docker"]
            "#,
        );
        connect_test_agent(&state, "agent-1", "prod-vps-1").await;

        // When: the behavior under test runs.
        let response = agent_config(
            State(state),
            agent_headers("agent-1", "test-connection", Some("wrong-secret")),
        )
        .await;

        // Then: the assertions confirm that agent config rejects wrong config token for host override.
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn agent_hello_rejects_duplicate_agent_id() {
        // Given: the scenario for agent hello rejects duplicate agent id is prepared.
        let state = test_state(
            r#"
            [agents."agent-1"]
            hostname = "host-1"
            config_token = "agent-secret"
            "#,
        );
        connect_test_agent_with_connection(&state, "agent-1", "host-1", "existing-connection")
            .await;
        let mut current_agent_id = None;

        // When: the behavior under test runs.
        let result = register_agent_connection(
            &state,
            "new-connection",
            &mut current_agent_id,
            Some("agent-secret"),
            test_hello("agent-1", "host-1"),
        )
        .await;

        // Then: the assertions confirm that agent hello rejects duplicate agent id.
        assert!(result.is_none());
        assert_eq!(current_agent_id, None);
        let agents = state.agents.read().await;
        assert_eq!(
            agents
                .get("agent-1")
                .map(|agent| agent.connection_id.as_str()),
            Some("existing-connection")
        );
    }

    #[tokio::test]
    async fn agent_hello_rejects_unenrolled_agent_id() {
        // Given: the scenario for agent hello rejects unenrolled agent id is prepared.
        let state = test_state("");
        let mut current_agent_id = None;

        // When: the behavior under test runs.
        let result = register_agent_connection(
            &state,
            "test-connection",
            &mut current_agent_id,
            Some("agent-secret"),
            test_hello("agent-1", "host-1"),
        )
        .await;

        // Then: the assertions confirm that agent hello rejects unenrolled agent id.
        assert!(result.is_none());
        assert_eq!(current_agent_id, None);
        assert!(!state.agents.read().await.contains_key("agent-1"));
    }

    #[tokio::test]
    async fn agent_hello_requires_config_token_for_host_override() {
        // Given: the scenario for agent hello requires config token for host override is prepared.
        let state = test_state(
            r#"
            [agents."agent-1"]
            hostname = "prod-vps-1"
            config_token = "agent-secret"

            [hosts."prod-vps-1".storage]
            interval_seconds = 30
            mounts = ["/", "/var/lib/docker"]
            "#,
        );
        let mut current_agent_id = None;

        // When: the behavior under test runs.
        let result = register_agent_connection(
            &state,
            "test-connection",
            &mut current_agent_id,
            None,
            test_hello("agent-1", "prod-vps-1"),
        )
        .await;

        // Then: the assertions confirm that agent hello requires config token for host override.
        assert!(result.is_none());
        assert_eq!(current_agent_id, None);
        assert!(!state.agents.read().await.contains_key("agent-1"));
    }

    #[tokio::test]
    async fn agent_hello_accepts_config_token_for_host_override() {
        // Given: the scenario for agent hello accepts config token for host override is prepared.
        let state = test_state(
            r#"
            [agents."agent-1"]
            hostname = "prod-vps-1"
            config_token = "agent-secret"

            [hosts."prod-vps-1".storage]
            interval_seconds = 30
            mounts = ["/", "/var/lib/docker"]
            "#,
        );
        let mut current_agent_id = None;

        // When: the behavior under test runs.
        let result = register_agent_connection(
            &state,
            "test-connection",
            &mut current_agent_id,
            Some("agent-secret"),
            test_hello("agent-1", "prod-vps-1"),
        )
        .await;

        // Then: the assertions confirm that agent hello accepts config token for host override.
        assert!(result.is_some());
        assert_eq!(current_agent_id.as_deref(), Some("agent-1"));
        assert_eq!(
            state
                .agents
                .read()
                .await
                .get("agent-1")
                .map(|agent| agent.connection_id.as_str()),
            Some("test-connection")
        );
    }

    #[tokio::test]
    async fn admin_reload_rejects_invalid_runtime_config_without_swapping() {
        // Given: the scenario for admin reload rejects invalid runtime config without swapping is prepared.
        let path = test_control_config_path();
        fs::write(
            &path,
            r#"
            [defaults.agent]
            heartbeat_interval_seconds = 0
            "#,
        )
        .expect("invalid control config should be writable");
        let mut state = test_state(
            r#"
            [defaults.storage]
            interval_seconds = 45
            mounts = ["/"]
            "#,
        );
        state.config_path = Some(path.clone());

        // When: the behavior under test runs.
        let response = admin_config_reload(State(state.clone()), admin_headers()).await;

        // Then: the assertions confirm that admin reload rejects invalid runtime config without swapping.
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let active_config = state.config.read().await;
        let runtime = active_config
            .config
            .runtime_config_for("host-1")
            .expect("active runtime config should remain valid");
        assert_eq!(runtime.storage.interval_seconds, 45);
        drop(active_config);
        let _ = fs::remove_file(path);
    }

    fn test_state(config: &str) -> AppState {
        let (notifications, _) = broadcast::channel(128);
        AppState {
            agent_token: Arc::from("agent-token"),
            admin_token: Arc::from("admin-token"),
            config_path: None,
            config: Arc::new(RwLock::new(LoadedControlConfig {
                source_version: config_source_version(config),
                config: toml::from_str(config).expect("control config should parse"),
            })),
            notifications,
            agents: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    async fn connect_test_agent(state: &AppState, agent_id: &str, hostname: &str) {
        connect_test_agent_with_connection(state, agent_id, hostname, "test-connection").await;
    }

    async fn connect_test_agent_with_connection(
        state: &AppState,
        agent_id: &str,
        hostname: &str,
        connection_id: &str,
    ) {
        state.agents.write().await.insert(
            agent_id.to_string(),
            ConnectedAgent {
                agent_id: agent_id.to_string(),
                hostname: hostname.to_string(),
                agent_version: "test-agent".to_string(),
                boot_id: None,
                config_version: None,
                enabled_collectors: Vec::new(),
                connected_at_unix_seconds: 1,
                last_seen_unix_seconds: 1,
                connection_id: connection_id.to_string(),
            },
        );
    }

    fn test_hello(agent_id: &str, hostname: &str) -> AgentHello {
        AgentHello {
            agent_id: agent_id.to_string(),
            hostname: hostname.to_string(),
            agent_version: "test-agent".to_string(),
            boot_id: None,
        }
    }

    fn bearer(token: &str) -> HeaderMap {
        authorization_header(&format!("Bearer {token}"))
    }

    fn authorization_header(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(value).expect("authorization value should be a header value"),
        );
        headers
    }

    fn agent_headers(
        agent_id: &str,
        config_session_id: &str,
        agent_config_token: Option<&str>,
    ) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer agent-token"),
        );
        headers.insert(
            AGENT_ID_HEADER,
            HeaderValue::from_str(agent_id).expect("agent id should be a header value"),
        );
        headers.insert(
            CONFIG_SESSION_ID_HEADER,
            HeaderValue::from_str(config_session_id)
                .expect("config session id should be a header value"),
        );
        if let Some(agent_config_token) = agent_config_token {
            headers.insert(
                AGENT_CONFIG_TOKEN_HEADER,
                HeaderValue::from_str(agent_config_token)
                    .expect("agent config token should be a header value"),
            );
        }
        headers
    }

    fn admin_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer admin-token"),
        );
        headers
    }

    fn test_control_config_path() -> PathBuf {
        env::temp_dir().join(format!("talia-control-test-{}.toml", Uuid::new_v4()))
    }
}
