use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use talia_agent::identity;
use talia_agent::identity::AgentIdentity;
use talia_agent::runner;
use talia_agent::sinks::OtlpSink;
use talia_core::pipeline::Processor;
use talia_core::pipeline::Sink;
use tokio::sync::RwLock;

use crate::control::ControlClientConfig;
use crate::control::config_poll_loop;
use crate::control::control_websocket_loop;
use crate::local_config::load_bootstrap_config;
use crate::local_config::load_last_known_config;
use crate::local_config::optional_env_secret;

pub(crate) async fn execute(config_path: Option<&Path>) -> Result<()> {
    let mut bootstrap = load_bootstrap_config(config_path)?;
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
