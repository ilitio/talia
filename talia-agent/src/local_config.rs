use std::env;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;
use talia_core::config::AgentBootstrapConfig;
use talia_core::config::AgentRuntimeConfig;

const LAST_KNOWN_CONFIG_FILE: &str = "last-config.json";

pub(crate) fn load_bootstrap_config(path: Option<&Path>) -> Result<AgentBootstrapConfig> {
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

pub(crate) fn load_local_runtime_config(path: Option<&Path>) -> Result<AgentRuntimeConfig> {
    Ok(load_bootstrap_config(path)?.fallback_runtime_config())
}

pub(crate) fn optional_env_secret(name: &'static str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).with_context(|| format!("{name} must be valid unicode")),
    }
}

fn last_known_config_path(state_dir: &Path) -> PathBuf {
    state_dir.join(LAST_KNOWN_CONFIG_FILE)
}

pub(crate) fn load_last_known_config(state_dir: &Path) -> Option<AgentRuntimeConfig> {
    let path = last_known_config_path(state_dir);
    let contents = fs::read_to_string(path).ok()?;
    let config: AgentRuntimeConfig = serde_json::from_str(&contents).ok()?;
    config.validate().ok()?;
    Some(config)
}

pub(crate) fn save_last_known_config(state_dir: &Path, config: &AgentRuntimeConfig) -> Result<()> {
    fs::create_dir_all(state_dir)
        .with_context(|| format!("failed to create state directory {}", state_dir.display()))?;
    fs::write(
        last_known_config_path(state_dir),
        serde_json::to_vec_pretty(config).context("failed to serialize last known config")?,
    )
    .context("failed to write last known config")
}
