//! Talia agent bootstrap, runtime, and control-plane configuration.
//!
//! Agents load [`crate::config::AgentBootstrapConfig`] locally at startup, then
//! either apply its fallback [`crate::config::AgentRuntimeConfig`] or fetch a
//! host-specific runtime config from a control server. The control server loads
//! [`crate::config::ControlConfigFile`] and resolves it into the same runtime
//! type so validation stays identical on both sides.

use std::collections::BTreeMap;
use std::fs;
use std::net::IpAddr;
use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

const DEFAULT_CONFIG_POLL_INTERVAL_SECONDS: u64 = 15 * 60;
const DEFAULT_HEARTBEAT_INTERVAL_SECONDS: u64 = 60;
const DEFAULT_STORAGE_INTERVAL_SECONDS: u64 = 60;
const DEFAULT_MEMORY_INTERVAL_SECONDS: u64 = 60;
const DEFAULT_CPU_INTERVAL_SECONDS: u64 = 1;
const DEFAULT_NETWORK_INTERVAL_SECONDS: u64 = 1;
const DEFAULT_DISK_IO_INTERVAL_SECONDS: u64 = 1;
const DEFAULT_CONTROL_HTTP_URL: &str = "http://127.0.0.1:3501";
const DEFAULT_CONTROL_WS_URL: &str = "ws://127.0.0.1:3501/agent/control";
const DEFAULT_OTLP_ENDPOINT: &str = "http://127.0.0.1:4317";
const DEFAULT_STATE_DIR: &str = "/var/lib/talia";
const LOCAL_CONFIG_VERSION: &str = "local";

/// Errors returned while loading or validating Talia configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// A configuration file could not be read.
    #[error("failed to read config file {path}: {source}")]
    Read {
        /// Path that failed to read.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },
    /// A TOML configuration file could not be parsed.
    #[error("failed to parse TOML config file {path}: {source}")]
    Parse {
        /// Path that failed to parse.
        path: PathBuf,
        /// Underlying TOML parse error.
        source: toml::de::Error,
    },
    /// Runtime config version was blank.
    #[error("agent config version must not be empty")]
    EmptyVersion,
    /// Agent config polling interval was zero.
    #[error("agent config poll interval must be greater than zero")]
    EmptyConfigPollInterval,
    /// Agent heartbeat interval was zero.
    #[error("agent heartbeat interval must be greater than zero")]
    EmptyHeartbeatInterval,
    /// Storage collection interval was zero.
    #[error("storage interval must be greater than zero")]
    EmptyStorageInterval,
    /// Storage collection was enabled without any mount paths.
    #[error("storage collector is enabled but no mounts are configured")]
    EmptyStorageMounts,
    /// Storage mount path was relative.
    #[error("storage mount path must be absolute: {0}")]
    RelativeStorageMount(String),
    /// Memory collection interval was zero.
    #[error("memory interval must be greater than zero")]
    EmptyMemoryInterval,
    /// CPU collection interval was zero.
    #[error("cpu interval must be greater than zero")]
    EmptyCpuInterval,
    /// Network collection interval was zero.
    #[error("network interval must be greater than zero")]
    EmptyNetworkInterval,
    /// Disk I/O collection interval was zero.
    #[error("disk I/O interval must be greater than zero")]
    EmptyDiskIoInterval,
    /// Control URL used an insecure non-loopback endpoint.
    #[error("{field} must use https/wss, or http/ws only for loopback endpoints: {value}")]
    InsecureControlUrl {
        /// Config field name.
        field: &'static str,
        /// Configured URL value.
        value: String,
    },
}

/// Local bootstrap configuration read by a Talia agent at startup.
///
/// This file tells the agent where to reach the control plane, where to persist
/// identity and last-known config state, and what runtime settings to use before
/// remote configuration is available.
#[derive(Clone, Debug, Deserialize)]
pub struct AgentBootstrapConfig {
    #[serde(default = "default_control_http_url")]
    /// HTTP control-plane endpoint used for runtime config fetches.
    pub control_http_url: String,
    #[serde(default = "default_control_ws_url")]
    /// WebSocket control-plane endpoint used for heartbeat/control messages.
    pub control_ws_url: String,
    #[serde(default)]
    /// Allows http/ws control URLs for private internal deployments.
    pub allow_insecure_control_url: bool,
    #[serde(default)]
    /// Shared bearer token used for control-plane authentication.
    pub control_token: Option<String>,
    #[serde(default)]
    /// Optional per-agent credential used to fetch dynamic runtime config.
    pub agent_config_token: Option<String>,
    #[serde(default = "default_otlp_endpoint")]
    /// OpenTelemetry OTLP metric export endpoint.
    pub otlp_endpoint: String,
    #[serde(default = "default_state_dir")]
    /// Agent state directory for persistent identity and cached config.
    pub state_dir: PathBuf,
    #[serde(default)]
    /// Fallback local agent runtime settings.
    pub agent: AgentRuntimeSettings,
    #[serde(default)]
    /// Fallback local storage collector settings.
    pub storage: StorageConfig,
    #[serde(default)]
    /// Fallback local memory collector settings.
    pub memory: MemoryConfig,
    #[serde(default)]
    /// Fallback local CPU collector settings.
    pub cpu: CpuConfig,
    #[serde(default)]
    /// Fallback local network collector settings.
    pub network: NetworkConfig,
    #[serde(default)]
    /// Fallback local disk I/O collector settings.
    pub disk_io: DiskIoConfig,
}

impl AgentBootstrapConfig {
    /// Loads bootstrap configuration from a TOML file.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        toml::from_str(&contents).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })
    }

    /// Builds the runtime config used before or without a control-plane response.
    pub fn fallback_runtime_config(&self) -> AgentRuntimeConfig {
        AgentRuntimeConfig {
            version: LOCAL_CONFIG_VERSION.to_string(),
            agent: self.agent.clone(),
            storage: self.storage.clone(),
            memory: self.memory.clone(),
            cpu: self.cpu.clone(),
            network: self.network.clone(),
            disk_io: self.disk_io.clone(),
        }
    }

    /// Validates bootstrap URLs and fallback runtime configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_control_url(
            "control_http_url",
            &self.control_http_url,
            "https://",
            "http://",
            self.allow_insecure_control_url,
        )?;
        validate_control_url(
            "control_ws_url",
            &self.control_ws_url,
            "wss://",
            "ws://",
            self.allow_insecure_control_url,
        )?;
        self.fallback_runtime_config().validate()
    }
}

impl Default for AgentBootstrapConfig {
    fn default() -> Self {
        Self {
            control_http_url: default_control_http_url(),
            control_ws_url: default_control_ws_url(),
            allow_insecure_control_url: false,
            control_token: None,
            agent_config_token: None,
            otlp_endpoint: default_otlp_endpoint(),
            state_dir: default_state_dir(),
            agent: AgentRuntimeSettings::default(),
            storage: StorageConfig::default(),
            memory: MemoryConfig::default(),
            cpu: CpuConfig::default(),
            network: NetworkConfig::default(),
            disk_io: DiskIoConfig::default(),
        }
    }
}

/// Runtime configuration delivered to or derived by a Talia agent.
///
/// This is the applied collector contract. Both local fallback settings and
/// control-plane host overrides resolve into this type before validation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, utoipa::ToSchema)]
pub struct AgentRuntimeConfig {
    /// Runtime config version identifier reported in heartbeats and metrics.
    pub version: String,
    #[serde(default)]
    /// Agent control-loop settings.
    pub agent: AgentRuntimeSettings,
    #[serde(default)]
    /// Storage collector settings.
    pub storage: StorageConfig,
    #[serde(default)]
    /// Memory collector settings.
    pub memory: MemoryConfig,
    #[serde(default)]
    /// CPU collector settings.
    pub cpu: CpuConfig,
    #[serde(default)]
    /// Network collector settings.
    pub network: NetworkConfig,
    #[serde(default)]
    /// Disk I/O collector settings.
    pub disk_io: DiskIoConfig,
}

impl AgentRuntimeConfig {
    /// Validates runtime settings before they are applied by the agent.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.version.trim().is_empty() {
            return Err(ConfigError::EmptyVersion);
        }
        self.agent.validate()?;
        self.storage.validate()?;
        self.memory.validate()?;
        self.cpu.validate()?;
        self.network.validate()?;
        self.disk_io.validate()
    }
}

impl Default for AgentRuntimeConfig {
    fn default() -> Self {
        Self {
            version: LOCAL_CONFIG_VERSION.to_string(),
            agent: AgentRuntimeSettings::default(),
            storage: StorageConfig::default(),
            memory: MemoryConfig::default(),
            cpu: CpuConfig::default(),
            network: NetworkConfig::default(),
            disk_io: DiskIoConfig::default(),
        }
    }
}

/// Agent control-loop runtime settings.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, utoipa::ToSchema)]
pub struct AgentRuntimeSettings {
    #[serde(default = "default_config_poll_interval_seconds")]
    /// Seconds between dynamic config polls.
    pub config_poll_interval_seconds: u64,
    #[serde(default = "default_heartbeat_interval_seconds")]
    /// Seconds between heartbeat messages.
    pub heartbeat_interval_seconds: u64,
}

impl AgentRuntimeSettings {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.config_poll_interval_seconds == 0 {
            return Err(ConfigError::EmptyConfigPollInterval);
        }
        if self.heartbeat_interval_seconds == 0 {
            return Err(ConfigError::EmptyHeartbeatInterval);
        }
        Ok(())
    }
}

impl Default for AgentRuntimeSettings {
    fn default() -> Self {
        Self {
            config_poll_interval_seconds: default_config_poll_interval_seconds(),
            heartbeat_interval_seconds: default_heartbeat_interval_seconds(),
        }
    }
}

/// Storage collector runtime settings.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, utoipa::ToSchema)]
pub struct StorageConfig {
    #[serde(default = "default_true")]
    /// Whether storage collection is enabled.
    pub enabled: bool,
    #[serde(default = "default_storage_interval_seconds")]
    /// Seconds between storage collection samples.
    pub interval_seconds: u64,
    #[serde(default = "default_storage_mounts")]
    /// Absolute mount paths to monitor.
    pub mounts: Vec<String>,
}

impl StorageConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.interval_seconds == 0 {
            return Err(ConfigError::EmptyStorageInterval);
        }
        if self.enabled && self.mounts.is_empty() {
            return Err(ConfigError::EmptyStorageMounts);
        }
        for mount in &self.mounts {
            if !Path::new(mount).is_absolute() {
                return Err(ConfigError::RelativeStorageMount(mount.clone()));
            }
        }
        Ok(())
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_seconds: default_storage_interval_seconds(),
            mounts: default_storage_mounts(),
        }
    }
}

/// Memory collector runtime settings.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, utoipa::ToSchema)]
pub struct MemoryConfig {
    #[serde(default = "default_true")]
    /// Whether memory collection is enabled.
    pub enabled: bool,
    #[serde(default = "default_memory_interval_seconds")]
    /// Seconds between memory collection samples.
    pub interval_seconds: u64,
}

impl MemoryConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.interval_seconds == 0 {
            return Err(ConfigError::EmptyMemoryInterval);
        }
        Ok(())
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_seconds: default_memory_interval_seconds(),
        }
    }
}

/// CPU collector runtime settings.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, utoipa::ToSchema)]
pub struct CpuConfig {
    #[serde(default)]
    /// Whether CPU collection is enabled.
    pub enabled: bool,
    #[serde(default = "default_cpu_interval_seconds")]
    /// Seconds between CPU collection samples.
    pub interval_seconds: u64,
}

impl CpuConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.interval_seconds == 0 {
            return Err(ConfigError::EmptyCpuInterval);
        }
        Ok(())
    }
}

impl Default for CpuConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_seconds: default_cpu_interval_seconds(),
        }
    }
}

/// Network collector runtime settings.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, utoipa::ToSchema)]
pub struct NetworkConfig {
    #[serde(default)]
    /// Whether network collection is enabled.
    pub enabled: bool,
    #[serde(default = "default_network_interval_seconds")]
    /// Seconds between network collection samples.
    pub interval_seconds: u64,
}

impl NetworkConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.interval_seconds == 0 {
            return Err(ConfigError::EmptyNetworkInterval);
        }
        Ok(())
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_seconds: default_network_interval_seconds(),
        }
    }
}

/// Disk I/O collector runtime settings.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, utoipa::ToSchema)]
pub struct DiskIoConfig {
    #[serde(default)]
    /// Whether disk I/O collection is enabled.
    pub enabled: bool,
    #[serde(default = "default_disk_io_interval_seconds")]
    /// Seconds between disk I/O collection samples.
    pub interval_seconds: u64,
}

impl DiskIoConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.interval_seconds == 0 {
            return Err(ConfigError::EmptyDiskIoInterval);
        }
        Ok(())
    }
}

impl Default for DiskIoConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_seconds: default_disk_io_interval_seconds(),
        }
    }
}

/// Control-plane configuration file containing defaults, host overrides, and enrolled agents.
///
/// Defaults apply to every host, host overrides refine a specific hostname, and
/// agent enrollment records bind persistent agent ids to the hostnames they are
/// allowed to represent.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct ControlConfigFile {
    #[serde(default)]
    /// Defaults applied to every host runtime config.
    pub defaults: HostConfigOverride,
    #[serde(default)]
    /// Hostname-specific runtime config overrides.
    pub hosts: BTreeMap<String, HostConfigOverride>,
    #[serde(default)]
    /// Agent enrollment records keyed by agent id.
    pub agents: BTreeMap<String, AgentEnrollment>,
}

impl ControlConfigFile {
    /// Loads a control-plane configuration file from TOML.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        toml::from_str(&contents).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })
    }

    /// Builds the effective runtime config for a hostname.
    ///
    /// The returned config includes defaults, hostname overrides, a stable
    /// computed version, and full runtime validation.
    pub fn runtime_config_for(&self, hostname: &str) -> Result<AgentRuntimeConfig, ConfigError> {
        let mut runtime = AgentRuntimeConfig::default();
        self.defaults.apply_to(&mut runtime);
        if let Some(host_config) = self.hosts.get(hostname) {
            host_config.apply_to(&mut runtime);
        }
        runtime.version = effective_config_version(hostname, &runtime);
        runtime.validate()?;
        Ok(runtime)
    }

    /// Returns whether a host has an explicit override block.
    pub fn has_host_override(&self, hostname: &str) -> bool {
        self.hosts.contains_key(hostname)
    }

    /// Returns whether an agent id is enrolled for the provided hostname.
    pub fn agent_is_enrolled_for_hostname(&self, agent_id: &str, hostname: &str) -> bool {
        self.agents
            .get(agent_id)
            .is_some_and(|agent| agent.hostname == hostname)
    }

    /// Returns whether an agent's config token matches the enrolled token.
    pub fn agent_config_token_matches(&self, agent_id: &str, config_token: &str) -> bool {
        self.agents
            .get(agent_id)
            .and_then(|agent| agent.config_token.as_deref())
            .is_some_and(|token| crate::secret::matches(token, config_token))
    }
}

/// Agent enrollment record in the control-plane config.
///
/// Enrollment prevents an arbitrary connected agent id from claiming any
/// hostname when fetching runtime configuration.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct AgentEnrollment {
    /// Hostname this agent id is allowed to represent.
    pub hostname: String,
    /// Optional token required to fetch config for this agent.
    pub config_token: Option<String>,
}

/// Host-level runtime config override.
///
/// All fields are partial overrides so control-plane config can set broad
/// defaults once and only mention host-specific differences.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct HostConfigOverride {
    #[serde(default)]
    /// Agent settings override.
    pub agent: AgentSettingsOverride,
    #[serde(default)]
    /// Storage settings override.
    pub storage: StorageConfigOverride,
    #[serde(default)]
    /// Memory settings override.
    pub memory: MemoryConfigOverride,
    #[serde(default)]
    /// CPU settings override.
    pub cpu: CpuConfigOverride,
    #[serde(default)]
    /// Network settings override.
    pub network: NetworkConfigOverride,
    #[serde(default)]
    /// Disk I/O settings override.
    pub disk_io: DiskIoConfigOverride,
}

impl HostConfigOverride {
    fn apply_to(&self, runtime: &mut AgentRuntimeConfig) {
        self.agent.apply_to(&mut runtime.agent);
        self.storage.apply_to(&mut runtime.storage);
        self.memory.apply_to(&mut runtime.memory);
        self.cpu.apply_to(&mut runtime.cpu);
        self.network.apply_to(&mut runtime.network);
        self.disk_io.apply_to(&mut runtime.disk_io);
    }
}

/// Partial override for agent control-loop settings.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct AgentSettingsOverride {
    /// Optional replacement for runtime config poll interval seconds.
    pub config_poll_interval_seconds: Option<u64>,
    /// Optional replacement for heartbeat interval seconds.
    pub heartbeat_interval_seconds: Option<u64>,
}

impl AgentSettingsOverride {
    fn apply_to(&self, agent: &mut AgentRuntimeSettings) {
        if let Some(interval) = self.config_poll_interval_seconds {
            agent.config_poll_interval_seconds = interval;
        }
        if let Some(interval) = self.heartbeat_interval_seconds {
            agent.heartbeat_interval_seconds = interval;
        }
    }
}

/// Partial override for storage collector settings.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct StorageConfigOverride {
    /// Optional replacement for whether storage collection is enabled.
    pub enabled: Option<bool>,
    /// Optional replacement for storage collection interval seconds.
    pub interval_seconds: Option<u64>,
    /// Optional replacement for monitored mount paths.
    pub mounts: Option<Vec<String>>,
}

impl StorageConfigOverride {
    fn apply_to(&self, storage: &mut StorageConfig) {
        if let Some(enabled) = self.enabled {
            storage.enabled = enabled;
        }
        if let Some(interval) = self.interval_seconds {
            storage.interval_seconds = interval;
        }
        if let Some(mounts) = &self.mounts {
            storage.mounts.clone_from(mounts);
        }
    }
}

/// Partial override for memory collector settings.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct MemoryConfigOverride {
    /// Optional replacement for whether memory collection is enabled.
    pub enabled: Option<bool>,
    /// Optional replacement for memory collection interval seconds.
    pub interval_seconds: Option<u64>,
}

impl MemoryConfigOverride {
    fn apply_to(&self, memory: &mut MemoryConfig) {
        if let Some(enabled) = self.enabled {
            memory.enabled = enabled;
        }
        if let Some(interval) = self.interval_seconds {
            memory.interval_seconds = interval;
        }
    }
}

/// Partial override for CPU collector settings.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct CpuConfigOverride {
    /// Optional replacement for whether CPU collection is enabled.
    pub enabled: Option<bool>,
    /// Optional replacement for CPU collection interval seconds.
    pub interval_seconds: Option<u64>,
}

impl CpuConfigOverride {
    fn apply_to(&self, cpu: &mut CpuConfig) {
        if let Some(enabled) = self.enabled {
            cpu.enabled = enabled;
        }
        if let Some(interval) = self.interval_seconds {
            cpu.interval_seconds = interval;
        }
    }
}

/// Partial override for network collector settings.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct NetworkConfigOverride {
    /// Optional replacement for whether network collection is enabled.
    pub enabled: Option<bool>,
    /// Optional replacement for network collection interval seconds.
    pub interval_seconds: Option<u64>,
}

impl NetworkConfigOverride {
    fn apply_to(&self, network: &mut NetworkConfig) {
        if let Some(enabled) = self.enabled {
            network.enabled = enabled;
        }
        if let Some(interval) = self.interval_seconds {
            network.interval_seconds = interval;
        }
    }
}

/// Partial override for disk I/O collector settings.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct DiskIoConfigOverride {
    /// Optional replacement for whether disk I/O collection is enabled.
    pub enabled: Option<bool>,
    /// Optional replacement for disk I/O collection interval seconds.
    pub interval_seconds: Option<u64>,
}

impl DiskIoConfigOverride {
    fn apply_to(&self, disk_io: &mut DiskIoConfig) {
        if let Some(enabled) = self.enabled {
            disk_io.enabled = enabled;
        }
        if let Some(interval) = self.interval_seconds {
            disk_io.interval_seconds = interval;
        }
    }
}

/// Computes a stable version identifier for raw config file contents.
pub fn config_source_version(contents: &str) -> String {
    format!("source-{:016x}", fnv1a64(contents.as_bytes()))
}

fn effective_config_version(hostname: &str, runtime: &AgentRuntimeConfig) -> String {
    let mut versioned = runtime.clone();
    versioned.version.clear();
    let serialized = serde_json::to_string(&versioned).unwrap_or_default();
    let mut bytes = hostname.as_bytes().to_vec();
    bytes.extend_from_slice(serialized.as_bytes());
    format!("cfg-{:016x}", fnv1a64(&bytes))
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn default_control_http_url() -> String {
    DEFAULT_CONTROL_HTTP_URL.to_string()
}

fn default_control_ws_url() -> String {
    DEFAULT_CONTROL_WS_URL.to_string()
}

fn default_otlp_endpoint() -> String {
    DEFAULT_OTLP_ENDPOINT.to_string()
}

fn default_state_dir() -> PathBuf {
    PathBuf::from(DEFAULT_STATE_DIR)
}

fn default_true() -> bool {
    true
}

fn default_config_poll_interval_seconds() -> u64 {
    DEFAULT_CONFIG_POLL_INTERVAL_SECONDS
}

fn default_heartbeat_interval_seconds() -> u64 {
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS
}

fn default_storage_interval_seconds() -> u64 {
    DEFAULT_STORAGE_INTERVAL_SECONDS
}

fn default_memory_interval_seconds() -> u64 {
    DEFAULT_MEMORY_INTERVAL_SECONDS
}

fn default_storage_mounts() -> Vec<String> {
    vec!["/".to_string()]
}

fn default_cpu_interval_seconds() -> u64 {
    DEFAULT_CPU_INTERVAL_SECONDS
}

fn default_network_interval_seconds() -> u64 {
    DEFAULT_NETWORK_INTERVAL_SECONDS
}

fn default_disk_io_interval_seconds() -> u64 {
    DEFAULT_DISK_IO_INTERVAL_SECONDS
}

fn validate_control_url(
    field: &'static str,
    value: &str,
    secure_prefix: &str,
    loopback_prefix: &str,
    allow_insecure_control_url: bool,
) -> Result<(), ConfigError> {
    if value.starts_with(secure_prefix) {
        return Ok(());
    }
    if allow_insecure_control_url && value.starts_with(loopback_prefix) {
        return Ok(());
    }
    if value.starts_with(loopback_prefix) && plaintext_url_is_loopback(value, loopback_prefix) {
        return Ok(());
    }
    Err(ConfigError::InsecureControlUrl {
        field,
        value: value.to_string(),
    })
}

fn plaintext_url_is_loopback(value: &str, prefix: &str) -> bool {
    let authority = value[prefix.len()..]
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    if authority.contains('@') {
        return false;
    }
    let host = if let Some(rest) = authority.strip_prefix('[') {
        rest.split(']').next().unwrap_or_default()
    } else {
        authority.split(':').next().unwrap_or_default()
    };
    if host == "localhost" {
        return true;
    }
    host.parse::<IpAddr>()
        .is_ok_and(|address| address.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_config_applies_hostname_override() {
        // Given: the scenario for control config applies hostname override is prepared.
        let config: ControlConfigFile = toml::from_str(
            r#"
            [agents."agent-1"]
            hostname = "prod-vps-1"
            config_token = "agent-secret"

            [defaults.agent]
            config_poll_interval_seconds = 900

            [defaults.storage]
            interval_seconds = 60
            mounts = ["/"]

            [defaults.memory]
            enabled = true
            interval_seconds = 60

            [defaults.cpu]
            enabled = true
            interval_seconds = 1

            [defaults.network]
            enabled = true
            interval_seconds = 1

            [defaults.disk_io]
            enabled = true
            interval_seconds = 1

            [hosts."prod-vps-1".storage]
            interval_seconds = 30
            mounts = ["/", "/var/lib/docker"]

            [hosts."prod-vps-1".memory]
            interval_seconds = 30

            [hosts."prod-vps-1".cpu]
            interval_seconds = 2

            [hosts."prod-vps-1".network]
            interval_seconds = 2

            [hosts."prod-vps-1".disk_io]
            interval_seconds = 2
            "#,
        )
        .expect("config should parse");

        // When: the behavior under test runs.
        let runtime = config
            .runtime_config_for("prod-vps-1")
            .expect("runtime config should validate");

        // Then: the assertions confirm that control config applies hostname override.
        assert_eq!(runtime.agent.config_poll_interval_seconds, 900);
        assert_eq!(runtime.storage.interval_seconds, 30);
        assert_eq!(runtime.storage.mounts, ["/", "/var/lib/docker"]);
        assert!(runtime.memory.enabled);
        assert_eq!(runtime.memory.interval_seconds, 30);
        assert!(runtime.cpu.enabled);
        assert_eq!(runtime.cpu.interval_seconds, 2);
        assert!(runtime.network.enabled);
        assert_eq!(runtime.network.interval_seconds, 2);
        assert!(runtime.disk_io.enabled);
        assert_eq!(runtime.disk_io.interval_seconds, 2);
        assert!(runtime.version.starts_with("cfg-"));
        assert!(config.has_host_override("prod-vps-1"));
        assert!(config.agent_is_enrolled_for_hostname("agent-1", "prod-vps-1"));
        assert!(config.agent_config_token_matches("agent-1", "agent-secret"));
        assert!(!config.agent_config_token_matches("agent-1", "wrong-secret"));
        assert!(!config.agent_is_enrolled_for_hostname("agent-2", "prod-vps-1"));
    }

    #[test]
    fn enabled_storage_requires_absolute_mounts() {
        // Given: the scenario for enabled storage requires absolute mounts is prepared.
        let config = AgentRuntimeConfig {
            storage: StorageConfig {
                mounts: vec!["var".to_string()],
                ..StorageConfig::default()
            },
            ..AgentRuntimeConfig::default()
        };

        // When: the behavior under test runs.
        let result = config.validate();

        // Then: the assertions confirm that enabled storage requires absolute mounts.
        assert!(matches!(
            result,
            Err(ConfigError::RelativeStorageMount(mount)) if mount == "var"
        ));
    }

    #[test]
    fn cpu_interval_must_be_positive() {
        // Given: the scenario for cpu interval must be positive is prepared.
        let config = AgentRuntimeConfig {
            cpu: CpuConfig {
                enabled: true,
                interval_seconds: 0,
            },
            ..AgentRuntimeConfig::default()
        };

        // When: the behavior under test runs.
        let result = config.validate();

        // Then: the assertions confirm that cpu interval must be positive.
        assert!(matches!(result, Err(ConfigError::EmptyCpuInterval)));
    }

    #[test]
    fn memory_interval_must_be_positive() {
        // Given: the scenario for memory interval must be positive is prepared.
        let config = AgentRuntimeConfig {
            memory: MemoryConfig {
                enabled: true,
                interval_seconds: 0,
            },
            ..AgentRuntimeConfig::default()
        };

        // When: the behavior under test runs.
        let result = config.validate();

        // Then: the assertions confirm that memory interval must be positive.
        assert!(matches!(result, Err(ConfigError::EmptyMemoryInterval)));
    }

    #[test]
    fn network_interval_must_be_positive() {
        // Given: the scenario for network interval must be positive is prepared.
        let config = AgentRuntimeConfig {
            network: NetworkConfig {
                enabled: true,
                interval_seconds: 0,
            },
            ..AgentRuntimeConfig::default()
        };

        // When: the behavior under test runs.
        let result = config.validate();

        // Then: the assertions confirm that network interval must be positive.
        assert!(matches!(result, Err(ConfigError::EmptyNetworkInterval)));
    }

    #[test]
    fn disk_io_interval_must_be_positive() {
        // Given: the scenario for disk io interval must be positive is prepared.
        let config = AgentRuntimeConfig {
            disk_io: DiskIoConfig {
                enabled: true,
                interval_seconds: 0,
            },
            ..AgentRuntimeConfig::default()
        };

        // When: the behavior under test runs.
        let result = config.validate();

        // Then: the assertions confirm that disk io interval must be positive.
        assert!(matches!(result, Err(ConfigError::EmptyDiskIoInterval)));
    }

    #[test]
    fn bootstrap_config_allows_loopback_plaintext_control_urls() {
        // Given: the scenario for bootstrap config allows loopback plaintext control urls is prepared.
        let config = AgentBootstrapConfig {
            control_http_url: "http://127.0.0.1:3501".to_string(),
            control_ws_url: "ws://localhost:3501/agent/control".to_string(),
            ..AgentBootstrapConfig::default()
        };

        // When: the behavior under test runs.
        let result = config.validate();

        // Then: the assertions confirm that bootstrap config allows loopback plaintext control urls.
        assert!(result.is_ok());
    }

    #[test]
    fn bootstrap_config_rejects_remote_plaintext_control_urls() {
        // Given: the scenario for bootstrap config rejects remote plaintext control urls is prepared.
        let config = AgentBootstrapConfig {
            control_http_url: "http://control.example.test:3501".to_string(),
            control_ws_url: "wss://control.example.test/agent/control".to_string(),
            ..AgentBootstrapConfig::default()
        };

        // When: the behavior under test runs.
        let result = config.validate();

        // Then: the assertions confirm that bootstrap config rejects remote plaintext control urls.
        assert!(matches!(
            result,
            Err(ConfigError::InsecureControlUrl {
                field: "control_http_url",
                ..
            })
        ));
    }

    #[test]
    fn bootstrap_config_allows_remote_plaintext_control_urls_when_explicitly_enabled() {
        // Given: the scenario for bootstrap config allows remote plaintext control urls when explicitly enabled is prepared.
        let config = AgentBootstrapConfig {
            control_http_url: "http://control.example.test:3501".to_string(),
            control_ws_url: "ws://control.example.test:3501/agent/control".to_string(),
            allow_insecure_control_url: true,
            ..AgentBootstrapConfig::default()
        };

        // When: the behavior under test runs.
        let result = config.validate();

        // Then: the assertions confirm that bootstrap config allows remote plaintext control urls when explicitly enabled.
        assert!(result.is_ok());
    }

    #[test]
    fn bootstrap_config_rejects_plaintext_hostnames_that_start_with_127() {
        // Given: the scenario for bootstrap config rejects plaintext hostnames that start with 127 is prepared.
        let config = AgentBootstrapConfig {
            control_http_url: "http://127.evil.test:3501".to_string(),
            control_ws_url: "wss://control.example.test/agent/control".to_string(),
            ..AgentBootstrapConfig::default()
        };

        // When: the behavior under test runs.
        let result = config.validate();

        // Then: the assertions confirm that bootstrap config rejects plaintext hostnames that start with 127.
        assert!(matches!(
            result,
            Err(ConfigError::InsecureControlUrl {
                field: "control_http_url",
                ..
            })
        ));
    }
}
