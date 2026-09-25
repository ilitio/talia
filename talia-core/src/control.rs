//! Talia agent/control-plane protocol messages.
//!
//! The protocol uses HTTP headers for authenticated config fetches and tagged
//! JSON messages for long-lived WebSocket control sessions. Field names are part
//! of the wire contract between `talia-agent` and `talia-control`.

use serde::Deserialize;
use serde::Serialize;

/// HTTP header carrying the persistent Talia agent id.
pub const AGENT_ID_HEADER: &str = "x-talia-agent-id";
/// HTTP header carrying the optional per-agent config credential.
pub const AGENT_CONFIG_TOKEN_HEADER: &str = "x-talia-agent-config-token";
/// HTTP header carrying the active WebSocket config session id.
pub const CONFIG_SESSION_ID_HEADER: &str = "x-talia-config-session-id";

/// Message sent by a Talia agent to the control plane.
///
/// Agents first identify themselves with [`AgentControlMessage::Hello`], then
/// report liveness, applied config, and active collectors through
/// [`AgentControlMessage::Heartbeat`].
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentControlMessage {
    /// Initial message sent when an agent opens a control WebSocket.
    Hello {
        /// Agent identifier.
        agent_id: String,
        /// Hostname reported by the agent.
        hostname: String,
        /// Agent binary version.
        agent_version: String,
        /// Host boot identifier, when available.
        boot_id: Option<String>,
    },
    /// Periodic liveness and applied-config report.
    Heartbeat {
        /// Agent identifier.
        agent_id: String,
        /// Hostname reported by the agent.
        hostname: String,
        /// Agent binary version.
        agent_version: String,
        /// Runtime config version currently applied by the agent.
        config_version: String,
        /// Collectors currently enabled by the agent.
        enabled_collectors: Vec<String>,
    },
}

/// Message sent by the control plane to a Talia agent.
///
/// The server acknowledges the control session and can later notify the agent to
/// fetch a newer runtime config through the HTTP config endpoint.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerControlMessage {
    /// Acknowledges an agent hello message and binds the connection to a config session.
    HelloAck {
        /// Control-plane server time in Unix seconds.
        server_time_unix_seconds: u64,
        /// Config session id assigned to the agent connection.
        config_session_id: String,
    },
    /// Notifies the agent that runtime config changed and should be refetched.
    ConfigChanged {
        /// New runtime config version available to fetch.
        version: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_messages_use_tagged_json_shape() {
        // Given: the scenario for control messages use tagged json shape is prepared.
        let message = AgentControlMessage::Heartbeat {
            agent_id: "agent-1".to_string(),
            hostname: "host-1".to_string(),
            agent_version: "0.1.0".to_string(),
            config_version: "cfg-1".to_string(),
            enabled_collectors: vec!["storage".to_string()],
        };

        // When: the behavior under test runs.
        let value = serde_json::to_value(message).expect("message should serialize");

        // Then: the assertions confirm that control messages use tagged json shape.
        assert_eq!(value["type"], "heartbeat");
        assert_eq!(value["config_version"], "cfg-1");
    }

    #[test]
    fn hello_ack_includes_config_session_id() {
        // Given: the scenario for hello ack includes config session id is prepared.
        let message = ServerControlMessage::HelloAck {
            server_time_unix_seconds: 1,
            config_session_id: "session-1".to_string(),
        };

        // When: the behavior under test runs.
        let value = serde_json::to_value(message).expect("message should serialize");

        // Then: the assertions confirm that hello ack includes config session id.
        assert_eq!(value["type"], "hello_ack");
        assert_eq!(value["config_session_id"], "session-1");
    }
}
