//! OpenAPI contract for the Talia control plane.

use axum::Json;
use talia_core::config::AgentRuntimeConfig;
use talia_core::config::AgentRuntimeSettings;
use talia_core::config::CpuConfig;
use talia_core::config::DiskIoConfig;
use talia_core::config::MemoryConfig;
use talia_core::config::NetworkConfig;
use talia_core::config::StorageConfig;
use talia_core::control::AgentControlMessage;
use talia_core::control::ServerControlMessage;
use utoipa::OpenApi;
use utoipa::ToSchema;
use utoipa::openapi::security::HttpAuthScheme;
use utoipa::openapi::security::HttpBuilder;
use utoipa::openapi::security::SecurityScheme;

use super::ConnectedAgent;
use super::ReloadResponse;

struct ControlSecurity;

impl utoipa::Modify for ControlSecurity {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "agent_bearer",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .description(Some("TALIA_CONTROL_TOKEN used by enrolled agents."))
                    .build(),
            ),
        );
        components.add_security_scheme(
            "admin_bearer",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .description(Some("TALIA_ADMIN_TOKEN used by control-plane operators."))
                    .build(),
            ),
        );
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Talia Control API",
        version = env!("CARGO_PKG_VERSION"),
        description = "Operational API for Talia agent configuration, WebSocket control sessions, and administration."
    ),
    paths(
        health_contract,
        agent_config_contract,
        agent_control_contract,
        admin_agents_contract,
        admin_config_reload_contract
    ),
    components(schemas(
        PlainHealthResponse,
        AgentRuntimeConfig,
        AgentRuntimeSettings,
        StorageConfig,
        MemoryConfig,
        CpuConfig,
        NetworkConfig,
        DiskIoConfig,
        AgentControlMessage,
        ServerControlMessage,
        ConnectedAgent,
        ReloadResponse
    )),
    modifiers(&ControlSecurity),
    tags(
        (name = "system", description = "Control-plane health."),
        (name = "agents", description = "Authenticated agent configuration and control."),
        (name = "admin", description = "Authenticated control-plane administration.")
    )
)]
struct ApiDoc;

#[derive(serde::Serialize, ToSchema)]
struct PlainHealthResponse(String);

pub(crate) async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(document())
}

fn document() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}

#[utoipa::path(
    get,
    path = "/health",
    operation_id = "health",
    tag = "system",
    responses(
        (status = 200, description = "Control server is running.", body = PlainHealthResponse, content_type = "text/plain")
    )
)]
#[allow(dead_code)]
fn health_contract() {}

#[utoipa::path(
    get,
    path = "/agent/config",
    operation_id = "agentConfig",
    tag = "agents",
    description = "Returns the resolved runtime config for an enrolled agent connected through the active WebSocket session.",
    security(
        ("agent_bearer" = [])
    ),
    params(
        ("x-talia-agent-id" = String, Header, description = "Persistent id of the enrolled agent."),
        ("x-talia-config-session-id" = String, Header, description = "Session id returned by the WebSocket `hello_ack` message."),
        ("x-talia-agent-config-token" = String, Header, description = "Per-agent enrollment credential.")
    ),
    responses(
        (status = 200, description = "Resolved runtime configuration.", body = AgentRuntimeConfig),
        (status = 400, description = "Agent id or config session id header is missing."),
        (status = 401, description = "The control bearer token is invalid."),
        (status = 403, description = "The agent, session, hostname, or per-agent credential does not match enrollment."),
        (status = 500, description = "The resolved runtime configuration is invalid.")
    )
)]
#[allow(dead_code)]
fn agent_config_contract() {}

#[utoipa::path(
    get,
    path = "/agent/control",
    operation_id = "agentControl",
    tag = "agents",
    description = "Upgrades to the Talia control WebSocket. Agents send `AgentControlMessage` values and receive `ServerControlMessage` values after upgrade.",
    security(
        ("agent_bearer" = [])
    ),
    params(
        ("x-talia-agent-config-token" = String, Header, description = "Per-agent enrollment credential used to authorize the initial hello message."),
        ("Connection" = String, Header, description = "WebSocket upgrade connection header."),
        ("Upgrade" = String, Header, description = "Must request the `websocket` protocol upgrade."),
        ("Sec-WebSocket-Key" = String, Header, description = "Base64-encoded random nonce used to validate the handshake."),
        ("Sec-WebSocket-Version" = String, Header, description = "WebSocket protocol version. Must be `13`.")
    ),
    responses(
        (status = 101, description = "WebSocket control session established.", headers(
            ("Connection" = String, description = "WebSocket upgrade confirmation."),
            ("Upgrade" = String, description = "Selected WebSocket protocol."),
            ("Sec-WebSocket-Accept" = String, description = "Handshake response derived from `Sec-WebSocket-Key`.")
        )),
        (status = 401, description = "The control bearer token is invalid."),
        (status = 400, description = "The WebSocket upgrade request is invalid.")
    )
)]
#[allow(dead_code)]
fn agent_control_contract() {}

#[utoipa::path(
    get,
    path = "/admin/agents",
    operation_id = "adminAgents",
    tag = "admin",
    security(
        ("admin_bearer" = [])
    ),
    responses(
        (status = 200, description = "Agents currently connected to this control server.", body = [ConnectedAgent]),
        (status = 401, description = "The admin bearer token is invalid.")
    )
)]
#[allow(dead_code)]
fn admin_agents_contract() {}

#[utoipa::path(
    post,
    path = "/admin/config/reload",
    operation_id = "adminConfigReload",
    tag = "admin",
    description = "Reloads the control config and notifies connected agents that a new version is available.",
    security(
        ("admin_bearer" = [])
    ),
    responses(
        (status = 200, description = "Config reloaded and agents notified.", body = ReloadResponse),
        (status = 401, description = "The admin bearer token is invalid."),
        (status = 500, description = "The control config could not be loaded or validated.")
    )
)]
#[allow(dead_code)]
fn admin_config_reload_contract() {}

#[cfg(test)]
mod tests {
    #[test]
    fn document_covers_control_routes_and_authentication_roles() {
        // Given: Talia's independent control-plane OpenAPI document.
        let document = serde_json::to_value(super::document()).expect("serialize OpenAPI document");

        // When: documented operations and security schemes are inspected.
        let paths = document
            .get("paths")
            .and_then(serde_json::Value::as_object)
            .expect("OpenAPI paths");
        let security_schemes = document
            .pointer("/components/securitySchemes")
            .and_then(serde_json::Value::as_object)
            .expect("OpenAPI security schemes");

        // Then: every functional route and both distinct bearer roles are explicit.
        assert_eq!(paths.len(), 5);
        assert!(paths["/health"].get("get").is_some());
        assert!(paths["/agent/config"].get("get").is_some());
        assert!(paths["/agent/control"].get("get").is_some());
        assert!(paths["/admin/agents"].get("get").is_some());
        assert!(paths["/admin/config/reload"].get("post").is_some());
        assert!(security_schemes.contains_key("agent_bearer"));
        assert!(security_schemes.contains_key("admin_bearer"));
    }

    #[test]
    fn document_connects_agent_headers_and_protocol_schemas() {
        // Given: the documented agent config and WebSocket control operations.
        let document = serde_json::to_value(super::document()).expect("serialize OpenAPI document");
        let config_operation = document
            .pointer("/paths/~1agent~1config/get")
            .expect("agent config operation");
        let control_operation = document
            .pointer("/paths/~1agent~1control/get")
            .expect("agent control operation");

        // When: required headers, upgrade response, and shared schemas are inspected.
        let config_headers = config_operation
            .get("parameters")
            .and_then(serde_json::Value::as_array)
            .expect("agent config headers");
        let schemas = document
            .pointer("/components/schemas")
            .and_then(serde_json::Value::as_object)
            .expect("protocol schemas");

        // Then: clients can discover the complete config and WebSocket protocol contract.
        for header in [
            "x-talia-agent-id",
            "x-talia-config-session-id",
            "x-talia-agent-config-token",
        ] {
            assert!(config_headers.iter().any(|parameter| {
                parameter.get("name") == Some(&serde_json::json!(header))
                    && parameter.get("required") == Some(&serde_json::json!(true))
            }));
        }
        let control_headers = control_operation
            .get("parameters")
            .and_then(serde_json::Value::as_array)
            .expect("agent control headers");
        for header in [
            "Connection",
            "Upgrade",
            "Sec-WebSocket-Key",
            "Sec-WebSocket-Version",
        ] {
            assert!(control_headers.iter().any(|parameter| {
                parameter.get("name") == Some(&serde_json::json!(header))
                    && parameter.get("required") == Some(&serde_json::json!(true))
            }));
        }
        assert!(control_operation.pointer("/responses/101").is_some());
        assert!(
            control_operation
                .pointer("/responses/101/headers/Sec-WebSocket-Accept")
                .is_some()
        );
        assert!(schemas.contains_key("AgentRuntimeConfig"));
        assert!(schemas.contains_key("AgentControlMessage"));
        assert!(schemas.contains_key("ServerControlMessage"));
    }
}
