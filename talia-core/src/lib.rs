//! Shared Talia monitoring agent and control-plane contracts.
//!
//! `talia-core` owns the serializable types that must stay consistent between
//! the Linux host agent and the control server: bootstrap config, runtime config,
//! enrollment metadata, HTTP headers, and WebSocket control messages. Keep
//! transport-independent validation here so both binaries apply the same rules.

#![deny(warnings)]

/// Agent bootstrap, runtime, enrollment, and control-plane configuration types.
pub mod config;
/// HTTP headers and WebSocket messages used by the agent control protocol.
pub mod control;
/// Constant-time comparison for control-plane secrets.
pub mod secret;
