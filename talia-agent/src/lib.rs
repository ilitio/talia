//! Linux host monitoring collectors used by the Talia agent binary.

#![deny(warnings)]

#[cfg(all(not(target_os = "linux"), not(doc)))]
compile_error!("talia-agent supports Linux only");

#[cfg(target_os = "linux")]
pub mod identity;
#[cfg(target_os = "linux")]
pub mod modules;
#[cfg(target_os = "linux")]
pub mod processors;
#[cfg(target_os = "linux")]
pub mod runner;
#[cfg(target_os = "linux")]
pub mod sinks;
