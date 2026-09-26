//! Neutral telemetry data model and provider plugin traits.
//!
//! Providers produce [`Sample`]s without knowing where the data ends up;
//! sinks consume samples without knowing which provider produced them. This
//! module is the seam that lets new collectors and new export destinations be
//! added without touching the agent bootstrap.

mod provider;
mod sample;

pub use provider::Provider;
pub use provider::ProviderError;
pub use sample::Sample;
pub use sample::SampleValue;
