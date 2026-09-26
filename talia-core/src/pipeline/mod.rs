//! Neutral telemetry data model and pipeline plugin traits.
//!
//! Providers produce [`Sample`]s without knowing where the data ends up;
//! processors transform or drop samples mid-pipeline; sinks consume samples
//! without knowing which provider produced them. This module is the seam that
//! lets new collectors, pipeline stages, and export destinations be added
//! without touching the agent bootstrap.

mod processor;
mod provider;
mod sample;
mod sink;

pub use processor::Processor;
pub use processor::process_sample;
pub use provider::Provider;
pub use provider::ProviderError;
pub use sample::Sample;
pub use sample::SampleValue;
pub use sink::Sink;
