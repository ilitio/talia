//! Neutral data model and plugin traits for the Talia collection pipeline.
//!
//! Providers produce [`Sample`]s without knowing where the data ends up; sinks
//! will consume samples without knowing which provider produced them. This
//! module is the seam that lets new collectors and new export destinations be
//! added without touching the agent bootstrap.

use std::collections::BTreeMap;
use std::time::SystemTime;

use thiserror::Error;

/// A single measured value emitted by a provider.
#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
    /// Dotted metric name, e.g. `system.filesystem.usage`.
    pub name: String,
    /// The measured value.
    pub value: SampleValue,
    /// Dimensions describing the measurement, e.g. `mountpoint` -> `/`.
    ///
    /// Transport-level attributes (such as the active config version) are
    /// added by the sink, not the provider.
    pub attributes: BTreeMap<String, String>,
    /// When the measurement was taken.
    pub timestamp: SystemTime,
}

/// The value carried by a [`Sample`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SampleValue {
    /// A monotonically increasing count, e.g. bytes transferred.
    Counter(u64),
    /// A point-in-time unsigned measurement, e.g. bytes used.
    GaugeU64(u64),
    /// A point-in-time float measurement, e.g. a utilization ratio.
    GaugeF64(f64),
}

/// Error returned when a provider fails to collect.
#[derive(Debug, Error)]
#[error("provider '{provider}' collection failed: {source}")]
pub struct ProviderError {
    /// Stable name of the provider that failed.
    pub provider: &'static str,
    /// The underlying failure.
    #[source]
    pub source: Box<dyn std::error::Error + Send + Sync>,
}

impl ProviderError {
    /// Wraps a provider-specific failure, attaching the provider name.
    pub fn new(
        provider: &'static str,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            provider,
            source: Box::new(source),
        }
    }
}

/// A source of telemetry: eBPF programs, procfs readers, or anything else that
/// can produce [`Sample`]s on demand.
///
/// Providers are deliberately synchronous and pull-based: the runner decides
/// *when* to collect, the provider only decides *how*.
pub trait Provider: Send {
    /// Stable registry name, e.g. `"storage"`. Used in config and CLI output.
    fn name(&self) -> &'static str;

    /// Collects one batch of samples. Called once per collection interval.
    fn collect(&mut self) -> Result<Vec<Sample>, ProviderError>;
}

#[cfg(test)]
mod tests {
    use super::ProviderError;
    use std::io;

    #[test]
    fn provider_error_reports_provider_and_source() {
        let source = io::Error::new(io::ErrorKind::NotFound, "no such mount");
        let error = ProviderError::new("storage", source);
        assert_eq!(error.provider, "storage");
        assert!(error.to_string().contains("storage"));
        assert!(error.to_string().contains("no such mount"));
    }
}
