//! Provider plugin trait and error type.

use std::time::Duration;

use thiserror::Error;

use crate::config::AgentRuntimeConfig;

use super::sample::Sample;

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

    /// Updates the collection window before a collection. Default is a no-op.
    ///
    /// eBPF providers use the window to scale their per-interval accounting.
    fn set_window(&mut self, window: Duration) {
        let _ = window;
    }

    /// Applies runtime config changes before a collection. Default is a no-op.
    ///
    /// Lets providers react to config reloads without being rebuilt, e.g.
    /// the storage provider watching its mount list.
    fn reconfigure(&mut self, config: &AgentRuntimeConfig) {
        let _ = config;
    }
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
