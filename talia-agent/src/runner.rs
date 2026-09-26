//! Generic collector runner: one task per provider, shared scheduling logic.
//!
//! Each collector is described by a [`CollectorSpec`]: how to read its
//! schedule from the runtime config and how to build its provider. Adding a
//! new collector is one entry in [`collector_specs`], not a new loop.

use std::sync::Arc;
use std::time::Duration;

use talia_core::config::AgentRuntimeConfig;
use talia_core::pipeline::Provider;
use talia_core::pipeline::ProviderError;
use talia_core::pipeline::Sink;
use tokio::sync::RwLock;

use crate::modules::cpu::CpuProvider;
use crate::modules::disk_io::DiskIoProvider;
use crate::modules::memory::MemoryProvider;
use crate::modules::network::NetworkProvider;
use crate::modules::storage::StorageProvider;

/// Builds the provider for one collector. Called lazily on first enable and
/// retried after load failures; receives the current collection interval.
pub type ProviderFactory =
    Box<dyn FnMut(Duration) -> Result<Box<dyn Provider>, ProviderError> + Send>;

/// How to build and schedule one collector.
pub struct CollectorSpec {
    /// Stable collector name, used in logs.
    pub name: &'static str,
    /// Reads `(enabled, interval)` from the runtime config.
    pub schedule: fn(&AgentRuntimeConfig) -> (bool, Duration),
    /// Builds the provider. Called lazily on first enable and retried after
    /// load failures.
    pub factory: ProviderFactory,
    /// Whether to sleep one interval before each collection. Providers that
    /// measure rates over the window (e.g. eBPF) need a full elapsed window;
    /// point-in-time readers collect immediately.
    pub sleep_before_collect: bool,
}

/// One spec per provider.
pub fn collector_specs() -> Vec<CollectorSpec> {
    vec![
        CollectorSpec {
            name: "storage",
            schedule: |config| {
                (
                    config.storage.enabled,
                    Duration::from_secs(config.storage.interval_seconds),
                )
            },
            factory: Box::new(|_| {
                Ok(Box::new(StorageProvider::new(Vec::new())) as Box<dyn Provider>)
            }),
            sleep_before_collect: false,
        },
        CollectorSpec {
            name: "memory",
            schedule: |config| {
                (
                    config.memory.enabled,
                    Duration::from_secs(config.memory.interval_seconds),
                )
            },
            factory: Box::new(|_| Ok(Box::new(MemoryProvider::new()) as Box<dyn Provider>)),
            sleep_before_collect: false,
        },
        CollectorSpec {
            name: "cpu",
            schedule: |config| {
                (
                    config.cpu.enabled,
                    Duration::from_secs(config.cpu.interval_seconds),
                )
            },
            factory: Box::new(|window| {
                CpuProvider::load(window).map(|provider| Box::new(provider) as Box<dyn Provider>)
            }),
            sleep_before_collect: true,
        },
        CollectorSpec {
            name: "network",
            schedule: |config| {
                (
                    config.network.enabled,
                    Duration::from_secs(config.network.interval_seconds),
                )
            },
            factory: Box::new(|window| {
                NetworkProvider::load(window)
                    .map(|provider| Box::new(provider) as Box<dyn Provider>)
            }),
            sleep_before_collect: true,
        },
        CollectorSpec {
            name: "disk_io",
            schedule: |config| {
                (
                    config.disk_io.enabled,
                    Duration::from_secs(config.disk_io.interval_seconds),
                )
            },
            factory: Box::new(|window| {
                DiskIoProvider::load(window).map(|provider| Box::new(provider) as Box<dyn Provider>)
            }),
            sleep_before_collect: true,
        },
    ]
}

/// Runs one collector forever: lazy provider load, per-interval collection,
/// and fan-out of every sample to all sinks.
pub async fn run_collector(
    mut spec: CollectorSpec,
    sinks: Vec<Arc<dyn Sink>>,
    shared_config: Arc<RwLock<AgentRuntimeConfig>>,
) {
    let mut provider: Option<Box<dyn Provider>> = None;
    let mut disabled_logged = false;

    loop {
        let config = shared_config.read().await.clone();
        let (enabled, interval) = (spec.schedule)(&config);
        if !enabled {
            if provider.take().is_some() {
                tracing::info!(collector = spec.name, "talia_collector_stopped");
            }
            if !disabled_logged {
                tracing::info!(collector = spec.name, "talia_collector_disabled");
                disabled_logged = true;
            }
            tokio::time::sleep(interval).await;
            continue;
        }
        disabled_logged = false;
        if provider.is_none() {
            tracing::info!(collector = spec.name, "talia_collector_starting");
            match (spec.factory)(interval) {
                Ok(loaded) => {
                    provider = Some(loaded);
                    tracing::info!(collector = spec.name, "talia_collector_started");
                },
                Err(error) => {
                    tracing::warn!(
                        collector = spec.name,
                        error = %error,
                        "talia_collector_start_failed"
                    );
                    tokio::time::sleep(interval).await;
                    continue;
                },
            }
        }

        if spec.sleep_before_collect {
            tokio::time::sleep(interval).await;
        }
        let Some(provider) = provider.as_mut() else {
            continue;
        };
        provider.set_window(interval);
        provider.reconfigure(&config);
        match provider.collect() {
            Ok(samples) => {
                for sample in &samples {
                    for sink in &sinks {
                        sink.emit(sample, &config.version);
                    }
                }
                tracing::debug!(
                    collector = spec.name,
                    sample_count = samples.len(),
                    "talia_collected"
                );
            },
            Err(error) => {
                tracing::warn!(
                    collector = spec.name,
                    error = %error,
                    "talia_collection_failed"
                );
            },
        }
        if !spec.sleep_before_collect {
            tokio::time::sleep(interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::collector_specs;

    #[test]
    fn registry_lists_every_provider_once() {
        let names: Vec<_> = collector_specs().iter().map(|spec| spec.name).collect();
        assert_eq!(names, ["storage", "memory", "cpu", "network", "disk_io"]);
    }
}
