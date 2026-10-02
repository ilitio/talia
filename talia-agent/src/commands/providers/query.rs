use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use talia_agent::processors::NameFilter;
use talia_agent::sinks::StdoutSink;
use talia_core::config::AgentRuntimeConfig;
use talia_core::pipeline::Processor;
use talia_core::pipeline::Sink;
use talia_core::pipeline::process_sample;

use super::config::collector_spec;
use super::config::with_provider_overrides;
use crate::local_config::load_local_runtime_config;

pub(crate) async fn execute(
    name: &str,
    duration: Duration,
    interval: Option<Duration>,
    keep: Vec<String>,
    set: &[String],
    path: Option<&Path>,
) -> Result<()> {
    let config = with_provider_overrides(load_local_runtime_config(path)?, name, set)?;
    query_provider(name, duration, interval, keep, &config).await
}

/// Collects one provider's samples to stdout for the given duration,
/// bpftrace-style.
async fn query_provider(
    name: &str,
    duration: Duration,
    interval: Option<Duration>,
    keep: Vec<String>,
    config: &AgentRuntimeConfig,
) -> Result<()> {
    let mut spec = collector_spec(name)?;
    let interval = interval.unwrap_or_else(|| (spec.schedule)(config).1);
    anyhow::ensure!(
        !interval.is_zero(),
        "query interval must be greater than zero"
    );
    let mut provider =
        (spec.factory)(interval).with_context(|| format!("failed to load provider '{name}'"))?;
    provider.reconfigure(config);
    let processors: Vec<Arc<dyn Processor>> = if keep.is_empty() {
        Vec::new()
    } else {
        vec![Arc::new(NameFilter::new(keep))]
    };
    let sink = StdoutSink::new();
    let start = std::time::Instant::now();
    let mut collected = false;
    let mut last_error = None;
    loop {
        if spec.sleep_before_collect {
            tokio::time::sleep(interval).await;
        }
        provider.set_window(interval);
        match provider.collect() {
            Ok(samples) => {
                collected = true;
                for sample in samples {
                    if let Some(sample) = process_sample(&processors, sample) {
                        sink.emit(&sample, "query");
                    }
                }
            },
            Err(error) => {
                tracing::warn!(provider = name, error = %error, "talia_query_collection_failed");
                last_error = Some(error.to_string());
            },
        }
        if start.elapsed() >= duration {
            break;
        }
        if !spec.sleep_before_collect {
            tokio::time::sleep(interval.min(duration.saturating_sub(start.elapsed()))).await;
        }
    }
    if !collected {
        anyhow::bail!(
            "provider '{name}' did not collect successfully: {}",
            last_error.unwrap_or_else(|| "no collection was attempted".to_string())
        );
    }
    Ok(())
}
