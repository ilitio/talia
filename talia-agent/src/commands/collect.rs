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

use super::providers::config::collector_spec;
use super::providers::config::with_provider_overrides;
use crate::cli::CollectArgs;
use crate::local_config::load_local_runtime_config;

pub(crate) async fn execute(collect: &CollectArgs, path: Option<&Path>) -> Result<()> {
    let config = with_provider_overrides(
        load_local_runtime_config(path)?,
        &collect.provider,
        &collect.set,
    )?;
    let sink = StdoutSink::new();
    collect_provider(
        &collect.provider,
        collect.r#for,
        collect.interval,
        collect.keep.clone(),
        &sink,
        &config,
    )
    .await
}

/// Collects one provider's samples into a sink for the given duration.
async fn collect_provider(
    name: &str,
    duration: Duration,
    interval: Option<Duration>,
    keep: Vec<String>,
    sink: &dyn Sink,
    config: &AgentRuntimeConfig,
) -> Result<()> {
    let mut spec = collector_spec(name)?;
    let interval = interval.unwrap_or_else(|| (spec.schedule)(config).1);
    anyhow::ensure!(
        !interval.is_zero(),
        "collection interval must be greater than zero"
    );
    let mut provider =
        (spec.factory)(interval).with_context(|| format!("failed to load provider '{name}'"))?;
    provider.reconfigure(config);
    let processors: Vec<Arc<dyn Processor>> = if keep.is_empty() {
        Vec::new()
    } else {
        vec![Arc::new(NameFilter::new(keep))]
    };
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
                        sink.emit(&sample, &config.version);
                    }
                }
            },
            Err(error) => {
                tracing::warn!(provider = name, error = %error, "talia_collection_failed");
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
