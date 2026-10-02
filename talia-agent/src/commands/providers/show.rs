use std::path::Path;

use anyhow::Result;
use talia_agent::runner;

use super::config::collector_spec;
use super::config::with_provider_overrides;
use crate::local_config::load_local_runtime_config;

/// Prints provider settings from the local bootstrap file, including defaults.
pub(crate) fn execute(name: Option<&str>, path: Option<&Path>, set: &[String]) -> Result<()> {
    anyhow::ensure!(
        name.is_some() || set.is_empty(),
        "--set requires a provider name"
    );
    let mut config = load_local_runtime_config(path)?;
    if let Some(name) = name {
        config = with_provider_overrides(config, name, set)?;
    }
    let names: Vec<&str> = match name {
        Some(name) => vec![collector_spec(name)?.name],
        None => runner::collector_specs()
            .iter()
            .map(|spec| spec.name)
            .collect(),
    };
    match path {
        Some(path) => println!(
            "# Local fallback settings from {} (including defaults); a running agent may use cached or remote settings.",
            path.display()
        ),
        None => println!(
            "# Built-in local fallback settings; a running agent may use cached or remote settings."
        ),
    }
    if !set.is_empty() {
        println!("# Command-line --set values applied.");
    }
    for name in names {
        let body = match name {
            "storage" => toml::to_string_pretty(&config.storage)?,
            "memory" => toml::to_string_pretty(&config.memory)?,
            "cpu" => toml::to_string_pretty(&config.cpu)?,
            "network" => toml::to_string_pretty(&config.network)?,
            "disk_io" => toml::to_string_pretty(&config.disk_io)?,
            "tcp" => toml::to_string_pretty(&config.tcp)?,
            _ => unreachable!("collector names are validated above"),
        };
        println!("\n[{name}]\n{}", body.trim_end());
        if name == "tcp" {
            let discovery = toml::to_string_pretty(&config.pod_discovery)?;
            println!("\n[pod_discovery]\n{}", discovery.trim_end());
        }
    }
    Ok(())
}
