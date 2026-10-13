use anyhow::Context;
use anyhow::Result;
use talia_core::config::AgentRuntimeConfig;

use super::config::collector_spec;

/// Prints the keys accepted by --set and the equivalent TOML sections.
pub(crate) fn execute(name: &str) -> Result<()> {
    collector_spec(name)?;
    let defaults = serde_json::to_value(AgentRuntimeConfig::default())
        .context("failed to serialize provider defaults")?;
    println!("Provider: {name}");
    println!("Accepted --set KEY=VALUE settings (built-in defaults):");

    let mut sections = vec![(name, "")];
    if name == "tcp" {
        sections.push(("pod_discovery", "pod_discovery."));
    }
    for (section, prefix) in sections {
        let fields = defaults
            .get(section)
            .and_then(serde_json::Value::as_object)
            .with_context(|| format!("missing [{section}] provider defaults"))?;
        for (field, default) in fields {
            let format = match (section, field.as_str()) {
                (_, "enabled") => "true | false",
                (_, "interval_seconds") => "positive integer (seconds)",
                ("storage", "mounts") => "JSON array of absolute paths",
                ("pod_discovery", "cri_socket") => "all | first | socket path",
                _ => anyhow::bail!("undocumented [{section}].{field} setting"),
            };
            let default = default
                .as_str()
                .map_or_else(|| default.to_string(), str::to_string);
            println!("  {prefix}{field}: {format} (default: {default})");
        }
    }
    if name == "storage" {
        println!("  mounts must contain at least one path when enabled.");
    }
    println!("Use these keys with `providers config show {name} --set` or `collect {name} --set`.");
    println!("Provider keys may also use the `{name}.` prefix with --set.");
    println!("In TOML, put provider keys under [{name}].");
    if name == "tcp" {
        println!("Put pod_discovery.cri_socket under [pod_discovery] in TOML.");
    }
    Ok(())
}
