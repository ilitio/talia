use anyhow::Context;
use anyhow::Result;
use talia_agent::runner;
use talia_core::config::AgentRuntimeConfig;

/// Looks up a collector spec by provider name.
pub(crate) fn collector_spec(name: &str) -> Result<runner::CollectorSpec> {
    runner::collector_specs()
        .into_iter()
        .find(|spec| spec.name == name)
        .with_context(|| {
            let known: Vec<_> = runner::collector_specs()
                .iter()
                .map(|spec| spec.name)
                .collect();
            format!(
                "unknown provider '{name}'; known providers: {}",
                known.join(", ")
            )
        })
}

/// Applies CLI settings only to the selected provider (or its shared inputs).
pub(crate) fn with_provider_overrides(
    config: AgentRuntimeConfig,
    provider: &str,
    overrides: &[String],
) -> Result<AgentRuntimeConfig> {
    collector_spec(provider)?;
    if overrides.is_empty() {
        return Ok(config);
    }
    let mut value = serde_json::to_value(config).context("failed to serialize runtime config")?;
    for assignment in overrides {
        let (key, raw) = assignment
            .split_once('=')
            .with_context(|| format!("expected KEY=VALUE after --set, got '{assignment}'"))?;
        let key = key.trim();
        let raw = raw.trim();
        anyhow::ensure!(
            !key.is_empty() && !raw.is_empty(),
            "invalid --set '{assignment}'"
        );

        let mut fields: Vec<&str> = key.split('.').collect();
        let first = fields.first().copied();
        let section =
            if first == Some(provider) || (provider == "tcp" && first == Some("pod_discovery")) {
                fields.remove(0)
            } else {
                provider
            };
        anyhow::ensure!(
            !fields.is_empty() && fields.iter().all(|field| !field.is_empty()),
            "invalid setting name '{key}'"
        );
        let mut target = value
            .get_mut(section)
            .with_context(|| format!("unknown provider config section '{section}'"))?;
        for field in fields {
            let table = target
                .as_object_mut()
                .with_context(|| format!("setting '{key}' is not a table"))?;
            let available = table.keys().cloned().collect::<Vec<_>>().join(", ");
            target = table.get_mut(field).with_context(|| {
                format!(
                    "unknown setting '{key}' for provider '{provider}'; available in [{section}]: {available}"
                )
            })?;
        }
        *target = if target.is_string() {
            let string = if raw.starts_with('"') {
                serde_json::from_str::<String>(raw)
                    .with_context(|| format!("invalid quoted value for '{key}'"))?
            } else {
                raw.to_string()
            };
            serde_json::Value::String(string)
        } else {
            serde_json::from_str(raw)
                .with_context(|| format!("invalid value for '{key}': expected a JSON literal"))?
        };
    }
    let config: AgentRuntimeConfig =
        serde_json::from_value(value).context("invalid provider settings from --set")?;
    config
        .validate()
        .context("provider settings from --set are invalid")?;
    Ok(config)
}
