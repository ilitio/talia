use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use clap::Subcommand;

#[derive(Parser)]
#[command(
    about = "Talia host monitoring agent",
    version,
    propagate_version = true
)]
pub(crate) struct Args {
    /// Local TOML file for agent bootstrap and fallback provider settings.
    #[arg(long, global = true, env = "TALIA_AGENT_CONFIG")]
    pub(crate) config: Option<PathBuf>,
    #[arg(long, default_value = "info")]
    pub(crate) log_filter: String,
    #[command(subcommand)]
    pub(crate) command: Option<Command>,
}

#[derive(Subcommand)]
pub(crate) enum Command {
    /// Run the agent service (default when omitted).
    Run,
    /// Work with data providers.
    Providers {
        #[command(subcommand)]
        action: ProvidersAction,
    },
}

#[derive(Subcommand)]
pub(crate) enum ProvidersAction {
    /// List available provider names.
    List,
    /// List a provider's accepted settings, value formats, and built-in defaults.
    Describe {
        /// Provider name (see `providers list`).
        provider: String,
    },
    /// Print local fallback provider settings as TOML (all when omitted).
    Show {
        /// Provider name, e.g. `tcp`.
        provider: Option<String>,
        /// Override a provider setting, e.g. `--set interval_seconds=5`.
        #[arg(long, value_name = "KEY=VALUE")]
        set: Vec<String>,
    },
    /// Collect one provider's samples to stdout for a fixed duration.
    Query {
        /// Provider name (see `providers list`).
        provider: String,
        /// How long to collect, e.g. `30s`, `5m`.
        #[arg(long, value_parser = humantime::parse_duration)]
        r#for: Duration,
        /// Collection interval; defaults to the provider's local setting.
        #[arg(long, value_parser = humantime::parse_duration)]
        interval: Option<Duration>,
        /// Only keep samples whose name starts with one of these prefixes.
        #[arg(long)]
        keep: Vec<String>,
        /// Override a provider setting, e.g. `--set pod_discovery.cri_socket=all`.
        #[arg(long, value_name = "KEY=VALUE")]
        set: Vec<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_cli_does_not_accept_secret_flags() {
        // Given: the scenario for agent cli does not accept secret flags is prepared.
        // When: the behavior under test runs.
        use clap::CommandFactory;
        let command = Args::command();
        let long_flags = command
            .get_arguments()
            .filter_map(|argument| argument.get_long())
            .map(str::to_string)
            .collect::<Vec<_>>();

        // Then: the assertions confirm that agent cli does not accept secret flags.
        assert!(!long_flags.contains(&"control-token".to_string()));
        assert!(!long_flags.contains(&"agent-config-token".to_string()));
    }
}
