//! Runs the Talia host monitoring agent.

#![deny(warnings)]
#![allow(
    clippy::disallowed_methods,
    reason = "the CLI reads documented TALIA_* exporter and secret variables at startup"
)]

mod cli;
mod commands;
mod control;
mod init_tracing;
mod local_config;

use anyhow::Result;
use clap::CommandFactory;
use clap::Parser;
use cli::Args;
use cli::Command;
use cli::ProvidersAction;

#[tokio::main]
async fn main() {
    if let Err(error) = dispatch_command().await {
        eprintln!("talia-agent failed: {error:#}");
        std::process::exit(1);
    }
}

async fn dispatch_command() -> Result<()> {
    // Select the process-wide TLS provider before HTTP or WebSocket clients are created.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let args = Args::parse();
    init_tracing::init(&args.log_filter);

    match &args.command {
        Some(Command::Providers { action }) => match action {
            ProvidersAction::List => commands::providers::list::execute(),
            ProvidersAction::Describe { provider } => {
                commands::providers::describe::execute(provider)
            },
            ProvidersAction::Show { provider, set } => {
                commands::providers::show::execute(provider.as_deref(), args.config.as_deref(), set)
            },
            ProvidersAction::Query {
                provider,
                r#for,
                interval,
                keep,
                set,
            } => {
                commands::providers::query::execute(
                    provider,
                    *r#for,
                    *interval,
                    keep.clone(),
                    set,
                    args.config.as_deref(),
                )
                .await
            },
        },
        Some(Command::Run) => commands::run::execute(args.config.as_deref()).await,
        None => {
            Args::command().print_help()?;
            println!();
            Ok(())
        },
    }
}
