//! `forge`: a single binary that ingests agent traces and serves trace queries.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use forge_model::config::Config;

#[derive(Parser)]
#[command(name = "forge", version, about = "Forge trace database")]
struct Cli {
    /// Path to forge.toml.
    #[arg(short, long, global = true, env = "FORGE_CONFIG")]
    config: Option<PathBuf>,
    /// Put storage, metastore, WAL and cache under one directory (overrides their config).
    #[arg(long, global = true, env = "FORGE_DATA_DIR")]
    data_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run every role: receivers, query API, flusher, compactor, GC.
    Serve,
}

fn load_config(cli: &Cli) -> anyhow::Result<Config> {
    let mut c = Config::load(cli.config.as_deref())?;
    if let Some(d) = &cli.data_dir {
        c = c.with_data_dir(d);
    }
    Ok(c)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cli = Cli::parse();
    let config = load_config(&cli)?;
    match cli.command {
        Command::Serve => {
            let f = forge_server::Forge::open(config).await?;
            forge_server::serve(f, async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await?;
        }
    }
    Ok(())
}
