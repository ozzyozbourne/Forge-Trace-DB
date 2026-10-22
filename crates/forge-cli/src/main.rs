//! `forge`: a single binary that ingests agent traces and serves trace queries.

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use forge_model::config::Config;
use forge_model::traits::{SegmentFilter, SegmentState, SegmentStore};

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
    /// Inspect stored objects.
    Inspect {
        #[command(subcommand)]
        what: Inspect,
    },
}

#[derive(Subcommand)]
enum Inspect {
    /// Print a segment's footer, statistics and index summary. PATH is an object path
    /// (`segments/...`) relative to the storage root, or a local `.vortex` file.
    Segment { path: String },
    /// List segments the metastore knows about.
    Segments {
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        all: bool,
    },
}

fn load_config(cli: &Cli) -> anyhow::Result<Config> {
    let mut c = Config::load(cli.config.as_deref())?;
    if let Some(d) = &cli.data_dir {
        c = c.with_data_dir(d);
    }
    Ok(c)
}

async fn inspect_segment(config: &Config, path: &str) -> anyhow::Result<()> {
    let local = std::path::Path::new(path);
    let (store, object_path) = if local.is_absolute() && local.exists() {
        let dir = local.parent().unwrap();
        let fs = object_store::local::LocalFileSystem::new_with_prefix(dir)?;
        let s: forge_storage::ObjectStoreRef = Arc::new(fs);
        (s, local.file_name().unwrap().to_string_lossy().to_string())
    } else {
        (
            forge_storage::from_url(&config.storage.url, &config.storage.options)?,
            path.to_string(),
        )
    };
    let segs = forge_storage::VortexSegmentStore::new(store);
    println!("segment {path}");
    print!("{}", segs.describe(&object_path).await?);
    if let Ok(meta) = forge_meta::open(&config.metastore.url) {
        let all = meta
            .list_segments(&SegmentFilter {
                states: Some(vec![
                    SegmentState::Live,
                    SegmentState::Compacting,
                    SegmentState::Deleted,
                ]),
                ..Default::default()
            })
            .await
            .unwrap_or_default();
        if let Some(r) = all.iter().find(|r| r.path == object_path) {
            println!("metastore: {}", serde_json::to_string_pretty(r)?);
        }
    }
    let fts = forge_storage::paths::fts_path(&object_path);
    match segs.get_object(&fts).await {
        Ok(b) => print!("index {fts}\n{}", forge_index::fts::describe(&b)?),
        Err(_) => println!("index: none"),
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
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
        Command::Inspect {
            what: Inspect::Segment { path },
        } => inspect_segment(&config, &path).await?,
        Command::Inspect {
            what: Inspect::Segments { project, all },
        } => {
            let meta = forge_meta::open(&config.metastore.url)?;
            let states = all.then(|| {
                vec![
                    SegmentState::Live,
                    SegmentState::Compacting,
                    SegmentState::Deleted,
                ]
            });
            for s in meta
                .list_segments(&SegmentFilter {
                    project_id: project,
                    states,
                    ..Default::default()
                })
                .await?
            {
                println!(
                    "{}\tL{}\t{}\trows={}\tbytes={}\t{}",
                    s.id,
                    s.level,
                    s.state.as_str(),
                    s.row_count,
                    s.byte_size,
                    s.path
                );
            }
        }
    }
    Ok(())
}
