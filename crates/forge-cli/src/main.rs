//! `forge`: a single binary that ingests agent traces and serves trace queries.

use std::path::PathBuf;
use std::sync::Arc;

mod bench;

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
    /// Run read-only SQL against a running server (--server) or a data directory (flushed
    /// data only).
    Sql {
        query: String,
        #[arg(long)]
        server: Option<String>,
        #[arg(long)]
        project: Option<String>,
        #[arg(long, env = "FORGE_API_KEY")]
        api_key: Option<String>,
    },
    /// Built-in load generator.
    Bench {
        #[command(subcommand)]
        what: Bench,
    },
}

#[derive(Subcommand)]
enum Bench {
    /// Ingest synthetic agent traces in-process and report throughput.
    Ingest {
        #[command(flatten)]
        gen_opts: bench::GenOptions,
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
    },
    /// Load a dataset (plus one large trace), then time the query fast paths.
    Query {
        #[command(flatten)]
        gen_opts: bench::GenOptions,
        /// Spans in the large trace used for tree loads.
        #[arg(long, default_value_t = 2000)]
        big_trace_spans: usize,
        #[arg(long, default_value_t = 50)]
        iterations: usize,
        /// Skip loading and only time queries on existing data.
        #[arg(long)]
        no_load: bool,
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

async fn run_sql(
    config: &Config,
    query: &str,
    server: Option<String>,
    project: Option<String>,
    key: Option<String>,
) -> anyhow::Result<()> {
    if let Some(url) = server {
        let mut req = reqwest::Client::new()
            .post(format!("{}/v1/sql", url.trim_end_matches('/')))
            .json(&serde_json::json!({"query": query, "project": project, "format": "table"}));
        if let Some(k) = key {
            req = req.header("x-api-key", k);
        }
        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            anyhow::bail!("{status}: {body}");
        }
        println!("{body}");
        return Ok(());
    }
    let meta = forge_meta::open(&config.metastore.url)?;
    let store = forge_storage::from_url(&config.storage.url, &config.storage.options)?;
    let segs: forge_model::traits::SegmentStoreRef =
        Arc::new(forge_storage::VortexSegmentStore::new(store));
    let fts: forge_model::traits::IndexReaderRef =
        Arc::new(forge_index::FtsReader::new(segs.clone(), 64 << 20));
    let engine = forge_query::QueryEngine::new(
        meta,
        segs,
        Some(fts),
        Arc::new(forge_query::NoMemtables),
        config.query.clone(),
    );
    let r = engine.sql(query, project).await?;
    println!("{}", forge_query::to_pretty(&r.batches)?);
    eprintln!(
        "{} rows (flushed segments only; unflushed WAL data is not read offline)",
        r.num_rows()
    );
    Ok(())
}

async fn run_bench(mut config: Config, what: Bench, has_dir: bool) -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    if !has_dir {
        config = config.with_data_dir(tmp.path());
        eprintln!("bench data in {}", tmp.path().display());
    }
    config.compaction.enabled = false;
    match what {
        Bench::Ingest {
            gen_opts,
            concurrency,
        } => {
            let forge = forge_server::Forge::open(config).await?;
            let stop = tokio::sync::watch::channel(false);
            let flusher = forge.ingester.spawn_flusher(stop.1);
            let (n, took) = bench::ingest(&forge, &gen_opts, concurrency).await?;
            println!(
                "ingested {n} spans in {:.2}s: {:.0} spans/s",
                took.as_secs_f64(),
                n as f64 / took.as_secs_f64()
            );
            let t = std::time::Instant::now();
            let segs = forge.ingester.flush_now().await?;
            println!(
                "final flush: {segs} segments in {:.2}s",
                t.elapsed().as_secs_f64()
            );
            let _ = stop.0.send(true);
            let _ = flusher.await;
        }
        Bench::Query {
            gen_opts,
            big_trace_spans,
            iterations,
            no_load,
        } => {
            let project = gen_opts.project.clone();
            let mut big_trace = None;
            if !no_load {
                let forge = forge_server::Forge::open(config.clone()).await?;
                let (n, took) = bench::ingest(&forge, &gen_opts, 8).await?;
                println!("loaded {n} spans in {:.2}s", took.as_secs_f64());
                // One large trace for tree loads.
                let big = bench::GenOptions {
                    traces: 1,
                    spans_per_trace: big_trace_spans,
                    seed: gen_opts.seed + 1,
                    open_fraction: 0.0,
                    duplicate_fraction: 0.0,
                    ..gen_opts.clone()
                };
                let mut rng = <rand::rngs::StdRng as rand::SeedableRng>::seed_from_u64(big.seed);
                let spans =
                    bench::gen_trace(&big, &mut rng, 1 << 40, bench::now_ns() - 1_800_000_000_000);
                let mut t = [0u8; 16];
                t.copy_from_slice(&spans[0].trace_id);
                big_trace = Some(t);
                for r in bench::requests(&big, spans) {
                    forge.ingest_otlp(&r, None).await?;
                }
                forge.ingester.flush_now().await?;
                let segs = forge.metastore.list_segments(&Default::default()).await?;
                println!(
                    "{} segments, {} bytes",
                    segs.len(),
                    segs.iter().map(|s| s.byte_size).sum::<u64>()
                );
            }
            // Cold: a fresh process view with empty caches.
            let forge = forge_server::Forge::open(config).await?;
            if let Some(t) = big_trace {
                let r = bench::time(1, || {
                    forge_query::fastpath::trace_tree(&forge.query.reader, &project, &t, true)
                })
                .await?;
                println!("trace tree load, cold ({big_trace_spans} spans)  {r}");
            }
            bench::queries(&forge, &project, big_trace, iterations).await?;
        }
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
        Command::Sql {
            query,
            server,
            project,
            api_key,
        } => run_sql(&config, &query, server, project, api_key).await?,
        Command::Bench { what } => run_bench(config, what, cli.data_dir.is_some()).await?,
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
