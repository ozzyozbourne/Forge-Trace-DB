//! The query engine: one DataFusion session per SQL request, with Forge's table, functions,
//! memory limit and timeout.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SQLOptions, SessionConfig, SessionContext};
use forge_model::config::QueryConfig;
use forge_model::traits::{IndexReaderRef, MetastoreRef, SegmentStoreRef};
use forge_model::{ForgeError, Result};

use crate::provider::SpansTableProvider;
use crate::reader::{MemtableSource, Reader};
use crate::udf::{TraceTreeFunction, udfs};

#[derive(Clone)]
pub struct QueryEngine {
    pub reader: Reader,
    pub config: QueryConfig,
}

pub struct SqlResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

impl SqlResult {
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
}

fn df_to_forge(e: datafusion::error::DataFusionError) -> ForgeError {
    use datafusion::error::DataFusionError as D;
    match e {
        D::External(inner) => match inner.downcast::<ForgeError>() {
            Ok(f) => *f,
            Err(other) => ForgeError::Internal(anyhow::anyhow!("{other}")),
        },
        D::Plan(m) | D::SQL(_, Some(m)) => ForgeError::InvalidInput(m),
        D::ResourcesExhausted(m) => {
            ForgeError::InvalidInput(format!("query exceeded its memory limit: {m}"))
        }
        D::Context(c, inner) => match df_to_forge(*inner) {
            ForgeError::InvalidInput(m) => ForgeError::InvalidInput(format!("{c}: {m}")),
            other => other,
        },
        other => {
            let msg = other.to_string();
            // Parse, planning and type errors are the caller's.
            if msg.contains("SQL error")
                || msg.contains("Error during planning")
                || msg.contains("Schema error")
                || msg.contains("not supported")
            {
                ForgeError::InvalidInput(msg)
            } else {
                ForgeError::Internal(anyhow::anyhow!(msg))
            }
        }
    }
}

impl QueryEngine {
    pub fn new(
        metastore: MetastoreRef,
        segments: SegmentStoreRef,
        index: Option<IndexReaderRef>,
        mem: Arc<dyn MemtableSource>,
        config: QueryConfig,
    ) -> Self {
        QueryEngine {
            reader: Reader {
                metastore,
                segments,
                index,
                mem,
            },
            config,
        }
    }

    /// A session with `spans`, the UDFs and `trace_tree` registered. `scope` restricts every
    /// table to one project.
    pub fn session(&self, scope: Option<String>) -> Result<SessionContext> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(self.config.memory_limit.bytes() as usize, 1.0)
            .build_arc()
            .map_err(df_to_forge)?;
        let cfg = SessionConfig::new()
            .with_information_schema(true)
            .with_target_partitions(
                std::thread::available_parallelism()
                    .map_or(4, |n| n.get())
                    .min(8),
            );
        let ctx = SessionContext::new_with_config_rt(cfg, runtime);
        ctx.register_table(
            "spans",
            Arc::new(SpansTableProvider::new(self.reader.clone(), scope.clone())),
        )
        .map_err(df_to_forge)?;
        for u in udfs() {
            ctx.register_udf(u);
        }
        ctx.register_udtf(
            "trace_tree",
            Arc::new(TraceTreeFunction {
                reader: self.reader.clone(),
                scope,
            }),
        );
        Ok(ctx)
    }

    /// Run read-only SQL.
    pub async fn sql(&self, query: &str, scope: Option<String>) -> Result<SqlResult> {
        let ctx = self.session(scope)?;
        let opts = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let timeout: Duration = self.config.timeout.get();
        let run = async {
            let df = ctx
                .sql_with_options(query, opts)
                .await
                .map_err(df_to_forge)?;
            let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
            let batches = df.collect().await.map_err(df_to_forge)?;
            Ok::<_, ForgeError>(SqlResult { schema, batches })
        };
        match tokio::time::timeout(timeout, run).await {
            Ok(r) => r,
            Err(_) => Err(ForgeError::Timeout(format!("query exceeded {timeout:?}"))),
        }
    }
}

/// Binary ids as hex, for JSON output.
fn hexify(batch: &RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut fields = Vec::new();
    let mut cols: Vec<ArrayRef> = Vec::new();
    for (f, c) in schema.fields().iter().zip(batch.columns()) {
        let hex_col: Option<StringArray> = match c.data_type() {
            DataType::FixedSizeBinary(_) => {
                let a = c.as_fixed_size_binary();
                Some(
                    (0..a.len())
                        .map(|i| a.is_valid(i).then(|| hex::encode(a.value(i))))
                        .collect(),
                )
            }
            DataType::Binary => {
                let a = c.as_binary::<i32>();
                Some(
                    (0..a.len())
                        .map(|i| a.is_valid(i).then(|| hex::encode(a.value(i))))
                        .collect(),
                )
            }
            _ => None,
        };
        match hex_col {
            Some(h) => {
                fields.push(Field::new(f.name(), DataType::Utf8, f.is_nullable()));
                cols.push(Arc::new(h));
            }
            None => {
                fields.push(f.as_ref().clone());
                cols.push(c.clone());
            }
        }
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?)
}

/// Rows as a JSON array of objects.
pub fn to_json_rows(batches: &[RecordBatch]) -> Result<serde_json::Value> {
    let mut buf = Vec::new();
    {
        let mut w = arrow::json::ArrayWriter::new(&mut buf);
        for b in batches {
            w.write(&hexify(b)?)?;
        }
        w.finish()?;
    }
    if buf.is_empty() {
        return Ok(serde_json::Value::Array(vec![]));
    }
    Ok(serde_json::from_slice(&buf)?)
}

pub fn to_pretty(batches: &[RecordBatch]) -> Result<String> {
    let hexed: Vec<RecordBatch> = batches.iter().map(hexify).collect::<Result<_>>()?;
    Ok(arrow::util::pretty::pretty_format_batches(&hexed)?.to_string())
}
