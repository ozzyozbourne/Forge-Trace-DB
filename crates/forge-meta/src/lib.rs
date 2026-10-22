//! The metastore: which segments are live, which segments hold which trace, projects and
//! compaction jobs. The default implementation is embedded SQLite, so the binary has no
//! external dependency. The [`Metastore`] trait is what a PostgreSQL implementation would add.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use forge_model::traits::{
    CompactionJob, Metastore, NewSegment, ProjectRecord, SegmentFilter, SegmentRecord, SegmentState,
};
use forge_model::{ForgeError, Result, TraceId, now_ms};
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};

/// Ordered migrations; index + 1 is the schema version.
const MIGRATIONS: &[&str] = &[r#"
    CREATE TABLE projects (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        api_key_hash TEXT UNIQUE,
        retention_days INTEGER,
        created_at_ms INTEGER NOT NULL
    );
    CREATE TABLE segments (
        id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        level INTEGER NOT NULL,
        partition_hour INTEGER NOT NULL,
        path TEXT NOT NULL UNIQUE,
        fts_path TEXT,
        row_count INTEGER NOT NULL,
        byte_size INTEGER NOT NULL,
        min_start_time INTEGER,
        max_start_time INTEGER,
        min_trace_id BLOB NOT NULL,
        max_trace_id BLOB NOT NULL,
        max_version INTEGER NOT NULL,
        created_at_ms INTEGER NOT NULL,
        state TEXT NOT NULL,
        deleted_at_ms INTEGER
    );
    CREATE INDEX segments_by_partition ON segments(project_id, partition_hour, state);
    CREATE INDEX segments_by_state ON segments(state);
    CREATE TABLE trace_index (
        project_id TEXT NOT NULL,
        trace_id BLOB NOT NULL,
        segment_id TEXT NOT NULL,
        PRIMARY KEY (project_id, trace_id, segment_id)
    ) WITHOUT ROWID;
    CREATE INDEX trace_index_by_segment ON trace_index(segment_id);
    CREATE TABLE compaction_jobs (
        id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        partition_hour INTEGER NOT NULL,
        inputs TEXT NOT NULL,
        outputs TEXT NOT NULL,
        status TEXT NOT NULL,
        lease_expiry_ms INTEGER NOT NULL,
        created_at_ms INTEGER NOT NULL
    );
    CREATE INDEX compaction_jobs_by_partition ON compaction_jobs(project_id, partition_hour, status);
    "#];

pub struct SqliteMetastore {
    conn: Arc<Mutex<Connection>>,
}

fn db_err(e: rusqlite::Error) -> ForgeError {
    ForgeError::Internal(anyhow::Error::new(e).context("metastore"))
}

impl SqliteMetastore {
    /// Open from a `sqlite://` URL (`sqlite:///abs/path.db`, `sqlite://rel.db`, `sqlite::memory:`).
    pub fn open_url(url: &str) -> Result<Self> {
        if url == "sqlite::memory:" || url == "sqlite://:memory:" {
            return Self::open_memory();
        }
        let path = url.strip_prefix("sqlite://").ok_or_else(|| {
            ForgeError::invalid(format!(
                "metastore url must start with sqlite://, got {url}"
            ))
        })?;
        Self::open(Path::new(path))
    }

    pub fn open(path: &Path) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        }
        let conn = Connection::open(path).map_err(db_err)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;",
        )
        .map_err(db_err)?;
        Self::init(conn)
    }

    pub fn open_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory().map_err(db_err)?)
    }

    fn init(mut conn: Connection) -> Result<Self> {
        migrate(&mut conn)?;
        Ok(SqliteMetastore {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Run a closure on the connection off the async runtime.
    async fn with<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut c = conn.lock();
            f(&mut c)
        })
        .await
        .map_err(|e| ForgeError::internal(format!("metastore task: {e}")))?
    }

    pub fn schema_version(&self) -> Result<i64> {
        let c = self.conn.lock();
        c.query_row("SELECT MAX(version) FROM schema_versions", [], |r| r.get(0))
            .map_err(db_err)
    }
}

fn migrate(conn: &mut Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_versions (version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);",
    )
    .map_err(db_err)?;
    let current: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(version), 0) FROM schema_versions",
            [],
            |r| r.get(0),
        )
        .map_err(db_err)?;
    for (i, sql) in MIGRATIONS.iter().enumerate() {
        let v = i as i64 + 1;
        if v <= current {
            continue;
        }
        let tx = conn.transaction().map_err(db_err)?;
        tx.execute_batch(sql).map_err(db_err)?;
        tx.execute(
            "INSERT INTO schema_versions(version, applied_at_ms) VALUES (?1, ?2)",
            params![v, now_ms()],
        )
        .map_err(db_err)?;
        tx.commit().map_err(db_err)?;
        tracing::info!(version = v, "applied metastore migration");
    }
    Ok(())
}

const SEGMENT_COLS: &str = "id, project_id, level, partition_hour, path, fts_path, row_count, byte_size, \
    min_start_time, max_start_time, min_trace_id, max_trace_id, max_version, created_at_ms, state, deleted_at_ms";

fn row_to_segment(r: &rusqlite::Row<'_>) -> rusqlite::Result<SegmentRecord> {
    let state: String = r.get(14)?;
    Ok(SegmentRecord {
        id: r.get(0)?,
        project_id: r.get(1)?,
        level: r.get::<_, i64>(2)? as u8,
        partition_hour: r.get(3)?,
        path: r.get(4)?,
        fts_path: r.get(5)?,
        row_count: r.get::<_, i64>(6)? as u64,
        byte_size: r.get::<_, i64>(7)? as u64,
        min_start_time: r.get(8)?,
        max_start_time: r.get(9)?,
        min_trace_id: r.get(10)?,
        max_trace_id: r.get(11)?,
        max_version: r.get(12)?,
        created_at_ms: r.get(13)?,
        state: SegmentState::parse(&state).unwrap_or(SegmentState::Deleted),
        deleted_at_ms: r.get(15)?,
    })
}

fn row_to_project(r: &rusqlite::Row<'_>) -> rusqlite::Result<ProjectRecord> {
    Ok(ProjectRecord {
        id: r.get(0)?,
        name: r.get(1)?,
        api_key_hash: r.get(2)?,
        retention_days: r.get::<_, Option<i64>>(3)?.map(|d| d as u32),
        created_at_ms: r.get(4)?,
    })
}

fn row_to_job(r: &rusqlite::Row<'_>) -> rusqlite::Result<CompactionJob> {
    let inputs: String = r.get(3)?;
    let outputs: String = r.get(4)?;
    Ok(CompactionJob {
        id: r.get(0)?,
        project_id: r.get(1)?,
        partition_hour: r.get(2)?,
        inputs: serde_json::from_str(&inputs).unwrap_or_default(),
        outputs: serde_json::from_str(&outputs).unwrap_or_default(),
        status: r.get(5)?,
        lease_expiry_ms: r.get(6)?,
    })
}

#[async_trait]
impl Metastore for SqliteMetastore {
    async fn create_project(&self, p: ProjectRecord) -> Result<()> {
        self.with(move |c| {
            let n = c
                .execute(
                    "INSERT INTO projects(id, name, api_key_hash, retention_days, created_at_ms) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(id) DO NOTHING",
                    params![p.id, p.name, p.api_key_hash, p.retention_days.map(|d| d as i64), p.created_at_ms],
                )
                .map_err(db_err)?;
            if n == 0 {
                return Err(ForgeError::invalid(format!("project {} already exists", p.id)));
            }
            Ok(())
        })
        .await
    }

    async fn get_project(&self, id: &str) -> Result<Option<ProjectRecord>> {
        let id = id.to_string();
        self.with(move |c| {
            c.query_row(
                "SELECT id, name, api_key_hash, retention_days, created_at_ms FROM projects WHERE id = ?1",
                [id],
                row_to_project,
            )
            .optional()
            .map_err(db_err)
        })
        .await
    }

    async fn project_by_key_hash(&self, hash: &str) -> Result<Option<ProjectRecord>> {
        let hash = hash.to_string();
        self.with(move |c| {
            c.query_row(
                "SELECT id, name, api_key_hash, retention_days, created_at_ms FROM projects WHERE api_key_hash = ?1",
                [hash],
                row_to_project,
            )
            .optional()
            .map_err(db_err)
        })
        .await
    }

    async fn list_projects(&self) -> Result<Vec<ProjectRecord>> {
        self.with(|c| {
            let mut st = c
                .prepare("SELECT id, name, api_key_hash, retention_days, created_at_ms FROM projects ORDER BY id")
                .map_err(db_err)?;
            let rows = st.query_map([], row_to_project).map_err(db_err)?;
            rows.collect::<rusqlite::Result<Vec<_>>>().map_err(db_err)
        })
        .await
    }

    async fn set_project_key(&self, id: &str, hash: &str) -> Result<()> {
        let (id, hash) = (id.to_string(), hash.to_string());
        self.with(move |c| {
            let n = c
                .execute(
                    "UPDATE projects SET api_key_hash = ?2 WHERE id = ?1",
                    params![id, hash],
                )
                .map_err(db_err)?;
            if n == 0 {
                return Err(ForgeError::NotFound(format!("project {id}")));
            }
            Ok(())
        })
        .await
    }

    async fn commit_segments(&self, add: Vec<NewSegment>, remove: Vec<String>) -> Result<()> {
        self.with(move |c| {
            let tx = c.transaction().map_err(db_err)?;
            {
                let mut ins = tx
                    .prepare_cached(&format!(
                        "INSERT INTO segments({SEGMENT_COLS}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)"
                    ))
                    .map_err(db_err)?;
                let mut tidx = tx
                    .prepare_cached(
                        "INSERT OR IGNORE INTO trace_index(project_id, trace_id, segment_id) VALUES (?1, ?2, ?3)",
                    )
                    .map_err(db_err)?;
                for s in &add {
                    let r = &s.record;
                    ins.execute(params![
                        r.id,
                        r.project_id,
                        r.level as i64,
                        r.partition_hour,
                        r.path,
                        r.fts_path,
                        r.row_count as i64,
                        r.byte_size as i64,
                        r.min_start_time,
                        r.max_start_time,
                        r.min_trace_id,
                        r.max_trace_id,
                        r.max_version,
                        r.created_at_ms,
                        r.state.as_str(),
                        r.deleted_at_ms,
                    ])
                    .map_err(db_err)?;
                    for t in &s.trace_ids {
                        tidx.execute(params![r.project_id, t.as_slice(), r.id]).map_err(db_err)?;
                    }
                }
                let mut del = tx
                    .prepare_cached(
                        "UPDATE segments SET state = 'deleted', deleted_at_ms = ?2 WHERE id = ?1 AND state != 'deleted'",
                    )
                    .map_err(db_err)?;
                let mut del_idx = tx
                    .prepare_cached("DELETE FROM trace_index WHERE segment_id = ?1")
                    .map_err(db_err)?;
                let now = now_ms();
                for id in &remove {
                    del.execute(params![id, now]).map_err(db_err)?;
                    del_idx.execute([id]).map_err(db_err)?;
                }
            }
            tx.commit().map_err(db_err)
        })
        .await
    }

    async fn list_segments(&self, f: &SegmentFilter) -> Result<Vec<SegmentRecord>> {
        let f = f.clone();
        self.with(move |c| {
            let mut sql = format!("SELECT {SEGMENT_COLS} FROM segments WHERE 1=1");
            let mut args: Vec<rusqlite::types::Value> = Vec::new();
            use rusqlite::types::Value as V;
            if let Some(p) = &f.project_id {
                sql.push_str(" AND project_id = ?");
                args.push(V::Text(p.clone()));
            }
            // Segments with unknown start times are always candidates.
            if let Some(min) = f.start_min {
                sql.push_str(" AND (max_start_time IS NULL OR max_start_time >= ?)");
                args.push(V::Integer(min));
            }
            if let Some(max) = f.start_max {
                sql.push_str(" AND (min_start_time IS NULL OR min_start_time < ?)");
                args.push(V::Integer(max));
            }
            if let Some(t) = &f.trace_id {
                sql.push_str(" AND min_trace_id <= ? AND max_trace_id >= ?");
                args.push(V::Blob(t.to_vec()));
                args.push(V::Blob(t.to_vec()));
            }
            if let Some(l) = f.level {
                sql.push_str(" AND level = ?");
                args.push(V::Integer(l as i64));
            }
            if let Some(h) = f.partition_hour {
                sql.push_str(" AND partition_hour = ?");
                args.push(V::Integer(h));
            }
            let states = f
                .states
                .clone()
                .unwrap_or_else(|| vec![SegmentState::Live, SegmentState::Compacting]);
            sql.push_str(&format!(
                " AND state IN ({})",
                states.iter().map(|_| "?").collect::<Vec<_>>().join(",")
            ));
            for s in &states {
                args.push(V::Text(s.as_str().to_string()));
            }
            sql.push_str(" ORDER BY partition_hour DESC, max_start_time DESC, id");
            let mut st = c.prepare(&sql).map_err(db_err)?;
            let rows = st
                .query_map(params_from_iter(args.iter()), row_to_segment)
                .map_err(db_err)?;
            rows.collect::<rusqlite::Result<Vec<_>>>().map_err(db_err)
        })
        .await
    }

    async fn segments_for_trace(
        &self,
        project_id: &str,
        trace_id: &TraceId,
    ) -> Result<Vec<SegmentRecord>> {
        let (p, t) = (project_id.to_string(), trace_id.to_vec());
        self.with(move |c| {
            let cols = SEGMENT_COLS
                .split(", ")
                .map(|x| format!("s.{x}"))
                .collect::<Vec<_>>()
                .join(", ");
            let mut st = c
                .prepare_cached(&format!(
                    "SELECT {cols} FROM trace_index t JOIN segments s ON s.id = t.segment_id \
                     WHERE t.project_id = ?1 AND t.trace_id = ?2 AND s.state != 'deleted' ORDER BY s.created_at_ms"
                ))
                .map_err(db_err)?;
            let rows = st.query_map(params![p, t], row_to_segment).map_err(db_err)?;
            rows.collect::<rusqlite::Result<Vec<_>>>().map_err(db_err)
        })
        .await
    }

    async fn all_object_paths(&self) -> Result<HashSet<String>> {
        self.with(|c| {
            let mut st = c
                .prepare("SELECT path, fts_path FROM segments")
                .map_err(db_err)?;
            let mut out = HashSet::new();
            let rows = st
                .query_map([], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
                })
                .map_err(db_err)?;
            for row in rows {
                let (p, f) = row.map_err(db_err)?;
                out.insert(p);
                if let Some(f) = f {
                    out.insert(f);
                }
            }
            Ok(out)
        })
        .await
    }

    async fn purge_segments(&self, ids: &[String]) -> Result<()> {
        let ids = ids.to_vec();
        self.with(move |c| {
            let tx = c.transaction().map_err(db_err)?;
            for id in &ids {
                tx.execute("DELETE FROM trace_index WHERE segment_id = ?1", [id])
                    .map_err(db_err)?;
                tx.execute(
                    "DELETE FROM segments WHERE id = ?1 AND state = 'deleted'",
                    [id],
                )
                .map_err(db_err)?;
            }
            tx.commit().map_err(db_err)
        })
        .await
    }

    async fn expire_partitions(&self, project_id: &str, before_hour: i64) -> Result<usize> {
        let p = project_id.to_string();
        self.with(move |c| {
            let tx = c.transaction().map_err(db_err)?;
            tx.execute(
                "DELETE FROM trace_index WHERE segment_id IN (SELECT id FROM segments WHERE project_id = ?1 AND partition_hour < ?2 AND state != 'deleted')",
                params![p, before_hour],
            )
            .map_err(db_err)?;
            let n = tx
                .execute(
                    "UPDATE segments SET state = 'deleted', deleted_at_ms = ?3 WHERE project_id = ?1 AND partition_hour < ?2 AND state != 'deleted'",
                    params![p, before_hour, now_ms()],
                )
                .map_err(db_err)?;
            tx.commit().map_err(db_err)?;
            Ok(n)
        })
        .await
    }

    async fn lease_compaction(
        &self,
        project_id: &str,
        partition_hour: i64,
        inputs: &[String],
        lease_ms: i64,
    ) -> Result<Option<CompactionJob>> {
        let (p, inputs) = (project_id.to_string(), inputs.to_vec());
        self.with(move |c| {
            let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate).map_err(db_err)?;
            let now = now_ms();
            let held: Option<String> = tx
                .query_row(
                    "SELECT id FROM compaction_jobs WHERE project_id = ?1 AND partition_hour = ?2 AND status = 'running' AND lease_expiry_ms > ?3",
                    params![p, partition_hour, now],
                    |r| r.get(0),
                )
                .optional()
                .map_err(db_err)?;
            if held.is_some() {
                return Ok(None);
            }
            // Expired leases are abandoned.
            tx.execute(
                "UPDATE compaction_jobs SET status = 'expired' WHERE project_id = ?1 AND partition_hour = ?2 AND status = 'running'",
                params![p, partition_hour],
            )
            .map_err(db_err)?;
            let job = CompactionJob {
                id: ulid::Ulid::new().to_string(),
                project_id: p.clone(),
                partition_hour,
                inputs: inputs.clone(),
                outputs: vec![],
                status: "running".into(),
                lease_expiry_ms: now + lease_ms,
            };
            tx.execute(
                "INSERT INTO compaction_jobs(id, project_id, partition_hour, inputs, outputs, status, lease_expiry_ms, created_at_ms) VALUES (?1,?2,?3,?4,'[]','running',?5,?6)",
                params![job.id, p, partition_hour, serde_json::to_string(&inputs).unwrap(), job.lease_expiry_ms, now],
            )
            .map_err(db_err)?;
            for id in &inputs {
                tx.execute("UPDATE segments SET state = 'compacting' WHERE id = ?1 AND state = 'live'", [id])
                    .map_err(db_err)?;
            }
            tx.commit().map_err(db_err)?;
            Ok(Some(job))
        })
        .await
    }

    async fn finish_compaction(
        &self,
        job_id: &str,
        outputs: &[String],
        status: &str,
    ) -> Result<()> {
        let (id, outputs, status) = (job_id.to_string(), outputs.to_vec(), status.to_string());
        self.with(move |c| {
            let tx = c.transaction().map_err(db_err)?;
            let inputs: Option<String> = tx
                .query_row(
                    "SELECT inputs FROM compaction_jobs WHERE id = ?1",
                    [&id],
                    |r| r.get(0),
                )
                .optional()
                .map_err(db_err)?;
            tx.execute(
                "UPDATE compaction_jobs SET outputs = ?2, status = ?3 WHERE id = ?1",
                params![id, serde_json::to_string(&outputs).unwrap(), status],
            )
            .map_err(db_err)?;
            if status != "done" {
                // Release inputs that were not replaced.
                let inputs: Vec<String> = inputs
                    .and_then(|s| serde_json::from_str(&s).ok())
                    .unwrap_or_default();
                for i in inputs {
                    tx.execute(
                        "UPDATE segments SET state = 'live' WHERE id = ?1 AND state = 'compacting'",
                        [i],
                    )
                    .map_err(db_err)?;
                }
            }
            tx.commit().map_err(db_err)
        })
        .await
    }

    async fn list_compaction_jobs(&self, limit: usize) -> Result<Vec<CompactionJob>> {
        self.with(move |c| {
            let mut st = c
                .prepare("SELECT id, project_id, partition_hour, inputs, outputs, status, lease_expiry_ms FROM compaction_jobs ORDER BY created_at_ms DESC, id DESC LIMIT ?1")
                .map_err(db_err)?;
            let rows = st.query_map([limit as i64], row_to_job).map_err(db_err)?;
            rows.collect::<rusqlite::Result<Vec<_>>>().map_err(db_err)
        })
        .await
    }

    async fn max_version(&self) -> Result<i64> {
        self.with(|c| {
            c.query_row(
                "SELECT COALESCE(MAX(max_version), 0) FROM segments",
                [],
                |r| r.get(0),
            )
            .map_err(db_err)
        })
        .await
    }
}

/// Open the metastore named by a config URL.
pub fn open(url: &str) -> Result<Arc<dyn Metastore>> {
    if url.starts_with("sqlite:") {
        Ok(Arc::new(SqliteMetastore::open_url(url)?))
    } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        Err(ForgeError::invalid(
            "the PostgreSQL metastore is planned for multi-node setups; v1 ships SQLite only",
        ))
    } else {
        Err(ForgeError::invalid(format!(
            "unsupported metastore url {url}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_model::traits::{HOUR_NS, NewSegment};

    fn seg(id: &str, project: &str, hour: i64, level: u8, t0: u8, t1: u8) -> SegmentRecord {
        SegmentRecord {
            id: id.into(),
            project_id: project.into(),
            level,
            partition_hour: hour,
            path: format!("segments/{project}/{id}.vortex"),
            fts_path: Some(format!("segments/{project}/{id}.fts")),
            row_count: 10,
            byte_size: 100,
            min_start_time: Some(hour),
            max_start_time: Some(hour + 1000),
            min_trace_id: vec![t0; 16],
            max_trace_id: vec![t1; 16],
            max_version: 42,
            created_at_ms: now_ms(),
            state: SegmentState::Live,
            deleted_at_ms: None,
        }
    }

    #[tokio::test]
    async fn segments_and_trace_index() {
        let dir = tempfile::tempdir().unwrap();
        let m = SqliteMetastore::open(&dir.path().join("meta.db")).unwrap();
        assert_eq!(m.schema_version().unwrap(), 1);
        m.commit_segments(
            vec![
                NewSegment {
                    record: seg("a", "p", 0, 0, 1, 5),
                    trace_ids: vec![[1; 16], [5; 16]],
                },
                NewSegment {
                    record: seg("b", "p", HOUR_NS, 0, 3, 9),
                    trace_ids: vec![[3; 16], [9; 16]],
                },
                NewSegment {
                    record: seg("c", "q", 0, 0, 1, 1),
                    trace_ids: vec![[1; 16]],
                },
            ],
            vec![],
        )
        .await
        .unwrap();
        let p = |s: &[SegmentRecord]| s.iter().map(|x| x.id.clone()).collect::<Vec<_>>();
        let all = m
            .list_segments(&SegmentFilter {
                project_id: Some("p".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(p(&all), vec!["b", "a"]);
        let t = m
            .list_segments(&SegmentFilter {
                project_id: Some("p".into()),
                trace_id: Some([4; 16]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(p(&t), vec!["b", "a"]);
        let t = m
            .list_segments(&SegmentFilter {
                project_id: Some("p".into()),
                start_min: Some(HOUR_NS),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(p(&t), vec!["b"]);
        assert_eq!(
            p(&m.segments_for_trace("p", &[1; 16]).await.unwrap()),
            vec!["a"]
        );
        assert_eq!(m.max_version().await.unwrap(), 42);

        // Replace a with d atomically.
        m.commit_segments(
            vec![NewSegment {
                record: seg("d", "p", 0, 1, 1, 5),
                trace_ids: vec![[1; 16]],
            }],
            vec!["a".into()],
        )
        .await
        .unwrap();
        assert_eq!(
            p(&m.segments_for_trace("p", &[1; 16]).await.unwrap()),
            vec!["d"]
        );
        let deleted = m
            .list_segments(&SegmentFilter {
                states: Some(vec![SegmentState::Deleted]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(p(&deleted), vec!["a"]);
        assert!(
            m.all_object_paths()
                .await
                .unwrap()
                .contains("segments/p/a.vortex")
        );
        m.purge_segments(&["a".into()]).await.unwrap();
        assert!(
            !m.all_object_paths()
                .await
                .unwrap()
                .contains("segments/p/a.vortex")
        );

        assert_eq!(m.expire_partitions("p", HOUR_NS).await.unwrap(), 1);
        assert_eq!(
            p(&m.list_segments(&SegmentFilter {
                project_id: Some("p".into()),
                ..Default::default()
            })
            .await
            .unwrap()),
            vec!["b"]
        );

        // Reopen: migrations are idempotent and data persists.
        drop(m);
        let m = SqliteMetastore::open(&dir.path().join("meta.db")).unwrap();
        assert_eq!(m.schema_version().unwrap(), 1);
        assert_eq!(
            m.list_segments(&SegmentFilter::default())
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn leases_and_projects() {
        let m = SqliteMetastore::open_memory().unwrap();
        m.commit_segments(
            vec![NewSegment {
                record: seg("a", "p", 0, 0, 1, 1),
                trace_ids: vec![],
            }],
            vec![],
        )
        .await
        .unwrap();
        let job = m
            .lease_compaction("p", 0, &["a".into()], 60_000)
            .await
            .unwrap()
            .unwrap();
        assert!(
            m.lease_compaction("p", 0, &["a".into()], 60_000)
                .await
                .unwrap()
                .is_none()
        );
        let s = m.list_segments(&SegmentFilter::default()).await.unwrap();
        assert_eq!(s[0].state, SegmentState::Compacting);
        m.finish_compaction(&job.id, &[], "failed").await.unwrap();
        let s = m.list_segments(&SegmentFilter::default()).await.unwrap();
        assert_eq!(s[0].state, SegmentState::Live);
        assert!(
            m.lease_compaction("p", 0, &["a".into()], 60_000)
                .await
                .unwrap()
                .is_some()
        );

        let proj = ProjectRecord {
            id: "p".into(),
            name: "P".into(),
            api_key_hash: Some("h".into()),
            retention_days: Some(7),
            created_at_ms: 1,
        };
        m.create_project(proj.clone()).await.unwrap();
        assert!(m.create_project(proj.clone()).await.is_err());
        assert_eq!(m.project_by_key_hash("h").await.unwrap().unwrap().id, "p");
        m.set_project_key("p", "h2").await.unwrap();
        assert!(m.project_by_key_hash("h").await.unwrap().is_none());
        assert_eq!(m.list_projects().await.unwrap().len(), 1);
    }
}
