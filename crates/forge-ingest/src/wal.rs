//! Write-ahead log. A batch is acknowledged only after it is durable here.
//!
//! Records are `[u32 len][u32 crc32][payload]`, payload being an Arrow IPC stream of one
//! normalized batch. Appends are group-committed: a writer task gathers requests for up to
//! `commit_interval` or `commit_bytes`, writes them, syncs once, then acknowledges them all.
//!
//! Every append returns a sequence number. [`Wal::rotate`] returns a boundary such that every
//! record appended before the call has `seq <= boundary`; [`Wal::truncate_through`] removes
//! those records once the memtable holding them is flushed.
//!
//! Two backends: local segmented files (`<dir>/<seq>.wal`, rotated per memtable) and object
//! storage (`wal/<node_id>/<seq>.wal`, one object per group commit) for disk-less deployments.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use bytes::{BufMut, Bytes, BytesMut};
use forge_model::{ForgeError, Result};
use forge_storage::ObjectStoreRef;
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjPath;
use tokio::sync::{mpsc, oneshot};

pub fn encode_batch(batch: &RecordBatch) -> Result<Bytes> {
    let mut buf = Vec::with_capacity(batch.get_array_memory_size());
    {
        let mut w = StreamWriter::try_new(&mut buf, &batch.schema())?;
        w.write(batch)?;
        w.finish()?;
    }
    Ok(Bytes::from(buf))
}

pub fn decode_batches(payload: &[u8]) -> Result<Vec<RecordBatch>> {
    let r = StreamReader::try_new(payload, None)?;
    Ok(r.collect::<std::result::Result<Vec<_>, _>>()?)
}

fn frame(payload: &[u8], out: &mut BytesMut) {
    out.put_u32_le(payload.len() as u32);
    out.put_u32_le(crc32fast::hash(payload));
    out.put_slice(payload);
}

/// Parse framed records, stopping at the first torn or corrupt one (a crash mid-write).
pub fn unframe(data: &[u8]) -> (Vec<Bytes>, bool) {
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos + 8 <= data.len() {
        let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap());
        let end = pos + 8 + len;
        if end > data.len() || crc32fast::hash(&data[pos + 8..end]) != crc {
            return (out, false);
        }
        out.push(Bytes::copy_from_slice(&data[pos + 8..end]));
        pos = end;
    }
    (out, pos == data.len())
}

enum Cmd {
    Append(Bytes, oneshot::Sender<Result<u64>>),
    Rotate(oneshot::Sender<Result<u64>>),
    Truncate(u64, oneshot::Sender<Result<()>>),
}

#[derive(Clone)]
pub struct Wal {
    tx: mpsc::Sender<Cmd>,
}

pub struct WalOptions {
    pub commit_interval: Duration,
    pub commit_bytes: usize,
}

/// Storage side of the WAL, driven by the single writer task.
#[async_trait::async_trait]
trait Backend: Send + 'static {
    /// Durably write a group of framed records; returns their sequence number.
    async fn write_group(&mut self, framed: Bytes) -> Result<u64>;
    async fn rotate(&mut self) -> Result<u64>;
    async fn truncate_through(&mut self, seq: u64) -> Result<()>;
}

impl Wal {
    fn spawn(mut backend: impl Backend, opts: WalOptions) -> Wal {
        let (tx, mut rx) = mpsc::channel::<Cmd>(4096);
        tokio::spawn(async move {
            let mut pending: Option<Cmd> = None;
            loop {
                let first = match pending.take() {
                    Some(c) => c,
                    None => match rx.recv().await {
                        Some(c) => c,
                        None => break,
                    },
                };
                match first {
                    Cmd::Rotate(reply) => {
                        let _ = reply.send(backend.rotate().await);
                    }
                    Cmd::Truncate(seq, reply) => {
                        let _ = reply.send(backend.truncate_through(seq).await);
                    }
                    Cmd::Append(payload, reply) => {
                        let mut buf = BytesMut::with_capacity(payload.len() + 8);
                        frame(&payload, &mut buf);
                        let mut replies = vec![reply];
                        let deadline = tokio::time::Instant::now() + opts.commit_interval;
                        // Gather more appends until the window closes or the group is big.
                        while buf.len() < opts.commit_bytes {
                            match tokio::time::timeout_at(deadline, rx.recv()).await {
                                Ok(Some(Cmd::Append(p, r))) => {
                                    frame(&p, &mut buf);
                                    replies.push(r);
                                }
                                Ok(Some(other)) => {
                                    pending = Some(other);
                                    break;
                                }
                                Ok(None) | Err(_) => break,
                            }
                        }
                        let result = backend.write_group(buf.freeze()).await;
                        for r in replies {
                            let _ = r.send(match &result {
                                Ok(seq) => Ok(*seq),
                                Err(e) => Err(ForgeError::internal(format!("wal write: {e}"))),
                            });
                        }
                    }
                }
            }
        });
        Wal { tx }
    }

    async fn call<T>(&self, make: impl FnOnce(oneshot::Sender<Result<T>>) -> Cmd) -> Result<T> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(make(tx))
            .await
            .map_err(|_| ForgeError::internal("wal writer stopped"))?;
        rx.await
            .map_err(|_| ForgeError::internal("wal writer dropped request"))?
    }

    /// Append a payload; resolves once it is durable. Returns its sequence number.
    pub async fn append(&self, payload: Bytes) -> Result<u64> {
        self.call(|r| Cmd::Append(payload, r)).await
    }

    pub async fn rotate(&self) -> Result<u64> {
        self.call(Cmd::Rotate).await
    }

    pub async fn truncate_through(&self, seq: u64) -> Result<()> {
        self.call(|r| Cmd::Truncate(seq, r)).await
    }

    /// Open a local WAL directory. Returns the WAL and the payloads to replay, in order, with
    /// the boundary covering them.
    pub async fn open_local(dir: &Path, opts: WalOptions) -> Result<(Wal, Vec<Bytes>, u64)> {
        std::fs::create_dir_all(dir)?;
        let mut files = list_local(dir)?;
        files.sort();
        let mut replay = Vec::new();
        for (seq, path) in &files {
            let mut data = Vec::new();
            std::fs::File::open(path)?.read_to_end(&mut data)?;
            let (records, clean) = unframe(&data);
            if !clean {
                tracing::warn!(file = %path.display(), seq, "WAL file has a torn tail; replaying the intact prefix");
            }
            replay.extend(records);
        }
        let last = files.last().map(|(s, _)| *s).unwrap_or(0);
        let backend = LocalBackend::create(dir.to_path_buf(), last + 1)?;
        Ok((Wal::spawn(backend, opts), replay, last))
    }

    /// Open an object-store WAL under `wal/<node_id>/`.
    pub async fn open_object_store(
        store: ObjectStoreRef,
        node_id: &str,
        opts: WalOptions,
    ) -> Result<(Wal, Vec<Bytes>, u64)> {
        let prefix = ObjPath::from(format!("wal/{node_id}"));
        let mut objects: Vec<(u64, ObjPath)> = store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| ForgeError::Internal(e.into()))?
            .into_iter()
            .filter_map(|m| {
                let name = m.location.filename()?.to_string();
                let seq = name.strip_suffix(".wal")?.parse().ok()?;
                Some((seq, m.location))
            })
            .collect();
        objects.sort();
        let mut replay = Vec::new();
        for (_, path) in &objects {
            let data = store
                .get(path)
                .await
                .map_err(|e| ForgeError::Internal(e.into()))?
                .bytes()
                .await
                .map_err(|e| ForgeError::Internal(e.into()))?;
            let (records, _) = unframe(&data);
            replay.extend(records);
        }
        let last = objects.last().map(|(s, _)| *s).unwrap_or(0);
        let backend = ObjectBackend {
            store,
            node_id: node_id.to_string(),
            next: last + 1,
            written: objects.into_iter().map(|(s, _)| s).collect(),
        };
        Ok((Wal::spawn(backend, opts), replay, last))
    }
}

fn list_local(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        let name = e.file_name().to_string_lossy().to_string();
        if let Some(seq) = name
            .strip_suffix(".wal")
            .and_then(|s| s.parse::<u64>().ok())
        {
            out.push((seq, e.path()));
        }
    }
    Ok(out)
}

struct LocalBackend {
    dir: PathBuf,
    seq: u64,
    file: Arc<std::fs::File>,
}

impl LocalBackend {
    fn create(dir: PathBuf, seq: u64) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(format!("{seq:020}.wal")))?;
        // Make the new file's directory entry durable.
        if let Ok(d) = std::fs::File::open(&dir) {
            let _ = d.sync_all();
        }
        Ok(LocalBackend {
            dir,
            seq,
            file: Arc::new(file),
        })
    }
}

#[async_trait::async_trait]
impl Backend for LocalBackend {
    async fn write_group(&mut self, framed: Bytes) -> Result<u64> {
        let file = self.file.clone();
        let result = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            (&*file).write_all(&framed)?;
            file.sync_data()
        })
        .await
        .map_err(|e| ForgeError::internal(e.to_string()))?;
        if let Err(e) = result {
            // The file may now end in a partial record; replay stops there, so continue in a
            // fresh file rather than appending good records after it.
            let next = LocalBackend::create(self.dir.clone(), self.seq + 1)?;
            *self = next;
            return Err(e.into());
        }
        Ok(self.seq)
    }

    async fn rotate(&mut self) -> Result<u64> {
        let done = self.seq;
        let next = LocalBackend::create(self.dir.clone(), done + 1)?;
        *self = next;
        Ok(done)
    }

    async fn truncate_through(&mut self, seq: u64) -> Result<()> {
        for (s, path) in list_local(&self.dir)? {
            if s <= seq && s != self.seq {
                std::fs::remove_file(&path)?;
            }
        }
        Ok(())
    }
}

struct ObjectBackend {
    store: ObjectStoreRef,
    node_id: String,
    next: u64,
    written: Vec<u64>,
}

#[async_trait::async_trait]
impl Backend for ObjectBackend {
    async fn write_group(&mut self, framed: Bytes) -> Result<u64> {
        let seq = self.next;
        self.next += 1;
        let path = ObjPath::from(forge_storage::paths::wal_path(&self.node_id, seq));
        self.store
            .put(&path, framed.into())
            .await
            .map_err(|e| ForgeError::Internal(e.into()))?;
        self.written.push(seq);
        Ok(seq)
    }

    async fn rotate(&mut self) -> Result<u64> {
        Ok(self.next - 1)
    }

    async fn truncate_through(&mut self, seq: u64) -> Result<()> {
        let (gone, keep): (Vec<u64>, Vec<u64>) = self.written.iter().partition(|s| **s <= seq);
        for s in gone {
            let path = ObjPath::from(forge_storage::paths::wal_path(&self.node_id, s));
            match self.store.delete(&path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(ForgeError::Internal(e.into())),
            }
        }
        self.written = keep;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts() -> WalOptions {
        WalOptions {
            commit_interval: Duration::from_millis(5),
            commit_bytes: 1 << 20,
        }
    }

    #[test]
    fn framing_stops_at_torn_tail() {
        let mut b = BytesMut::new();
        frame(b"hello", &mut b);
        frame(b"world", &mut b);
        let full = b.freeze();
        let (r, clean) = unframe(&full);
        assert_eq!(r.len(), 2);
        assert!(clean);
        let (r, clean) = unframe(&full[..full.len() - 2]);
        assert_eq!(r.len(), 1);
        assert!(!clean);
        let mut bad = full.to_vec();
        bad[10] ^= 0xff;
        assert_eq!(unframe(&bad).0.len(), 0);
    }

    #[tokio::test]
    async fn local_append_rotate_truncate_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, replay, _) = Wal::open_local(dir.path(), opts()).await.unwrap();
        assert!(replay.is_empty());
        // Concurrent appends share group commits.
        let mut handles = vec![];
        for i in 0..50u32 {
            let w = wal.clone();
            handles.push(tokio::spawn(async move {
                w.append(Bytes::from(i.to_le_bytes().to_vec())).await
            }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap().unwrap(), 1);
        }
        let boundary = wal.rotate().await.unwrap();
        assert_eq!(boundary, 1);
        assert_eq!(wal.append(Bytes::from_static(b"after")).await.unwrap(), 2);
        drop(wal);

        let (wal, replay, last) = Wal::open_local(dir.path(), opts()).await.unwrap();
        assert_eq!(replay.len(), 51);
        assert_eq!(last, 2);
        wal.truncate_through(1).await.unwrap();
        drop(wal);
        let (_, replay, _) = Wal::open_local(dir.path(), opts()).await.unwrap();
        assert_eq!(replay.len(), 1);
        assert_eq!(&replay[0][..], b"after");
    }

    #[tokio::test]
    async fn object_store_wal() {
        let store: ObjectStoreRef = Arc::new(object_store::memory::InMemory::new());
        let (wal, _, _) = Wal::open_object_store(store.clone(), "n1", opts())
            .await
            .unwrap();
        let s1 = wal.append(Bytes::from_static(b"a")).await.unwrap();
        let b = wal.rotate().await.unwrap();
        let s2 = wal.append(Bytes::from_static(b"b")).await.unwrap();
        assert!(s1 <= b && s2 > b);
        wal.truncate_through(b).await.unwrap();
        let (_, replay, _) = Wal::open_object_store(store, "n1", opts()).await.unwrap();
        assert_eq!(replay, vec![Bytes::from_static(b"b")]);
    }
}
