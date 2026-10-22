//! The `.fts` sidecar: an inverted index over one segment, laid out for object storage so a
//! search term costs at most three range reads (footer, term dictionary, postings) and the
//! first two are cached after the first query.
//!
//! ```text
//! +-------------------------------+  offset 0
//! | posting lists                 |  serialized RoaringBitmaps of row ids, in term order
//! +-------------------------------+  fst_offset
//! | term dictionary (FST)         |  term -> (posting offset << 24) | posting length
//! +-------------------------------+  file_len - FOOTER_LEN
//! | footer                        |  magic, version, rows, fst_offset, fst_len, terms
//! +-------------------------------+
//! ```
//!
//! Roaring bitmaps already partition row ids into 64K-row containers, which plays the role of
//! the row groups in the design; each term has a single bitmap.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, AsArray, RecordBatch};
use async_trait::async_trait;
use bytes::Bytes;
use forge_model::schema::INDEXED_COLUMNS;
use forge_model::traits::{IndexReader, SearchExpr, SegmentRecord, SegmentStoreRef};
use forge_model::{ForgeError, Result};
use fst::{Map, MapBuilder};
use roaring::{RoaringBitmap, RoaringTreemap};

use crate::terms::{leaf_keys, terms_for};

pub const MAGIC: &[u8; 4] = b"FFTS";
pub const VERSION: u32 = 1;
pub const FOOTER_LEN: u64 = 4 + 8 + 8 + 8 + 8 + 4 + 4;
const LEN_BITS: u64 = 24;
const MAX_POSTING_LEN: u64 = (1 << LEN_BITS) - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Footer {
    pub version: u32,
    pub num_rows: u64,
    pub fst_offset: u64,
    pub fst_len: u64,
    pub num_terms: u64,
}

impl Footer {
    fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(FOOTER_LEN as usize);
        b.extend_from_slice(&self.version.to_le_bytes());
        b.extend_from_slice(&self.num_rows.to_le_bytes());
        b.extend_from_slice(&self.fst_offset.to_le_bytes());
        b.extend_from_slice(&self.fst_len.to_le_bytes());
        b.extend_from_slice(&self.num_terms.to_le_bytes());
        b.extend_from_slice(&[0u8; 4]);
        b.extend_from_slice(MAGIC);
        b
    }

    pub fn decode(b: &[u8]) -> Result<Footer> {
        if b.len() != FOOTER_LEN as usize || &b[b.len() - 4..] != MAGIC {
            return Err(ForgeError::internal("not an fts sidecar (bad footer)"));
        }
        let u64_at = |i: usize| u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        let version = u32::from_le_bytes(b[0..4].try_into().unwrap());
        if version != VERSION {
            return Err(ForgeError::internal(format!(
                "unsupported fts version {version}"
            )));
        }
        Ok(Footer {
            version,
            num_rows: u64_at(4),
            fst_offset: u64_at(12),
            fst_len: u64_at(20),
            num_terms: u64_at(28),
        })
    }
}

/// Accumulates postings for one segment.
#[derive(Default)]
pub struct FtsBuilder {
    postings: BTreeMap<Vec<u8>, RoaringBitmap>,
    rows: u64,
}

impl FtsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_text(&mut self, row: u32, column: &str, text: &str) {
        let postings = &mut self.postings;
        terms_for(column, text, &mut |t| {
            postings.entry(t).or_default().insert(row);
        });
        self.rows = self.rows.max(row as u64 + 1);
    }

    /// Index every row of a batch (logical or storage schema). `offset` is the row id of the
    /// batch's first row within the segment.
    pub fn add_batch(&mut self, batch: &RecordBatch, offset: u32) {
        let schema = batch.schema();
        for col in INDEXED_COLUMNS {
            let Ok(i) = schema.index_of(col) else {
                continue;
            };
            let arr = batch.column(i);
            let Some(strings) = arr.as_string_opt::<i32>() else {
                continue;
            };
            for r in 0..strings.len() {
                if strings.is_valid(r) {
                    self.add_text(offset + r as u32, col, strings.value(r));
                }
            }
        }
        self.rows = self.rows.max(offset as u64 + batch.num_rows() as u64);
    }

    pub fn num_terms(&self) -> usize {
        self.postings.len()
    }

    pub fn finish(self) -> Result<Bytes> {
        let mut out: Vec<u8> = Vec::new();
        let mut fst = MapBuilder::memory();
        for (term, bitmap) in &self.postings {
            let offset = out.len() as u64;
            bitmap
                .serialize_into(&mut out)
                .map_err(|e| ForgeError::internal(format!("posting encode: {e}")))?;
            let len = out.len() as u64 - offset;
            if len > MAX_POSTING_LEN || offset >= 1 << (64 - LEN_BITS) {
                return Err(ForgeError::internal("fts sidecar too large"));
            }
            fst.insert(term, (offset << LEN_BITS) | len)
                .map_err(|e| ForgeError::internal(format!("fst: {e}")))?;
        }
        let fst_bytes = fst
            .into_inner()
            .map_err(|e| ForgeError::internal(format!("fst: {e}")))?;
        let footer = Footer {
            version: VERSION,
            num_rows: self.rows,
            fst_offset: out.len() as u64,
            fst_len: fst_bytes.len() as u64,
            num_terms: self.postings.len() as u64,
        };
        out.extend_from_slice(&fst_bytes);
        out.extend_from_slice(&footer.encode());
        Ok(Bytes::from(out))
    }
}

/// Build a sidecar for a whole segment's rows.
pub fn build(batches: &[RecordBatch]) -> Result<Bytes> {
    let mut b = FtsBuilder::new();
    let mut offset = 0u32;
    for batch in batches {
        b.add_batch(batch, offset);
        offset += batch.num_rows() as u32;
    }
    b.finish()
}

/// A loaded footer + term dictionary.
pub struct FtsDictionary {
    pub footer: Footer,
    pub fst: Map<Vec<u8>>,
}

impl FtsDictionary {
    /// `(offset, len)` of a term's postings.
    pub fn get(&self, term: &[u8]) -> Option<(u64, u64)> {
        self.fst
            .get(term)
            .map(|v| (v >> LEN_BITS, v & MAX_POSTING_LEN))
    }

    fn weight(&self) -> u32 {
        (self.fst.as_fst().size() as u64 + FOOTER_LEN).min(u32::MAX as u64) as u32
    }

    /// Parse a whole sidecar held in memory.
    pub fn from_bytes(b: &[u8]) -> Result<Self> {
        if (b.len() as u64) < FOOTER_LEN {
            return Err(ForgeError::internal("fts sidecar too short"));
        }
        let footer = Footer::decode(&b[b.len() - FOOTER_LEN as usize..])?;
        let fst_bytes =
            b[footer.fst_offset as usize..(footer.fst_offset + footer.fst_len) as usize].to_vec();
        let fst = Map::new(fst_bytes).map_err(|e| ForgeError::internal(format!("fst: {e}")))?;
        Ok(FtsDictionary { footer, fst })
    }
}

/// Evaluate a query over postings fetched for its leaves.
fn eval(expr: &SearchExpr, postings: &BTreeMap<Vec<u8>, RoaringBitmap>) -> RoaringBitmap {
    match expr {
        SearchExpr::Term {
            column,
            path,
            token,
        } => {
            let mut acc = RoaringBitmap::new();
            for k in leaf_keys(column, path, token) {
                if let Some(b) = postings.get(&k) {
                    acc |= b;
                }
            }
            acc
        }
        SearchExpr::And(v) => {
            let mut it = v.iter();
            let Some(first) = it.next() else {
                return RoaringBitmap::new();
            };
            let mut acc = eval(first, postings);
            for e in it {
                if acc.is_empty() {
                    break;
                }
                acc &= eval(e, postings);
            }
            acc
        }
        SearchExpr::Or(v) => v.iter().fold(RoaringBitmap::new(), |mut acc, e| {
            acc |= eval(e, postings);
            acc
        }),
    }
}

fn all_leaf_keys(expr: &SearchExpr, out: &mut Vec<Vec<u8>>) {
    match expr {
        SearchExpr::Term {
            column,
            path,
            token,
        } => out.extend(leaf_keys(column, path, token)),
        SearchExpr::And(v) | SearchExpr::Or(v) => v.iter().for_each(|e| all_leaf_keys(e, out)),
    }
}

/// Search sidecar bytes held in memory (tests, `forge inspect`).
pub fn search_bytes(b: &[u8], expr: &SearchExpr) -> Result<RoaringBitmap> {
    let dict = FtsDictionary::from_bytes(b)?;
    let mut keys = Vec::new();
    all_leaf_keys(expr, &mut keys);
    let mut postings = BTreeMap::new();
    for k in keys {
        if let Some((off, len)) = dict.get(&k) {
            let bm = RoaringBitmap::deserialize_from(&b[off as usize..(off + len) as usize])
                .map_err(|e| ForgeError::internal(format!("posting decode: {e}")))?;
            postings.insert(k, bm);
        }
    }
    Ok(eval(expr, &postings))
}

/// [`IndexReader`] over sidecars in object storage, with dictionaries cached in memory.
pub struct FtsReader {
    store: SegmentStoreRef,
    dicts: moka::future::Cache<String, Arc<FtsDictionary>>,
}

impl FtsReader {
    pub fn new(store: SegmentStoreRef, cache_bytes: u64) -> Self {
        FtsReader {
            store,
            dicts: moka::future::Cache::builder()
                .weigher(|_k: &String, v: &Arc<FtsDictionary>| v.weight())
                .max_capacity(cache_bytes)
                .build(),
        }
    }

    pub async fn dictionary(&self, path: &str) -> Result<Arc<FtsDictionary>> {
        let store = self.store.clone();
        let p = path.to_string();
        self.dicts
            .try_get_with(p.clone(), async move {
                // Footer and dictionary: read the tail, which usually covers both in one GET.
                let size = store
                    .list_objects(&p)
                    .await?
                    .into_iter()
                    .find(|(path, _, _)| path == &p)
                    .map(|(_, _, s)| s)
                    .ok_or_else(|| ForgeError::NotFound(p.clone()))?;
                if size < FOOTER_LEN {
                    return Err(ForgeError::internal("fts sidecar too short"));
                }
                let tail_len = size.min(256 * 1024);
                let tail = store.get_range(&p, size - tail_len..size).await?;
                let footer = Footer::decode(&tail[tail.len() - FOOTER_LEN as usize..])?;
                let fst_bytes = if footer.fst_offset >= size - tail_len {
                    let start = (footer.fst_offset - (size - tail_len)) as usize;
                    tail[start..start + footer.fst_len as usize].to_vec()
                } else {
                    store
                        .get_range(&p, footer.fst_offset..footer.fst_offset + footer.fst_len)
                        .await?
                        .to_vec()
                };
                let fst =
                    Map::new(fst_bytes).map_err(|e| ForgeError::internal(format!("fst: {e}")))?;
                Ok::<_, ForgeError>(Arc::new(FtsDictionary { footer, fst }))
            })
            .await
            .map_err(|e: Arc<ForgeError>| match e.as_ref() {
                ForgeError::NotFound(p) => ForgeError::NotFound(p.clone()),
                other => ForgeError::internal(other.to_string()),
            })
    }

    pub async fn search_path(&self, path: &str, expr: &SearchExpr) -> Result<RoaringBitmap> {
        let dict = self.dictionary(path).await?;
        let mut keys = Vec::new();
        all_leaf_keys(expr, &mut keys);
        let found: Vec<(Vec<u8>, (u64, u64))> = keys
            .into_iter()
            .filter_map(|k| dict.get(&k).map(|r| (k, r)))
            .collect();
        // A conjunction with a missing term matches nothing; skip the posting reads entirely.
        let mut postings = BTreeMap::new();
        if found.is_empty() {
            return Ok(RoaringBitmap::new());
        }
        // Coalesce: one ranged read covering all postings when they are close together.
        let lo = found.iter().map(|(_, (o, _))| *o).min().unwrap();
        let hi = found.iter().map(|(_, (o, l))| o + l).max().unwrap();
        let total: u64 = found.iter().map(|(_, (_, l))| *l).sum();
        if hi - lo <= total * 4 + 64 * 1024 {
            let bytes = self.store.get_range(path, lo..hi).await?;
            for (k, (o, l)) in found {
                let s = (o - lo) as usize;
                let bm = RoaringBitmap::deserialize_from(&bytes[s..s + l as usize])
                    .map_err(|e| ForgeError::internal(format!("posting decode: {e}")))?;
                postings.insert(k, bm);
            }
        } else {
            let reads = found
                .iter()
                .map(|(_, (o, l))| self.store.get_range(path, *o..o + l));
            let results = futures::future::try_join_all(reads).await?;
            for ((k, _), bytes) in found.into_iter().zip(results) {
                let bm = RoaringBitmap::deserialize_from(&bytes[..])
                    .map_err(|e| ForgeError::internal(format!("posting decode: {e}")))?;
                postings.insert(k, bm);
            }
        }
        Ok(eval(expr, &postings))
    }
}

#[async_trait]
impl IndexReader for FtsReader {
    async fn lookup(
        &self,
        segment: &SegmentRecord,
        query: &SearchExpr,
    ) -> Result<Option<RoaringTreemap>> {
        let Some(path) = &segment.fts_path else {
            return Ok(None);
        };
        let bm = self.search_path(path, query).await?;
        Ok(Some(bm.iter().map(u64::from).collect()))
    }
}

/// Human-readable summary of a sidecar, for `forge inspect`.
pub fn describe(b: &[u8]) -> Result<String> {
    let d = FtsDictionary::from_bytes(b)?;
    let mut out = format!(
        "fts version {}, {} rows, {} terms, dictionary {} bytes, postings {} bytes\n",
        d.footer.version,
        d.footer.num_rows,
        d.footer.num_terms,
        d.footer.fst_len,
        d.footer.fst_offset
    );
    let mut per_col: BTreeMap<String, usize> = BTreeMap::new();
    let mut stream = d.fst.keys();
    use fst::Streamer;
    while let Some(k) = stream.next() {
        let col = k.split(|b| *b == 0).next().unwrap_or_default();
        *per_col
            .entry(String::from_utf8_lossy(col).into_owned())
            .or_default() += 1;
    }
    for (c, n) in per_col {
        out.push_str(&format!("  {c}: {n} terms\n"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::terms::{parse_query, path_query};
    use forge_model::row::{SpanRow, rows_to_batch};

    fn batch() -> RecordBatch {
        let rows: Vec<SpanRow> = (0..2000u32)
            .map(|i| {
                let mut r = SpanRow::new("p", [1; 16], [0; 8]);
                r.name = Some(if i % 2 == 0 {
                    "ChatOpenAI".into()
                } else {
                    "tool_call".into()
                });
                r.inputs = Some(format!(
                    r#"{{"messages":[{{"role":"user","content":"order {i} refund"}}]}}"#
                ));
                if i % 100 == 0 {
                    r.error = Some("Timeout talking to upstream".into());
                }
                r
            })
            .collect();
        rows_to_batch(&rows).unwrap()
    }

    #[test]
    fn build_and_search() {
        let b = build(&[batch()]).unwrap();
        let q = |s: &str| search_bytes(&b, &parse_query(s, None).unwrap()).unwrap();
        assert_eq!(q("refund").len(), 2000);
        assert_eq!(q("timeout").len(), 20);
        assert_eq!(q("chatopenai timeout").len(), 20);
        assert_eq!(q("name:timeout").len(), 0);
        assert_eq!(q("order 7").iter().collect::<Vec<_>>(), vec![7]);
        assert_eq!(q("nonexistent").len(), 0);
        assert_eq!(q("nonexistent OR timeout").len(), 20);
        let p = search_bytes(
            &b,
            &path_query("inputs", "$.messages[*].content", "refund").unwrap(),
        )
        .unwrap();
        assert_eq!(p.len(), 2000);
        let p = search_bytes(
            &b,
            &path_query("inputs", "$.messages[*].role", "refund").unwrap(),
        )
        .unwrap();
        assert_eq!(p.len(), 0);
        let d = FtsDictionary::from_bytes(&b).unwrap();
        assert_eq!(d.footer.num_rows, 2000);
    }

    proptest::proptest! {
        #[test]
        fn index_matches_brute_force(docs in proptest::collection::vec("[a-e ]{0,20}", 1..50), query in "[a-e]{1,2}( [a-e]{1,2})?") {
            let mut b = FtsBuilder::new();
            for (i, d) in docs.iter().enumerate() { b.add_text(i as u32, "name", d); }
            let bytes = b.finish().unwrap();
            let Ok(expr) = parse_query(&query, None) else { return Ok(()) };
            let hits = search_bytes(&bytes, &expr).unwrap();
            for (i, d) in docs.iter().enumerate() {
                let m = crate::terms::matches(&expr, &|c| (c == "name").then(|| d.clone()));
                proptest::prop_assert_eq!(m, hits.contains(i as u32));
            }
        }
    }
}
