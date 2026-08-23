use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::Json;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;
use tantivy::collector::{DocSetCollector, TopDocs};
use tantivy::indexer::{IndexWriterOptions, LogMergePolicy};
use tantivy::query::{AllQuery, QueryParser};
use tantivy::schema::{
    Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, STORED, STRING, TEXT,
};
use tantivy::tokenizer::NgramTokenizer;
use tantivy::{doc, Document, Index, IndexReader, TantivyDocument};
use tokio::sync::{mpsc, Mutex as AsyncMutex, OnceCell};

use crate::auth;
use crate::db::Db;
use crate::error::{AppError, Result};
use crate::resource::{ResourceClass, ResourceGovernor};
use crate::AppState;

pub(crate) mod outbox;

#[derive(Debug, Deserialize)]
pub struct SearchRequest {
    pub q: Option<String>,
    pub limit: Option<usize>,
    pub reader: Option<SearchReaderRequest>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SearchReaderRequest {
    #[default]
    Production,
    Shadow,
}

#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub query: String,
    pub hits: Vec<SearchHit>,
    pub rebuilt: bool,
    pub took_ms: u128,
    pub reader: SearchReaderRequest,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canary: Option<SearchCanaryComparison>,
}

#[derive(Debug, Serialize)]
pub struct SearchHit {
    pub work_id: i64,
    pub score: f32,
    pub title: String,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SearchCanaryComparison {
    pub production_count: usize,
    pub shadow_count: usize,
    pub id_match: bool,
    pub order_match: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SearchShadowFactReport {
    pub status: &'static str,
    pub catalog_revision: i64,
    pub catalog_revision_after: i64,
    pub shadow_applied_revision: i64,
    pub shadow_applied_revision_after: i64,
    pub search_revision: i64,
    pub search_revision_after: i64,
    pub shadow_applied_search_revision: i64,
    pub shadow_applied_search_revision_after: i64,
    pub sqlite_work_count: usize,
    pub shadow_document_count: usize,
    pub shadow_unique_work_count: usize,
    pub missing_work_ids: usize,
    pub unexpected_work_ids: usize,
    pub duplicate_documents: usize,
    pub invalid_documents: usize,
    pub sqlite_ids_sha256: String,
    pub shadow_ids_sha256: String,
    pub took_ms: u128,
}

/// A full Tantivy snapshot is fenced by both Catalog facts and the independent
/// search-source generation.  The latter advances for tag text/link changes
/// that intentionally do not churn the Catalog revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SearchIndexSnapshot {
    pub documents: usize,
    pub catalog_revision: i64,
    pub search_revision: i64,
}

#[derive(Debug, Clone)]
struct SearchIndexRow {
    id: i64,
    kind: String,
    title: String,
    category: Option<String>,
    description: Option<String>,
    source_path: Option<String>,
    tags: Option<String>,
}

impl SearchIndexRow {
    /// Text contract shared by the full rebuild and incremental outbox paths.
    ///
    /// Source paths intentionally remain searchable for compatibility with
    /// filename/folder-based discovery in a local media library. Tests for
    /// title replacement must therefore use a stable path that does not repeat
    /// the title token.
    fn body_text(&self) -> String {
        [
            self.category.as_deref().unwrap_or_default(),
            self.description.as_deref().unwrap_or_default(),
            self.source_path.as_deref().unwrap_or_default(),
            self.tags.as_deref().unwrap_or_default(),
        ]
        .join(" ")
    }
}

#[derive(Clone, Copy)]
struct SearchFields {
    work_id: Field,
    kind: Field,
    title: Field,
    title_ngram: Field,
    body: Field,
    body_ngram: Field,
}

const CJK_NGRAM_TOKENIZER: &str = "cjk_ngram_v1";
const SEARCH_INDEX_CHANNEL_CAPACITY: usize = 256;
const SEARCH_INDEX_WRITER_THREADS: usize = 1;
// Tantivy's convenience writer starts four merge threads by default. That
// is a poor fit for the N100 budget and makes first-build file creation more
// susceptible to transient Windows/NAS filesystem locking. Keep both the
// indexing and merge side single-threaded; the caller already serializes
// rebuilds and the index is not ready until the merge completes.
const SEARCH_INDEX_MERGE_THREADS: usize = 1;
// With the N100 profile's 32 MiB writer budget, the default eight-segment
// merge threshold can start a large merge while the final baseline segments
// are still being flushed.  That creates unnecessary simultaneous reads and
// writes on NAS/Windows filesystems and has produced transient permission
// failures.  Keep the bounded segments searchable and defer consolidation
// until there is a larger backlog.  Higher-memory profiles retain Tantivy's
// default merge policy.
const SEARCH_INDEX_LOW_MEMORY_BYTES: usize = 32 * 1024 * 1024;
const SEARCH_INDEX_LOW_MEMORY_MIN_SEGMENTS: usize = 32;
const SEARCH_FACT_MAX_WORKS: usize = 50_000;
const SEARCH_FACT_RECONCILIATION_BUDGET_BYTES: u64 = 16 * 1024 * 1024;
const SEARCH_FACT_RECONCILIATION_INITIAL_DELAY: Duration = Duration::from_secs(30);
const SEARCH_FACT_RECONCILIATION_POLL: Duration = Duration::from_secs(60);
const SEARCH_FACT_RECONCILIATION_ERROR_POLL: Duration = Duration::from_secs(30);
const CANDIDATE_CACHE_MAX_ENTRIES: usize = 16;
const CANDIDATE_CACHE_MAX_IDS: usize = 500_000;
const CANDIDATE_CACHE_TTL: Duration = Duration::from_secs(5);
// Keep the parser and candidate-cache key bounded before any reader or
// blocking Tantivy task is opened. Catalog query filters use the same budget.
const MAX_SEARCH_QUERY_BYTES: usize = 512;
const LEGACY_SEARCH_INDEX_NAME: &str = "production-v2";
const LEGACY_SEARCH_REBUILD_JOB_TYPE: &str = "rebuild-search-index";
const LEGACY_SEARCH_INDEX_ERROR_MAX_CHARS: usize = 2_048;
static SEARCH_REBUILD_LOCK: LazyLock<AsyncMutex<()>> = LazyLock::new(|| AsyncMutex::new(()));
static SEARCH_FACT_RECONCILIATION_LOCK: LazyLock<AsyncMutex<()>> =
    LazyLock::new(|| AsyncMutex::new(()));

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CandidateCacheKey {
    index_dir: PathBuf,
    query: String,
    limit: usize,
}

struct CandidateCacheEntry {
    created_at: Instant,
    last_used: u64,
    weight_ids: Option<usize>,
    value: Arc<OnceCell<Vec<i64>>>,
}

/// Candidate IDs are only safe to join with a Catalog snapshot carrying the
/// same revision.  Keeping the revision beside the JSON payload prevents a
/// fresh SQLite page from silently consuming IDs produced by an older index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogCandidateIds {
    pub ids: Vec<i64>,
    pub catalog_revision: i64,
    pub search_revision: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LegacySearchIndexFreshness {
    catalog_revision: i64,
    applied_revision: i64,
    search_revision: i64,
    applied_search_revision: i64,
    ready: bool,
    status_ready: bool,
}

impl LegacySearchIndexFreshness {
    fn is_current(self) -> bool {
        self.ready
            && self.status_ready
            && self.applied_revision == self.catalog_revision
            && self.applied_search_revision == self.search_revision
    }
}

#[derive(Default)]
struct CandidateCacheState {
    entries: BTreeMap<CandidateCacheKey, CandidateCacheEntry>,
    total_ids: usize,
    clock: u64,
}

impl CandidateCacheState {
    fn remove(&mut self, key: &CandidateCacheKey) {
        if let Some(entry) = self.entries.remove(key) {
            self.total_ids = self
                .total_ids
                .saturating_sub(entry.weight_ids.unwrap_or_default());
        }
    }

    fn prune_expired(&mut self, now: Instant) -> u64 {
        let expired = self
            .entries
            .iter()
            .filter(|(_, entry)| now.duration_since(entry.created_at) >= CANDIDATE_CACHE_TTL)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        let removed = expired.len() as u64;
        for key in expired {
            self.remove(&key);
        }
        removed
    }

    fn enforce_limits(&mut self, protected: Option<&CandidateCacheKey>) -> u64 {
        let mut removed = 0_u64;
        while self.entries.len() > CANDIDATE_CACHE_MAX_ENTRIES
            || self.total_ids > CANDIDATE_CACHE_MAX_IDS
        {
            let Some(key) = self
                .entries
                .iter()
                .filter(|(key, _)| protected != Some(*key))
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            self.remove(&key);
            removed = removed.saturating_add(1);
        }
        removed
    }

    fn invalidate_index(&mut self, index_dir: &Path) -> u64 {
        let keys = self
            .entries
            .keys()
            .filter(|key| key.index_dir.as_path() == index_dir)
            .cloned()
            .collect::<Vec<_>>();
        let removed = keys.len() as u64;
        for key in keys {
            self.remove(&key);
        }
        removed
    }
}

struct SearchIndexReader {
    index: Index,
    fields: SearchFields,
    reader: IndexReader,
}

impl SearchIndexReader {
    fn open(index_dir: &Path) -> Result<Self> {
        let (index, fields) = open_or_create_index(index_dir)?;
        let reader = index.reader().map_err(search_error)?;
        Ok(Self {
            index,
            fields,
            reader,
        })
    }

    fn query(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>> {
        query_open_index(&self.index, &self.reader, self.fields, query, limit)
    }

    fn reload(&self) -> Result<()> {
        self.reader.reload().map_err(search_error)
    }
}

struct SearchRuntimeInner {
    readers: AsyncMutex<BTreeMap<PathBuf, Arc<OnceCell<Arc<SearchIndexReader>>>>>,
    candidates: AsyncMutex<CandidateCacheState>,
    reader_opens: AtomicU64,
    reader_reloads: AtomicU64,
    candidate_hits: AtomicU64,
    candidate_misses: AtomicU64,
    candidate_coalesced: AtomicU64,
    candidate_evictions: AtomicU64,
    candidate_invalidations: AtomicU64,
    canary_queries: AtomicU64,
    canary_id_mismatches: AtomicU64,
    canary_order_mismatches: AtomicU64,
    canary_failures: AtomicU64,
    canary_rejections: AtomicU64,
}

#[derive(Clone)]
pub struct SearchRuntime {
    inner: Arc<SearchRuntimeInner>,
}

impl Default for SearchRuntime {
    fn default() -> Self {
        Self {
            inner: Arc::new(SearchRuntimeInner {
                readers: AsyncMutex::new(BTreeMap::new()),
                candidates: AsyncMutex::new(CandidateCacheState::default()),
                reader_opens: AtomicU64::new(0),
                reader_reloads: AtomicU64::new(0),
                candidate_hits: AtomicU64::new(0),
                candidate_misses: AtomicU64::new(0),
                candidate_coalesced: AtomicU64::new(0),
                candidate_evictions: AtomicU64::new(0),
                candidate_invalidations: AtomicU64::new(0),
                canary_queries: AtomicU64::new(0),
                canary_id_mismatches: AtomicU64::new(0),
                canary_order_mismatches: AtomicU64::new(0),
                canary_failures: AtomicU64::new(0),
                canary_rejections: AtomicU64::new(0),
            }),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct SearchRuntimeSnapshot {
    pub readers: usize,
    pub candidate_entries: usize,
    pub candidate_ids: usize,
    pub reader_opens: u64,
    pub reader_reloads: u64,
    pub candidate_hits: u64,
    pub candidate_misses: u64,
    pub candidate_coalesced: u64,
    pub candidate_evictions: u64,
    pub candidate_invalidations: u64,
    pub candidate_cache_max_entries: usize,
    pub candidate_cache_max_ids: usize,
    pub candidate_cache_ttl_millis: u64,
    pub canary_queries: u64,
    pub canary_id_mismatches: u64,
    pub canary_order_mismatches: u64,
    pub canary_failures: u64,
    pub canary_rejections: u64,
}

impl SearchRuntime {
    async fn reader(&self, index_dir: PathBuf) -> Result<Arc<SearchIndexReader>> {
        let cell = {
            let mut readers = self.inner.readers.lock().await;
            readers
                .entry(index_dir.clone())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };
        let inner = self.inner.clone();
        let reader = cell
            .get_or_try_init(|| async move {
                let worker_path = index_dir;
                let opened = tokio::task::spawn_blocking(move || {
                    SearchIndexReader::open(&worker_path).map(Arc::new)
                })
                .await
                .map_err(|error| {
                    AppError::Other(format!("search reader worker failed: {error}"))
                })??;
                inner.reader_opens.fetch_add(1, Ordering::Relaxed);
                Ok::<_, AppError>(opened)
            })
            .await?;
        Ok(reader.clone())
    }

    async fn query_index(
        &self,
        index_dir: PathBuf,
        query: String,
        limit: usize,
    ) -> Result<Vec<SearchHit>> {
        let query = bounded_search_query(&query)?;
        let reader = self.reader(index_dir).await?;
        tokio::task::spawn_blocking(move || reader.query(&query, limit))
            .await
            .map_err(|error| AppError::Other(format!("search query worker failed: {error}")))?
    }

    /// Open and retain a bounded Tantivy reader before the first interactive
    /// query.  This intentionally does not execute a user-shaped search or
    /// populate the candidate cache: an arbitrary query cannot be predicted
    /// safely, and retaining a large result set would violate the cache
    /// budget.  The opt-in startup hook uses this only to move reader mmap and
    /// segment setup out of the first request.
    pub async fn prewarm_reader(&self, index_dir: PathBuf) -> Result<()> {
        self.reader(index_dir).await.map(|_| ())
    }

    async fn candidate_ids(
        &self,
        index_dir: PathBuf,
        query: String,
        limit: usize,
    ) -> Result<Vec<i64>> {
        let query = bounded_search_query(&query)?;
        let key = CandidateCacheKey {
            index_dir: index_dir.clone(),
            query,
            limit,
        };
        let now = Instant::now();
        let (cell, existing, ready) = {
            let mut cache = self.inner.candidates.lock().await;
            let expired = cache.prune_expired(now);
            self.inner
                .candidate_evictions
                .fetch_add(expired, Ordering::Relaxed);
            cache.clock = cache.clock.saturating_add(1);
            let clock = cache.clock;
            if let Some(entry) = cache.entries.get_mut(&key) {
                entry.last_used = clock;
                (entry.value.clone(), true, entry.value.get().is_some())
            } else {
                let cell = Arc::new(OnceCell::new());
                cache.entries.insert(
                    key.clone(),
                    CandidateCacheEntry {
                        created_at: now,
                        last_used: clock,
                        weight_ids: None,
                        value: cell.clone(),
                    },
                );
                let evicted = cache.enforce_limits(Some(&key));
                self.inner
                    .candidate_evictions
                    .fetch_add(evicted, Ordering::Relaxed);
                (cell, false, false)
            }
        };
        if ready {
            self.inner.candidate_hits.fetch_add(1, Ordering::Relaxed);
        } else if existing {
            self.inner
                .candidate_coalesced
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner.candidate_misses.fetch_add(1, Ordering::Relaxed);
        }

        let runtime = self.clone();
        let worker_query = key.query.clone();
        let result = cell
            .get_or_try_init(|| async move {
                Ok::<_, AppError>(
                    runtime
                        .query_index(index_dir, worker_query, limit)
                        .await?
                        .into_iter()
                        .map(|hit| hit.work_id)
                        .collect(),
                )
            })
            .await;
        match result {
            Ok(ids) => {
                let mut cache = self.inner.candidates.lock().await;
                cache.clock = cache.clock.saturating_add(1);
                let clock = cache.clock;
                let mut added_weight = None;
                if let Some(entry) = cache.entries.get_mut(&key) {
                    if Arc::ptr_eq(&entry.value, &cell) {
                        entry.last_used = clock;
                        if entry.weight_ids.is_none() {
                            entry.weight_ids = Some(ids.len());
                            added_weight = Some(ids.len());
                        }
                    }
                }
                if let Some(weight) = added_weight {
                    cache.total_ids = cache.total_ids.saturating_add(weight);
                }
                let evicted = cache.enforce_limits(Some(&key));
                self.inner
                    .candidate_evictions
                    .fetch_add(evicted, Ordering::Relaxed);
                Ok(ids.clone())
            }
            Err(error) => {
                let mut cache = self.inner.candidates.lock().await;
                if cache
                    .entries
                    .get(&key)
                    .is_some_and(|entry| Arc::ptr_eq(&entry.value, &cell))
                {
                    cache.remove(&key);
                }
                Err(error)
            }
        }
    }

    pub async fn reset_index(&self, index_dir: &Path) {
        self.inner.readers.lock().await.remove(index_dir);
        let mut candidates = self.inner.candidates.lock().await;
        let invalidated = candidates.invalidate_index(index_dir);
        self.inner
            .candidate_invalidations
            .fetch_add(invalidated, Ordering::Relaxed);
    }

    /// Force an already-open Tantivy reader to observe a committed index and
    /// invalidate only candidate IDs derived from that index. Tantivy's
    /// delayed automatic reload is useful for ordinary readers, but an
    /// incremental outbox commit is also the freshness boundary for Catalog
    /// candidate filtering. Making that boundary explicit avoids serving a
    /// stale candidate set after a shadow/active batch commit while preserving
    /// readers and candidates for any other index directory.
    pub async fn refresh_index(&self, index_dir: &Path) -> Result<bool> {
        let reader = {
            let readers = self.inner.readers.lock().await;
            readers.get(index_dir).and_then(|cell| cell.get().cloned())
        };
        let had_reader = reader.is_some();
        if let Some(reader) = reader {
            tokio::task::spawn_blocking(move || reader.reload())
                .await
                .map_err(|error| {
                    AppError::Other(format!("search reader reload worker failed: {error}"))
                })??;
            self.inner.reader_reloads.fetch_add(1, Ordering::Relaxed);
        }
        let mut candidates = self.inner.candidates.lock().await;
        let invalidated = candidates.invalidate_index(index_dir);
        self.inner
            .candidate_invalidations
            .fetch_add(invalidated, Ordering::Relaxed);
        Ok(had_reader)
    }

    fn record_canary_comparison(&self, comparison: &SearchCanaryComparison) {
        self.inner.canary_queries.fetch_add(1, Ordering::Relaxed);
        if !comparison.id_match {
            self.inner
                .canary_id_mismatches
                .fetch_add(1, Ordering::Relaxed);
        }
        if !comparison.order_match {
            self.inner
                .canary_order_mismatches
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_canary_failure(&self) {
        self.inner.canary_failures.fetch_add(1, Ordering::Relaxed);
    }

    fn record_canary_rejection(&self) {
        self.inner.canary_rejections.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn snapshot(&self) -> SearchRuntimeSnapshot {
        let readers = self.inner.readers.lock().await.len();
        let candidates = self.inner.candidates.lock().await;
        SearchRuntimeSnapshot {
            readers,
            candidate_entries: candidates.entries.len(),
            candidate_ids: candidates.total_ids,
            reader_opens: self.inner.reader_opens.load(Ordering::Relaxed),
            reader_reloads: self.inner.reader_reloads.load(Ordering::Relaxed),
            candidate_hits: self.inner.candidate_hits.load(Ordering::Relaxed),
            candidate_misses: self.inner.candidate_misses.load(Ordering::Relaxed),
            candidate_coalesced: self.inner.candidate_coalesced.load(Ordering::Relaxed),
            candidate_evictions: self.inner.candidate_evictions.load(Ordering::Relaxed),
            candidate_invalidations: self.inner.candidate_invalidations.load(Ordering::Relaxed),
            candidate_cache_max_entries: CANDIDATE_CACHE_MAX_ENTRIES,
            candidate_cache_max_ids: CANDIDATE_CACHE_MAX_IDS,
            candidate_cache_ttl_millis: CANDIDATE_CACHE_TTL.as_millis() as u64,
            canary_queries: self.inner.canary_queries.load(Ordering::Relaxed),
            canary_id_mismatches: self.inner.canary_id_mismatches.load(Ordering::Relaxed),
            canary_order_mismatches: self.inner.canary_order_mismatches.load(Ordering::Relaxed),
            canary_failures: self.inner.canary_failures.load(Ordering::Relaxed),
            canary_rejections: self.inner.canary_rejections.load(Ordering::Relaxed),
        }
    }
}

fn compare_search_hits(production: &[SearchHit], shadow: &[SearchHit]) -> SearchCanaryComparison {
    let production_order = production.iter().map(|hit| hit.work_id).collect::<Vec<_>>();
    let shadow_order = shadow.iter().map(|hit| hit.work_id).collect::<Vec<_>>();
    let mut production_ids = production_order.clone();
    let mut shadow_ids = shadow_order.clone();
    production_ids.sort_unstable();
    shadow_ids.sort_unstable();
    SearchCanaryComparison {
        production_count: production.len(),
        shadow_count: shadow.len(),
        id_match: production_ids == shadow_ids,
        order_match: production_order == shadow_order,
    }
}

const SEARCH_INDEX_SQL: &str = r#"
    SELECT
        w.id,
        w.kind,
        w.title,
        w.category,
        w.description,
        w.source_path,
        GROUP_CONCAT(
            DISTINCT t.namespace || ':' || t.key || ' ' || t.label || ' ' || COALESCE(t.translated_label, '')
        ) AS tags
    FROM works w
    LEFT JOIN work_tags wt ON wt.work_id = w.id
    LEFT JOIN tags t ON t.id = wt.tag_id
    WHERE w.deleted_at IS NULL
    GROUP BY w.id
    ORDER BY w.updated_at DESC, w.id DESC
"#;

pub async fn search(
    State(state): State<Arc<AppState>>,
    Query(input): Query<SearchRequest>,
) -> Result<Json<SearchResponse>> {
    let query = bounded_search_query(input.q.as_deref().unwrap_or_default())?;
    let limit = input.limit.unwrap_or(48).clamp(1, 200);
    let reader = input.reader.unwrap_or_default();
    if query.is_empty() {
        if reader == SearchReaderRequest::Shadow {
            return Err(AppError::BadRequest(
                "shadow search canary query must not be empty".to_string(),
            ));
        }
        return Ok(Json(SearchResponse {
            query,
            hits: Vec::new(),
            rebuilt: false,
            took_ms: 0,
            reader,
            canary: None,
        }));
    }

    let started = Instant::now();
    match reader {
        SearchReaderRequest::Production => {
            let (index_dir, rebuilt) = production_search_index(state.clone()).await?;
            let hits = match state
                .search_runtime
                .query_index(index_dir, query.clone(), limit)
                .await
            {
                Ok(hits) => hits,
                Err(error) if is_corrupt_search_index_error(&error) => {
                    return Err(recover_search_index_after_error(&state, &error).await);
                }
                Err(error) => return Err(error),
            };
            Ok(Json(SearchResponse {
                query,
                hits,
                rebuilt,
                took_ms: started.elapsed().as_millis(),
                reader,
                canary: None,
            }))
        }
        SearchReaderRequest::Shadow => {
            let (hits, canary) = search_shadow_canary(state, query.clone(), limit).await?;
            Ok(Json(SearchResponse {
                query,
                hits,
                rebuilt: false,
                took_ms: started.elapsed().as_millis(),
                reader,
                canary: Some(canary),
            }))
        }
    }
}

async fn search_shadow_canary(
    state: Arc<AppState>,
    query: String,
    limit: usize,
) -> Result<(Vec<SearchHit>, SearchCanaryComparison)> {
    if !state.config.search_shadow_canary_enabled {
        state.search_runtime.record_canary_rejection();
        return Err(AppError::BadRequest(
            "shadow search canary is disabled".to_string(),
        ));
    }
    if !state.config.search_outbox_shadow_enabled {
        state.search_runtime.record_canary_rejection();
        return Err(AppError::BadRequest(
            "shadow search worker is disabled".to_string(),
        ));
    }
    let shadow_status = outbox::shadow_index_status(&state.db).await?;
    let production_dir = search_index_dir(&state);
    let shadow_dir = outbox::shadow_index_dir(&state.config.data_dir);
    if let Err(error) = validate_shadow_canary_state(
        &shadow_status,
        production_dir.join("meta.json").is_file(),
        shadow_dir.join("meta.json").is_file(),
    ) {
        state.search_runtime.record_canary_rejection();
        return Err(error);
    }

    let production_query = query.clone();
    let shadow_query = query;
    let (production, shadow) = tokio::join!(
        state
            .search_runtime
            .query_index(production_dir, production_query, limit,),
        state
            .search_runtime
            .query_index(shadow_dir, shadow_query, limit),
    );
    let (production, shadow) = match (production, shadow) {
        (Ok(production), Ok(shadow)) => (production, shadow),
        (Err(error), _) | (_, Err(error)) => {
            state.search_runtime.record_canary_failure();
            return Err(error);
        }
    };
    let comparison = compare_search_hits(&production, &shadow);
    state.search_runtime.record_canary_comparison(&comparison);
    Ok((shadow, comparison))
}

fn validate_shadow_canary_state(
    status: &outbox::ShadowIndexStatus,
    production_index_available: bool,
    shadow_index_available: bool,
) -> Result<()> {
    if !status.ready || status.status != "ready" {
        return Err(AppError::Overloaded {
            message: "shadow search index is not ready for canary reads".to_string(),
            retry_after_seconds: 2,
        });
    }
    if !production_index_available || !shadow_index_available {
        return Err(AppError::Overloaded {
            message: "search canary indexes are not both available".to_string(),
            retry_after_seconds: 2,
        });
    }
    Ok(())
}

pub async fn reconcile_shadow_facts(
    State(state): State<Arc<AppState>>,
) -> Result<Json<SearchShadowFactReport>> {
    Ok(Json(reconcile_shadow_facts_for_state(state).await?))
}

async fn reconcile_shadow_facts_for_state(state: Arc<AppState>) -> Result<SearchShadowFactReport> {
    let _single_flight = SEARCH_FACT_RECONCILIATION_LOCK.lock().await;
    reconcile_shadow_facts_unlocked(state).await
}

async fn reconcile_shadow_facts_unlocked(state: Arc<AppState>) -> Result<SearchShadowFactReport> {
    if !state.config.search_shadow_canary_enabled {
        state.search_runtime.record_canary_rejection();
        return Err(AppError::BadRequest(
            "shadow search canary is disabled".to_string(),
        ));
    }
    if !state.config.search_outbox_shadow_enabled {
        state.search_runtime.record_canary_rejection();
        return Err(AppError::BadRequest(
            "shadow search worker is disabled".to_string(),
        ));
    }
    let started = Instant::now();
    let initial_status = outbox::shadow_reconciliation_snapshot(&state.db).await?;
    let shadow_status = initial_status.shadow;
    let outbox_status = initial_status.outbox;
    // A freshly committed baseline is intentionally persisted as `shadow`
    // while Catalog ownership is still on the legacy writer.  It is already
    // a complete Tantivy snapshot and must be eligible for the first SQLite /
    // Tantivy fact comparison; waiting for `ready` here would deadlock the
    // promotion sequence because `ready` itself is only derived after facts
    // reconciliation.  The production reader remains gated by
    // `ensure_incremental_reader_gate`, which still requires all kind
    // ownership cutovers and a passed reconciliation.
    let baseline_reconciliation_ready = matches!(shadow_status.status.as_str(), "shadow" | "ready")
        && shadow_status.schema_version == 1
        && shadow_status.baseline_revision >= 0
        && shadow_status.baseline_search_revision >= 0
        && shadow_status.applied_revision == outbox_status.shadow_applied_revision
        && shadow_status.applied_search_revision == outbox_status.shadow_applied_search_revision
        && outbox_status.pending == 0
        && outbox_status.revision_lag == 0
        && outbox_status.search_revision_lag == 0;
    let reconciliation_can_retry = shadow_status.status == "degraded"
        && outbox_status.pending == 0
        && outbox_status.revision_lag == 0
        && outbox_status.search_revision_lag == 0;
    let reconciliation_ready = baseline_reconciliation_ready || reconciliation_can_retry;
    if !reconciliation_ready {
        state.search_runtime.record_canary_rejection();
        return Err(AppError::Overloaded {
            message: "shadow search index is not ready for fact reconciliation".to_string(),
            retry_after_seconds: 2,
        });
    }
    let shadow_dir = outbox::shadow_index_dir(&state.config.data_dir);
    if !shadow_dir.join("meta.json").is_file() {
        state.search_runtime.record_canary_rejection();
        return Err(AppError::Overloaded {
            message: "shadow search index is not available for fact reconciliation".to_string(),
            retry_after_seconds: 2,
        });
    }

    // The Search fence follows the revision represented by the baseline and
    // outbox, not every Catalog maintenance revision. Facet-count rebuilds
    // can advance catalog_state without changing searchable work documents.
    let (_, sqlite_ids) = sqlite_work_ids(&state.db).await?;
    let catalog_revision = outbox_status.catalog_revision;
    let index_snapshot = tokio::task::spawn_blocking(move || shadow_index_work_ids(&shadow_dir))
        .await
        .map_err(|error| AppError::Other(format!("shadow fact worker failed: {error}")))??;
    let search_revision = outbox_status.search_revision;
    let after_status = outbox::shadow_reconciliation_snapshot(&state.db).await?;
    let catalog_revision_after = after_status.outbox.catalog_revision;
    let search_revision_after = after_status.outbox.search_revision;
    let shadow_status_after = after_status.shadow;
    let (missing_work_ids, unexpected_work_ids) =
        sorted_id_difference_counts(&sqlite_ids, &index_snapshot.unique_ids);
    let revisions_stable = catalog_revision == shadow_status.applied_revision
        && catalog_revision == catalog_revision_after
        && shadow_status.applied_revision == shadow_status_after.applied_revision
        && search_revision == shadow_status.applied_search_revision
        && search_revision == search_revision_after
        && shadow_status.applied_search_revision == shadow_status_after.applied_search_revision
        && matches!(
            shadow_status_after.status.as_str(),
            "shadow" | "ready" | "degraded"
        );
    let facts_match = revisions_stable
        && missing_work_ids == 0
        && unexpected_work_ids == 0
        && index_snapshot.duplicate_documents == 0
        && index_snapshot.invalid_documents == 0;
    let mut report = SearchShadowFactReport {
        status: if !revisions_stable {
            "stale"
        } else if facts_match {
            "passed"
        } else {
            "failed"
        },
        catalog_revision,
        catalog_revision_after,
        shadow_applied_revision: shadow_status.applied_revision,
        shadow_applied_revision_after: shadow_status_after.applied_revision,
        search_revision,
        search_revision_after,
        shadow_applied_search_revision: shadow_status.applied_search_revision,
        shadow_applied_search_revision_after: shadow_status_after.applied_search_revision,
        sqlite_work_count: sqlite_ids.len(),
        shadow_document_count: index_snapshot.document_count,
        shadow_unique_work_count: index_snapshot.unique_ids.len(),
        missing_work_ids,
        unexpected_work_ids,
        duplicate_documents: index_snapshot.duplicate_documents,
        invalid_documents: index_snapshot.invalid_documents,
        sqlite_ids_sha256: hash_work_ids(&sqlite_ids),
        shadow_ids_sha256: hash_work_ids(&index_snapshot.unique_ids),
        took_ms: started.elapsed().as_millis(),
    };
    report.status = outbox::record_shadow_reconciliation(&state.db, &report).await?;
    Ok(report)
}

fn shadow_reconciliation_worker_enabled(config: &crate::config::Config) -> bool {
    config.search_outbox_shadow_enabled && config.search_shadow_canary_enabled
}

/// Periodically checks the shadow index against SQLite facts.
///
/// This is deliberately opt-in.  The production reader remains unchanged,
/// and the worker only runs when both the shadow outbox and canary flags are
/// explicitly enabled.  A low-frequency cadence keeps a 4 GiB NAS from
/// turning a maintenance check into continuous disk and CPU pressure.
pub(crate) fn spawn_shadow_reconciliation_worker(
    state: Arc<AppState>,
) -> Option<tokio::task::JoinHandle<()>> {
    if !shadow_reconciliation_worker_enabled(&state.config) {
        return None;
    }
    Some(tokio::spawn(async move {
        tokio::time::sleep(SEARCH_FACT_RECONCILIATION_INITIAL_DELAY).await;
        loop {
            let lease = state
                .resources
                .reserve_background(
                    ResourceClass::SearchWriter,
                    SEARCH_FACT_RECONCILIATION_BUDGET_BYTES,
                    0,
                )
                .await;
            match lease {
                Ok(resource_lease) => {
                    let result = reconcile_shadow_facts_for_state(state.clone()).await;
                    // The permit accounts for the reconciliation operation,
                    // not its polling delay. Holding the sole SearchWriter
                    // permit during the sleep can starve the shadow outbox
                    // worker and, in turn, block the incremental-reader gate.
                    drop(resource_lease);
                    match result {
                        Ok(report) => {
                            tracing::debug!(
                                status = report.status,
                                catalog_revision = report.catalog_revision,
                                shadow_applied_revision = report.shadow_applied_revision,
                                missing_work_ids = report.missing_work_ids,
                                unexpected_work_ids = report.unexpected_work_ids,
                                took_ms = report.took_ms,
                                "automatic shadow search fact reconciliation completed"
                            );
                            if state.config.search_incremental_reader_enabled
                                && report.status == "passed"
                            {
                                match arm_incremental_reader(&state).await {
                                    Ok(()) => tracing::debug!(
                                        "incremental search reader cutover armed after reconciliation"
                                    ),
                                    Err(error) => tracing::debug!(
                                        error = %error,
                                        "incremental search reader remains fail-closed after reconciliation"
                                    ),
                                }
                            }
                            tokio::time::sleep(SEARCH_FACT_RECONCILIATION_POLL).await;
                        }
                        Err(error) => {
                            tracing::debug!(
                                error = %error,
                                "automatic shadow search fact reconciliation deferred"
                            );
                            tokio::time::sleep(SEARCH_FACT_RECONCILIATION_ERROR_POLL).await;
                        }
                    }
                }
                Err(error) => {
                    tracing::debug!(
                        error = %error,
                        "automatic shadow search fact reconciliation could not reserve resources"
                    );
                    tokio::time::sleep(SEARCH_FACT_RECONCILIATION_ERROR_POLL).await;
                }
            }
        }
    }))
}

async fn sqlite_work_ids(db: &Db) -> Result<(i64, Vec<i64>)> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let revision =
        sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
            .fetch_one(&mut *transaction)
            .await?;
    let ids = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM works WHERE deleted_at IS NULL ORDER BY id LIMIT ?1",
    )
    .bind(i64::try_from(SEARCH_FACT_MAX_WORKS + 1).unwrap_or(i64::MAX))
    .fetch_all(&mut *transaction)
    .await?;
    transaction.commit().await?;
    if ids.len() > SEARCH_FACT_MAX_WORKS {
        return Err(AppError::BadRequest(format!(
            "search fact reconciliation exceeds {SEARCH_FACT_MAX_WORKS} works"
        )));
    }
    Ok((revision, ids))
}

struct ShadowIndexWorkIds {
    document_count: usize,
    unique_ids: Vec<i64>,
    duplicate_documents: usize,
    invalid_documents: usize,
}

fn shadow_index_work_ids(index_dir: &Path) -> Result<ShadowIndexWorkIds> {
    let reader = SearchIndexReader::open(index_dir)?;
    let searcher = reader.reader.searcher();
    let document_count = usize::try_from(searcher.num_docs()).unwrap_or(usize::MAX);
    if document_count > SEARCH_FACT_MAX_WORKS {
        return Err(AppError::BadRequest(format!(
            "shadow search fact reconciliation exceeds {SEARCH_FACT_MAX_WORKS} documents"
        )));
    }
    let schema = reader.index.schema();
    let addresses = searcher
        .search(&AllQuery, &DocSetCollector)
        .map_err(search_error)?;
    let mut ids = Vec::with_capacity(addresses.len());
    let mut invalid_documents = 0_usize;
    for address in addresses {
        let document: TantivyDocument = searcher.doc(address).map_err(search_error)?;
        let value: Value =
            serde_json::from_str(&document.to_json(&schema)).unwrap_or_else(|_| json!({}));
        if let Some(work_id) =
            first_stored_text(&value, "work_id").and_then(|id| id.parse::<i64>().ok())
        {
            ids.push(work_id);
        } else {
            invalid_documents = invalid_documents.saturating_add(1);
        }
    }
    ids.sort_unstable();
    let valid_documents = ids.len();
    ids.dedup();
    Ok(ShadowIndexWorkIds {
        document_count,
        duplicate_documents: valid_documents.saturating_sub(ids.len()),
        invalid_documents,
        unique_ids: ids,
    })
}

fn sorted_id_difference_counts(expected: &[i64], actual: &[i64]) -> (usize, usize) {
    let mut expected_index = 0_usize;
    let mut actual_index = 0_usize;
    let mut missing = 0_usize;
    let mut unexpected = 0_usize;
    while expected_index < expected.len() && actual_index < actual.len() {
        match expected[expected_index].cmp(&actual[actual_index]) {
            std::cmp::Ordering::Less => {
                missing = missing.saturating_add(1);
                expected_index += 1;
            }
            std::cmp::Ordering::Greater => {
                unexpected = unexpected.saturating_add(1);
                actual_index += 1;
            }
            std::cmp::Ordering::Equal => {
                expected_index += 1;
                actual_index += 1;
            }
        }
    }
    missing = missing.saturating_add(expected.len().saturating_sub(expected_index));
    unexpected = unexpected.saturating_add(actual.len().saturating_sub(actual_index));
    (missing, unexpected)
}

fn hash_work_ids(ids: &[i64]) -> String {
    let mut hasher = Sha256::new();
    hasher.update((ids.len() as u64).to_be_bytes());
    for id in ids {
        hasher.update(id.to_be_bytes());
    }
    format!("{:x}", hasher.finalize())
}

async fn legacy_search_index_freshness(db: &Db) -> Result<LegacySearchIndexFreshness> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let catalog_revision =
        sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
            .fetch_one(&mut *transaction)
            .await?;
    let search_revision = sqlx::query_scalar::<_, i64>(
        "SELECT revision FROM search_source_state WHERE singleton = 1",
    )
    .fetch_one(&mut *transaction)
    .await?;
    let state = sqlx::query(
        r#"
        SELECT applied_revision, applied_search_revision, ready, status
        FROM search_index_state
        WHERE index_name = ?1
        "#,
    )
    .bind(LEGACY_SEARCH_INDEX_NAME)
    .fetch_optional(&mut *transaction)
    .await?;
    transaction.commit().await?;
    let Some(state) = state else {
        return Ok(LegacySearchIndexFreshness {
            catalog_revision,
            applied_revision: -1,
            search_revision,
            applied_search_revision: -1,
            ready: false,
            status_ready: false,
        });
    };
    Ok(LegacySearchIndexFreshness {
        catalog_revision,
        applied_revision: state.get("applied_revision"),
        search_revision,
        applied_search_revision: state.get("applied_search_revision"),
        ready: state.get::<i64, _>("ready") != 0,
        status_ready: state.get::<String, _>("status") == "ready",
    })
}

async fn record_legacy_search_index_ready(db: &Db, snapshot: SearchIndexSnapshot) -> Result<()> {
    let indexed_documents = i64::try_from(snapshot.documents).map_err(|_| {
        AppError::Other("legacy search document count exceeds SQLite range".to_string())
    })?;
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    let result = sqlx::query(
        r#"
        UPDATE search_index_state
        SET baseline_revision = ?1,
            applied_revision = ?1,
            baseline_search_revision = ?2,
            applied_search_revision = ?2,
            indexed_documents = ?3,
            ready = 1,
            status = 'ready',
            last_error = NULL,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE index_name = ?4
        "#,
    )
    .bind(snapshot.catalog_revision)
    .bind(snapshot.search_revision)
    .bind(indexed_documents)
    .bind(LEGACY_SEARCH_INDEX_NAME)
    .execute(&mut *transaction)
    .await?;
    if result.rows_affected() != 1 {
        transaction.rollback().await?;
        return Err(AppError::Other(
            "legacy search index state row is missing".to_string(),
        ));
    }
    transaction.commit().await?;
    Ok(())
}

async fn mark_legacy_search_index_building(db: &Db) -> Result<()> {
    update_legacy_search_index_state(db, "building", false, None).await
}

async fn mark_legacy_search_index_degraded(db: &Db, error: &str) -> Result<()> {
    update_legacy_search_index_state(
        db,
        "degraded",
        false,
        Some(
            error
                .chars()
                .take(LEGACY_SEARCH_INDEX_ERROR_MAX_CHARS)
                .collect(),
        ),
    )
    .await
}

async fn update_legacy_search_index_state(
    db: &Db,
    status: &str,
    ready: bool,
    last_error: Option<String>,
) -> Result<()> {
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    let result = sqlx::query(
        r#"
        UPDATE search_index_state
        SET ready = ?1,
            status = ?2,
            last_error = ?3,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE index_name = ?4
        "#,
    )
    .bind(i64::from(ready))
    .bind(status)
    .bind(last_error)
    .bind(LEGACY_SEARCH_INDEX_NAME)
    .execute(&mut *transaction)
    .await?;
    if result.rows_affected() != 1 {
        transaction.rollback().await?;
        return Err(AppError::Other(
            "legacy search index state row is missing".to_string(),
        ));
    }
    transaction.commit().await?;
    Ok(())
}

async fn queue_legacy_search_rebuild(db: &Db) -> Result<()> {
    let (_, created) = db
        .create_job_if_absent(
            LEGACY_SEARCH_REBUILD_JOB_TYPE,
            "queued",
            json!({ "source": "search-freshness-fence" }),
        )
        .await?;
    if created {
        tracing::debug!("queued legacy search rebuild after a revision fence rejection");
    }
    Ok(())
}

async fn recover_legacy_search_index_after_error(state: &AppState, error: &AppError) -> AppError {
    if let Err(state_error) = mark_legacy_search_index_degraded(&state.db, &error.to_string()).await
    {
        tracing::error!(
            error = %state_error,
            original_error = %error,
            "failed to persist degraded legacy search state after a query error"
        );
    }
    if let Err(queue_error) = queue_legacy_search_rebuild(&state.db).await {
        tracing::error!(
            error = %queue_error,
            original_error = %error,
            "failed to queue legacy search recovery after a query error"
        );
    }
    AppError::Overloaded {
        message: "legacy search index is unavailable; rebuild has been queued".to_string(),
        retry_after_seconds: 2,
    }
}

async fn recover_incremental_search_index_after_error(
    state: &AppState,
    error: &AppError,
) -> AppError {
    if let Err(state_error) = outbox::record_shadow_error(&state.db, &error.to_string()).await {
        tracing::error!(
            error = %state_error,
            original_error = %error,
            "failed to persist degraded incremental search state after a query error"
        );
    }
    if let Err(queue_error) = state
        .db
        .create_job_if_absent(
            outbox::REBUILD_SHADOW_SEARCH_JOB_TYPE,
            "queued",
            json!({ "source": "search-index-corruption" }),
        )
        .await
    {
        tracing::error!(
            error = %queue_error,
            original_error = %error,
            "failed to queue incremental search recovery after a query error"
        );
    }
    AppError::Overloaded {
        message: "incremental search index is unavailable; shadow rebuild has been queued"
            .to_string(),
        retry_after_seconds: 2,
    }
}

async fn recover_search_index_after_error(state: &AppState, error: &AppError) -> AppError {
    if state.config.search_incremental_reader_enabled {
        recover_incremental_search_index_after_error(state, error).await
    } else {
        recover_legacy_search_index_after_error(state, error).await
    }
}

fn legacy_search_stale_error(freshness: LegacySearchIndexFreshness) -> AppError {
    AppError::Overloaded {
        message: format!(
            "legacy search index is catching up (index revision {}, catalog revision {})",
            freshness.applied_revision, freshness.catalog_revision
        ),
        retry_after_seconds: 2,
    }
}

pub(crate) async fn catalog_candidate_ids(
    state: Arc<AppState>,
    query: String,
    limit: usize,
) -> Result<CatalogCandidateIds> {
    let query = bounded_search_query(&query)?;
    if query.is_empty() {
        let revisions = state.db.revision_fence_snapshot().await?;
        return Ok(CatalogCandidateIds {
            ids: Vec::new(),
            catalog_revision: revisions.catalog_revision,
            search_revision: revisions.search_revision,
        });
    }
    let (index_dir, _) = production_search_index(state.clone()).await?;
    let (catalog_revision, search_revision) = if state.config.search_incremental_reader_enabled {
        let status = outbox::status(&state.db).await?;
        if status.pending != 0
            || status.catalog_revision != status.shadow_applied_revision
            || status.search_revision != status.shadow_applied_search_revision
        {
            return Err(AppError::Overloaded {
                message: "incremental search index is catching up".to_string(),
                retry_after_seconds: 2,
            });
        }
        (status.catalog_revision, status.search_revision)
    } else {
        let freshness = legacy_search_index_freshness(&state.db).await?;
        if !freshness.is_current() {
            queue_legacy_search_rebuild(&state.db).await?;
            return Err(legacy_search_stale_error(freshness));
        }
        (freshness.catalog_revision, freshness.search_revision)
    };
    let ids = match state
        .search_runtime
        .candidate_ids(index_dir, query, limit)
        .await
    {
        Ok(ids) => ids,
        Err(error) if is_corrupt_search_index_error(&error) => {
            return Err(recover_search_index_after_error(&state, &error).await);
        }
        Err(error) => return Err(error),
    };
    Ok(CatalogCandidateIds {
        ids,
        catalog_revision,
        search_revision,
    })
}

fn validate_incremental_reader_dependencies(state: &AppState) -> Result<()> {
    if !state.config.search_incremental_reader_enabled {
        return Ok(());
    }
    if !state.config.search_outbox_shadow_enabled {
        return Err(AppError::Other(
            "incremental search reader requires the shadow outbox worker".to_string(),
        ));
    }
    Ok(())
}

fn incremental_reader_lock_timeout(state: &AppState) -> Duration {
    Duration::from_millis(state.resources.limits().interactive_wait_timeout_millis)
}

fn ensure_incremental_reader_index_available(state: &AppState) -> Result<()> {
    if !outbox::shadow_index_dir(&state.config.data_dir)
        .join("meta.json")
        .is_file()
    {
        return Err(AppError::Other(
            "incremental search reader shadow index is not available".to_string(),
        ));
    }
    Ok(())
}

/// Validate the independent incremental-reader cutover gate without changing
/// persisted state. Interactive reads use this read-only check so an
/// unarmed deployment fails closed rather than promoting itself on its first
/// query.
pub(crate) async fn validate_incremental_reader(state: &AppState) -> Result<()> {
    validate_incremental_reader_dependencies(state)?;
    if !state.config.search_incremental_reader_enabled {
        return Ok(());
    }
    outbox::validate_incremental_reader_gate(&state.db, incremental_reader_lock_timeout(state))
        .await?;
    ensure_incremental_reader_index_available(state)
}

/// Perform the strict persisted-facts check and arm the incremental reader.
/// This is reserved for startup and the background reconciliation worker; the
/// interactive request path must call `validate_incremental_reader` instead.
pub(crate) async fn arm_incremental_reader(state: &AppState) -> Result<()> {
    validate_incremental_reader_dependencies(state)?;
    if !state.config.search_incremental_reader_enabled {
        return Ok(());
    }
    outbox::ensure_incremental_reader_gate(&state.db, incremental_reader_lock_timeout(state))
        .await?;
    ensure_incremental_reader_index_available(state)
}

/// Validate and retain the incremental production reader before readiness.
///
/// The shadow index is only eligible after the same persisted reconciliation
/// gate used by interactive reads.  Keeping the gate check here prevents a
/// startup hook from opening a stale or degraded index, while the bounded
/// caller timeout preserves the normal fail-closed/lazy fallback.
pub async fn prewarm_incremental_reader(state: Arc<AppState>) -> Result<()> {
    if !state.config.search_incremental_reader_enabled {
        return Ok(());
    }
    arm_incremental_reader(&state).await?;
    state
        .search_runtime
        .prewarm_reader(outbox::shadow_index_dir(&state.config.data_dir))
        .await
}

async fn production_search_index(state: Arc<AppState>) -> Result<(PathBuf, bool)> {
    if !state.config.search_incremental_reader_enabled {
        let index_dir = search_index_dir(&state);
        let rebuilt = ensure_search_index(state.clone()).await?;
        let freshness = legacy_search_index_freshness(&state.db).await?;
        if !freshness.is_current() {
            queue_legacy_search_rebuild(&state.db).await?;
            return Err(legacy_search_stale_error(freshness));
        }
        return Ok((index_dir, rebuilt));
    }
    if let Err(error) = validate_incremental_reader(&state).await {
        return Err(AppError::Overloaded {
            message: format!("incremental search reader is unavailable: {error}"),
            retry_after_seconds: 5,
        });
    }
    Ok((outbox::shadow_index_dir(&state.config.data_dir), false))
}

async fn production_search_index_is_openable(index_dir: &Path) -> bool {
    if !index_dir.join("meta.json").is_file() {
        return false;
    }
    let index_dir = index_dir.to_path_buf();
    tokio::task::spawn_blocking(move || SearchIndexReader::open(&index_dir).is_ok())
        .await
        .unwrap_or(false)
}

/// Build the legacy production index before the listener is exposed when it
/// does not already exist.  Without this startup prewarm, the first Catalog
/// request pays the full 40k-work GROUP_CONCAT snapshot cost while holding an
/// interactive HTTP request open.  The caller deliberately awaits this
/// bounded maintenance step so readiness means the first search-backed shelf
/// request is warm; subsequent revisions still use the existing job path.
pub async fn prewarm_production_index(state: Arc<AppState>) -> Result<bool> {
    if state.config.search_incremental_reader_enabled {
        return Ok(false);
    }
    let index_dir = search_index_dir(&state);
    let freshness = legacy_search_index_freshness(&state.db).await?;
    if freshness.is_current() && production_search_index_is_openable(&index_dir).await {
        return Ok(false);
    }
    let _guard = SEARCH_REBUILD_LOCK.lock().await;
    let freshness = legacy_search_index_freshness(&state.db).await?;
    if freshness.is_current() && production_search_index_is_openable(&index_dir).await {
        return Ok(false);
    }
    rebuild_search_index_unlocked(state).await?;
    Ok(true)
}

/// Best-effort opt-in reader prewarm.  The production index itself must have
/// been built first; a missing index is reported as an error so the caller can
/// keep the rollout fail-closed and leave the normal lazy path available.
pub async fn prewarm_production_reader(state: Arc<AppState>) -> Result<()> {
    if state.config.search_incremental_reader_enabled {
        return Ok(());
    }
    let index_dir = search_index_dir(&state);
    state.search_runtime.prewarm_reader(index_dir).await
}

pub async fn enqueue_rebuild(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>> {
    auth::require_csrf(&state, &headers, "search.rebuild").await?;
    let (id, created) = state
        .db
        .create_job_if_absent("rebuild-search-index", "queued", json!({ "source": "api" }))
        .await?;
    if created {
        state
            .db
            .audit("search.rebuild", "queued", json!({ "job_id": id }))
            .await?;
    }
    Ok(Json(
        json!({ "job_id": id, "status": if created { "queued" } else { "active" } }),
    ))
}

pub async fn enqueue_shadow_rebuild(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>> {
    auth::require_csrf(&state, &headers, "search.shadow-rebuild").await?;
    if !state.config.search_outbox_shadow_enabled {
        return Err(AppError::BadRequest(
            "shadow search worker is disabled".to_string(),
        ));
    }
    let (id, created) = state
        .db
        .create_job_if_absent(
            outbox::REBUILD_SHADOW_SEARCH_JOB_TYPE,
            "queued",
            json!({ "source": "api" }),
        )
        .await?;
    if created {
        state
            .db
            .audit("search.shadow-rebuild", "queued", json!({ "job_id": id }))
            .await?;
    }
    Ok(Json(
        json!({ "job_id": id, "status": if created { "queued" } else { "active" } }),
    ))
}

pub async fn rebuild_search_index(state: Arc<AppState>) -> Result<usize> {
    let _guard = SEARCH_REBUILD_LOCK.lock().await;
    rebuild_search_index_unlocked(state).await
}

/// Explicitly recover the persisted shadow index after a degraded/corrupt
/// state. This path is separate from the production rebuild job: it never
/// replaces the production index and it resets the incremental reader arm
/// until a fresh fact reconciliation passes.
pub async fn rebuild_shadow_search_index(state: Arc<AppState>) -> Result<usize> {
    if !state.config.search_outbox_shadow_enabled {
        return Err(AppError::BadRequest(
            "shadow search worker is disabled".to_string(),
        ));
    }
    let index_dir = outbox::shadow_index_dir(&state.config.data_dir);
    let count =
        outbox::rebuild_shadow_index(&state.db, &state.resources, index_dir.clone()).await?;
    state.search_runtime.reset_index(&index_dir).await;
    outbox::refresh_shadow_progress(&state.db).await?;
    state
        .db
        .audit("search.shadow-rebuild", "done", json!({ "works": count }))
        .await?;
    Ok(count)
}

async fn ensure_search_index(state: Arc<AppState>) -> Result<bool> {
    let index_dir = search_index_dir(&state);
    if index_dir.join("meta.json").is_file() {
        return Ok(false);
    }
    let _guard = SEARCH_REBUILD_LOCK.lock().await;
    if index_dir.join("meta.json").is_file() {
        return Ok(false);
    }
    rebuild_search_index_unlocked(state).await?;
    Ok(true)
}

async fn rebuild_search_index_unlocked(state: Arc<AppState>) -> Result<usize> {
    let index_dir = search_index_dir(&state);
    mark_legacy_search_index_building(&state.db).await?;
    let snapshot =
        match build_search_index_snapshot(&state.db, &state.resources, index_dir.clone()).await {
            Ok(result) => result,
            Err(error) => {
                if let Err(state_error) =
                    mark_legacy_search_index_degraded(&state.db, &error.to_string()).await
                {
                    tracing::error!(
                        error = %state_error,
                        original_error = %error,
                        "failed to persist degraded legacy search index state"
                    );
                }
                return Err(error);
            }
        };
    let count = snapshot.documents;
    let finalize_result: Result<()> = async {
        // Legacy scans can enqueue delete tombstones while the shadow worker is
        // disabled. The freshly rebuilt production snapshot already contains
        // every work at or below this revision, so acknowledge only that prefix;
        // writes that raced with the snapshot remain pending for a future rebuild
        // or an eventual shadow cutover. Never acknowledge the queue while the
        // shadow reader is active because its independent index may still lag.
        if !state.config.search_outbox_shadow_enabled {
            let acknowledged =
                acknowledge_legacy_rebuild_outbox(&state.db, snapshot.search_revision).await?;
            if acknowledged > 0 {
                tracing::info!(
                    acknowledged,
                    catalog_revision = snapshot.catalog_revision,
                    search_revision = snapshot.search_revision,
                    "acknowledged search outbox rows covered by the full rebuild"
                );
            }
        }
        // State remains `building` until stale readers and candidate IDs have
        // been discarded. Publishing `ready` first would let a concurrent
        // Catalog request validate against the new revision while its in-memory
        // reader still served the old index.
        state.search_runtime.reset_index(&index_dir).await;
        record_legacy_search_index_ready(&state.db, snapshot).await
    }
    .await;
    if let Err(error) = finalize_result {
        if let Err(state_error) =
            mark_legacy_search_index_degraded(&state.db, &error.to_string()).await
        {
            tracing::error!(
                error = %state_error,
                original_error = %error,
                "failed to persist degraded legacy search index state"
            );
        }
        return Err(error);
    }
    // Audit persistence is useful operational evidence but does not alter the
    // newly committed Tantivy snapshot. Do not withdraw an otherwise valid
    // index if a non-authoritative audit write happens to fail.
    if let Err(error) = state
        .db
        .audit(
            "search.rebuild",
            "done",
            json!({
                "works": count,
                "catalog_revision": snapshot.catalog_revision,
                "search_revision": snapshot.search_revision,
            }),
        )
        .await
    {
        tracing::warn!(
            error = %error,
            works = count,
            catalog_revision = snapshot.catalog_revision,
            search_revision = snapshot.search_revision,
            "legacy search rebuild completed but audit persistence failed"
        );
    }
    Ok(count)
}

async fn acknowledge_legacy_rebuild_outbox(db: &Db, search_revision: i64) -> Result<u64> {
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    let affected = sqlx::query(
        r#"
        UPDATE search_outbox
        SET committed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
            claimed_by = NULL,
            claimed_at = NULL,
            last_error = NULL,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE committed_at IS NULL
          AND search_revision <= ?1
        "#,
    )
    .bind(search_revision)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    transaction.commit().await?;
    Ok(affected)
}

pub(crate) async fn build_search_index_snapshot(
    db: &Db,
    resources: &ResourceGovernor,
    index_dir: PathBuf,
) -> Result<SearchIndexSnapshot> {
    let worker_index_dir = index_dir;
    let writer_heap_bytes = resources.limits().search_writer_heap_bytes;
    let _resource_lease = resources
        .reserve_background(ResourceClass::SearchWriter, writer_heap_bytes as u64, 0)
        .await?;
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let catalog_revision =
        sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
            .fetch_one(&mut *transaction)
            .await?;
    let search_revision = sqlx::query_scalar::<_, i64>(
        "SELECT revision FROM search_source_state WHERE singleton = 1",
    )
    .fetch_one(&mut *transaction)
    .await?;
    let (sender, receiver) = mpsc::channel(SEARCH_INDEX_CHANNEL_CAPACITY);
    let worker = tokio::task::spawn_blocking(move || {
        rebuild_search_index_from_receiver(worker_index_dir, receiver, writer_heap_bytes)
    });
    let mut rows = sqlx::query(SEARCH_INDEX_SQL).fetch(&mut *transaction);
    let mut producer_error = None;
    loop {
        match rows.try_next().await {
            Ok(Some(row)) => {
                let item = search_index_row(&row);
                if sender.send(Ok(item)).await.is_err() {
                    producer_error = Some(AppError::Other(
                        "search index writer stopped before the database snapshot completed"
                            .to_string(),
                    ));
                    break;
                }
            }
            Ok(None) => break,
            Err(err) => {
                let error: AppError = err.into();
                let message = error.to_string();
                let _ = sender.send(Err(error)).await;
                producer_error = Some(AppError::Other(message));
                break;
            }
        }
    }
    drop(rows);
    if producer_error.is_some() {
        transaction.rollback().await?;
    } else if let Err(error) = transaction.commit().await {
        let error: AppError = error.into();
        let message = error.to_string();
        let _ = sender.send(Err(error)).await;
        producer_error = Some(AppError::Other(message));
    }
    drop(sender);
    let worker_result = worker
        .await
        .map_err(|error| AppError::Other(format!("search index worker failed: {error}")))?;
    if let Some(error) = producer_error {
        worker_result?;
        return Err(error);
    }
    Ok(SearchIndexSnapshot {
        documents: worker_result?,
        catalog_revision,
        search_revision,
    })
}

fn search_index_row(row: &sqlx::sqlite::SqliteRow) -> SearchIndexRow {
    SearchIndexRow {
        id: row.get("id"),
        kind: row.get("kind"),
        title: row.get("title"),
        category: row.get("category"),
        description: row.get("description"),
        source_path: row.get("source_path"),
        tags: row.get("tags"),
    }
}

#[cfg(test)]
fn rebuild_search_index_blocking(index_dir: PathBuf, rows: Vec<SearchIndexRow>) -> Result<()> {
    rebuild_search_index_from_iter(index_dir, rows.into_iter().map(Ok), 50_000_000).map(|_| ())
}

fn rebuild_search_index_from_receiver(
    index_dir: PathBuf,
    mut receiver: mpsc::Receiver<Result<SearchIndexRow>>,
    writer_heap_bytes: usize,
) -> Result<usize> {
    let rows = std::iter::from_fn(move || receiver.blocking_recv());
    rebuild_search_index_from_iter(index_dir, rows, writer_heap_bytes)
}

fn rebuild_search_index_from_iter<I>(
    index_dir: PathBuf,
    rows: I,
    writer_heap_bytes: usize,
) -> Result<usize>
where
    I: IntoIterator<Item = Result<SearchIndexRow>>,
{
    std::fs::create_dir_all(&index_dir)?;
    let (index, fields) = match open_or_create_index(&index_dir) {
        Ok(opened) => opened,
        Err(error) if is_corrupt_search_index_error(&error) => {
            quarantine_corrupt_search_index(&index_dir)?;
            std::fs::create_dir_all(&index_dir)?;
            open_or_create_index(&index_dir)?
        }
        Err(error) => return Err(error),
    };
    let mut writer = open_search_writer(&index, writer_heap_bytes)?;
    writer.delete_all_documents().map_err(search_error)?;
    let mut count = 0;
    for row in rows {
        let row = row?;
        let id = row.id.to_string();
        let body = row.body_text();
        writer
            .add_document(doc!(
                fields.work_id => id,
                fields.kind => row.kind,
                fields.title => row.title.clone(),
                fields.title_ngram => row.title,
                fields.body => body.clone(),
                fields.body_ngram => body,
            ))
            .map_err(search_error)?;
        count += 1;
    }
    writer.commit().map_err(search_error)?;
    // Do not expose readiness while Tantivy still has merge work in flight.
    // Besides making the startup latency measurable, this propagates a merge
    // filesystem error instead of letting it kill a background worker after
    // the process has already started serving requests.
    writer.wait_merging_threads().map_err(search_error)?;
    Ok(count)
}

fn is_corrupt_search_index_error(error: &AppError) -> bool {
    let text = error.to_string();
    text.contains("Data corrupted")
        || text.contains("Meta file cannot be deserialized")
        || text.contains("meta.json") && text.contains("cannot be deserialized")
}

fn quarantine_corrupt_search_index(index_dir: &Path) -> Result<()> {
    if !index_dir.exists() {
        return Ok(());
    }
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let name = index_dir
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("search-index");
    let quarantine =
        index_dir.with_file_name(format!("{name}.corrupt-{}-{stamp}", std::process::id()));
    std::fs::rename(index_dir, &quarantine).map_err(|error| {
        AppError::Other(format!(
            "failed to quarantine corrupt search index {}: {error}",
            index_dir.display()
        ))
    })?;
    tracing::warn!(
        source = %index_dir.display(),
        quarantine = %quarantine.display(),
        "quarantined corrupt search index before rebuilding"
    );
    Ok(())
}

pub(crate) fn open_search_writer(
    index: &Index,
    writer_heap_bytes: usize,
) -> Result<tantivy::IndexWriter> {
    let options = IndexWriterOptions::builder()
        .num_worker_threads(SEARCH_INDEX_WRITER_THREADS)
        .memory_budget_per_thread(writer_heap_bytes / SEARCH_INDEX_WRITER_THREADS)
        .num_merge_threads(SEARCH_INDEX_MERGE_THREADS)
        .build();
    let writer = index.writer_with_options(options).map_err(search_error)?;
    if writer_heap_bytes <= SEARCH_INDEX_LOW_MEMORY_BYTES {
        let mut merge_policy = LogMergePolicy::default();
        merge_policy.set_min_num_segments(SEARCH_INDEX_LOW_MEMORY_MIN_SEGMENTS);
        writer.set_merge_policy(Box::new(merge_policy));
    }
    Ok(writer)
}

#[cfg(test)]
fn query_search_index_blocking(
    index_dir: PathBuf,
    query: String,
    limit: usize,
) -> Result<Vec<SearchHit>> {
    SearchIndexReader::open(&index_dir)?.query(&query, limit)
}

fn query_open_index(
    index: &Index,
    reader: &IndexReader,
    fields: SearchFields,
    query: &str,
    limit: usize,
) -> Result<Vec<SearchHit>> {
    let query = bounded_search_query(query)?;
    let searcher = reader.searcher();
    let schema = index.schema();
    let parser = QueryParser::for_index(
        index,
        vec![
            fields.title,
            fields.title_ngram,
            fields.body,
            fields.body_ngram,
            fields.kind,
        ],
    );
    let query_text = safe_query_text(&query);
    if query_text.is_empty() {
        return Ok(Vec::new());
    }
    let parsed = parser
        .parse_query(&query_text)
        .or_else(|_| parser.parse_query(&format!("\"{query_text}\"")))
        .map_err(search_error)?;
    let top_docs = searcher
        .search(&parsed, &TopDocs::with_limit(limit).order_by_score())
        .map_err(search_error)?;

    let mut hits = Vec::with_capacity(top_docs.len());
    for (score, address) in top_docs {
        let doc: TantivyDocument = searcher.doc(address).map_err(search_error)?;
        let value: Value =
            serde_json::from_str(&doc.to_json(&schema)).unwrap_or_else(|_| json!({}));
        let Some(work_id) =
            first_stored_text(&value, "work_id").and_then(|id| id.parse::<i64>().ok())
        else {
            continue;
        };
        hits.push(SearchHit {
            work_id,
            score,
            title: first_stored_text(&value, "title").unwrap_or_default(),
            kind: first_stored_text(&value, "kind").unwrap_or_default(),
        });
    }
    Ok(hits)
}

fn open_or_create_index(index_dir: &Path) -> Result<(Index, SearchFields)> {
    let (index, fields) = if index_dir.join("meta.json").exists() {
        let index = Index::open_in_dir(index_dir).map_err(search_error)?;
        let schema = index.schema();
        let fields = fields_from_schema(&schema)?;
        (index, fields)
    } else {
        let (schema, fields) = build_schema();
        let index = Index::create_in_dir(index_dir, schema).map_err(search_error)?;
        (index, fields)
    };
    index.tokenizers().register(
        CJK_NGRAM_TOKENIZER,
        NgramTokenizer::new(2, 3, false).map_err(search_error)?,
    );
    Ok((index, fields))
}

fn build_schema() -> (Schema, SearchFields) {
    let mut builder = Schema::builder();
    let work_id = builder.add_text_field("work_id", STRING | STORED);
    let kind = builder.add_text_field("kind", STRING | STORED);
    let title = builder.add_text_field("title", TEXT | STORED);
    let ngram_options = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(CJK_NGRAM_TOKENIZER)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );
    let title_ngram = builder.add_text_field("title_ngram", ngram_options.clone());
    let body = builder.add_text_field("body", TEXT);
    let body_ngram = builder.add_text_field("body_ngram", ngram_options);
    (
        builder.build(),
        SearchFields {
            work_id,
            kind,
            title,
            title_ngram,
            body,
            body_ngram,
        },
    )
}

fn fields_from_schema(schema: &Schema) -> Result<SearchFields> {
    let required = |name| {
        schema
            .get_field(name)
            .map_err(|_| AppError::Other(format!("search schema missing {name}")))
    };
    Ok(SearchFields {
        work_id: required("work_id")?,
        kind: required("kind")?,
        title: required("title")?,
        title_ngram: required("title_ngram")?,
        body: required("body")?,
        body_ngram: required("body_ngram")?,
    })
}

fn first_stored_text(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn safe_query_text(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_alphanumeric() || ch.is_whitespace() || matches!(ch, '_' | '-' | ':') {
                ch
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn bounded_search_query(value: &str) -> Result<String> {
    let value = value.trim();
    if value.len() > MAX_SEARCH_QUERY_BYTES {
        return Err(AppError::BadRequest(format!(
            "search query exceeds {MAX_SEARCH_QUERY_BYTES} bytes"
        )));
    }
    Ok(value.to_string())
}

fn search_index_dir(state: &AppState) -> PathBuf {
    state.config.data_dir.join("search-index-v2")
}

fn search_error(error: impl std::fmt::Display) -> AppError {
    AppError::Other(format!("search index error: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assets;
    use crate::config::Config;

    fn row(id: i64, title: &str) -> SearchIndexRow {
        SearchIndexRow {
            id,
            kind: "novel".to_string(),
            title: title.to_string(),
            category: None,
            description: None,
            source_path: None,
            tags: None,
        }
    }

    fn hit(id: i64) -> SearchHit {
        SearchHit {
            work_id: id,
            score: 1.0,
            title: format!("Work {id}"),
            kind: "novel".to_string(),
        }
    }

    #[test]
    fn search_query_budget_is_trimmed_and_fail_closed() {
        assert_eq!(
            bounded_search_query("  author:perf  ").unwrap(),
            "author:perf"
        );
        assert!(bounded_search_query(&"x".repeat(MAX_SEARCH_QUERY_BYTES)).is_ok());
        let oversized = bounded_search_query(&"x".repeat(MAX_SEARCH_QUERY_BYTES + 1)).unwrap_err();
        assert!(matches!(oversized, AppError::BadRequest(message) if message.contains("512")));
        let multibyte =
            bounded_search_query(&"中".repeat((MAX_SEARCH_QUERY_BYTES / 3) + 1)).unwrap_err();
        assert!(matches!(multibyte, AppError::BadRequest(_)));
    }

    fn shadow_status(ready: bool, status: &str) -> outbox::ShadowIndexStatus {
        outbox::ShadowIndexStatus {
            index_name: "shadow-v3".to_string(),
            schema_version: 1,
            baseline_revision: 4,
            applied_revision: 4,
            baseline_search_revision: 4,
            applied_search_revision: 4,
            indexed_documents: 2,
            ready,
            status: status.to_string(),
            last_error: None,
            updated_at: "2026-07-31T00:00:00Z".to_string(),
        }
    }

    async fn insert_work_at_path(db: &Db, title: &str, source_path: &str) -> i64 {
        db.upsert_work(
            "novel",
            title,
            Some(source_path),
            Some("Light Novel"),
            Some("canary fixture"),
            None,
            json!({}),
        )
        .await
        .unwrap()
    }

    async fn canary_state(temp: &tempfile::TempDir) -> Arc<AppState> {
        let data_dir = temp.path().join("data");
        let generated_dir = temp.path().join("generated");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&generated_dir).unwrap();
        let database_url = format!("sqlite://{}", data_dir.join("library.sqlite").display());
        let db = Db::connect(&database_url).await.unwrap();
        db.migrate().await.unwrap();
        insert_work_at_path(&db, "Canary Shared First", "/novels/canary-first.epub").await;
        insert_work_at_path(&db, "Canary Shared Second", "/novels/canary-second.epub").await;
        let resources = ResourceGovernor::standard();
        let production_dir = data_dir.join("search-index-v2");
        let shadow_dir = outbox::shadow_index_dir(&data_dir);
        let production_snapshot = build_search_index_snapshot(&db, &resources, production_dir)
            .await
            .unwrap();
        let shadow_snapshot = build_search_index_snapshot(&db, &resources, shadow_dir)
            .await
            .unwrap();
        // The fixture builds both indexes from the same snapshot. Mirror the
        // baseline's acknowledgement so later canary assertions start from a
        // caught-up outbox rather than treating fixture setup writes as lag.
        sqlx::query(
            r#"
            UPDATE search_outbox
            SET committed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE committed_at IS NULL AND search_revision <= ?1
            "#,
        )
        .bind(production_snapshot.search_revision)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2'")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query(
            r#"
            UPDATE search_index_state
            SET baseline_revision = ?1,
                applied_revision = ?1,
                baseline_search_revision = ?2,
                applied_search_revision = ?2,
                indexed_documents = ?3,
                ready = 1,
                status = 'ready',
                last_error = NULL
            WHERE index_name = 'shadow-v3'
            "#,
        )
        .bind(shadow_snapshot.catalog_revision)
        .bind(shadow_snapshot.search_revision)
        .bind(i64::try_from(shadow_snapshot.documents).unwrap())
        .execute(db.pool())
        .await
        .unwrap();

        Arc::new(AppState {
            config: Config {
                bind: "127.0.0.1:0".to_string(),
                database_url,
                data_dir,
                cover_cache_dir: temp.path().join("cover-cache"),
                comic_cover_cache_dir: temp.path().join("cover-cache/comic"),
                novel_cover_cache_dir: temp.path().join("cover-cache/novel"),
                audio_cover_cache_dir: temp.path().join("cover-cache/audio"),
                gallery_cover_cache_dir: temp.path().join("cover-cache/gallery"),
                coser_picture_cover_cache_dir: temp.path().join("cover-cache/coser-picture"),
                comics_dir: temp.path().join("comics"),
                novels_dir: temp.path().join("novels"),
                audio_dir: temp.path().join("audio"),
                gallery_dir: temp.path().join("gallery"),
                coser_picture_dir: temp.path().join("coser-picture"),
                generated_dir,
                app_admin_password: "test-admin".to_string(),
                admin_password_persisted: false,
                admin_password_ephemeral: false,
                lightnovel_api_bases: Vec::new(),
                lightnovel_access_token: None,
                enrichment_concurrency: 1,
                ehtt_url: String::new(),
                openai_api_key: None,
                openai_image_model: "gpt-image-2".to_string(),
                qmediasync_base_url: String::new(),
                cloud_cache_max_bytes: 1024,
                thumbnail_cache_max_bytes_per_dir: 1024,
                catalog_v2_enabled: true,
                facet_bitmap_enabled: false,
                inventory_scanner_enabled: false,
                inventory_scanner_kinds: std::collections::BTreeSet::new(),
                search_outbox_shadow_enabled: true,
                search_shadow_canary_enabled: true,
                search_incremental_reader_enabled: false,
                search_reader_prewarm_enabled: false,
                derivative_cache_v2_enabled: false,
                jpeg_thumbnail_downscale_enabled: false,
                derivative_cache_dir: temp.path().join("derivatives"),
                derivative_cache_max_bytes: 1024,
                derivative_cache_low_watermark_bytes: 512,
                session_secret: "test-secret".to_string(),
                enable_file_watcher: false,
                watch_debounce_seconds: 20,
            },
            derivatives: crate::derivative::DerivativeCache::disabled(
                db.clone(),
                temp.path().join("derivatives"),
            ),
            db,
            http: reqwest::Client::new(),
            resources,
            catalog_runtime: crate::catalog::CatalogRuntime::default(),
            search_runtime: SearchRuntime::default(),
            comic_page_cache: Arc::new(assets::ComicPageCache::default()),
            auth_epoch: Arc::new(tokio::sync::RwLock::new("test".to_string())),
            admin_password_persisted: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    async fn current_search_snapshot(state: &AppState, documents: usize) -> SearchIndexSnapshot {
        let revisions = state.db.revision_fence_snapshot().await.unwrap();
        SearchIndexSnapshot {
            documents,
            catalog_revision: revisions.catalog_revision,
            search_revision: revisions.search_revision,
        }
    }

    #[test]
    fn cjk_substring_searches_and_rebuild_removes_stale_documents() {
        let temp = tempfile::tempdir().unwrap();
        let index_dir = temp.path().join("index");

        rebuild_search_index_blocking(index_dir.clone(), vec![row(7, "败犬女主太多了")]).unwrap();
        let hits = query_search_index_blocking(index_dir.clone(), "败犬".to_string(), 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].work_id, 7);

        rebuild_search_index_blocking(index_dir.clone(), Vec::new()).unwrap();
        let hits = query_search_index_blocking(index_dir, "败犬".to_string(), 10).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn low_memory_search_writer_uses_deferred_merge_policy_only_at_threshold() {
        let temp = tempfile::tempdir().unwrap();

        let low_index_dir = temp.path().join("low-memory-index");
        std::fs::create_dir_all(&low_index_dir).unwrap();
        let (low_index, _) = open_or_create_index(&low_index_dir).unwrap();
        let low_writer = open_search_writer(&low_index, SEARCH_INDEX_LOW_MEMORY_BYTES).unwrap();
        let low_policy = format!("{:?}", low_writer.get_merge_policy());
        assert!(low_policy.contains("min_num_segments: 32"), "{low_policy}");
        drop(low_writer);

        let high_index_dir = temp.path().join("high-memory-index");
        std::fs::create_dir_all(&high_index_dir).unwrap();
        let (high_index, _) = open_or_create_index(&high_index_dir).unwrap();
        let high_writer =
            open_search_writer(&high_index, SEARCH_INDEX_LOW_MEMORY_BYTES + 1).unwrap();
        let high_policy = format!("{:?}", high_writer.get_merge_policy());
        assert!(high_policy.contains("min_num_segments: 8"), "{high_policy}");
    }

    #[test]
    fn canary_requires_ready_state_and_both_persisted_indexes() {
        assert!(matches!(
            validate_shadow_canary_state(&shadow_status(false, "shadow"), true, true),
            Err(AppError::Overloaded { .. })
        ));
        assert!(matches!(
            validate_shadow_canary_state(&shadow_status(true, "shadow"), true, true),
            Err(AppError::Overloaded { .. })
        ));
        assert!(matches!(
            validate_shadow_canary_state(&shadow_status(true, "ready"), true, false),
            Err(AppError::Overloaded { .. })
        ));
        validate_shadow_canary_state(&shadow_status(true, "ready"), true, true).unwrap();
    }

    #[test]
    fn candidate_cache_is_bounded_by_entries_and_total_ids() {
        let mut cache = CandidateCacheState::default();
        for index in 0..32_u64 {
            let key = CandidateCacheKey {
                index_dir: PathBuf::from("test-index"),
                query: format!("query-{index}"),
                limit: 50_000,
            };
            cache.entries.insert(
                key,
                CandidateCacheEntry {
                    created_at: Instant::now(),
                    last_used: index,
                    weight_ids: Some(40_000),
                    value: Arc::new(OnceCell::new()),
                },
            );
            cache.total_ids += 40_000;
        }
        let removed = cache.enforce_limits(None);
        assert!(removed > 0);
        assert!(cache.entries.len() <= CANDIDATE_CACHE_MAX_ENTRIES);
        assert!(cache.total_ids <= CANDIDATE_CACHE_MAX_IDS);
    }

    #[tokio::test]
    async fn legacy_rebuild_acknowledges_only_outbox_rows_covered_by_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let database_url = format!(
            "sqlite://{}",
            temp.path()
                .join("search-outbox.sqlite")
                .to_string_lossy()
                .replace('\\', "/")
        );
        let db = Db::connect(&database_url).await.unwrap();
        db.migrate().await.unwrap();
        for (work_id, catalog_revision, search_revision) in [
            (1_i64, 2_i64, 5_i64),
            (2_i64, 3_i64, 6_i64),
            (3_i64, 4_i64, 7_i64),
        ] {
            sqlx::query(
                r#"
                INSERT INTO search_outbox (
                    work_id, operation, catalog_revision, search_revision, payload_version,
                    available_at, claimed_by, claimed_at, updated_at
                )
                VALUES (?1, 'upsert', ?2, ?3, 1, '2026-08-17T00:00:00.000Z', 'worker',
                        '2026-08-17T00:00:00.000Z', '2026-08-17T00:00:00.000Z')
                "#,
            )
            .bind(work_id)
            .bind(catalog_revision)
            .bind(search_revision)
            .execute(db.pool())
            .await
            .unwrap();
        }

        let before = db.write_snapshot();
        assert_eq!(acknowledge_legacy_rebuild_outbox(&db, 6).await.unwrap(), 2);
        let after = db.write_snapshot();
        assert_eq!(after.completed, before.completed + 1);
        let covered = sqlx::query_as::<_, (i64, Option<String>, Option<String>)>(
            "SELECT work_id, committed_at, claimed_by FROM search_outbox ORDER BY work_id",
        )
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert!(covered[0].1.is_some());
        assert!(covered[0].2.is_none());
        assert!(covered[1].1.is_some());
        assert!(covered[1].2.is_none());
        assert!(covered[2].1.is_none());
        assert_eq!(covered[2].2.as_deref(), Some("worker"));
    }

    #[tokio::test]
    async fn runtime_reuses_reader_and_singleflights_catalog_candidates() {
        let temp = tempfile::tempdir().unwrap();
        let index_dir = temp.path().join("index");
        rebuild_search_index_blocking(index_dir.clone(), vec![row(7, "败犬女主太多了")]).unwrap();
        let runtime = SearchRuntime::default();

        let (first, second, third) = tokio::join!(
            runtime.candidate_ids(index_dir.clone(), "败犬".to_string(), 50_000),
            runtime.candidate_ids(index_dir.clone(), "败犬".to_string(), 50_000),
            runtime.candidate_ids(index_dir.clone(), "败犬".to_string(), 50_000),
        );
        assert_eq!(first.unwrap(), vec![7]);
        assert_eq!(second.unwrap(), vec![7]);
        assert_eq!(third.unwrap(), vec![7]);

        assert_eq!(
            runtime
                .candidate_ids(index_dir.clone(), "败犬".to_string(), 50_000)
                .await
                .unwrap(),
            vec![7]
        );
        let snapshot = runtime.snapshot().await;
        assert_eq!(snapshot.readers, 1);
        assert_eq!(snapshot.reader_opens, 1);
        assert_eq!(snapshot.candidate_entries, 1);
        assert_eq!(snapshot.candidate_ids, 1);
        assert_eq!(snapshot.candidate_misses, 1);
        assert_eq!(snapshot.candidate_coalesced, 2);
        assert_eq!(snapshot.candidate_hits, 1);

        runtime.reset_index(&index_dir).await;
        let reset = runtime.snapshot().await;
        assert_eq!(reset.readers, 0);
        assert_eq!(reset.candidate_entries, 0);
        assert_eq!(reset.candidate_ids, 0);
    }

    #[tokio::test]
    async fn reader_prewarm_opens_once_without_populating_unbounded_candidates() {
        let temp = tempfile::tempdir().unwrap();
        let index_dir = temp.path().join("prewarm-index");
        rebuild_search_index_blocking(index_dir.clone(), vec![row(7, "预热作品")]).unwrap();
        let runtime = SearchRuntime::default();

        runtime.prewarm_reader(index_dir.clone()).await.unwrap();
        let before = runtime.snapshot().await;
        assert_eq!(before.readers, 1);
        assert_eq!(before.reader_opens, 1);
        assert_eq!(before.candidate_entries, 0);
        assert_eq!(before.candidate_ids, 0);

        assert_eq!(
            runtime
                .candidate_ids(index_dir, "预热".to_string(), 50_000)
                .await
                .unwrap(),
            vec![7]
        );
        let after = runtime.snapshot().await;
        assert_eq!(after.reader_opens, 1);
        assert_eq!(after.candidate_misses, 1);
    }

    #[tokio::test]
    async fn refresh_index_reloads_reader_and_invalidates_only_that_index() {
        let temp = tempfile::tempdir().unwrap();
        let first_dir = temp.path().join("first");
        let second_dir = temp.path().join("second");
        rebuild_search_index_blocking(first_dir.clone(), vec![row(7, "败犬女主太多了")]).unwrap();
        rebuild_search_index_blocking(second_dir.clone(), vec![row(8, "共享候选")]).unwrap();
        let runtime = SearchRuntime::default();

        assert_eq!(
            runtime
                .candidate_ids(first_dir.clone(), "败犬".to_string(), 50_000)
                .await
                .unwrap(),
            vec![7]
        );
        assert_eq!(
            runtime
                .candidate_ids(second_dir.clone(), "共享".to_string(), 50_000)
                .await
                .unwrap(),
            vec![8]
        );

        rebuild_search_index_blocking(first_dir.clone(), vec![row(7, "更新后的作品")]).unwrap();
        assert!(runtime.refresh_index(&first_dir).await.unwrap());

        assert!(runtime
            .candidate_ids(first_dir.clone(), "败犬".to_string(), 50_000)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            runtime
                .candidate_ids(second_dir, "共享".to_string(), 50_000)
                .await
                .unwrap(),
            vec![8]
        );
        let snapshot = runtime.snapshot().await;
        assert_eq!(snapshot.reader_reloads, 1);
        assert_eq!(snapshot.candidate_invalidations, 1);
        assert_eq!(snapshot.candidate_entries, 2);
        assert_eq!(snapshot.candidate_ids, 1);
    }

    #[tokio::test]
    async fn canary_comparison_distinguishes_id_and_order_drift_and_reports_metrics() {
        let runtime = SearchRuntime::default();
        let order_only = compare_search_hits(&[hit(1), hit(2)], &[hit(2), hit(1)]);
        assert!(order_only.id_match);
        assert!(!order_only.order_match);
        runtime.record_canary_comparison(&order_only);

        let identity_drift = compare_search_hits(&[hit(1), hit(2)], &[hit(1), hit(3)]);
        assert!(!identity_drift.id_match);
        assert!(!identity_drift.order_match);
        runtime.record_canary_comparison(&identity_drift);
        runtime.record_canary_failure();
        runtime.record_canary_rejection();

        let snapshot = runtime.snapshot().await;
        assert_eq!(snapshot.canary_queries, 2);
        assert_eq!(snapshot.canary_id_mismatches, 1);
        assert_eq!(snapshot.canary_order_mismatches, 2);
        assert_eq!(snapshot.canary_failures, 1);
        assert_eq!(snapshot.canary_rejections, 1);
    }

    #[tokio::test]
    async fn explicit_shadow_canary_queries_both_persisted_indexes_and_surfaces_drift() {
        let temp = tempfile::tempdir().unwrap();
        let state = canary_state(&temp).await;
        // A freshly committed baseline is intentionally `shadow` until its
        // first fact reconciliation has completed. The reconciliation route
        // must be able to establish the evidence that later promotes it to
        // `ready`; requiring ready here would make that transition circular.
        sqlx::query(
            "UPDATE search_index_state SET ready = 0, status = 'shadow' WHERE index_name = 'shadow-v3'",
        )
        .execute(state.db.pool())
        .await
        .unwrap();
        let facts = reconcile_shadow_facts_for_state(state.clone())
            .await
            .unwrap();
        assert_eq!(facts.status, "passed");
        assert_eq!(facts.sqlite_work_count, 2);
        assert_eq!(facts.shadow_document_count, 2);
        assert_eq!(facts.shadow_unique_work_count, 2);
        assert_eq!(facts.missing_work_ids, 0);
        assert_eq!(facts.unexpected_work_ids, 0);
        assert_eq!(facts.sqlite_ids_sha256, facts.shadow_ids_sha256);
        let persisted = outbox::reconciliation_status(&state.db).await.unwrap();
        assert_eq!(persisted.status, "passed");
        assert_eq!(persisted.consecutive_passes, 1);
        assert_eq!(persisted.catalog_revision, facts.catalog_revision);
        assert_eq!(persisted.applied_revision, facts.shadow_applied_revision);

        let repeated = reconcile_shadow_facts_for_state(state.clone())
            .await
            .unwrap();
        assert_eq!(repeated.status, "passed");
        assert_eq!(
            outbox::reconciliation_status(&state.db)
                .await
                .unwrap()
                .consecutive_passes,
            2
        );

        // Catalog-only maintenance does not change searchable work facts, so
        // it must not invalidate a passed Search reconciliation.
        sqlx::query("UPDATE catalog_state SET revision = revision + 1 WHERE singleton = 1")
            .execute(state.db.pool())
            .await
            .unwrap();
        let refreshed = outbox::record_shadow_reconciliation(&state.db, &repeated)
            .await
            .unwrap();
        assert_eq!(refreshed, "passed");
        assert_eq!(
            outbox::reconciliation_status(&state.db)
                .await
                .unwrap()
                .status,
            "passed"
        );
        sqlx::query("UPDATE catalog_state SET revision = ?1 WHERE singleton = 1")
            .bind(repeated.catalog_revision)
            .execute(state.db.pool())
            .await
            .unwrap();

        sqlx::query(
            "UPDATE search_reconciliation_state SET cutover_armed = 1 WHERE index_name = 'shadow-v3'",
        )
        .execute(state.db.pool())
        .await
        .unwrap();

        let (hits, comparison) =
            search_shadow_canary(state.clone(), "Canary Shared".to_string(), 10)
                .await
                .unwrap();
        assert_eq!(hits.len(), 2);
        assert!(comparison.id_match);
        assert!(comparison.order_match);

        let shadow_dir = outbox::shadow_index_dir(&state.config.data_dir);
        rebuild_search_index_blocking(
            shadow_dir.clone(),
            vec![row(999, "Canary Shared Replacement")],
        )
        .unwrap();
        state.search_runtime.reset_index(&shadow_dir).await;
        let (_, drift) = search_shadow_canary(state.clone(), "Canary Shared".to_string(), 10)
            .await
            .unwrap();
        assert!(!drift.id_match);
        assert!(!drift.order_match);
        let drifted_facts = reconcile_shadow_facts_for_state(state.clone())
            .await
            .unwrap();
        assert_eq!(drifted_facts.status, "failed");
        assert_eq!(drifted_facts.missing_work_ids, 2);
        assert_eq!(drifted_facts.unexpected_work_ids, 1);
        assert_ne!(
            drifted_facts.sqlite_ids_sha256,
            drifted_facts.shadow_ids_sha256
        );
        let degraded = outbox::shadow_index_status(&state.db).await.unwrap();
        assert!(!degraded.ready);
        assert_eq!(degraded.status, "degraded");
        let persisted_drift = outbox::reconciliation_status(&state.db).await.unwrap();
        assert_eq!(persisted_drift.status, "failed");
        assert_eq!(persisted_drift.consecutive_passes, 0);
        assert!(!persisted_drift.cutover_armed);
        assert_eq!(persisted_drift.missing_work_ids, 2);
        assert_eq!(persisted_drift.unexpected_work_ids, 1);

        let rebuilt_snapshot =
            build_search_index_snapshot(&state.db, &state.resources, shadow_dir.clone())
                .await
                .unwrap();
        assert_eq!(
            rebuilt_snapshot.catalog_revision,
            drifted_facts.catalog_revision_after
        );
        assert_eq!(
            rebuilt_snapshot.search_revision,
            drifted_facts.search_revision_after
        );
        state.search_runtime.reset_index(&shadow_dir).await;
        let recovered = reconcile_shadow_facts_for_state(state.clone())
            .await
            .unwrap();
        assert_eq!(recovered.status, "passed");
        let recovered_state = outbox::shadow_index_status(&state.db).await.unwrap();
        assert!(recovered_state.ready);
        assert_eq!(recovered_state.status, "ready");

        let snapshot = state.search_runtime.snapshot().await;
        assert_eq!(snapshot.canary_queries, 2);
        assert_eq!(snapshot.canary_id_mismatches, 1);
        assert_eq!(snapshot.canary_order_mismatches, 1);
        assert_eq!(snapshot.canary_failures, 0);
        assert_eq!(snapshot.canary_rejections, 0);
    }

    #[tokio::test]
    async fn shadow_baseline_reconciliation_runs_before_legacy_kind_promotion() {
        let temp = tempfile::tempdir().unwrap();
        let state = canary_state(&temp).await;
        sqlx::query("UPDATE catalog_kind_ownership SET authoritative_writer = 'legacy'")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query(
            "UPDATE search_index_state SET ready = 0, status = 'shadow' WHERE index_name = 'shadow-v3'",
        )
        .execute(state.db.pool())
        .await
        .unwrap();

        let report = reconcile_shadow_facts_for_state(state.clone())
            .await
            .unwrap();
        assert_eq!(report.status, "passed");
        let shadow = outbox::shadow_index_status(&state.db).await.unwrap();
        assert_eq!(shadow.status, "shadow");
        assert!(!shadow.ready);
        assert_eq!(
            outbox::reconciliation_status(&state.db)
                .await
                .unwrap()
                .status,
            "passed"
        );
        assert!(
            outbox::ensure_incremental_reader_gate(&state.db, Duration::from_secs(1))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn incremental_reader_requires_a_current_persisted_reconciliation() {
        let temp = tempfile::tempdir().unwrap();
        let mut state = canary_state(&temp).await;
        Arc::get_mut(&mut state)
            .unwrap()
            .config
            .search_incremental_reader_enabled = true;

        let error = validate_incremental_reader(&state).await.unwrap_err();
        assert!(error.to_string().contains("cutover is not armed"));

        let report = reconcile_shadow_facts_for_state(state.clone())
            .await
            .unwrap();
        assert_eq!(report.status, "passed");
        let unarmed = validate_incremental_reader(&state)
            .await
            .expect_err("interactive validation must not arm an unarmed reader");
        assert!(unarmed.to_string().contains("cutover is not armed"));
        assert!(
            !outbox::reconciliation_status(&state.db)
                .await
                .unwrap()
                .cutover_armed
        );
        arm_incremental_reader(&state).await.unwrap();
        validate_incremental_reader(&state).await.unwrap();
        let (index_dir, rebuilt) = production_search_index(state.clone()).await.unwrap();
        assert_eq!(index_dir, outbox::shadow_index_dir(&state.config.data_dir));
        assert!(!rebuilt);
    }

    #[tokio::test]
    async fn production_search_fails_closed_until_incremental_reader_is_armed() {
        let temp = tempfile::tempdir().unwrap();
        let mut state = canary_state(&temp).await;
        Arc::get_mut(&mut state)
            .unwrap()
            .config
            .search_incremental_reader_enabled = true;

        reconcile_shadow_facts_for_state(state.clone())
            .await
            .unwrap();
        let error = production_search_index(state.clone())
            .await
            .expect_err("unarmed incremental reader must not serve production queries");
        assert!(matches!(error, AppError::Overloaded { .. }));
        assert!(
            !outbox::reconciliation_status(&state.db)
                .await
                .unwrap()
                .cutover_armed
        );

        arm_incremental_reader(&state).await.unwrap();
        let (index_dir, rebuilt) = production_search_index(state).await.unwrap();
        assert!(index_dir.ends_with("search-index-v3-shadow"));
        assert!(!rebuilt);
    }

    #[tokio::test]
    async fn incremental_reader_prewarm_opens_shadow_reader_without_candidates() {
        let temp = tempfile::tempdir().unwrap();
        let mut state = canary_state(&temp).await;
        Arc::get_mut(&mut state)
            .unwrap()
            .config
            .search_incremental_reader_enabled = true;

        reconcile_shadow_facts_for_state(state.clone())
            .await
            .unwrap();
        prewarm_incremental_reader(state.clone()).await.unwrap();

        let snapshot = state.search_runtime.snapshot().await;
        assert_eq!(snapshot.readers, 1);
        assert_eq!(snapshot.reader_opens, 1);
        assert_eq!(snapshot.candidate_entries, 0);
        assert_eq!(snapshot.candidate_ids, 0);
    }

    #[tokio::test]
    async fn automatic_shadow_reconciliation_requires_both_opt_in_flags() {
        let temp = tempfile::tempdir().unwrap();
        let mut state = canary_state(&temp).await;

        Arc::get_mut(&mut state)
            .unwrap()
            .config
            .search_shadow_canary_enabled = false;
        assert!(spawn_shadow_reconciliation_worker(state.clone()).is_none());

        Arc::get_mut(&mut state)
            .unwrap()
            .config
            .search_shadow_canary_enabled = true;
        Arc::get_mut(&mut state)
            .unwrap()
            .config
            .search_outbox_shadow_enabled = false;
        assert!(spawn_shadow_reconciliation_worker(state.clone()).is_none());

        Arc::get_mut(&mut state)
            .unwrap()
            .config
            .search_outbox_shadow_enabled = true;
        let worker = spawn_shadow_reconciliation_worker(state).unwrap();
        worker.abort();
    }

    #[tokio::test]
    async fn legacy_search_index_revision_fence_rejects_stale_candidates() {
        let temp = tempfile::tempdir().unwrap();
        let state = canary_state(&temp).await;
        let snapshot = current_search_snapshot(state.as_ref(), 2).await;

        let before = legacy_search_index_freshness(&state.db).await.unwrap();
        assert!(!before.is_current());

        record_legacy_search_index_ready(&state.db, snapshot)
            .await
            .unwrap();
        let ready = legacy_search_index_freshness(&state.db).await.unwrap();
        assert_eq!(ready.catalog_revision, snapshot.catalog_revision);
        assert_eq!(ready.applied_revision, snapshot.catalog_revision);
        assert_eq!(ready.search_revision, snapshot.search_revision);
        assert_eq!(ready.applied_search_revision, snapshot.search_revision);
        assert!(ready.is_current());

        sqlx::query("UPDATE catalog_state SET revision = revision + 1 WHERE singleton = 1")
            .execute(state.db.pool())
            .await
            .unwrap();
        let stale = legacy_search_index_freshness(&state.db).await.unwrap();
        assert_eq!(stale.applied_revision, snapshot.catalog_revision);
        assert_eq!(stale.catalog_revision, snapshot.catalog_revision + 1);
        assert!(!stale.is_current());
    }

    #[tokio::test]
    async fn legacy_search_freshness_rejects_tag_source_revision_without_catalog_churn() {
        let temp = tempfile::tempdir().unwrap();
        let state = canary_state(&temp).await;
        let snapshot = current_search_snapshot(state.as_ref(), 2).await;
        record_legacy_search_index_ready(&state.db, snapshot)
            .await
            .unwrap();
        assert!(legacy_search_index_freshness(&state.db)
            .await
            .unwrap()
            .is_current());

        let catalog_before = state.db.revision_snapshot().await.unwrap().catalog_revision;
        sqlx::query("UPDATE search_source_state SET revision = revision + 1 WHERE singleton = 1")
            .execute(state.db.pool())
            .await
            .unwrap();
        let after = legacy_search_index_freshness(&state.db).await.unwrap();
        assert_eq!(after.catalog_revision, catalog_before);
        assert_eq!(after.applied_revision, catalog_before);
        assert_eq!(after.search_revision, snapshot.search_revision + 1);
        assert_eq!(after.applied_search_revision, snapshot.search_revision);
        assert!(!after.is_current());
    }

    #[tokio::test]
    async fn legacy_search_index_ready_fails_when_state_row_is_missing() {
        let temp = tempfile::tempdir().unwrap();
        let state = canary_state(&temp).await;
        sqlx::query("DELETE FROM search_index_state WHERE index_name = ?1")
            .bind(LEGACY_SEARCH_INDEX_NAME)
            .execute(state.db.pool())
            .await
            .unwrap();

        let error = record_legacy_search_index_ready(
            &state.db,
            current_search_snapshot(state.as_ref(), 1).await,
        )
        .await
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("legacy search index state row is missing"));
    }

    #[tokio::test]
    async fn legacy_search_index_rebuild_state_fails_closed_until_ready() {
        let temp = tempfile::tempdir().unwrap();
        let state = canary_state(&temp).await;
        let snapshot = current_search_snapshot(state.as_ref(), 2).await;
        record_legacy_search_index_ready(&state.db, snapshot)
            .await
            .unwrap();

        mark_legacy_search_index_building(&state.db).await.unwrap();
        let building = legacy_search_index_freshness(&state.db).await.unwrap();
        assert!(!building.is_current());
        let building_row = sqlx::query(
            "SELECT ready, status, last_error FROM search_index_state WHERE index_name = ?1",
        )
        .bind(LEGACY_SEARCH_INDEX_NAME)
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(building_row.get::<i64, _>("ready"), 0);
        assert_eq!(building_row.get::<String, _>("status"), "building");
        assert!(building_row
            .get::<Option<String>, _>("last_error")
            .is_none());

        mark_legacy_search_index_degraded(&state.db, "synthetic rebuild failure")
            .await
            .unwrap();
        let degraded = legacy_search_index_freshness(&state.db).await.unwrap();
        assert!(!degraded.is_current());
        let degraded_row = sqlx::query(
            "SELECT ready, status, last_error FROM search_index_state WHERE index_name = ?1",
        )
        .bind(LEGACY_SEARCH_INDEX_NAME)
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(degraded_row.get::<i64, _>("ready"), 0);
        assert_eq!(degraded_row.get::<String, _>("status"), "degraded");
        assert_eq!(
            degraded_row.get::<String, _>("last_error"),
            "synthetic rebuild failure"
        );
    }

    #[tokio::test]
    async fn prewarm_rebuilds_a_corrupt_but_revision_current_legacy_index() {
        let temp = tempfile::tempdir().unwrap();
        let state = canary_state(&temp).await;
        let snapshot = current_search_snapshot(state.as_ref(), 2).await;
        record_legacy_search_index_ready(&state.db, snapshot)
            .await
            .unwrap();
        let index_dir = search_index_dir(&state);
        std::fs::write(index_dir.join("meta.json"), "corrupt tantivy metadata").unwrap();
        assert!(!production_search_index_is_openable(&index_dir).await);

        assert!(prewarm_production_index(state.clone()).await.unwrap());
        assert!(production_search_index_is_openable(&index_dir).await);
        let quarantined = std::fs::read_dir(index_dir.parent().unwrap())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .any(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("search-index-v2.corrupt-"))
            });
        assert!(quarantined);
        assert!(legacy_search_index_freshness(&state.db)
            .await
            .unwrap()
            .is_current());
    }

    #[tokio::test]
    async fn corrupt_current_legacy_query_marks_degraded_and_queues_rebuild() {
        let temp = tempfile::tempdir().unwrap();
        let state = canary_state(&temp).await;
        let snapshot = current_search_snapshot(state.as_ref(), 2).await;
        record_legacy_search_index_ready(&state.db, snapshot)
            .await
            .unwrap();
        let index_dir = search_index_dir(&state);
        std::fs::write(index_dir.join("meta.json"), "corrupt tantivy metadata").unwrap();

        let (_, rebuilt) = production_search_index(state.clone()).await.unwrap();
        assert!(!rebuilt);
        let query_error = state
            .search_runtime
            .query_index(index_dir, "Canary".to_string(), 10)
            .await
            .unwrap_err();
        assert!(is_corrupt_search_index_error(&query_error));
        let recovery = recover_legacy_search_index_after_error(&state, &query_error).await;
        assert!(matches!(recovery, AppError::Overloaded { .. }));
        assert!(!legacy_search_index_freshness(&state.db)
            .await
            .unwrap()
            .is_current());
        let state_row =
            sqlx::query("SELECT ready, status FROM search_index_state WHERE index_name = ?1")
                .bind(LEGACY_SEARCH_INDEX_NAME)
                .fetch_one(state.db.pool())
                .await
                .unwrap();
        assert_eq!(state_row.get::<i64, _>("ready"), 0);
        assert_eq!(state_row.get::<String, _>("status"), "degraded");
        let job_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM jobs WHERE job_type = ?1 AND status = 'queued'",
        )
        .bind(LEGACY_SEARCH_REBUILD_JOB_TYPE)
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(job_count, 1);
    }

    #[tokio::test]
    async fn corrupt_incremental_query_marks_shadow_degraded_and_queues_shadow_rebuild() {
        let temp = tempfile::tempdir().unwrap();
        let mut state = canary_state(&temp).await;
        Arc::get_mut(&mut state)
            .unwrap()
            .config
            .search_incremental_reader_enabled = true;

        let recovery = recover_search_index_after_error(
            &state,
            &AppError::Other("Data corrupted: synthetic shadow metadata".to_string()),
        )
        .await;
        assert!(matches!(recovery, AppError::Overloaded { .. }));

        let shadow = outbox::shadow_index_status(&state.db).await.unwrap();
        assert!(!shadow.ready);
        assert_eq!(shadow.status, "degraded");
        assert!(shadow
            .last_error
            .as_deref()
            .is_some_and(|message| message.contains("synthetic shadow metadata")));

        let shadow_jobs = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM jobs WHERE job_type = ?1 AND status = 'queued'",
        )
        .bind(outbox::REBUILD_SHADOW_SEARCH_JOB_TYPE)
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(shadow_jobs, 1);

        let legacy_jobs = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM jobs WHERE job_type = ?1 AND status = 'queued'",
        )
        .bind(LEGACY_SEARCH_REBUILD_JOB_TYPE)
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(legacy_jobs, 0);
    }
}
