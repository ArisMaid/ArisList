mod facet_bitmap;

use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path as FsPath;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{Path, Query, State};
use axum::Json;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx_sqlite::SqliteConnection;
use tokio::sync::{Mutex as AsyncMutex, OnceCell};

use crate::db::{Db, RevisionSnapshot};
use crate::error::{AppError, Result};
use crate::models::WorkDetailAssetMode;
use crate::resource::{ResourceClass, ResourceGovernor};
use crate::search;
use crate::sqlite::SqliteRow;
use crate::{AppState, Row};

const DEFAULT_PAGE_SIZE: i64 = 60;
const MAX_PAGE_SIZE: i64 = 200;
const MAX_SELECTED_TAGS: usize = 32;
const MAX_QUERY_BYTES: usize = 512;
const STATS_BACKFILL_BATCH: i64 = 256;
const STATS_BACKFILL_PROCESSING_BYTES: u64 = 16 * 1024 * 1024;
const QUERY_PLANNER_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(15 * 60);
const CATALOG_SEARCH_CANDIDATE_LIMIT: usize = 50_000;
const CATALOG_SLOW_QUERY_MILLIS: u128 = 100;
const FACET_CACHE_MAX_ENTRIES: usize = 32;
const FACET_CACHE_MAX_ITEMS: usize = 4_096;
const FACET_CACHE_TTL: Duration = Duration::from_secs(5);
const FACET_REVISION_RETRY_LIMIT: usize = 2;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FacetCacheKey {
    catalog_revision: i64,
    request_hash: String,
}

struct FacetCacheEntry {
    completed_at: Option<Instant>,
    last_used: u64,
    weight_items: Option<usize>,
    value: Arc<OnceCell<FacetCacheLoad>>,
}

/// A request that raced a catalog write must not populate the cache entry
/// selected by an older revision key.  The caller retries from a fresh
/// revision instead of serving a response under the wrong cache generation.
#[derive(Clone)]
enum FacetCacheLoad {
    Ready(CatalogFacetsResponse),
    RevisionChanged,
}

#[derive(Default)]
struct FacetCacheState {
    entries: BTreeMap<FacetCacheKey, FacetCacheEntry>,
    total_items: usize,
    clock: u64,
}

impl FacetCacheState {
    fn remove(&mut self, key: &FacetCacheKey) {
        if let Some(entry) = self.entries.remove(key) {
            self.total_items = self
                .total_items
                .saturating_sub(entry.weight_items.unwrap_or_default());
        }
    }

    fn prune_expired(&mut self, now: Instant) -> u64 {
        let expired = self
            .entries
            .iter()
            .filter(|(_, entry)| {
                entry
                    .completed_at
                    .is_some_and(|completed| now.duration_since(completed) >= FACET_CACHE_TTL)
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        let removed = expired.len() as u64;
        for key in expired {
            self.remove(&key);
        }
        removed
    }

    fn enforce_limits(&mut self, protected: Option<&FacetCacheKey>) -> u64 {
        let mut removed = 0_u64;
        while self.entries.len() > FACET_CACHE_MAX_ENTRIES
            || self.total_items > FACET_CACHE_MAX_ITEMS
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
}

struct CatalogRuntimeInner {
    facets: AsyncMutex<FacetCacheState>,
    facet_bitmap: facet_bitmap::FacetBitmapRuntime,
    facet_hits: AtomicU64,
    facet_misses: AtomicU64,
    facet_coalesced: AtomicU64,
    facet_evictions: AtomicU64,
    work_detail_legacy_requests: AtomicU64,
    work_detail_summary_requests: AtomicU64,
}

#[derive(Clone)]
pub struct CatalogRuntime {
    inner: Arc<CatalogRuntimeInner>,
}

impl Default for CatalogRuntime {
    fn default() -> Self {
        Self {
            inner: Arc::new(CatalogRuntimeInner {
                facets: AsyncMutex::new(FacetCacheState::default()),
                facet_bitmap: facet_bitmap::FacetBitmapRuntime::default(),
                facet_hits: AtomicU64::new(0),
                facet_misses: AtomicU64::new(0),
                facet_coalesced: AtomicU64::new(0),
                facet_evictions: AtomicU64::new(0),
                work_detail_legacy_requests: AtomicU64::new(0),
                work_detail_summary_requests: AtomicU64::new(0),
            }),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct CatalogRuntimeSnapshot {
    pub facet_entries: usize,
    pub facet_items: usize,
    pub facet_hits: u64,
    pub facet_misses: u64,
    pub facet_coalesced: u64,
    pub facet_evictions: u64,
    pub facet_cache_max_entries: usize,
    pub facet_cache_max_items: usize,
    pub facet_cache_ttl_millis: u64,
    pub facet_bitmap: facet_bitmap::FacetBitmapRuntimeSnapshot,
    pub work_detail_legacy_requests: u64,
    pub work_detail_summary_requests: u64,
}

impl CatalogRuntime {
    /// Record compatibility-mode usage without adding a database write or
    /// retaining request paths or user data.
    pub fn record_work_detail_mode(&self, mode: WorkDetailAssetMode) {
        match mode {
            WorkDetailAssetMode::Legacy => {
                self.inner
                    .work_detail_legacy_requests
                    .fetch_add(1, AtomicOrdering::Relaxed);
            }
            WorkDetailAssetMode::Summary => {
                self.inner
                    .work_detail_summary_requests
                    .fetch_add(1, AtomicOrdering::Relaxed);
            }
        }
    }

    async fn cached_tag_facets<F, Fut>(
        &self,
        key: FacetCacheKey,
        load: F,
    ) -> Result<Option<CatalogFacetsResponse>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<FacetCacheLoad>>,
    {
        let now = Instant::now();
        let (cell, existing, ready) = {
            let mut cache = self.inner.facets.lock().await;
            let expired = cache.prune_expired(now);
            self.inner
                .facet_evictions
                .fetch_add(expired, AtomicOrdering::Relaxed);
            cache.clock = cache.clock.saturating_add(1);
            let clock = cache.clock;
            if let Some(entry) = cache.entries.get_mut(&key) {
                entry.last_used = clock;
                (entry.value.clone(), true, entry.value.get().is_some())
            } else {
                let cell = Arc::new(OnceCell::new());
                cache.entries.insert(
                    key.clone(),
                    FacetCacheEntry {
                        completed_at: None,
                        last_used: clock,
                        weight_items: None,
                        value: cell.clone(),
                    },
                );
                let evicted = cache.enforce_limits(Some(&key));
                self.inner
                    .facet_evictions
                    .fetch_add(evicted, AtomicOrdering::Relaxed);
                (cell, false, false)
            }
        };
        if ready {
            self.inner.facet_hits.fetch_add(1, AtomicOrdering::Relaxed);
        } else if existing {
            self.inner
                .facet_coalesced
                .fetch_add(1, AtomicOrdering::Relaxed);
        } else {
            self.inner
                .facet_misses
                .fetch_add(1, AtomicOrdering::Relaxed);
        }

        let result = cell.get_or_try_init(load).await;
        match result {
            Ok(FacetCacheLoad::Ready(response)) => {
                let mut cache = self.inner.facets.lock().await;
                cache.clock = cache.clock.saturating_add(1);
                let clock = cache.clock;
                let mut added_weight = None;
                if let Some(entry) = cache.entries.get_mut(&key) {
                    if Arc::ptr_eq(&entry.value, &cell) {
                        entry.last_used = clock;
                        entry.completed_at.get_or_insert_with(Instant::now);
                        if entry.weight_items.is_none() {
                            entry.weight_items = Some(response.items.len());
                            added_weight = Some(response.items.len());
                        }
                    }
                }
                if let Some(weight) = added_weight {
                    cache.total_items = cache.total_items.saturating_add(weight);
                }
                let evicted = cache.enforce_limits(Some(&key));
                self.inner
                    .facet_evictions
                    .fetch_add(evicted, AtomicOrdering::Relaxed);
                Ok(Some(response.clone()))
            }
            Ok(FacetCacheLoad::RevisionChanged) => {
                let mut cache = self.inner.facets.lock().await;
                if cache
                    .entries
                    .get(&key)
                    .is_some_and(|entry| Arc::ptr_eq(&entry.value, &cell))
                {
                    cache.remove(&key);
                }
                Ok(None)
            }
            Err(error) => {
                let mut cache = self.inner.facets.lock().await;
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

    pub async fn snapshot(&self) -> CatalogRuntimeSnapshot {
        let facet_bitmap = self.inner.facet_bitmap.snapshot().await;
        let cache = self.inner.facets.lock().await;
        CatalogRuntimeSnapshot {
            facet_entries: cache.entries.len(),
            facet_items: cache.total_items,
            facet_hits: self.inner.facet_hits.load(AtomicOrdering::Relaxed),
            facet_misses: self.inner.facet_misses.load(AtomicOrdering::Relaxed),
            facet_coalesced: self.inner.facet_coalesced.load(AtomicOrdering::Relaxed),
            facet_evictions: self.inner.facet_evictions.load(AtomicOrdering::Relaxed),
            facet_cache_max_entries: FACET_CACHE_MAX_ENTRIES,
            facet_cache_max_items: FACET_CACHE_MAX_ITEMS,
            facet_cache_ttl_millis: FACET_CACHE_TTL.as_millis() as u64,
            facet_bitmap,
            work_detail_legacy_requests: self
                .inner
                .work_detail_legacy_requests
                .load(AtomicOrdering::Relaxed),
            work_detail_summary_requests: self
                .inner
                .work_detail_summary_requests
                .load(AtomicOrdering::Relaxed),
        }
    }
}

#[derive(Debug, Default, Clone)]
struct CatalogMetricAccumulator {
    count: u64,
    slow_count: u64,
    total_micros: u128,
    max_micros: u128,
}

#[derive(Debug, Serialize)]
pub struct CatalogQueryMetric {
    pub count: u64,
    pub slow_count: u64,
    pub average_millis: f64,
    pub max_millis: f64,
}

static CATALOG_QUERY_METRICS: LazyLock<Mutex<BTreeMap<&'static str, CatalogMetricAccumulator>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

struct CatalogQueryTimer {
    class: &'static str,
    started: Instant,
}

impl CatalogQueryTimer {
    fn start(class: &'static str) -> Self {
        Self {
            class,
            started: Instant::now(),
        }
    }
}

impl Drop for CatalogQueryTimer {
    fn drop(&mut self) {
        let elapsed = self.started.elapsed();
        let micros = elapsed.as_micros();
        if let Ok(mut metrics) = CATALOG_QUERY_METRICS.lock() {
            let metric = metrics.entry(self.class).or_default();
            metric.count = metric.count.saturating_add(1);
            metric.slow_count = metric
                .slow_count
                .saturating_add(u64::from(elapsed.as_millis() >= CATALOG_SLOW_QUERY_MILLIS));
            metric.total_micros = metric.total_micros.saturating_add(micros);
            metric.max_micros = metric.max_micros.max(micros);
        }
        if elapsed.as_millis() >= CATALOG_SLOW_QUERY_MILLIS {
            tracing::warn!(
                query_class = self.class,
                elapsed_millis = elapsed.as_millis() as u64,
                "slow catalog request"
            );
        }
    }
}

pub fn query_metrics_snapshot() -> BTreeMap<&'static str, CatalogQueryMetric> {
    let Ok(metrics) = CATALOG_QUERY_METRICS.lock() else {
        return BTreeMap::new();
    };
    metrics
        .iter()
        .map(|(class, metric)| {
            let average_millis = if metric.count == 0 {
                0.0
            } else {
                metric.total_micros as f64 / metric.count as f64 / 1000.0
            };
            (
                *class,
                CatalogQueryMetric {
                    count: metric.count,
                    slow_count: metric.slow_count,
                    average_millis,
                    max_millis: metric.max_micros as f64 / 1000.0,
                },
            )
        })
        .collect()
}

#[derive(Debug, Deserialize)]
pub struct CatalogWorksQuery {
    pub kind: Option<String>,
    pub collection: Option<String>,
    pub include_tag: Option<String>,
    pub q: Option<String>,
    pub sort: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogWorkItem {
    pub id: i64,
    pub kind: String,
    pub title: String,
    pub subtitle: Option<String>,
    pub cover_asset_id: Option<i64>,
    pub cover_version: String,
    pub progress: f64,
    pub asset_count: i64,
    pub tag_count: i64,
    pub image_count: i64,
    pub track_count: i64,
    pub page_count: i64,
    pub collection_key: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct CatalogWorksResponse {
    pub items: Vec<CatalogWorkItem>,
    pub next_cursor: Option<String>,
    pub catalog_revision: i64,
    pub activity_revision: i64,
}

#[derive(Debug, Deserialize)]
pub struct CatalogRandomQuery {
    pub kind: Option<String>,
    pub collection: Option<String>,
    pub include_tag: Option<String>,
    pub q: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CatalogRandomResponse {
    pub item: Option<CatalogWorkItem>,
    pub catalog_revision: i64,
    pub activity_revision: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogFacetsQuery {
    pub kind: Option<String>,
    pub collection: Option<String>,
    pub include_tag: Option<String>,
    pub q: Option<String>,
    pub tag_q: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogTagFacet {
    pub id: i64,
    pub namespace: String,
    pub key: String,
    pub label: String,
    pub translated_label: Option<String>,
    pub translated_namespace: Option<String>,
    pub context_count: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogFacetsResponse {
    pub items: Vec<CatalogTagFacet>,
    pub next_cursor: Option<String>,
    pub catalog_revision: i64,
}

#[derive(Debug, Serialize)]
pub struct TagKindCountStatus {
    pub ready: bool,
    pub catalog_revision: i64,
    pub rows: i64,
    pub associations: i64,
    pub updated_at: String,
    pub last_error: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CatalogCountsQuery {
    pub include_tag: Option<String>,
    pub q: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CatalogCountsResponse {
    pub kinds: BTreeMap<String, i64>,
    pub total: i64,
    pub catalog_revision: i64,
    pub activity_revision: i64,
}

#[derive(Debug, Deserialize)]
pub struct CatalogCollectionsQuery {
    pub kind: Option<String>,
    pub include_tag: Option<String>,
    pub q: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogCollectionItem {
    pub id: i64,
    pub kind: String,
    pub title: String,
    pub subtitle: String,
    pub cover_asset_id: Option<i64>,
    pub cover_version: String,
    pub progress: f64,
    pub work_count: i64,
    pub tag_count: i64,
    pub page_count: i64,
    pub collection_key: String,
    pub first_work_id: i64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct CatalogCollectionsResponse {
    pub items: Vec<CatalogCollectionItem>,
    pub next_cursor: Option<String>,
    pub catalog_revision: i64,
    pub activity_revision: i64,
    pub backfill_pending: bool,
}

#[derive(Debug, Deserialize)]
pub struct CatalogHistoryQuery {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogHistoryItem {
    #[serde(flatten)]
    pub work: CatalogWorkItem,
    pub position: Option<String>,
    pub last_opened_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct CatalogHistoryResponse {
    pub items: Vec<CatalogHistoryItem>,
    pub next_cursor: Option<String>,
    pub catalog_revision: i64,
    pub activity_revision: i64,
}

#[derive(Debug, Deserialize)]
pub struct CatalogAssetsQuery {
    pub role: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogAssetItem {
    pub id: i64,
    pub work_id: i64,
    pub name: String,
    pub mime: String,
    pub role: String,
    pub variant: Option<String>,
    pub position: Option<i64>,
    pub size: Option<i64>,
    pub meta_json: String,
    pub created_at: DateTime<Utc>,
}

fn catalog_asset_item_from_row(row: SqliteRow) -> CatalogAssetItem {
    let path: String = row.get("path");
    CatalogAssetItem {
        id: row.get("id"),
        work_id: row.get("work_id"),
        name: asset_display_name(&path),
        mime: row.get("mime"),
        role: row.get("role"),
        variant: row.get("variant"),
        position: row.get("position"),
        size: row.get("size"),
        meta_json: row.get("meta_json"),
        created_at: row.get("created_at"),
    }
}

fn catalog_asset_order(item: &CatalogAssetItem) -> (&str, i64, i64) {
    (&item.role, item.position.unwrap_or(i64::MAX), item.id)
}

#[derive(Debug, Serialize)]
pub struct CatalogAssetsResponse {
    pub items: Vec<CatalogAssetItem>,
    pub next_cursor: Option<String>,
    pub total: i64,
    pub source_version: String,
}

#[derive(Debug, Clone)]
struct CatalogFilter {
    kind: Option<String>,
    collection: Option<String>,
    tags: Vec<String>,
    q: String,
    sort: CatalogSort,
}

#[derive(Debug, Clone, Copy)]
enum CatalogSort {
    UpdatedDesc,
    Relevance,
}

#[derive(Debug, Serialize, Deserialize)]
struct WorkCursor {
    rank: Option<i64>,
    updated_at: DateTime<Utc>,
    id: i64,
    query_hash: String,
    #[serde(default)]
    catalog_revision: Option<i64>,
    #[serde(default)]
    activity_revision: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct FacetCursor {
    context_count: i64,
    tag_id: i64,
    query_hash: String,
    #[serde(default)]
    catalog_revision: Option<i64>,
}

struct DynamicFacetQuery<'a> {
    filter: &'a CatalogFilter,
    tags_json: &'a str,
    search_candidates: Option<&'a str>,
    tag_query: &'a str,
    cursor: Option<&'a FacetCursor>,
    limit: i64,
    query_hash: &'a str,
    expected_catalog_revision: i64,
    expected_search_revision: Option<i64>,
}

#[derive(Debug, Clone)]
struct CatalogSearchCandidates {
    encoded_ids: String,
    catalog_revision: i64,
    search_revision: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct CollectionCursor {
    updated_at: DateTime<Utc>,
    collection_key: String,
    query_hash: String,
    #[serde(default)]
    catalog_revision: Option<i64>,
    #[serde(default)]
    activity_revision: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct HistoryCursor {
    last_opened_at: DateTime<Utc>,
    work_id: i64,
    query_hash: String,
    #[serde(default)]
    catalog_revision: Option<i64>,
    #[serde(default)]
    activity_revision: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AssetCursor {
    role: String,
    position: i64,
    id: i64,
    query_hash: String,
    #[serde(default)]
    source_version: Option<String>,
    #[serde(default)]
    catalog_revision: Option<i64>,
}

struct WorkRow {
    item: CatalogWorkItem,
    stats_pending: bool,
    search_rank: Option<i64>,
}

/// New keyset cursors carry the revision of the snapshot that produced them.
/// Older cursors are accepted during the migration window because their
/// revision fields deserialize as `None`; they retain the previous best-effort
/// semantics until the client requests a fresh page.
fn validate_cursor_revisions(
    catalog_revision: Option<i64>,
    activity_revision: Option<i64>,
    current: RevisionSnapshot,
    label: &str,
) -> Result<()> {
    if catalog_revision.is_some_and(|revision| revision != current.catalog_revision)
        || activity_revision.is_some_and(|revision| revision != current.activity_revision)
    {
        return Err(AppError::BadRequest(format!(
            "{label} cursor expired after the catalog changed"
        )));
    }
    Ok(())
}

pub async fn works(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CatalogWorksQuery>,
) -> Result<Json<CatalogWorksResponse>> {
    let _timer = CatalogQueryTimer::start("works");
    let candidates = catalog_search_candidates(&state, query.q.as_deref()).await?;
    Ok(Json(
        query_works_with_candidates(
            &state.db,
            query,
            candidates.as_ref().map(|value| value.encoded_ids.as_str()),
            candidates.as_ref().map(|value| value.catalog_revision),
            candidates.as_ref().map(|value| value.search_revision),
        )
        .await?,
    ))
}

pub async fn random(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CatalogRandomQuery>,
) -> Result<Json<CatalogRandomResponse>> {
    let _timer = CatalogQueryTimer::start("random");
    let candidates = catalog_search_candidates(&state, query.q.as_deref()).await?;
    Ok(Json(
        query_random_work_with_candidates(
            &state.db,
            query,
            candidates.as_ref().map(|value| value.encoded_ids.as_str()),
            candidates.as_ref().map(|value| value.catalog_revision),
            candidates.as_ref().map(|value| value.search_revision),
        )
        .await?,
    ))
}

pub async fn facets_tags(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CatalogFacetsQuery>,
) -> Result<Json<CatalogFacetsResponse>> {
    let _timer = CatalogQueryTimer::start("facets.tags");
    let candidates = catalog_search_candidates(&state, query.q.as_deref()).await?;
    for _attempt in 0..=FACET_REVISION_RETRY_LIMIT {
        let revisions = state.db.revision_fence_snapshot().await?;
        let revision = revisions.catalog_revision;
        if candidates
            .as_ref()
            .is_some_and(|value| value.catalog_revision != revision)
            || candidates
                .as_ref()
                .is_some_and(|value| value.search_revision != revisions.search_revision)
        {
            return Err(AppError::Overloaded {
                message: "search candidates expired while preparing facets".to_string(),
                retry_after_seconds: 1,
            });
        }
        let cache_key = facet_cache_key(
            revision,
            &query,
            candidates.as_ref().map(|value| value.encoded_ids.as_str()),
        )?;
        let db = state.db.clone();
        let runtime = state.catalog_runtime.clone();
        let resources = state.resources.clone();
        let candidates_for_load = candidates.clone();
        let query_for_load = query.clone();
        let facet_bitmap_enabled = state.config.facet_bitmap_enabled;
        let response = state
            .catalog_runtime
            .cached_tag_facets(cache_key, move || async move {
                let response = query_tag_facets_with_candidates(
                    &db,
                    query_for_load,
                    candidates_for_load
                        .as_ref()
                        .map(|value| value.encoded_ids.as_str()),
                    facet_bitmap_enabled.then_some((&runtime, &resources)),
                    revision,
                    candidates_for_load
                        .as_ref()
                        .map(|value| value.search_revision),
                )
                .await?;
                Ok(if response.catalog_revision == revision {
                    FacetCacheLoad::Ready(response)
                } else {
                    FacetCacheLoad::RevisionChanged
                })
            })
            .await?;
        if let Some(response) = response {
            return Ok(Json(response));
        }
    }
    Err(AppError::Overloaded {
        message: "catalog changed repeatedly while preparing tag facets".to_string(),
        retry_after_seconds: 1,
    })
}

pub async fn counts(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CatalogCountsQuery>,
) -> Result<Json<CatalogCountsResponse>> {
    let _timer = CatalogQueryTimer::start("counts");
    let candidates = catalog_search_candidates(&state, query.q.as_deref()).await?;
    Ok(Json(
        query_counts_with_candidates(
            &state.db,
            query,
            candidates.as_ref().map(|value| value.encoded_ids.as_str()),
            candidates.as_ref().map(|value| value.catalog_revision),
            candidates.as_ref().map(|value| value.search_revision),
        )
        .await?,
    ))
}

pub async fn collections(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CatalogCollectionsQuery>,
) -> Result<Json<CatalogCollectionsResponse>> {
    let _timer = CatalogQueryTimer::start("collections");
    let candidates = catalog_search_candidates(&state, query.q.as_deref()).await?;
    Ok(Json(
        query_collections_with_candidates(
            &state.db,
            query,
            candidates.as_ref().map(|value| value.encoded_ids.as_str()),
            candidates.as_ref().map(|value| value.catalog_revision),
            candidates.as_ref().map(|value| value.search_revision),
        )
        .await?,
    ))
}

pub async fn history(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CatalogHistoryQuery>,
) -> Result<Json<CatalogHistoryResponse>> {
    let _timer = CatalogQueryTimer::start("history");
    Ok(Json(query_history(&state.db, query).await?))
}

pub async fn assets(
    State(state): State<Arc<AppState>>,
    Path(work_id): Path<i64>,
    Query(query): Query<CatalogAssetsQuery>,
) -> Result<Json<CatalogAssetsResponse>> {
    let _timer = CatalogQueryTimer::start("assets");
    Ok(Json(query_assets(&state.db, work_id, query).await?))
}

#[cfg(test)]
async fn query_works(db: &Db, query: CatalogWorksQuery) -> Result<CatalogWorksResponse> {
    query_works_with_candidates(db, query, None, None, None).await
}

#[cfg(test)]
async fn query_random_work(db: &Db, query: CatalogRandomQuery) -> Result<CatalogRandomResponse> {
    query_random_work_with_candidates(db, query, None, None, None).await
}

async fn query_random_work_with_candidates(
    db: &Db,
    query: CatalogRandomQuery,
    search_candidates: Option<&str>,
    expected_catalog_revision: Option<i64>,
    expected_search_revision: Option<i64>,
) -> Result<CatalogRandomResponse> {
    let filter = CatalogFilter::from_random_query(&query)?;
    let mut retried_stats_backfill = false;
    let (row, revisions) = loop {
        let mut transaction = db.begin_tracked_read_transaction().await?;
        let revisions = db
            .revision_snapshot_with_connection(&mut transaction)
            .await?;
        if expected_catalog_revision.is_some_and(|revision| revision != revisions.catalog_revision)
            || expected_search_revision
                .is_some_and(|revision| revision != revisions.search_revision)
        {
            transaction.rollback().await?;
            return Err(search_candidates_expired_error());
        }
        let row = fetch_random_work_row(&mut transaction, &filter, search_candidates).await?;
        if row.as_ref().is_some_and(|row| row.stats_pending) && !retried_stats_backfill {
            let work_id = row.as_ref().map(|row| row.item.id).unwrap_or_default();
            transaction.rollback().await?;
            backfill_stats_for_ids(db, &[work_id]).await?;
            retried_stats_backfill = true;
            continue;
        }
        transaction.commit().await?;
        break (row, revisions);
    };
    Ok(CatalogRandomResponse {
        item: row.map(|row| row.item),
        catalog_revision: revisions.catalog_revision,
        activity_revision: revisions.activity_revision,
    })
}

async fn query_works_with_candidates(
    db: &Db,
    query: CatalogWorksQuery,
    search_candidates: Option<&str>,
    expected_catalog_revision: Option<i64>,
    expected_search_revision: Option<i64>,
) -> Result<CatalogWorksResponse> {
    let filter = CatalogFilter::from_works_query(&query)?;
    let query_hash = filter.hash("works", &search_candidate_scope(search_candidates))?;
    let cursor = query
        .cursor
        .as_deref()
        .map(decode_cursor::<WorkCursor>)
        .transpose()?;
    if cursor
        .as_ref()
        .is_some_and(|cursor| cursor.query_hash != query_hash)
    {
        return Err(AppError::BadRequest(
            "catalog cursor does not match the active filters".to_string(),
        ));
    }
    let limit = query
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let mut retried_stats_backfill = false;
    let (mut rows, revisions) = loop {
        let mut transaction = db.begin_tracked_read_transaction().await?;
        let revisions = db
            .revision_snapshot_with_connection(&mut transaction)
            .await?;
        if expected_catalog_revision.is_some_and(|revision| revision != revisions.catalog_revision)
            || expected_search_revision
                .is_some_and(|revision| revision != revisions.search_revision)
        {
            transaction.rollback().await?;
            return Err(search_candidates_expired_error());
        }
        if let Some(cursor) = cursor.as_ref() {
            if let Err(error) = validate_cursor_revisions(
                cursor.catalog_revision,
                cursor.activity_revision,
                revisions,
                "catalog",
            ) {
                transaction.rollback().await?;
                return Err(error);
            }
        }
        let rows = fetch_work_rows(
            &mut transaction,
            &filter,
            search_candidates,
            cursor.as_ref(),
            limit + 1,
        )
        .await?;
        let pending_ids = rows
            .iter()
            .filter(|row| row.stats_pending)
            .map(|row| row.item.id)
            .collect::<Vec<_>>();
        if !pending_ids.is_empty() && !retried_stats_backfill {
            transaction.rollback().await?;
            backfill_stats_for_ids(db, &pending_ids).await?;
            retried_stats_backfill = true;
            continue;
        }
        transaction.commit().await?;
        break (rows, revisions);
    };
    let has_more = rows.len() as i64 > limit;
    rows.truncate(limit as usize);
    let next_cursor = if has_more {
        rows.last()
            .map(|row| {
                encode_cursor(&WorkCursor {
                    rank: row.search_rank,
                    updated_at: row.item.updated_at,
                    id: row.item.id,
                    query_hash: query_hash.to_string(),
                    catalog_revision: Some(revisions.catalog_revision),
                    activity_revision: Some(revisions.activity_revision),
                })
            })
            .transpose()?
    } else {
        None
    };
    Ok(CatalogWorksResponse {
        items: rows.into_iter().map(|row| row.item).collect(),
        next_cursor,
        catalog_revision: revisions.catalog_revision,
        activity_revision: revisions.activity_revision,
    })
}

async fn fetch_work_rows(
    connection: &mut SqliteConnection,
    filter: &CatalogFilter,
    search_candidates: Option<&str>,
    cursor: Option<&WorkCursor>,
    limit: i64,
) -> Result<Vec<WorkRow>> {
    let tags_json = selected_tags_json(&filter.tags)?;
    let relevance_order =
        search_candidates.is_some() && matches!(filter.sort, CatalogSort::Relevance);
    let mut sql = String::new();
    let mut has_cte = false;
    if !filter.tags.is_empty() {
        sql.push_str(
            r#"
            WITH selected_tags(tag_id) AS (
                SELECT tag.id
                FROM json_each(?) AS input
                JOIN tags AS tag
                  ON tag.namespace = json_extract(input.value, '$[0]')
                 AND tag.key = json_extract(input.value, '$[1]')
            ),
            matched(work_id) AS (
                SELECT work_tag.work_id
                FROM work_tags AS work_tag
                JOIN selected_tags AS selected ON selected.tag_id = work_tag.tag_id
                GROUP BY work_tag.work_id
                HAVING COUNT(DISTINCT work_tag.tag_id) = json_array_length(?)
            )
            "#,
        );
        has_cte = true;
    }
    if search_candidates.is_some() {
        if has_cte {
            sql.push(',');
        } else {
            sql.push_str("WITH ");
        }
        sql.push_str(
            r#"
            search_candidates(work_id, rank) AS (
                SELECT CAST(value AS INTEGER), CAST(key AS INTEGER)
                FROM json_each(?)
            )
            "#,
        );
    }
    sql.push_str(
        r#"
        SELECT
            work.id, work.kind, work.title, work.subtitle, work.cover_asset_id,
            work.progress, work.updated_at,
            COALESCE(stats.asset_count, 0) AS asset_count,
            COALESCE(stats.tag_count, 0) AS tag_count,
            COALESCE(stats.image_count, 0) AS image_count,
            COALESCE(stats.track_count, 0) AS track_count,
            COALESCE(stats.page_count, 0) AS page_count,
            stats.collection_key,
            "#,
    );
    if relevance_order {
        sql.push_str(" search_candidate.rank AS search_rank,");
    } else {
        sql.push_str(" NULL AS search_rank,");
    }
    sql.push_str(
        r#"
            CASE WHEN stats.computed_at IS NULL THEN 1 ELSE 0 END AS stats_pending
        FROM works AS work
        LEFT JOIN work_stats AS stats ON stats.work_id = work.id
        "#,
    );
    if search_candidates.is_some() {
        sql.push_str(
            " JOIN search_candidates AS search_candidate ON search_candidate.work_id = work.id",
        );
    }
    sql.push_str(
        r#"
        WHERE 1 = 1
        "#,
    );
    sql.push_str(" AND work.deleted_at IS NULL");
    if filter.kind.is_some() {
        sql.push_str(" AND work.kind = ?");
    }
    if !filter.tags.is_empty() {
        sql.push_str(" AND work.id IN (SELECT work_id FROM matched)");
    }
    if filter.collection.is_some() {
        sql.push_str(" AND stats.collection_key = ?");
    }
    if search_candidates.is_none() && !filter.q.is_empty() {
        sql.push_str(
            r#" AND (
                work.title LIKE ? ESCAPE '\'
                OR COALESCE(work.subtitle, '') LIKE ? ESCAPE '\'
            )"#,
        );
    }
    if let Some(cursor) = cursor {
        if relevance_order {
            if cursor.rank.is_none() {
                return Err(AppError::BadRequest(
                    "relevance cursor is missing its rank".to_string(),
                ));
            }
            sql.push_str(
                " AND (search_candidate.rank > ? OR (search_candidate.rank = ? AND work.id > ?))",
            );
        } else {
            sql.push_str(" AND (work.updated_at < ? OR (work.updated_at = ? AND work.id < ?))");
        }
    }
    match (filter.sort, relevance_order) {
        (CatalogSort::Relevance, true) => {
            sql.push_str(" ORDER BY search_candidate.rank ASC, work.id ASC LIMIT ?")
        }
        _ => sql.push_str(" ORDER BY work.updated_at DESC, work.id DESC LIMIT ?"),
    }

    let mut statement = sqlx::query(&sql);
    if !filter.tags.is_empty() {
        statement = statement.bind(&tags_json).bind(&tags_json);
    }
    if let Some(search_candidates) = search_candidates {
        statement = statement.bind(search_candidates);
    }
    if let Some(kind) = &filter.kind {
        statement = statement.bind(kind);
    }
    if let Some(collection) = &filter.collection {
        statement = statement.bind(collection);
    }
    if search_candidates.is_none() && !filter.q.is_empty() {
        let pattern = contains_pattern(&filter.q);
        statement = statement.bind(pattern.clone()).bind(pattern);
    }
    if let Some(cursor) = cursor {
        if relevance_order {
            statement = statement
                .bind(cursor.rank.unwrap_or_default())
                .bind(cursor.rank.unwrap_or_default())
                .bind(cursor.id);
        } else {
            statement = statement
                .bind(cursor.updated_at)
                .bind(cursor.updated_at)
                .bind(cursor.id);
        }
    }
    statement = statement.bind(limit);
    let rows = statement.fetch_all(&mut *connection).await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let updated_at: DateTime<Utc> = row.get("updated_at");
            WorkRow {
                item: CatalogWorkItem {
                    id: row.get("id"),
                    kind: row.get("kind"),
                    title: row.get("title"),
                    subtitle: row.get("subtitle"),
                    cover_asset_id: row.get("cover_asset_id"),
                    cover_version: updated_at.to_rfc3339(),
                    progress: row.get("progress"),
                    asset_count: row.get("asset_count"),
                    tag_count: row.get("tag_count"),
                    image_count: row.get("image_count"),
                    track_count: row.get("track_count"),
                    page_count: row.get("page_count"),
                    collection_key: row.get("collection_key"),
                    updated_at,
                },
                stats_pending: row.get::<i64, _>("stats_pending") != 0,
                search_rank: row.get("search_rank"),
            }
        })
        .collect())
}

async fn fetch_random_work_row(
    connection: &mut SqliteConnection,
    filter: &CatalogFilter,
    search_candidates: Option<&str>,
) -> Result<Option<WorkRow>> {
    let tags_json = selected_tags_json(&filter.tags)?;
    let mut sql = String::new();
    if !filter.tags.is_empty() {
        sql.push_str(
            r#"
            WITH selected_tags(tag_id) AS (
                SELECT tag.id
                FROM json_each(?) AS input
                JOIN tags AS tag
                  ON tag.namespace = json_extract(input.value, '$[0]')
                 AND tag.key = json_extract(input.value, '$[1]')
            ),
            matched(work_id) AS (
                SELECT work_tag.work_id
                FROM work_tags AS work_tag
                JOIN selected_tags AS selected ON selected.tag_id = work_tag.tag_id
                GROUP BY work_tag.work_id
                HAVING COUNT(DISTINCT work_tag.tag_id) = json_array_length(?)
            ),
            "#,
        );
    } else {
        sql.push_str("WITH ");
    }
    sql.push_str(
        r#"
        candidates(work_id) AS MATERIALIZED (
            SELECT work.id
            FROM works AS work
            LEFT JOIN work_stats AS stats ON stats.work_id = work.id
            WHERE 1 = 1
        "#,
    );
    sql.push_str(" AND work.deleted_at IS NULL");
    if filter.kind.is_some() {
        sql.push_str(" AND work.kind = ?");
    }
    if !filter.tags.is_empty() {
        sql.push_str(" AND work.id IN (SELECT work_id FROM matched)");
    }
    if filter.collection.is_some() {
        sql.push_str(" AND stats.collection_key = ?");
    }
    if search_candidates.is_some() {
        sql.push_str(" AND work.id IN (SELECT CAST(value AS INTEGER) FROM json_each(?))");
    } else if !filter.q.is_empty() {
        sql.push_str(
            r#" AND (
                work.title LIKE ? ESCAPE '\'
                OR COALESCE(work.subtitle, '') LIKE ? ESCAPE '\'
            )"#,
        );
    }
    sql.push_str(
        r#"
        ),
        chosen(work_id) AS (
            SELECT work_id
            FROM candidates
            LIMIT 1 OFFSET (
                SELECT CASE
                    WHEN COUNT(*) = 0 THEN 0
                    ELSE (random() & 9223372036854775807) % COUNT(*)
                END
                FROM candidates
            )
        )
        SELECT
            work.id, work.kind, work.title, work.subtitle, work.cover_asset_id,
            work.progress, work.updated_at,
            COALESCE(stats.asset_count, 0) AS asset_count,
            COALESCE(stats.tag_count, 0) AS tag_count,
            COALESCE(stats.image_count, 0) AS image_count,
            COALESCE(stats.track_count, 0) AS track_count,
            COALESCE(stats.page_count, 0) AS page_count,
            stats.collection_key,
            CASE WHEN stats.computed_at IS NULL THEN 1 ELSE 0 END AS stats_pending
        FROM chosen
        JOIN works AS work ON work.id = chosen.work_id
        LEFT JOIN work_stats AS stats ON stats.work_id = work.id
        "#,
    );

    let mut statement = sqlx::query(&sql);
    if !filter.tags.is_empty() {
        statement = statement.bind(&tags_json).bind(&tags_json);
    }
    if let Some(kind) = &filter.kind {
        statement = statement.bind(kind);
    }
    if let Some(collection) = &filter.collection {
        statement = statement.bind(collection);
    }
    if let Some(search_candidates) = search_candidates {
        statement = statement.bind(search_candidates);
    } else if !filter.q.is_empty() {
        let pattern = contains_pattern(&filter.q);
        statement = statement.bind(pattern.clone()).bind(pattern);
    }
    let row = statement.fetch_optional(&mut *connection).await?;
    Ok(row.map(|row| {
        let updated_at: DateTime<Utc> = row.get("updated_at");
        WorkRow {
            item: CatalogWorkItem {
                id: row.get("id"),
                kind: row.get("kind"),
                title: row.get("title"),
                subtitle: row.get("subtitle"),
                cover_asset_id: row.get("cover_asset_id"),
                cover_version: updated_at.to_rfc3339(),
                progress: row.get("progress"),
                asset_count: row.get("asset_count"),
                tag_count: row.get("tag_count"),
                image_count: row.get("image_count"),
                track_count: row.get("track_count"),
                page_count: row.get("page_count"),
                collection_key: row.get("collection_key"),
                updated_at,
            },
            stats_pending: row.get::<i64, _>("stats_pending") != 0,
            search_rank: None,
        }
    }))
}

#[cfg(test)]
async fn query_tag_facets(db: &Db, query: CatalogFacetsQuery) -> Result<CatalogFacetsResponse> {
    let revision = catalog_revision(db).await?;
    query_tag_facets_with_candidates(db, query, None, None, revision, None).await
}

async fn query_tag_facets_with_candidates(
    db: &Db,
    query: CatalogFacetsQuery,
    search_candidates: Option<&str>,
    facet_bitmap_runtime: Option<(&CatalogRuntime, &ResourceGovernor)>,
    expected_revision: i64,
    expected_search_revision: Option<i64>,
) -> Result<CatalogFacetsResponse> {
    let filter = CatalogFilter::from_facet_query(&query)?;
    let tag_query = bounded_query(query.tag_q.as_deref())?;
    let query_hash = filter.hash(
        "facets",
        &format!("{tag_query}\0{}", search_candidate_scope(search_candidates)),
    )?;
    let cursor = query
        .cursor
        .as_deref()
        .map(decode_cursor::<FacetCursor>)
        .transpose()?;
    if cursor
        .as_ref()
        .is_some_and(|cursor| cursor.query_hash != query_hash)
    {
        return Err(AppError::BadRequest(
            "facet cursor does not match the active filters".to_string(),
        ));
    }
    if cursor
        .as_ref()
        .and_then(|cursor| cursor.catalog_revision)
        .is_some_and(|revision| revision != expected_revision)
    {
        return Err(AppError::BadRequest(
            "facet cursor expired after the catalog changed".to_string(),
        ));
    }
    let limit = query.limit.unwrap_or(100).clamp(1, MAX_PAGE_SIZE);
    let tags_json = selected_tags_json(&filter.tags)?;
    if filter.collection.is_none() && filter.q.is_empty() && search_candidates.is_none() {
        if let Some(response) = try_query_preaggregated_tag_facets(
            db,
            filter.kind.as_deref(),
            (!filter.tags.is_empty()).then_some(tags_json.as_str()),
            &tag_query,
            cursor.as_ref(),
            limit,
            &query_hash,
        )
        .await?
        {
            return Ok(response);
        }
    }
    if let Some((runtime, resources)) = facet_bitmap_runtime {
        if filter.collection.is_none()
            && filter.q.is_empty()
            && search_candidates.is_none()
            && !filter.tags.is_empty()
        {
            let Some(selected_tag_ids) =
                resolve_selected_tag_ids_for_revision(db, &tags_json, expected_revision).await?
            else {
                // The short tag-ID resolver observed a newer catalog snapshot.
                // Do not mix those IDs with an index built for
                // `expected_revision`; the dynamic path below will read one
                // current snapshot and the outer cache wrapper will retry if
                // its revision no longer matches the cache key.
                return query_dynamic_tag_facets(
                    db,
                    DynamicFacetQuery {
                        filter: &filter,
                        tags_json: &tags_json,
                        search_candidates,
                        tag_query: &tag_query,
                        cursor: cursor.as_ref(),
                        limit,
                        query_hash: &query_hash,
                        expected_catalog_revision: expected_revision,
                        expected_search_revision,
                    },
                )
                .await;
            };
            if selected_tag_ids.len() != filter.tags.len() {
                return Ok(CatalogFacetsResponse {
                    items: Vec::new(),
                    next_cursor: None,
                    catalog_revision: expected_revision,
                });
            }
            if let Some(bitmap_counts) = runtime
                .inner
                .facet_bitmap
                .try_counts(
                    db,
                    resources,
                    expected_revision,
                    filter.kind.as_deref(),
                    &selected_tag_ids,
                )
                .await?
            {
                if let Some(response) = query_bitmap_tag_facets(
                    db,
                    bitmap_counts,
                    &tag_query,
                    cursor.as_ref(),
                    limit,
                    &query_hash,
                )
                .await?
                {
                    return Ok(response);
                }
            }
        }
    }
    if filter.collection.is_none()
        && filter.q.is_empty()
        && search_candidates.is_none()
        && filter.tags.len() == 1
    {
        return query_single_selected_tag_facets(
            db,
            &tags_json,
            filter.kind.as_deref(),
            &tag_query,
            cursor.as_ref(),
            limit,
            &query_hash,
        )
        .await;
    }
    query_dynamic_tag_facets(
        db,
        DynamicFacetQuery {
            filter: &filter,
            tags_json: &tags_json,
            search_candidates,
            tag_query: &tag_query,
            cursor: cursor.as_ref(),
            limit,
            query_hash: &query_hash,
            expected_catalog_revision: expected_revision,
            expected_search_revision,
        },
    )
    .await
}

/// Run the fact-table facet query from one explicit snapshot.  Keeping the
/// dynamic fallback in a helper lets the bitmap route reject a mismatched
/// resolver revision without holding a read transaction across index refresh
/// or resource admission.
async fn query_dynamic_tag_facets(
    db: &Db,
    request: DynamicFacetQuery<'_>,
) -> Result<CatalogFacetsResponse> {
    let DynamicFacetQuery {
        filter,
        tags_json,
        search_candidates,
        tag_query,
        cursor,
        limit,
        query_hash,
        expected_catalog_revision,
        expected_search_revision,
    } = request;
    let mut sql = String::new();
    if !filter.tags.is_empty() {
        sql.push_str(
            r#"
            WITH selected_tags(tag_id) AS (
                SELECT tag.id FROM json_each(?) AS input
                JOIN tags AS tag
                  ON tag.namespace = json_extract(input.value, '$[0]')
                 AND tag.key = json_extract(input.value, '$[1]')
            ),
            matched(work_id) AS (
                SELECT work_tag.work_id
                FROM work_tags AS work_tag
                JOIN selected_tags AS selected ON selected.tag_id = work_tag.tag_id
                GROUP BY work_tag.work_id
                HAVING COUNT(DISTINCT work_tag.tag_id) = json_array_length(?)
            ),
            candidates(work_id) AS (
                SELECT work.id FROM works AS work WHERE work.deleted_at IS NULL
            "#,
        );
    } else {
        sql.push_str(
            r#"
            WITH candidates(work_id) AS (
                SELECT work.id FROM works AS work WHERE work.deleted_at IS NULL
            "#,
        );
    }
    if filter.kind.is_some() {
        sql.push_str(" AND work.kind = ?");
    }
    if !filter.tags.is_empty() {
        sql.push_str(" AND work.id IN (SELECT work_id FROM matched)");
    }
    if filter.collection.is_some() {
        sql.push_str(" AND work.id IN (SELECT work_id FROM work_stats WHERE collection_key = ?)");
    }
    if search_candidates.is_some() {
        sql.push_str(" AND work.id IN (SELECT CAST(value AS INTEGER) FROM json_each(?))");
    } else if !filter.q.is_empty() {
        sql.push_str(
            r#" AND (
                work.title LIKE ? ESCAPE '\'
                OR COALESCE(work.subtitle, '') LIKE ? ESCAPE '\'
            )"#,
        );
    }
    sql.push_str(
        r#"
            )
        SELECT tag.id, tag.namespace, tag.key, tag.label,
               tag.translated_label, tag.translated_namespace,
               COUNT(*) AS context_count
        FROM candidates AS candidate
        JOIN work_tags AS work_tag ON work_tag.work_id = candidate.work_id
        JOIN tags AS tag ON tag.id = work_tag.tag_id
        WHERE 1 = 1
        "#,
    );
    if !tag_query.is_empty() {
        sql.push_str(
            r#" AND (
                tag.key LIKE ? ESCAPE '\'
                OR tag.label LIKE ? ESCAPE '\'
                OR COALESCE(tag.translated_label, '') LIKE ? ESCAPE '\'
            )"#,
        );
    }
    sql.push_str(
        " GROUP BY tag.id, tag.namespace, tag.key, tag.label, tag.translated_label, tag.translated_namespace",
    );
    if cursor.is_some() {
        sql.push_str(" HAVING COUNT(*) < ? OR (COUNT(*) = ? AND tag.id > ?)");
    }
    sql.push_str(" ORDER BY context_count DESC, tag.id ASC LIMIT ?");

    let mut statement = sqlx::query(&sql);
    if !filter.tags.is_empty() {
        statement = statement.bind(tags_json).bind(tags_json);
    }
    if let Some(kind) = &filter.kind {
        statement = statement.bind(kind);
    }
    if let Some(collection) = &filter.collection {
        statement = statement.bind(collection);
    }
    if let Some(search_candidates) = search_candidates {
        statement = statement.bind(search_candidates);
    } else if !filter.q.is_empty() {
        let pattern = contains_pattern(&filter.q);
        statement = statement.bind(pattern.clone()).bind(pattern);
    }
    if !tag_query.is_empty() {
        let pattern = contains_pattern(tag_query);
        statement = statement
            .bind(pattern.clone())
            .bind(pattern.clone())
            .bind(pattern);
    }
    if let Some(cursor) = &cursor {
        statement = statement
            .bind(cursor.context_count)
            .bind(cursor.context_count)
            .bind(cursor.tag_id);
    }
    statement = statement.bind(limit + 1);
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let revisions = db
        .revision_snapshot_with_connection(&mut transaction)
        .await?;
    let revision = revisions.catalog_revision;
    if revision != expected_catalog_revision
        || expected_search_revision
            .is_some_and(|candidate_revision| candidate_revision != revisions.search_revision)
    {
        transaction.rollback().await?;
        return Err(search_candidates_expired_error());
    }
    if let Some(cursor) = cursor.as_ref() {
        if cursor
            .catalog_revision
            .is_some_and(|cursor_revision| cursor_revision != revision)
        {
            transaction.rollback().await?;
            return Err(AppError::BadRequest(
                "facet cursor expired after the catalog changed".to_string(),
            ));
        }
    }
    let rows = statement.fetch_all(&mut *transaction).await?;
    let mut items = rows
        .into_iter()
        .map(|row| CatalogTagFacet {
            id: row.get("id"),
            namespace: row.get("namespace"),
            key: row.get("key"),
            label: row.get("label"),
            translated_label: row.get("translated_label"),
            translated_namespace: row.get("translated_namespace"),
            context_count: row.get("context_count"),
        })
        .collect::<Vec<_>>();
    let has_more = items.len() as i64 > limit;
    items.truncate(limit as usize);
    let next_cursor = if has_more {
        items
            .last()
            .map(|item| {
                encode_cursor(&FacetCursor {
                    context_count: item.context_count,
                    tag_id: item.id,
                    query_hash: query_hash.to_string(),
                    catalog_revision: Some(revision),
                })
            })
            .transpose()?
    } else {
        None
    };
    transaction.commit().await?;
    Ok(CatalogFacetsResponse {
        items,
        next_cursor,
        catalog_revision: revision,
    })
}

async fn query_single_selected_tag_facets(
    db: &Db,
    selected_tags_json: &str,
    kind: Option<&str>,
    tag_query: &str,
    cursor: Option<&FacetCursor>,
    limit: i64,
    query_hash: &str,
) -> Result<CatalogFacetsResponse> {
    // A single selected tag does not need the general matched(work_id) CTE.
    // Starting from idx_work_tags_tag_work avoids a full work-set candidate
    // materialization and lets SQLite walk only the selected tag's members.
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let revision =
        sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
            .fetch_one(&mut *transaction)
            .await?;
    if cursor
        .and_then(|cursor| cursor.catalog_revision)
        .is_some_and(|cursor_revision| cursor_revision != revision)
    {
        transaction.rollback().await?;
        return Err(AppError::BadRequest(
            "facet cursor expired after the catalog changed".to_string(),
        ));
    }
    let Some(selected_tag_id) = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT tag.id
        FROM json_each(?1) AS input
        JOIN tags AS tag
          ON tag.namespace = json_extract(input.value, '$[0]')
         AND tag.key = json_extract(input.value, '$[1]')
        ORDER BY tag.id
        LIMIT 1
        "#,
    )
    .bind(selected_tags_json)
    .fetch_optional(&mut *transaction)
    .await?
    else {
        transaction.commit().await?;
        return Ok(CatalogFacetsResponse {
            items: Vec::new(),
            next_cursor: None,
            catalog_revision: revision,
        });
    };
    let mut sql = String::from(
        r#"
        SELECT tag.id, tag.namespace, tag.key, tag.label,
               tag.translated_label, tag.translated_namespace,
               COUNT(*) AS context_count
        FROM work_tags AS selected_work_tag
        JOIN works AS selected_work ON selected_work.id = selected_work_tag.work_id
        JOIN work_tags AS context_work_tag
          ON context_work_tag.work_id = selected_work_tag.work_id
        JOIN tags AS tag ON tag.id = context_work_tag.tag_id
        WHERE selected_work_tag.tag_id = ?
          AND selected_work.deleted_at IS NULL
        "#,
    );
    if kind.is_some() {
        sql.push_str(" AND selected_work.kind = ?");
    }
    if !tag_query.is_empty() {
        sql.push_str(
            r#" AND (
                tag.key LIKE ? ESCAPE '\'
                OR tag.label LIKE ? ESCAPE '\'
                OR COALESCE(tag.translated_label, '') LIKE ? ESCAPE '\'
            )"#,
        );
    }
    sql.push_str(
        " GROUP BY tag.id, tag.namespace, tag.key, tag.label, tag.translated_label, tag.translated_namespace",
    );
    if cursor.is_some() {
        sql.push_str(" HAVING COUNT(*) < ? OR (COUNT(*) = ? AND tag.id > ?)");
    }
    sql.push_str(" ORDER BY context_count DESC, tag.id ASC LIMIT ?");

    let mut statement = sqlx::query(&sql).bind(selected_tag_id);
    if let Some(kind) = kind {
        statement = statement.bind(kind);
    }
    if !tag_query.is_empty() {
        let pattern = contains_pattern(tag_query);
        statement = statement
            .bind(pattern.clone())
            .bind(pattern.clone())
            .bind(pattern);
    }
    if let Some(cursor) = cursor {
        statement = statement
            .bind(cursor.context_count)
            .bind(cursor.context_count)
            .bind(cursor.tag_id);
    }
    let rows = statement
        .bind(limit + 1)
        .fetch_all(&mut *transaction)
        .await?;
    let mut items = rows
        .into_iter()
        .map(|row| CatalogTagFacet {
            id: row.get("id"),
            namespace: row.get("namespace"),
            key: row.get("key"),
            label: row.get("label"),
            translated_label: row.get("translated_label"),
            translated_namespace: row.get("translated_namespace"),
            context_count: row.get("context_count"),
        })
        .collect::<Vec<_>>();
    let has_more = items.len() as i64 > limit;
    items.truncate(limit as usize);
    let next_cursor = if has_more {
        items
            .last()
            .map(|item| {
                encode_cursor(&FacetCursor {
                    context_count: item.context_count,
                    tag_id: item.id,
                    query_hash: query_hash.to_string(),
                    catalog_revision: Some(revision),
                })
            })
            .transpose()?
    } else {
        None
    };
    transaction.commit().await?;
    Ok(CatalogFacetsResponse {
        items,
        next_cursor,
        catalog_revision: revision,
    })
}

/// Resolve selected tag IDs in a short tracked snapshot that is explicitly
/// fenced to the Facet cache generation.  The caller intentionally commits
/// before any bitmap refresh or resource wait, then revalidates the bitmap
/// response against SQLite before returning it.
async fn resolve_selected_tag_ids_for_revision(
    db: &Db,
    selected_tags_json: &str,
    expected_revision: i64,
) -> Result<Option<Vec<i64>>> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let revision =
        sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
            .fetch_one(&mut *transaction)
            .await?;
    if revision != expected_revision {
        transaction.rollback().await?;
        return Ok(None);
    }
    let tag_ids =
        resolve_selected_tag_ids_with_connection(&mut transaction, selected_tags_json).await?;
    transaction.commit().await?;
    Ok(Some(tag_ids))
}

async fn resolve_selected_tag_ids_with_connection(
    connection: &mut SqliteConnection,
    selected_tags_json: &str,
) -> Result<Vec<i64>> {
    Ok(sqlx::query_scalar::<_, i64>(
        r#"
        SELECT tag.id
        FROM json_each(?1) AS input
        JOIN tags AS tag
          ON tag.namespace = json_extract(input.value, '$[0]')
         AND tag.key = json_extract(input.value, '$[1]')
        ORDER BY tag.id
        "#,
    )
    .bind(selected_tags_json)
    .fetch_all(&mut *connection)
    .await?)
}

async fn query_bitmap_tag_facets(
    db: &Db,
    bitmap_counts: facet_bitmap::FacetBitmapCounts,
    tag_query: &str,
    cursor: Option<&FacetCursor>,
    limit: i64,
    query_hash: &str,
) -> Result<Option<CatalogFacetsResponse>> {
    let counts_json = serde_json::to_string(&bitmap_counts.counts)
        .map_err(|error| AppError::Other(error.to_string()))?;
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let revision =
        sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
            .fetch_one(&mut *transaction)
            .await?;
    if revision != bitmap_counts.catalog_revision {
        transaction.rollback().await?;
        return Ok(None);
    }
    if cursor
        .and_then(|cursor| cursor.catalog_revision)
        .is_some_and(|cursor_revision| cursor_revision != revision)
    {
        transaction.rollback().await?;
        return Err(AppError::BadRequest(
            "facet cursor expired after the catalog changed".to_string(),
        ));
    }
    let mut sql = String::from(
        r#"
        WITH bitmap_counts(tag_id, context_count) AS (
            SELECT
                CAST(json_extract(value, '$[0]') AS INTEGER),
                CAST(json_extract(value, '$[1]') AS INTEGER)
            FROM json_each(?)
        )
        SELECT tag.id, tag.namespace, tag.key, tag.label,
               tag.translated_label, tag.translated_namespace,
               bitmap_counts.context_count
        FROM bitmap_counts
        JOIN tags AS tag ON tag.id = bitmap_counts.tag_id
        WHERE 1 = 1
        "#,
    );
    if !tag_query.is_empty() {
        sql.push_str(
            r#" AND (
                tag.key LIKE ? ESCAPE '\'
                OR tag.label LIKE ? ESCAPE '\'
                OR COALESCE(tag.translated_label, '') LIKE ? ESCAPE '\'
            )"#,
        );
    }
    if cursor.is_some() {
        sql.push_str(
            " AND (bitmap_counts.context_count < ? OR (bitmap_counts.context_count = ? AND tag.id > ?))",
        );
    }
    sql.push_str(" ORDER BY bitmap_counts.context_count DESC, tag.id ASC LIMIT ?");

    let mut statement = sqlx::query(&sql).bind(&counts_json);
    if !tag_query.is_empty() {
        let pattern = contains_pattern(tag_query);
        statement = statement
            .bind(pattern.clone())
            .bind(pattern.clone())
            .bind(pattern);
    }
    if let Some(cursor) = cursor {
        statement = statement
            .bind(cursor.context_count)
            .bind(cursor.context_count)
            .bind(cursor.tag_id);
    }
    let rows = statement
        .bind(limit + 1)
        .fetch_all(&mut *transaction)
        .await?;
    let mut items = rows
        .into_iter()
        .map(|row| CatalogTagFacet {
            id: row.get("id"),
            namespace: row.get("namespace"),
            key: row.get("key"),
            label: row.get("label"),
            translated_label: row.get("translated_label"),
            translated_namespace: row.get("translated_namespace"),
            context_count: row.get("context_count"),
        })
        .collect::<Vec<_>>();
    let has_more = items.len() as i64 > limit;
    items.truncate(limit as usize);
    let next_cursor = if has_more {
        items
            .last()
            .map(|item| {
                encode_cursor(&FacetCursor {
                    context_count: item.context_count,
                    tag_id: item.id,
                    query_hash: query_hash.to_string(),
                    catalog_revision: Some(revision),
                })
            })
            .transpose()?
    } else {
        None
    };
    transaction.commit().await?;
    Ok(Some(CatalogFacetsResponse {
        items,
        next_cursor,
        catalog_revision: bitmap_counts.catalog_revision,
    }))
}

async fn tag_kind_counts_ready(db: &Db) -> Result<bool> {
    Ok(
        sqlx::query_scalar::<_, i64>("SELECT ready FROM tag_kind_count_state WHERE singleton = 1")
            .fetch_one(db.pool())
            .await?
            != 0,
    )
}

pub async fn tag_kind_count_status_in(
    connection: &mut SqliteConnection,
) -> Result<TagKindCountStatus> {
    let row = sqlx::query(
        r#"
        SELECT state.ready, state.catalog_revision, state.updated_at, state.last_error,
               (SELECT COUNT(*) FROM tag_kind_counts) AS rows,
               (SELECT COALESCE(SUM(work_count), 0) FROM tag_kind_counts) AS associations
        FROM tag_kind_count_state AS state
        WHERE state.singleton = 1
        "#,
    )
    .fetch_one(&mut *connection)
    .await?;
    Ok(TagKindCountStatus {
        ready: row.get::<i64, _>("ready") != 0,
        catalog_revision: row.get("catalog_revision"),
        rows: row.get("rows"),
        associations: row.get("associations"),
        updated_at: row.get("updated_at"),
        last_error: row.get("last_error"),
    })
}

async fn try_query_preaggregated_tag_facets(
    db: &Db,
    kind: Option<&str>,
    selected_tags_json: Option<&str>,
    tag_query: &str,
    cursor: Option<&FacetCursor>,
    limit: i64,
    query_hash: &str,
) -> Result<Option<CatalogFacetsResponse>> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let revision =
        sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
            .fetch_one(&mut *transaction)
            .await?;
    if cursor
        .and_then(|cursor| cursor.catalog_revision)
        .is_some_and(|cursor_revision| cursor_revision != revision)
    {
        transaction.rollback().await?;
        return Err(AppError::BadRequest(
            "facet cursor expired after the catalog changed".to_string(),
        ));
    }
    let ready =
        sqlx::query_scalar::<_, i64>("SELECT ready FROM tag_kind_count_state WHERE singleton = 1")
            .fetch_one(&mut *transaction)
            .await?
            != 0;
    if !ready {
        transaction.rollback().await?;
        return Ok(None);
    }
    if let Some(selected_tags_json) = selected_tags_json {
        // A selected tag that covers the complete active kind/global scope is
        // an exact no-op. Validate that fact in the same snapshot used to read
        // tag_kind_counts, then reuse the preaggregated facet page. If the
        // count state is dirty, a tag is missing, or any tag is selective, the
        // caller falls back to the fact-table query.
        let universal = sqlx::query_scalar::<_, i64>(
            r#"
            WITH selected(namespace, key) AS (
                SELECT
                    json_extract(value, '$[0]'),
                    json_extract(value, '$[1]')
                FROM json_each(?1)
            ),
            scope_total(work_count) AS (
                SELECT COUNT(*) FROM works
                WHERE deleted_at IS NULL
                  AND (?2 IS NULL OR kind = ?2)
            ),
            scope_tag_counts(tag_id, work_count) AS (
                SELECT tag_id, SUM(work_count)
                FROM tag_kind_counts
                WHERE ?2 IS NULL OR kind = ?2
                GROUP BY tag_id
            )
            SELECT CASE
                WHEN COUNT(*) = json_array_length(?1)
                 AND COUNT(tag.id) = json_array_length(?1)
                 AND MIN(COALESCE(scope.work_count, -1)) = MAX(total.work_count)
                THEN 1 ELSE 0
            END
            FROM selected
            LEFT JOIN tags AS tag
              ON tag.namespace = selected.namespace AND tag.key = selected.key
            LEFT JOIN scope_tag_counts AS scope ON scope.tag_id = tag.id
            CROSS JOIN scope_total AS total
            "#,
        )
        .bind(selected_tags_json)
        .bind(kind)
        .fetch_one(&mut *transaction)
        .await?
            != 0;
        if !universal {
            transaction.rollback().await?;
            return Ok(None);
        }
    }
    let mut sql = String::from(
        r#"
        SELECT tag.id, tag.namespace, tag.key, tag.label,
               tag.translated_label, tag.translated_namespace,
               SUM(counts.work_count) AS context_count
        FROM tag_kind_counts AS counts
        JOIN tags AS tag ON tag.id = counts.tag_id
        WHERE 1 = 1
        "#,
    );
    if kind.is_some() {
        sql.push_str(" AND counts.kind = ?");
    }
    if !tag_query.is_empty() {
        sql.push_str(
            r#" AND (
                tag.key LIKE ? ESCAPE '\'
                OR tag.label LIKE ? ESCAPE '\'
                OR COALESCE(tag.translated_label, '') LIKE ? ESCAPE '\'
            )"#,
        );
    }
    sql.push_str(
        " GROUP BY tag.id, tag.namespace, tag.key, tag.label, tag.translated_label, tag.translated_namespace",
    );
    if cursor.is_some() {
        sql.push_str(
            " HAVING SUM(counts.work_count) < ? OR (SUM(counts.work_count) = ? AND tag.id > ?)",
        );
    }
    sql.push_str(" ORDER BY context_count DESC, tag.id ASC LIMIT ?");

    let mut statement = sqlx::query(&sql);
    if let Some(kind) = kind {
        statement = statement.bind(kind);
    }
    if !tag_query.is_empty() {
        let pattern = contains_pattern(tag_query);
        statement = statement
            .bind(pattern.clone())
            .bind(pattern.clone())
            .bind(pattern);
    }
    if let Some(cursor) = cursor {
        statement = statement
            .bind(cursor.context_count)
            .bind(cursor.context_count)
            .bind(cursor.tag_id);
    }
    let rows = statement
        .bind(limit + 1)
        .fetch_all(&mut *transaction)
        .await?;
    let mut items = rows
        .into_iter()
        .map(|row| CatalogTagFacet {
            id: row.get("id"),
            namespace: row.get("namespace"),
            key: row.get("key"),
            label: row.get("label"),
            translated_label: row.get("translated_label"),
            translated_namespace: row.get("translated_namespace"),
            context_count: row.get("context_count"),
        })
        .collect::<Vec<_>>();
    let has_more = items.len() as i64 > limit;
    items.truncate(limit as usize);
    let next_cursor = if has_more {
        items
            .last()
            .map(|item| {
                encode_cursor(&FacetCursor {
                    context_count: item.context_count,
                    tag_id: item.id,
                    query_hash: query_hash.to_string(),
                    catalog_revision: Some(revision),
                })
            })
            .transpose()?
    } else {
        None
    };
    transaction.commit().await?;
    Ok(Some(CatalogFacetsResponse {
        items,
        next_cursor,
        catalog_revision: revision,
    }))
}

async fn query_counts_with_candidates(
    db: &Db,
    query: CatalogCountsQuery,
    search_candidates: Option<&str>,
    expected_catalog_revision: Option<i64>,
    expected_search_revision: Option<i64>,
) -> Result<CatalogCountsResponse> {
    let tags = selected_tags(query.include_tag.as_deref())?;
    let q = bounded_query(query.q.as_deref())?;
    let tags_json = selected_tags_json(&tags)?;
    let mut sql = String::new();
    if !tags.is_empty() {
        sql.push_str(
            r#"
            WITH selected_tags(tag_id) AS (
                SELECT tag.id FROM json_each(?) AS input
                JOIN tags AS tag
                  ON tag.namespace = json_extract(input.value, '$[0]')
                 AND tag.key = json_extract(input.value, '$[1]')
            ),
            matched(work_id) AS (
                SELECT work_tag.work_id FROM work_tags AS work_tag
                JOIN selected_tags AS selected ON selected.tag_id = work_tag.tag_id
                GROUP BY work_tag.work_id
                HAVING COUNT(DISTINCT work_tag.tag_id) = json_array_length(?)
            )
            "#,
        );
    }
    sql.push_str(
        " SELECT work.kind, COUNT(*) AS count FROM works AS work WHERE work.deleted_at IS NULL",
    );
    if !tags.is_empty() {
        sql.push_str(" AND work.id IN (SELECT work_id FROM matched)");
    }
    if search_candidates.is_some() {
        sql.push_str(" AND work.id IN (SELECT CAST(value AS INTEGER) FROM json_each(?))");
    } else if !q.is_empty() {
        sql.push_str(
            r#" AND (
                work.title LIKE ? ESCAPE '\'
                OR COALESCE(work.subtitle, '') LIKE ? ESCAPE '\'
            )"#,
        );
    }
    sql.push_str(" GROUP BY work.kind ORDER BY work.kind");
    let mut statement = sqlx::query(&sql);
    if !tags.is_empty() {
        statement = statement.bind(&tags_json).bind(&tags_json);
    }
    if let Some(search_candidates) = search_candidates {
        statement = statement.bind(search_candidates);
    } else if !q.is_empty() {
        let pattern = contains_pattern(&q);
        statement = statement.bind(pattern.clone()).bind(pattern);
    }
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let revisions = db
        .revision_snapshot_with_connection(&mut transaction)
        .await?;
    if expected_catalog_revision.is_some_and(|revision| revision != revisions.catalog_revision)
        || expected_search_revision.is_some_and(|revision| revision != revisions.search_revision)
    {
        transaction.rollback().await?;
        return Err(search_candidates_expired_error());
    }
    let rows = statement.fetch_all(&mut *transaction).await?;
    let mut kinds = BTreeMap::new();
    let mut total = 0_i64;
    for row in rows {
        let count: i64 = row.get("count");
        total = total.saturating_add(count);
        kinds.insert(row.get("kind"), count);
    }
    let history_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM reading_history")
        .fetch_one(&mut *transaction)
        .await?;
    kinds.insert("history".to_string(), history_count);
    transaction.commit().await?;
    Ok(CatalogCountsResponse {
        kinds,
        total,
        catalog_revision: revisions.catalog_revision,
        activity_revision: revisions.activity_revision,
    })
}

#[cfg(test)]
async fn query_collections(
    db: &Db,
    query: CatalogCollectionsQuery,
) -> Result<CatalogCollectionsResponse> {
    query_collections_with_candidates(db, query, None, None, None).await
}

async fn query_collections_with_candidates(
    db: &Db,
    query: CatalogCollectionsQuery,
    search_candidates: Option<&str>,
    expected_catalog_revision: Option<i64>,
    expected_search_revision: Option<i64>,
) -> Result<CatalogCollectionsResponse> {
    let filter = CatalogFilter::from_collections_query(&query)?;
    let query_hash = filter.hash("collections", &search_candidate_scope(search_candidates))?;
    let cursor = query
        .cursor
        .as_deref()
        .map(decode_cursor::<CollectionCursor>)
        .transpose()?;
    if cursor
        .as_ref()
        .is_some_and(|cursor| cursor.query_hash != query_hash)
    {
        return Err(AppError::BadRequest(
            "collection cursor does not match the active filters".to_string(),
        ));
    }
    let limit = query
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let tags_json = selected_tags_json(&filter.tags)?;
    let mut sql = String::new();
    if !filter.tags.is_empty() {
        sql.push_str(
            r#"
            WITH selected_tags(tag_id) AS (
                SELECT tag.id FROM json_each(?) AS input
                JOIN tags AS tag
                  ON tag.namespace = json_extract(input.value, '$[0]')
                 AND tag.key = json_extract(input.value, '$[1]')
            ),
            matched(work_id) AS (
                SELECT work_tag.work_id
                FROM work_tags AS work_tag
                JOIN selected_tags AS selected ON selected.tag_id = work_tag.tag_id
                GROUP BY work_tag.work_id
                HAVING COUNT(DISTINCT work_tag.tag_id) = json_array_length(?)
            ),
            "#,
        );
    } else {
        sql.push_str("WITH ");
    }
    sql.push_str(
        r#"
        candidates AS (
            SELECT
                work.id, work.kind, work.title, work.cover_asset_id,
                work.progress, work.updated_at,
                stats.collection_key, stats.collection_title,
                stats.tag_count, stats.page_count
            FROM works AS work
            JOIN work_stats AS stats ON stats.work_id = work.id
            WHERE stats.collection_key IS NOT NULL
              AND work.deleted_at IS NULL
        "#,
    );
    if filter.kind.is_some() {
        sql.push_str(" AND work.kind = ?");
    }
    if !filter.tags.is_empty() {
        sql.push_str(" AND work.id IN (SELECT work_id FROM matched)");
    }
    if search_candidates.is_some() {
        sql.push_str(" AND work.id IN (SELECT CAST(value AS INTEGER) FROM json_each(?))");
    } else if !filter.q.is_empty() {
        sql.push_str(
            r#" AND (
                work.title LIKE ? ESCAPE '\'
                OR COALESCE(work.subtitle, '') LIKE ? ESCAPE '\'
            )"#,
        );
    }
    sql.push_str(
        r#"
        ),
        grouped AS (
            SELECT
                kind,
                collection_key,
                MIN(collection_title) AS collection_title,
                COUNT(*) AS work_count,
                AVG(progress) AS progress,
                SUM(tag_count) AS tag_count,
                SUM(page_count) AS page_count,
                MAX(updated_at) AS updated_at
            FROM candidates
            GROUP BY kind, collection_key
        ),
        summaries AS (
            SELECT
                grouped.*,
                (
                    SELECT candidate.id FROM candidates AS candidate
                    WHERE candidate.collection_key = grouped.collection_key
                    ORDER BY candidate.title COLLATE NOCASE, candidate.id
                    LIMIT 1
                ) AS first_work_id,
                (
                    SELECT candidate.cover_asset_id FROM candidates AS candidate
                    WHERE candidate.collection_key = grouped.collection_key
                    ORDER BY candidate.cover_asset_id IS NULL,
                             candidate.updated_at DESC, candidate.id DESC
                    LIMIT 1
                ) AS cover_asset_id
            FROM grouped
        )
        SELECT summaries.*, single_work.title AS single_title, single_work.subtitle AS single_subtitle
        FROM summaries JOIN works AS single_work ON single_work.id = summaries.first_work_id
        WHERE 1 = 1
        "#,
    );
    if cursor.is_some() {
        sql.push_str(" AND (summaries.updated_at < ? OR (summaries.updated_at = ? AND summaries.collection_key > ?))");
    }
    sql.push_str(" ORDER BY summaries.updated_at DESC, summaries.collection_key ASC LIMIT ?");

    let mut statement = sqlx::query(&sql);
    if !filter.tags.is_empty() {
        statement = statement.bind(&tags_json).bind(&tags_json);
    }
    if let Some(kind) = &filter.kind {
        statement = statement.bind(kind);
    }
    if let Some(search_candidates) = search_candidates {
        statement = statement.bind(search_candidates);
    } else if !filter.q.is_empty() {
        let pattern = contains_pattern(&filter.q);
        statement = statement.bind(pattern.clone()).bind(pattern);
    }
    if let Some(cursor) = &cursor {
        statement = statement
            .bind(cursor.updated_at)
            .bind(cursor.updated_at)
            .bind(&cursor.collection_key);
    }
    statement = statement.bind(limit + 1);
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let revisions = db
        .revision_snapshot_with_connection(&mut transaction)
        .await?;
    if expected_catalog_revision.is_some_and(|revision| revision != revisions.catalog_revision)
        || expected_search_revision.is_some_and(|revision| revision != revisions.search_revision)
    {
        transaction.rollback().await?;
        return Err(search_candidates_expired_error());
    }
    if let Some(cursor) = cursor.as_ref() {
        if let Err(error) = validate_cursor_revisions(
            cursor.catalog_revision,
            cursor.activity_revision,
            revisions,
            "collection",
        ) {
            transaction.rollback().await?;
            return Err(error);
        }
    }
    let backfill_pending =
        catalog_backfill_pending_with_connection(&mut transaction, filter.kind.as_deref()).await?;
    let rows = statement.fetch_all(&mut *transaction).await?;
    let mut items = rows
        .into_iter()
        .map(|row| {
            let base_kind: String = row.get("kind");
            let work_count: i64 = row.get("work_count");
            let updated_at: DateTime<Utc> = row.get("updated_at");
            let title: Option<String> = row.get("collection_title");
            CatalogCollectionItem {
                id: row.get("first_work_id"),
                kind: if work_count > 1 {
                    format!("{base_kind}-collection")
                } else {
                    base_kind.clone()
                },
                title: if work_count == 1 {
                    row.get("single_title")
                } else {
                    title.unwrap_or_else(|| "未命名合集".to_string())
                },
                subtitle: if work_count == 1 {
                    row.get("single_subtitle")
                } else {
                    collection_subtitle(&base_kind, work_count)
                },
                cover_asset_id: row.get("cover_asset_id"),
                cover_version: updated_at.to_rfc3339(),
                progress: row.get("progress"),
                work_count,
                tag_count: row.get("tag_count"),
                page_count: row.get("page_count"),
                collection_key: row.get("collection_key"),
                first_work_id: row.get("first_work_id"),
                updated_at,
            }
        })
        .collect::<Vec<_>>();
    let has_more = items.len() as i64 > limit;
    items.truncate(limit as usize);
    let next_cursor = if has_more {
        items
            .last()
            .map(|item| {
                encode_cursor(&CollectionCursor {
                    updated_at: item.updated_at,
                    collection_key: item.collection_key.clone(),
                    query_hash: query_hash.clone(),
                    catalog_revision: Some(revisions.catalog_revision),
                    activity_revision: Some(revisions.activity_revision),
                })
            })
            .transpose()?
    } else {
        None
    };
    transaction.commit().await?;
    Ok(CatalogCollectionsResponse {
        items,
        next_cursor,
        catalog_revision: revisions.catalog_revision,
        activity_revision: revisions.activity_revision,
        backfill_pending,
    })
}

async fn query_history(db: &Db, query: CatalogHistoryQuery) -> Result<CatalogHistoryResponse> {
    let query_hash = "catalog-history-v1".to_string();
    let cursor = query
        .cursor
        .as_deref()
        .map(decode_cursor::<HistoryCursor>)
        .transpose()?;
    if cursor
        .as_ref()
        .is_some_and(|cursor| cursor.query_hash != query_hash)
    {
        return Err(AppError::BadRequest(
            "history cursor does not match the active query".to_string(),
        ));
    }
    let limit = query
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let mut sql = String::from(
        r#"
        SELECT
            work.id, work.kind, work.title, work.subtitle, work.cover_asset_id,
            work.progress, work.updated_at,
            COALESCE(stats.asset_count, 0) AS asset_count,
            COALESCE(stats.tag_count, 0) AS tag_count,
            COALESCE(stats.image_count, 0) AS image_count,
            COALESCE(stats.track_count, 0) AS track_count,
            COALESCE(stats.page_count, 0) AS page_count,
            stats.collection_key,
            history.position, history.last_opened_at
        FROM reading_history AS history
        JOIN works AS work ON work.id = history.work_id
        LEFT JOIN work_stats AS stats ON stats.work_id = work.id
        WHERE 1 = 1
        "#,
    );
    if cursor.is_some() {
        sql.push_str(
            " AND (history.last_opened_at < ? OR (history.last_opened_at = ? AND work.id < ?))",
        );
    }
    sql.push_str(" ORDER BY history.last_opened_at DESC, work.id DESC LIMIT ?");
    let mut statement = sqlx::query(&sql);
    if let Some(cursor) = &cursor {
        statement = statement
            .bind(cursor.last_opened_at)
            .bind(cursor.last_opened_at)
            .bind(cursor.work_id);
    }
    statement = statement.bind(limit + 1);
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let revisions = db
        .revision_snapshot_with_connection(&mut transaction)
        .await?;
    if let Some(cursor) = cursor.as_ref() {
        if let Err(error) = validate_cursor_revisions(
            cursor.catalog_revision,
            cursor.activity_revision,
            revisions,
            "history",
        ) {
            transaction.rollback().await?;
            return Err(error);
        }
    }
    let rows = statement.fetch_all(&mut *transaction).await?;
    let mut items = rows
        .into_iter()
        .map(|row| {
            let updated_at: DateTime<Utc> = row.get("updated_at");
            CatalogHistoryItem {
                work: CatalogWorkItem {
                    id: row.get("id"),
                    kind: row.get("kind"),
                    title: row.get("title"),
                    subtitle: row.get("subtitle"),
                    cover_asset_id: row.get("cover_asset_id"),
                    cover_version: updated_at.to_rfc3339(),
                    progress: row.get("progress"),
                    asset_count: row.get("asset_count"),
                    tag_count: row.get("tag_count"),
                    image_count: row.get("image_count"),
                    track_count: row.get("track_count"),
                    page_count: row.get("page_count"),
                    collection_key: row.get("collection_key"),
                    updated_at,
                },
                position: row.get("position"),
                last_opened_at: row.get("last_opened_at"),
            }
        })
        .collect::<Vec<_>>();
    let has_more = items.len() as i64 > limit;
    items.truncate(limit as usize);
    let next_cursor = if has_more {
        items
            .last()
            .map(|item| {
                encode_cursor(&HistoryCursor {
                    last_opened_at: item.last_opened_at,
                    work_id: item.work.id,
                    query_hash: query_hash.clone(),
                    catalog_revision: Some(revisions.catalog_revision),
                    activity_revision: Some(revisions.activity_revision),
                })
            })
            .transpose()?
    } else {
        None
    };
    transaction.commit().await?;
    Ok(CatalogHistoryResponse {
        items,
        next_cursor,
        catalog_revision: revisions.catalog_revision,
        activity_revision: revisions.activity_revision,
    })
}

async fn query_assets(
    db: &Db,
    work_id: i64,
    query: CatalogAssetsQuery,
) -> Result<CatalogAssetsResponse> {
    if work_id <= 0 {
        return Err(AppError::BadRequest("invalid work id".to_string()));
    }
    let role = bounded_optional(query.role.as_deref(), "asset role")?;
    // Treat the public `track` filter as the canonical playable-audio
    // predicate.  Older imports may have an audio MIME type without the
    // normalized role, and those records must still be reachable by the
    // bounded audio detail/queue path.
    let playable_track_filter = role.as_deref() == Some("track");
    let mut hasher = Sha256::new();
    hasher.update(work_id.to_le_bytes());
    hasher.update([0]);
    hasher.update(role.as_deref().unwrap_or("").as_bytes());
    let query_hash = format!("{:x}", hasher.finalize());
    let cursor = query
        .cursor
        .as_deref()
        .map(decode_cursor::<AssetCursor>)
        .transpose()?;
    if cursor
        .as_ref()
        .is_some_and(|cursor| cursor.query_hash != query_hash)
    {
        return Err(AppError::BadRequest(
            "asset cursor does not match the active query".to_string(),
        ));
    }
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let source_version = match sqlx::query_scalar::<_, DateTime<Utc>>(
        "SELECT updated_at FROM works WHERE id = ? AND deleted_at IS NULL",
    )
    .bind(work_id)
    .fetch_optional(&mut *transaction)
    .await?
    {
        Some(source_version) => source_version,
        None => {
            transaction.rollback().await?;
            return Err(AppError::NotFound(format!("work {work_id} not found")));
        }
    };
    let catalog_revision =
        sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
            .fetch_one(&mut *transaction)
            .await?;
    let source_version = source_version.to_rfc3339();
    if let Some(cursor) = cursor.as_ref() {
        if cursor
            .source_version
            .as_deref()
            .is_some_and(|version| version != source_version)
        {
            transaction.rollback().await?;
            return Err(AppError::BadRequest(
                "asset cursor expired after the work changed".to_string(),
            ));
        }
        if cursor
            .catalog_revision
            .is_some_and(|revision| revision != catalog_revision)
        {
            transaction.rollback().await?;
            return Err(AppError::BadRequest(
                "asset cursor expired after the catalog changed".to_string(),
            ));
        }
    }
    let total = if playable_track_filter {
        // `work_stats.track_count` is trigger-maintained and avoids a COUNT
        // over the complete asset table for every queue page. Retain an exact
        // fallback while a legacy database is backfilled, but run both
        // branches in the same snapshot as the asset page.
        maintained_asset_count_with_connection(
            work_id,
            "track_count",
            "SELECT
                (SELECT COUNT(*) FROM assets WHERE work_id = ?1 AND role = 'track')
                +
                (SELECT COUNT(*) FROM assets
                 WHERE work_id = ?1
                   AND role <> 'track'
                   AND lower(mime) LIKE 'audio/%')",
            &mut transaction,
        )
        .await?
    } else if let Some(role) = &role {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM assets WHERE work_id = ? AND role = ?")
            .bind(work_id)
            .bind(role)
            .fetch_one(&mut *transaction)
            .await?
    } else {
        // Unfiltered Catalog asset pages are used by older readers and admin
        // detail views. On a 10k-chapter EPUB or a large gallery, issuing the
        // exact COUNT on every cursor page is needless SQLite work: the same
        // trigger-maintained `asset_count` already backs Catalog work cards.
        // Do not trust the migration placeholder while stats are pending.
        maintained_asset_count_with_connection(
            work_id,
            "asset_count",
            "SELECT COUNT(*) FROM assets WHERE work_id = ?1",
            &mut transaction,
        )
        .await?
    };
    let limit = query.limit.unwrap_or(100).clamp(1, MAX_PAGE_SIZE);
    let mut items = if playable_track_filter {
        // Keep the legacy MIME fallback, but split it into two sargable,
        // independently ordered branches.  Each branch is limited before
        // leaving SQLite; merging the two short vectors here avoids the
        // UNION ALL materialization and TEMP B-TREE sort that otherwise scans
        // every track on every continuation request.
        let mut merged = Vec::with_capacity((limit as usize + 1) * 2);
        for branch in [
            "role = 'track'",
            "role <> 'track' AND lower(mime) LIKE 'audio/%'",
        ] {
            let mut sql =
                "SELECT id, work_id, path, mime, role, variant, position, size, meta_json, created_at\n"
                    .to_owned();
            sql.push_str("FROM assets WHERE work_id = ? AND ");
            sql.push_str(branch);
            if cursor.is_some() {
                sql.push_str(
                    r#" AND (role, COALESCE(position, 9223372036854775807), id) > (?, ?, ?)"#,
                );
            }
            sql.push_str(
                " ORDER BY role ASC, COALESCE(position, 9223372036854775807) ASC, id ASC LIMIT ?",
            );
            let mut statement = sqlx::query(&sql).bind(work_id);
            if let Some(cursor) = &cursor {
                statement = statement
                    .bind(&cursor.role)
                    .bind(cursor.position)
                    .bind(cursor.id);
            }
            let rows = statement
                .bind(limit + 1)
                .fetch_all(&mut *transaction)
                .await?;
            merged.extend(rows.into_iter().map(catalog_asset_item_from_row));
        }
        merged.sort_unstable_by(|left, right| {
            catalog_asset_order(left).cmp(&catalog_asset_order(right))
        });
        merged
    } else {
        let mut sql = String::from(
            r#"
            SELECT id, work_id, path, mime, role, variant, position, size, meta_json, created_at
            FROM assets
            WHERE work_id = ?
            "#,
        );
        if role.is_some() {
            sql.push_str(" AND role = ?");
        }
        if cursor.is_some() {
            sql.push_str(r#" AND (role, COALESCE(position, 9223372036854775807), id) > (?, ?, ?)"#);
        }
        sql.push_str(
            " ORDER BY role ASC, COALESCE(position, 9223372036854775807) ASC, id ASC LIMIT ?",
        );
        let mut statement = sqlx::query(&sql).bind(work_id);
        if let Some(role) = &role {
            statement = statement.bind(role);
        }
        if let Some(cursor) = &cursor {
            statement = statement
                .bind(&cursor.role)
                .bind(cursor.position)
                .bind(cursor.id);
        }
        statement
            .bind(limit + 1)
            .fetch_all(&mut *transaction)
            .await?
            .into_iter()
            .map(catalog_asset_item_from_row)
            .collect::<Vec<_>>()
    };
    let has_more = items.len() as i64 > limit;
    items.truncate(limit as usize);
    let next_cursor = if has_more {
        items
            .last()
            .map(|item| {
                encode_cursor(&AssetCursor {
                    role: item.role.clone(),
                    position: item.position.unwrap_or(i64::MAX),
                    id: item.id,
                    query_hash: query_hash.clone(),
                    source_version: Some(source_version.clone()),
                    catalog_revision: Some(catalog_revision),
                })
            })
            .transpose()?
    } else {
        None
    };
    transaction.commit().await?;
    Ok(CatalogAssetsResponse {
        items,
        next_cursor,
        total,
        source_version,
    })
}

/// Read a trigger-maintained work_stats counter when the row is known to be
/// valid, otherwise fall back to the exact fact-table count. The column name
/// is deliberately restricted to the two internal counters used by this
/// route; callers cannot inject SQL through this helper.
async fn maintained_asset_count_with_connection(
    work_id: i64,
    stat_column: &str,
    fallback_sql: &str,
    connection: &mut SqliteConnection,
) -> Result<i64> {
    let maintained_sql = match stat_column {
        "asset_count" => {
            "SELECT stats.asset_count
             FROM work_stats AS stats
             JOIN works ON works.id = stats.work_id
             WHERE stats.work_id = ?1
               AND works.deleted_at IS NULL
               AND stats.computed_at IS NOT NULL"
        }
        "track_count" => {
            "SELECT stats.track_count
             FROM work_stats AS stats
             JOIN works ON works.id = stats.work_id
             WHERE stats.work_id = ?1
               AND works.deleted_at IS NULL
               AND stats.computed_at IS NOT NULL"
        }
        _ => {
            return Err(AppError::Other(
                "unsupported maintained asset counter".to_string(),
            ))
        }
    };
    if let Some(count) = sqlx::query_scalar::<_, i64>(maintained_sql)
        .bind(work_id)
        .fetch_optional(&mut *connection)
        .await?
    {
        return Ok(count);
    }
    Ok(sqlx::query_scalar::<_, i64>(fallback_sql)
        .bind(work_id)
        .fetch_one(&mut *connection)
        .await?)
}

impl CatalogFilter {
    fn from_works_query(query: &CatalogWorksQuery) -> Result<Self> {
        let q = bounded_query(query.q.as_deref())?;
        let default_sort = if q.is_empty() {
            "updated_desc"
        } else {
            "relevance"
        };
        let sort = match query.sort.as_deref().unwrap_or(default_sort) {
            "updated_desc" | "updated" => CatalogSort::UpdatedDesc,
            "relevance" | "rank" => CatalogSort::Relevance,
            other => {
                return Err(AppError::BadRequest(format!(
                    "unsupported catalog sort {other:?}"
                )))
            }
        };
        Ok(Self {
            kind: normalized_kind(query.kind.as_deref()),
            collection: bounded_optional(query.collection.as_deref(), "catalog collection")?,
            tags: selected_tags(query.include_tag.as_deref())?,
            q,
            sort,
        })
    }

    fn from_facet_query(query: &CatalogFacetsQuery) -> Result<Self> {
        Ok(Self {
            kind: normalized_kind(query.kind.as_deref()),
            collection: bounded_optional(query.collection.as_deref(), "catalog collection")?,
            tags: selected_tags(query.include_tag.as_deref())?,
            q: bounded_query(query.q.as_deref())?,
            sort: CatalogSort::UpdatedDesc,
        })
    }

    fn from_random_query(query: &CatalogRandomQuery) -> Result<Self> {
        Ok(Self {
            kind: normalized_kind(query.kind.as_deref()),
            collection: bounded_optional(query.collection.as_deref(), "catalog collection")?,
            tags: selected_tags(query.include_tag.as_deref())?,
            q: bounded_query(query.q.as_deref())?,
            sort: CatalogSort::UpdatedDesc,
        })
    }

    fn from_collections_query(query: &CatalogCollectionsQuery) -> Result<Self> {
        Ok(Self {
            kind: normalized_kind(query.kind.as_deref()),
            collection: None,
            tags: selected_tags(query.include_tag.as_deref())?,
            q: bounded_query(query.q.as_deref())?,
            sort: CatalogSort::UpdatedDesc,
        })
    }

    fn hash(&self, scope: &str, extra: &str) -> Result<String> {
        let mut hasher = Sha256::new();
        for part in [
            scope,
            self.kind.as_deref().unwrap_or(""),
            self.collection.as_deref().unwrap_or(""),
            self.q.as_str(),
            match self.sort {
                CatalogSort::UpdatedDesc => "updated_desc",
                CatalogSort::Relevance => "relevance",
            },
            extra,
        ] {
            hasher.update((part.len() as u64).to_le_bytes());
            hasher.update(part.as_bytes());
        }
        for tag in &self.tags {
            hasher.update((tag.len() as u64).to_le_bytes());
            hasher.update(tag.as_bytes());
        }
        Ok(format!("{:x}", hasher.finalize()))
    }
}

fn normalized_kind(kind: Option<&str>) -> Option<String> {
    kind.map(str::trim)
        .filter(|value| !value.is_empty() && *value != "all")
        .map(ToOwned::to_owned)
}

fn selected_tags(value: Option<&str>) -> Result<Vec<String>> {
    let mut tags = value
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|tag| !tag.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    tags.sort();
    tags.dedup();
    if tags.len() > MAX_SELECTED_TAGS {
        return Err(AppError::BadRequest(format!(
            "at most {MAX_SELECTED_TAGS} include tags are supported"
        )));
    }
    if tags.iter().any(|tag| tag.len() > MAX_QUERY_BYTES) {
        return Err(AppError::BadRequest(
            "catalog tag filter is too long".to_string(),
        ));
    }
    if tags.iter().any(|tag| {
        tag.split_once(':')
            .is_none_or(|(namespace, key)| namespace.is_empty() || key.is_empty())
    }) {
        return Err(AppError::BadRequest(
            "catalog tags must use namespace:key form".to_string(),
        ));
    }
    Ok(tags)
}

fn selected_tags_json(tags: &[String]) -> Result<String> {
    let parts = tags
        .iter()
        .filter_map(|tag| tag.split_once(':'))
        .map(|(namespace, key)| [namespace, key])
        .collect::<Vec<_>>();
    serde_json::to_string(&parts).map_err(|err| AppError::Other(err.to_string()))
}

fn bounded_query(value: Option<&str>) -> Result<String> {
    let value = value.unwrap_or_default().trim();
    if value.len() > MAX_QUERY_BYTES {
        return Err(AppError::BadRequest(format!(
            "catalog query exceeds {MAX_QUERY_BYTES} bytes"
        )));
    }
    Ok(value.to_string())
}

fn bounded_optional(value: Option<&str>, label: &str) -> Result<Option<String>> {
    let value = value.unwrap_or_default().trim();
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() > MAX_QUERY_BYTES {
        return Err(AppError::BadRequest(format!(
            "{label} exceeds {MAX_QUERY_BYTES} bytes"
        )));
    }
    Ok(Some(value.to_string()))
}

async fn catalog_search_candidates(
    state: &Arc<AppState>,
    value: Option<&str>,
) -> Result<Option<CatalogSearchCandidates>> {
    let query = bounded_query(value)?;
    if query.is_empty() {
        return Ok(None);
    }
    let candidates =
        search::catalog_candidate_ids(state.clone(), query, CATALOG_SEARCH_CANDIDATE_LIMIT).await?;
    let encoded_ids =
        serde_json::to_string(&candidates.ids).map_err(|err| AppError::Other(err.to_string()))?;
    Ok(Some(CatalogSearchCandidates {
        encoded_ids,
        catalog_revision: candidates.catalog_revision,
        search_revision: candidates.search_revision,
    }))
}

fn search_candidates_expired_error() -> AppError {
    AppError::Overloaded {
        message: "search candidates expired while the catalog changed".to_string(),
        retry_after_seconds: 1,
    }
}

fn facet_cache_key(
    catalog_revision: i64,
    query: &CatalogFacetsQuery,
    search_candidates: Option<&str>,
) -> Result<FacetCacheKey> {
    let filter = CatalogFilter::from_facet_query(query)?;
    let tag_query = bounded_query(query.tag_q.as_deref())?;
    let limit = query.limit.unwrap_or(100).clamp(1, MAX_PAGE_SIZE);
    let extra = format!(
        "{tag_query}\0{}\0{}\0{limit}",
        search_candidate_scope(search_candidates),
        query.cursor.as_deref().unwrap_or_default()
    );
    Ok(FacetCacheKey {
        catalog_revision,
        request_hash: filter.hash("facets-cache", &extra)?,
    })
}

fn search_candidate_scope(search_candidates: Option<&str>) -> String {
    let Some(search_candidates) = search_candidates else {
        return String::new();
    };
    let mut hasher = Sha256::new();
    hasher.update(search_candidates.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn contains_pattern(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    format!("%{escaped}%")
}

fn collection_subtitle(kind: &str, count: i64) -> String {
    let unit = match kind {
        "novel" => "卷",
        "coser-picture" => "套",
        _ => "本",
    };
    format!("{count}{unit}")
}

fn asset_display_name(path: &str) -> String {
    let normalized = path.replace('\\', "/");
    FsPath::new(&normalized)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("media")
        .to_string()
}

async fn catalog_backfill_pending_with_connection(
    connection: &mut SqliteConnection,
    kind: Option<&str>,
) -> Result<bool> {
    let pending = if let Some(kind) = kind {
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM work_stats AS stats
                JOIN works AS work ON work.id = stats.work_id
                WHERE stats.computed_at IS NULL
                  AND work.deleted_at IS NULL
                  AND work.kind = ?
                LIMIT 1
            )
            "#,
        )
        .bind(kind)
        .fetch_one(&mut *connection)
        .await?
    } else {
        sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                SELECT 1
                FROM work_stats AS stats
                JOIN works AS work ON work.id = stats.work_id
                WHERE stats.computed_at IS NULL AND work.deleted_at IS NULL
                LIMIT 1
            )",
        )
        .fetch_one(&mut *connection)
        .await?
    };
    Ok(pending != 0)
}

fn encode_cursor<T: Serialize>(cursor: &T) -> Result<String> {
    let bytes = serde_json::to_vec(cursor).map_err(|err| AppError::Other(err.to_string()))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_cursor<T>(cursor: &str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| AppError::BadRequest("invalid catalog cursor".to_string()))?;
    serde_json::from_slice(&bytes)
        .map_err(|_| AppError::BadRequest("invalid catalog cursor".to_string()))
}

#[cfg(test)]
async fn catalog_revision(db: &Db) -> Result<i64> {
    Ok(
        sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
            .fetch_one(db.pool())
            .await?,
    )
}

// Backfill a bounded work set with two grouped fact-table passes. The earlier
// shape issued five correlated COUNT/SUM subqueries per work, so a single
// 256-work batch repeatedly revisited the same asset rows. Keep selection in
// JSON to preserve the bounded caller contract, but aggregate assets and tags
// once each before updating the selected stats rows.
const STATS_BACKFILL_AGGREGATE_SQL: &str = r#"
    WITH selected(work_id) AS MATERIALIZED (
        SELECT DISTINCT CAST(value AS INTEGER)
        FROM json_each(?1)
    ),
    asset_counts AS MATERIALIZED (
        SELECT
            asset.work_id,
            COUNT(*) AS asset_count,
            SUM(CASE WHEN asset.mime LIKE 'image/%' THEN 1 ELSE 0 END) AS image_count,
            SUM(CASE
                WHEN asset.role = 'track' OR asset.mime LIKE 'audio/%' THEN 1
                ELSE 0
            END) AS track_count,
            SUM(CASE
                WHEN asset.role = 'page' THEN 1
                WHEN asset.role = 'archive' THEN CAST(
                    COALESCE(json_extract(asset.meta_json, '$.page_count'), 0) AS INTEGER
                )
                ELSE 0
            END) AS page_count
        FROM assets AS asset
        JOIN selected ON selected.work_id = asset.work_id
        GROUP BY asset.work_id
    ),
    tag_counts AS MATERIALIZED (
        SELECT work_tag.work_id, COUNT(*) AS tag_count
        FROM work_tags AS work_tag
        JOIN selected ON selected.work_id = work_tag.work_id
        GROUP BY work_tag.work_id
    )
    UPDATE work_stats
    SET
        asset_count = COALESCE((
            SELECT asset_count FROM asset_counts
            WHERE asset_counts.work_id = work_stats.work_id
        ), 0),
        tag_count = COALESCE((
            SELECT tag_count FROM tag_counts
            WHERE tag_counts.work_id = work_stats.work_id
        ), 0),
        image_count = COALESCE((
            SELECT image_count FROM asset_counts
            WHERE asset_counts.work_id = work_stats.work_id
        ), 0),
        track_count = COALESCE((
            SELECT track_count FROM asset_counts
            WHERE asset_counts.work_id = work_stats.work_id
        ), 0),
        page_count = COALESCE((
            SELECT page_count FROM asset_counts
            WHERE asset_counts.work_id = work_stats.work_id
        ), 0),
        catalog_revision = (SELECT revision FROM catalog_state WHERE singleton = 1),
        computed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    WHERE work_id IN (SELECT work_id FROM selected)
"#;

#[derive(Debug)]
struct CollectionSource {
    id: i64,
    kind: String,
    title: String,
    source_path: Option<String>,
    meta_json: String,
    artist_tag: Option<String>,
}

fn derive_collection(source: &CollectionSource) -> (String, String) {
    let meta = serde_json::from_str::<serde_json::Value>(&source.meta_json)
        .unwrap_or(serde_json::Value::Null);
    match source.kind.as_str() {
        "novel" => source
            .source_path
            .as_deref()
            .and_then(parent_collection)
            .map(|(dimension, seed, title)| {
                (hashed_collection_key(&source.kind, dimension, &seed), title)
            })
            .or_else(|| {
                meta_text(&meta, &["series"]).map(|title| {
                    let seed = normalize_collection_seed(&title);
                    (hashed_collection_key(&source.kind, "series", &seed), title)
                })
            })
            .unwrap_or_else(|| (format!("single:{}", source.id), source.title.clone())),
        "comic" => meta_text(&meta, &["artist", "penciller", "creator"])
            .or_else(|| normalized_display(source.artist_tag.as_deref()))
            .map(|title| {
                let seed = normalize_collection_seed(&title);
                (hashed_collection_key(&source.kind, "artist", &seed), title)
            })
            .unwrap_or_else(|| (format!("single:{}", source.id), source.title.clone())),
        "coser-picture" => meta_text(&meta, &["coser"])
            .or_else(|| {
                source
                    .source_path
                    .as_deref()
                    .and_then(parent_collection)
                    .map(|(_, _, title)| title)
            })
            .or_else(|| normalized_display(source.artist_tag.as_deref()))
            .map(|title| {
                let seed = normalize_collection_seed(&title);
                (hashed_collection_key(&source.kind, "coser", &seed), title)
            })
            .unwrap_or_else(|| (format!("single:{}", source.id), source.title.clone())),
        _ => (format!("single:{}", source.id), source.title.clone()),
    }
}

fn meta_text(meta: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        meta.get(*key)
            .and_then(serde_json::Value::as_str)
            .and_then(|value| normalized_display(Some(value)))
    })
}

fn normalized_display(value: Option<&str>) -> Option<String> {
    let value = value?.split_whitespace().collect::<Vec<_>>().join(" ");
    (!value.is_empty()).then_some(value)
}

fn parent_collection(path: &str) -> Option<(&'static str, String, String)> {
    let normalized = path.replace('\\', "/");
    let parent = normalized.trim_end_matches('/').rsplit_once('/')?.0;
    let title = parent.rsplit('/').next()?.trim();
    if title.is_empty() {
        return None;
    }
    Some((
        "folder",
        normalize_collection_seed(parent),
        title.to_string(),
    ))
}

fn normalize_collection_seed(value: &str) -> String {
    value
        .replace('\\', "/")
        .trim_end_matches('/')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn hashed_collection_key(kind: &str, dimension: &str, seed: &str) -> String {
    let mut hasher = Sha256::new();
    for part in [kind, dimension, seed] {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    format!("{kind}:{dimension}:{:x}", hasher.finalize())
}

async fn backfill_stats_for_ids(db: &Db, work_ids: &[i64]) -> Result<u64> {
    if work_ids.is_empty() {
        return Ok(0);
    }
    let ids_json =
        serde_json::to_string(work_ids).map_err(|err| AppError::Other(err.to_string()))?;
    let _write_slot = db.acquire_write_slot(256 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    let sources = sqlx::query(
        r#"
        SELECT
            work.id, work.kind, work.title, work.source_path, work.meta_json,
            (
                SELECT COALESCE(NULLIF(tag.label, ''), tag.key)
                FROM work_tags AS work_tag
                JOIN tags AS tag ON tag.id = work_tag.tag_id
                WHERE work_tag.work_id = work.id AND tag.namespace = 'artist'
                ORDER BY tag.id
                LIMIT 1
            ) AS artist_tag
        FROM works AS work
        WHERE work.deleted_at IS NULL
          AND work.id IN (SELECT CAST(value AS INTEGER) FROM json_each(?))
        ORDER BY work.id
        "#,
    )
    .bind(&ids_json)
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .map(|row| CollectionSource {
        id: row.get("id"),
        kind: row.get("kind"),
        title: row.get("title"),
        source_path: row.get("source_path"),
        meta_json: row.get("meta_json"),
        artist_tag: row.get("artist_tag"),
    })
    .collect::<Vec<_>>();
    let collection_rows = sources
        .iter()
        .map(|source| {
            let (key, title) = derive_collection(source);
            serde_json::json!([source.id, key, title])
        })
        .collect::<Vec<_>>();
    let collection_json =
        serde_json::to_string(&collection_rows).map_err(|err| AppError::Other(err.to_string()))?;
    sqlx::query(STATS_BACKFILL_AGGREGATE_SQL)
        .bind(&ids_json)
        .execute(&mut *transaction)
        .await?;
    sqlx::query(
        r#"
        CREATE TEMP TABLE IF NOT EXISTS temp_catalog_collection_backfill (
            work_id INTEGER PRIMARY KEY,
            collection_key TEXT NOT NULL,
            collection_title TEXT NOT NULL
        ) WITHOUT ROWID
        "#,
    )
    .execute(&mut *transaction)
    .await?;
    sqlx::query("DELETE FROM temp_catalog_collection_backfill")
        .execute(&mut *transaction)
        .await?;
    sqlx::query(
        r#"
        INSERT INTO temp_catalog_collection_backfill(work_id, collection_key, collection_title)
        SELECT
            CAST(json_extract(value, '$[0]') AS INTEGER),
            CAST(json_extract(value, '$[1]') AS TEXT),
            CAST(json_extract(value, '$[2]') AS TEXT)
        FROM json_each(?)
        "#,
    )
    .bind(collection_json)
    .execute(&mut *transaction)
    .await?;
    let affected = sqlx::query(
        r#"
        UPDATE work_stats
        SET collection_key = (
                SELECT collection_key FROM temp_catalog_collection_backfill AS input
                WHERE input.work_id = work_stats.work_id
            ),
            collection_title = (
                SELECT collection_title FROM temp_catalog_collection_backfill AS input
                WHERE input.work_id = work_stats.work_id
            ),
            computed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE work_id IN (SELECT work_id FROM temp_catalog_collection_backfill)
        "#,
    )
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    sqlx::query("DELETE FROM temp_catalog_collection_backfill")
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(affected)
}

async fn backfill_stats_batch(db: &Db, limit: i64) -> Result<u64> {
    let work_ids = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT work_id FROM work_stats
        WHERE computed_at IS NULL
        ORDER BY work_id
        LIMIT ?
        "#,
    )
    .bind(limit)
    .fetch_all(db.pool())
    .await?;
    backfill_stats_for_ids(db, &work_ids).await
}

async fn rebuild_tag_kind_counts(db: &Db) -> Result<u64> {
    let _write_slot = db.acquire_write_slot(8 * 1024 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    sqlx::query(
        r#"
        UPDATE catalog_state
        SET revision = revision + 1,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE singleton = 1
        "#,
    )
    .execute(&mut *transaction)
    .await?;
    sqlx::query("DELETE FROM tag_kind_counts")
        .execute(&mut *transaction)
        .await?;
    let inserted = sqlx::query(
        r#"
        INSERT INTO tag_kind_counts (
            kind, tag_id, work_count, catalog_revision
        )
        SELECT work.kind, work_tag.tag_id, COUNT(*), state.revision
        FROM work_tags AS work_tag
        JOIN works AS work ON work.id = work_tag.work_id
        CROSS JOIN catalog_state AS state
        WHERE state.singleton = 1
          AND work.deleted_at IS NULL
        GROUP BY work.kind, work_tag.tag_id
        "#,
    )
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    sqlx::query(
        r#"
        UPDATE tag_kind_count_state
        SET ready = 1,
            catalog_revision = (
                SELECT revision FROM catalog_state WHERE singleton = 1
            ),
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
            last_error = NULL
        WHERE singleton = 1
        "#,
    )
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(inserted)
}

async fn record_tag_kind_count_error(db: &Db, error: &str) {
    let bounded = error.chars().take(2048).collect::<String>();
    let update = async {
        let _write_slot = db.acquire_write_slot(64 * 1024).await?;
        sqlx::query(
            r#"
            UPDATE tag_kind_count_state
            SET last_error = ?1,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE singleton = 1
            "#,
        )
        .bind(bounded)
        .execute(db.pool())
        .await?;
        Ok::<(), AppError>(())
    }
    .await;
    if let Err(update_error) = update {
        tracing::warn!(error = %update_error, "failed to record tag facet backfill error");
    }
}

pub fn spawn_stats_backfill(db: Db, resources: ResourceGovernor) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_query_planner_optimize = None::<Instant>;
        let mut last_query_planner_revision = None::<i64>;
        loop {
            let scanner_active = match sqlx::query_scalar::<_, i64>(
                "SELECT EXISTS(SELECT 1 FROM scanner_locks WHERE name = 'library')",
            )
            .fetch_one(db.pool())
            .await
            {
                Ok(active) => active != 0,
                Err(err) => {
                    tracing::warn!(error = %err, "failed to inspect catalog maintenance state");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            if scanner_active {
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
            let lease = match resources
                .reserve_background(
                    ResourceClass::CatalogWriter,
                    STATS_BACKFILL_PROCESSING_BYTES,
                    0,
                )
                .await
            {
                Ok(lease) => lease,
                Err(err) => {
                    tracing::warn!(error = %err, "catalog maintenance could not reserve resources");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
            };
            let mut did_work = false;
            match tag_kind_counts_ready(&db).await {
                Ok(false) => match rebuild_tag_kind_counts(&db).await {
                    Ok(rows) => {
                        did_work = true;
                        tracing::info!(rows, "backfilled tag kind facet counts");
                    }
                    Err(err) => {
                        record_tag_kind_count_error(&db, &err.to_string()).await;
                        tracing::warn!(error = %err, "tag facet backfill failed");
                        drop(lease);
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        continue;
                    }
                },
                Ok(true) => {}
                Err(err) => {
                    tracing::warn!(error = %err, "failed to inspect tag facet backfill state");
                    drop(lease);
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
            }
            let result = backfill_stats_batch(&db, STATS_BACKFILL_BATCH).await;
            match result {
                Ok(0) if !did_work => {
                    let revision = sqlx::query_scalar::<_, i64>(
                        "SELECT revision FROM catalog_state WHERE singleton = 1",
                    )
                    .fetch_optional(db.pool())
                    .await
                    .ok()
                    .flatten();
                    let should_optimize = revision.is_some()
                        && revision != last_query_planner_revision
                        && last_query_planner_optimize.is_none_or(|last| {
                            last.elapsed() >= QUERY_PLANNER_MAINTENANCE_INTERVAL
                        });
                    if should_optimize {
                        match db.optimize_query_planner().await {
                            Ok(()) => {
                                last_query_planner_optimize = Some(Instant::now());
                                last_query_planner_revision = revision;
                                tracing::debug!(
                                    catalog_revision = revision,
                                    "optimized SQLite query planner statistics"
                                );
                            }
                            Err(err) => {
                                // Failed maintenance must obey the same
                                // backoff as a successful run; otherwise a
                                // read-only/locked NAS could log and retry
                                // every five-second idle poll forever.
                                last_query_planner_optimize = Some(Instant::now());
                                tracing::warn!(
                                    error = %err,
                                    catalog_revision = revision,
                                    "SQLite query planner optimization failed"
                                );
                            }
                        }
                    }
                    drop(lease);
                    tokio::time::sleep(Duration::from_secs(5)).await
                }
                Ok(0) => {
                    drop(lease);
                    tokio::task::yield_now().await
                }
                Ok(count) => {
                    drop(lease);
                    tracing::debug!(count, "backfilled catalog work stats batch");
                    tokio::task::yield_now().await;
                }
                Err(err) => {
                    drop(lease);
                    tracing::warn!(error = %err, "catalog stats backfill batch failed");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::resource::ResourceLimits;

    fn facet_response(revision: i64, item_count: usize) -> CatalogFacetsResponse {
        CatalogFacetsResponse {
            items: (0..item_count)
                .map(|index| CatalogTagFacet {
                    id: index as i64 + 1,
                    namespace: "test".to_string(),
                    key: format!("tag-{index}"),
                    label: format!("Tag {index}"),
                    translated_label: None,
                    translated_namespace: None,
                    context_count: item_count.saturating_sub(index) as i64,
                })
                .collect(),
            next_cursor: None,
            catalog_revision: revision,
        }
    }

    #[tokio::test]
    async fn facet_cache_singleflights_hits_and_separates_catalog_revisions() {
        let runtime = CatalogRuntime::default();
        let key = FacetCacheKey {
            catalog_revision: 1,
            request_hash: "same-filter".to_string(),
        };
        let loads = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let first_runtime = runtime.clone();
        let first_key = key.clone();
        let first_loads = loads.clone();
        let first = tokio::spawn(async move {
            first_runtime
                .cached_tag_facets(first_key, move || async move {
                    first_loads.fetch_add(1, Ordering::SeqCst);
                    let _ = started_tx.send(());
                    let _ = release_rx.await;
                    Ok(FacetCacheLoad::Ready(facet_response(1, 2)))
                })
                .await
                .unwrap()
        });
        started_rx.await.unwrap();

        let second_runtime = runtime.clone();
        let second_key = key.clone();
        let second = tokio::spawn(async move {
            second_runtime
                .cached_tag_facets(second_key, move || async move {
                    panic!("a coalesced request must not execute a second query")
                })
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if runtime.snapshot().await.facet_coalesced == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        release_tx.send(()).unwrap();
        assert_eq!(first.await.unwrap().unwrap().catalog_revision, 1);
        assert_eq!(second.await.unwrap().unwrap().catalog_revision, 1);
        assert_eq!(loads.load(Ordering::SeqCst), 1);

        let hit = runtime
            .cached_tag_facets(key, move || async move {
                panic!("a ready cache hit must not execute the query")
            })
            .await
            .unwrap();
        assert_eq!(hit.unwrap().items.len(), 2);

        let newer = runtime
            .cached_tag_facets(
                FacetCacheKey {
                    catalog_revision: 2,
                    request_hash: "same-filter".to_string(),
                },
                move || async move { Ok(FacetCacheLoad::Ready(facet_response(2, 1))) },
            )
            .await
            .unwrap();
        assert_eq!(newer.unwrap().catalog_revision, 2);
        let snapshot = runtime.snapshot().await;
        assert_eq!(snapshot.facet_hits, 1);
        assert_eq!(snapshot.facet_misses, 2);
        assert_eq!(snapshot.facet_coalesced, 1);
        assert_eq!(snapshot.facet_entries, 2);
        assert_eq!(snapshot.facet_items, 3);
    }

    #[tokio::test]
    async fn facet_cache_is_bounded_by_entries_and_materialized_items() {
        let runtime = CatalogRuntime::default();
        for index in 0..(FACET_CACHE_MAX_ENTRIES * 2) {
            runtime
                .cached_tag_facets(
                    FacetCacheKey {
                        catalog_revision: 1,
                        request_hash: format!("filter-{index}"),
                    },
                    move || async move {
                        Ok(FacetCacheLoad::Ready(facet_response(
                            1,
                            MAX_PAGE_SIZE as usize,
                        )))
                    },
                )
                .await
                .unwrap();
        }
        let snapshot = runtime.snapshot().await;
        assert!(snapshot.facet_entries <= FACET_CACHE_MAX_ENTRIES);
        assert!(snapshot.facet_items <= FACET_CACHE_MAX_ITEMS);
        assert!(snapshot.facet_evictions > 0);
    }

    #[tokio::test]
    async fn facet_cache_discards_a_revision_race_before_retrying() {
        let runtime = CatalogRuntime::default();
        let key = FacetCacheKey {
            catalog_revision: 41,
            request_hash: "race".to_string(),
        };
        assert!(runtime
            .cached_tag_facets(key.clone(), move || async move {
                Ok(FacetCacheLoad::RevisionChanged)
            })
            .await
            .unwrap()
            .is_none());
        let after_race = runtime.snapshot().await;
        assert_eq!(after_race.facet_entries, 0);
        assert_eq!(after_race.facet_items, 0);

        let retry = runtime
            .cached_tag_facets(key, move || async move {
                Ok(FacetCacheLoad::Ready(facet_response(41, 2)))
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retry.catalog_revision, 41);
        let after_retry = runtime.snapshot().await;
        assert_eq!(after_retry.facet_entries, 1);
        assert_eq!(after_retry.facet_items, 2);
        assert_eq!(after_retry.facet_misses, 2);
    }

    #[test]
    fn facet_cache_key_binds_revision_request_and_search_candidates() {
        let query = CatalogFacetsQuery {
            kind: None,
            collection: None,
            include_tag: Some("genre:test".to_string()),
            q: None,
            tag_q: None,
            cursor: None,
            limit: Some(100),
        };
        let first = facet_cache_key(1, &query, Some("[1,2]")).unwrap();
        assert_ne!(first, facet_cache_key(2, &query, Some("[1,2]")).unwrap());
        assert_ne!(first, facet_cache_key(1, &query, Some("[2,3]")).unwrap());
        let mut changed_query = query;
        changed_query.tag_q = Some("test".to_string());
        assert_ne!(
            first,
            facet_cache_key(1, &changed_query, Some("[1,2]")).unwrap()
        );

        let canonical = CatalogFacetsQuery {
            kind: Some("all".to_string()),
            collection: None,
            include_tag: Some(" genre:test,genre:test ".to_string()),
            q: Some("  ".to_string()),
            tag_q: None,
            cursor: None,
            limit: Some(MAX_PAGE_SIZE + 100),
        };
        let normalized = CatalogFacetsQuery {
            kind: None,
            collection: None,
            include_tag: Some("genre:test".to_string()),
            q: None,
            tag_q: None,
            cursor: None,
            limit: Some(MAX_PAGE_SIZE),
        };
        assert_eq!(
            facet_cache_key(1, &canonical, None).unwrap(),
            facet_cache_key(1, &normalized, None).unwrap()
        );
    }

    async fn test_db() -> (tempfile::TempDir, Db) {
        let temp = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}",
            temp.path()
                .join("library.sqlite")
                .to_string_lossy()
                .replace('\\', "/")
        );
        let db = Db::connect(&url).await.unwrap();
        db.migrate().await.unwrap();
        (temp, db)
    }

    async fn add_work(db: &Db, id: i64, title: &str, updated_at: &str) -> i64 {
        let work_id = db
            .upsert_work(
                "gallery",
                title,
                Some(&format!("/gallery/{id}")),
                None,
                None,
                None,
                serde_json::json!({}),
            )
            .await
            .unwrap();
        sqlx::query("UPDATE works SET updated_at = ?1 WHERE id = ?2")
            .bind(updated_at)
            .bind(work_id)
            .execute(db.pool())
            .await
            .unwrap();
        work_id
    }

    async fn add_kind_work(
        db: &Db,
        kind: &str,
        title: &str,
        path: &str,
        meta: serde_json::Value,
        updated_at: &str,
    ) -> i64 {
        let work_id = db
            .upsert_work(kind, title, Some(path), None, None, None, meta)
            .await
            .unwrap();
        sqlx::query("UPDATE works SET updated_at = ?1 WHERE id = ?2")
            .bind(updated_at)
            .bind(work_id)
            .execute(db.pool())
            .await
            .unwrap();
        work_id
    }

    #[tokio::test]
    async fn bitmap_facet_path_matches_dynamic_sql_after_background_warmup() {
        let (_temp, db) = test_db().await;
        let first = add_work(&db, 1, "first", "2025-01-01T00:00:00.000Z").await;
        let second = add_work(&db, 2, "second", "2025-01-02T00:00:00.000Z").await;
        let selected = db
            .upsert_tag(
                "scope", "selected", "selected", None, None, "test", None, None,
            )
            .await
            .unwrap();
        let first_only = db
            .upsert_tag(
                "scope",
                "first-only",
                "first-only",
                None,
                None,
                "test",
                None,
                None,
            )
            .await
            .unwrap();
        let second_only = db
            .upsert_tag(
                "scope",
                "second-only",
                "second-only",
                None,
                None,
                "test",
                None,
                None,
            )
            .await
            .unwrap();
        db.link_tag(first, selected).await.unwrap();
        db.link_tag(first, first_only).await.unwrap();
        db.link_tag(second, selected).await.unwrap();
        db.link_tag(second, second_only).await.unwrap();
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'gallery'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let revision = catalog_revision(&db).await.unwrap();
        let query = CatalogFacetsQuery {
            kind: Some("gallery".to_string()),
            collection: None,
            include_tag: Some("scope:selected".to_string()),
            q: None,
            tag_q: None,
            cursor: None,
            limit: Some(100),
        };
        let dynamic =
            query_tag_facets_with_candidates(&db, query.clone(), None, None, revision, None)
                .await
                .unwrap();
        let runtime = CatalogRuntime::default();
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let warming = query_tag_facets_with_candidates(
            &db,
            query.clone(),
            None,
            Some((&runtime, &resources)),
            revision,
            None,
        )
        .await
        .unwrap();
        assert_eq!(warming.items.len(), dynamic.items.len());
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if runtime.snapshot().await.facet_bitmap.state == "ready" {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let bitmap = query_tag_facets_with_candidates(
            &db,
            query,
            None,
            Some((&runtime, &resources)),
            revision,
            None,
        )
        .await
        .unwrap();
        let dynamic_counts = dynamic
            .items
            .into_iter()
            .map(|item| (item.key, item.context_count))
            .collect::<BTreeMap<_, _>>();
        let bitmap_counts = bitmap
            .items
            .into_iter()
            .map(|item| (item.key, item.context_count))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(bitmap_counts, dynamic_counts);
        assert_eq!(runtime.snapshot().await.facet_bitmap.queries, 1);
    }

    #[tokio::test]
    async fn catalog_pages_are_complete_non_overlapping_and_cursor_bound() {
        let (_temp, db) = test_db().await;
        for id in 1..=5 {
            add_work(
                &db,
                id,
                &format!("work {id}"),
                &format!("2025-01-0{id}T00:00:00.000Z"),
            )
            .await;
        }
        let first = query_works(
            &db,
            CatalogWorksQuery {
                kind: Some("gallery".to_string()),
                collection: None,
                include_tag: None,
                q: None,
                sort: None,
                cursor: None,
                limit: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            first
                .items
                .iter()
                .map(|item| item.title.as_str())
                .collect::<Vec<_>>(),
            vec!["work 5", "work 4"]
        );
        let second = query_works(
            &db,
            CatalogWorksQuery {
                kind: Some("gallery".to_string()),
                collection: None,
                include_tag: None,
                q: None,
                sort: None,
                cursor: first.next_cursor,
                limit: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            second
                .items
                .iter()
                .map(|item| item.title.as_str())
                .collect::<Vec<_>>(),
            vec!["work 3", "work 2"]
        );
        let mismatched = query_works(
            &db,
            CatalogWorksQuery {
                kind: Some("comic".to_string()),
                collection: None,
                include_tag: None,
                q: None,
                sort: None,
                cursor: second.next_cursor,
                limit: Some(2),
            },
        )
        .await;
        assert!(matches!(mismatched, Err(AppError::BadRequest(_))));
    }

    #[tokio::test]
    async fn catalog_works_page_and_revisions_share_one_tracked_snapshot() {
        let (_temp, db) = test_db().await;
        add_work(
            &db,
            1,
            "snapshot-bound catalog page",
            "2025-01-01T00:00:00.000Z",
        )
        .await;
        sqlx::query(
            "UPDATE work_stats SET computed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE work_id = 1",
        )
        .execute(db.pool())
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM work_stats WHERE computed_at IS NULL",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            0,
            "fixture must not trigger the stats-backfill retry"
        );

        let before = db.runtime_snapshot().await.read_snapshot;
        let response = query_works(
            &db,
            CatalogWorksQuery {
                kind: Some("gallery".to_string()),
                collection: None,
                include_tag: None,
                q: None,
                sort: None,
                cursor: None,
                limit: Some(10),
            },
        )
        .await
        .unwrap();
        let after = db.runtime_snapshot().await.read_snapshot;

        assert_eq!(response.items.len(), 1);
        assert_eq!(
            after.samples.saturating_sub(before.samples),
            1,
            "works page must use one explicit read snapshot"
        );
        assert_eq!(
            after.completed.saturating_sub(before.completed),
            1,
            "works page snapshot must be closed before returning"
        );
        assert_eq!(
            after
                .implicit_rollbacks
                .saturating_sub(before.implicit_rollbacks),
            0,
            "successful works page must commit its read snapshot"
        );
        assert_eq!(after.active, before.active);
        assert_eq!(
            response.catalog_revision,
            catalog_revision(&db).await.unwrap()
        );
    }

    #[tokio::test]
    async fn catalog_secondary_pages_use_tracked_snapshots_and_expire_cursors() {
        let (_temp, db) = test_db().await;
        let first = add_kind_work(
            &db,
            "audio",
            "first audio",
            "/audio/first",
            serde_json::json!({}),
            "2025-01-02T00:00:00.000Z",
        )
        .await;
        let second = add_kind_work(
            &db,
            "audio",
            "second audio",
            "/audio/second",
            serde_json::json!({}),
            "2025-01-01T00:00:00.000Z",
        )
        .await;
        let first_asset = db
            .upsert_asset(
                first,
                "/audio/first/track-1.flac",
                "audio/flac",
                "track",
                None,
                Some(0),
                Some(10),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        db.upsert_asset(
            first,
            "/audio/first/track-2.flac",
            "audio/flac",
            "track",
            None,
            Some(1),
            Some(10),
            serde_json::json!({}),
        )
        .await
        .unwrap();
        while backfill_stats_batch(&db, 16).await.unwrap() > 0 {}
        db.update_work_progress(first, 0.1, Some("one"), 1)
            .await
            .unwrap();
        db.update_work_progress(second, 0.2, Some("two"), 1)
            .await
            .unwrap();

        let before = db.runtime_snapshot().await.read_snapshot;
        let _counts = query_counts_with_candidates(
            &db,
            CatalogCountsQuery {
                include_tag: None,
                q: None,
            },
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let _random = query_random_work_with_candidates(
            &db,
            CatalogRandomQuery {
                kind: Some("audio".to_string()),
                collection: None,
                include_tag: None,
                q: None,
            },
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let _collections = query_collections(
            &db,
            CatalogCollectionsQuery {
                kind: Some("audio".to_string()),
                include_tag: None,
                q: None,
                cursor: None,
                limit: Some(10),
            },
        )
        .await
        .unwrap();
        let history = query_history(
            &db,
            CatalogHistoryQuery {
                cursor: None,
                limit: Some(1),
            },
        )
        .await
        .unwrap();
        let assets = query_assets(
            &db,
            first,
            CatalogAssetsQuery {
                role: Some("track".to_string()),
                cursor: None,
                limit: Some(1),
            },
        )
        .await
        .unwrap();
        let after = db.runtime_snapshot().await.read_snapshot;
        assert_eq!(
            after.samples.saturating_sub(before.samples),
            5,
            "counts/random/collections/history/assets must each use one tracked snapshot"
        );
        assert_eq!(after.active, before.active);
        assert!(history.next_cursor.is_some());
        assert!(assets.next_cursor.is_some());

        db.update_work_progress(first, 0.3, Some("three"), 2)
            .await
            .unwrap();
        let stale_history = query_history(
            &db,
            CatalogHistoryQuery {
                cursor: history.next_cursor,
                limit: Some(1),
            },
        )
        .await;
        assert!(matches!(stale_history, Err(AppError::BadRequest(_))));

        db.set_work_cover(first, first_asset).await.unwrap();
        let stale_assets = query_assets(
            &db,
            first,
            CatalogAssetsQuery {
                role: Some("track".to_string()),
                cursor: assets.next_cursor,
                limit: Some(1),
            },
        )
        .await;
        assert!(matches!(stale_assets, Err(AppError::BadRequest(_))));
    }

    #[tokio::test]
    async fn catalog_assets_uses_valid_maintained_count_and_exact_pending_fallback() {
        let (_temp, db) = test_db().await;
        let work_id = add_kind_work(
            &db,
            "novel",
            "large novel assets",
            "/novels/large.epub",
            serde_json::json!({}),
            "2025-01-01T00:00:00.000Z",
        )
        .await;
        for position in 0..3_i64 {
            let path = format!("/novels/large/chapter-{position}.xhtml");
            db.upsert_asset(
                work_id,
                &path,
                "application/xhtml+xml",
                "chapter",
                None,
                Some(position),
                Some(10),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        }
        while backfill_stats_batch(&db, 16).await.unwrap() > 0 {}

        // A valid stats row is the maintained source of truth for the
        // unfiltered total; use a sentinel to prove this branch does not run
        // COUNT(*) over all assets on every cursor page.
        sqlx::query(
            "UPDATE work_stats SET asset_count = 77, computed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE work_id = ?1",
        )
        .bind(work_id)
        .execute(db.pool())
        .await
        .unwrap();
        let maintained = query_assets(
            &db,
            work_id,
            CatalogAssetsQuery {
                role: None,
                cursor: None,
                limit: Some(1),
            },
        )
        .await
        .unwrap();
        assert_eq!(maintained.total, 77);

        // Pending/migrated stats are not trusted. The exact fact-table count
        // remains the compatibility fallback until the bounded backfill is
        // complete.
        sqlx::query("UPDATE work_stats SET asset_count = 0, computed_at = NULL WHERE work_id = ?1")
            .bind(work_id)
            .execute(db.pool())
            .await
            .unwrap();
        let fallback = query_assets(
            &db,
            work_id,
            CatalogAssetsQuery {
                role: None,
                cursor: None,
                limit: Some(1),
            },
        )
        .await
        .unwrap();
        assert_eq!(fallback.total, 3);
        assert!(fallback.next_cursor.is_some());
    }

    #[tokio::test]
    async fn progress_updates_advance_activity_revision_without_invalidating_catalog_revision() {
        let (_temp, db) = test_db().await;
        let work_id = add_work(&db, 1, "activity revision", "2025-01-01T00:00:00.000Z").await;
        let before = query_works(
            &db,
            CatalogWorksQuery {
                kind: Some("gallery".to_string()),
                collection: None,
                include_tag: None,
                q: None,
                sort: None,
                cursor: None,
                limit: Some(10),
            },
        )
        .await
        .unwrap();

        db.update_work_progress(work_id, 0.6, Some("image-6"), 1)
            .await
            .unwrap();
        let after = query_works(
            &db,
            CatalogWorksQuery {
                kind: Some("gallery".to_string()),
                collection: None,
                include_tag: None,
                q: None,
                sort: None,
                cursor: None,
                limit: Some(10),
            },
        )
        .await
        .unwrap();
        assert_eq!(after.catalog_revision, before.catalog_revision);
        assert_eq!(after.activity_revision, before.activity_revision + 1);
        assert_eq!(after.items[0].progress, 0.6);

        let counts = query_counts_with_candidates(
            &db,
            CatalogCountsQuery {
                include_tag: None,
                q: None,
            },
            None,
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(counts.catalog_revision, after.catalog_revision);
        assert_eq!(counts.activity_revision, after.activity_revision);
        assert_eq!(counts.kinds.get("history"), Some(&1));

        let history = query_history(
            &db,
            CatalogHistoryQuery {
                cursor: None,
                limit: Some(10),
            },
        )
        .await
        .unwrap();
        assert_eq!(history.catalog_revision, after.catalog_revision);
        assert_eq!(history.activity_revision, after.activity_revision);
        assert_eq!(history.items[0].work.id, work_id);
    }

    #[tokio::test]
    async fn selected_tags_use_and_semantics_and_facets_are_contextual() {
        let (_temp, db) = test_db().await;
        let first = add_work(&db, 1, "first", "2025-01-03T00:00:00.000Z").await;
        let second = add_work(&db, 2, "second", "2025-01-02T00:00:00.000Z").await;
        let third = add_work(&db, 3, "third", "2025-01-01T00:00:00.000Z").await;
        let red = db
            .upsert_tag("color", "red", "red", None, None, "test", None, None)
            .await
            .unwrap();
        let blue = db
            .upsert_tag("color", "blue", "blue", None, None, "test", None, None)
            .await
            .unwrap();
        db.link_tag(first, red).await.unwrap();
        db.link_tag(first, blue).await.unwrap();
        db.link_tag(second, red).await.unwrap();
        db.link_tag(third, blue).await.unwrap();

        let page = query_works(
            &db,
            CatalogWorksQuery {
                kind: Some("gallery".to_string()),
                collection: None,
                include_tag: Some("color:red,color:blue".to_string()),
                q: None,
                sort: None,
                cursor: None,
                limit: Some(20),
            },
        )
        .await
        .unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].id, first);

        let facets = query_tag_facets(
            &db,
            CatalogFacetsQuery {
                kind: Some("gallery".to_string()),
                collection: None,
                include_tag: Some("color:red".to_string()),
                q: None,
                tag_q: None,
                cursor: None,
                limit: Some(20),
            },
        )
        .await
        .unwrap();
        let counts = facets
            .items
            .into_iter()
            .map(|item| {
                (
                    format!("{}:{}", item.namespace, item.key),
                    item.context_count,
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(counts.get("color:red"), Some(&2));
        assert_eq!(counts.get("color:blue"), Some(&1));
    }

    #[tokio::test]
    async fn single_selected_tag_facets_preserve_kind_search_and_keyset_cursor() {
        let (_temp, db) = test_db().await;
        let gallery_one = add_kind_work(
            &db,
            "gallery",
            "gallery one",
            "/gallery/one",
            serde_json::json!({}),
            "2025-01-03T00:00:00.000Z",
        )
        .await;
        let gallery_two = add_kind_work(
            &db,
            "gallery",
            "gallery two",
            "/gallery/two",
            serde_json::json!({}),
            "2025-01-02T00:00:00.000Z",
        )
        .await;
        let comic = add_kind_work(
            &db,
            "comic",
            "comic",
            "/comic/one.cbz",
            serde_json::json!({}),
            "2025-01-01T00:00:00.000Z",
        )
        .await;
        let selected = db
            .upsert_tag(
                "scope", "selected", "selected", None, None, "test", None, None,
            )
            .await
            .unwrap();
        let alpha = db
            .upsert_tag("facet", "alpha", "alpha", None, None, "test", None, None)
            .await
            .unwrap();
        let beta = db
            .upsert_tag("facet", "beta", "beta", None, None, "test", None, None)
            .await
            .unwrap();
        db.link_tag(gallery_one, selected).await.unwrap();
        db.link_tag(gallery_one, alpha).await.unwrap();
        db.link_tag(gallery_one, beta).await.unwrap();
        db.link_tag(gallery_two, selected).await.unwrap();
        db.link_tag(gallery_two, alpha).await.unwrap();
        db.link_tag(comic, selected).await.unwrap();
        db.link_tag(comic, beta).await.unwrap();

        let filtered = query_tag_facets(
            &db,
            CatalogFacetsQuery {
                kind: Some("gallery".to_string()),
                collection: None,
                include_tag: Some("scope:selected".to_string()),
                q: None,
                tag_q: Some("alpha".to_string()),
                cursor: None,
                limit: Some(20),
            },
        )
        .await
        .unwrap();
        assert_eq!(filtered.items.len(), 1);
        assert_eq!(filtered.items[0].key, "alpha");
        assert_eq!(filtered.items[0].context_count, 2);

        let first = query_tag_facets(
            &db,
            CatalogFacetsQuery {
                kind: Some("gallery".to_string()),
                collection: None,
                include_tag: Some("scope:selected".to_string()),
                q: None,
                tag_q: None,
                cursor: None,
                limit: Some(1),
            },
        )
        .await
        .unwrap();
        assert_eq!(first.items.len(), 1);
        assert!(first.next_cursor.is_some());

        let second = query_tag_facets(
            &db,
            CatalogFacetsQuery {
                kind: Some("gallery".to_string()),
                collection: None,
                include_tag: Some("scope:selected".to_string()),
                q: None,
                tag_q: None,
                cursor: first.next_cursor,
                limit: Some(20),
            },
        )
        .await
        .unwrap();
        let keys = second
            .items
            .iter()
            .map(|item| item.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["alpha", "beta"]);
        assert_eq!(second.items[0].context_count, 2);
        assert_eq!(second.items[1].context_count, 1);
        assert!(second.next_cursor.is_none());

        let comic_facets = query_tag_facets(
            &db,
            CatalogFacetsQuery {
                kind: Some("comic".to_string()),
                collection: None,
                include_tag: Some("scope:selected".to_string()),
                q: None,
                tag_q: None,
                cursor: None,
                limit: Some(20),
            },
        )
        .await
        .unwrap();
        let comic_counts = comic_facets
            .items
            .into_iter()
            .map(|item| (item.key, item.context_count))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(comic_counts.get("selected"), Some(&1));
        assert_eq!(comic_counts.get("beta"), Some(&1));
        assert!(!comic_counts.contains_key("alpha"));
    }

    #[tokio::test]
    async fn preaggregated_kind_facets_backfill_and_follow_tag_kind_and_work_changes() {
        let (_temp, db) = test_db().await;
        let gallery_one = add_kind_work(
            &db,
            "gallery",
            "gallery one",
            "/gallery/one",
            serde_json::json!({}),
            "2025-01-03T00:00:00.000Z",
        )
        .await;
        let gallery_two = add_kind_work(
            &db,
            "gallery",
            "gallery two",
            "/gallery/two",
            serde_json::json!({}),
            "2025-01-02T00:00:00.000Z",
        )
        .await;
        let comic = add_kind_work(
            &db,
            "comic",
            "comic",
            "/comic/one.cbz",
            serde_json::json!({}),
            "2025-01-01T00:00:00.000Z",
        )
        .await;
        let red = db
            .upsert_tag("color", "red", "red", None, None, "test", None, None)
            .await
            .unwrap();
        let blue = db
            .upsert_tag("color", "blue", "blue", None, None, "test", None, None)
            .await
            .unwrap();
        db.link_tag(gallery_one, red).await.unwrap();
        db.link_tag(gallery_one, blue).await.unwrap();
        db.link_tag(gallery_two, red).await.unwrap();
        db.link_tag(comic, blue).await.unwrap();

        assert!(!tag_kind_counts_ready(&db).await.unwrap());
        let dynamic = query_tag_facets(
            &db,
            CatalogFacetsQuery {
                kind: Some("gallery".to_string()),
                collection: None,
                include_tag: None,
                q: None,
                tag_q: None,
                cursor: None,
                limit: Some(20),
            },
        )
        .await
        .unwrap();
        let dynamic_counts = dynamic
            .items
            .into_iter()
            .map(|item| (item.key, item.context_count))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(dynamic_counts.get("red"), Some(&2));
        assert_eq!(dynamic_counts.get("blue"), Some(&1));

        assert_eq!(rebuild_tag_kind_counts(&db).await.unwrap(), 3);
        assert!(tag_kind_counts_ready(&db).await.unwrap());
        let preaggregated = query_tag_facets(
            &db,
            CatalogFacetsQuery {
                kind: Some("gallery".to_string()),
                collection: None,
                include_tag: None,
                q: None,
                tag_q: None,
                cursor: None,
                limit: Some(20),
            },
        )
        .await
        .unwrap();
        let preaggregated_counts = preaggregated
            .items
            .into_iter()
            .map(|item| (item.key, item.context_count))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(preaggregated_counts, dynamic_counts);

        let revision_before_dirty = catalog_revision(&db).await.unwrap();
        db.link_tag(gallery_two, blue).await.unwrap();
        let revision_after_dirty = catalog_revision(&db).await.unwrap();
        assert_eq!(revision_after_dirty, revision_before_dirty + 1);
        db.link_tag(gallery_two, blue).await.unwrap();
        assert_eq!(catalog_revision(&db).await.unwrap(), revision_after_dirty);
        sqlx::query("UPDATE works SET kind = 'comic' WHERE id = ?1")
            .bind(gallery_one)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM works WHERE id = ?1")
            .bind(gallery_one)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM work_tags WHERE work_id = ?1 AND tag_id = ?2")
            .bind(gallery_two)
            .bind(red)
            .execute(db.pool())
            .await
            .unwrap();

        assert!(!tag_kind_counts_ready(&db).await.unwrap());
        let dynamic_while_dirty = query_tag_facets(
            &db,
            CatalogFacetsQuery {
                kind: Some("gallery".to_string()),
                collection: None,
                include_tag: None,
                q: None,
                tag_q: None,
                cursor: None,
                limit: Some(20),
            },
        )
        .await
        .unwrap();
        let dirty_counts = dynamic_while_dirty
            .items
            .into_iter()
            .map(|item| (item.key, item.context_count))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(dirty_counts.get("blue"), Some(&1));
        assert!(!dirty_counts.contains_key("red"));
        assert_eq!(rebuild_tag_kind_counts(&db).await.unwrap(), 2);
        assert!(tag_kind_counts_ready(&db).await.unwrap());

        let rows = sqlx::query(
            r#"
            SELECT counts.kind, tag.key, counts.work_count
            FROM tag_kind_counts AS counts
            JOIN tags AS tag ON tag.id = counts.tag_id
            ORDER BY counts.kind, tag.key
            "#,
        )
        .fetch_all(db.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                (row.get::<String, _>("kind"), row.get::<String, _>("key")),
                row.get::<i64, _>("work_count"),
            )
        })
        .collect::<BTreeMap<_, _>>();
        assert_eq!(
            rows.get(&("gallery".to_string(), "blue".to_string())),
            Some(&1)
        );
        assert_eq!(
            rows.get(&("comic".to_string(), "blue".to_string())),
            Some(&1)
        );
        assert!(!rows.contains_key(&("gallery".to_string(), "red".to_string())));
        assert!(!rows.contains_key(&("comic".to_string(), "red".to_string())));
        assert!(rows.values().all(|count| *count > 0));
    }

    #[tokio::test]
    async fn tag_kind_count_error_uses_the_single_writer_gate() {
        let (_temp, db) = test_db().await;
        let before = db.write_snapshot();
        record_tag_kind_count_error(&db, "synthetic facet backfill failure").await;
        let error = sqlx::query_scalar::<_, Option<String>>(
            "SELECT last_error FROM tag_kind_count_state WHERE singleton = 1",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(error.as_deref(), Some("synthetic facet backfill failure"));
        let after = db.write_snapshot();
        assert_eq!(after.queue_depth, 0);
        assert_eq!(after.active, 0);
        assert_eq!(after.completed, before.completed + 1);
    }

    #[tokio::test]
    async fn universal_selected_tag_uses_ready_kind_counts_but_selective_or_dirty_tags_do_not() {
        let (_temp, db) = test_db().await;
        let first = add_kind_work(
            &db,
            "gallery",
            "first",
            "/gallery/first",
            serde_json::json!({}),
            "2025-01-03T00:00:00.000Z",
        )
        .await;
        let second = add_kind_work(
            &db,
            "gallery",
            "second",
            "/gallery/second",
            serde_json::json!({}),
            "2025-01-02T00:00:00.000Z",
        )
        .await;
        add_kind_work(
            &db,
            "comic",
            "comic",
            "/comic/one.cbz",
            serde_json::json!({}),
            "2025-01-01T00:00:00.000Z",
        )
        .await;
        let universal = db
            .upsert_tag(
                "scope",
                "all-gallery",
                "all",
                None,
                None,
                "test",
                None,
                None,
            )
            .await
            .unwrap();
        let selective = db
            .upsert_tag(
                "scope",
                "one-gallery",
                "one",
                None,
                None,
                "test",
                None,
                None,
            )
            .await
            .unwrap();
        db.link_tag(first, universal).await.unwrap();
        db.link_tag(second, universal).await.unwrap();
        db.link_tag(first, selective).await.unwrap();
        rebuild_tag_kind_counts(&db).await.unwrap();

        let universal_json = selected_tags_json(&["scope:all-gallery".to_string()]).unwrap();
        assert!(try_query_preaggregated_tag_facets(
            &db,
            Some("gallery"),
            Some(&universal_json),
            "",
            None,
            20,
            "universal",
        )
        .await
        .unwrap()
        .is_some());
        assert!(try_query_preaggregated_tag_facets(
            &db,
            None,
            Some(&universal_json),
            "",
            None,
            20,
            "global",
        )
        .await
        .unwrap()
        .is_none());

        let selective_json = selected_tags_json(&["scope:one-gallery".to_string()]).unwrap();
        assert!(try_query_preaggregated_tag_facets(
            &db,
            Some("gallery"),
            Some(&selective_json),
            "",
            None,
            20,
            "selective",
        )
        .await
        .unwrap()
        .is_none());

        db.link_tag(second, selective).await.unwrap();
        assert!(!tag_kind_counts_ready(&db).await.unwrap());
        assert!(try_query_preaggregated_tag_facets(
            &db,
            Some("gallery"),
            Some(&universal_json),
            "",
            None,
            20,
            "dirty",
        )
        .await
        .unwrap()
        .is_none());
    }

    #[tokio::test]
    async fn pending_stats_are_repaired_in_one_bounded_page_batch() {
        let (_temp, db) = test_db().await;
        let work_id = add_work(&db, 1, "with assets", "2025-01-01T00:00:00.000Z").await;
        db.upsert_asset(
            work_id,
            "/gallery/1/one.jpg",
            "image/jpeg",
            "page",
            None,
            Some(0),
            Some(10),
            serde_json::json!({}),
        )
        .await
        .unwrap();
        db.upsert_asset(
            work_id,
            "/gallery/1/cover.jpg",
            "image/jpeg",
            "cover",
            None,
            None,
            Some(10),
            serde_json::json!({}),
        )
        .await
        .unwrap();
        db.upsert_asset(
            work_id,
            "/gallery/1/track.mp3",
            "audio/mpeg",
            "track",
            None,
            Some(1),
            Some(10),
            serde_json::json!({}),
        )
        .await
        .unwrap();
        db.upsert_asset(
            work_id,
            "/gallery/1/archive.cbz",
            "application/zip",
            "archive",
            None,
            None,
            Some(10),
            serde_json::json!({ "page_count": 2 }),
        )
        .await
        .unwrap();
        let tag_id = db
            .upsert_tag("test", "stats", "stats", None, None, "test", None, None)
            .await
            .unwrap();
        db.link_tag(work_id, tag_id).await.unwrap();
        sqlx::query(
            "UPDATE work_stats
             SET asset_count = 0, tag_count = 0, image_count = 0, track_count = 0, page_count = 0,
                 computed_at = NULL
             WHERE work_id = ?1",
        )
        .bind(work_id)
        .execute(db.pool())
        .await
        .unwrap();
        let page = query_works(
            &db,
            CatalogWorksQuery {
                kind: None,
                collection: None,
                include_tag: None,
                q: None,
                sort: None,
                cursor: None,
                limit: Some(10),
            },
        )
        .await
        .unwrap();
        assert_eq!(page.items[0].asset_count, 4);
        assert_eq!(page.items[0].tag_count, 1);
        assert_eq!(page.items[0].image_count, 2);
        assert_eq!(page.items[0].track_count, 1);
        assert_eq!(page.items[0].page_count, 3);
    }

    #[tokio::test]
    async fn collections_are_server_materialized_and_members_use_keyset_pages() {
        let (_temp, db) = test_db().await;
        let first = add_kind_work(
            &db,
            "comic",
            "Alice 01",
            "/comic/alice-01.cbz",
            serde_json::json!({ "artist": "Alice" }),
            "2025-01-03T00:00:00.000Z",
        )
        .await;
        let second = add_kind_work(
            &db,
            "comic",
            "Alice 02",
            "/comic/alice-02.cbz",
            serde_json::json!({ "artist": "Alice" }),
            "2025-01-02T00:00:00.000Z",
        )
        .await;
        add_kind_work(
            &db,
            "comic",
            "Standalone",
            "/comic/standalone.cbz",
            serde_json::json!({}),
            "2025-01-01T00:00:00.000Z",
        )
        .await;
        while backfill_stats_batch(&db, 2).await.unwrap() > 0 {}

        let collections = query_collections(
            &db,
            CatalogCollectionsQuery {
                kind: Some("comic".to_string()),
                include_tag: None,
                q: None,
                cursor: None,
                limit: Some(20),
            },
        )
        .await
        .unwrap();
        assert!(!collections.backfill_pending);
        assert_eq!(collections.items.len(), 2);
        let alice = collections
            .items
            .iter()
            .find(|item| item.title == "Alice")
            .unwrap();
        assert_eq!(alice.kind, "comic-collection");
        assert_eq!(alice.work_count, 2);
        let collection_key = alice.collection_key.clone();

        let page = query_works(
            &db,
            CatalogWorksQuery {
                kind: Some("comic".to_string()),
                collection: Some(collection_key),
                include_tag: None,
                q: None,
                sort: None,
                cursor: None,
                limit: Some(20),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            page.items
                .iter()
                .map(|item| item.id)
                .collect::<std::collections::BTreeSet<_>>(),
            [first, second].into_iter().collect()
        );
    }

    #[tokio::test]
    async fn catalog_random_work_honors_kind_collection_tag_and_query_filters() {
        let (_temp, db) = test_db().await;
        let alice = add_kind_work(
            &db,
            "comic",
            "Alice volume",
            "/comic/alice/volume.cbz",
            serde_json::json!({ "artist": "Alice" }),
            "2025-01-03T00:00:00.000Z",
        )
        .await;
        add_kind_work(
            &db,
            "comic",
            "Bob volume",
            "/comic/bob/volume.cbz",
            serde_json::json!({ "artist": "Bob" }),
            "2025-01-02T00:00:00.000Z",
        )
        .await;
        let favorite = db
            .upsert_tag(
                "state", "favorite", "favorite", None, None, "test", None, None,
            )
            .await
            .unwrap();
        db.link_tag(alice, favorite).await.unwrap();
        while backfill_stats_batch(&db, 8).await.unwrap() > 0 {}

        let collections = query_collections(
            &db,
            CatalogCollectionsQuery {
                kind: Some("comic".to_string()),
                include_tag: None,
                q: None,
                cursor: None,
                limit: Some(20),
            },
        )
        .await
        .unwrap();
        let alice_collection = collections
            .items
            .iter()
            .find(|item| item.title == "Alice volume")
            .unwrap()
            .collection_key
            .clone();

        let selected = query_random_work(
            &db,
            CatalogRandomQuery {
                kind: Some("comic".to_string()),
                collection: Some(alice_collection),
                include_tag: Some("state:favorite".to_string()),
                q: Some("Alice".to_string()),
            },
        )
        .await
        .unwrap();
        assert_eq!(selected.item.unwrap().id, alice);

        let empty = query_random_work(
            &db,
            CatalogRandomQuery {
                kind: Some("audio".to_string()),
                collection: None,
                include_tag: None,
                q: None,
            },
        )
        .await
        .unwrap();
        assert!(empty.item.is_none());
    }

    #[tokio::test]
    async fn catalog_assets_are_path_redacted_and_cursor_bound_to_role() {
        let (_temp, db) = test_db().await;
        let work_id = add_kind_work(
            &db,
            "audio",
            "tracks",
            "/audio/RJ1",
            serde_json::json!({}),
            "2025-01-01T00:00:00.000Z",
        )
        .await;
        for position in 0..5 {
            db.upsert_asset(
                work_id,
                &format!("/private/audio/track-{position}.flac"),
                "audio/flac",
                "track",
                None,
                Some(position),
                Some(100),
                serde_json::json!({ "track_key": position }),
            )
            .await
            .unwrap();
        }
        let first = query_assets(
            &db,
            work_id,
            CatalogAssetsQuery {
                role: Some("track".to_string()),
                cursor: None,
                limit: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(first.total, 5);
        assert_eq!(first.items[0].name, "track-0.flac");
        assert!(!serde_json::to_string(&first)
            .unwrap()
            .contains("/private/audio"));
        let second = query_assets(
            &db,
            work_id,
            CatalogAssetsQuery {
                role: Some("track".to_string()),
                cursor: first.next_cursor.clone(),
                limit: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(second.items[0].name, "track-2.flac");
        let mismatched = query_assets(
            &db,
            work_id,
            CatalogAssetsQuery {
                role: Some("cover".to_string()),
                cursor: first.next_cursor,
                limit: Some(2),
            },
        )
        .await;
        assert!(matches!(mismatched, Err(AppError::BadRequest(_))));
    }

    #[tokio::test]
    async fn catalog_playable_assets_merge_track_and_mime_branches_before_cursoring() {
        let (_temp, db) = test_db().await;
        let work_id = add_kind_work(
            &db,
            "audio",
            "mixed legacy tracks",
            "/audio/mixed",
            serde_json::json!({}),
            "2025-01-01T00:00:00.000Z",
        )
        .await;
        for (name, role, position) in [
            ("track-0.flac", "track", 0),
            ("legacy-1.mp3", "legacy", 1),
            ("track-2.flac", "track", 2),
            ("legacy-3.mp3", "legacy", 3),
        ] {
            db.upsert_asset(
                work_id,
                &format!("/private/audio/{name}"),
                if role == "track" {
                    "audio/flac"
                } else {
                    "audio/mpeg"
                },
                role,
                None,
                Some(position),
                Some(100),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        }
        let first = query_assets(
            &db,
            work_id,
            CatalogAssetsQuery {
                role: Some("track".to_string()),
                cursor: None,
                limit: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(first.total, 4);
        assert_eq!(
            first
                .items
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            vec!["legacy-1.mp3", "legacy-3.mp3"]
        );
        let second = query_assets(
            &db,
            work_id,
            CatalogAssetsQuery {
                role: Some("track".to_string()),
                cursor: first.next_cursor,
                limit: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            second
                .items
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            vec!["track-0.flac", "track-2.flac"]
        );
        assert!(second.next_cursor.is_none());
    }

    #[tokio::test]
    async fn catalog_asset_keyset_keeps_sentinel_positions_in_order() {
        let (_temp, db) = test_db().await;
        let work_id = add_kind_work(
            &db,
            "novel",
            "nullable asset positions",
            "/novels/nullable.epub",
            serde_json::json!({}),
            "2025-01-01T00:00:00.000Z",
        )
        .await;
        for (name, position) in [
            ("chapter-0.xhtml", Some(0)),
            ("chapter-1.xhtml", Some(1)),
            ("cover.xhtml", None),
        ] {
            db.upsert_asset(
                work_id,
                &format!("/private/novel/{name}"),
                "application/xhtml+xml",
                "chapter",
                None,
                position,
                Some(100),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        }
        let first = query_assets(
            &db,
            work_id,
            CatalogAssetsQuery {
                role: Some("chapter".to_string()),
                cursor: None,
                limit: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            first
                .items
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            vec!["cover.xhtml", "chapter-0.xhtml"]
        );
        let second = query_assets(
            &db,
            work_id,
            CatalogAssetsQuery {
                role: Some("chapter".to_string()),
                cursor: first.next_cursor,
                limit: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(second.items.len(), 1);
        assert_eq!(second.items[0].name, "chapter-1.xhtml");
        assert!(second.next_cursor.is_none());
    }

    #[tokio::test]
    async fn catalog_history_uses_stable_keyset_order() {
        let (_temp, db) = test_db().await;
        let mut ids = Vec::new();
        for id in 1..=3 {
            let work_id = add_work(
                &db,
                id,
                &format!("history {id}"),
                &format!("2025-01-0{id}T00:00:00.000Z"),
            )
            .await;
            db.update_work_progress(work_id, id as f64 / 10.0, None, id)
                .await
                .unwrap();
            sqlx::query("UPDATE reading_history SET last_opened_at = ? WHERE work_id = ?")
                .bind(format!("2025-02-0{id}T00:00:00.000Z"))
                .bind(work_id)
                .execute(db.pool())
                .await
                .unwrap();
            ids.push(work_id);
        }
        let first = query_history(
            &db,
            CatalogHistoryQuery {
                cursor: None,
                limit: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            first
                .items
                .iter()
                .map(|item| item.work.id)
                .collect::<Vec<_>>(),
            vec![ids[2], ids[1]]
        );
        let second = query_history(
            &db,
            CatalogHistoryQuery {
                cursor: first.next_cursor,
                limit: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(second.items[0].work.id, ids[0]);
    }

    #[tokio::test]
    async fn tantivy_candidates_are_filtered_in_sql_and_bound_to_the_cursor() {
        let (_temp, db) = test_db().await;
        let first_id = add_work(&db, 1, "alpha", "2025-01-03T00:00:00.000Z").await;
        let second_id = add_work(&db, 2, "beta", "2025-01-02T00:00:00.000Z").await;
        let third_id = add_work(&db, 3, "gamma", "2025-01-01T00:00:00.000Z").await;
        let candidates = serde_json::to_string(&vec![second_id, first_id, third_id]).unwrap();
        let first = query_works_with_candidates(
            &db,
            CatalogWorksQuery {
                kind: Some("gallery".to_string()),
                collection: None,
                include_tag: None,
                q: Some("indexed body only".to_string()),
                sort: None,
                cursor: None,
                limit: Some(1),
            },
            Some(&candidates),
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(first.items[0].id, second_id);
        let second = query_works_with_candidates(
            &db,
            CatalogWorksQuery {
                kind: Some("gallery".to_string()),
                collection: None,
                include_tag: None,
                q: Some("indexed body only".to_string()),
                sort: None,
                cursor: first.next_cursor.clone(),
                limit: Some(1),
            },
            Some(&candidates),
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(second.items[0].id, first_id);

        let changed_candidates = serde_json::to_string(&vec![second_id, third_id]).unwrap();
        let stale = query_works_with_candidates(
            &db,
            CatalogWorksQuery {
                kind: Some("gallery".to_string()),
                collection: None,
                include_tag: None,
                q: Some("indexed body only".to_string()),
                sort: None,
                cursor: first.next_cursor,
                limit: Some(1),
            },
            Some(&changed_candidates),
            None,
            None,
        )
        .await;
        assert!(matches!(stale, Err(AppError::BadRequest(_))));
    }

    #[tokio::test]
    async fn search_candidate_revision_fence_rejects_a_newer_catalog_snapshot() {
        let (_temp, db) = test_db().await;
        let work_id = add_work(&db, 1, "revision fence", "2025-01-01T00:00:00.000Z").await;
        let candidates = serde_json::to_string(&vec![work_id]).unwrap();
        let candidate_revision = catalog_revision(&db).await.unwrap();

        sqlx::query("UPDATE catalog_state SET revision = revision + 1 WHERE singleton = 1")
            .execute(db.pool())
            .await
            .unwrap();
        let result = query_works_with_candidates(
            &db,
            CatalogWorksQuery {
                kind: Some("gallery".to_string()),
                collection: None,
                include_tag: None,
                q: Some("indexed body only".to_string()),
                sort: None,
                cursor: None,
                limit: Some(1),
            },
            Some(&candidates),
            Some(candidate_revision),
            None,
        )
        .await;
        assert!(matches!(result, Err(AppError::Overloaded { .. })));
    }

    #[tokio::test]
    async fn search_candidate_revision_fence_rejects_a_newer_search_source_snapshot() {
        let (_temp, db) = test_db().await;
        let work_id = add_work(&db, 1, "search source fence", "2025-01-01T00:00:00.000Z").await;
        let candidates = serde_json::to_string(&vec![work_id]).unwrap();
        let candidate_fence = db.revision_fence_snapshot().await.unwrap();

        let tag_id = db
            .upsert_tag(
                "scope",
                "search-source-change",
                "search-source-change",
                None,
                None,
                "test",
                None,
                None,
            )
            .await
            .unwrap();
        db.link_tag(work_id, tag_id).await.unwrap();
        let changed_fence = db.revision_fence_snapshot().await.unwrap();
        assert_eq!(
            changed_fence.catalog_revision,
            candidate_fence.catalog_revision
        );
        assert!(changed_fence.search_revision > candidate_fence.search_revision);

        let result = query_works_with_candidates(
            &db,
            CatalogWorksQuery {
                kind: Some("gallery".to_string()),
                collection: None,
                include_tag: None,
                q: Some("indexed body only".to_string()),
                sort: None,
                cursor: None,
                limit: Some(1),
            },
            Some(&candidates),
            Some(candidate_fence.catalog_revision),
            Some(candidate_fence.search_revision),
        )
        .await;
        assert!(matches!(result, Err(AppError::Overloaded { .. })));
    }

    #[tokio::test]
    async fn catalog_query_plans_use_covering_order_and_tag_indexes() {
        let (_temp, db) = test_db().await;
        let shelf_plan = sqlx::query(
            "EXPLAIN QUERY PLAN SELECT id FROM works ORDER BY updated_at DESC, id DESC LIMIT 61",
        )
        .fetch_all(db.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>()
        .join("\n");
        assert!(
            shelf_plan.contains("idx_works_updated"),
            "unexpected shelf plan: {shelf_plan}"
        );

        let tag_plan = sqlx::query(
            r#"
            EXPLAIN QUERY PLAN
            SELECT tag.id
            FROM json_each(?) AS input
            JOIN tags AS tag
              ON tag.namespace = json_extract(input.value, '$[0]')
             AND tag.key = json_extract(input.value, '$[1]')
            "#,
        )
        .bind(r#"[["color","red"]]"#)
        .fetch_all(db.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>()
        .join("\n");
        assert!(
            tag_plan.contains("sqlite_autoindex_tags_1"),
            "unexpected tag plan: {tag_plan}"
        );

        let facet_plan = sqlx::query(
            r#"
            EXPLAIN QUERY PLAN
            SELECT tag_id, work_count
            FROM tag_kind_counts
            WHERE kind = ?
            ORDER BY work_count DESC, tag_id
            LIMIT 101
            "#,
        )
        .bind("gallery")
        .fetch_all(db.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>()
        .join("\n");
        assert!(
            facet_plan.contains("idx_tag_kind_counts_order"),
            "unexpected preaggregated facet plan: {facet_plan}"
        );
    }

    #[tokio::test]
    async fn audio_asset_query_plan_uses_compatibility_partial_indexes() {
        let (_temp, db) = test_db().await;
        let work_id = db
            .upsert_work(
                "audio",
                "query-plan audio",
                Some("/audio/query-plan"),
                None,
                None,
                None,
                serde_json::json!({}),
            )
            .await
            .unwrap();
        for position in 0..512_i64 {
            db.upsert_asset(
                work_id,
                &format!("/audio/track-{position:04}.flac"),
                "audio/flac",
                "legacy",
                None,
                Some(position),
                Some(1),
                serde_json::json!({}),
            )
            .await
            .unwrap();
            db.upsert_asset(
                work_id,
                &format!("/audio/track-{position:04}.flac.cover.jpg"),
                "image/jpeg",
                "cover",
                None,
                Some(position),
                Some(1),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        }
        sqlx::query("ANALYZE").execute(db.pool()).await.unwrap();
        let role_plan = sqlx::query(
            r#"
            EXPLAIN QUERY PLAN
            SELECT id, role, mime, position
            FROM assets
            WHERE work_id = ?1
              AND role = 'track'
            ORDER BY role ASC, COALESCE(position, 9223372036854775807) ASC, id ASC
            LIMIT 129
            "#,
        )
        .bind(work_id)
        .fetch_all(db.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>()
        .join("\n");
        let mime_plan = sqlx::query(
            r#"
            EXPLAIN QUERY PLAN
            SELECT id, role, mime, position
            FROM assets
            WHERE work_id = ?1
              AND role <> 'track'
              AND lower(mime) LIKE 'audio/%'
            ORDER BY role ASC, COALESCE(position, 9223372036854775807) ASC, id ASC
            LIMIT 129
            "#,
        )
        .bind(work_id)
        .fetch_all(db.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>()
        .join("\n");
        assert!(
            role_plan.contains("idx_assets_work_audio_role_keyset"),
            "audio role branch did not use the NULL-last keyset index: {role_plan}"
        );
        assert!(
            mime_plan.contains("idx_assets_work_audio_mime_keyset"),
            "audio MIME compatibility branch did not use its partial index: {mime_plan}"
        );
    }
}
