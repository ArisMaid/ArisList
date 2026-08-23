use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Pool, Row, Sqlite};
use sqlx_core::pool::MaybePoolConnection;
use sqlx_core::transaction::Transaction;
use sqlx_sqlite::SqliteConnection;
use std::collections::BTreeMap;
use std::env;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::{AppError, Result};
use crate::migrations;
use crate::models::{
    Asset, ExternalId, HistoryRecord, Job, LibraryResponse, Tag, Work, WorkDetail,
    WorkDetailAssetMode, WorkKind, WorkSummary,
};

const AUDIT_RETENTION_DAYS: i64 = 90;
const AUDIT_MAX_RECORDS: i64 = 20_000;
const AUDIT_PRUNE_INTERVAL: i64 = 256;
const MIB: u64 = 1024 * 1024;
const CATALOG_REVISION_DELTA_LOG_MAX: usize = 2_048;
const SCANNER_ASSET_BATCH_SIZE: usize = 512;
// A detail response is opened on the interactive path.  Keep its asset
// payload bounded even when an audio work contains many thousands of tracks;
// the remaining tracks are available through /works/{id}/assets.
const AUDIO_DETAIL_TRACK_LIMIT: i64 = 128;
const AUDIO_DETAIL_NON_TRACK_LIMIT: i64 = 64;
// Archive readers obtain their page manifest and page bytes through dedicated
// paged/streaming routes. Keep the interactive detail response bounded even
// if an imported work also contains page/image asset rows.
const ARCHIVE_DETAIL_ASSET_LIMIT: i64 = 16;
const SUMMARY_DETAIL_ASSET_LIMIT: i64 = 16;
const LEGACY_LIBRARY_PAGE_LIMIT: i64 = 500;

/// A gallery page and its exact count are read from one SQLite snapshot.
///
/// The legacy `/works/{id}/gallery` route still accepts offset cursors for
/// compatibility, but the response now carries the same source/revision
/// boundary as Catalog asset pages so a later keyset request cannot silently
/// continue across a committed work mutation.
#[derive(Debug)]
pub struct GalleryAssetsPage {
    pub kind: String,
    pub source_version: DateTime<Utc>,
    pub catalog_revision: i64,
    pub total: i64,
    pub items: Vec<Asset>,
}

/// The bounded source rows needed by the gallery cover route.  Keeping the
/// work row and its possible fallback archive in one snapshot avoids two
/// independent pool checkouts for every grid cover miss.
#[derive(Debug)]
pub struct WorkCoverSource {
    pub kind: String,
    pub image_cover: Option<Asset>,
    pub archive: Option<Asset>,
}

/// The archive and metadata rows needed by a comic/CoserPicture manifest.
/// The manifest itself is read from the filesystem, so only these small DB
/// facts are kept in the tracked snapshot.
#[derive(Debug)]
pub struct WorkArchiveSource {
    pub archive: Asset,
    pub kind: String,
    pub meta_json: String,
}

/// One logical searchable mutation may touch a work, several tag links, and
/// every work sharing a changed tag.  All of those outbox rows must carry one
/// monotonic source revision so a consumer can acknowledge exactly the facts
/// it read without turning a large tag fan-out into one global revision per
/// association.
#[derive(Debug, Default)]
pub(crate) struct SearchOutboxPublication {
    search_revision: Option<i64>,
}

impl SearchOutboxPublication {
    pub(crate) async fn revision(
        &mut self,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<i64> {
        if let Some(revision) = self.search_revision {
            return Ok(revision);
        }
        let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let revision = sqlx::query_scalar::<_, i64>(
            r#"
            UPDATE search_source_state
            SET revision = revision + 1,
                updated_at = ?1
            WHERE singleton = 1
            RETURNING revision
            "#,
        )
        .bind(now)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| AppError::Other("search source state row is missing".to_string()))?;
        self.search_revision = Some(revision);
        Ok(revision)
    }
}

#[derive(Debug, Clone, Copy)]
struct CatalogRevisionDelta {
    to_revision: i64,
    work_id: i64,
}

#[derive(Debug, Default)]
struct CatalogRevisionDeltaLog {
    by_from_revision: BTreeMap<i64, CatalogRevisionDelta>,
    evictions: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SqliteRuntimeConfig {
    pub profile: String,
    pub max_connections: u32,
    pub cache_kib_per_connection: u32,
    pub mmap_size_bytes: u64,
    pub busy_timeout_millis: u64,
    pub acquire_timeout_millis: u64,
    pub wal_autocheckpoint_pages: u32,
    pub journal_size_limit_bytes: u64,
    pub writer_queue_max_depth: u32,
    pub writer_queue_max_bytes: u64,
}

impl SqliteRuntimeConfig {
    fn from_env() -> Result<Self> {
        let profile = env::var("RESOURCE_PROFILE")
            .unwrap_or_else(|_| "standard".to_string())
            .trim()
            .to_ascii_lowercase();
        let mut config = Self::for_profile(&profile);
        config.max_connections = env_u32("SQLITE_MAX_CONNECTIONS", config.max_connections, 1, 32)?;
        config.cache_kib_per_connection = env_u32(
            "SQLITE_CACHE_KIB_PER_CONNECTION",
            config.cache_kib_per_connection,
            1024,
            256 * 1024,
        )?;
        config.mmap_size_bytes = env_u64_range(
            "SQLITE_MMAP_SIZE_BYTES",
            config.mmap_size_bytes,
            0,
            1024 * MIB,
        )?;
        config.busy_timeout_millis = env_u64_range(
            "SQLITE_BUSY_TIMEOUT_MILLIS",
            config.busy_timeout_millis,
            100,
            60_000,
        )?;
        config.acquire_timeout_millis = env_u64_range(
            "SQLITE_ACQUIRE_TIMEOUT_MILLIS",
            config.acquire_timeout_millis,
            100,
            60_000,
        )?;
        config.wal_autocheckpoint_pages = env_u32(
            "SQLITE_WAL_AUTOCHECKPOINT_PAGES",
            config.wal_autocheckpoint_pages,
            100,
            100_000,
        )?;
        config.journal_size_limit_bytes = env_u64_range(
            "SQLITE_JOURNAL_SIZE_LIMIT_BYTES",
            config.journal_size_limit_bytes,
            16 * MIB,
            4 * 1024 * MIB,
        )?;
        config.writer_queue_max_depth = env_u32(
            "SQLITE_WRITER_QUEUE_MAX_DEPTH",
            config.writer_queue_max_depth,
            1,
            1024,
        )?;
        config.writer_queue_max_bytes = env_u64_range(
            "SQLITE_WRITER_QUEUE_MAX_BYTES",
            config.writer_queue_max_bytes,
            MIB,
            1024 * MIB,
        )?;
        Ok(config)
    }

    fn for_profile(profile: &str) -> Self {
        match profile {
            "nas-n100-4g" | "n100" => Self {
                profile: profile.to_string(),
                max_connections: 5,
                cache_kib_per_connection: 24 * 1024,
                mmap_size_bytes: 64 * MIB,
                busy_timeout_millis: 10_000,
                acquire_timeout_millis: 10_000,
                wal_autocheckpoint_pages: 4_000,
                journal_size_limit_bytes: 256 * MIB,
                writer_queue_max_depth: 32,
                writer_queue_max_bytes: 64 * MIB,
            },
            _ => Self {
                profile: profile.to_string(),
                max_connections: 8,
                cache_kib_per_connection: 32 * 1024,
                mmap_size_bytes: 128 * MIB,
                busy_timeout_millis: 30_000,
                acquire_timeout_millis: 30_000,
                wal_autocheckpoint_pages: 4_000,
                journal_size_limit_bytes: 256 * MIB,
                writer_queue_max_depth: 128,
                writer_queue_max_bytes: 256 * MIB,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SqliteRuntimeSnapshot {
    pub config: SqliteRuntimeConfig,
    pub pool_size: u32,
    pub idle_connections: usize,
    /// Connections currently checked out by SQLx.  This is a point-in-time
    /// gauge (`pool_size - idle_connections`) and does not claim that every
    /// SQL statement has been routed through an application wrapper.
    pub active_connections: u32,
    pub pool_saturated: bool,
    /// These counters are populated by SQLx's `before_acquire` callback, so
    /// they include implicit acquires performed by `query(...).fetch_*` as
    /// well as explicit transactions.  `idle_*` measures how long a checked
    /// out connection had been idle before reuse; it is intentionally not
    /// mislabeled as queue wait time.
    pub pool_checkout_samples: u64,
    pub pool_checkout_idle_total_micros: u64,
    pub pool_checkout_idle_max_micros: u64,
    pub pool_connections_opened: u64,
    pub pool_tracked_acquire_samples: u64,
    pub pool_tracked_acquire_wait_total_micros: u64,
    pub pool_tracked_acquire_wait_max_micros: u64,
    /// Errors observed by the explicit tracked-transaction helper.  Legacy
    /// direct pool calls remain outside this counter until they are migrated.
    pub pool_acquire_timeouts: u64,
    pub pool_acquire_errors: u64,
    pub sqlite_busy_errors: u64,
    pub read_snapshot: ReadSnapshotRuntime,
    pub database_bytes: Option<u64>,
    pub wal_bytes: Option<u64>,
    pub shm_bytes: Option<u64>,
    pub query_planner_optimize_count: u64,
    pub query_planner_last_optimized_at: Option<String>,
    pub query_planner_last_error: Option<String>,
    pub write_gate: DbWriteSnapshot,
    pub wal_checkpoint: WalCheckpointSnapshot,
}

/// Runtime evidence for explicitly tracked read transactions.  This is not a
/// claim that every single SQL statement has a transaction-level snapshot:
/// one-shot pool queries and legacy transactions remain outside these fields
/// until their call sites migrate to `begin_tracked_read_transaction`.
#[derive(Debug, Clone, Serialize, Default)]
pub struct ReadSnapshotRuntime {
    pub active: usize,
    pub samples: u64,
    pub completed: u64,
    pub hold_total_micros: u64,
    pub hold_max_micros: u64,
    pub oldest_active_micros: u64,
    pub implicit_rollbacks: u64,
}

/// Runtime evidence for the explicit single-writer gate used by Catalog v2
/// and other bounded maintenance paths.  The legacy code still has a few
/// direct SQL writes, so this is deliberately reported as a gate rather than
/// claiming that every SQLite statement has already migrated to one actor.
#[derive(Debug, Clone, Serialize, Default)]
pub struct DbWriteSnapshot {
    pub queue_depth: usize,
    pub queue_bytes: u64,
    pub queue_max_depth: u32,
    pub queue_max_bytes: u64,
    pub active: usize,
    pub active_bytes: u64,
    pub acquire_samples: u64,
    pub acquire_wait_total_micros: u64,
    pub acquire_wait_max_micros: u64,
    pub completed: u64,
    pub hold_total_micros: u64,
    pub hold_max_micros: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct WalCheckpointSnapshot {
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
    pub last_duration_micros: u64,
    /// SQLite's `busy` result from the most recent passive checkpoint.  A
    /// non-zero value means a reader prevented a complete checkpoint and is
    /// useful evidence of a long-lived read snapshot on a NAS.
    pub last_busy_pages: i64,
    pub last_log_pages: i64,
    pub last_checkpointed_pages: i64,
    pub last_error: Option<String>,
}

#[derive(Debug, Default)]
struct QueryPlannerState {
    optimize_count: u64,
    last_optimized_at: Option<String>,
    last_error: Option<String>,
}

#[derive(Debug, Default)]
struct SqlitePoolMetrics {
    checkout_samples: AtomicU64,
    checkout_idle_total_micros: AtomicU64,
    checkout_idle_max_micros: AtomicU64,
    connections_opened: AtomicU64,
    tracked_acquire_samples: AtomicU64,
    tracked_acquire_wait_total_micros: AtomicU64,
    tracked_acquire_wait_max_micros: AtomicU64,
    acquire_timeouts: AtomicU64,
    acquire_errors: AtomicU64,
    sqlite_busy_errors: AtomicU64,
}

#[derive(Debug, Default)]
struct SqliteReadSnapshotMetrics {
    active: AtomicUsize,
    samples: AtomicU64,
    completed: AtomicU64,
    hold_total_micros: AtomicU64,
    hold_max_micros: AtomicU64,
    implicit_rollbacks: AtomicU64,
    next_id: AtomicU64,
    active_started_at: Mutex<BTreeMap<u64, Instant>>,
}

/// RAII wrapper for a read transaction whose lifetime is visible in the
/// SQLite health snapshot.  The wrapper dereferences to the underlying
/// `SqliteConnection`, matching SQLx 0.8's executor contract while retaining
/// the native transaction for the commit/rollback boundary.
pub struct TrackedReadTransaction {
    inner: Option<Transaction<'static, Sqlite>>,
    metrics: Arc<SqliteReadSnapshotMetrics>,
    id: u64,
    started_at: Instant,
    finished: bool,
}

impl TrackedReadTransaction {
    fn new(inner: Transaction<'static, Sqlite>, metrics: Arc<SqliteReadSnapshotMetrics>) -> Self {
        let id = metrics.next_id.fetch_add(1, Ordering::Relaxed);
        let started_at = Instant::now();
        metrics.active.fetch_add(1, Ordering::AcqRel);
        metrics.samples.fetch_add(1, Ordering::Relaxed);
        metrics
            .active_started_at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(id, started_at);
        Self {
            inner: Some(inner),
            metrics,
            id,
            started_at,
            finished: false,
        }
    }

    /// Expose the checked-out SQLite connection for diagnostic helpers that
    /// need SQLx's concrete `Executor` implementation.  The transaction
    /// remains the owner of the connection and its tracked lifetime.
    pub fn connection(&mut self) -> &mut SqliteConnection {
        self
    }

    fn finish(&mut self, implicit_rollback: bool) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.metrics.active.fetch_sub(1, Ordering::AcqRel);
        self.metrics.completed.fetch_add(1, Ordering::Relaxed);
        let hold_micros = self.started_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
        self.metrics
            .hold_total_micros
            .fetch_add(hold_micros, Ordering::Relaxed);
        self.metrics
            .hold_max_micros
            .fetch_max(hold_micros, Ordering::Relaxed);
        if implicit_rollback {
            self.metrics
                .implicit_rollbacks
                .fetch_add(1, Ordering::Relaxed);
        }
        self.metrics
            .active_started_at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.id);
    }

    pub async fn commit(mut self) -> std::result::Result<(), sqlx::Error> {
        let inner = self
            .inner
            .take()
            .expect("tracked read transaction already finished");
        let result = inner.commit().await;
        self.finish(result.is_err());
        result
    }

    pub async fn rollback(mut self) -> std::result::Result<(), sqlx::Error> {
        let inner = self
            .inner
            .take()
            .expect("tracked read transaction already finished");
        let result = inner.rollback().await;
        self.finish(result.is_err());
        result
    }
}

impl SqliteReadSnapshotMetrics {
    fn snapshot(&self) -> ReadSnapshotRuntime {
        let active = self.active.load(Ordering::Acquire);
        let now = Instant::now();
        let oldest_active_micros = self
            .active_started_at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .map(|started_at| {
                now.duration_since(*started_at)
                    .as_micros()
                    .min(u64::MAX as u128) as u64
            })
            .max()
            .unwrap_or(0);
        ReadSnapshotRuntime {
            active,
            samples: self.samples.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            hold_total_micros: self.hold_total_micros.load(Ordering::Relaxed),
            hold_max_micros: self.hold_max_micros.load(Ordering::Relaxed),
            oldest_active_micros,
            implicit_rollbacks: self.implicit_rollbacks.load(Ordering::Relaxed),
        }
    }
}

impl Deref for TrackedReadTransaction {
    type Target = SqliteConnection;

    fn deref(&self) -> &Self::Target {
        let inner = self
            .inner
            .as_ref()
            .expect("tracked read transaction already finished");
        inner
    }
}

impl DerefMut for TrackedReadTransaction {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let inner = self
            .inner
            .as_mut()
            .expect("tracked read transaction already finished");
        inner
    }
}

impl Drop for TrackedReadTransaction {
    fn drop(&mut self) {
        // A cancelled future, early error, or panic still releases the SQLx
        // transaction and must close the observation interval.  Count it as
        // an implicit rollback so a Gate can distinguish clean commits from
        // abandoned/failed read paths.
        self.finish(true);
    }
}

#[derive(Debug, Default)]
struct DbWriteMetrics {
    queue_depth: AtomicUsize,
    queue_bytes: AtomicU64,
    active: AtomicUsize,
    active_bytes: AtomicU64,
    acquire_samples: AtomicU64,
    acquire_wait_total_micros: AtomicU64,
    acquire_wait_max_micros: AtomicU64,
    completed: AtomicU64,
    hold_total_micros: AtomicU64,
    hold_max_micros: AtomicU64,
}

#[derive(Debug)]
struct DbWriteGate {
    semaphore: Arc<Semaphore>,
    metrics: Arc<DbWriteMetrics>,
    admission: Mutex<()>,
    max_depth: u32,
    max_bytes: u64,
}

impl DbWriteGate {
    fn new(max_depth: u32, max_bytes: u64) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(1)),
            metrics: Arc::new(DbWriteMetrics::default()),
            admission: Mutex::new(()),
            max_depth: max_depth.max(1),
            max_bytes: max_bytes.max(MIB),
        }
    }
}

pub struct DbWriteGuard {
    _permit: OwnedSemaphorePermit,
    metrics: Arc<DbWriteMetrics>,
    started_at: Instant,
    estimated_bytes: u64,
}

struct QueuedWriteReservation<'a> {
    gate: &'a DbWriteGate,
    estimated_bytes: u64,
    armed: bool,
}

impl QueuedWriteReservation<'_> {
    fn disarm(&mut self) {
        self.armed = false;
        self.gate.metrics.queue_depth.fetch_sub(1, Ordering::AcqRel);
        self.gate
            .metrics
            .queue_bytes
            .fetch_sub(self.estimated_bytes, Ordering::AcqRel);
    }
}

impl Drop for QueuedWriteReservation<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.gate.metrics.queue_depth.fetch_sub(1, Ordering::AcqRel);
            self.gate
                .metrics
                .queue_bytes
                .fetch_sub(self.estimated_bytes, Ordering::AcqRel);
        }
    }
}

impl Drop for DbWriteGuard {
    fn drop(&mut self) {
        self.metrics.active.fetch_sub(1, Ordering::AcqRel);
        self.metrics
            .active_bytes
            .fetch_sub(self.estimated_bytes, Ordering::AcqRel);
        let hold_micros = self.started_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
        self.metrics.completed.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .hold_total_micros
            .fetch_add(hold_micros, Ordering::Relaxed);
        self.metrics
            .hold_max_micros
            .fetch_max(hold_micros, Ordering::Relaxed);
    }
}

impl DbWriteGate {
    async fn acquire(&self, estimated_bytes: u64) -> Result<DbWriteGuard> {
        let queued_at = Instant::now();
        {
            let _admission = self
                .admission
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let depth = self.metrics.queue_depth.load(Ordering::Acquire);
            let bytes = self.metrics.queue_bytes.load(Ordering::Acquire);
            if depth >= self.max_depth as usize
                || estimated_bytes > self.max_bytes
                || bytes > self.max_bytes.saturating_sub(estimated_bytes)
            {
                return Err(AppError::Overloaded {
                    message: "database writer queue is full".to_string(),
                    retry_after_seconds: 1,
                });
            }
            self.metrics.queue_depth.fetch_add(1, Ordering::AcqRel);
            self.metrics
                .queue_bytes
                .fetch_add(estimated_bytes, Ordering::AcqRel);
        }
        let mut reservation = QueuedWriteReservation {
            gate: self,
            estimated_bytes,
            armed: true,
        };
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| AppError::Other("database writer gate is closed".to_string()))?;
        reservation.disarm();
        let wait_micros = queued_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
        self.metrics.acquire_samples.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .acquire_wait_total_micros
            .fetch_add(wait_micros, Ordering::Relaxed);
        self.metrics
            .acquire_wait_max_micros
            .fetch_max(wait_micros, Ordering::Relaxed);
        self.metrics.active.fetch_add(1, Ordering::AcqRel);
        self.metrics
            .active_bytes
            .fetch_add(estimated_bytes, Ordering::AcqRel);
        Ok(DbWriteGuard {
            _permit: permit,
            metrics: self.metrics.clone(),
            started_at: Instant::now(),
            estimated_bytes,
        })
    }

    fn snapshot(&self) -> DbWriteSnapshot {
        DbWriteSnapshot {
            queue_depth: self.metrics.queue_depth.load(Ordering::Acquire),
            queue_bytes: self.metrics.queue_bytes.load(Ordering::Acquire),
            queue_max_depth: self.max_depth,
            queue_max_bytes: self.max_bytes,
            active: self.metrics.active.load(Ordering::Acquire),
            active_bytes: self.metrics.active_bytes.load(Ordering::Acquire),
            acquire_samples: self.metrics.acquire_samples.load(Ordering::Relaxed),
            acquire_wait_total_micros: self
                .metrics
                .acquire_wait_total_micros
                .load(Ordering::Relaxed),
            acquire_wait_max_micros: self.metrics.acquire_wait_max_micros.load(Ordering::Relaxed),
            completed: self.metrics.completed.load(Ordering::Relaxed),
            hold_total_micros: self.metrics.hold_total_micros.load(Ordering::Relaxed),
            hold_max_micros: self.metrics.hold_max_micros.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default)]
struct WalCheckpointState {
    snapshot: WalCheckpointSnapshot,
}

fn env_u32(name: &str, fallback: u32, min: u32, max: u32) -> Result<u32> {
    let Some(raw) = env::var(name).ok() else {
        return Ok(fallback);
    };
    let value = raw.parse::<u32>().map_err(|_| {
        AppError::Other(format!("{name} must be an integer between {min} and {max}"))
    })?;
    if !(min..=max).contains(&value) {
        return Err(AppError::Other(format!(
            "{name} must be between {min} and {max}"
        )));
    }
    Ok(value)
}

fn env_u64_range(name: &str, fallback: u64, min: u64, max: u64) -> Result<u64> {
    let Some(raw) = env::var(name).ok() else {
        return Ok(fallback);
    };
    let value = raw.parse::<u64>().map_err(|_| {
        AppError::Other(format!("{name} must be an integer between {min} and {max}"))
    })?;
    if !(min..=max).contains(&value) {
        return Err(AppError::Other(format!(
            "{name} must be between {min} and {max}"
        )));
    }
    Ok(value)
}

fn sqlite_database_path(url: &str) -> Option<PathBuf> {
    let raw = url
        .strip_prefix("sqlite://")
        .or_else(|| url.strip_prefix("sqlite:"))?
        .split('?')
        .next()
        .unwrap_or_default();
    if raw.is_empty() || raw == ":memory:" {
        None
    } else {
        Some(PathBuf::from(raw))
    }
}

fn sqlite_sidecar_path(path: &std::path::Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

#[derive(Debug, Serialize, Deserialize)]
struct LibraryCursor {
    next_id: i64,
    /// New cursors carry the complete ordering key used by the first page.
    /// `None` keeps already-issued id-only cursors readable during the
    /// compatibility window.
    #[serde(default)]
    next_updated_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct ScannerAssetInput {
    pub path: String,
    pub mime: String,
    pub role: String,
    pub variant: Option<String>,
    pub position: Option<i64>,
    pub size: Option<i64>,
    pub meta: Value,
}

#[derive(Debug)]
pub struct ScannerTagInput {
    pub namespace: String,
    pub key: String,
    pub label: String,
    pub source: String,
}

#[derive(Debug)]
pub struct ScannerExternalIdInput {
    pub source: String,
    pub external_id: String,
    pub token: Option<String>,
    pub url: Option<String>,
}

/// Complete legacy-scanner facts for one work.  Small works can commit all
/// facts under one scanner lease transaction; large works continue to use the
/// existing bounded asset transactions so SQLite's writer lock is not held
/// while staging tens of thousands of rows.
#[derive(Debug)]
pub struct ScannerWorkSnapshot {
    pub kind: String,
    pub title: String,
    pub source_path: Option<String>,
    pub category: Option<String>,
    pub description: Option<String>,
    pub rating: Option<f64>,
    pub meta: Value,
    pub fingerprint: String,
    pub assets: Vec<ScannerAssetInput>,
    pub tags: Vec<ScannerTagInput>,
    pub external_ids: Vec<ScannerExternalIdInput>,
}

#[derive(Debug)]
pub struct EnrichmentTagInput {
    pub namespace: String,
    pub key: String,
    pub label: String,
    pub source: String,
}

#[derive(Debug)]
pub struct EnrichmentExternalIdInput {
    pub source: String,
    pub external_id: String,
    pub token: Option<String>,
    pub url: Option<String>,
}

#[derive(Debug)]
pub struct ScannerEnrichmentInput {
    pub title: Option<String>,
    pub category: Option<String>,
    pub description: Option<String>,
    pub rating: Option<f64>,
    pub meta: Value,
    pub tags: Vec<EnrichmentTagInput>,
    pub external_ids: Vec<EnrichmentExternalIdInput>,
}

#[derive(Debug)]
pub struct ProgressWrite {
    pub accepted: bool,
    pub progress: f64,
    pub position: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RevisionSnapshot {
    pub catalog_revision: i64,
    pub activity_revision: i64,
    pub search_revision: i64,
}

/// The revision fence used by a scan/search boundary.  Catalog and Search
/// revisions are read from one short SQLite snapshot so a scan cannot compare
/// a catalog value from one pool checkout with a search-source value from a
/// later checkout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RevisionFenceSnapshot {
    pub catalog_revision: i64,
    pub activity_revision: i64,
    pub search_revision: i64,
}

#[derive(Clone)]
pub struct Db {
    pool: Pool<Sqlite>,
    runtime: SqliteRuntimeConfig,
    database_path: Option<PathBuf>,
    pool_metrics: Arc<SqlitePoolMetrics>,
    read_snapshot_metrics: Arc<SqliteReadSnapshotMetrics>,
    catalog_revision_deltas: Arc<Mutex<CatalogRevisionDeltaLog>>,
    query_planner_state: Arc<Mutex<QueryPlannerState>>,
    write_gate: Arc<DbWriteGate>,
    wal_checkpoint_state: Arc<Mutex<WalCheckpointState>>,
}

impl Db {
    pub async fn connect(url: &str) -> Result<Self> {
        let runtime = SqliteRuntimeConfig::from_env()?;
        let pool_metrics = Arc::new(SqlitePoolMetrics::default());
        let read_snapshot_metrics = Arc::new(SqliteReadSnapshotMetrics::default());
        let before_acquire_metrics = pool_metrics.clone();
        let after_connect_metrics = pool_metrics.clone();
        let options = SqliteConnectOptions::from_str(url)
            .map_err(|e| AppError::Other(e.to_string()))?
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_millis(runtime.busy_timeout_millis))
            .pragma(
                "cache_size",
                format!("-{}", runtime.cache_kib_per_connection),
            )
            .pragma("mmap_size", runtime.mmap_size_bytes.to_string())
            .pragma(
                "wal_autocheckpoint",
                runtime.wal_autocheckpoint_pages.to_string(),
            )
            .pragma(
                "journal_size_limit",
                runtime.journal_size_limit_bytes.to_string(),
            );
        let pool = SqlitePoolOptions::new()
            .max_connections(runtime.max_connections)
            .acquire_timeout(Duration::from_millis(runtime.acquire_timeout_millis))
            .before_acquire(move |_connection, metadata| {
                let metrics = before_acquire_metrics.clone();
                Box::pin(async move {
                    let idle_micros = metadata.idle_for.as_micros().min(u64::MAX as u128) as u64;
                    metrics.checkout_samples.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .checkout_idle_total_micros
                        .fetch_add(idle_micros, Ordering::Relaxed);
                    metrics
                        .checkout_idle_max_micros
                        .fetch_max(idle_micros, Ordering::Relaxed);
                    Ok(true)
                })
            })
            .after_connect(move |_connection, _metadata| {
                let metrics = after_connect_metrics.clone();
                Box::pin(async move {
                    metrics.connections_opened.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                })
            })
            .connect_with(options)
            .await?;
        let write_gate = Arc::new(DbWriteGate::new(
            runtime.writer_queue_max_depth,
            runtime.writer_queue_max_bytes,
        ));
        Ok(Self {
            pool,
            runtime,
            database_path: sqlite_database_path(url),
            pool_metrics,
            read_snapshot_metrics,
            catalog_revision_deltas: Arc::new(Mutex::new(CatalogRevisionDeltaLog::default())),
            query_planner_state: Arc::new(Mutex::new(QueryPlannerState::default())),
            write_gate,
            wal_checkpoint_state: Arc::new(Mutex::new(WalCheckpointState::default())),
        })
    }

    pub fn pool(&self) -> &Pool<Sqlite> {
        &self.pool
    }

    /// Acquire the bounded, observable single-writer gate for a transaction.
    ///
    /// This is intentionally separate from `pool()` while the legacy writer
    /// is being retired.  New authoritative paths must hold this guard from
    /// transaction begin through commit; the queue depth and wait time then
    /// become directly visible in `/api/health/resources`.
    pub async fn acquire_write_slot(&self, estimated_bytes: u64) -> Result<DbWriteGuard> {
        self.write_gate.acquire(estimated_bytes).await
    }

    pub fn write_snapshot(&self) -> DbWriteSnapshot {
        self.write_gate.snapshot()
    }

    pub(crate) fn record_catalog_work_revision_delta(
        &self,
        from_revision: i64,
        to_revision: i64,
        work_id: i64,
    ) {
        if from_revision < 0 || to_revision <= from_revision || work_id <= 0 {
            return;
        }
        let mut log = self
            .catalog_revision_deltas
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        log.by_from_revision.insert(
            from_revision,
            CatalogRevisionDelta {
                to_revision,
                work_id,
            },
        );
        while log.by_from_revision.len() > CATALOG_REVISION_DELTA_LOG_MAX {
            log.by_from_revision.pop_first();
            log.evictions = log.evictions.saturating_add(1);
        }
    }

    pub(crate) fn catalog_work_ids_for_revision_range(
        &self,
        from_revision: i64,
        to_revision: i64,
        max_deltas: usize,
    ) -> Option<Vec<i64>> {
        if from_revision == to_revision {
            return Some(Vec::new());
        }
        if from_revision < 0 || to_revision < from_revision || max_deltas == 0 {
            return None;
        }
        let log = self
            .catalog_revision_deltas
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut revision = from_revision;
        let mut work_ids = Vec::new();
        while revision < to_revision {
            if work_ids.len() >= max_deltas {
                return None;
            }
            let delta = log.by_from_revision.get(&revision)?;
            if delta.to_revision <= revision || delta.to_revision > to_revision {
                return None;
            }
            work_ids.push(delta.work_id);
            revision = delta.to_revision;
        }
        (revision == to_revision).then_some(work_ids)
    }

    pub async fn runtime_snapshot(&self) -> SqliteRuntimeSnapshot {
        let file_size = |path: PathBuf| async move {
            tokio::fs::metadata(path)
                .await
                .ok()
                .map(|metadata| metadata.len())
        };
        let (database_bytes, wal_bytes, shm_bytes) = if let Some(path) = &self.database_path {
            tokio::join!(
                file_size(path.clone()),
                file_size(sqlite_sidecar_path(path, "-wal")),
                file_size(sqlite_sidecar_path(path, "-shm")),
            )
        } else {
            (None, None, None)
        };
        let query_planner_state = self
            .query_planner_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let wal_checkpoint = self
            .wal_checkpoint_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .snapshot
            .clone();
        let pool_size = self.pool.size();
        let idle_connections = self.pool.num_idle();
        let active_connections = pool_size.saturating_sub(idle_connections as u32);
        SqliteRuntimeSnapshot {
            config: self.runtime.clone(),
            pool_size,
            idle_connections,
            active_connections,
            // A pool that has opened its configured number of connections is
            // not saturated when all of them are idle.  Report saturation
            // only when no idle connection is available and a new checkout
            // would have to wait for an active one to be returned.
            pool_saturated: pool_size >= self.runtime.max_connections && idle_connections == 0,
            pool_checkout_samples: self.pool_metrics.checkout_samples.load(Ordering::Relaxed),
            pool_checkout_idle_total_micros: self
                .pool_metrics
                .checkout_idle_total_micros
                .load(Ordering::Relaxed),
            pool_checkout_idle_max_micros: self
                .pool_metrics
                .checkout_idle_max_micros
                .load(Ordering::Relaxed),
            pool_connections_opened: self.pool_metrics.connections_opened.load(Ordering::Relaxed),
            pool_tracked_acquire_samples: self
                .pool_metrics
                .tracked_acquire_samples
                .load(Ordering::Relaxed),
            pool_tracked_acquire_wait_total_micros: self
                .pool_metrics
                .tracked_acquire_wait_total_micros
                .load(Ordering::Relaxed),
            pool_tracked_acquire_wait_max_micros: self
                .pool_metrics
                .tracked_acquire_wait_max_micros
                .load(Ordering::Relaxed),
            pool_acquire_timeouts: self.pool_metrics.acquire_timeouts.load(Ordering::Relaxed),
            pool_acquire_errors: self.pool_metrics.acquire_errors.load(Ordering::Relaxed),
            sqlite_busy_errors: self.pool_metrics.sqlite_busy_errors.load(Ordering::Relaxed),
            read_snapshot: self.read_snapshot_metrics.snapshot(),
            database_bytes,
            wal_bytes,
            shm_bytes,
            query_planner_optimize_count: query_planner_state.optimize_count,
            query_planner_last_optimized_at: query_planner_state.last_optimized_at.clone(),
            query_planner_last_error: query_planner_state.last_error.clone(),
            write_gate: self.write_snapshot(),
            wal_checkpoint,
        }
    }

    /// Begin a transaction while recording the SQLx pool checkout path.
    ///
    /// The returned transaction owns its pool connection just like
    /// `Pool::begin()`.  This helper only measures the explicit call sites
    /// that have migrated to it; implicit query acquires are counted by the
    /// pool's `before_acquire` callback above.
    pub async fn begin_tracked_transaction(&self) -> Result<Transaction<'static, Sqlite>> {
        let started = Instant::now();
        let connection = match self.pool.acquire().await {
            Ok(connection) => connection,
            Err(error) => {
                self.record_pool_acquire_error(&error);
                return Err(error.into());
            }
        };
        let acquire_micros = started.elapsed().as_micros().min(u64::MAX as u128) as u64;
        self.pool_metrics
            .tracked_acquire_samples
            .fetch_add(1, Ordering::Relaxed);
        self.pool_metrics
            .tracked_acquire_wait_total_micros
            .fetch_add(acquire_micros, Ordering::Relaxed);
        self.pool_metrics
            .tracked_acquire_wait_max_micros
            .fetch_max(acquire_micros, Ordering::Relaxed);
        Transaction::begin(MaybePoolConnection::PoolConnection(connection), None)
            .await
            .map_err(|error| {
                self.record_sqlite_error(&error);
                error.into()
            })
    }

    /// Begin a read transaction and retain an explicit lifetime record until
    /// commit, rollback, cancellation, or drop.  Only call sites that truly
    /// need a consistent multi-query snapshot should use this helper; a
    /// one-shot pool query should remain a one-shot query and is intentionally
    /// outside the snapshot counters.
    pub async fn begin_tracked_read_transaction(&self) -> Result<TrackedReadTransaction> {
        let transaction = self.begin_tracked_transaction().await?;
        Ok(TrackedReadTransaction::new(
            transaction,
            self.read_snapshot_metrics.clone(),
        ))
    }

    fn record_pool_acquire_error(&self, error: &sqlx::Error) {
        if matches!(error, sqlx::Error::PoolTimedOut) {
            self.pool_metrics
                .acquire_timeouts
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.pool_metrics
                .acquire_errors
                .fetch_add(1, Ordering::Relaxed);
        }
        self.record_sqlite_error(error);
    }

    fn record_sqlite_error(&self, error: &sqlx::Error) {
        let text = error.to_string().to_ascii_lowercase();
        if text.contains("database is locked")
            || text.contains("database is busy")
            || text.contains("sqlite_busy")
        {
            self.pool_metrics
                .sqlite_busy_errors
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Run a non-blocking SQLite WAL checkpoint and retain its timing/result
    /// as runtime evidence.  `PASSIVE` never waits for readers or evicts a
    /// reader snapshot, making this safe for a health/maintenance probe.
    pub async fn checkpoint_wal(&self) -> Result<WalCheckpointSnapshot> {
        // PASSIVE avoids waiting on readers, but the checkpoint can still
        // advance SQLite's WAL state. Serialize it with other writers so the
        // health probe cannot race a catalog transaction and inflate busy/WAL
        // measurements on the N100.
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let started = Instant::now();
        let result = sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
            .fetch_one(&self.pool)
            .await;
        let duration_micros = started.elapsed().as_micros().min(u64::MAX as u128) as u64;
        let mut state = self
            .wal_checkpoint_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.snapshot.attempts = state.snapshot.attempts.saturating_add(1);
        state.snapshot.last_duration_micros = duration_micros;
        match result {
            Ok(row) => {
                state.snapshot.successes = state.snapshot.successes.saturating_add(1);
                state.snapshot.last_busy_pages = row.try_get("busy")?;
                state.snapshot.last_log_pages = row.try_get("log")?;
                state.snapshot.last_checkpointed_pages = row.try_get("checkpointed")?;
                state.snapshot.last_error = None;
            }
            Err(error) => {
                state.snapshot.failures = state.snapshot.failures.saturating_add(1);
                state.snapshot.last_error = Some(error.to_string().chars().take(2048).collect());
                return Err(error.into());
            }
        }
        Ok(state.snapshot.clone())
    }

    /// Give SQLite a bounded opportunity to refresh query-planner statistics.
    ///
    /// `PRAGMA optimize` is intentionally kept separate from migrations and
    /// bulk writers.  Callers can schedule it during an idle maintenance
    /// window, so a low-power NAS never pays an ANALYZE cost while an
    /// interactive request or scanner transaction is holding the writer.
    pub async fn optimize_query_planner(&self) -> Result<()> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let result = sqlx::query("PRAGMA optimize")
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(AppError::from);
        let mut state = self
            .query_planner_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &result {
            Ok(()) => {
                state.optimize_count = state.optimize_count.saturating_add(1);
                state.last_optimized_at =
                    Some(Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true));
                state.last_error = None;
            }
            Err(error) => {
                state.last_error = Some(error.to_string().chars().take(2048).collect());
            }
        }
        result
    }

    pub async fn migrate(&self) -> Result<()> {
        {
            let _write_slot = self.acquire_write_slot(64 * 1024).await?;
            migrations::ensure_table(&self.pool).await?;
        }
        migrations::validate_compatible(&self.pool).await?;
        let tables = [
            "PRAGMA journal_mode = WAL",
            "PRAGMA foreign_keys = ON",
            r#"
            CREATE TABLE IF NOT EXISTS works (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                title TEXT NOT NULL,
                subtitle TEXT,
                category TEXT,
                description TEXT,
                rating REAL,
                progress REAL NOT NULL DEFAULT 0,
                source_path TEXT,
                cover_asset_id INTEGER,
                meta_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                UNIQUE(kind, source_path)
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
                path TEXT NOT NULL,
                mime TEXT NOT NULL,
                role TEXT NOT NULL,
                variant TEXT NOT NULL DEFAULT '',
                position INTEGER NOT NULL DEFAULT -1,
                size INTEGER,
                meta_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                UNIQUE(work_id, path, role, variant)
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                namespace TEXT NOT NULL,
                key TEXT NOT NULL,
                label TEXT NOT NULL,
                translated_label TEXT,
                translated_namespace TEXT,
                source TEXT NOT NULL DEFAULT 'local',
                intro TEXT,
                links TEXT,
                count INTEGER NOT NULL DEFAULT 0,
                UNIQUE(namespace, key)
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS work_tags (
                work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
                tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
                PRIMARY KEY(work_id, tag_id)
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS external_ids (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
                source TEXT NOT NULL,
                external_id TEXT NOT NULL,
                token TEXT,
                url TEXT,
                UNIQUE(work_id, source, external_id)
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                attempts INTEGER NOT NULL DEFAULT 0,
                retry_at TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS reading_history (
                work_id INTEGER PRIMARY KEY REFERENCES works(id) ON DELETE CASCADE,
                progress REAL NOT NULL DEFAULT 0,
                position TEXT,
                last_opened_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                update_token INTEGER NOT NULL DEFAULT 0
            )"#,
        ];

        {
            let _write_slot = self.acquire_write_slot(512 * 1024).await?;
            for statement in tables {
                sqlx::query(statement).execute(&self.pool).await?;
            }
        }
        self.ensure_reading_history_update_token().await?;

        self.migrate_asset_identity().await?;

        let scan_tables = [
            r#"
            CREATE TABLE IF NOT EXISTS scanner_works (
                work_id INTEGER PRIMARY KEY REFERENCES works(id) ON DELETE CASCADE,
                scope TEXT NOT NULL,
                seen_token TEXT NOT NULL,
                fingerprint TEXT
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS scanner_assets (
                asset_id INTEGER PRIMARY KEY REFERENCES assets(id) ON DELETE CASCADE,
                work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
                seen_token TEXT NOT NULL
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS work_tag_sources (
                work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
                tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
                owner TEXT NOT NULL,
                seen_token TEXT,
                PRIMARY KEY(work_id, tag_id, owner)
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS scanner_locks (
                name TEXT PRIMARY KEY,
                token TEXT NOT NULL,
                acquired_at TEXT NOT NULL
            )"#,
        ];
        {
            let _write_slot = self.acquire_write_slot(256 * 1024).await?;
            for statement in scan_tables {
                sqlx::query(statement).execute(&self.pool).await?;
            }
        }

        self.migrate_scanner_ownership().await?;
        {
            let _write_slot = self.acquire_write_slot(64 * 1024).await?;
            sqlx::query("DROP INDEX IF EXISTS idx_jobs_one_active_maintenance")
                .execute(&self.pool)
                .await?;
        }
        self.coalesce_existing_jobs().await?;

        let indexes = [
            "CREATE INDEX IF NOT EXISTS idx_assets_work ON assets(work_id)",
            "CREATE INDEX IF NOT EXISTS idx_assets_work_role_id ON assets(work_id, role, id DESC)",
            "CREATE INDEX IF NOT EXISTS idx_assets_role ON assets(role, work_id, position)",
            "CREATE INDEX IF NOT EXISTS idx_assets_gallery_position ON assets(work_id, role, position, id)",
            // Catalog asset pages use the same NULL-last ordering for every
            // role.  Keep the normalized position in the index so keyset
            // continuation can seek directly instead of sorting all rows for
            // large EPUB/audio works.
            "CREATE INDEX IF NOT EXISTS idx_assets_work_role_position_keyset ON assets(work_id, role, COALESCE(position, 9223372036854775807), id)",
            "CREATE INDEX IF NOT EXISTS idx_assets_work_audio_role_keyset ON assets(work_id, role, COALESCE(position, 9223372036854775807), id) WHERE role = 'track'",
            "CREATE INDEX IF NOT EXISTS idx_assets_work_audio_mime_keyset ON assets(work_id, role, COALESCE(position, 9223372036854775807), id) WHERE role <> 'track' AND lower(mime) LIKE 'audio/%'",
            // Audio queue pages accept legacy rows whose role was not
            // normalized but whose MIME still identifies a playable track.
            // Keep those compatibility branches indexed without adding the
            // full gallery/novel asset population to another global index.
            "CREATE INDEX IF NOT EXISTS idx_assets_work_audio_role ON assets(work_id, role, position, id) WHERE role = 'track'",
            "CREATE INDEX IF NOT EXISTS idx_assets_work_audio_mime_lower ON assets(work_id, lower(mime), position, id) WHERE role <> 'track' AND lower(mime) LIKE 'audio/%'",
            "CREATE INDEX IF NOT EXISTS idx_works_kind_updated ON works(kind, updated_at DESC, id DESC)",
            "CREATE INDEX IF NOT EXISTS idx_works_updated ON works(updated_at DESC, id DESC)",
            "CREATE INDEX IF NOT EXISTS idx_works_source_path ON works(source_path)",
            "CREATE INDEX IF NOT EXISTS idx_work_tags_tag ON work_tags(tag_id)",
            "CREATE INDEX IF NOT EXISTS idx_work_tags_work ON work_tags(work_id)",
            "CREATE INDEX IF NOT EXISTS idx_tags_count ON tags(count DESC, namespace, key)",
            "CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, retry_at)",
            "CREATE INDEX IF NOT EXISTS idx_audit_logs_created ON audit_logs(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_history_opened ON reading_history(last_opened_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_scanner_works_scope ON scanner_works(scope, seen_token)",
            "CREATE INDEX IF NOT EXISTS idx_scanner_assets_work ON scanner_assets(work_id, seen_token)",
            "CREATE INDEX IF NOT EXISTS idx_work_tag_sources_work ON work_tag_sources(work_id, owner, seen_token)",
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_one_queued_maintenance
            ON jobs(job_type)
            WHERE status = 'queued'
              AND job_type IN ('scan-library', 'rebuild-search-index')
            "#,
        ];

        {
            let _write_slot = self.acquire_write_slot(512 * 1024).await?;
            for statement in indexes {
                sqlx::query(statement).execute(&self.pool).await?;
            }
        }
        self.prune_audit_logs().await?;
        {
            let _write_slot = self.acquire_write_slot(8 * 1024 * 1024).await?;
            migrations::apply_pending(&self.pool).await?;
        }
        Ok(())
    }

    pub async fn schema_version(&self) -> Result<i64> {
        migrations::current_version(&self.pool).await
    }

    #[cfg(test)]
    pub(crate) async fn revision_snapshot(&self) -> Result<RevisionSnapshot> {
        let (catalog_revision, activity_revision, search_revision) =
            sqlx::query_as::<_, (i64, i64, i64)>(
                r#"
            SELECT catalog.revision, activity.revision, search.revision
            FROM catalog_state AS catalog
            JOIN activity_state AS activity ON activity.singleton = catalog.singleton
            JOIN search_source_state AS search ON search.singleton = catalog.singleton
            WHERE catalog.singleton = 1
            "#,
            )
            .fetch_one(&self.pool)
            .await?;
        Ok(RevisionSnapshot {
            catalog_revision,
            activity_revision,
            search_revision,
        })
    }

    /// Read Catalog, activity, and Search source revisions from one explicit
    /// snapshot.  This is intentionally a separate helper from the legacy
    /// one-counter methods so callers that only need one revision retain the
    /// cheaper single-statement pool query.
    pub(crate) async fn revision_fence_snapshot(&self) -> Result<RevisionFenceSnapshot> {
        let mut transaction = self.begin_tracked_read_transaction().await?;
        let (catalog_revision, activity_revision) = sqlx::query_as::<_, (i64, i64)>(
            r#"
                SELECT catalog.revision, activity.revision
                FROM catalog_state AS catalog
                JOIN activity_state AS activity ON activity.singleton = catalog.singleton
                WHERE catalog.singleton = 1
                "#,
        )
        .fetch_one(&mut *transaction)
        .await?;
        let search_revision = sqlx::query_scalar::<_, i64>(
            "SELECT revision FROM search_source_state WHERE singleton = 1",
        )
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(RevisionFenceSnapshot {
            catalog_revision,
            activity_revision,
            search_revision,
        })
    }

    /// Read both revision counters from an already-open SQLite snapshot.
    /// Catalog pages use this after fetching their rows so the response cannot
    /// combine a page from one content revision with counters from another.
    pub(crate) async fn revision_snapshot_with_connection(
        &self,
        connection: &mut SqliteConnection,
    ) -> Result<RevisionSnapshot> {
        let (catalog_revision, activity_revision, search_revision) =
            sqlx::query_as::<_, (i64, i64, i64)>(
                r#"
                SELECT catalog.revision, activity.revision, search.revision
                FROM catalog_state AS catalog
                JOIN activity_state AS activity ON activity.singleton = catalog.singleton
                JOIN search_source_state AS search ON search.singleton = catalog.singleton
                WHERE catalog.singleton = 1
                "#,
            )
            .fetch_one(&mut *connection)
            .await?;
        Ok(RevisionSnapshot {
            catalog_revision,
            activity_revision,
            search_revision,
        })
    }

    async fn ensure_reading_history_update_token(&self) -> Result<()> {
        let columns = sqlx::query("PRAGMA table_info(reading_history)")
            .fetch_all(&self.pool)
            .await?;
        if columns
            .iter()
            .any(|row| row.get::<String, _>("name") == "update_token")
        {
            return Ok(());
        }
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        sqlx::query(
            "ALTER TABLE reading_history ADD COLUMN update_token INTEGER NOT NULL DEFAULT 0",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn migrate_asset_identity(&self) -> Result<()> {
        let schema = sqlx::query_scalar::<_, String>(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'assets'",
        )
        .fetch_optional(&self.pool)
        .await?
        .unwrap_or_default();
        let compact = schema
            .chars()
            .filter(|ch| !ch.is_whitespace())
            .collect::<String>()
            .to_ascii_lowercase();
        if !compact.contains("unique(work_id,path,role,variant,position)") {
            return Ok(());
        }

        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        sqlx::query(
            r#"
            CREATE TABLE assets_v2 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
                path TEXT NOT NULL,
                mime TEXT NOT NULL,
                role TEXT NOT NULL,
                variant TEXT NOT NULL DEFAULT '',
                position INTEGER NOT NULL DEFAULT -1,
                size INTEGER,
                meta_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                UNIQUE(work_id, path, role, variant)
            )
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            CREATE TEMP TABLE asset_identity_map AS
            SELECT
                candidate.id AS old_id,
                COALESCE(
                    MAX(CASE WHEN keeper.id = work.cover_asset_id THEN keeper.id END),
                    MAX(keeper.id)
                ) AS keep_id
            FROM assets candidate
            JOIN works work ON work.id = candidate.work_id
            JOIN assets keeper
              ON keeper.work_id = candidate.work_id
             AND keeper.path = candidate.path
             AND keeper.role = candidate.role
             AND keeper.variant = candidate.variant
            GROUP BY candidate.id
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO assets_v2
                (id, work_id, path, mime, role, variant, position, size, meta_json, created_at)
            SELECT
                asset.id, asset.work_id, asset.path, asset.mime, asset.role, asset.variant,
                asset.position, asset.size, asset.meta_json, asset.created_at
            FROM assets asset
            JOIN asset_identity_map mapping ON mapping.old_id = asset.id
            WHERE mapping.old_id = mapping.keep_id
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            UPDATE works
            SET cover_asset_id = (
                SELECT keep_id FROM asset_identity_map WHERE old_id = works.cover_asset_id
            )
            WHERE cover_asset_id IS NOT NULL
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query("DROP TABLE assets")
            .execute(&mut *transaction)
            .await?;
        sqlx::query("ALTER TABLE assets_v2 RENAME TO assets")
            .execute(&mut *transaction)
            .await?;
        sqlx::query("DROP TABLE asset_identity_map")
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn migrate_scanner_ownership(&self) -> Result<()> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO scanner_works (work_id, scope, seen_token, fingerprint)
            SELECT id, ('legacy|' || kind), 'legacy', NULL
            FROM works
            WHERE kind IN ('comic', 'novel', 'audio', 'gallery', 'coser-picture')
              AND source_path IS NOT NULL
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO scanner_assets (asset_id, work_id, seen_token)
            SELECT asset.id, asset.work_id, 'legacy'
            FROM assets asset
            JOIN scanner_works scanner ON scanner.work_id = asset.work_id
            WHERE asset.role != 'generated'
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO work_tag_sources (work_id, tag_id, owner, seen_token)
            SELECT work_tag.work_id, work_tag.tag_id, 'scanner', 'legacy'
            FROM work_tags work_tag
            JOIN tags tag ON tag.id = work_tag.tag_id
            JOIN scanner_works scanner ON scanner.work_id = work_tag.work_id
            WHERE tag.source IN (
                'comic-info', 'epub', 'audio-folder', 'gallery-folder',
                'gallery-filename', 'coser-picture-zip', 'qmediasync'
            )
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO work_tag_sources (work_id, tag_id, owner, seen_token)
            SELECT work_tag.work_id, work_tag.tag_id, 'external', NULL
            FROM work_tags work_tag
            JOIN tags tag ON tag.id = work_tag.tag_id
            LEFT JOIN scanner_works scanner ON scanner.work_id = work_tag.work_id
            WHERE scanner.work_id IS NULL
               OR tag.source NOT IN (
                    'comic-info', 'epub', 'audio-folder', 'gallery-folder',
                    'gallery-filename', 'coser-picture-zip', 'qmediasync'
               )
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            DELETE FROM work_tag_sources
            WHERE owner = 'scanner'
              AND seen_token = 'legacy'
              AND tag_id IN (
                  SELECT id FROM tags
                  WHERE source NOT IN (
                      'comic-info', 'epub', 'audio-folder', 'gallery-folder',
                      'gallery-filename', 'coser-picture-zip', 'qmediasync'
                  )
              )
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn coalesce_existing_jobs(&self) -> Result<()> {
        // Migration-time job coalescing is still a SQLite write.  Keep it
        // behind the same bounded gate as runtime writers so startup cannot
        // race an already-running maintenance task when a process is
        // restarted against a shared database.
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        sqlx::query(
            r#"
            WITH active AS (
                SELECT
                    job_type,
                    status,
                    MIN(id) AS keep_id
                FROM jobs
                WHERE status IN ('queued', 'running')
                  AND job_type IN ('scan-library', 'rebuild-search-index')
                GROUP BY job_type, status
            )
            UPDATE jobs
            SET
                status = 'superseded',
                last_error = COALESCE(last_error, 'coalesced during migration'),
                updated_at = ?1
            WHERE status IN ('queued', 'running')
              AND job_type IN ('scan-library', 'rebuild-search-index')
              AND id != (
                  SELECT keep_id FROM active
                  WHERE active.job_type = jobs.job_type AND active.status = jobs.status
              )
            "#,
        )
        .bind(Utc::now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn requeue_interrupted_running_jobs(&self) -> Result<u64> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        // This method is called once, before workers are started. Any persisted
        // scanner lease therefore belongs to the previous process and must not
        // make its requeued scan job fail until the normal lease timeout.
        sqlx::query("DELETE FROM scanner_locks")
            .execute(&mut *transaction)
            .await?;
        let now = Utc::now();
        // A queued singleton is the successor of the interrupted running job.
        // Fold its request into the job that will be requeued before marking the
        // successor superseded, otherwise a restart can silently drop options
        // such as enqueue_enrichment=true.
        sqlx::query(
            r#"
            UPDATE jobs AS running
            SET
                payload_json = json_set(
                    json_patch(
                        running.payload_json,
                        COALESCE((
                            SELECT queued.payload_json
                            FROM jobs AS queued
                            WHERE queued.job_type = running.job_type
                              AND queued.status = 'queued'
                            ORDER BY queued.id
                            LIMIT 1
                        ), '{}')
                    ),
                    '$.enqueue_enrichment',
                    json(CASE
                        WHEN COALESCE(json_extract(running.payload_json, '$.enqueue_enrichment'), 0) != 0
                          OR COALESCE((
                              SELECT json_extract(queued.payload_json, '$.enqueue_enrichment')
                              FROM jobs AS queued
                              WHERE queued.job_type = running.job_type
                                AND queued.status = 'queued'
                              ORDER BY queued.id
                              LIMIT 1
                          ), 0) != 0
                        THEN 'true'
                        ELSE 'false'
                    END)
                ),
                updated_at = ?1
            WHERE running.status = 'running'
              AND running.job_type = 'scan-library'
              AND EXISTS (
                  SELECT 1 FROM jobs AS queued
                  WHERE queued.job_type = running.job_type
                    AND queued.status = 'queued'
              )
            "#,
        )
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            UPDATE jobs AS running
            SET
                payload_json = json_patch(
                    running.payload_json,
                    COALESCE((
                        SELECT queued.payload_json
                        FROM jobs AS queued
                        WHERE queued.job_type = running.job_type
                          AND queued.status = 'queued'
                        ORDER BY queued.id
                        LIMIT 1
                    ), '{}')
                ),
                updated_at = ?1
            WHERE running.status = 'running'
              AND running.job_type = 'rebuild-search-index'
              AND EXISTS (
                  SELECT 1 FROM jobs AS queued
                  WHERE queued.job_type = running.job_type
                    AND queued.status = 'queued'
              )
            "#,
        )
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            UPDATE jobs AS queued
            SET
                status = 'superseded',
                last_error = COALESCE(last_error, 'covered by interrupted running job'),
                updated_at = ?1
            WHERE queued.status = 'queued'
              AND queued.job_type IN ('scan-library', 'rebuild-search-index')
              AND EXISTS (
                  SELECT 1 FROM jobs AS running
                  WHERE running.job_type = queued.job_type AND running.status = 'running'
              )
            "#,
        )
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        let result = sqlx::query(
            r#"
            UPDATE jobs SET
                status = 'queued',
                retry_at = NULL,
                last_error = COALESCE(last_error, 'interrupted by server restart'),
                updated_at = ?1
            WHERE status = 'running'
            "#,
        )
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        let recovered = result.rows_affected();
        transaction.commit().await?;
        Ok(recovered)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_work(
        &self,
        kind: &str,
        title: &str,
        source_path: Option<&str>,
        category: Option<&str>,
        description: Option<&str>,
        rating: Option<f64>,
        meta: Value,
    ) -> Result<i64> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        let row = sqlx::query(
            r#"
            INSERT INTO works (kind, title, category, description, rating, source_path, meta_json, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(kind, source_path) DO UPDATE SET
                title = excluded.title,
                category = excluded.category,
                description = COALESCE(excluded.description, works.description),
                rating = COALESCE(excluded.rating, works.rating),
                meta_json = json_patch(
                    CASE WHEN json_valid(works.meta_json) THEN works.meta_json ELSE '{}' END,
                    excluded.meta_json
                ),
                deleted_at = NULL,
                deleted_reason = NULL,
                updated_at = excluded.updated_at
            RETURNING id
            "#,
        )
        .bind(kind)
        .bind(title)
        .bind(category)
        .bind(description)
        .bind(rating)
        .bind(source_path)
        .bind(meta.to_string())
        .bind(Utc::now())
        .fetch_one(&mut *transaction)
        .await?;
        let work_id = row.get(0);
        let mut publication = SearchOutboxPublication::default();
        enqueue_search_upsert_outbox_for_work(&mut transaction, work_id, &mut publication).await?;
        transaction.commit().await?;
        Ok(work_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_scanner_work(
        &self,
        kind: &str,
        title: &str,
        source_path: Option<&str>,
        category: Option<&str>,
        description: Option<&str>,
        rating: Option<f64>,
        meta: Value,
        seen_token: &str,
        fingerprint: &str,
    ) -> Result<i64> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        require_scanner_lease(&mut transaction, "library", seen_token).await?;
        let work_id = upsert_scanner_work_in_transaction(
            &mut transaction,
            kind,
            title,
            source_path,
            category,
            description,
            rating,
            meta,
            fingerprint,
        )
        .await?;
        transaction.commit().await?;
        Ok(work_id)
    }

    /// Commit one complete legacy-scanner work snapshot under a single
    /// scanner lease transaction.  This is intended for small works such as
    /// comics, novels, and CoserPicture archives; callers with very large
    /// asset sets should keep using `upsert_scanner_assets` in bounded chunks.
    pub async fn commit_scanner_work_snapshot(
        &self,
        snapshot: ScannerWorkSnapshot,
        seen_token: &str,
        scope: &str,
    ) -> Result<i64> {
        let _write_slot = self.acquire_write_slot(256 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        require_scanner_lease(&mut transaction, "library", seen_token).await?;
        let work_id = upsert_scanner_work_in_transaction(
            &mut transaction,
            &snapshot.kind,
            &snapshot.title,
            snapshot.source_path.as_deref(),
            snapshot.category.as_deref(),
            snapshot.description.as_deref(),
            snapshot.rating,
            snapshot.meta,
            &snapshot.fingerprint,
        )
        .await?;

        let mut pending_assets = snapshot.assets.into_iter();
        loop {
            let chunk = pending_assets
                .by_ref()
                .take(SCANNER_ASSET_BATCH_SIZE)
                .collect::<Vec<_>>();
            if chunk.is_empty() {
                break;
            }
            upsert_scanner_asset_chunk_in_transaction(
                &mut transaction,
                work_id,
                &chunk,
                seen_token,
                Utc::now(),
            )
            .await?;
        }
        let mut publication = SearchOutboxPublication::default();
        for tag in snapshot.tags {
            upsert_and_link_scanner_tag_in_transaction(
                &mut transaction,
                work_id,
                &tag.namespace,
                &tag.key,
                &tag.label,
                None,
                None,
                &tag.source,
                None,
                None,
                seen_token,
                &mut publication,
            )
            .await?;
        }
        for external_id in snapshot.external_ids {
            upsert_scanner_external_id_in_transaction(
                &mut transaction,
                work_id,
                &external_id.source,
                &external_id.external_id,
                external_id.token.as_deref(),
                external_id.url.as_deref(),
                seen_token,
            )
            .await?;
        }
        finish_scanner_work_in_transaction(
            &mut transaction,
            work_id,
            scope,
            seen_token,
            &snapshot.fingerprint,
            &mut publication,
        )
        .await?;
        transaction.commit().await?;
        Ok(work_id)
    }

    /// Finish a work whose assets were written in bounded transactions.
    ///
    /// Large audio/gallery works cannot keep SQLite's writer lock while all
    /// assets are staged, so their asset chunks are committed separately.
    /// Tags, external IDs, stale-fact cleanup and the scanner finish marker do
    /// not need to be split and are committed together here.
    pub async fn commit_scanner_work_metadata(
        &self,
        work_id: i64,
        tags: Vec<ScannerTagInput>,
        external_ids: Vec<ScannerExternalIdInput>,
        seen_token: &str,
        scope: &str,
        fingerprint: &str,
    ) -> Result<()> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        require_scanner_lease(&mut transaction, "library", seen_token).await?;
        let mut publication = SearchOutboxPublication::default();
        for tag in tags {
            upsert_and_link_scanner_tag_in_transaction(
                &mut transaction,
                work_id,
                &tag.namespace,
                &tag.key,
                &tag.label,
                None,
                None,
                &tag.source,
                None,
                None,
                seen_token,
                &mut publication,
            )
            .await?;
        }
        for external_id in external_ids {
            upsert_scanner_external_id_in_transaction(
                &mut transaction,
                work_id,
                &external_id.source,
                &external_id.external_id,
                external_id.token.as_deref(),
                external_id.url.as_deref(),
                seen_token,
            )
            .await?;
        }
        finish_scanner_work_in_transaction(
            &mut transaction,
            work_id,
            scope,
            seen_token,
            fingerprint,
            &mut publication,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_asset(
        &self,
        work_id: i64,
        path: &str,
        mime: &str,
        role: &str,
        variant: Option<&str>,
        position: Option<i64>,
        size: Option<i64>,
        meta: Value,
    ) -> Result<i64> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        let row = sqlx::query(
            r#"
            INSERT INTO assets (work_id, path, mime, role, variant, position, size, meta_json)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(work_id, path, role, variant) DO UPDATE SET
                mime = excluded.mime,
                position = excluded.position,
                size = excluded.size,
                meta_json = excluded.meta_json
            RETURNING id
            "#,
        )
        .bind(work_id)
        .bind(path)
        .bind(mime)
        .bind(role)
        .bind(variant.unwrap_or(""))
        .bind(position.unwrap_or(-1))
        .bind(size)
        .bind(meta.to_string())
        .fetch_one(&mut *transaction)
        .await?;

        let asset_id: i64 = row.get(0);
        if role == "cover" {
            sqlx::query("UPDATE works SET cover_asset_id = ?1, updated_at = ?2 WHERE id = ?3")
                .bind(asset_id)
                .bind(Utc::now())
                .bind(work_id)
                .execute(&mut *transaction)
                .await?;
        }
        let mut publication = SearchOutboxPublication::default();
        enqueue_search_upsert_outbox_for_work(&mut transaction, work_id, &mut publication).await?;
        transaction.commit().await?;
        Ok(asset_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_scanner_asset(
        &self,
        work_id: i64,
        path: &str,
        mime: &str,
        role: &str,
        variant: Option<&str>,
        position: Option<i64>,
        size: Option<i64>,
        meta: Value,
        seen_token: &str,
    ) -> Result<i64> {
        let inputs = vec![ScannerAssetInput {
            path: path.to_string(),
            mime: mime.to_string(),
            role: role.to_string(),
            variant: variant.map(str::to_string),
            position,
            size,
            meta,
        }];
        self.upsert_scanner_assets(work_id, inputs, seen_token)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| {
                AppError::Other("scanner asset batch was unexpectedly empty".to_string())
            })
    }

    pub async fn upsert_scanner_assets(
        &self,
        work_id: i64,
        assets: Vec<ScannerAssetInput>,
        seen_token: &str,
    ) -> Result<Vec<i64>> {
        if assets.is_empty() {
            return Ok(Vec::new());
        }
        let mut asset_ids = Vec::with_capacity(assets.len());
        let mut pending = assets.into_iter();
        loop {
            // Bound the SQLite writer hold time for very large galleries/audio
            // works. The final scanner-work transaction remains the commit
            // marker, so an interrupted partial batch never triggers deletion.
            let chunk = pending
                .by_ref()
                .take(SCANNER_ASSET_BATCH_SIZE)
                .collect::<Vec<_>>();
            if chunk.is_empty() {
                break;
            }
            let version = Utc::now();
            let _write_slot = self.acquire_write_slot(8 * 1024 * 1024).await?;
            let mut transaction = self.begin_tracked_transaction().await?;
            require_scanner_lease(&mut transaction, "library", seen_token).await?;
            let chunk_asset_ids = upsert_scanner_asset_chunk_in_transaction(
                &mut transaction,
                work_id,
                &chunk,
                seen_token,
                version,
            )
            .await?;
            asset_ids.extend(chunk_asset_ids);
            transaction.commit().await?;
        }
        Ok(asset_ids)
    }

    pub async fn mark_scanner_work(
        &self,
        work_id: i64,
        scope: &str,
        seen_token: &str,
        fingerprint: Option<&str>,
    ) -> Result<()> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        sqlx::query(
            r#"
            INSERT INTO scanner_works (work_id, scope, seen_token, fingerprint)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(work_id) DO UPDATE SET
                scope = excluded.scope,
                seen_token = excluded.seen_token,
                fingerprint = excluded.fingerprint
            "#,
        )
        .bind(work_id)
        .bind(scope)
        .bind(seen_token)
        .bind(fingerprint)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn adopt_scanner_scope(
        &self,
        kind: &str,
        source_prefix: &str,
        scope: &str,
        seen_token: &str,
    ) -> Result<()> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        require_scanner_lease(&mut transaction, "library", seen_token).await?;
        sqlx::query(
            r#"
            UPDATE scanner_works
            SET scope = ?1
            WHERE scope = ('legacy|' || ?2)
              AND work_id IN (
                  SELECT id FROM works
                  WHERE kind = ?2
                    AND (
                        source_path = ?3
                        OR substr(source_path, 1, length(?3) + 1) = (?3 || '/')
                    )
              )
            "#,
        )
        .bind(scope)
        .bind(kind)
        .bind(source_prefix.trim_end_matches('/'))
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn scanner_work_fingerprint(
        &self,
        kind: &str,
        source_path: &str,
    ) -> Result<Option<(i64, String)>> {
        Ok(sqlx::query_as::<_, (i64, String)>(
            r#"
            SELECT work.id, scanner.fingerprint
            FROM works work
            JOIN scanner_works scanner ON scanner.work_id = work.id
            WHERE work.kind = ?1 AND work.source_path = ?2 AND scanner.fingerprint IS NOT NULL
            "#,
        )
        .bind(kind)
        .bind(source_path)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn scanner_work_id(&self, kind: &str, source_path: &str) -> Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            r#"
            SELECT work.id
            FROM works work
            JOIN scanner_works scanner ON scanner.work_id = work.id
            WHERE work.kind = ?1 AND work.source_path = ?2
            "#,
        )
        .bind(kind)
        .bind(source_path)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn touch_scanner_work(
        &self,
        work_id: i64,
        scope: &str,
        seen_token: &str,
    ) -> Result<()> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        require_scanner_lease(&mut transaction, "library", seen_token).await?;
        sqlx::query("UPDATE scanner_works SET scope = ?1, seen_token = ?2 WHERE work_id = ?3")
            .bind(scope)
            .bind(seen_token)
            .bind(work_id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn finish_scanner_work(
        &self,
        work_id: i64,
        scope: &str,
        seen_token: &str,
        fingerprint: &str,
    ) -> Result<()> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        require_scanner_lease(&mut transaction, "library", seen_token).await?;
        let mut publication = SearchOutboxPublication::default();
        finish_scanner_work_in_transaction(
            &mut transaction,
            work_id,
            scope,
            seen_token,
            fingerprint,
            &mut publication,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn finish_scanner_scope(&self, scope: &str, seen_token: &str) -> Result<u64> {
        let _write_slot = self.acquire_write_slot(128 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        require_scanner_lease(&mut transaction, "library", seen_token).await?;
        prepare_scanner_tombstone_tables(&mut transaction).await?;
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO temp_scanner_tombstone_work_ids(work_id)
            SELECT work_id FROM scanner_works
            WHERE scope = ?1 AND seen_token != ?2
            "#,
        )
        .bind(scope)
        .bind(seen_token)
        .execute(&mut *transaction)
        .await?;
        let (_, tombstoned) = tombstone_scanner_work_ids(&mut transaction).await?;
        let deleted = tombstoned.len() as u64;
        transaction.commit().await?;
        Ok(deleted)
    }

    pub async fn finish_removed_scanner_scopes(
        &self,
        kind: &str,
        active_scopes: &[String],
        seen_token: &str,
    ) -> Result<u64> {
        let _write_slot = self.acquire_write_slot(128 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        require_scanner_lease(&mut transaction, "library", seen_token).await?;
        prepare_scanner_tombstone_tables(&mut transaction).await?;
        let prefix = format!("{kind}|");
        let scopes = sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT scope FROM scanner_works WHERE scope LIKE ?1 OR scope = ?2",
        )
        .bind(format!("{prefix}%"))
        .bind(format!("legacy|{kind}"))
        .fetch_all(&mut *transaction)
        .await?;
        for scope in scopes {
            if active_scopes.iter().any(|active| active == &scope) {
                continue;
            }
            sqlx::query(
                "INSERT OR IGNORE INTO temp_scanner_tombstone_work_ids(work_id) SELECT work_id FROM scanner_works WHERE scope = ?1",
            )
            .bind(scope)
            .execute(&mut *transaction)
            .await?;
        }
        let (_, tombstoned) = tombstone_scanner_work_ids(&mut transaction).await?;
        let deleted = tombstoned.len() as u64;
        transaction.commit().await?;
        Ok(deleted)
    }

    pub async fn try_acquire_scanner_lock(
        &self,
        name: &str,
        token: &str,
        stale_after_seconds: i64,
    ) -> Result<bool> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        let stale_before = Utc::now() - chrono::Duration::seconds(stale_after_seconds.max(60));
        let result = sqlx::query(
            r#"
            INSERT INTO scanner_locks (name, token, acquired_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(name) DO UPDATE SET
                token = excluded.token,
                acquired_at = excluded.acquired_at
            WHERE scanner_locks.acquired_at < ?4
            "#,
        )
        .bind(name)
        .bind(token)
        .bind(Utc::now())
        .bind(stale_before)
        .execute(&mut *transaction)
        .await?;
        let acquired = result.rows_affected() > 0;
        transaction.commit().await?;
        Ok(acquired)
    }

    pub async fn heartbeat_scanner_lock(&self, name: &str, token: &str) -> Result<bool> {
        let _write_slot = self.acquire_write_slot(16 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        let result =
            sqlx::query("UPDATE scanner_locks SET acquired_at = ?1 WHERE name = ?2 AND token = ?3")
                .bind(Utc::now())
                .bind(name)
                .bind(token)
                .execute(&mut *transaction)
                .await?;
        let updated = result.rows_affected() > 0;
        transaction.commit().await?;
        Ok(updated)
    }

    pub async fn release_scanner_lock(&self, name: &str, token: &str) -> Result<()> {
        let _write_slot = self.acquire_write_slot(16 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        sqlx::query("DELETE FROM scanner_locks WHERE name = ?1 AND token = ?2")
            .bind(name)
            .bind(token)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn refresh_tag_counts(&self) -> Result<()> {
        let _write_slot = self.acquire_write_slot(256 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        // Recount in two bounded set-oriented statements.  The previous
        // correlated COUNT(*) expression performed one index probe per tag;
        // with a large tag dictionary that repeatedly walked the same
        // work_tags table after every scan.  First clear stale non-zero tags
        // that have no associations, then aggregate work_tags once and join
        // the result for tags that are still present.  Both statements keep
        // the legacy global-count semantics and update only changed rows.
        sqlx::query(
            r#"
            UPDATE tags
            SET count = 0
            WHERE count != 0
              AND NOT EXISTS (
                  SELECT 1 FROM work_tags
                  WHERE work_tags.tag_id = tags.id
              )
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            WITH tag_counts(tag_id, next_count) AS (
                SELECT tag_id, COUNT(*)
                FROM work_tags
                GROUP BY tag_id
            )
            UPDATE tags
            SET count = tag_counts.next_count
            FROM tag_counts
            WHERE tags.id = tag_counts.tag_id
              AND tags.count IS NOT tag_counts.next_count
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn set_work_cover(&self, work_id: i64, asset_id: i64) -> Result<()> {
        let _write_slot = self.acquire_write_slot(16 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        let affected = sqlx::query(
            "UPDATE works
             SET cover_asset_id = ?1, updated_at = ?2
             WHERE id = ?3
               AND EXISTS (
                   SELECT 1 FROM assets
                   WHERE assets.id = ?1 AND assets.work_id = works.id
               )",
        )
        .bind(asset_id)
        .bind(Utc::now())
        .bind(work_id)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if affected == 0 {
            let work_exists =
                sqlx::query_scalar::<_, i64>("SELECT 1 FROM works WHERE id = ?1 LIMIT 1")
                    .bind(work_id)
                    .fetch_optional(&mut *transaction)
                    .await?
                    .is_some();
            transaction.rollback().await?;
            if work_exists {
                return Err(AppError::NotFound(format!(
                    "asset {asset_id} not found for work {work_id}"
                )));
            }
            return Err(AppError::NotFound(format!("work {work_id} not found")));
        }
        let mut publication = SearchOutboxPublication::default();
        enqueue_search_upsert_outbox_for_work(&mut transaction, work_id, &mut publication).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn generated_assets_work(&self) -> Result<i64> {
        self.upsert_work(
            "generated",
            "Generated UI Assets",
            Some("__generated_ui_assets__"),
            Some("UI Assets"),
            Some("Safe local UI backgrounds, empty states, and placeholder covers generated through the image asset queue."),
            None,
            json!({
                "system": true,
                "source": "openai-image-generation",
                "collection": "ui-assets"
            }),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_tag(
        &self,
        namespace: &str,
        key: &str,
        label: &str,
        translated_label: Option<&str>,
        translated_namespace: Option<&str>,
        source: &str,
        intro: Option<&str>,
        links: Option<&str>,
    ) -> Result<i64> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        let (tag_id, tag_changed) = upsert_tag_in_transaction(
            &mut transaction,
            namespace,
            key,
            label,
            translated_label,
            translated_namespace,
            source,
            intro,
            links,
        )
        .await?;
        if tag_changed {
            let mut publication = SearchOutboxPublication::default();
            enqueue_search_upsert_outbox_for_tag(&mut transaction, tag_id, &mut publication)
                .await?;
        }
        transaction.commit().await?;
        Ok(tag_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_and_link_scanner_tag(
        &self,
        work_id: i64,
        namespace: &str,
        key: &str,
        label: &str,
        translated_label: Option<&str>,
        translated_namespace: Option<&str>,
        source: &str,
        intro: Option<&str>,
        links: Option<&str>,
        seen_token: &str,
    ) -> Result<i64> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        require_scanner_lease(&mut transaction, "library", seen_token).await?;
        let mut publication = SearchOutboxPublication::default();
        let tag_id = upsert_and_link_scanner_tag_in_transaction(
            &mut transaction,
            work_id,
            namespace,
            key,
            label,
            translated_label,
            translated_namespace,
            source,
            intro,
            links,
            seen_token,
            &mut publication,
        )
        .await?;
        transaction.commit().await?;
        Ok(tag_id)
    }

    pub async fn link_tag(&self, work_id: i64, tag_id: i64) -> Result<()> {
        self.link_tag_owned(work_id, tag_id, "external", None).await
    }

    pub async fn link_scanner_tag(
        &self,
        work_id: i64,
        tag_id: i64,
        seen_token: &str,
    ) -> Result<()> {
        self.link_tag_owned(work_id, tag_id, "scanner", Some(seen_token))
            .await
    }

    pub async fn link_current_scanner_tag(&self, work_id: i64, tag_id: i64) -> Result<()> {
        // Read the current scanner lease and write the association from the
        // same SQLite snapshot.  The old implementation fetched the token
        // from the pool and then opened a second writer transaction, which
        // left a race where the lease could be replaced between those two
        // operations.
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        let seen_token = sqlx::query_scalar::<_, String>(
            "SELECT token FROM scanner_locks WHERE name = 'library'",
        )
        .fetch_optional(&mut *transaction)
        .await?
        .unwrap_or_else(|| "direct".to_string());
        self.link_tag_owned_in_transaction(
            work_id,
            tag_id,
            "scanner",
            Some(&seen_token),
            &mut transaction,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn link_tag_owned(
        &self,
        work_id: i64,
        tag_id: i64,
        owner: &str,
        seen_token: Option<&str>,
    ) -> Result<()> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        self.link_tag_owned_in_transaction(work_id, tag_id, owner, seen_token, &mut transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn link_tag_owned_in_transaction(
        &self,
        work_id: i64,
        tag_id: i64,
        owner: &str,
        seen_token: Option<&str>,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<()> {
        if owner == "scanner" {
            let seen_token = seen_token.ok_or_else(|| {
                AppError::Other("scanner tag link is missing a lease token".to_string())
            })?;
            require_scanner_lease(transaction, "library", seen_token).await?;
        }
        let linked =
            sqlx::query("INSERT OR IGNORE INTO work_tags (work_id, tag_id) VALUES (?1, ?2)")
                .bind(work_id)
                .bind(tag_id)
                .execute(&mut **transaction)
                .await?
                .rows_affected()
                > 0;
        sqlx::query(
            r#"
            INSERT INTO work_tag_sources (work_id, tag_id, owner, seen_token)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(work_id, tag_id, owner) DO UPDATE SET
                seen_token = excluded.seen_token
            "#,
        )
        .bind(work_id)
        .bind(tag_id)
        .bind(owner)
        .bind(seen_token)
        .execute(&mut **transaction)
        .await?;
        sqlx::query(
            r#"
            UPDATE tags
            SET count = (SELECT COUNT(*) FROM work_tags WHERE tag_id = ?1)
            WHERE id = ?1
              AND count IS NOT (SELECT COUNT(*) FROM work_tags WHERE tag_id = ?1)
            "#,
        )
        .bind(tag_id)
        .execute(&mut **transaction)
        .await?;
        if linked {
            let mut publication = SearchOutboxPublication::default();
            enqueue_search_upsert_outbox_for_work(transaction, work_id, &mut publication).await?;
        }
        Ok(())
    }

    pub async fn upsert_external_id(
        &self,
        work_id: i64,
        source: &str,
        external_id: &str,
        token: Option<&str>,
        url: Option<&str>,
    ) -> Result<()> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        let external_id_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO external_ids (work_id, source, external_id, token, url)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(work_id, source, external_id) DO UPDATE SET
                token = COALESCE(excluded.token, external_ids.token),
                url = COALESCE(excluded.url, external_ids.url)
            RETURNING id
            "#,
        )
        .bind(work_id)
        .bind(source)
        .bind(external_id)
        .bind(token)
        .bind(url)
        .fetch_one(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO external_id_sources (external_id_id, work_id, owner, seen_token)
            VALUES (?1, ?2, 'external', NULL)
            ON CONFLICT(external_id_id, owner) DO NOTHING
            "#,
        )
        .bind(external_id_id)
        .bind(work_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_scanner_external_id(
        &self,
        work_id: i64,
        source: &str,
        external_id: &str,
        token: Option<&str>,
        url: Option<&str>,
        seen_token: &str,
    ) -> Result<()> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        require_scanner_lease(&mut transaction, "library", seen_token).await?;
        let external_id_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO external_ids (work_id, source, external_id, token, url)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(work_id, source, external_id) DO UPDATE SET
                token = COALESCE(excluded.token, external_ids.token),
                url = COALESCE(excluded.url, external_ids.url)
            RETURNING id
            "#,
        )
        .bind(work_id)
        .bind(source)
        .bind(external_id)
        .bind(token)
        .bind(url)
        .fetch_one(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO external_id_sources (external_id_id, work_id, owner, seen_token)
            VALUES (?1, ?2, 'scanner', ?3)
            ON CONFLICT(external_id_id, owner) DO UPDATE SET
                work_id = excluded.work_id,
                seen_token = excluded.seen_token
            "#,
        )
        .bind(external_id_id)
        .bind(work_id)
        .bind(seen_token)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn create_job(&self, job_type: &str, status: &str, payload: Value) -> Result<i64> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        let row = sqlx::query(
            r#"
            INSERT INTO jobs (job_type, status, payload_json, created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?4)
            RETURNING id
            "#,
        )
        .bind(job_type)
        .bind(status)
        .bind(payload.to_string())
        .bind(Utc::now())
        .fetch_one(&mut *transaction)
        .await?;
        let job_id = row.get(0);
        transaction.commit().await?;
        Ok(job_id)
    }

    pub async fn create_job_if_absent(
        &self,
        job_type: &str,
        status: &str,
        payload: Value,
    ) -> Result<(i64, bool)> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let payload_value = payload;
        let payload = payload_value.to_string();
        for _ in 0..3 {
            // Start each retry from a fresh SQLite snapshot.  The process-local
            // writer gate serializes normal callers, but a second process or a
            // recovery tool can still change job state between attempts.
            let mut transaction = self.begin_tracked_transaction().await?;
            let now = Utc::now();
            let inserted = sqlx::query_scalar::<_, i64>(
                r#"
                INSERT OR IGNORE INTO jobs (job_type, status, payload_json, created_at, updated_at)
                VALUES (?1, ?2, ?3, ?4, ?4)
                RETURNING id
                "#,
            )
            .bind(job_type)
            .bind(status)
            .bind(&payload)
            .bind(now)
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some(id) = inserted {
                transaction.commit().await?;
                return Ok((id, true));
            }
            let existing = sqlx::query_as::<_, (i64, String, String)>(
                r#"
                SELECT id, status, payload_json FROM jobs
                WHERE job_type = ?1 AND status IN ('queued', 'running')
                ORDER BY CASE WHEN status = 'queued' THEN 0 ELSE 1 END, id
                LIMIT 1
                "#,
            )
            .bind(job_type)
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some((id, existing_status, existing_payload)) = existing {
                if existing_status == "queued" {
                    if job_type == "scan-library" {
                        let merged_payload =
                            Self::merge_scan_job_payload(&existing_payload, &payload_value);
                        let updated = sqlx::query(
                            r#"
                            UPDATE jobs
                            SET payload_json = ?1, updated_at = ?2
                            WHERE id = ?3 AND status = 'queued'
                            "#,
                        )
                        .bind(merged_payload.to_string())
                        .bind(Utc::now())
                        .bind(id)
                        .execute(&mut *transaction)
                        .await?
                        .rows_affected();
                        if updated > 0 {
                            transaction.commit().await?;
                            return Ok((id, false));
                        }
                    } else {
                        let updated = sqlx::query(
                            r#"
                            UPDATE jobs
                            SET payload_json = json_patch(payload_json, ?1), updated_at = ?2
                            WHERE id = ?3 AND status = 'queued'
                            "#,
                        )
                        .bind(&payload)
                        .bind(Utc::now())
                        .bind(id)
                        .execute(&mut *transaction)
                        .await?
                        .rows_affected();
                        if updated > 0 {
                            transaction.commit().await?;
                            return Ok((id, false));
                        }
                    }
                }
            }
            transaction.rollback().await?;
        }
        Err(AppError::Other(format!(
            "job {job_type} changed state repeatedly while being queued"
        )))
    }

    /// Merge scan requests without narrowing an already queued full scan to a
    /// single media kind.  A full scan is represented by an absent `kind`; if
    /// two different kind-scoped requests coalesce, the merged request also
    /// becomes full-scope so neither request can silently lose coverage.
    fn merge_scan_job_payload(existing_json: &str, requested: &Value) -> Value {
        let mut merged = serde_json::from_str::<Value>(existing_json)
            .ok()
            .filter(Value::is_object)
            .unwrap_or_else(|| json!({}));
        let requested_object = requested.as_object();
        let existing_kind = merged.get("kind").and_then(Value::as_str);
        let requested_kind = requested.get("kind").and_then(Value::as_str);
        let existing_enqueue = merged
            .get("enqueue_enrichment")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let merged_kind = match (existing_kind, requested_kind) {
            (Some(left), Some(right)) if left == right => Some(right.to_string()),
            (None, None) => None,
            _ => None,
        };
        if let Some(object) = merged.as_object_mut() {
            if let Some(requested_object) = requested_object {
                for (key, value) in requested_object {
                    if key != "kind" && key != "enqueue_enrichment" {
                        object.insert(key.clone(), value.clone());
                    }
                }
            }
            let enqueue = existing_enqueue
                || requested
                    .get("enqueue_enrichment")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
            object.insert("enqueue_enrichment".to_string(), Value::Bool(enqueue));
            match merged_kind {
                Some(kind) => {
                    object.insert("kind".to_string(), Value::String(kind));
                }
                None => {
                    object.remove("kind");
                }
            }
        }
        merged
    }

    pub async fn create_work_job_once(
        &self,
        job_type: &str,
        work_id: i64,
        fingerprint: &str,
        payload: Value,
    ) -> Result<(i64, bool)> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let payload = payload.to_string();
        for _ in 0..3 {
            // See create_job_if_absent: retries intentionally do not reuse a
            // read snapshot that may have observed a transient external state.
            let mut transaction = self.begin_tracked_transaction().await?;
            let inserted = sqlx::query_scalar::<_, i64>(
                r#"
                INSERT INTO jobs (job_type, status, payload_json, created_at, updated_at)
                SELECT ?1, 'queued', ?2, ?3, ?3
                WHERE NOT EXISTS (
                    SELECT 1 FROM jobs
                    WHERE job_type = ?1
                      AND status IN ('queued', 'running', 'done')
                      AND CAST(json_extract(payload_json, '$.work_id') AS INTEGER) = ?4
                      AND json_extract(payload_json, '$.fingerprint') = ?5
                )
                RETURNING id
                "#,
            )
            .bind(job_type)
            .bind(&payload)
            .bind(Utc::now())
            .bind(work_id)
            .bind(fingerprint)
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some(id) = inserted {
                transaction.commit().await?;
                return Ok((id, true));
            }
            if let Some(id) = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT id FROM jobs
                WHERE job_type = ?1
                  AND status IN ('queued', 'running', 'done')
                  AND CAST(json_extract(payload_json, '$.work_id') AS INTEGER) = ?2
                  AND json_extract(payload_json, '$.fingerprint') = ?3
                ORDER BY id DESC
                LIMIT 1
                "#,
            )
            .bind(job_type)
            .bind(work_id)
            .bind(fingerprint)
            .fetch_optional(&mut *transaction)
            .await?
            {
                transaction.commit().await?;
                return Ok((id, false));
            }
            transaction.rollback().await?;
        }
        Err(AppError::Other(format!(
            "work job {job_type}/{work_id} changed state repeatedly while being queued"
        )))
    }

    pub async fn update_job(&self, id: i64, status: &str, last_error: Option<&str>) -> Result<()> {
        let _write_slot = self.acquire_write_slot(16 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        sqlx::query(
            r#"
            UPDATE jobs SET
                status = ?1,
                last_error = ?2,
                attempts = CASE WHEN ?1 = 'running' THEN attempts + 1 ELSE attempts END,
                updated_at = ?3
            WHERE id = ?4
            "#,
        )
        .bind(status)
        .bind(last_error)
        .bind(Utc::now())
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn reschedule_job(
        &self,
        id: i64,
        last_error: &str,
        retry_delay_seconds: i64,
    ) -> Result<()> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        let job_type = sqlx::query_scalar::<_, String>("SELECT job_type FROM jobs WHERE id = ?1")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("job {id} not found")))?;
        if matches!(job_type.as_str(), "scan-library" | "rebuild-search-index") {
            let successor = sqlx::query_scalar::<_, i64>(
                "SELECT id FROM jobs WHERE job_type = ?1 AND status = 'queued' AND id != ?2 ORDER BY id LIMIT 1",
            )
            .bind(&job_type)
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some(successor_id) = successor {
                let merge_sql = if job_type == "scan-library" {
                    r#"
                    UPDATE jobs
                    SET
                        payload_json = json_set(
                            json_patch(
                                COALESCE((SELECT payload_json FROM jobs WHERE id = ?1), '{}'),
                                payload_json
                            ),
                            '$.enqueue_enrichment',
                            json(CASE
                                WHEN COALESCE(json_extract(
                                    COALESCE((SELECT payload_json FROM jobs WHERE id = ?1), '{}'),
                                    '$.enqueue_enrichment'
                                ), 0) != 0
                                  OR COALESCE(json_extract(payload_json, '$.enqueue_enrichment'), 0) != 0
                                THEN 'true'
                                ELSE 'false'
                            END)
                        ),
                        updated_at = ?2
                    WHERE id = ?3 AND status = 'queued'
                    "#
                } else {
                    r#"
                    UPDATE jobs
                    SET
                        payload_json = json_patch(
                            COALESCE((SELECT payload_json FROM jobs WHERE id = ?1), '{}'),
                            payload_json
                        ),
                        updated_at = ?2
                    WHERE id = ?3 AND status = 'queued'
                    "#
                };
                sqlx::query(merge_sql)
                    .bind(id)
                    .bind(Utc::now())
                    .bind(successor_id)
                    .execute(&mut *transaction)
                    .await?;
                sqlx::query(
                    "UPDATE jobs SET status = 'superseded', last_error = ?1, updated_at = ?2 WHERE id = ?3",
                )
                .bind(format!("{last_error}; retry covered by queued successor {successor_id}"))
                .bind(Utc::now())
                .bind(id)
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
                return Ok(());
            }
        }
        let retry_at = Utc::now() + chrono::Duration::seconds(retry_delay_seconds);
        sqlx::query(
            r#"
            UPDATE jobs SET
                status = 'queued',
                last_error = ?1,
                retry_at = ?2,
                updated_at = ?3
            WHERE id = ?4
            "#,
        )
        .bind(last_error)
        .bind(retry_at)
        .bind(Utc::now())
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn update_work_enrichment(
        &self,
        id: i64,
        title: Option<&str>,
        category: Option<&str>,
        description: Option<&str>,
        rating: Option<f64>,
        meta: Value,
    ) -> Result<()> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        let affected = sqlx::query(
            r#"
            UPDATE works SET
                title = COALESCE(?1, title),
                category = COALESCE(?2, category),
                description = COALESCE(?3, description),
                rating = COALESCE(?4, rating),
                meta_json = ?5,
                updated_at = ?6
            WHERE id = ?7
            "#,
        )
        .bind(title)
        .bind(category)
        .bind(description)
        .bind(rating)
        .bind(meta.to_string())
        .bind(Utc::now())
        .bind(id)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if affected > 0 {
            let mut publication = SearchOutboxPublication::default();
            enqueue_search_upsert_outbox_for_work(&mut transaction, id, &mut publication).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn scanner_fingerprint_matches(
        &self,
        work_id: i64,
        fingerprint: &str,
    ) -> Result<bool> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM scanner_works WHERE work_id = ?1 AND fingerprint = ?2",
        )
        .bind(work_id)
        .bind(fingerprint)
        .fetch_optional(&self.pool)
        .await?
        .is_some())
    }

    pub async fn apply_scanner_enrichment(
        &self,
        work_id: i64,
        fingerprint: &str,
        enrichment: ScannerEnrichmentInput,
    ) -> Result<bool> {
        let _write_slot = self.acquire_write_slot(256 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        // This no-op UPDATE both checks the generation and acquires SQLite's
        // writer reservation. The scanner cannot publish a different
        // fingerprint until all enrichment fields, ids, and tags commit.
        let guarded = sqlx::query(
            r#"
            UPDATE scanner_works
            SET fingerprint = fingerprint
            WHERE work_id = ?1
              AND fingerprint = ?2
              AND (
                  json_extract((SELECT meta_json FROM works WHERE id = ?1), '$._scanner_fingerprint') IS NULL
                  OR json_extract((SELECT meta_json FROM works WHERE id = ?1), '$._scanner_fingerprint') = ?2
              )
            "#,
        )
        .bind(work_id)
        .bind(fingerprint)
        .execute(&mut *transaction)
        .await?
        .rows_affected()
            > 0;
        if !guarded {
            transaction.rollback().await?;
            return Ok(false);
        }

        let mut publication = SearchOutboxPublication::default();
        sqlx::query(
            r#"
            UPDATE works SET
                title = COALESCE(?1, title),
                category = COALESCE(?2, category),
                description = COALESCE(?3, description),
                rating = COALESCE(?4, rating),
                meta_json = ?5,
                updated_at = ?6
            WHERE id = ?7
            "#,
        )
        .bind(enrichment.title.as_deref())
        .bind(enrichment.category.as_deref())
        .bind(enrichment.description.as_deref())
        .bind(enrichment.rating)
        .bind(enrichment.meta.to_string())
        .bind(Utc::now())
        .bind(work_id)
        .execute(&mut *transaction)
        .await?;

        for external in enrichment.external_ids {
            let external_id_id = sqlx::query_scalar::<_, i64>(
                r#"
                INSERT INTO external_ids (work_id, source, external_id, token, url)
                VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(work_id, source, external_id) DO UPDATE SET
                    token = COALESCE(excluded.token, external_ids.token),
                    url = COALESCE(excluded.url, external_ids.url)
                RETURNING id
                "#,
            )
            .bind(work_id)
            .bind(external.source)
            .bind(external.external_id)
            .bind(external.token)
            .bind(external.url)
            .fetch_one(&mut *transaction)
            .await?;
            sqlx::query(
                r#"
                INSERT INTO external_id_sources (external_id_id, work_id, owner, seen_token)
                VALUES (?1, ?2, 'external', NULL)
                ON CONFLICT(external_id_id, owner) DO NOTHING
                "#,
            )
            .bind(external_id_id)
            .bind(work_id)
            .execute(&mut *transaction)
            .await?;
        }

        for tag in enrichment.tags {
            let (tag_id, tag_changed) = upsert_tag_in_transaction(
                &mut transaction,
                &tag.namespace,
                &tag.key,
                &tag.label,
                None,
                None,
                &tag.source,
                None,
                None,
            )
            .await?;
            let linked =
                sqlx::query("INSERT OR IGNORE INTO work_tags (work_id, tag_id) VALUES (?1, ?2)")
                    .bind(work_id)
                    .bind(tag_id)
                    .execute(&mut *transaction)
                    .await?
                    .rows_affected()
                    > 0;
            sqlx::query(
                r#"
                INSERT INTO work_tag_sources (work_id, tag_id, owner, seen_token)
                VALUES (?1, ?2, 'external', NULL)
                ON CONFLICT(work_id, tag_id, owner) DO NOTHING
                "#,
            )
            .bind(work_id)
            .bind(tag_id)
            .execute(&mut *transaction)
            .await?;
            if tag_changed {
                enqueue_search_upsert_outbox_for_tag(&mut transaction, tag_id, &mut publication)
                    .await?;
            } else if linked {
                enqueue_search_upsert_outbox_for_work(&mut transaction, work_id, &mut publication)
                    .await?;
            }
        }
        enqueue_search_upsert_outbox_for_work(&mut transaction, work_id, &mut publication).await?;
        transaction.commit().await?;
        Ok(true)
    }

    pub async fn update_work_meta(&self, id: i64, meta: Value) -> Result<()> {
        let _write_slot = self.acquire_write_slot(32 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        let affected = sqlx::query(
            r#"
            UPDATE works SET meta_json = ?1, updated_at = ?2
            WHERE id = ?3
            "#,
        )
        .bind(meta.to_string())
        .bind(Utc::now())
        .bind(id)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if affected == 0 {
            transaction.rollback().await?;
            return Err(AppError::NotFound(format!("work {id} not found")));
        }
        let mut publication = SearchOutboxPublication::default();
        enqueue_search_upsert_outbox_for_work(&mut transaction, id, &mut publication).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn update_work_progress(
        &self,
        id: i64,
        progress: f64,
        position: Option<&str>,
        update_token: i64,
    ) -> Result<ProgressWrite> {
        let progress = progress.clamp(0.0, 1.0);
        let now = Utc::now();
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        let exists = sqlx::query_scalar::<_, i64>("SELECT 1 FROM works WHERE id = ?1")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
        if !exists {
            return Err(AppError::NotFound(format!("work {id} not found")));
        }

        let history_affected = sqlx::query(
            r#"
            INSERT INTO reading_history (work_id, progress, position, last_opened_at, update_token)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(work_id) DO UPDATE SET
                progress = excluded.progress,
                position = excluded.position,
                last_opened_at = excluded.last_opened_at,
                update_token = excluded.update_token
            WHERE excluded.update_token > reading_history.update_token
            "#,
        )
        .bind(id)
        .bind(progress)
        .bind(position)
        .bind(now)
        .bind(update_token)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if history_affected == 0 {
            let current = sqlx::query_as::<_, (f64, Option<String>)>(
                "SELECT progress, position FROM reading_history WHERE work_id = ?1",
            )
            .bind(id)
            .fetch_one(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return Ok(ProgressWrite {
                accepted: false,
                progress: current.0,
                position: current.1,
            });
        }

        sqlx::query(
            r#"
            UPDATE works SET progress = ?1
            WHERE id = ?2
            "#,
        )
        .bind(progress)
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(ProgressWrite {
            accepted: true,
            progress,
            position: position.map(str::to_string),
        })
    }

    pub async fn library(&self) -> Result<LibraryResponse> {
        // The legacy helper predates keyset pagination.  Keep its return type
        // for internal compatibility, but never materialize the entire
        // catalog into one response; callers can continue with next_cursor.
        self.library_page(None, LEGACY_LIBRARY_PAGE_LIMIT, true)
            .await
    }

    pub async fn library_page(
        &self,
        cursor: Option<&str>,
        limit: i64,
        include_context: bool,
    ) -> Result<LibraryResponse> {
        let cursor = cursor.map(decode_library_cursor).transpose()?;
        let limit = limit.clamp(1, LEGACY_LIBRARY_PAGE_LIMIT);
        let fetch_limit = limit.saturating_add(1);
        let mut transaction = self.begin_tracked_read_transaction().await?;
        const LIBRARY_SELECT: &str = r#"
            SELECT
                w.id, w.kind, w.title, w.subtitle, w.category, w.rating, w.progress, w.source_path,
                w.cover_asset_id, w.meta_json,
                NULL AS tag_keys,
                CASE
                    WHEN stats.computed_at IS NOT NULL THEN stats.tag_count
                    ELSE (SELECT COUNT(*) FROM work_tags wt WHERE wt.work_id = w.id)
                END AS tag_count,
                CASE
                    WHEN stats.computed_at IS NOT NULL THEN stats.asset_count
                    ELSE (SELECT COUNT(*) FROM assets a WHERE a.work_id = w.id)
                END AS asset_count,
                w.updated_at
            FROM works AS w
            LEFT JOIN work_stats AS stats ON stats.work_id = w.id
        "#;
        let mut works = if let Some(cursor) = cursor.as_ref() {
            if let Some(next_updated_at) = cursor.next_updated_at {
                let sql = format!(
                    "{LIBRARY_SELECT}\nWHERE w.deleted_at IS NULL\n  AND (w.updated_at < ?1 OR (w.updated_at = ?1 AND w.id < ?2))\nORDER BY w.updated_at DESC, w.id DESC\nLIMIT ?3"
                );
                sqlx::query_as::<_, WorkSummary>(&sql)
                    .bind(next_updated_at)
                    .bind(cursor.next_id)
                    .bind(fetch_limit)
                    .fetch_all(&mut *transaction)
                    .await?
            } else {
                // Cursors issued before the composite ordering key was
                // introduced remain valid for old clients.  They retain the
                // historical id-only continuation semantics and naturally
                // expire as clients receive new cursors.
                let sql = format!(
                    "{LIBRARY_SELECT}\nWHERE w.deleted_at IS NULL AND w.id < ?1\nORDER BY w.id DESC\nLIMIT ?2"
                );
                sqlx::query_as::<_, WorkSummary>(&sql)
                    .bind(cursor.next_id)
                    .bind(fetch_limit)
                    .fetch_all(&mut *transaction)
                    .await?
            }
        } else {
            let sql = format!(
                "{LIBRARY_SELECT}\nWHERE w.deleted_at IS NULL\nORDER BY w.updated_at DESC, w.id DESC\nLIMIT ?1"
            );
            sqlx::query_as::<_, WorkSummary>(&sql)
                .bind(fetch_limit)
                .fetch_all(&mut *transaction)
                .await?
        };
        let has_more = works.len() as i64 > limit;
        works.truncate(limit as usize);
        self.populate_library_tag_keys_with_connection(&mut works, &mut transaction)
            .await?;
        let next_cursor = if !has_more {
            None
        } else {
            works
                .last()
                .map(|work| encode_library_cursor(work.updated_at, work.id))
                .transpose()?
        };
        let (tags, jobs, history) = if include_context {
            (
                self.tags_with_connection(&mut transaction).await?,
                self.jobs_with_connection(20, &mut transaction).await?,
                self.history_with_connection(20, &mut transaction).await?,
            )
        } else {
            (Vec::new(), Vec::new(), Vec::new())
        };
        transaction.commit().await?;
        Ok(LibraryResponse {
            works,
            tags,
            jobs,
            history,
            next_cursor,
        })
    }

    /// Fill the compatibility `tag_keys` field with one ordered query for the
    /// current page.  The old shape evaluated a correlated GROUP_CONCAT for
    /// every work row, which multiplied the same work_tags/tags join by the
    /// page size.  A page contains at most 500 works, so retaining the small
    /// result in memory is bounded by the response itself and keeps the
    /// SQLite read snapshot short.
    async fn populate_library_tag_keys_with_connection(
        &self,
        works: &mut [WorkSummary],
        connection: &mut SqliteConnection,
    ) -> Result<()> {
        if works.is_empty() {
            return Ok(());
        }
        let work_ids = works.iter().map(|work| work.id).collect::<Vec<_>>();
        let work_ids_json =
            serde_json::to_string(&work_ids).map_err(|error| AppError::Other(error.to_string()))?;
        let tag_rows = sqlx::query_as::<_, (i64, String)>(
            r#"
            WITH selected(work_id) AS MATERIALIZED (
                SELECT CAST(value AS INTEGER)
                FROM json_each(?1)
            )
            SELECT work_tag.work_id,
                   group_concat(
                       tag.namespace || ':' || tag.key
                       ORDER BY tag.namespace, tag.key
                   ) AS tag_keys
            FROM work_tags AS work_tag
            JOIN selected ON selected.work_id = work_tag.work_id
            JOIN tags AS tag ON tag.id = work_tag.tag_id
            GROUP BY work_tag.work_id
            ORDER BY work_tag.work_id
            "#,
        )
        .bind(work_ids_json)
        .fetch_all(&mut *connection)
        .await?;

        let tag_keys_by_work = tag_rows.into_iter().collect::<BTreeMap<_, _>>();
        for work in works {
            work.tag_keys = tag_keys_by_work.get(&work.id).cloned();
        }
        Ok(())
    }

    pub async fn history(&self, limit: i64) -> Result<Vec<HistoryRecord>> {
        let mut transaction = self.begin_tracked_read_transaction().await?;
        let history = self
            .history_with_connection(limit, &mut transaction)
            .await?;
        transaction.commit().await?;
        Ok(history)
    }

    async fn history_with_connection(
        &self,
        limit: i64,
        connection: &mut SqliteConnection,
    ) -> Result<Vec<HistoryRecord>> {
        Ok(sqlx::query_as::<_, HistoryRecord>(
            r#"
            SELECT
                w.id AS work_id,
                w.kind,
                w.title,
                w.subtitle,
                w.cover_asset_id,
                h.progress,
                h.position,
                h.last_opened_at
            FROM reading_history h
            JOIN works w ON w.id = h.work_id
            ORDER BY h.last_opened_at DESC
            LIMIT ?1
            "#,
        )
        .bind(limit.clamp(1, 500))
        .fetch_all(&mut *connection)
        .await?)
    }

    pub async fn work_history(&self, work_id: i64) -> Result<Option<HistoryRecord>> {
        let mut transaction = self.begin_tracked_read_transaction().await?;
        let history = self
            .work_history_with_connection(work_id, &mut transaction)
            .await?;
        transaction.commit().await?;
        Ok(history)
    }

    async fn work_history_with_connection(
        &self,
        work_id: i64,
        connection: &mut SqliteConnection,
    ) -> Result<Option<HistoryRecord>> {
        Ok(sqlx::query_as::<_, HistoryRecord>(
            r#"
            SELECT
                w.id AS work_id,
                w.kind,
                w.title,
                w.subtitle,
                w.cover_asset_id,
                h.progress,
                h.position,
                h.last_opened_at
            FROM reading_history h
            JOIN works w ON w.id = h.work_id
            WHERE h.work_id = ?1
            "#,
        )
        .bind(work_id)
        .fetch_optional(&mut *connection)
        .await?)
    }

    pub async fn work_detail(&self, id: i64) -> Result<WorkDetail> {
        self.work_detail_with_mode(id, WorkDetailAssetMode::Legacy)
            .await
    }

    pub async fn work_detail_with_mode(
        &self,
        id: i64,
        asset_mode: WorkDetailAssetMode,
    ) -> Result<WorkDetail> {
        self.work_detail_with_snapshot(id, asset_mode).await
    }

    /// Read detail fields from one explicit SQLite snapshot.  Summary callers
    /// are fully bounded; legacy callers retain their compatibility asset
    /// semantics but no longer assemble work, counters, assets, tags and IDs
    /// from independent pool checkouts.
    async fn work_detail_with_snapshot(
        &self,
        id: i64,
        asset_mode: WorkDetailAssetMode,
    ) -> Result<WorkDetail> {
        let mut transaction = self.begin_tracked_read_transaction().await?;
        let work =
            sqlx::query_as::<_, Work>("SELECT * FROM works WHERE id = ?1 AND deleted_at IS NULL")
                .bind(id)
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or_else(|| AppError::NotFound(format!("work {id} not found")))?;
        let (assets, asset_count, track_count) = self
            .work_detail_assets_with_connection(&work, id, asset_mode, &mut transaction)
            .await?;
        let tags = sqlx::query_as::<_, Tag>(
            r#"
            SELECT t.* FROM tags t
            JOIN work_tags wt ON wt.tag_id = t.id
            WHERE wt.work_id = ?1
            ORDER BY t.namespace, t.key
            "#,
        )
        .bind(id)
        .fetch_all(&mut *transaction)
        .await?;
        let external_ids =
            sqlx::query_as::<_, ExternalId>("SELECT * FROM external_ids WHERE work_id = ?1")
                .bind(id)
                .fetch_all(&mut *transaction)
                .await?;
        let assets_complete = assets.len() as i64 == asset_count;
        transaction.commit().await?;
        Ok(WorkDetail {
            work,
            assets,
            tags,
            external_ids,
            asset_count,
            track_count,
            assets_complete,
        })
    }

    async fn work_detail_assets_with_connection(
        &self,
        work: &Work,
        id: i64,
        asset_mode: WorkDetailAssetMode,
        connection: &mut SqliteConnection,
    ) -> Result<(Vec<Asset>, i64, i64)> {
        let (asset_count, track_count) = self
            .work_asset_counts_with_connection(id, connection)
            .await?;
        let assets = if work.kind == WorkKind::Audio.as_str() {
            let mut track_assets = sqlx::query_as::<_, Asset>(
                r#"
                SELECT id, work_id, path, mime, role, variant, position, size, meta_json, created_at
                FROM (
                    SELECT id, work_id, path, mime, role, variant, position, size, meta_json, created_at
                    FROM assets
                    WHERE work_id = ?1 AND role = 'track'
                    UNION ALL
                    SELECT id, work_id, path, mime, role, variant, position, size, meta_json, created_at
                    FROM assets
                    WHERE work_id = ?1
                      AND role <> 'track'
                      AND lower(mime) LIKE 'audio/%'
                ) AS playable
                ORDER BY role, COALESCE(position, 9223372036854775807), id
                LIMIT ?2
                "#,
            )
            .bind(id)
            .bind(AUDIO_DETAIL_TRACK_LIMIT)
            .fetch_all(&mut *connection)
            .await?;
            let mut supporting_assets = sqlx::query_as::<_, Asset>(
                r#"
                SELECT * FROM assets
                WHERE work_id = ?1 AND NOT (role = 'track' OR mime LIKE 'audio/%')
                ORDER BY role, COALESCE(position, 9223372036854775807), id
                LIMIT ?2
                "#,
            )
            .bind(id)
            .bind(AUDIO_DETAIL_NON_TRACK_LIMIT)
            .fetch_all(&mut *connection)
            .await?;
            supporting_assets.append(&mut track_assets);
            supporting_assets.sort_by(|left, right| {
                left.role
                    .cmp(&right.role)
                    .then_with(|| {
                        left.position
                            .unwrap_or(i64::MAX)
                            .cmp(&right.position.unwrap_or(i64::MAX))
                    })
                    .then_with(|| left.id.cmp(&right.id))
            });
            supporting_assets
        } else if matches!(
            work.kind.as_str(),
            kind if kind == WorkKind::Comic.as_str() || kind == WorkKind::CoserPicture.as_str()
        ) {
            sqlx::query_as::<_, Asset>(
                r#"
                SELECT * FROM assets
                WHERE work_id = ?1 AND role NOT IN ('page', 'image')
                ORDER BY
                    CASE role
                        WHEN 'archive' THEN 0
                        WHEN 'cover' THEN 1
                        ELSE 2
                    END,
                    role,
                    COALESCE(position, 9223372036854775807),
                    id
                LIMIT ?2
                "#,
            )
            .bind(id)
            .bind(ARCHIVE_DETAIL_ASSET_LIMIT)
            .fetch_all(&mut *connection)
            .await?
        } else {
            let asset_query = if work.kind == WorkKind::Gallery.as_str() {
                "SELECT * FROM assets WHERE work_id = ?1 AND role != 'image' ORDER BY role, position, id"
            } else {
                "SELECT * FROM assets WHERE work_id = ?1 ORDER BY role, position, id"
            };
            match asset_mode {
                WorkDetailAssetMode::Legacy => {
                    sqlx::query_as::<_, Asset>(asset_query)
                        .bind(id)
                        .fetch_all(&mut *connection)
                        .await?
                }
                WorkDetailAssetMode::Summary => {
                    let asset_query = format!("{asset_query} LIMIT ?2");
                    sqlx::query_as::<_, Asset>(&asset_query)
                        .bind(id)
                        .bind(SUMMARY_DETAIL_ASSET_LIMIT)
                        .fetch_all(&mut *connection)
                        .await?
                }
            }
        };
        Ok((assets, asset_count, track_count))
    }

    async fn work_asset_counts_with_connection(
        &self,
        id: i64,
        connection: &mut SqliteConnection,
    ) -> Result<(i64, i64)> {
        if let Some(counts) = sqlx::query_as::<_, (i64, i64)>(
            "SELECT asset_count, track_count FROM work_stats WHERE work_id = ?1 AND computed_at IS NOT NULL",
        )
        .bind(id)
        .fetch_optional(&mut *connection)
        .await?
        {
            return Ok(counts);
        }
        let asset_count =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM assets WHERE work_id = ?1")
                .bind(id)
                .fetch_one(&mut *connection)
                .await?;
        let track_count = sqlx::query_scalar::<_, i64>(
            "SELECT
                (SELECT COUNT(*) FROM assets WHERE work_id = ?1 AND role = 'track')
                +
                (SELECT COUNT(*) FROM assets
                 WHERE work_id = ?1
                   AND role <> 'track'
                   AND lower(mime) LIKE 'audio/%')",
        )
        .bind(id)
        .fetch_one(&mut *connection)
        .await?;
        Ok((asset_count, track_count))
    }

    pub async fn work_kind_and_meta(&self, id: i64) -> Result<(String, String)> {
        sqlx::query_as::<_, (String, String)>(
            "SELECT kind, meta_json FROM works WHERE id = ?1 AND deleted_at IS NULL",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("work {id} not found")))
    }

    /// Read only the fields needed to resolve a work cover.  Cover requests
    /// are frequent for gallery grids, and loading `WorkDetail` here would
    /// materialize every image asset belonging to a large gallery.
    pub async fn work_kind_and_cover_asset_id(&self, id: i64) -> Result<(String, Option<i64>)> {
        sqlx::query_as::<_, (String, Option<i64>)>(
            "SELECT kind, cover_asset_id FROM works WHERE id = ?1 AND deleted_at IS NULL",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("work {id} not found")))
    }

    /// Resolve a gallery-grid cover from one short, tracked read snapshot.
    ///
    /// A cover request ordinarily needs the work kind plus either its explicit
    /// cover asset or, for archive-backed media, its archive fallback. Those
    /// facts must not be assembled from separate pool checkouts while a scan
    /// can replace the cover or archive row between the two reads. The helper
    /// returns at most two asset rows and never touches a work detail.
    pub async fn work_cover_source(&self, id: i64) -> Result<WorkCoverSource> {
        let mut transaction = self.begin_tracked_read_transaction().await?;
        let (kind, cover_asset_id) = sqlx::query_as::<_, (String, Option<i64>)>(
            "SELECT kind, cover_asset_id FROM works WHERE id = ?1 AND deleted_at IS NULL",
        )
        .bind(id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("work {id} not found")))?;

        // Preserve the legacy behavior for a dangling explicit cover ID: it
        // is a data-integrity failure, not a signal to silently pick another
        // source. A non-image explicit cover still falls through to the
        // archive fallback for Comic/CoserPicture.
        let explicit_cover = match cover_asset_id {
            Some(asset_id) => Some(
                self.asset_with_connection(id, asset_id, &mut transaction)
                    .await?,
            ),
            None => None,
        };
        let image_cover = explicit_cover.filter(|asset| asset.mime.starts_with("image/"));
        let archive = if image_cover.is_none() && matches!(kind.as_str(), "comic" | "coser-picture")
        {
            match self
                .work_asset_by_role_with_connection(id, "archive", None, &mut transaction)
                .await
            {
                Ok(asset) => Some(asset),
                Err(AppError::NotFound(_)) => None,
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        transaction.commit().await?;
        Ok(WorkCoverSource {
            kind,
            image_cover,
            archive,
        })
    }

    /// Read the archive source and the small work metadata needed by a
    /// Comic/CoserPicture manifest from one snapshot. Opening and parsing the
    /// ZIP remains outside the transaction, so an HDD-bound operation never
    /// pins a SQLite reader.
    pub async fn work_archive_and_meta(&self, work_id: i64) -> Result<WorkArchiveSource> {
        let mut transaction = self.begin_tracked_read_transaction().await?;
        let archive = self
            .work_asset_by_role_with_connection(work_id, "archive", None, &mut transaction)
            .await?;
        let (kind, meta_json) = sqlx::query_as::<_, (String, String)>(
            "SELECT kind, meta_json FROM works WHERE id = ?1 AND deleted_at IS NULL",
        )
        .bind(work_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("work {work_id} not found")))?;
        transaction.commit().await?;
        Ok(WorkArchiveSource {
            archive,
            kind,
            meta_json,
        })
    }

    /// Read the title and metadata needed by enrichment without materializing
    /// any of the work's assets, tags, or external IDs.
    pub async fn work_title_and_meta(&self, id: i64) -> Result<(String, String)> {
        sqlx::query_as::<_, (String, String)>(
            "SELECT title, meta_json FROM works WHERE id = ?1 AND deleted_at IS NULL",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("work {id} not found")))
    }

    pub async fn asset(&self, id: i64) -> Result<Asset> {
        sqlx::query_as::<_, Asset>(
            "SELECT assets.* FROM assets JOIN works ON works.id = assets.work_id WHERE assets.id = ?1 AND works.deleted_at IS NULL",
        )
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("asset {id} not found")))
    }

    async fn asset_with_connection(
        &self,
        work_id: i64,
        id: i64,
        connection: &mut SqliteConnection,
    ) -> Result<Asset> {
        sqlx::query_as::<_, Asset>(
            "SELECT assets.*
             FROM assets
             JOIN works ON works.id = assets.work_id
             WHERE assets.work_id = ?1
               AND assets.id = ?2
               AND works.deleted_at IS NULL",
        )
        .bind(work_id)
        .bind(id)
        .fetch_optional(&mut *connection)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("asset {id} not found")))
    }

    pub async fn work_asset_path(
        &self,
        work_id: i64,
        role: &str,
        variant: Option<&str>,
    ) -> Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            r#"
            SELECT path FROM assets
            JOIN works ON works.id = assets.work_id
            WHERE assets.work_id = ?1 AND assets.role = ?2 AND assets.variant = ?3
              AND works.deleted_at IS NULL
            ORDER BY assets.id DESC
            LIMIT 1
            "#,
        )
        .bind(work_id)
        .bind(role)
        .bind(variant.unwrap_or(""))
        .fetch_optional(&self.pool)
        .await?)
    }

    /// Fetch one source asset without materializing the work detail, tags or
    /// external IDs. Media stream and manifest routes only need the archive
    /// or book row; using the full detail query there multiplies SQLite reads
    /// on every page request, especially for large works.
    pub async fn work_asset_by_role(
        &self,
        work_id: i64,
        role: &str,
        mime: Option<&str>,
    ) -> Result<Asset> {
        sqlx::query_as::<_, Asset>(
            r#"
            SELECT assets.*
            FROM assets
            JOIN works ON works.id = assets.work_id
            WHERE assets.work_id = ?1
              AND assets.role = ?2
              AND (?3 = '' OR assets.mime = ?3)
              AND works.deleted_at IS NULL
            ORDER BY assets.id DESC
            LIMIT 1
            "#,
        )
        .bind(work_id)
        .bind(role)
        .bind(mime.unwrap_or(""))
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            AppError::NotFound(format!("asset role {role:?} not found for work {work_id}"))
        })
    }

    async fn work_asset_by_role_with_connection(
        &self,
        work_id: i64,
        role: &str,
        mime: Option<&str>,
        connection: &mut SqliteConnection,
    ) -> Result<Asset> {
        sqlx::query_as::<_, Asset>(
            r#"
            SELECT assets.*
            FROM assets
            JOIN works ON works.id = assets.work_id
            WHERE assets.work_id = ?1
              AND assets.role = ?2
              AND (?3 = '' OR assets.mime = ?3)
              AND works.deleted_at IS NULL
            ORDER BY assets.id DESC
            LIMIT 1
            "#,
        )
        .bind(work_id)
        .bind(role)
        .bind(mime.unwrap_or(""))
        .fetch_optional(&mut *connection)
        .await?
        .ok_or_else(|| {
            AppError::NotFound(format!("asset role {role:?} not found for work {work_id}"))
        })
    }

    pub async fn asset_path_reference_count(&self, path: &str) -> Result<i64> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM assets WHERE path = ?1")
                .bind(path)
                .fetch_one(&self.pool)
                .await?,
        )
    }

    pub async fn gallery_assets(
        &self,
        work_id: i64,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<Asset>> {
        let mut transaction = self.begin_tracked_read_transaction().await?;
        let assets = self
            .gallery_assets_with_connection(work_id, offset, limit, &mut transaction)
            .await?;
        transaction.commit().await?;
        Ok(assets)
    }

    async fn gallery_assets_with_connection(
        &self,
        work_id: i64,
        offset: i64,
        limit: i64,
        connection: &mut SqliteConnection,
    ) -> Result<Vec<Asset>> {
        Ok(sqlx::query_as::<_, Asset>(
            r#"
            SELECT assets.* FROM assets
            JOIN works ON works.id = assets.work_id
            WHERE assets.work_id = ?1 AND assets.role = 'image' AND assets.position >= ?3
              AND works.deleted_at IS NULL
            ORDER BY position, id
            LIMIT ?2
            "#,
        )
        .bind(work_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&mut *connection)
        .await?)
    }

    /// Fetch a gallery page after the `(position, id)` keyset boundary.  The
    /// legacy offset-based method above remains available for callers that
    /// need an explicit random-access jump, while the interactive reader can
    /// advance through very large galleries without SQLite discarding every
    /// preceding row on each request.
    pub async fn gallery_assets_after(
        &self,
        work_id: i64,
        position: i64,
        asset_id: i64,
        limit: i64,
    ) -> Result<Vec<Asset>> {
        let mut transaction = self.begin_tracked_read_transaction().await?;
        let assets = self
            .gallery_assets_after_with_connection(
                work_id,
                position,
                asset_id,
                limit,
                &mut transaction,
            )
            .await?;
        transaction.commit().await?;
        Ok(assets)
    }

    async fn gallery_assets_after_with_connection(
        &self,
        work_id: i64,
        position: i64,
        asset_id: i64,
        limit: i64,
        connection: &mut SqliteConnection,
    ) -> Result<Vec<Asset>> {
        Ok(sqlx::query_as::<_, Asset>(
            r#"
            SELECT assets.* FROM assets
            JOIN works ON works.id = assets.work_id
            WHERE assets.work_id = ?1 AND assets.role = 'image'
              AND (
                    assets.position > ?3
                    OR (assets.position = ?3 AND assets.id > ?4)
                  )
              AND works.deleted_at IS NULL
            ORDER BY assets.position, assets.id
            LIMIT ?2
            "#,
        )
        .bind(work_id)
        .bind(limit)
        .bind(position)
        .bind(asset_id)
        .fetch_all(&mut *connection)
        .await?)
    }

    /// Read the legacy gallery page, its exact/maintained count, and the
    /// revision fence from one explicit SQLite snapshot.
    pub async fn gallery_assets_page(
        &self,
        work_id: i64,
        offset: Option<i64>,
        after: Option<(i64, i64)>,
        limit: i64,
    ) -> Result<GalleryAssetsPage> {
        if offset.is_some() == after.is_some() {
            return Err(AppError::Other(
                "gallery page requires exactly one continuation mode".to_string(),
            ));
        }
        let mut transaction = self.begin_tracked_read_transaction().await?;
        let (kind, source_version) = sqlx::query_as::<_, (String, DateTime<Utc>)>(
            "SELECT kind, updated_at FROM works WHERE id = ?1 AND deleted_at IS NULL",
        )
        .bind(work_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("work {work_id} not found")))?;
        let catalog_revision =
            sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
                .fetch_one(&mut *transaction)
                .await?;
        let total = self
            .gallery_asset_count_with_connection(work_id, &mut transaction)
            .await?;
        let limit = limit.clamp(1, 241);
        let items = if let Some((position, asset_id)) = after {
            self.gallery_assets_after_with_connection(
                work_id,
                position,
                asset_id,
                limit,
                &mut transaction,
            )
            .await?
        } else {
            self.gallery_assets_with_connection(
                work_id,
                offset.unwrap_or_default().max(0),
                limit,
                &mut transaction,
            )
            .await?
        };
        transaction.commit().await?;
        Ok(GalleryAssetsPage {
            kind,
            source_version,
            catalog_revision,
            total,
            items,
        })
    }

    pub async fn gallery_asset_count(&self, work_id: i64) -> Result<i64> {
        let mut transaction = self.begin_tracked_read_transaction().await?;
        let count = self
            .gallery_asset_count_with_connection(work_id, &mut transaction)
            .await?;
        transaction.commit().await?;
        Ok(count)
    }

    async fn gallery_asset_count_with_connection(
        &self,
        work_id: i64,
        connection: &mut SqliteConnection,
    ) -> Result<i64> {
        // Gallery work_stats.image_count includes the cover asset because the
        // cover is an image too.  Once the stats row is computed, subtracting
        // the (normally single) image cover is constant-time and avoids a
        // 700k-row COUNT on every virtualized reader request.  Old or dirty
        // rows deliberately fall back to the exact fact-table count.
        if let Some(count) = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT MAX(
                0,
                stats.image_count - COALESCE((
                    SELECT COUNT(*) FROM assets AS cover
                    WHERE cover.work_id = stats.work_id
                      AND cover.role = 'cover'
                      AND cover.mime LIKE 'image/%'
                ), 0)
            )
            FROM work_stats AS stats
            JOIN works ON works.id = stats.work_id
            WHERE stats.work_id = ?1
              AND works.kind = 'gallery'
              AND works.deleted_at IS NULL
              AND stats.computed_at IS NOT NULL
            "#,
        )
        .bind(work_id)
        .fetch_optional(&mut *connection)
        .await?
        {
            return Ok(count);
        }
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM assets
             JOIN works ON works.id = assets.work_id
             WHERE assets.work_id = ?1 AND assets.role = 'image' AND works.deleted_at IS NULL",
        )
        .bind(work_id)
        .fetch_one(&mut *connection)
        .await?)
    }

    /// Return the maintained playable-track counter when it is valid, with an
    /// exact count fallback for databases that are still being backfilled.
    pub async fn track_asset_count(&self, work_id: i64) -> Result<i64> {
        if let Some(count) = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT stats.track_count
            FROM work_stats AS stats
            JOIN works ON works.id = stats.work_id
            WHERE stats.work_id = ?1
              AND works.deleted_at IS NULL
              AND stats.computed_at IS NOT NULL
            "#,
        )
        .bind(work_id)
        .fetch_optional(&self.pool)
        .await?
        {
            return Ok(count);
        }
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM assets
             WHERE work_id = ?1 AND (role = 'track' OR mime LIKE 'audio/%')",
        )
        .bind(work_id)
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn tags(&self) -> Result<Vec<Tag>> {
        let mut transaction = self.begin_tracked_read_transaction().await?;
        let tags = self.tags_with_connection(&mut transaction).await?;
        transaction.commit().await?;
        Ok(tags)
    }

    async fn tags_with_connection(&self, connection: &mut SqliteConnection) -> Result<Vec<Tag>> {
        Ok(sqlx::query_as::<_, Tag>(
            "SELECT * FROM tags WHERE count > 0 ORDER BY count DESC, namespace, key LIMIT 500",
        )
        .fetch_all(&mut *connection)
        .await?)
    }

    pub async fn jobs(&self, limit: i64) -> Result<Vec<Job>> {
        let mut transaction = self.begin_tracked_read_transaction().await?;
        let jobs = self.jobs_with_connection(limit, &mut transaction).await?;
        transaction.commit().await?;
        Ok(jobs)
    }

    async fn jobs_with_connection(
        &self,
        limit: i64,
        connection: &mut SqliteConnection,
    ) -> Result<Vec<Job>> {
        Ok(sqlx::query_as::<_, Job>(
            r#"
            SELECT * FROM jobs
            WHERE job_type NOT IN ('enrich-asmr-work', 'enrich-lightnovel-work', 'generate-image-asset')
            ORDER BY updated_at DESC, id DESC
            LIMIT ?1
            "#,
        )
        .bind(limit.clamp(1, 500))
        .fetch_all(&mut *connection)
        .await?)
    }

    pub async fn claim_next_queued_job(&self) -> Result<Option<Job>> {
        let _write_slot = self.acquire_write_slot(16 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        let result = sqlx::query_as::<_, Job>(
            r#"
            UPDATE jobs SET
                status = 'running',
                last_error = NULL,
                attempts = attempts + 1,
                updated_at = ?1
            WHERE id = (
                SELECT id FROM jobs
                WHERE status = 'queued' AND (retry_at IS NULL OR retry_at <= ?1)
                  AND (
                    job_type NOT IN ('scan-library', 'rebuild-search-index')
                    OR NOT EXISTS (
                        SELECT 1 FROM jobs running
                        WHERE running.job_type = jobs.job_type AND running.status = 'running'
                    )
                  )
                ORDER BY
                    CASE
                        WHEN job_type IN ('scan-library', 'rebuild-search-index') THEN 0
                        WHEN job_type = 'generate-image-asset' THEN 1
                        WHEN job_type = 'import-tag-translations' THEN 2
                        WHEN job_type = 'enrich-asmr-work' THEN 4
                        ELSE 9
                    END,
                    created_at ASC
                LIMIT 1
            )
            RETURNING *
            "#,
        )
        .bind(Utc::now())
        .fetch_optional(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(result)
    }

    pub async fn audit(&self, action: &str, status: &str, payload: Value) -> Result<()> {
        let _write_slot = self.acquire_write_slot(32 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        let id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO audit_logs (action, status, payload_json, created_at)
            VALUES (?1, ?2, ?3, ?4)
            RETURNING id
            "#,
        )
        .bind(action)
        .bind(status)
        .bind(payload.to_string())
        .bind(Utc::now())
        .fetch_one(&mut *transaction)
        .await?;
        if id % AUDIT_PRUNE_INTERVAL == 0 {
            if let Err(err) = self.prune_audit_logs_in_transaction(&mut transaction).await {
                tracing::warn!(error = %err, "failed to prune audit logs");
            }
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn prune_audit_logs(&self) -> Result<()> {
        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        self.prune_audit_logs_in_transaction(&mut transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn prune_audit_logs_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<()> {
        let cutoff = Utc::now() - ChronoDuration::days(AUDIT_RETENTION_DAYS);
        sqlx::query(
            r#"
            DELETE FROM audit_logs
            WHERE created_at < ?1
               OR id < COALESCE((
                    SELECT id FROM audit_logs
                    ORDER BY id DESC
                    LIMIT 1 OFFSET ?2
               ), 0)
            "#,
        )
        .bind(cutoff)
        .bind(AUDIT_MAX_RECORDS - 1)
        .execute(&mut **transaction)
        .await?;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn upsert_scanner_work_in_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    kind: &str,
    title: &str,
    source_path: Option<&str>,
    category: Option<&str>,
    description: Option<&str>,
    rating: Option<f64>,
    mut meta: Value,
    fingerprint: &str,
) -> Result<i64> {
    if let Some(object) = meta.as_object_mut() {
        object.insert("_scanner_fingerprint".to_string(), json!(fingerprint));
    }
    let row = sqlx::query(
        r#"
        INSERT INTO works (kind, title, category, description, rating, source_path, meta_json, updated_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        ON CONFLICT(kind, source_path) DO UPDATE SET
            title = excluded.title,
            category = excluded.category,
            description = COALESCE(excluded.description, works.description),
            rating = COALESCE(excluded.rating, works.rating),
            meta_json = json_patch(
                CASE WHEN json_valid(works.meta_json) THEN works.meta_json ELSE '{}' END,
                excluded.meta_json
            ),
            deleted_at = NULL,
            deleted_reason = NULL,
            updated_at = excluded.updated_at
        WHERE works.title IS NOT excluded.title
           OR works.category IS NOT excluded.category
           OR works.description IS NOT COALESCE(excluded.description, works.description)
           OR works.rating IS NOT COALESCE(excluded.rating, works.rating)
           OR works.meta_json IS NOT json_patch(
                CASE WHEN json_valid(works.meta_json) THEN works.meta_json ELSE '{}' END,
                excluded.meta_json
              )
           OR works.deleted_at IS NOT NULL
           OR works.deleted_reason IS NOT NULL
        RETURNING id
        "#,
    )
    .bind(kind)
    .bind(title)
    .bind(category)
    .bind(description)
    .bind(rating)
    .bind(source_path)
    .bind(meta.to_string())
    .bind(Utc::now())
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some(row) = row {
        return Ok(row.get(0));
    }

    // SQLite does not emit a RETURNING row when the conflict branch's WHERE
    // predicate rejects the update.  Resolve the existing identity without
    // touching the row; this is the hot path for an unchanged maintenance
    // scan and is what prevents a needless catalog revision/WAL write.
    let work_id = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM works WHERE kind = ?1 AND source_path IS ?2 LIMIT 1",
    )
    .bind(kind)
    .bind(source_path)
    .fetch_optional(&mut **transaction)
    .await?;
    work_id.ok_or_else(|| {
        AppError::Other(format!(
            "scanner upsert returned no work identity for kind {kind:?} and source path {source_path:?}"
        ))
    })
}

async fn upsert_scanner_asset_chunk_in_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    work_id: i64,
    chunk: &[ScannerAssetInput],
    seen_token: &str,
    version: chrono::DateTime<Utc>,
) -> Result<Vec<i64>> {
    if chunk.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query(
        r#"
        CREATE TEMP TABLE IF NOT EXISTS temp_scanner_asset_batch (
            ordinal INTEGER PRIMARY KEY,
            path TEXT NOT NULL,
            mime TEXT NOT NULL,
            role TEXT NOT NULL,
            variant TEXT NOT NULL,
            position INTEGER NOT NULL,
            size INTEGER,
            meta_json TEXT NOT NULL
        ) WITHOUT ROWID
        "#,
    )
    .execute(&mut **transaction)
    .await?;
    sqlx::query("DELETE FROM temp_scanner_asset_batch")
        .execute(&mut **transaction)
        .await?;

    let staged = chunk
        .iter()
        .enumerate()
        .map(|(ordinal, asset)| {
            json!({
                "ordinal": ordinal,
                "path": asset.path,
                "mime": asset.mime,
                "role": asset.role,
                "variant": asset.variant.as_deref().unwrap_or(""),
                "position": asset.position.unwrap_or(-1),
                "size": asset.size,
                "meta_json": asset.meta.to_string(),
            })
        })
        .collect::<Vec<_>>();
    let staged_json = serde_json::to_string(&staged)
        .map_err(|err| AppError::Other(format!("failed to encode scanner asset batch: {err}")))?;
    sqlx::query(
        r#"
        INSERT INTO temp_scanner_asset_batch (
            ordinal, path, mime, role, variant, position, size, meta_json
        )
        SELECT
            CAST(json_extract(input.value, '$.ordinal') AS INTEGER),
            CAST(json_extract(input.value, '$.path') AS TEXT),
            CAST(json_extract(input.value, '$.mime') AS TEXT),
            CAST(json_extract(input.value, '$.role') AS TEXT),
            CAST(json_extract(input.value, '$.variant') AS TEXT),
            CAST(json_extract(input.value, '$.position') AS INTEGER),
            CAST(json_extract(input.value, '$.size') AS INTEGER),
            CAST(json_extract(input.value, '$.meta_json') AS TEXT)
        FROM json_each(?1) AS input
        "#,
    )
    .bind(staged_json)
    .execute(&mut **transaction)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO assets (
            work_id, path, mime, role, variant, position, size, meta_json
        )
        SELECT ?1, path, mime, role, variant, position, size, meta_json
        FROM temp_scanner_asset_batch
        WHERE 1
        ON CONFLICT(work_id, path, role, variant) DO UPDATE SET
            mime = excluded.mime,
            position = excluded.position,
            size = excluded.size,
            meta_json = excluded.meta_json,
            created_at = CASE
                WHEN assets.mime IS NOT excluded.mime
                  OR json_extract(assets.meta_json, '$._source_version')
                     IS NOT json_extract(excluded.meta_json, '$._source_version')
                THEN ?2
                ELSE assets.created_at
            END
        WHERE assets.mime IS NOT excluded.mime
           OR assets.position IS NOT excluded.position
           OR assets.size IS NOT excluded.size
           OR assets.meta_json IS NOT excluded.meta_json
        "#,
    )
    .bind(work_id)
    .bind(version)
    .execute(&mut **transaction)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO scanner_assets (asset_id, work_id, seen_token)
        SELECT asset.id, ?1, ?2
        FROM temp_scanner_asset_batch AS input
        JOIN assets AS asset
          ON asset.work_id = ?1
         AND asset.path = input.path
         AND asset.role = input.role
         AND asset.variant = input.variant
        WHERE 1
        ON CONFLICT(asset_id) DO UPDATE SET
            work_id = excluded.work_id,
            seen_token = excluded.seen_token
        WHERE scanner_assets.work_id IS NOT excluded.work_id
           OR scanner_assets.seen_token IS NOT excluded.seen_token
        "#,
    )
    .bind(work_id)
    .bind(seen_token)
    .execute(&mut **transaction)
    .await?;

    let cover_asset_id = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT asset.id
        FROM temp_scanner_asset_batch AS input
        JOIN assets AS asset
          ON asset.work_id = ?1
         AND asset.path = input.path
         AND asset.role = input.role
         AND asset.variant = input.variant
        WHERE input.role = 'cover'
        ORDER BY input.ordinal DESC
        LIMIT 1
        "#,
    )
    .bind(work_id)
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some(cover_asset_id) = cover_asset_id {
        sqlx::query(
            "UPDATE works SET cover_asset_id = ?1, updated_at = ?2 WHERE id = ?3 AND cover_asset_id IS NOT ?1",
        )
        .bind(cover_asset_id)
        .bind(version)
        .bind(work_id)
        .execute(&mut **transaction)
        .await?;
    }

    let asset_ids = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT asset.id
        FROM temp_scanner_asset_batch AS input
        JOIN assets AS asset
          ON asset.work_id = ?1
         AND asset.path = input.path
         AND asset.role = input.role
         AND asset.variant = input.variant
        ORDER BY input.ordinal
        "#,
    )
    .bind(work_id)
    .fetch_all(&mut **transaction)
    .await?;
    sqlx::query("DELETE FROM temp_scanner_asset_batch")
        .execute(&mut **transaction)
        .await?;
    Ok(asset_ids)
}

#[allow(clippy::too_many_arguments)]
async fn upsert_tag_in_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    namespace: &str,
    key: &str,
    label: &str,
    translated_label: Option<&str>,
    translated_namespace: Option<&str>,
    source: &str,
    intro: Option<&str>,
    links: Option<&str>,
) -> Result<(i64, bool)> {
    let row = sqlx::query(
        r#"
        INSERT INTO tags (namespace, key, label, translated_label, translated_namespace, source, intro, links)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        ON CONFLICT(namespace, key) DO UPDATE SET
            label = excluded.label,
            translated_label = COALESCE(excluded.translated_label, tags.translated_label),
            translated_namespace = COALESCE(excluded.translated_namespace, tags.translated_namespace),
            source = excluded.source,
            intro = COALESCE(excluded.intro, tags.intro),
            links = COALESCE(excluded.links, tags.links)
        WHERE tags.label IS NOT excluded.label
           OR tags.translated_label IS NOT COALESCE(excluded.translated_label, tags.translated_label)
           OR tags.translated_namespace IS NOT COALESCE(excluded.translated_namespace, tags.translated_namespace)
           OR tags.source IS NOT excluded.source
           OR tags.intro IS NOT COALESCE(excluded.intro, tags.intro)
           OR tags.links IS NOT COALESCE(excluded.links, tags.links)
        RETURNING id
        "#,
    )
    .bind(namespace)
    .bind(key)
    .bind(label)
    .bind(translated_label)
    .bind(translated_namespace)
    .bind(source)
    .bind(intro)
    .bind(links)
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some(row) = row {
        return Ok((row.get(0), true));
    }
    let tag_id = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM tags WHERE namespace = ?1 AND key = ?2 LIMIT 1",
    )
    .bind(namespace)
    .bind(key)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| {
        AppError::Other(format!(
            "tag upsert returned no identity for namespace {namespace:?} and key {key:?}"
        ))
    })?;
    Ok((tag_id, false))
}

#[allow(clippy::too_many_arguments)]
async fn upsert_and_link_scanner_tag_in_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    work_id: i64,
    namespace: &str,
    key: &str,
    label: &str,
    translated_label: Option<&str>,
    translated_namespace: Option<&str>,
    source: &str,
    intro: Option<&str>,
    links: Option<&str>,
    seen_token: &str,
    publication: &mut SearchOutboxPublication,
) -> Result<i64> {
    let (tag_id, tag_changed) = upsert_tag_in_transaction(
        transaction,
        namespace,
        key,
        label,
        translated_label,
        translated_namespace,
        source,
        intro,
        links,
    )
    .await?;
    let linked = sqlx::query("INSERT OR IGNORE INTO work_tags (work_id, tag_id) VALUES (?1, ?2)")
        .bind(work_id)
        .bind(tag_id)
        .execute(&mut **transaction)
        .await?
        .rows_affected()
        > 0;
    sqlx::query(
        r#"
        INSERT INTO work_tag_sources (work_id, tag_id, owner, seen_token)
        VALUES (?1, ?2, 'scanner', ?3)
        ON CONFLICT(work_id, tag_id, owner) DO UPDATE SET
            seen_token = excluded.seen_token
        "#,
    )
    .bind(work_id)
    .bind(tag_id)
    .bind(seen_token)
    .execute(&mut **transaction)
    .await?;
    if tag_changed {
        enqueue_search_upsert_outbox_for_tag(transaction, tag_id, publication).await?;
    } else if linked {
        enqueue_search_upsert_outbox_for_work(transaction, work_id, publication).await?;
    }
    Ok(tag_id)
}

async fn upsert_scanner_external_id_in_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    work_id: i64,
    source: &str,
    external_id: &str,
    token: Option<&str>,
    url: Option<&str>,
    seen_token: &str,
) -> Result<()> {
    let external_id_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO external_ids (work_id, source, external_id, token, url)
        VALUES (?1, ?2, ?3, ?4, ?5)
        ON CONFLICT(work_id, source, external_id) DO UPDATE SET
            token = COALESCE(excluded.token, external_ids.token),
            url = COALESCE(excluded.url, external_ids.url)
        RETURNING id
        "#,
    )
    .bind(work_id)
    .bind(source)
    .bind(external_id)
    .bind(token)
    .bind(url)
    .fetch_one(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO external_id_sources (external_id_id, work_id, owner, seen_token)
        VALUES (?1, ?2, 'scanner', ?3)
        ON CONFLICT(external_id_id, owner) DO UPDATE SET
            work_id = excluded.work_id,
            seen_token = excluded.seen_token
        "#,
    )
    .bind(external_id_id)
    .bind(work_id)
    .bind(seen_token)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn finish_scanner_work_in_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    work_id: i64,
    scope: &str,
    seen_token: &str,
    fingerprint: &str,
    publication: &mut SearchOutboxPublication,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO scanner_works (work_id, scope, seen_token, fingerprint)
        VALUES (?1, ?2, ?3, ?4)
        ON CONFLICT(work_id) DO UPDATE SET
            scope = excluded.scope,
            seen_token = excluded.seen_token,
            fingerprint = excluded.fingerprint
        "#,
    )
    .bind(work_id)
    .bind(scope)
    .bind(seen_token)
    .bind(fingerprint)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        UPDATE works
        SET cover_asset_id = NULL
        WHERE id = ?1
          AND cover_asset_id IN (
              SELECT asset_id FROM scanner_assets
              WHERE work_id = ?1 AND seen_token != ?2
          )
        "#,
    )
    .bind(work_id)
    .bind(seen_token)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        DELETE FROM assets
        WHERE id IN (
            SELECT asset_id FROM scanner_assets
            WHERE work_id = ?1 AND seen_token != ?2
        )
        "#,
    )
    .bind(work_id)
    .bind(seen_token)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "DELETE FROM work_tag_sources WHERE work_id = ?1 AND owner = 'scanner' AND seen_token != ?2",
    )
    .bind(work_id)
    .bind(seen_token)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        DELETE FROM work_tags
        WHERE work_id = ?1
          AND NOT EXISTS (
              SELECT 1 FROM work_tag_sources source
              WHERE source.work_id = work_tags.work_id AND source.tag_id = work_tags.tag_id
          )
        "#,
    )
    .bind(work_id)
    .execute(&mut **transaction)
    .await?;
    enqueue_search_upsert_outbox_for_work(transaction, work_id, publication).await?;
    Ok(())
}

/// Mark a work for incremental search after its content transaction has
/// reached its final Catalog revision.  The scanner's asset chunks may be
/// committed separately, but its final metadata/cleanup transaction is the
/// publication fence; placing the outbox upsert here prevents a reader
/// cutover from observing a newer Catalog revision with no corresponding
/// searchable work mutation.
async fn enqueue_search_upsert_outbox_for_work(
    transaction: &mut Transaction<'_, Sqlite>,
    work_id: i64,
    publication: &mut SearchOutboxPublication,
) -> Result<()> {
    let revision =
        sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
            .fetch_one(&mut **transaction)
            .await?;
    let search_revision = publication.revision(transaction).await?;
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    sqlx::query(
        r#"
        INSERT INTO search_outbox (
            work_id, operation, catalog_revision, search_revision, payload_version,
            attempts, available_at, claimed_by, claimed_at,
            committed_at, last_error, created_at, updated_at
        )
        VALUES (?1, 'upsert', ?2, ?3, ?4, 0, ?5, NULL, NULL, NULL, NULL, ?5, ?5)
        ON CONFLICT(work_id) DO UPDATE SET
            operation = 'upsert',
            catalog_revision = excluded.catalog_revision,
            search_revision = excluded.search_revision,
            payload_version = excluded.payload_version,
            attempts = 0,
            available_at = excluded.available_at,
            claimed_by = NULL,
            claimed_at = NULL,
            committed_at = NULL,
            last_error = NULL,
            updated_at = excluded.updated_at
        WHERE search_outbox.search_revision <= excluded.search_revision
        "#,
    )
    .bind(work_id)
    .bind(revision)
    .bind(search_revision)
    .bind(crate::search::outbox::OUTBOX_PAYLOAD_VERSION)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

/// A tag label/translation participates in every linked search document.
/// Queue all live works sharing the tag when its metadata actually changes;
/// unchanged scanner maintenance therefore remains a single-work operation.
async fn enqueue_search_upsert_outbox_for_tag(
    transaction: &mut Transaction<'_, Sqlite>,
    tag_id: i64,
    publication: &mut SearchOutboxPublication,
) -> Result<()> {
    // A tag row can be maintained before it is linked to any live work.  Such
    // metadata is not part of a searchable document yet, so do not consume a
    // global Search source revision that would make every index look stale.
    let has_live_work = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT EXISTS(
            SELECT 1
            FROM work_tags
            JOIN works ON works.id = work_tags.work_id
            WHERE work_tags.tag_id = ?1
              AND works.deleted_at IS NULL
        )
        "#,
    )
    .bind(tag_id)
    .fetch_one(&mut **transaction)
    .await?;
    if has_live_work == 0 {
        return Ok(());
    }
    let revision =
        sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
            .fetch_one(&mut **transaction)
            .await?;
    let search_revision = publication.revision(transaction).await?;
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    sqlx::query(
        r#"
        INSERT INTO search_outbox (
            work_id, operation, catalog_revision, search_revision, payload_version,
            attempts, available_at, claimed_by, claimed_at,
            committed_at, last_error, created_at, updated_at
        )
        SELECT DISTINCT
            work_tags.work_id, 'upsert', ?1, ?2, ?3,
            0, ?4, NULL, NULL, NULL, NULL, ?4, ?4
        FROM work_tags
        JOIN works ON works.id = work_tags.work_id
        WHERE work_tags.tag_id = ?5
          AND works.deleted_at IS NULL
        ON CONFLICT(work_id) DO UPDATE SET
            operation = 'upsert',
            catalog_revision = excluded.catalog_revision,
            search_revision = excluded.search_revision,
            payload_version = excluded.payload_version,
            attempts = 0,
            available_at = excluded.available_at,
            claimed_by = NULL,
            claimed_at = NULL,
            committed_at = NULL,
            last_error = NULL,
            updated_at = excluded.updated_at
        WHERE search_outbox.search_revision <= excluded.search_revision
        "#,
    )
    .bind(revision)
    .bind(search_revision)
    .bind(crate::search::outbox::OUTBOX_PAYLOAD_VERSION)
    .bind(now)
    .bind(tag_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn require_scanner_lease(
    transaction: &mut Transaction<'_, Sqlite>,
    name: &str,
    token: &str,
) -> Result<()> {
    // Make the lease check the transaction's first write. A read followed by a
    // write can fail with SQLITE_BUSY_SNAPSHOT if the heartbeat commits between
    // them; this conditional renewal both fences the token and acquires the
    // SQLite writer reservation up front.
    let held =
        sqlx::query("UPDATE scanner_locks SET acquired_at = ?1 WHERE name = ?2 AND token = ?3")
            .bind(Utc::now())
            .bind(name)
            .bind(token)
            .execute(&mut **transaction)
            .await?
            .rows_affected()
            > 0;
    if !held {
        return Err(AppError::Other(format!(
            "scanner lease {name} is no longer held by this scan"
        )));
    }
    Ok(())
}

async fn prepare_scanner_tombstone_tables(transaction: &mut Transaction<'_, Sqlite>) -> Result<()> {
    for statement in [
        "CREATE TEMP TABLE IF NOT EXISTS temp_scanner_tombstone_work_ids (work_id INTEGER PRIMARY KEY) WITHOUT ROWID",
        "CREATE TEMP TABLE IF NOT EXISTS temp_scanner_tombstone_dirty_tags (tag_id INTEGER PRIMARY KEY) WITHOUT ROWID",
        "DELETE FROM temp_scanner_tombstone_work_ids",
        "DELETE FROM temp_scanner_tombstone_dirty_tags",
    ] {
        sqlx::query(statement).execute(&mut **transaction).await?;
    }
    Ok(())
}

/// Convert a set of scanner-missing work IDs into soft tombstones in one
/// bounded SQLite transaction. The caller must have already validated the
/// scanner lease and populated `temp_scanner_tombstone_work_ids`.
async fn tombstone_scanner_work_ids(
    transaction: &mut Transaction<'_, Sqlite>,
) -> Result<(i64, Vec<i64>)> {
    let base_revision =
        sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
            .fetch_one(&mut **transaction)
            .await?;
    let work_ids = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT selected.work_id
        FROM temp_scanner_tombstone_work_ids AS selected
        JOIN works AS work ON work.id = selected.work_id
        WHERE work.deleted_at IS NULL
        ORDER BY selected.work_id
        "#,
    )
    .fetch_all(&mut **transaction)
    .await?;
    if work_ids.is_empty() {
        return Ok((base_revision, work_ids));
    }

    sqlx::query(
        r#"
        INSERT OR IGNORE INTO temp_scanner_tombstone_dirty_tags(tag_id)
        SELECT work_tag.tag_id
        FROM work_tags AS work_tag
        JOIN temp_scanner_tombstone_work_ids AS selected
          ON selected.work_id = work_tag.work_id
        "#,
    )
    .execute(&mut **transaction)
    .await?;
    let now = Utc::now();
    let outbox_now = now.to_rfc3339_opts(SecondsFormat::Millis, true);
    sqlx::query(
        r#"
        UPDATE works
        SET deleted_at = ?1,
            deleted_reason = 'scanner-missing',
            cover_asset_id = NULL,
            updated_at = ?1
        WHERE id IN (SELECT work_id FROM temp_scanner_tombstone_work_ids)
          AND deleted_at IS NULL
        "#,
    )
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        DELETE FROM assets
        WHERE id IN (
            SELECT scanner.asset_id
            FROM scanner_assets AS scanner
            JOIN temp_scanner_tombstone_work_ids AS selected
              ON selected.work_id = scanner.work_id
        )
        "#,
    )
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        DELETE FROM work_tag_sources
        WHERE owner = 'scanner'
          AND work_id IN (SELECT work_id FROM temp_scanner_tombstone_work_ids)
        "#,
    )
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        DELETE FROM work_tags
        WHERE work_id IN (SELECT work_id FROM temp_scanner_tombstone_work_ids)
          AND NOT EXISTS (
              SELECT 1 FROM work_tag_sources AS source
              WHERE source.work_id = work_tags.work_id
                AND source.tag_id = work_tags.tag_id
          )
        "#,
    )
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        DELETE FROM external_id_sources
        WHERE owner = 'scanner'
          AND work_id IN (SELECT work_id FROM temp_scanner_tombstone_work_ids)
        "#,
    )
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        DELETE FROM external_ids
        WHERE work_id IN (SELECT work_id FROM temp_scanner_tombstone_work_ids)
          AND NOT EXISTS (
              SELECT 1 FROM external_id_sources AS source
              WHERE source.external_id_id = external_ids.id
          )
        "#,
    )
    .execute(&mut **transaction)
    .await?;

    let final_revision = base_revision.checked_add(1).ok_or_else(|| {
        AppError::Other("catalog revision overflowed signed 64-bit range".to_string())
    })?;
    let mut publication = SearchOutboxPublication::default();
    let search_revision = publication.revision(transaction).await?;
    sqlx::query(
        r#"
        UPDATE work_stats
        SET asset_count = (SELECT COUNT(*) FROM assets WHERE assets.work_id = work_stats.work_id),
            tag_count = (SELECT COUNT(*) FROM work_tags WHERE work_tags.work_id = work_stats.work_id),
            image_count = (SELECT COUNT(*) FROM assets WHERE assets.work_id = work_stats.work_id AND assets.mime LIKE 'image/%'),
            track_count =
                (SELECT COUNT(*) FROM assets
                 WHERE assets.work_id = work_stats.work_id
                   AND assets.role = 'track')
                +
                (SELECT COUNT(*) FROM assets
                 WHERE assets.work_id = work_stats.work_id
                   AND assets.role <> 'track'
                   AND lower(assets.mime) LIKE 'audio/%'),
            page_count = COALESCE((
                SELECT SUM(CASE
                    WHEN assets.role = 'page' THEN 1
                    WHEN assets.role = 'archive' THEN CAST(COALESCE(json_extract(assets.meta_json, '$.page_count'), 0) AS INTEGER)
                    ELSE 0 END)
                FROM assets WHERE assets.work_id = work_stats.work_id
            ), 0),
            catalog_revision = ?1,
            computed_at = NULL
        WHERE work_id IN (SELECT work_id FROM temp_scanner_tombstone_work_ids)
        "#,
    )
    .bind(final_revision)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        UPDATE tags
        SET count = (SELECT COUNT(*) FROM work_tags WHERE work_tags.tag_id = tags.id)
        WHERE id IN (SELECT tag_id FROM temp_scanner_tombstone_dirty_tags)
        "#,
    )
    .execute(&mut **transaction)
    .await?;
    sqlx::query("UPDATE catalog_state SET revision = ?1, updated_at = ?2 WHERE singleton = 1")
        .bind(final_revision)
        .bind(now)
        .execute(&mut **transaction)
        .await?;
    sqlx::query(
        r#"
        UPDATE tag_kind_count_state
        SET catalog_revision = ?1,
            updated_at = CASE WHEN ready = 0 THEN ?2 ELSE updated_at END
        WHERE singleton = 1 AND ready = 0
        "#,
    )
    .bind(final_revision)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO search_outbox (
            work_id, operation, catalog_revision, search_revision, payload_version,
            attempts, available_at, committed_at, updated_at
        )
        SELECT selected.work_id, 'delete', ?1, ?2, 1, 0, ?3, NULL, ?3
        FROM temp_scanner_tombstone_work_ids AS selected
        WHERE 1
        ON CONFLICT(work_id) DO UPDATE SET
            operation = 'delete',
            catalog_revision = excluded.catalog_revision,
            search_revision = excluded.search_revision,
            payload_version = excluded.payload_version,
            attempts = 0,
            available_at = excluded.available_at,
            claimed_by = NULL,
            claimed_at = NULL,
            committed_at = NULL,
            last_error = NULL,
            updated_at = excluded.updated_at
        WHERE search_outbox.search_revision <= excluded.search_revision
        "#,
    )
    .bind(final_revision)
    .bind(search_revision)
    .bind(&outbox_now)
    .execute(&mut **transaction)
    .await?;
    Ok((final_revision, work_ids))
}

fn encode_library_cursor(next_updated_at: chrono::DateTime<Utc>, next_id: i64) -> Result<String> {
    let encoded = serde_json::to_vec(&LibraryCursor {
        next_id,
        next_updated_at: Some(next_updated_at),
    })
    .map_err(|err| AppError::Other(format!("failed to encode library cursor: {err}")))?;
    Ok(URL_SAFE_NO_PAD.encode(encoded))
}

fn decode_library_cursor(encoded: &str) -> Result<LibraryCursor> {
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| AppError::BadRequest("invalid library cursor".to_string()))?;
    serde_json::from_slice(&bytes)
        .map_err(|_| AppError::BadRequest("invalid library cursor".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database_url(temp: &tempfile::TempDir) -> String {
        format!(
            "sqlite://{}",
            temp.path()
                .join("library.sqlite")
                .to_string_lossy()
                .replace('\\', "/")
        )
    }

    #[tokio::test]
    async fn scanner_snapshot_commits_and_cleans_owned_facts_atomically() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let token = "snapshot-token";
        let scope = "coser-picture|/library/coser";
        assert!(db
            .try_acquire_scanner_lock("library", token, 60)
            .await
            .unwrap());
        sqlx::query(
            r#"
            CREATE TABLE scanner_lease_audit (id INTEGER PRIMARY KEY);
            CREATE TRIGGER test_scanner_lease_audit
            AFTER UPDATE OF acquired_at ON scanner_locks
            BEGIN
                INSERT INTO scanner_lease_audit (id) VALUES (NULL);
            END;
            "#,
        )
        .execute(db.pool())
        .await
        .unwrap();

        db.commit_scanner_work_snapshot(
            ScannerWorkSnapshot {
                kind: "coser-picture".to_string(),
                title: "Set".to_string(),
                source_path: Some("/library/coser/set.zip".to_string()),
                category: Some("CoserPicture".to_string()),
                description: Some("artist/set.zip".to_string()),
                rating: None,
                meta: json!({ "page_count": 3 }),
                fingerprint: "snapshot-v1".to_string(),
                assets: vec![ScannerAssetInput {
                    path: "/library/coser/set.zip".to_string(),
                    mime: "application/zip".to_string(),
                    role: "archive".to_string(),
                    variant: Some("zip".to_string()),
                    position: None,
                    size: Some(123),
                    meta: json!({ "_source_version": "snapshot-v1" }),
                }],
                tags: vec![
                    ScannerTagInput {
                        namespace: "coser-picture".to_string(),
                        key: "image-set".to_string(),
                        label: "CoserPicture".to_string(),
                        source: "coser-picture-zip".to_string(),
                    },
                    ScannerTagInput {
                        namespace: "artist".to_string(),
                        key: "alice".to_string(),
                        label: "Alice".to_string(),
                        source: "coser-picture-zip".to_string(),
                    },
                ],
                external_ids: vec![],
            },
            token,
            scope,
        )
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM scanner_lease_audit")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            1
        );

        let work_id = sqlx::query_scalar::<_, i64>(
            "SELECT id FROM works WHERE kind = 'coser-picture' AND source_path = '/library/coser/set.zip'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM assets WHERE work_id = ?1 AND role = 'archive'",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM work_tag_sources WHERE work_id = ?1 AND owner = 'scanner' AND seen_token = ?2",
            )
            .bind(work_id)
            .bind(token)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT fingerprint FROM scanner_works WHERE work_id = ?1",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "snapshot-v1"
        );
        let first_outbox_revision = sqlx::query_scalar::<_, i64>(
            "SELECT catalog_revision FROM search_outbox WHERE work_id = ?1 AND operation = 'upsert'",
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(
            first_outbox_revision,
            db.revision_snapshot().await.unwrap().catalog_revision
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM search_outbox WHERE work_id = ?1 AND committed_at IS NULL",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1
        );

        db.release_scanner_lock("library", token).await.unwrap();
        let token2 = "snapshot-token-2";
        assert!(db
            .try_acquire_scanner_lock("library", token2, 60)
            .await
            .unwrap());
        db.commit_scanner_work_snapshot(
            ScannerWorkSnapshot {
                kind: "coser-picture".to_string(),
                title: "Set renamed".to_string(),
                source_path: Some("/library/coser/set.zip".to_string()),
                category: Some("CoserPicture".to_string()),
                description: None,
                rating: None,
                meta: json!({ "page_count": 4 }),
                fingerprint: "snapshot-v2".to_string(),
                assets: vec![],
                tags: vec![ScannerTagInput {
                    namespace: "coser-picture".to_string(),
                    key: "image-set".to_string(),
                    label: "CoserPicture".to_string(),
                    source: "coser-picture-zip".to_string(),
                }],
                external_ids: vec![],
            },
            token2,
            scope,
        )
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM scanner_lease_audit")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM assets WHERE work_id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM work_tag_sources WHERE work_id = ?1 AND owner = 'scanner'",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1
        );
        let second_outbox_revision = sqlx::query_scalar::<_, i64>(
            "SELECT catalog_revision FROM search_outbox WHERE work_id = ?1 AND operation = 'upsert'",
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert!(second_outbox_revision > first_outbox_revision);
        assert_eq!(
            second_outbox_revision,
            db.revision_snapshot().await.unwrap().catalog_revision
        );
    }

    #[tokio::test]
    #[ignore = "explicitly initializes a new empty database for performance fixtures"]
    async fn initialize_empty_perf_database_schema() {
        let requested = std::env::var("PERF_DATABASE_PATH")
            .expect("PERF_DATABASE_PATH must name a new empty performance database");
        let requested = PathBuf::from(requested);
        let path = if requested.is_absolute() {
            requested
        } else {
            std::env::current_dir().unwrap().join(requested)
        };
        if path.exists() {
            let metadata = std::fs::metadata(&path).unwrap();
            assert!(
                metadata.is_file() && metadata.len() == 0,
                "refusing to initialize an existing non-empty path: {}",
                path.display()
            );
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let url = format!("sqlite://{}", path.to_string_lossy().replace('\\', "/"));
        let db = Db::connect(&url).await.unwrap();
        db.migrate().await.unwrap();
        assert_eq!(db.schema_version().await.unwrap(), 24);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM works")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            0
        );
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .fetch_all(db.pool())
            .await
            .unwrap();
        db.pool().close().await;
        println!("initialized empty performance database: {}", path.display());
    }

    #[tokio::test]
    async fn migration_v17_upgrades_the_v16_progress_revision_contract() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();

        let mut rollback = db.pool().begin().await.unwrap();
        for statement in [
            "DROP TRIGGER IF EXISTS reading_history_activity_after_insert",
            "DROP TRIGGER IF EXISTS reading_history_activity_after_update",
            "DROP TRIGGER IF EXISTS reading_history_activity_after_delete",
            "DROP TABLE activity_state",
            "DROP TRIGGER IF EXISTS catalog_work_after_update",
            "DROP INDEX IF EXISTS idx_jobs_one_active_catalog_reconciliation_coser_picture",
            "DROP INDEX IF EXISTS idx_jobs_one_active_catalog_reconciliation_audio",
            "DROP INDEX IF EXISTS idx_jobs_one_active_catalog_reconciliation_gallery",
            "DROP INDEX IF EXISTS idx_inventory_present_work_cover",
            "DROP TRIGGER IF EXISTS archive_manifest_cache_after_insert",
            "DROP TRIGGER IF EXISTS archive_manifest_cache_after_update",
            "DROP TRIGGER IF EXISTS archive_manifest_cache_after_delete",
            "DROP INDEX IF EXISTS idx_archive_manifest_cache_eviction",
            "DROP TABLE IF EXISTS archive_manifest_cache_state",
            "DROP TABLE IF EXISTS archive_manifest_cache",
            // Keep this synthetic downgrade faithful to the v16 schema.  The
            // test deliberately removes migration records before re-applying
            // every later migration, so v24's additive state and columns must
            // be removed as well rather than making the production migration
            // spuriously idempotent.
            "DROP TABLE IF EXISTS search_source_state",
            "ALTER TABLE search_outbox DROP COLUMN search_revision",
            "ALTER TABLE search_index_state DROP COLUMN baseline_search_revision",
            "ALTER TABLE search_index_state DROP COLUMN applied_search_revision",
            "ALTER TABLE search_reconciliation_state DROP COLUMN search_revision",
            "ALTER TABLE search_reconciliation_state DROP COLUMN search_revision_after",
            "ALTER TABLE search_reconciliation_state DROP COLUMN applied_search_revision",
            "ALTER TABLE search_reconciliation_state DROP COLUMN applied_search_revision_after",
        ] {
            sqlx::query(statement)
                .execute(&mut *rollback)
                .await
                .unwrap();
        }
        sqlx::query(
            r#"
            CREATE TRIGGER catalog_work_after_update
            AFTER UPDATE OF kind, title, subtitle, category, rating, progress,
                            source_path, cover_asset_id, meta_json, updated_at,
                            deleted_at, deleted_reason ON works
            BEGIN
                UPDATE work_stats
                SET computed_at = CASE
                    WHEN OLD.kind IS NOT NEW.kind
                      OR OLD.title IS NOT NEW.title
                      OR OLD.source_path IS NOT NEW.source_path
                      OR OLD.meta_json IS NOT NEW.meta_json
                      OR OLD.deleted_at IS NOT NEW.deleted_at
                    THEN NULL ELSE computed_at END
                WHERE work_id = NEW.id;
                UPDATE catalog_state
                SET revision = revision + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
        )
        .execute(&mut *rollback)
        .await
        .unwrap();
        sqlx::query("ALTER TABLE library_roots DROP COLUMN audio_grouping")
            .execute(&mut *rollback)
            .await
            .unwrap();
        sqlx::query("DELETE FROM schema_migrations WHERE version >= 17")
            .execute(&mut *rollback)
            .await
            .unwrap();
        rollback.commit().await.unwrap();
        assert_eq!(db.schema_version().await.unwrap(), 16);

        // At this point the synthetic database is intentionally back at v16;
        // the v24 Search source state does not exist yet, so use the v16
        // storage contract directly instead of a current-version helper that
        // publishes a search outbox row.
        let work_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO works (kind, title, source_path, meta_json, updated_at)
            VALUES ('novel', 'v16 progress', '/novel/v16-progress.epub', '{}', ?1)
            RETURNING id
            "#,
        )
        .bind(Utc::now())
        .fetch_one(db.pool())
        .await
        .unwrap();
        let catalog_before_v16_progress =
            sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
                .fetch_one(db.pool())
                .await
                .unwrap();
        db.update_work_progress(work_id, 0.25, Some("chapter-1"), 1)
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1",)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            catalog_before_v16_progress + 1
        );

        db.migrate().await.unwrap();
        assert_eq!(db.schema_version().await.unwrap(), 24);
        let revisions_before_v17_progress = db.revision_snapshot().await.unwrap();
        db.update_work_progress(work_id, 0.5, Some("chapter-2"), 2)
            .await
            .unwrap();
        let revisions_after_v17_progress = db.revision_snapshot().await.unwrap();
        assert_eq!(
            revisions_after_v17_progress.catalog_revision,
            revisions_before_v17_progress.catalog_revision
        );
        assert_eq!(
            revisions_after_v17_progress.activity_revision,
            revisions_before_v17_progress.activity_revision + 1
        );
    }

    #[test]
    fn n100_sqlite_profile_has_a_bounded_connection_and_page_cache_budget() {
        let config = SqliteRuntimeConfig::for_profile("nas-n100-4g");
        assert_eq!(config.max_connections, 5);
        assert_eq!(config.cache_kib_per_connection, 24 * 1024);
        assert!(
            u64::from(config.max_connections) * u64::from(config.cache_kib_per_connection)
                <= 128 * 1024
        );
        assert_eq!(config.mmap_size_bytes, 64 * MIB);
        assert_eq!(config.journal_size_limit_bytes, 256 * MIB);
        assert_eq!(config.writer_queue_max_depth, 32);
        assert_eq!(config.writer_queue_max_bytes, 64 * MIB);
    }

    #[tokio::test]
    async fn audio_work_detail_is_bounded_but_reports_exact_asset_counts() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "audio",
                "large audio work",
                Some("/audio/large"),
                Some("Audio"),
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();

        let mut transaction = db.pool.begin().await.unwrap();
        sqlx::query(
            "INSERT INTO assets (work_id, path, mime, role, variant, position, size, meta_json) VALUES (?1, ?2, 'image/jpeg', 'cover', '', 0, 10, '{}')",
        )
        .bind(work_id)
        .bind("/audio/large/cover.jpg")
        .execute(&mut *transaction)
        .await
        .unwrap();
        for position in 0..300_i64 {
            sqlx::query(
                "INSERT INTO assets (work_id, path, mime, role, variant, position, size, meta_json) VALUES (?1, ?2, 'audio/mpeg', 'track', '', ?3, 100, '{\"title\":\"track\"}')",
            )
            .bind(work_id)
            .bind(format!("/audio/large/{position:04}.mp3"))
            .bind(position)
            .execute(&mut *transaction)
            .await
            .unwrap();
        }
        transaction.commit().await.unwrap();

        let detail = db.work_detail(work_id).await.unwrap();
        assert_eq!(detail.asset_count, 301);
        assert_eq!(detail.track_count, 300);
        assert!(!detail.assets_complete);
        assert_eq!(detail.assets.len(), 129);
        assert!(detail.assets.iter().any(|asset| asset.role == "cover"));
        assert_eq!(
            detail
                .assets
                .iter()
                .filter(|asset| asset.role == "track")
                .count(),
            128
        );

        // A migrated row remains safe while the bounded stats backfill is
        // pending: NULL computed_at forces exact fact-table counts instead of
        // exposing the zero-valued migration placeholder.
        sqlx::query(
            "UPDATE work_stats SET asset_count = 0, track_count = 0, computed_at = NULL WHERE work_id = ?1",
        )
        .bind(work_id)
        .execute(db.pool())
        .await
        .unwrap();
        let fallback = db.work_detail(work_id).await.unwrap();
        assert_eq!(fallback.asset_count, 301);
        assert_eq!(fallback.track_count, 300);
    }

    #[tokio::test]
    async fn archive_work_detail_excludes_page_assets_and_keeps_exact_counts() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();

        for kind in [WorkKind::Comic.as_str(), WorkKind::CoserPicture.as_str()] {
            let work_id = db
                .upsert_work(
                    kind,
                    "large archive work",
                    Some("/library/archive/work.cbz"),
                    Some("archive"),
                    None,
                    None,
                    json!({}),
                )
                .await
                .unwrap();

            let mut transaction = db.pool.begin().await.unwrap();
            for (path, mime, role) in [
                ("/library/archive/work.cbz", "application/zip", "archive"),
                ("/library/archive/cover.jpg", "image/jpeg", "cover"),
            ] {
                sqlx::query(
                    "INSERT INTO assets (work_id, path, mime, role, variant, position, size, meta_json) VALUES (?1, ?2, ?3, ?4, '', -1, 100, '{}')",
                )
                .bind(work_id)
                .bind(path)
                .bind(mime)
                .bind(role)
                .execute(&mut *transaction)
                .await
                .unwrap();
            }
            for position in 0..300_i64 {
                sqlx::query(
                    "INSERT INTO assets (work_id, path, mime, role, variant, position, size, meta_json) VALUES (?1, ?2, 'image/jpeg', 'page', '', ?3, 100, '{}')",
                )
                .bind(work_id)
                .bind(format!("/library/archive/page-{position:04}.jpg"))
                .bind(position)
                .execute(&mut *transaction)
                .await
                .unwrap();
            }
            transaction.commit().await.unwrap();

            let detail = db.work_detail(work_id).await.unwrap();
            assert_eq!(detail.asset_count, 302);
            assert_eq!(detail.track_count, 0);
            assert!(!detail.assets_complete);
            assert_eq!(detail.assets.len(), 2);
            assert!(detail.assets.iter().any(|asset| asset.role == "archive"));
            assert!(detail.assets.iter().any(|asset| asset.role == "cover"));
            assert!(detail.assets.iter().all(|asset| asset.role != "page"));
        }
    }

    #[tokio::test]
    async fn media_stream_asset_lookup_only_returns_the_requested_source_row() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                WorkKind::Comic.as_str(),
                "stream lookup fixture",
                Some("/library/stream.cbz"),
                Some("archive"),
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();

        let mut transaction = db.pool.begin().await.unwrap();
        for (path, mime, role) in [
            ("/library/stream.cbz", "application/zip", "archive"),
            ("/library/stream-cover.jpg", "image/jpeg", "cover"),
        ] {
            sqlx::query(
                "INSERT INTO assets (work_id, path, mime, role, variant, position, size, meta_json) VALUES (?1, ?2, ?3, ?4, '', -1, 100, '{}')",
            )
            .bind(work_id)
            .bind(path)
            .bind(mime)
            .bind(role)
            .execute(&mut *transaction)
            .await
            .unwrap();
        }
        transaction.commit().await.unwrap();

        let snapshot_before = db.runtime_snapshot().await.read_snapshot;
        let fallback_source = db.work_cover_source(work_id).await.unwrap();
        let snapshot_after = db.runtime_snapshot().await.read_snapshot;
        assert_eq!(fallback_source.kind, WorkKind::Comic.as_str());
        assert!(fallback_source.image_cover.is_none());
        assert_eq!(
            fallback_source
                .archive
                .as_ref()
                .map(|asset| asset.path.as_str()),
            Some("/library/stream.cbz")
        );
        assert_eq!(
            snapshot_after
                .samples
                .saturating_sub(snapshot_before.samples),
            1,
            "cover source lookup must use one tracked read snapshot"
        );
        assert_eq!(
            snapshot_after
                .completed
                .saturating_sub(snapshot_before.completed),
            1,
            "cover source snapshot must close after the bounded reads"
        );

        let (kind, cover_asset_id) = db.work_kind_and_cover_asset_id(work_id).await.unwrap();
        assert_eq!(kind, WorkKind::Comic.as_str());
        assert_eq!(cover_asset_id, None);
        let cover_asset = db.work_asset_by_role(work_id, "cover", None).await.unwrap();
        db.set_work_cover(work_id, cover_asset.id).await.unwrap();
        let (kind, cover_asset_id) = db.work_kind_and_cover_asset_id(work_id).await.unwrap();
        assert_eq!(kind, WorkKind::Comic.as_str());
        assert_eq!(cover_asset_id, Some(cover_asset.id));

        let snapshot_before = db.runtime_snapshot().await.read_snapshot;
        let explicit_source = db.work_cover_source(work_id).await.unwrap();
        let snapshot_after = db.runtime_snapshot().await.read_snapshot;
        assert_eq!(
            explicit_source
                .image_cover
                .as_ref()
                .map(|asset| asset.path.as_str()),
            Some("/library/stream-cover.jpg")
        );
        assert!(explicit_source.archive.is_none());
        assert_eq!(
            snapshot_after
                .samples
                .saturating_sub(snapshot_before.samples),
            1,
            "explicit cover lookup must not acquire a second pool snapshot"
        );
        assert_eq!(
            snapshot_after
                .implicit_rollbacks
                .saturating_sub(snapshot_before.implicit_rollbacks),
            0
        );

        let snapshot_before = db.runtime_snapshot().await.read_snapshot;
        let archive_source = db.work_archive_and_meta(work_id).await.unwrap();
        let snapshot_after = db.runtime_snapshot().await.read_snapshot;
        assert_eq!(archive_source.archive.path, "/library/stream.cbz");
        assert_eq!(archive_source.kind, WorkKind::Comic.as_str());
        assert_eq!(archive_source.meta_json, "{}");
        assert_eq!(
            snapshot_after
                .samples
                .saturating_sub(snapshot_before.samples),
            1,
            "archive and metadata lookup must use one tracked read snapshot"
        );

        let archive = db
            .work_asset_by_role(work_id, "archive", Some("application/zip"))
            .await
            .unwrap();
        assert_eq!(archive.path, "/library/stream.cbz");
        assert_eq!(archive.role, "archive");
        let cover = db.work_asset_by_role(work_id, "cover", None).await.unwrap();
        assert_eq!(cover.path, "/library/stream-cover.jpg");
        let plan = sqlx::query(
            r#"
            EXPLAIN QUERY PLAN
            SELECT assets.*
            FROM assets
            JOIN works ON works.id = assets.work_id
            WHERE assets.work_id = ?1
              AND assets.role = ?2
              AND works.deleted_at IS NULL
            ORDER BY assets.id DESC
            LIMIT 1
            "#,
        )
        .bind(work_id)
        .bind("archive")
        .fetch_all(db.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>()
        .join("\n");
        assert!(
            plan.contains("idx_assets_work_role_id"),
            "unexpected source asset query plan: {plan}"
        );
        assert!(matches!(
            db.work_asset_by_role(work_id, "book", Some("application/epub+zip"))
                .await,
            Err(AppError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn work_title_and_meta_lookup_remains_bounded_for_large_novel_assets() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                WorkKind::Novel.as_str(),
                "bounded title fixture",
                Some("/library/novels/book.epub"),
                Some("Light Novel"),
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();

        let mut transaction = db.pool.begin().await.unwrap();
        for position in 0..10_000_i64 {
            sqlx::query(
                "INSERT INTO assets (work_id, path, mime, role, variant, position, size, meta_json) VALUES (?1, ?2, 'application/xhtml+xml', 'chapter', '', ?3, 100, '{}')",
            )
            .bind(work_id)
            .bind(format!("/library/novels/book/chapter-{position:05}.xhtml"))
            .bind(position)
            .execute(&mut *transaction)
            .await
            .unwrap();
        }
        transaction.commit().await.unwrap();

        assert_eq!(
            db.work_title_and_meta(work_id).await.unwrap(),
            ("bounded title fixture".to_string(), "{}".to_string())
        );
        assert!(matches!(
            db.work_title_and_meta(i64::MAX).await,
            Err(AppError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn summary_work_detail_bounds_non_archive_assets_and_legacy_remains_complete() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                WorkKind::Novel.as_str(),
                "large summary fixture",
                Some("/novels/large.epub"),
                Some("Light Novel"),
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();

        let mut transaction = db.pool.begin().await.unwrap();
        for position in 0..64_i64 {
            sqlx::query(
                "INSERT INTO assets (work_id, path, mime, role, variant, position, size, meta_json) VALUES (?1, ?2, 'application/xhtml+xml', 'chapter', '', ?3, 100, '{}')",
            )
            .bind(work_id)
            .bind(format!("/novels/large/chapter-{position:03}.xhtml"))
            .bind(position)
            .execute(&mut *transaction)
            .await
            .unwrap();
        }
        transaction.commit().await.unwrap();

        let legacy_snapshot_before = db.runtime_snapshot().await.read_snapshot;
        let legacy = db.work_detail(work_id).await.unwrap();
        let legacy_snapshot_after = db.runtime_snapshot().await.read_snapshot;
        assert_eq!(legacy.asset_count, 64);
        assert_eq!(legacy.assets.len(), 64);
        assert!(legacy.assets_complete);
        assert_eq!(
            legacy_snapshot_after
                .samples
                .saturating_sub(legacy_snapshot_before.samples),
            1,
            "legacy detail must use one explicit tracked read snapshot"
        );
        assert_eq!(
            legacy_snapshot_after
                .completed
                .saturating_sub(legacy_snapshot_before.completed),
            1,
            "legacy detail must close its tracked read snapshot"
        );

        let read_snapshot_before = db.runtime_snapshot().await.read_snapshot;
        let summary = db
            .work_detail_with_mode(work_id, WorkDetailAssetMode::Summary)
            .await
            .unwrap();
        let read_snapshot_after = db.runtime_snapshot().await.read_snapshot;
        assert_eq!(summary.asset_count, 64);
        assert_eq!(summary.assets.len(), 16);
        assert!(!summary.assets_complete);
        assert_eq!(summary.assets[0].position, Some(0));
        assert_eq!(summary.assets[15].position, Some(15));
        assert_eq!(
            read_snapshot_after
                .samples
                .saturating_sub(read_snapshot_before.samples),
            1,
            "summary detail must use one explicit tracked read snapshot"
        );
        assert_eq!(
            read_snapshot_after
                .completed
                .saturating_sub(read_snapshot_before.completed),
            1,
            "summary detail must close its tracked read snapshot"
        );
        assert_eq!(
            read_snapshot_after
                .implicit_rollbacks
                .saturating_sub(read_snapshot_before.implicit_rollbacks),
            0,
            "successful summary detail must commit its tracked read snapshot"
        );
        assert_eq!(
            read_snapshot_after.active, read_snapshot_before.active,
            "summary detail must not leave a tracked read snapshot open"
        );
    }

    #[test]
    fn sqlite_file_urls_map_to_explicit_sidecar_paths_without_exposing_memory_databases() {
        assert_eq!(
            sqlite_database_path("sqlite:///app/data/library.sqlite?mode=rwc"),
            Some(PathBuf::from("/app/data/library.sqlite"))
        );
        assert_eq!(
            sqlite_sidecar_path(std::path::Path::new("/app/data/library.sqlite"), "-wal"),
            PathBuf::from("/app/data/library.sqlite-wal")
        );
        assert_eq!(sqlite_database_path("sqlite::memory:"), None);
    }

    #[tokio::test]
    async fn sqlite_runtime_applies_bounded_pragmas_and_reports_file_sizes() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let cache_size = sqlx::query_scalar::<_, i64>("PRAGMA cache_size")
            .fetch_one(db.pool())
            .await
            .unwrap();
        let wal_autocheckpoint = sqlx::query_scalar::<_, i64>("PRAGMA wal_autocheckpoint")
            .fetch_one(db.pool())
            .await
            .unwrap();
        let journal_size_limit = sqlx::query_scalar::<_, i64>("PRAGMA journal_size_limit")
            .fetch_one(db.pool())
            .await
            .unwrap();
        let snapshot = db.runtime_snapshot().await;
        assert_eq!(
            cache_size,
            -i64::from(snapshot.config.cache_kib_per_connection)
        );
        assert_eq!(
            wal_autocheckpoint,
            i64::from(snapshot.config.wal_autocheckpoint_pages)
        );
        assert_eq!(
            journal_size_limit,
            snapshot.config.journal_size_limit_bytes as i64
        );
        assert!(snapshot.pool_size <= snapshot.config.max_connections);
        assert_eq!(
            snapshot.active_connections,
            snapshot
                .pool_size
                .saturating_sub(snapshot.idle_connections as u32)
        );
        assert!(!snapshot.pool_saturated);
        assert!(snapshot.pool_checkout_samples > 0);
        assert!(snapshot.pool_connections_opened > 0);
        assert!(snapshot.pool_tracked_acquire_samples > 0);
        assert!(snapshot.pool_tracked_acquire_wait_total_micros > 0);
        assert!(snapshot.pool_tracked_acquire_wait_max_micros > 0);
        assert!(snapshot.database_bytes.is_some_and(|bytes| bytes > 0));
        db.optimize_query_planner().await.unwrap();
        let optimized = db.runtime_snapshot().await;
        assert_eq!(optimized.query_planner_optimize_count, 1);
        assert!(optimized.query_planner_last_optimized_at.is_some());
        assert!(optimized.query_planner_last_error.is_none());
    }

    #[tokio::test]
    async fn tracked_read_snapshot_records_active_and_committed_hold_time() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let baseline = db.runtime_snapshot().await.read_snapshot;

        let mut transaction = db.begin_tracked_read_transaction().await.unwrap();
        let active = db.runtime_snapshot().await.read_snapshot;
        assert_eq!(active.active, baseline.active + 1);
        assert_eq!(active.samples, baseline.samples + 1);
        assert!(active.oldest_active_micros > 0);

        let value = sqlx::query_scalar::<_, i64>("SELECT 1")
            .fetch_one(&mut *transaction)
            .await
            .unwrap();
        assert_eq!(value, 1);
        tokio::time::sleep(Duration::from_millis(5)).await;
        transaction.commit().await.unwrap();

        let completed = db.runtime_snapshot().await.read_snapshot;
        assert_eq!(completed.active, baseline.active);
        assert!(completed.completed > baseline.completed);
        assert!(completed.hold_total_micros >= baseline.hold_total_micros);
        assert!(completed.hold_max_micros > 0);
        assert_eq!(completed.implicit_rollbacks, baseline.implicit_rollbacks);
        assert_eq!(completed.oldest_active_micros, 0);
    }

    #[tokio::test]
    async fn dropped_tracked_read_snapshot_records_implicit_rollback() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let baseline = db.runtime_snapshot().await.read_snapshot;

        let transaction = db.begin_tracked_read_transaction().await.unwrap();
        assert_eq!(
            db.runtime_snapshot().await.read_snapshot.active,
            baseline.active + 1
        );
        drop(transaction);

        let dropped = db.runtime_snapshot().await.read_snapshot;
        assert_eq!(dropped.active, baseline.active);
        assert!(dropped.completed > baseline.completed);
        assert!(dropped.implicit_rollbacks > baseline.implicit_rollbacks);
        assert_eq!(dropped.oldest_active_micros, 0);
    }

    #[tokio::test]
    async fn failed_tracked_read_transaction_end_records_implicit_rollback() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let baseline = db.runtime_snapshot().await.read_snapshot;

        let mut transaction = db.begin_tracked_read_transaction().await.unwrap();
        sqlx::query("ROLLBACK")
            .execute(&mut *transaction)
            .await
            .unwrap();
        assert!(transaction.commit().await.is_err());

        let failed = db.runtime_snapshot().await.read_snapshot;
        assert_eq!(failed.active, baseline.active);
        assert!(failed.completed > baseline.completed);
        assert!(failed.implicit_rollbacks > baseline.implicit_rollbacks);
        assert_eq!(failed.oldest_active_micros, 0);
    }

    #[tokio::test]
    async fn write_gate_serializes_transactions_and_records_wait_evidence() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        // Migration and schema setup use the same write gate.  Capture the
        // counters after setup so this test only asserts the two guards it
        // acquires below instead of depending on migration internals.
        let baseline = db.write_snapshot();

        let first = db.acquire_write_slot(4096).await.unwrap();
        let waiting_db = db.clone();
        let waiting =
            tokio::spawn(async move { waiting_db.acquire_write_slot(8192).await.unwrap() });
        tokio::time::timeout(Duration::from_secs(1), async {
            while db.write_snapshot().queue_depth == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let queued = db.write_snapshot();
        assert_eq!(queued.queue_depth, 1);
        assert_eq!(queued.queue_bytes, 8192);
        let runtime_config = db.runtime_snapshot().await.config;
        assert_eq!(
            queued.queue_max_depth,
            runtime_config.writer_queue_max_depth
        );
        assert_eq!(
            queued.queue_max_bytes,
            runtime_config.writer_queue_max_bytes
        );
        assert_eq!(queued.active, 1);
        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
        drop(second);

        let snapshot = db.write_snapshot();
        assert_eq!(snapshot.queue_depth, 0);
        assert_eq!(snapshot.active, 0);
        assert_eq!(snapshot.active_bytes, 0);
        assert_eq!(
            snapshot
                .acquire_samples
                .saturating_sub(baseline.acquire_samples),
            2
        );
        assert!(snapshot.acquire_wait_max_micros > 0);
        assert_eq!(snapshot.completed.saturating_sub(baseline.completed), 2);
        assert!(snapshot.hold_total_micros > baseline.hold_total_micros);
    }

    #[tokio::test]
    async fn runtime_short_writes_use_tracked_acquire_and_single_writer_gate() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let baseline = db.write_snapshot();
        let runtime_baseline = db.runtime_snapshot().await;

        let work_id = db
            .upsert_work(
                "novel",
                "gated work",
                Some("/novels/gated-work.epub"),
                Some("Light Novel"),
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();
        let tag_id = db
            .upsert_tag("artist", "gated", "Gated", None, None, "test", None, None)
            .await
            .unwrap();
        assert!(work_id > 0);
        assert!(tag_id > 0);

        sqlx::query(
            "INSERT INTO jobs (job_type, status, payload_json) VALUES ('scan-library', 'running', '{}'), ('scan-library', 'running', '{}')",
        )
        .execute(db.pool())
        .await
        .unwrap();
        db.coalesce_existing_jobs().await.unwrap();

        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM jobs WHERE job_type = 'scan-library' AND status = 'superseded'",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1
        );
        let snapshot = db.write_snapshot();
        let runtime_snapshot = db.runtime_snapshot().await;
        assert_eq!(snapshot.queue_depth, 0);
        assert_eq!(snapshot.active, 0);
        assert_eq!(snapshot.completed.saturating_sub(baseline.completed), 3);
        assert!(
            runtime_snapshot
                .pool_tracked_acquire_samples
                .saturating_sub(runtime_baseline.pool_tracked_acquire_samples)
                >= 2
        );
    }

    #[tokio::test]
    async fn write_gate_rejects_requests_over_the_bounded_queue_budget() {
        let gate = Arc::new(DbWriteGate::new(1, 1024));
        let first = gate.acquire(512).await.unwrap();
        let waiting_gate = gate.clone();
        let waiting = tokio::spawn(async move { waiting_gate.acquire(512).await.unwrap() });
        tokio::time::timeout(Duration::from_secs(1), async {
            while gate.snapshot().queue_depth == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let rejected = gate.acquire(512).await;
        assert!(matches!(rejected, Err(AppError::Overloaded { .. })));
        drop(first);
        drop(
            tokio::time::timeout(Duration::from_secs(1), waiting)
                .await
                .unwrap()
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn cancelled_write_waiter_releases_queue_accounting() {
        let gate = Arc::new(DbWriteGate::new(4, 4096));
        let first = gate.acquire(1024).await.unwrap();
        let waiting_gate = gate.clone();
        let waiting = tokio::spawn(async move { waiting_gate.acquire(2048).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while gate.snapshot().queue_depth == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        waiting.abort();
        let _ = waiting.await;
        tokio::task::yield_now().await;
        assert_eq!(gate.snapshot().queue_depth, 0);
        assert_eq!(gate.snapshot().queue_bytes, 0);
        drop(first);
    }

    #[tokio::test]
    async fn work_covers_cannot_reference_an_asset_from_another_work() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let first_work = db
            .upsert_work(
                WorkKind::Comic.as_str(),
                "first cover work",
                Some("/library/first.cbz"),
                Some("archive"),
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();
        let second_work = db
            .upsert_work(
                WorkKind::Comic.as_str(),
                "second cover work",
                Some("/library/second.cbz"),
                Some("archive"),
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();
        let first_cover = db
            .upsert_asset(
                first_work,
                "/library/first-cover.jpg",
                "image/jpeg",
                "cover",
                None,
                Some(0),
                Some(10),
                json!({}),
            )
            .await
            .unwrap();
        let second_cover = db
            .upsert_asset(
                second_work,
                "/library/second-cover.jpg",
                "image/jpeg",
                "cover",
                None,
                Some(0),
                Some(10),
                json!({}),
            )
            .await
            .unwrap();

        let error = db
            .set_work_cover(first_work, second_cover)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            AppError::NotFound(message)
                if message == format!("asset {second_cover} not found for work {first_work}")
        ));
        assert_eq!(
            sqlx::query_scalar::<_, Option<i64>>("SELECT cover_asset_id FROM works WHERE id = ?1",)
                .bind(first_work)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            Some(first_cover)
        );

        sqlx::query("UPDATE works SET cover_asset_id = ?1 WHERE id = ?2")
            .bind(second_cover)
            .bind(first_work)
            .execute(db.pool())
            .await
            .unwrap();
        let error = db.work_cover_source(first_work).await.unwrap_err();
        assert!(matches!(
            error,
            AppError::NotFound(message)
                if message == format!("asset {second_cover} not found")
        ));
    }

    #[tokio::test]
    async fn passive_wal_checkpoint_records_reader_evidence() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let before = db.write_snapshot();
        let snapshot = db.checkpoint_wal().await.unwrap();
        assert_eq!(snapshot.attempts, 1);
        assert_eq!(snapshot.successes, 1);
        assert_eq!(snapshot.failures, 0);
        assert!(snapshot.last_duration_micros < 5_000_000);
        assert!(snapshot.last_busy_pages >= 0);
        assert!(snapshot.last_log_pages >= 0);
        assert!(snapshot.last_checkpointed_pages >= 0);
        assert_eq!(db.runtime_snapshot().await.wal_checkpoint.attempts, 1);
        let after = db.write_snapshot();
        assert_eq!(after.queue_depth, 0);
        assert_eq!(after.active, 0);
        assert_eq!(after.completed, before.completed + 1);
    }

    #[tokio::test]
    async fn migrates_reading_history_update_tokens() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        sqlx::query(
            r#"
            CREATE TABLE reading_history (
                work_id INTEGER PRIMARY KEY,
                progress REAL NOT NULL DEFAULT 0,
                position TEXT,
                last_opened_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )
            "#,
        )
        .execute(db.pool())
        .await
        .unwrap();

        db.migrate().await.unwrap();
        let columns = sqlx::query("PRAGMA table_info(reading_history)")
            .fetch_all(db.pool())
            .await
            .unwrap();
        assert!(columns
            .iter()
            .any(|row| row.get::<String, _>("name") == "update_token"));
    }

    #[tokio::test]
    async fn migrates_duplicate_asset_identity_and_preserves_cover() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        sqlx::query(
            r#"
            CREATE TABLE works (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                title TEXT NOT NULL,
                subtitle TEXT,
                category TEXT,
                description TEXT,
                rating REAL,
                progress REAL NOT NULL DEFAULT 0,
                source_path TEXT,
                cover_asset_id INTEGER,
                meta_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                UNIQUE(kind, source_path)
            )
            "#,
        )
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
                path TEXT NOT NULL,
                mime TEXT NOT NULL,
                role TEXT NOT NULL,
                variant TEXT NOT NULL DEFAULT '',
                position INTEGER NOT NULL DEFAULT -1,
                size INTEGER,
                meta_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                UNIQUE(work_id, path, role, variant, position)
            )
            "#,
        )
        .execute(db.pool())
        .await
        .unwrap();
        let work_id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO works (kind, title, source_path) VALUES ('gallery', 'set', '/set') RETURNING id",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        let cover_id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO assets (work_id, path, mime, role, variant, position) VALUES (?1, '/set/a.jpg', 'image/jpeg', 'cover', '', 0) RETURNING id",
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO assets (work_id, path, mime, role, variant, position) VALUES (?1, '/set/a.jpg', 'image/jpeg', 'cover', '', 1)",
        )
        .bind(work_id)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE works SET cover_asset_id = ?1 WHERE id = ?2")
            .bind(cover_id)
            .bind(work_id)
            .execute(db.pool())
            .await
            .unwrap();

        db.migrate().await.unwrap();

        let assets =
            sqlx::query_as::<_, (i64, i64)>("SELECT id, position FROM assets WHERE work_id = ?1")
                .bind(work_id)
                .fetch_all(db.pool())
                .await
                .unwrap();
        assert_eq!(assets, vec![(cover_id, 0)]);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT cover_asset_id FROM works WHERE id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            cover_id
        );

        let same_id = db
            .upsert_asset(
                work_id,
                "/set/a.jpg",
                "image/jpeg",
                "cover",
                None,
                Some(7),
                None,
                json!({}),
            )
            .await
            .unwrap();
        assert_eq!(same_id, cover_id);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT position FROM assets WHERE id = ?1")
                .bind(cover_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            7
        );
    }

    #[tokio::test]
    async fn scanner_updates_preserve_metadata_and_external_tags() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "audio",
                "work",
                Some("/audio/work"),
                Some("Audio"),
                None,
                None,
                json!({
                    "page_count": 1,
                    "runtime": { "bookmark": 42 },
                    "enrichment": { "provider": "asmr.one" }
                }),
            )
            .await
            .unwrap();
        db.upsert_work(
            "audio",
            "work rescanned",
            Some("/audio/work"),
            Some("Audio"),
            None,
            None,
            json!({ "page_count": 2, "scanner": { "version": 2 } }),
        )
        .await
        .unwrap();
        let metadata = sqlx::query_scalar::<_, String>("SELECT meta_json FROM works WHERE id = ?1")
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
        let metadata: Value = serde_json::from_str(&metadata).unwrap();
        assert_eq!(metadata["page_count"], 2);
        assert_eq!(metadata["runtime"]["bookmark"], 42);
        assert_eq!(metadata["enrichment"]["provider"], "asmr.one");

        let scanner_tag = db
            .upsert_tag(
                "audio",
                "asmr",
                "ASMR",
                None,
                None,
                "audio-folder",
                None,
                None,
            )
            .await
            .unwrap();
        let external_tag = db
            .upsert_tag(
                "provider", "asmr-one", "asmr.one", None, None, "asmr.one", None, None,
            )
            .await
            .unwrap();
        db.link_tag(work_id, scanner_tag).await.unwrap();
        db.link_tag(work_id, external_tag).await.unwrap();
        db.mark_scanner_work(work_id, "legacy|audio", "legacy", None)
            .await
            .unwrap();
        sqlx::query("DELETE FROM work_tag_sources WHERE work_id = ?1")
            .bind(work_id)
            .execute(db.pool())
            .await
            .unwrap();

        db.migrate_scanner_ownership().await.unwrap();

        let owners = sqlx::query_as::<_, (i64, String)>(
            "SELECT tag_id, owner FROM work_tag_sources WHERE work_id = ?1 ORDER BY tag_id, owner",
        )
        .bind(work_id)
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(
            owners,
            vec![
                (scanner_tag, "scanner".to_string()),
                (external_tag, "external".to_string())
            ]
        );

        assert!(db
            .try_acquire_scanner_lock("library", "new-scan", 3600)
            .await
            .unwrap());
        db.finish_scanner_work(work_id, "legacy|audio", "new-scan", "fingerprint")
            .await
            .unwrap();
        let remaining_tags = sqlx::query_scalar::<_, i64>(
            "SELECT tag_id FROM work_tags WHERE work_id = ?1 ORDER BY tag_id",
        )
        .bind(work_id)
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(remaining_tags, vec![external_tag]);
    }

    #[tokio::test]
    async fn refresh_tag_counts_skips_unchanged_rows() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "gallery",
                "tag count work",
                Some("/gallery/tag-count-work"),
                Some("Gallery"),
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();
        let linked = db
            .upsert_tag("artist", "linked", "Linked", None, None, "test", None, None)
            .await
            .unwrap();
        let stale = db
            .upsert_tag("artist", "stale", "Stale", None, None, "test", None, None)
            .await
            .unwrap();
        db.link_tag(work_id, linked).await.unwrap();
        sqlx::query("UPDATE tags SET count = 99 WHERE id = ?1")
            .bind(stale)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE tag_count_update_audit (tag_id INTEGER NOT NULL);
            CREATE TRIGGER test_tag_count_update_audit
            AFTER UPDATE OF count ON tags
            BEGIN
                INSERT INTO tag_count_update_audit (tag_id) VALUES (NEW.id);
            END;
            "#,
        )
        .execute(db.pool())
        .await
        .unwrap();

        db.refresh_tag_counts().await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count FROM tags WHERE id = ?1")
                .bind(linked)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count FROM tags WHERE id = ?1")
                .bind(stale)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tag_count_update_audit")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT tag_id FROM tag_count_update_audit LIMIT 1",)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            stale
        );

        // A second reconciliation with no tag membership changes must not
        // rewrite even the already-correct rows.
        db.refresh_tag_counts().await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tag_count_update_audit")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn unchanged_scanner_work_does_not_advance_catalog_revision() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        assert!(db
            .try_acquire_scanner_lock("library", "unchanged-work", 3600)
            .await
            .unwrap());

        let first_id = db
            .upsert_scanner_work(
                "novel",
                "Stable title",
                Some("/novels/stable.epub"),
                Some("Novel"),
                Some("Stable description"),
                Some(4.5),
                json!({ "chapter_count": 12 }),
                "unchanged-work",
                "stable-v1",
            )
            .await
            .unwrap();
        let first_revision =
            sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
                .fetch_one(db.pool())
                .await
                .unwrap();
        let first_updated_at =
            sqlx::query_scalar::<_, String>("SELECT updated_at FROM works WHERE id = ?1")
                .bind(first_id)
                .fetch_one(db.pool())
                .await
                .unwrap();

        let repeated_id = db
            .upsert_scanner_work(
                "novel",
                "Stable title",
                Some("/novels/stable.epub"),
                Some("Novel"),
                Some("Stable description"),
                Some(4.5),
                json!({ "chapter_count": 12 }),
                "unchanged-work",
                "stable-v1",
            )
            .await
            .unwrap();
        assert_eq!(repeated_id, first_id);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1",)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            first_revision
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT updated_at FROM works WHERE id = ?1")
                .bind(first_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            first_updated_at
        );

        let changed_id = db
            .upsert_scanner_work(
                "novel",
                "Changed title",
                Some("/novels/stable.epub"),
                Some("Novel"),
                Some("Stable description"),
                Some(4.5),
                json!({ "chapter_count": 13 }),
                "unchanged-work",
                "stable-v2",
            )
            .await
            .unwrap();
        assert_eq!(changed_id, first_id);
        assert!(
            sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1",)
                .fetch_one(db.pool())
                .await
                .unwrap()
                > first_revision
        );
        assert_ne!(
            sqlx::query_scalar::<_, String>("SELECT updated_at FROM works WHERE id = ?1")
                .bind(first_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            first_updated_at
        );
        db.release_scanner_lock("library", "unchanged-work")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn stale_scanner_lease_cannot_mutate_work_assets_or_tags() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "gallery",
                "current title",
                Some("/gallery/set"),
                Some("Gallery"),
                None,
                None,
                json!({ "generation": "current" }),
            )
            .await
            .unwrap();

        assert!(db
            .try_acquire_scanner_lock("library", "stale", 60)
            .await
            .unwrap());
        db.release_scanner_lock("library", "stale").await.unwrap();
        assert!(db
            .try_acquire_scanner_lock("library", "current", 60)
            .await
            .unwrap());

        assert!(db
            .upsert_scanner_work(
                "gallery",
                "stale title",
                Some("/gallery/set"),
                Some("Gallery"),
                None,
                None,
                json!({ "generation": "stale" }),
                "stale",
                "stale-fingerprint",
            )
            .await
            .is_err());
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT title FROM works WHERE id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "current title"
        );

        assert!(db
            .upsert_scanner_asset(
                work_id,
                "/gallery/set/stale.jpg",
                "image/jpeg",
                "image",
                None,
                Some(0),
                Some(10),
                json!({}),
                "stale",
            )
            .await
            .is_err());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM assets WHERE work_id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            0
        );

        assert!(db
            .upsert_and_link_scanner_tag(
                work_id,
                "folder",
                "stale",
                "stale",
                None,
                None,
                "scanner-test",
                None,
                None,
                "stale",
            )
            .await
            .is_err());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM work_tags WHERE work_id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn current_scanner_tag_reads_lease_inside_the_tracked_write_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "gallery",
                "current scanner tag fixture",
                Some("/gallery/current-scanner-tag"),
                Some("Gallery"),
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();
        let tag_id = db
            .upsert_tag(
                "folder",
                "current",
                "current",
                None,
                None,
                "scanner-test",
                None,
                None,
            )
            .await
            .unwrap();
        assert!(db
            .try_acquire_scanner_lock("library", "current-token", 60)
            .await
            .unwrap());

        let before = db.runtime_snapshot().await;
        db.link_current_scanner_tag(work_id, tag_id).await.unwrap();
        let after = db.runtime_snapshot().await;

        assert_eq!(
            after
                .pool_tracked_acquire_samples
                .saturating_sub(before.pool_tracked_acquire_samples),
            1,
            "current scanner tag must use one tracked writer transaction"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT seen_token FROM work_tag_sources WHERE work_id = ?1 AND tag_id = ?2 AND owner = 'scanner'",
            )
            .bind(work_id)
            .bind(tag_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "current-token"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM work_tags WHERE work_id = ?1 AND tag_id = ?2",
            )
            .bind(work_id)
            .bind(tag_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn enrichment_commit_is_fenced_by_scanner_fingerprint() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "novel",
                "local title",
                Some("/novels/book.epub"),
                Some("Light Novel"),
                None,
                None,
                json!({ "local": true }),
            )
            .await
            .unwrap();
        db.mark_scanner_work(work_id, "novel|/novels", "scan", Some("current"))
            .await
            .unwrap();

        let stale = ScannerEnrichmentInput {
            title: Some("stale remote title".to_string()),
            category: None,
            description: None,
            rating: None,
            meta: json!({ "remote": "stale" }),
            tags: vec![EnrichmentTagInput {
                namespace: "ln".to_string(),
                key: "stale".to_string(),
                label: "stale".to_string(),
                source: "test".to_string(),
            }],
            external_ids: vec![EnrichmentExternalIdInput {
                source: "test".to_string(),
                external_id: "stale".to_string(),
                token: None,
                url: None,
            }],
        };
        assert!(!db
            .apply_scanner_enrichment(work_id, "old", stale)
            .await
            .unwrap());
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT title FROM works WHERE id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "local title"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM work_tags WHERE work_id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            0
        );

        let current = ScannerEnrichmentInput {
            title: Some("current remote title".to_string()),
            category: None,
            description: None,
            rating: Some(4.5),
            meta: json!({ "remote": "current" }),
            tags: vec![EnrichmentTagInput {
                namespace: "ln".to_string(),
                key: "current".to_string(),
                label: "current".to_string(),
                source: "test".to_string(),
            }],
            external_ids: vec![EnrichmentExternalIdInput {
                source: "test".to_string(),
                external_id: "current".to_string(),
                token: None,
                url: Some("https://example.test/current".to_string()),
            }],
        };
        assert!(db
            .apply_scanner_enrichment(work_id, "current", current)
            .await
            .unwrap());
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT title FROM works WHERE id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "current remote title"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM work_tags WHERE work_id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM external_ids WHERE work_id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT operation FROM search_outbox WHERE work_id = ?1",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "upsert"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT catalog_revision FROM search_outbox WHERE work_id = ?1",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            db.revision_snapshot().await.unwrap().catalog_revision
        );

        // A new scan publishes its generation marker before its final
        // scanner_works fingerprint. The old response must be rejected in that
        // in-between window as well.
        sqlx::query(
            "UPDATE works SET meta_json = json_set(meta_json, '$._scanner_fingerprint', 'next') WHERE id = ?1",
        )
        .bind(work_id)
        .execute(db.pool())
        .await
        .unwrap();
        let late_current = ScannerEnrichmentInput {
            title: Some("late old response".to_string()),
            category: None,
            description: None,
            rating: None,
            meta: json!({ "_scanner_fingerprint": "current" }),
            tags: Vec::new(),
            external_ids: Vec::new(),
        };
        assert!(!db
            .apply_scanner_enrichment(work_id, "current", late_current)
            .await
            .unwrap());
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT title FROM works WHERE id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "current remote title"
        );
    }

    #[tokio::test]
    async fn scanner_refreshes_only_changed_asset_versions() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "gallery",
                "set",
                Some("/gallery/set"),
                Some("Gallery"),
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();
        assert!(db
            .try_acquire_scanner_lock("library", "asset-version", 60)
            .await
            .unwrap());

        let assets = |second_version: &str| {
            vec![
                ScannerAssetInput {
                    path: "/gallery/set/one.jpg".to_string(),
                    mime: "image/jpeg".to_string(),
                    role: "image".to_string(),
                    variant: None,
                    position: Some(0),
                    size: Some(3),
                    meta: json!({ "_source_version": "one-v1" }),
                },
                ScannerAssetInput {
                    path: "/gallery/set/two.jpg".to_string(),
                    mime: "image/jpeg".to_string(),
                    role: "image".to_string(),
                    variant: None,
                    position: Some(1),
                    size: Some(3),
                    meta: json!({ "_source_version": second_version }),
                },
            ]
        };
        db.upsert_scanner_assets(work_id, assets("two-v1"), "asset-version")
            .await
            .unwrap();
        sqlx::query("UPDATE assets SET created_at = '2000-01-01T00:00:00Z' WHERE work_id = ?1")
            .bind(work_id)
            .execute(db.pool())
            .await
            .unwrap();
        db.upsert_scanner_assets(work_id, assets("two-v2"), "asset-version")
            .await
            .unwrap();

        let versions = sqlx::query_as::<_, (String, String)>(
            "SELECT path, created_at FROM assets WHERE work_id = ?1 ORDER BY path",
        )
        .bind(work_id)
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(versions[0].1, "2000-01-01T00:00:00Z");
        assert_ne!(versions[1].1, "2000-01-01T00:00:00Z");
    }

    #[tokio::test]
    async fn scanner_asset_upsert_skips_unchanged_rows_and_cover_writes() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "gallery",
                "write amplification",
                Some("/gallery/write-amplification"),
                Some("Gallery"),
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();
        assert!(db
            .try_acquire_scanner_lock("library", "write-amplification", 60)
            .await
            .unwrap());

        // These test-only triggers make an otherwise invisible SQLite UPDATE
        // observable. They verify that the conflict paths do not fire when
        // every incoming value is identical to the stored row.
        sqlx::query(
            r#"
            CREATE TABLE asset_update_audit (asset_id INTEGER NOT NULL);
            CREATE TABLE cover_update_audit (work_id INTEGER NOT NULL);
            CREATE TRIGGER test_asset_update_audit
            AFTER UPDATE OF mime, role, position, size, meta_json ON assets
            BEGIN
                INSERT INTO asset_update_audit (asset_id) VALUES (NEW.id);
            END;
            CREATE TRIGGER test_cover_update_audit
            AFTER UPDATE OF cover_asset_id ON works
            BEGIN
                INSERT INTO cover_update_audit (work_id) VALUES (NEW.id);
            END;
            "#,
        )
        .execute(db.pool())
        .await
        .unwrap();

        let inputs = |second_version: &str, second_size: i64| {
            vec![
                ScannerAssetInput {
                    path: "/gallery/write-amplification/cover.jpg".to_string(),
                    mime: "image/jpeg".to_string(),
                    role: "cover".to_string(),
                    variant: None,
                    position: Some(0),
                    size: Some(10),
                    meta: json!({ "_source_version": "cover-v1" }),
                },
                ScannerAssetInput {
                    path: "/gallery/write-amplification/page.jpg".to_string(),
                    mime: "image/jpeg".to_string(),
                    role: "image".to_string(),
                    variant: None,
                    position: Some(1),
                    size: Some(second_size),
                    meta: json!({ "_source_version": second_version }),
                },
            ]
        };

        let first = db
            .upsert_scanner_assets(work_id, inputs("page-v1", 20), "write-amplification")
            .await
            .unwrap();
        assert_eq!(first.len(), 2);
        sqlx::query("DELETE FROM asset_update_audit")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM cover_update_audit")
            .execute(db.pool())
            .await
            .unwrap();

        let revision_before =
            sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
                .fetch_one(db.pool())
                .await
                .unwrap();
        let stats_before = sqlx::query_as::<_, (i64, i64, i64, i64, Option<String>)>(
            "SELECT asset_count, image_count, track_count, page_count, computed_at FROM work_stats WHERE work_id = ?1",
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap();

        // A repeated scan of an unchanged work should only refresh the
        // scanner fence; it must not update assets, works, stats, or catalog
        // revision.
        let second = db
            .upsert_scanner_assets(work_id, inputs("page-v1", 20), "write-amplification")
            .await
            .unwrap();
        assert_eq!(second, first);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM asset_update_audit")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM cover_update_audit")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1",)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            revision_before
        );
        assert_eq!(
            sqlx::query_as::<_, (i64, i64, i64, i64, Option<String>)>(
                "SELECT asset_count, image_count, track_count, page_count, computed_at FROM work_stats WHERE work_id = ?1",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            stats_before
        );

        // Changing one asset must still update exactly that asset, while the
        // unchanged cover remains untouched.
        db.upsert_scanner_assets(work_id, inputs("page-v2", 21), "write-amplification")
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM asset_update_audit")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM cover_update_audit")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM assets WHERE work_id = ?1 AND size = 21",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM assets WHERE work_id = ?1 AND size = 20",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn scanner_asset_set_merge_preserves_ids_order_and_cover_across_chunks() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "gallery",
                "large set",
                Some("/gallery/large-set"),
                Some("Gallery"),
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();
        assert!(db
            .try_acquire_scanner_lock("library", "asset-batch", 60)
            .await
            .unwrap());

        let inputs = |version: &str| {
            (0..1025)
                .map(|position| ScannerAssetInput {
                    path: format!("/gallery/large-set/{position:04}.jpg"),
                    mime: "image/jpeg".to_string(),
                    role: if matches!(position, 510 | 1024) {
                        "cover".to_string()
                    } else {
                        "image".to_string()
                    },
                    variant: None,
                    position: Some(position),
                    size: Some(3),
                    meta: json!({ "_source_version": format!("{version}-{position}") }),
                })
                .collect::<Vec<_>>()
        };

        let first = db
            .upsert_scanner_assets(work_id, inputs("v1"), "asset-batch")
            .await
            .unwrap();
        assert_eq!(first.len(), 1025);
        assert_eq!(
            first
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            first.len()
        );
        let stored = sqlx::query_as::<_, (i64, i64)>(
            "SELECT id, position FROM assets WHERE work_id = ?1 ORDER BY position",
        )
        .bind(work_id)
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(stored.iter().map(|row| row.0).collect::<Vec<_>>(), first);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT cover_asset_id FROM works WHERE id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            first[1024]
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM scanner_assets WHERE work_id = ?1 AND seen_token = ?2",
            )
            .bind(work_id)
            .bind("asset-batch")
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1025
        );

        let second = db
            .upsert_scanner_assets(work_id, inputs("v2"), "asset-batch")
            .await
            .unwrap();
        assert_eq!(second, first);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM assets WHERE work_id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            1025
        );
    }

    #[tokio::test]
    async fn tag_context_excludes_unreferenced_translation_rows() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "comic",
                "tag context",
                Some("/comic/tag-context"),
                None,
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();
        let used = db
            .upsert_tag("test", "used", "used", None, None, "test", None, None)
            .await
            .unwrap();
        db.upsert_tag(
            "test",
            "unused",
            "unused",
            None,
            None,
            "translation",
            None,
            None,
        )
        .await
        .unwrap();
        db.link_tag(work_id, used).await.unwrap();

        let tags = db.tags().await.unwrap();
        assert_eq!(
            tags.iter().map(|tag| tag.key.as_str()).collect::<Vec<_>>(),
            vec!["used"]
        );
    }

    #[tokio::test]
    async fn revision_fence_snapshot_reads_catalog_and_search_from_one_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();

        let before = db.runtime_snapshot().await.read_snapshot;
        let fence = db.revision_fence_snapshot().await.unwrap();
        let after = db.runtime_snapshot().await.read_snapshot;

        assert!(fence.catalog_revision >= 0);
        assert!(fence.activity_revision >= 0);
        assert!(fence.search_revision >= 0);
        assert_eq!(
            after.samples.saturating_sub(before.samples),
            1,
            "revision fence must use one tracked read snapshot"
        );
        assert_eq!(
            after.completed.saturating_sub(before.completed),
            1,
            "revision fence snapshot must close after both counters are read"
        );
        assert_eq!(after.active, before.active);
    }

    #[tokio::test]
    async fn search_source_revision_only_advances_for_live_tag_facts() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();

        let catalog_before = db.revision_snapshot().await.unwrap().catalog_revision;
        let search_before = db.revision_fence_snapshot().await.unwrap().search_revision;
        let unused = db
            .upsert_tag(
                "test",
                "unreferenced-revision",
                "Before",
                None,
                None,
                "test",
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            db.revision_snapshot().await.unwrap().catalog_revision,
            catalog_before
        );
        assert_eq!(
            db.revision_fence_snapshot().await.unwrap().search_revision,
            search_before
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM search_outbox WHERE work_id IN (SELECT work_id FROM work_tags WHERE tag_id = ?1)",
            )
            .bind(unused)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            0
        );

        let work_id = db
            .upsert_work(
                "novel",
                "live tag revision",
                Some("/novels/live-tag-revision.epub"),
                None,
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();
        db.link_tag(work_id, unused).await.unwrap();
        let linked_revision = db.revision_fence_snapshot().await.unwrap().search_revision;
        assert!(linked_revision > search_before);

        db.upsert_tag(
            "test",
            "unreferenced-revision",
            "After",
            None,
            None,
            "test",
            None,
            None,
        )
        .await
        .unwrap();
        let changed_revision = db.revision_fence_snapshot().await.unwrap().search_revision;
        assert!(changed_revision > linked_revision);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT search_revision FROM search_outbox WHERE work_id = ?1",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            changed_revision
        );
    }

    #[tokio::test]
    async fn library_keyset_pages_are_complete_and_context_is_optional() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let base_time = Utc::now() - ChronoDuration::minutes(10);
        // Deliberately make the updated-at order disagree with insertion/id
        // order.  The first page and every continuation must still expose
        // one stable `(updated_at DESC, id DESC)` sequence.
        let updated_ranks = [2_i64, 4, 0, 3, 1];
        let mut expected = Vec::new();
        for (index, updated_rank) in updated_ranks.into_iter().enumerate() {
            let id = db
                .upsert_work(
                    "comic",
                    &format!("work {index}"),
                    Some(&format!("/comic/{index}")),
                    None,
                    None,
                    None,
                    json!({}),
                )
                .await
                .unwrap();
            sqlx::query("UPDATE works SET updated_at = ?1 WHERE id = ?2")
                .bind(base_time + ChronoDuration::seconds(updated_rank))
                .bind(id)
                .execute(db.pool())
                .await
                .unwrap();
            expected.push((id, updated_rank));
        }
        expected.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| right.0.cmp(&left.0)));
        let expected = expected.into_iter().map(|(id, _)| id).collect::<Vec<_>>();
        let expected_ids = expected
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        db.create_job("test-job", "queued", json!({}))
            .await
            .unwrap();

        let mut cursor = None;
        let mut actual = std::collections::BTreeSet::new();
        let mut actual_order = Vec::new();
        let mut inserted_after_snapshot = None;
        loop {
            let include_context = cursor.is_none();
            let page = db
                .library_page(cursor.as_deref(), 2, include_context)
                .await
                .unwrap();
            if include_context {
                assert_eq!(page.jobs.len(), 1);
            } else {
                assert!(page.jobs.is_empty());
                assert!(page.tags.is_empty());
                assert!(page.history.is_empty());
            }
            let page_ids = page
                .works
                .into_iter()
                .map(|work| work.id)
                .collect::<Vec<_>>();
            actual.extend(page_ids.iter().copied());
            actual_order.extend(page_ids);
            cursor = page.next_cursor;
            if inserted_after_snapshot.is_none() && cursor.is_some() {
                sqlx::query("UPDATE works SET updated_at = ?1 WHERE id = ?2")
                    .bind(Utc::now())
                    .bind(expected[0])
                    .execute(db.pool())
                    .await
                    .unwrap();
                inserted_after_snapshot = Some(
                    db.upsert_work(
                        "comic",
                        "inserted during hydration",
                        Some("/comic/new"),
                        None,
                        None,
                        None,
                        json!({}),
                    )
                    .await
                    .unwrap(),
                );
            }
            if cursor.is_none() {
                break;
            }
        }

        assert_eq!(actual, expected_ids);
        assert_eq!(actual_order, expected);
        assert!(!actual.contains(&inserted_after_snapshot.unwrap()));
        assert!(matches!(
            db.library_page(Some("not-a-cursor"), 2, false).await,
            Err(AppError::BadRequest(_))
        ));
    }

    #[tokio::test]
    async fn legacy_library_helper_is_bounded_and_returns_a_continuation_cursor() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        sqlx::query(
            r#"
            WITH RECURSIVE generated(n) AS (
                SELECT 1
                UNION ALL
                SELECT n + 1 FROM generated WHERE n < 501
            )
            INSERT INTO works (kind, title, source_path)
            SELECT 'gallery', 'legacy-' || n, '/gallery/legacy-' || n
            FROM generated
            "#,
        )
        .execute(db.pool())
        .await
        .unwrap();

        let page = db.library().await.unwrap();
        assert_eq!(page.works.len(), LEGACY_LIBRARY_PAGE_LIMIT as usize);
        assert!(page.next_cursor.is_some());
    }

    #[tokio::test]
    async fn legacy_library_page_batches_tags_and_falls_back_for_pending_stats() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();

        let ready_work = db
            .upsert_work(
                "comic",
                "ready work",
                Some("/comic/ready.cbz"),
                None,
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();
        let pending_work = db
            .upsert_work(
                "comic",
                "pending work",
                Some("/comic/pending.cbz"),
                None,
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();

        let artist = db
            .upsert_tag("artist", "zebra", "zebra", None, None, "test", None, None)
            .await
            .unwrap();
        let character = db
            .upsert_tag(
                "character",
                "alpha",
                "alpha",
                None,
                None,
                "test",
                None,
                None,
            )
            .await
            .unwrap();
        db.link_tag(ready_work, artist).await.unwrap();
        db.link_tag(ready_work, character).await.unwrap();
        db.link_tag(pending_work, artist).await.unwrap();
        db.upsert_asset(
            ready_work,
            "/comic/ready.cbz",
            "application/zip",
            "archive",
            None,
            None,
            Some(10),
            json!({}),
        )
        .await
        .unwrap();
        db.upsert_asset(
            pending_work,
            "/comic/pending.cbz",
            "application/zip",
            "archive",
            None,
            None,
            Some(10),
            json!({}),
        )
        .await
        .unwrap();

        // Deliberately invalidate one maintained row and corrupt its stored
        // values.  The compatibility page must use exact fact-table counts
        // for that row while the ready row stays on the O(1) counters.
        sqlx::query(
            "UPDATE work_stats SET tag_count = 0, asset_count = 0, computed_at = NULL WHERE work_id = ?1",
        )
        .bind(pending_work)
        .execute(db.pool())
        .await
        .unwrap();

        let page = db.library_page(None, 10, false).await.unwrap();
        let ready = page
            .works
            .iter()
            .find(|work| work.id == ready_work)
            .unwrap();
        assert_eq!(
            ready.tag_keys.as_deref(),
            Some("artist:zebra,character:alpha")
        );
        assert_eq!(ready.tag_count, 2);
        assert_eq!(ready.asset_count, 1);

        let pending = page
            .works
            .iter()
            .find(|work| work.id == pending_work)
            .unwrap();
        assert_eq!(pending.tag_keys.as_deref(), Some("artist:zebra"));
        assert_eq!(pending.tag_count, 1);
        assert_eq!(pending.asset_count, 1);
    }

    #[tokio::test]
    async fn running_scan_keeps_one_queued_successor() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();

        let (first_id, first_created) = db
            .create_job_if_absent("scan-library", "queued", json!({ "source": "first" }))
            .await
            .unwrap();
        assert!(first_created);
        let running = db.claim_next_queued_job().await.unwrap().unwrap();
        assert_eq!(running.id, first_id);

        let (successor_id, successor_created) = db
            .create_job_if_absent("scan-library", "queued", json!({ "source": "watcher" }))
            .await
            .unwrap();
        assert!(successor_created);
        assert_ne!(successor_id, first_id);
        let (same_id, duplicate_created) = db
            .create_job_if_absent(
                "scan-library",
                "queued",
                json!({ "enqueue_enrichment": true }),
            )
            .await
            .unwrap();
        assert!(!duplicate_created);
        assert_eq!(same_id, successor_id);
        let (_, watcher_duplicate_created) = db
            .create_job_if_absent(
                "scan-library",
                "queued",
                json!({ "source": "watcher", "enqueue_enrichment": false }),
            )
            .await
            .unwrap();
        assert!(!watcher_duplicate_created);
        let queued_payload =
            sqlx::query_scalar::<_, String>("SELECT payload_json FROM jobs WHERE id = ?1")
                .bind(successor_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&queued_payload)
                .unwrap()
                .get("enqueue_enrichment")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert!(db.claim_next_queued_job().await.unwrap().is_none());

        db.reschedule_job(first_id, "transient scan failure", 30)
            .await
            .unwrap();
        let first_status = sqlx::query_scalar::<_, String>("SELECT status FROM jobs WHERE id = ?1")
            .bind(first_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(first_status, "superseded");
        let successor_payload =
            sqlx::query_scalar::<_, String>("SELECT payload_json FROM jobs WHERE id = ?1")
                .bind(successor_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&successor_payload)
                .unwrap()
                .get("enqueue_enrichment")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            db.claim_next_queued_job().await.unwrap().unwrap().id,
            successor_id
        );
    }

    #[tokio::test]
    async fn coalesced_scan_scope_never_drops_media_kind_coverage() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();

        let (full_id, created) = db
            .create_job_if_absent(
                "scan-library",
                "queued",
                json!({ "source": "manual", "enqueue_enrichment": false }),
            )
            .await
            .unwrap();
        assert!(created);
        let (same_id, merged) = db
            .create_job_if_absent(
                "scan-library",
                "queued",
                json!({ "source": "ownership", "kind": "gallery" }),
            )
            .await
            .unwrap();
        assert_eq!(same_id, full_id);
        assert!(!merged);
        let payload: Value = serde_json::from_str(
            &sqlx::query_scalar::<_, String>("SELECT payload_json FROM jobs WHERE id = ?1")
                .bind(full_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(payload.get("kind").is_none());
        assert_eq!(
            payload.get("source").and_then(Value::as_str),
            Some("ownership")
        );

        db.update_job(full_id, "superseded", Some("test"))
            .await
            .unwrap();
        let (gallery_id, _) = db
            .create_job_if_absent(
                "scan-library",
                "queued",
                json!({ "kind": "gallery", "enqueue_enrichment": false }),
            )
            .await
            .unwrap();
        let (audio_id, _) = db
            .create_job_if_absent(
                "scan-library",
                "queued",
                json!({ "kind": "audio", "enqueue_enrichment": true }),
            )
            .await
            .unwrap();
        assert_eq!(gallery_id, audio_id);
        let payload: Value = serde_json::from_str(
            &sqlx::query_scalar::<_, String>("SELECT payload_json FROM jobs WHERE id = ?1")
                .bind(gallery_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(payload.get("kind").is_none());
        assert_eq!(
            payload.get("enqueue_enrichment").and_then(Value::as_bool),
            Some(true)
        );
    }

    #[tokio::test]
    async fn restart_merges_scan_successor_before_requeueing_running_job() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();

        let (running_id, _) = db
            .create_job_if_absent(
                "scan-library",
                "queued",
                json!({ "source": "manual", "enqueue_enrichment": false }),
            )
            .await
            .unwrap();
        assert_eq!(
            db.claim_next_queued_job().await.unwrap().unwrap().id,
            running_id
        );
        let (successor_id, created) = db
            .create_job_if_absent(
                "scan-library",
                "queued",
                json!({ "source": "watcher", "enqueue_enrichment": true }),
            )
            .await
            .unwrap();
        assert!(created);

        assert_eq!(db.requeue_interrupted_running_jobs().await.unwrap(), 1);
        let (running_status, running_payload) = sqlx::query_as::<_, (String, String)>(
            "SELECT status, payload_json FROM jobs WHERE id = ?1",
        )
        .bind(running_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(running_status, "queued");
        let running_payload: Value = serde_json::from_str(&running_payload).unwrap();
        assert_eq!(
            running_payload
                .get("enqueue_enrichment")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            running_payload.get("source").and_then(Value::as_str),
            Some("watcher")
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM jobs WHERE id = ?1")
                .bind(successor_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "superseded"
        );
        assert_eq!(
            db.claim_next_queued_job().await.unwrap().unwrap().id,
            running_id
        );
    }

    #[tokio::test]
    async fn progress_and_history_are_written_together() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "novel",
                "book",
                Some("/novel/book.epub"),
                None,
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();

        let before_updated_at =
            sqlx::query_scalar::<_, DateTime<Utc>>("SELECT updated_at FROM works WHERE id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        let revisions_before = db.revision_snapshot().await.unwrap();

        let saved = db
            .update_work_progress(work_id, 0.42, Some("chapter-3"), 20)
            .await
            .unwrap();
        assert!(saved.accepted);
        let revisions_after_saved = db.revision_snapshot().await.unwrap();
        assert_eq!(
            revisions_after_saved.catalog_revision,
            revisions_before.catalog_revision
        );
        assert_eq!(
            revisions_after_saved.activity_revision,
            revisions_before.activity_revision + 1
        );
        let stale = db
            .update_work_progress(work_id, 0.1, Some("chapter-1"), 10)
            .await
            .unwrap();
        assert!(!stale.accepted);
        assert_eq!(stale.progress, 0.42);
        assert_eq!(stale.position.as_deref(), Some("chapter-3"));
        assert_eq!(db.revision_snapshot().await.unwrap(), revisions_after_saved);
        let work = sqlx::query_as::<_, (f64, DateTime<Utc>)>(
            "SELECT progress, updated_at FROM works WHERE id = ?1",
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        let history = sqlx::query_as::<_, (f64, Option<String>, DateTime<Utc>, i64)>(
            "SELECT progress, position, last_opened_at, update_token FROM reading_history WHERE work_id = ?1",
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(work.0, history.0);
        assert_eq!(history.1.as_deref(), Some("chapter-3"));
        assert_eq!(work.1, before_updated_at);
        assert!(history.2 >= before_updated_at);
        assert_eq!(history.3, 20);

        sqlx::query("UPDATE works SET title = 'book revised' WHERE id = ?1")
            .bind(work_id)
            .execute(db.pool())
            .await
            .unwrap();
        let revisions_after_content_update = db.revision_snapshot().await.unwrap();
        assert_eq!(
            revisions_after_content_update.catalog_revision,
            revisions_after_saved.catalog_revision + 1
        );
        assert_eq!(
            revisions_after_content_update.activity_revision,
            revisions_after_saved.activity_revision
        );
    }

    #[tokio::test]
    async fn completed_scanner_scope_tombstones_missing_work_without_cascading_history() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        assert!(db
            .try_acquire_scanner_lock("library", "scope-token", 60)
            .await
            .unwrap());
        let work_id = db
            .upsert_work(
                "novel",
                "Scope Tombstone",
                Some("/novels/scope-tombstone.epub"),
                Some("Light Novel"),
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();
        let asset_id = db
            .upsert_asset(
                work_id,
                "/novels/scope-tombstone.epub",
                "application/epub+zip",
                "book",
                Some("epub"),
                None,
                Some(123),
                json!({}),
            )
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO scanner_works(work_id, scope, seen_token, fingerprint) VALUES (?1, 'novel-scope', 'previous-token', 'fp')",
        )
        .bind(work_id)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query("INSERT INTO scanner_assets(asset_id, work_id, seen_token) VALUES (?1, ?2, 'previous-token')")
            .bind(asset_id)
            .bind(work_id)
            .execute(db.pool())
            .await
            .unwrap();
        db.update_work_progress(work_id, 0.51, Some("chapter-5"), 5)
            .await
            .unwrap();

        assert_eq!(
            db.finish_scanner_scope("novel-scope", "scope-token")
                .await
                .unwrap(),
            1
        );
        assert!(sqlx::query_scalar::<_, Option<String>>(
            "SELECT deleted_at FROM works WHERE id = ?1",
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap()
        .is_some());
        assert_eq!(
            sqlx::query_as::<_, (f64, Option<String>)>(
                "SELECT progress, position FROM reading_history WHERE work_id = ?1",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            (0.51, Some("chapter-5".to_string()))
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM assets WHERE id = ?1")
                .bind(asset_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT operation FROM search_outbox WHERE work_id = ?1",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "delete"
        );
        assert!(db.library().await.unwrap().works.is_empty());
    }

    #[tokio::test]
    async fn audit_retention_removes_expired_records() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        sqlx::query(
            "INSERT INTO audit_logs (action, status, payload_json, created_at) VALUES ('old', 'ok', '{}', ?1), ('new', 'ok', '{}', ?2)",
        )
        .bind(Utc::now() - ChronoDuration::days(AUDIT_RETENTION_DAYS + 1))
        .bind(Utc::now())
        .execute(db.pool())
        .await
        .unwrap();

        db.prune_audit_logs().await.unwrap();
        let actions = sqlx::query_scalar::<_, String>("SELECT action FROM audit_logs ORDER BY id")
            .fetch_all(db.pool())
            .await
            .unwrap();
        assert_eq!(actions, vec!["new"]);
    }

    #[tokio::test]
    async fn gallery_keyset_and_maintained_count_exclude_the_cover() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "gallery",
                "keyset gallery",
                Some("/gallery/keyset"),
                Some("Gallery"),
                None,
                None,
                json!({ "image_count": 5 }),
            )
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO assets (work_id, path, mime, role, variant, position, size, meta_json) VALUES (?1, '/gallery/keyset/cover.jpg', 'image/jpeg', 'cover', '', 0, 1, '{}')",
        )
        .bind(work_id)
        .execute(db.pool())
        .await
        .unwrap();
        for position in 0..5_i64 {
            sqlx::query(
                "INSERT INTO assets (work_id, path, mime, role, variant, position, size, meta_json) VALUES (?1, ?2, 'image/jpeg', 'image', '', ?3, 1, '{}')",
            )
            .bind(work_id)
            .bind(format!("/gallery/keyset/{position:04}.jpg"))
            .bind(position)
            .execute(db.pool())
            .await
            .unwrap();
        }

        assert_eq!(db.gallery_asset_count(work_id).await.unwrap(), 5);
        let tracked_before = db.runtime_snapshot().await.read_snapshot.samples;
        let page = db
            .gallery_assets_page(work_id, Some(0), None, 3)
            .await
            .unwrap();
        assert_eq!(page.kind, "gallery");
        assert_eq!(page.total, 5);
        assert_eq!(page.items.len(), 3);
        assert!(!page.source_version.to_rfc3339().is_empty());
        assert!(page.catalog_revision >= 0);
        assert_eq!(
            db.runtime_snapshot().await.read_snapshot.samples,
            tracked_before + 1
        );
        let page_after = db
            .gallery_assets_page(
                work_id,
                None,
                Some((page.items[2].position.unwrap(), page.items[2].id)),
                3,
            )
            .await
            .unwrap();
        assert_eq!(page_after.items.len(), 2);
        assert_eq!(page_after.catalog_revision, page.catalog_revision);
        let first = db.gallery_assets(work_id, 0, 3).await.unwrap();
        assert_eq!(first.len(), 3);
        let second = db
            .gallery_assets_after(work_id, first[2].position.unwrap(), first[2].id, 3)
            .await
            .unwrap();
        assert_eq!(second.len(), 2);
        assert_eq!(second[0].position, Some(3));
    }
}
