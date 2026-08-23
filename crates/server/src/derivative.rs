use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx_sqlite::SqliteConnection;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::db::Db;
use crate::error::{AppError, Result};
use crate::resource::{ResourceClass, ResourceGovernor};
use crate::Row;

const ENCODER_VERSION: &str = "arislist-image-v1";
const ACCESS_TOUCH_INTERVAL: Duration = Duration::from_secs(60 * 60);
const MAX_TRACKED_ACCESSES: usize = 100_000;
const EVICTION_INTERVAL: Duration = Duration::from_secs(30);
const EVICTION_BATCH_SIZE: usize = 128;
const EVICTION_STATUS_ORDER: [&str; 3] = ["stale", "orphan", "ready"];
const MAX_FAILURE_ERROR_BYTES: usize = 2_048;

#[derive(Debug, Clone)]
pub struct DerivativeKey {
    pub source_kind: String,
    pub source_id: i64,
    pub variant: String,
    pub source_version: String,
}

impl DerivativeKey {
    pub fn new(
        source_kind: impl Into<String>,
        source_id: i64,
        variant: impl Into<String>,
        source_version: impl Into<String>,
    ) -> Self {
        Self {
            source_kind: source_kind.into(),
            source_id,
            variant: variant.into(),
            source_version: source_version.into(),
        }
    }

    fn hash(&self, mime: &str) -> String {
        let source_id = self.source_id.to_string();
        let mut hasher = Sha256::new();
        for part in [
            self.source_kind.as_str(),
            source_id.as_str(),
            self.variant.as_str(),
            self.source_version.as_str(),
            mime,
            ENCODER_VERSION,
        ] {
            hasher.update((part.len() as u64).to_le_bytes());
            hasher.update(part.as_bytes());
        }
        format!("{:x}", hasher.finalize())
    }

    fn relative_path(&self, mime: &str) -> String {
        let hash = self.hash(mime);
        let extension = extension_for_mime(mime);
        format!("{}/{}/{}.{}", &hash[..2], &hash[2..4], hash, extension)
    }
}

#[derive(Debug, Clone)]
pub struct DerivativeFile {
    pub id: i64,
    pub path: PathBuf,
    pub mime: String,
    pub width: u32,
    pub height: u32,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DerivativeCacheStats {
    pub enabled: bool,
    pub root: String,
    pub high_watermark_bytes: u64,
    pub low_watermark_bytes: u64,
    pub resident_bytes: u64,
    pub resident_files: u64,
    pub statuses: BTreeMap<String, u64>,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub coalesced_requests: u64,
    pub generated_files: u64,
    pub generation_failures: u64,
    pub evicted_files: u64,
    pub eviction_failures: u64,
    pub eviction_pauses: u64,
}

#[derive(Clone)]
pub struct DerivativeCache {
    inner: Arc<DerivativeCacheInner>,
}

struct DerivativeCacheInner {
    db: Db,
    enabled: bool,
    root: PathBuf,
    high_watermark_bytes: u64,
    low_watermark_bytes: u64,
    generation_locks: Mutex<HashMap<String, Weak<AsyncMutex<()>>>>,
    recent_accesses: Mutex<HashMap<i64, Instant>>,
    eviction: AsyncMutex<()>,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    coalesced_requests: AtomicU64,
    generated_files: AtomicU64,
    generation_failures: AtomicU64,
    evicted_files: AtomicU64,
    eviction_failures: AtomicU64,
    eviction_pauses: AtomicU64,
}

struct EvictionCandidate {
    id: i64,
    relative_path: String,
    bytes: i64,
    status: String,
}

struct EvictionFailure {
    id: i64,
    error: String,
}

struct EvictionBatchResult {
    deleted: u64,
    had_failure: bool,
}

impl DerivativeCache {
    pub fn new(
        db: Db,
        enabled: bool,
        root: PathBuf,
        high_watermark_bytes: u64,
        low_watermark_bytes: u64,
    ) -> Result<Self> {
        if low_watermark_bytes >= high_watermark_bytes {
            return Err(AppError::Other(
                "derivative cache low watermark must be lower than its high watermark".to_string(),
            ));
        }
        Ok(Self {
            inner: Arc::new(DerivativeCacheInner {
                db,
                enabled,
                root,
                high_watermark_bytes,
                low_watermark_bytes,
                generation_locks: Mutex::new(HashMap::new()),
                recent_accesses: Mutex::new(HashMap::new()),
                eviction: AsyncMutex::new(()),
                cache_hits: AtomicU64::new(0),
                cache_misses: AtomicU64::new(0),
                coalesced_requests: AtomicU64::new(0),
                generated_files: AtomicU64::new(0),
                generation_failures: AtomicU64::new(0),
                evicted_files: AtomicU64::new(0),
                eviction_failures: AtomicU64::new(0),
                eviction_pauses: AtomicU64::new(0),
            }),
        })
    }

    pub fn enabled(&self) -> bool {
        self.inner.enabled
    }

    #[cfg(test)]
    pub fn disabled(db: Db, root: PathBuf) -> Self {
        Self::new(db, false, root, 1024, 512).expect("valid disabled derivative cache")
    }

    pub async fn recover_startup(&self) -> Result<()> {
        if self.enabled() {
            tokio::fs::create_dir_all(&self.inner.root).await?;
        }
        // The application claims its listener before startup recovery, so any
        // persisted generating/evicting state belongs to the previous process.
        // Atomic file publication means it is safe to queue generation again;
        // an interrupted eviction is retried from its explicit ledger path.
        let _write_slot = self.inner.db.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.inner.db.begin_tracked_transaction().await?;
        sqlx::query(
            r#"
            UPDATE derivatives
            SET status = 'queued', width = 0, height = 0, bytes = 0,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                last_error = COALESCE(last_error, 'generation interrupted by restart')
            WHERE status = 'generating'
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            UPDATE derivatives
            SET status = 'stale',
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                last_error = COALESCE(last_error, 'eviction interrupted by restart')
            WHERE status = 'evicting'
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub fn spawn_eviction_worker(
        &self,
        resources: ResourceGovernor,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if !self.enabled() {
            return None;
        }
        let cache = self.clone();
        Some(tokio::spawn(async move {
            tokio::time::sleep(EVICTION_INTERVAL).await;
            let mut interval = tokio::time::interval(EVICTION_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let over_high = cache
                    .capacity()
                    .await
                    .map(|(bytes, _)| bytes > cache.inner.high_watermark_bytes)
                    .unwrap_or(false);
                if !over_high {
                    continue;
                }
                let resources_snapshot = resources.snapshot();
                if resources_snapshot.background_paused
                    || resources_snapshot.interactive_waiters > 0
                {
                    continue;
                }
                let _writer_lease = match resources
                    .reserve_background(ResourceClass::CatalogWriter, 0, 0)
                    .await
                {
                    Ok(lease) => lease,
                    Err(err) => {
                        tracing::warn!(error = %err, "derivative eviction admission failed");
                        continue;
                    }
                };
                if let Err(err) = cache
                    .evict_to_low_watermark_with_resources(Some(&resources))
                    .await
                {
                    tracing::warn!(error = %err, "derivative cache eviction failed");
                }
            }
        }))
    }

    pub async fn get_or_generate<F, Fut>(
        &self,
        key: DerivativeKey,
        mime: &str,
        generate: F,
    ) -> Result<Option<DerivativeFile>>
    where
        F: FnOnce(PathBuf) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        if !self.enabled() {
            return Ok(None);
        }
        if let Some(ready) = self.lookup_ready(&key).await? {
            self.inner.cache_hits.fetch_add(1, AtomicOrdering::Relaxed);
            return Ok(Some(ready));
        }
        self.inner
            .cache_misses
            .fetch_add(1, AtomicOrdering::Relaxed);

        let cache_key = key.hash(mime);
        let generation_lock = self.generation_lock(&cache_key)?;
        let generation_guard = generation_lock.lock_owned().await;
        let cache = self.clone();
        let mime = mime.to_string();
        let task = tokio::spawn(async move {
            cache
                .generate_under_lock(key, mime, generation_guard, generate)
                .await
        });
        task.await
            .map_err(|err| AppError::Other(format!("derivative generation task failed: {err}")))?
            .map(Some)
    }

    /// Queue an optimization-only generation without making the interactive
    /// request wait for image decoding.  The per-key mutex is acquired with
    /// `try_lock_owned`, so repeated viewport requests do not accumulate
    /// tasks waiting behind the same expensive archive decode.
    pub async fn queue_generation<F, Fut>(
        &self,
        key: DerivativeKey,
        mime: &str,
        generate: F,
    ) -> Result<bool>
    where
        F: FnOnce(PathBuf) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        if !self.enabled() || self.lookup_ready(&key).await?.is_some() {
            return Ok(false);
        }
        // A generating or failed row is already represented by the key's
        // lock/backoff state.  Treat it as "already queued" for this
        // best-effort optimization path.
        match self.reject_active_backoff(&key).await {
            Ok(()) => {}
            Err(AppError::Overloaded { .. }) => return Ok(false),
            Err(error) => return Err(error),
        }
        let cache_key = key.hash(mime);
        let generation_lock = self.generation_lock(&cache_key)?;
        let generation_guard = match generation_lock.try_lock_owned() {
            Ok(guard) => guard,
            Err(_) => return Ok(false),
        };
        let cache = self.clone();
        let mime = mime.to_string();
        tokio::spawn(async move {
            if let Err(error) = cache
                .generate_under_lock(key, mime, generation_guard, generate)
                .await
            {
                tracing::debug!(error = %error, "background derivative generation failed");
            }
        });
        Ok(true)
    }

    async fn generate_under_lock<F, Fut>(
        &self,
        key: DerivativeKey,
        mime: String,
        _generation_guard: OwnedMutexGuard<()>,
        generate: F,
    ) -> Result<DerivativeFile>
    where
        F: FnOnce(PathBuf) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        if let Some(ready) = self.lookup_ready(&key).await? {
            self.inner
                .coalesced_requests
                .fetch_add(1, AtomicOrdering::Relaxed);
            return Ok(ready);
        }
        self.reject_active_backoff(&key).await?;
        let relative_path = key.relative_path(&mime);
        let path = self.resolve_relative_path(&relative_path)?;
        self.mark_generating(&key, &relative_path, &mime).await?;
        let parent = path
            .parent()
            .ok_or_else(|| AppError::Other("derivative path has no parent".to_string()))?;
        if let Err(err) = tokio::fs::create_dir_all(parent).await {
            self.inner
                .generation_failures
                .fetch_add(1, AtomicOrdering::Relaxed);
            self.mark_failed(&key, &err.to_string()).await?;
            return Err(err.into());
        }

        if let Err(err) = generate(path.clone()).await {
            self.inner
                .generation_failures
                .fetch_add(1, AtomicOrdering::Relaxed);
            self.mark_failed(&key, &err.to_string()).await?;
            return Err(err);
        }
        let inspected = inspect_image(path.clone()).await;
        let (width, height, bytes) = match inspected {
            Ok(inspected) => inspected,
            Err(err) => {
                self.inner
                    .generation_failures
                    .fetch_add(1, AtomicOrdering::Relaxed);
                let failure = match remove_one_file_if_present(&path).await {
                    Ok(()) => err.to_string(),
                    Err(cleanup) => format!("{err}; generated file cleanup failed: {cleanup}"),
                };
                self.mark_failed(&key, &failure).await?;
                return Err(err);
            }
        };
        let id = self.mark_ready(&key, width, height, bytes).await?;
        self.inner
            .generated_files
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.remember_access(id)?;
        Ok(DerivativeFile {
            id,
            path,
            mime,
            width,
            height,
            bytes,
        })
    }

    pub async fn lookup_ready(&self, key: &DerivativeKey) -> Result<Option<DerivativeFile>> {
        if !self.enabled() {
            return Ok(None);
        }
        let row = sqlx::query(
            r#"
            SELECT id, relative_path, mime, width, height, bytes
            FROM derivatives
            WHERE source_kind = ?1 AND source_id = ?2 AND variant = ?3
              AND source_version = ?4 AND status = 'ready'
            "#,
        )
        .bind(&key.source_kind)
        .bind(key.source_id)
        .bind(&key.variant)
        .bind(&key.source_version)
        .fetch_optional(self.inner.db.pool())
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let id: i64 = row.get("id");
        let relative_path: String = row.get("relative_path");
        let expected_bytes: i64 = row.get("bytes");
        let path = self.resolve_relative_path(&relative_path)?;
        let metadata = match tokio::fs::metadata(&path).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                self.mark_missing(id).await?;
                return Ok(None);
            }
            Err(err) => return Err(err.into()),
        };
        if !metadata.is_file() || expected_bytes <= 0 || metadata.len() != expected_bytes as u64 {
            self.mark_missing(id).await?;
            return Ok(None);
        }
        self.touch_access(id).await?;
        Ok(Some(DerivativeFile {
            id,
            path,
            mime: row.get("mime"),
            width: u32::try_from(row.get::<i64, _>("width")).unwrap_or(0),
            height: u32::try_from(row.get::<i64, _>("height")).unwrap_or(0),
            bytes: metadata.len(),
        }))
    }

    async fn reject_active_backoff(&self, key: &DerivativeKey) -> Result<()> {
        let row = sqlx::query(
            r#"
            SELECT status,
                   COALESCE(julianday(retry_at) > julianday('now'), 0) AS retry_blocked,
                   COALESCE(julianday(updated_at) > julianday('now', '-10 minutes'), 0)
                       AS generation_active
            FROM derivatives
            WHERE source_kind = ?1 AND source_id = ?2 AND variant = ?3 AND source_version = ?4
            "#,
        )
        .bind(&key.source_kind)
        .bind(key.source_id)
        .bind(&key.variant)
        .bind(&key.source_version)
        .fetch_optional(self.inner.db.pool())
        .await?;
        let Some(row) = row else {
            return Ok(());
        };
        let status: String = row.get("status");
        let retry_blocked: i64 = row.get("retry_blocked");
        let generation_active: i64 = row.get("generation_active");
        if status == "failed" && retry_blocked != 0 {
            return Err(AppError::Overloaded {
                message: "derivative generation is in retry backoff".to_string(),
                retry_after_seconds: 5,
            });
        }
        if status == "generating" && generation_active != 0 {
            return Err(AppError::Overloaded {
                message: "derivative generation is already active".to_string(),
                retry_after_seconds: 1,
            });
        }
        Ok(())
    }

    async fn mark_generating(
        &self,
        key: &DerivativeKey,
        relative_path: &str,
        mime: &str,
    ) -> Result<()> {
        let _write_slot = self.inner.db.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.inner.db.begin_tracked_transaction().await?;
        sqlx::query(
            r#"
            UPDATE derivatives
            SET status = 'stale', updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE source_kind = ?1 AND source_id = ?2 AND variant = ?3
              AND source_version != ?4
              AND status IN ('queued', 'generating', 'ready', 'failed')
            "#,
        )
        .bind(&key.source_kind)
        .bind(key.source_id)
        .bind(&key.variant)
        .bind(&key.source_version)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO derivatives (
                source_kind, source_id, variant, source_version, relative_path, mime,
                width, height, bytes, status, updated_at, last_access_at, retry_at, last_error
            )
            VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6,
                0, 0, 0, 'generating',
                strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                strftime('%Y-%m-%dT%H:%M:%fZ','now'), NULL, NULL
            )
            ON CONFLICT(source_kind, source_id, variant, source_version) DO UPDATE SET
                relative_path = excluded.relative_path,
                mime = excluded.mime,
                width = 0,
                height = 0,
                bytes = 0,
                status = 'generating',
                updated_at = excluded.updated_at,
                retry_at = NULL,
                last_error = NULL
            "#,
        )
        .bind(&key.source_kind)
        .bind(key.source_id)
        .bind(&key.variant)
        .bind(&key.source_version)
        .bind(relative_path)
        .bind(mime)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn mark_ready(
        &self,
        key: &DerivativeKey,
        width: u32,
        height: u32,
        bytes: u64,
    ) -> Result<i64> {
        let _write_slot = self.inner.db.acquire_write_slot(64 * 1024).await?;
        let bytes = i64::try_from(bytes)
            .map_err(|_| AppError::Other("derivative is too large for SQLite".to_string()))?;
        let row = sqlx::query(
            r#"
            UPDATE derivatives
            SET width = ?5, height = ?6, bytes = ?7, status = 'ready',
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                last_access_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                retry_at = NULL, error_count = 0, last_error = NULL
            WHERE source_kind = ?1 AND source_id = ?2 AND variant = ?3 AND source_version = ?4
            RETURNING id
            "#,
        )
        .bind(&key.source_kind)
        .bind(key.source_id)
        .bind(&key.variant)
        .bind(&key.source_version)
        .bind(i64::from(width))
        .bind(i64::from(height))
        .bind(bytes)
        .fetch_one(self.inner.db.pool())
        .await?;
        Ok(row.get("id"))
    }

    async fn mark_failed(&self, key: &DerivativeKey, error: &str) -> Result<()> {
        let _write_slot = self.inner.db.acquire_write_slot(64 * 1024).await?;
        let error_count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COALESCE(error_count, 0)
            FROM derivatives
            WHERE source_kind = ?1 AND source_id = ?2 AND variant = ?3 AND source_version = ?4
            "#,
        )
        .bind(&key.source_kind)
        .bind(key.source_id)
        .bind(&key.variant)
        .bind(&key.source_version)
        .fetch_optional(self.inner.db.pool())
        .await?
        .unwrap_or(0);
        let exponent = u32::try_from(error_count.clamp(0, 9)).unwrap_or(9);
        let retry_seconds = 5_u64.saturating_mul(1_u64 << exponent).min(3600);
        let retry_modifier = format!("+{retry_seconds} seconds");
        let mut error = error.to_string();
        error.truncate(MAX_FAILURE_ERROR_BYTES);
        sqlx::query(
            r#"
            UPDATE derivatives
            SET status = 'failed', width = 0, height = 0, bytes = 0,
                error_count = error_count + 1, last_error = ?5,
                retry_at = strftime('%Y-%m-%dT%H:%M:%fZ','now', ?6),
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE source_kind = ?1 AND source_id = ?2 AND variant = ?3 AND source_version = ?4
            "#,
        )
        .bind(&key.source_kind)
        .bind(key.source_id)
        .bind(&key.variant)
        .bind(&key.source_version)
        .bind(error)
        .bind(retry_modifier)
        .execute(self.inner.db.pool())
        .await?;
        Ok(())
    }

    async fn mark_missing(&self, id: i64) -> Result<()> {
        let _write_slot = self.inner.db.acquire_write_slot(64 * 1024).await?;
        sqlx::query(
            r#"
            UPDATE derivatives
            SET status = 'stale', width = 0, height = 0, bytes = 0,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                last_error = 'ledger file is missing or has a different size'
            WHERE id = ?1 AND status = 'ready'
            "#,
        )
        .bind(id)
        .execute(self.inner.db.pool())
        .await?;
        Ok(())
    }

    async fn touch_access(&self, id: i64) -> Result<()> {
        if !self.remember_access(id)? {
            return Ok(());
        }
        let _write_slot = self.inner.db.acquire_write_slot(64 * 1024).await?;
        sqlx::query(
            r#"
            UPDATE derivatives
            SET last_access_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ?1 AND status = 'ready'
              AND julianday(last_access_at) < julianday('now', '-1 hour')
            "#,
        )
        .bind(id)
        .execute(self.inner.db.pool())
        .await?;
        Ok(())
    }

    fn remember_access(&self, id: i64) -> Result<bool> {
        let mut accesses = self
            .inner
            .recent_accesses
            .lock()
            .map_err(|_| AppError::Other("derivative access registry poisoned".to_string()))?;
        if accesses
            .get(&id)
            .is_some_and(|last| last.elapsed() < ACCESS_TOUCH_INTERVAL)
        {
            return Ok(false);
        }
        if accesses.len() >= MAX_TRACKED_ACCESSES && !accesses.contains_key(&id) {
            accesses.clear();
        }
        accesses.insert(id, Instant::now());
        Ok(true)
    }

    fn generation_lock(&self, key: &str) -> Result<Arc<AsyncMutex<()>>> {
        let mut locks = self.inner.generation_locks.lock().map_err(|_| {
            AppError::Other("derivative generation lock registry poisoned".to_string())
        })?;
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(key).and_then(Weak::upgrade) {
            return Ok(lock);
        }
        let lock = Arc::new(AsyncMutex::new(()));
        locks.insert(key.to_string(), Arc::downgrade(&lock));
        Ok(lock)
    }

    pub async fn stats(&self) -> Result<DerivativeCacheStats> {
        let (resident_bytes, resident_files) = self.capacity().await?;
        let rows = sqlx::query(
            "SELECT status, row_count FROM derivative_status_counts WHERE row_count > 0 ORDER BY status",
        )
        .fetch_all(self.inner.db.pool())
        .await?;
        let statuses = rows
            .into_iter()
            .map(|row| {
                let status: String = row.get("status");
                let count: i64 = row.get("row_count");
                (status, u64::try_from(count).unwrap_or(0))
            })
            .collect();
        Ok(DerivativeCacheStats {
            enabled: self.enabled(),
            root: self.inner.root.to_string_lossy().to_string(),
            high_watermark_bytes: self.inner.high_watermark_bytes,
            low_watermark_bytes: self.inner.low_watermark_bytes,
            resident_bytes,
            resident_files,
            statuses,
            cache_hits: self.inner.cache_hits.load(AtomicOrdering::Relaxed),
            cache_misses: self.inner.cache_misses.load(AtomicOrdering::Relaxed),
            coalesced_requests: self.inner.coalesced_requests.load(AtomicOrdering::Relaxed),
            generated_files: self.inner.generated_files.load(AtomicOrdering::Relaxed),
            generation_failures: self.inner.generation_failures.load(AtomicOrdering::Relaxed),
            evicted_files: self.inner.evicted_files.load(AtomicOrdering::Relaxed),
            eviction_failures: self.inner.eviction_failures.load(AtomicOrdering::Relaxed),
            eviction_pauses: self.inner.eviction_pauses.load(AtomicOrdering::Relaxed),
        })
    }

    /// Read diagnostic counters on a caller-owned SQLite connection.  The
    /// health endpoint uses this to keep all database facts in one short
    /// tracked read snapshot and avoid a pool checkout for every counter.
    pub async fn stats_in(
        &self,
        connection: &mut SqliteConnection,
    ) -> Result<DerivativeCacheStats> {
        let row = sqlx::query(
            r#"
            SELECT state.resident_bytes, state.resident_files,
                   GROUP_CONCAT(CASE WHEN counts.row_count > 0
                                     THEN counts.status || ':' || counts.row_count END, ',')
                AS statuses
            FROM derivative_cache_state AS state
            CROSS JOIN derivative_status_counts AS counts
            WHERE state.singleton = 1
            GROUP BY state.resident_bytes, state.resident_files
            "#,
        )
        .fetch_one(&mut *connection)
        .await?;
        let statuses = row
            .try_get::<Option<String>, _>("statuses")
            .unwrap_or(None)
            .unwrap_or_default()
            .split(',')
            .filter_map(|entry| {
                let (status, count) = entry.split_once(':')?;
                Some((status.to_string(), count.parse::<u64>().ok()?))
            })
            .collect();
        Ok(DerivativeCacheStats {
            enabled: self.enabled(),
            root: self.inner.root.to_string_lossy().to_string(),
            high_watermark_bytes: self.inner.high_watermark_bytes,
            low_watermark_bytes: self.inner.low_watermark_bytes,
            resident_bytes: u64::try_from(row.get::<i64, _>("resident_bytes")).unwrap_or(0),
            resident_files: u64::try_from(row.get::<i64, _>("resident_files")).unwrap_or(0),
            statuses,
            cache_hits: self.inner.cache_hits.load(AtomicOrdering::Relaxed),
            cache_misses: self.inner.cache_misses.load(AtomicOrdering::Relaxed),
            coalesced_requests: self.inner.coalesced_requests.load(AtomicOrdering::Relaxed),
            generated_files: self.inner.generated_files.load(AtomicOrdering::Relaxed),
            generation_failures: self.inner.generation_failures.load(AtomicOrdering::Relaxed),
            evicted_files: self.inner.evicted_files.load(AtomicOrdering::Relaxed),
            eviction_failures: self.inner.eviction_failures.load(AtomicOrdering::Relaxed),
            eviction_pauses: self.inner.eviction_pauses.load(AtomicOrdering::Relaxed),
        })
    }

    async fn capacity(&self) -> Result<(u64, u64)> {
        let row = sqlx::query(
            "SELECT resident_bytes, resident_files FROM derivative_cache_state WHERE singleton = 1",
        )
        .fetch_one(self.inner.db.pool())
        .await?;
        Ok((
            u64::try_from(row.get::<i64, _>("resident_bytes")).unwrap_or(0),
            u64::try_from(row.get::<i64, _>("resident_files")).unwrap_or(0),
        ))
    }

    pub async fn evict_to_low_watermark(&self) -> Result<u64> {
        self.evict_to_low_watermark_with_resources(None).await
    }

    async fn evict_to_low_watermark_with_resources(
        &self,
        resources: Option<&ResourceGovernor>,
    ) -> Result<u64> {
        if !self.enabled() {
            return Ok(0);
        }
        let _eviction = self.inner.eviction.lock().await;
        let mut resident_bytes = self.capacity().await?.0;
        if resident_bytes <= self.inner.high_watermark_bytes {
            return Ok(0);
        }
        let mut deleted = 0_u64;
        while resident_bytes > self.inner.low_watermark_bytes {
            let bytes_to_reclaim = resident_bytes - self.inner.low_watermark_bytes;
            let candidates = self.claim_eviction_batch(bytes_to_reclaim).await?;
            if candidates.is_empty() {
                break;
            }
            let result = self.delete_eviction_batch(candidates).await?;
            deleted = deleted.saturating_add(result.deleted);
            resident_bytes = self.capacity().await?.0;
            if result.deleted == 0 || result.had_failure {
                break;
            }
            tokio::task::yield_now().await;
            if resources.is_some_and(|resources| {
                let snapshot = resources.snapshot();
                snapshot.background_paused || snapshot.interactive_waiters > 0
            }) {
                self.inner
                    .eviction_pauses
                    .fetch_add(1, AtomicOrdering::Relaxed);
                break;
            }
        }
        Ok(deleted)
    }

    async fn claim_eviction_batch(&self, bytes_to_reclaim: u64) -> Result<Vec<EvictionCandidate>> {
        let mut candidates = Vec::with_capacity(EVICTION_BATCH_SIZE);
        let mut candidate_bytes = 0_u64;
        for status in EVICTION_STATUS_ORDER {
            let remaining = EVICTION_BATCH_SIZE.saturating_sub(candidates.len());
            if remaining == 0 || candidate_bytes >= bytes_to_reclaim {
                break;
            }
            let rows = sqlx::query(
                r#"
                SELECT id, relative_path, bytes
                FROM derivatives INDEXED BY idx_derivatives_eviction_lru
                WHERE status = ?1 AND bytes > 0
                ORDER BY last_access_at, id
                LIMIT ?2
                "#,
            )
            .bind(status)
            .bind(i64::try_from(remaining).unwrap_or(i64::MAX))
            .fetch_all(self.inner.db.pool())
            .await?;
            for row in rows {
                let bytes: i64 = row.get("bytes");
                candidate_bytes =
                    candidate_bytes.saturating_add(u64::try_from(bytes).unwrap_or_default());
                candidates.push(EvictionCandidate {
                    id: row.get("id"),
                    relative_path: row.get("relative_path"),
                    bytes,
                    status: status.to_string(),
                });
                if candidate_bytes >= bytes_to_reclaim {
                    break;
                }
            }
        }

        if candidates.is_empty() {
            return Ok(candidates);
        }
        let _write_slot = self.inner.db.acquire_write_slot(128 * 1024).await?;
        let mut transaction = self.inner.db.begin_tracked_transaction().await?;
        let mut claimed = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let rows = sqlx::query(
                r#"
                UPDATE derivatives
                SET status = 'evicting',
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE id = ?1 AND status = ?2 AND bytes = ?3 AND bytes > 0
                "#,
            )
            .bind(candidate.id)
            .bind(&candidate.status)
            .bind(candidate.bytes)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            if rows == 1 {
                claimed.push(candidate);
            }
        }
        transaction.commit().await?;
        Ok(claimed)
    }

    async fn delete_eviction_batch(
        &self,
        candidates: Vec<EvictionCandidate>,
    ) -> Result<EvictionBatchResult> {
        let mut deleted_ids = Vec::with_capacity(candidates.len());
        let mut failures = Vec::new();
        for candidate in candidates {
            let result = match self.resolve_relative_path(&candidate.relative_path) {
                Ok(path) => match tokio::fs::remove_file(path).await {
                    Ok(()) => Ok(()),
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(err) => Err(err.to_string()),
                },
                Err(err) => Err(err.to_string()),
            };
            match result {
                Ok(()) => deleted_ids.push(candidate.id),
                Err(mut error) => {
                    error.truncate(MAX_FAILURE_ERROR_BYTES);
                    failures.push(EvictionFailure {
                        id: candidate.id,
                        error,
                    });
                }
            }
        }

        let _write_slot = self.inner.db.acquire_write_slot(128 * 1024).await?;
        let mut transaction = self.inner.db.begin_tracked_transaction().await?;
        let mut deleted = 0_u64;
        for id in deleted_ids {
            let rows = sqlx::query("DELETE FROM derivatives WHERE id = ?1 AND status = 'evicting'")
                .bind(id)
                .execute(&mut *transaction)
                .await?
                .rows_affected();
            deleted = deleted.saturating_add(rows);
        }
        for failure in &failures {
            sqlx::query(
                r#"
                UPDATE derivatives
                SET status = 'orphan', last_error = ?2,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                    last_access_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE id = ?1 AND status = 'evicting'
                "#,
            )
            .bind(failure.id)
            .bind(&failure.error)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        self.inner
            .evicted_files
            .fetch_add(deleted, AtomicOrdering::Relaxed);
        self.inner
            .eviction_failures
            .fetch_add(failures.len() as u64, AtomicOrdering::Relaxed);
        Ok(EvictionBatchResult {
            deleted,
            had_failure: !failures.is_empty(),
        })
    }

    fn resolve_relative_path(&self, relative_path: &str) -> Result<PathBuf> {
        let relative = Path::new(relative_path);
        if relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(AppError::Other(
                "derivative ledger contains an unsafe relative path".to_string(),
            ));
        }
        Ok(self.inner.root.join(relative))
    }
}

fn extension_for_mime(mime: &str) -> &'static str {
    match mime {
        "image/png" => "png",
        "image/webp" => "webp",
        "image/avif" => "avif",
        _ => "jpg",
    }
}

async fn inspect_image(path: PathBuf) -> Result<(u32, u32, u64)> {
    tokio::task::spawn_blocking(move || {
        let metadata = std::fs::metadata(&path)?;
        if !metadata.is_file() || metadata.len() == 0 {
            return Err(AppError::Other(
                "generated derivative is empty or not a file".to_string(),
            ));
        }
        let (width, height) = image::ImageReader::open(&path)
            .map_err(AppError::from)?
            .with_guessed_format()
            .map_err(AppError::from)?
            .into_dimensions()
            .map_err(|err| AppError::Other(err.to_string()))?;
        Ok((width, height, metadata.len()))
    })
    .await
    .map_err(|err| AppError::Other(format!("derivative inspection task failed: {err}")))?
}

async fn remove_one_file_if_present(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    const PNG_1X1: &[u8] = &[
        137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1, 8, 6,
        0, 0, 0, 31, 21, 196, 137, 0, 0, 0, 10, 73, 68, 65, 84, 120, 156, 99, 0, 1, 0, 0, 5, 0, 1,
        13, 10, 45, 180, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130,
    ];

    async fn test_cache(high: u64, low: u64) -> (tempfile::TempDir, Db, DerivativeCache) {
        let temp = tempfile::tempdir().unwrap();
        let database_url = format!(
            "sqlite://{}",
            temp.path()
                .join("library.sqlite")
                .to_string_lossy()
                .replace('\\', "/")
        );
        let db = Db::connect(&database_url).await.unwrap();
        db.migrate().await.unwrap();
        let cache =
            DerivativeCache::new(db.clone(), true, temp.path().join("derivatives"), high, low)
                .unwrap();
        cache.recover_startup().await.unwrap();
        (temp, db, cache)
    }

    #[test]
    fn cache_keys_are_stable_sharded_and_length_delimited() {
        let first = DerivativeKey::new("asset", 7, "thumb-256", "ab");
        let second = DerivativeKey::new("asset", 7, "thumb-25", "6ab");
        assert_ne!(first.hash("image/jpeg"), second.hash("image/jpeg"));
        let path = first.relative_path("image/jpeg");
        let parts = path.split('/').collect::<Vec<_>>();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].len(), 2);
        assert_eq!(parts[1].len(), 2);
        assert!(parts[2].ends_with(".jpg"));
    }

    #[tokio::test]
    async fn ledger_capacity_is_maintained_by_triggers() {
        let (_temp, db, cache) = test_cache(1_000, 500).await;
        sqlx::query(
            r#"
            INSERT INTO derivatives (
                source_kind, source_id, variant, source_version, relative_path,
                mime, width, height, bytes, status
            ) VALUES ('asset', 1, 'thumb-256', 'v1', 'aa/bb/file.png',
                      'image/png', 1, 1, 10, 'ready')
            "#,
        )
        .execute(db.pool())
        .await
        .unwrap();
        assert_eq!(cache.capacity().await.unwrap(), (10, 1));
        assert_eq!(cache.stats().await.unwrap().statuses.get("ready"), Some(&1));
        sqlx::query("UPDATE derivatives SET status = 'stale', bytes = 20 WHERE source_id = 1")
            .execute(db.pool())
            .await
            .unwrap();
        assert_eq!(cache.capacity().await.unwrap(), (20, 1));
        assert_eq!(cache.stats().await.unwrap().statuses.get("stale"), Some(&1));
        sqlx::query("DELETE FROM derivatives WHERE source_id = 1")
            .execute(db.pool())
            .await
            .unwrap();
        assert_eq!(cache.capacity().await.unwrap(), (0, 0));
        assert!(cache.stats().await.unwrap().statuses.is_empty());
    }

    #[tokio::test]
    async fn concurrent_misses_generate_once_and_return_the_same_file() {
        let (_temp, _db, cache) = test_cache(1_000_000, 500_000).await;
        let key = DerivativeKey::new("asset", 1, "thumb-256", "v1");
        let generations = Arc::new(AtomicUsize::new(0));
        let first_cache = cache.clone();
        let first_key = key.clone();
        let first_generations = generations.clone();
        let first = tokio::spawn(async move {
            first_cache
                .get_or_generate(first_key, "image/png", move |path| async move {
                    first_generations.fetch_add(1, Ordering::SeqCst);
                    crate::atomic_file::write(&path, PNG_1X1).await?;
                    Ok(())
                })
                .await
                .unwrap()
                .unwrap()
        });
        let second_cache = cache.clone();
        let second_key = key.clone();
        let second_generations = generations.clone();
        let second = tokio::spawn(async move {
            second_cache
                .get_or_generate(second_key, "image/png", move |path| async move {
                    second_generations.fetch_add(1, Ordering::SeqCst);
                    crate::atomic_file::write(&path, PNG_1X1).await?;
                    Ok(())
                })
                .await
                .unwrap()
                .unwrap()
        });
        let first = first.await.unwrap();
        let second = second.await.unwrap();
        assert_eq!(generations.load(Ordering::SeqCst), 1);
        assert_eq!(first.path, second.path);
        assert_eq!(first.bytes, PNG_1X1.len() as u64);
        assert_eq!(cache.capacity().await.unwrap(), (PNG_1X1.len() as u64, 1));
    }

    #[tokio::test]
    async fn queued_generation_returns_before_decode_and_coalesces_followers() {
        let (_temp, _db, cache) = test_cache(1_000_000, 500_000).await;
        let key = DerivativeKey::new("asset", 2, "thumb-256", "v1");
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let queued = cache
            .queue_generation(key.clone(), "image/png", move |path| async move {
                let _ = started_tx.send(());
                let _ = release_rx.await;
                crate::atomic_file::write(&path, PNG_1X1).await?;
                Ok(())
            })
            .await
            .unwrap();
        assert!(queued);
        started_rx.await.unwrap();

        let follower = cache
            .queue_generation(key.clone(), "image/png", move |_path| async move {
                panic!("a queued follower must not start a second generator")
            })
            .await
            .unwrap();
        assert!(!follower);
        assert!(cache.lookup_ready(&key).await.unwrap().is_none());

        release_tx.send(()).unwrap();
        let ready = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(ready) = cache.lookup_ready(&key).await.unwrap() {
                    break ready;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(ready.bytes, PNG_1X1.len() as u64);
    }

    #[tokio::test]
    async fn failed_generation_uses_backoff_instead_of_refresh_storms() {
        let (_temp, db, cache) = test_cache(1_000_000, 500_000).await;
        let key = DerivativeKey::new("asset", 9, "thumb-256", "v1");
        let attempts = Arc::new(AtomicUsize::new(0));
        let first_attempts = attempts.clone();
        let first = cache
            .get_or_generate(key.clone(), "image/png", move |_path| async move {
                first_attempts.fetch_add(1, Ordering::SeqCst);
                Err(AppError::Other("injected generation failure".to_string()))
            })
            .await;
        assert!(first.is_err());

        let second_attempts = attempts.clone();
        let second = cache
            .get_or_generate(key, "image/png", move |_path| async move {
                second_attempts.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await;
        assert!(matches!(second, Err(AppError::Overloaded { .. })));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        let row = sqlx::query(
            "SELECT status, error_count, retry_at FROM derivatives WHERE source_id = 9",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("status"), "failed");
        assert_eq!(row.get::<i64, _>("error_count"), 1);
        assert!(row.get::<Option<String>, _>("retry_at").is_some());
        let stats = cache.stats().await.unwrap();
        assert_eq!(stats.cache_misses, 2);
        assert_eq!(stats.generation_failures, 1);
        assert_eq!(stats.generated_files, 0);
    }

    #[tokio::test]
    async fn cache_metrics_distinguish_generation_from_a_ready_hit() {
        let (_temp, _db, cache) = test_cache(1_000_000, 500_000).await;
        let key = DerivativeKey::new("asset", 11, "thumb-256", "v1");
        cache
            .get_or_generate(key.clone(), "image/png", move |path| async move {
                crate::atomic_file::write(&path, PNG_1X1).await?;
                Ok(())
            })
            .await
            .unwrap();
        cache
            .get_or_generate(key, "image/png", move |_path| async move {
                panic!("a ready cache hit must not run the generator")
            })
            .await
            .unwrap();
        let stats = cache.stats().await.unwrap();
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.cache_misses, 1);
        assert_eq!(stats.generated_files, 1);
        assert_eq!(stats.generation_failures, 0);
    }

    #[tokio::test]
    async fn cache_directory_failure_leaves_a_retryable_failed_row() {
        let (_temp, db, cache) = test_cache(1_000_000, 500_000).await;
        let key = DerivativeKey::new("asset", 12, "thumb-256", "v1");
        let relative_path = key.relative_path("image/png");
        let first_shard = relative_path.split('/').next().unwrap();
        tokio::fs::write(cache.inner.root.join(first_shard), b"not a directory")
            .await
            .unwrap();

        let result = cache
            .get_or_generate(key, "image/png", move |_path| async move {
                panic!("generation must not start when its cache directory cannot be created")
            })
            .await;
        assert!(result.is_err());
        let row = sqlx::query(
            "SELECT status, bytes, error_count, last_error FROM derivatives WHERE source_id = 12",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("status"), "failed");
        assert_eq!(row.get::<i64, _>("bytes"), 0);
        assert_eq!(row.get::<i64, _>("error_count"), 1);
        assert!(row.get::<Option<String>, _>("last_error").is_some());
        assert_eq!(cache.stats().await.unwrap().generation_failures, 1);
    }

    #[tokio::test]
    async fn cancelled_request_does_not_abandon_started_derivative_generation() {
        let (_temp, _db, cache) = test_cache(1_000_000, 500_000).await;
        let key = DerivativeKey::new("asset", 10, "thumb-256", "v1");
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let request_cache = cache.clone();
        let request_key = key.clone();
        let request = tokio::spawn(async move {
            request_cache
                .get_or_generate(request_key, "image/png", move |path| async move {
                    let _ = started_tx.send(());
                    let _ = release_rx.await;
                    crate::atomic_file::write(&path, PNG_1X1).await?;
                    Ok(())
                })
                .await
        });
        started_rx.await.unwrap();
        request.abort();
        let _ = request.await;
        release_tx.send(()).unwrap();

        let ready = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(ready) = cache.lookup_ready(&key).await.unwrap() {
                    break ready;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(ready.bytes, PNG_1X1.len() as u64);
        assert_eq!(cache.stats().await.unwrap().statuses.get("ready"), Some(&1));
    }

    #[tokio::test]
    async fn startup_recovery_clears_interrupted_states_without_directory_walks() {
        let (_temp, db, cache) = test_cache(1_000, 500).await;
        sqlx::query(
            r#"
            INSERT INTO derivatives (
                source_kind, source_id, variant, source_version, relative_path,
                mime, bytes, status
            ) VALUES
                ('asset', 1, 'thumb-256', 'v1', 'aa/bb/one.png', 'image/png', 0, 'generating'),
                ('asset', 2, 'thumb-256', 'v1', 'aa/bb/two.png', 'image/png', 10, 'evicting')
            "#,
        )
        .execute(db.pool())
        .await
        .unwrap();
        cache.recover_startup().await.unwrap();
        let rows = sqlx::query("SELECT source_id, status FROM derivatives ORDER BY source_id")
            .fetch_all(db.pool())
            .await
            .unwrap();
        assert_eq!(rows[0].get::<String, _>("status"), "queued");
        assert_eq!(rows[1].get::<String, _>("status"), "stale");
        assert_eq!(cache.capacity().await.unwrap(), (10, 1));
    }

    #[tokio::test]
    async fn eviction_query_uses_partial_lru_index_without_temp_sorting() {
        let (_temp, db, _cache) = test_cache(1_000, 500).await;
        for status in EVICTION_STATUS_ORDER {
            let plan = sqlx::query(
                r#"
                EXPLAIN QUERY PLAN
                SELECT id, relative_path, bytes
                FROM derivatives INDEXED BY idx_derivatives_eviction_lru
                WHERE status = ?1 AND bytes > 0
                ORDER BY last_access_at, id
                LIMIT ?2
                "#,
            )
            .bind(status)
            .bind(EVICTION_BATCH_SIZE as i64)
            .fetch_all(db.pool())
            .await
            .unwrap();
            let details = plan
                .iter()
                .map(|row| row.get::<String, _>("detail"))
                .collect::<Vec<_>>();
            assert!(
                details
                    .iter()
                    .any(|detail| detail.contains("idx_derivatives_eviction_lru")),
                "query plan did not use the eviction LRU index: {details:?}"
            );
            assert!(
                details.iter().all(|detail| !detail.contains("TEMP B-TREE")),
                "query plan performed a temporary sort: {details:?}"
            );
        }
    }

    #[tokio::test]
    async fn eviction_uses_status_priority_lru_and_stops_near_low_watermark() {
        let (temp, db, cache) = test_cache(170, 100).await;
        let root = temp.path().join("derivatives");
        let ready_path = root.join("aa/bb/ready.bin");
        let orphan_path = root.join("cc/dd/orphan.bin");
        let stale_path = root.join("ee/ff/stale.bin");
        for (path, byte) in [(&ready_path, 1_u8), (&orphan_path, 2), (&stale_path, 3)] {
            tokio::fs::create_dir_all(path.parent().unwrap())
                .await
                .unwrap();
            tokio::fs::write(path, vec![byte; 60]).await.unwrap();
        }
        sqlx::query(
            r#"
            INSERT INTO derivatives (
                source_kind, source_id, variant, source_version, relative_path,
                mime, width, height, bytes, status, last_access_at
            ) VALUES
                ('asset', 1, 'thumb-256', 'v1', 'aa/bb/ready.bin',
                 'image/jpeg', 1, 1, 60, 'ready', '2010-01-01T00:00:00.000Z'),
                ('asset', 2, 'thumb-256', 'v1', 'cc/dd/orphan.bin',
                 'image/jpeg', 1, 1, 60, 'orphan', '2020-01-01T00:00:00.000Z'),
                ('asset', 3, 'thumb-256', 'v1', 'ee/ff/stale.bin',
                 'image/jpeg', 1, 1, 60, 'stale', '2021-01-01T00:00:00.000Z')
            "#,
        )
        .execute(db.pool())
        .await
        .unwrap();
        assert_eq!(cache.capacity().await.unwrap(), (180, 3));
        assert_eq!(cache.evict_to_low_watermark().await.unwrap(), 2);
        assert!(ready_path.exists());
        assert!(!orphan_path.exists());
        assert!(!stale_path.exists());
        assert_eq!(cache.capacity().await.unwrap(), (60, 1));
        let stats = cache.stats().await.unwrap();
        assert_eq!(stats.evicted_files, 2);
        assert_eq!(stats.eviction_failures, 0);
    }

    #[tokio::test]
    async fn eviction_failure_preserves_ledger_and_finishes_claimed_batch() {
        let (temp, db, cache) = test_cache(30, 10).await;
        let root = temp.path().join("derivatives");
        let invalid_file = root.join("aa/bb/is-a-directory.bin");
        let removable_file = root.join("cc/dd/removable.bin");
        tokio::fs::create_dir_all(&invalid_file).await.unwrap();
        tokio::fs::create_dir_all(removable_file.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&removable_file, vec![1_u8; 20])
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO derivatives (
                source_kind, source_id, variant, source_version, relative_path,
                mime, width, height, bytes, status, last_access_at
            ) VALUES
                ('asset', 1, 'thumb-256', 'v1', 'aa/bb/is-a-directory.bin',
                 'image/jpeg', 1, 1, 20, 'stale', '2020-01-01T00:00:00.000Z'),
                ('asset', 2, 'thumb-256', 'v1', 'cc/dd/removable.bin',
                 'image/jpeg', 1, 1, 20, 'stale', '2021-01-01T00:00:00.000Z')
            "#,
        )
        .execute(db.pool())
        .await
        .unwrap();

        assert_eq!(cache.evict_to_low_watermark().await.unwrap(), 1);
        assert!(invalid_file.is_dir());
        assert!(!removable_file.exists());
        let failed =
            sqlx::query("SELECT status, bytes, last_error FROM derivatives WHERE source_id = 1")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(failed.get::<String, _>("status"), "orphan");
        assert_eq!(failed.get::<i64, _>("bytes"), 20);
        assert!(failed.get::<Option<String>, _>("last_error").is_some());
        assert_eq!(cache.capacity().await.unwrap(), (20, 1));
        let stats = cache.stats().await.unwrap();
        assert_eq!(stats.evicted_files, 1);
        assert_eq!(stats.eviction_failures, 1);
    }

    #[tokio::test]
    #[ignore = "explicit 700k/1.4m derivative ledger acceptance benchmark"]
    async fn synthetic_derivative_ledger_scale_gate() {
        let rows = std::env::var("DERIVATIVE_LEDGER_GATE_ROWS")
            .ok()
            .map(|value| value.parse::<u64>().expect("valid ledger row count"))
            .unwrap_or(700_000);
        assert!(
            matches!(rows, 700_000 | 1_400_000),
            "DERIVATIVE_LEDGER_GATE_ROWS must be 700000 or 1400000"
        );

        const GIB: u64 = 1024 * 1024 * 1024;
        const INSERT_BATCH_ROWS: u64 = 10_000;
        let high = 32 * GIB;
        let low = 28 * GIB;
        let target_resident_bytes = high + 64 * 1024 * 1024;
        let bytes_per_file = target_resident_bytes.div_ceil(rows);
        let (temp, db, cache) = test_cache(high, low).await;

        let insert_started = Instant::now();
        let mut first_source_id = 1_u64;
        while first_source_id <= rows {
            let batch_rows = INSERT_BATCH_ROWS.min(rows - first_source_id + 1);
            let mut transaction = db.pool().begin().await.unwrap();
            sqlx::query(
                r#"
                WITH digits(n) AS (
                    VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)
                ), sequence(n) AS (
                    SELECT ones.n
                         + tens.n * 10
                         + hundreds.n * 100
                         + thousands.n * 1000
                    FROM digits AS ones
                    CROSS JOIN digits AS tens
                    CROSS JOIN digits AS hundreds
                    CROSS JOIN digits AS thousands
                )
                INSERT INTO derivatives (
                    source_kind, source_id, variant, source_version, relative_path,
                    mime, width, height, bytes, status, last_access_at
                )
                SELECT
                    'asset', ?1 + sequence.n, 'thumb-256', 'v1',
                    printf(
                        '%02x/%02x/%d.jpg',
                        ((?1 + sequence.n) >> 8) & 255,
                        (?1 + sequence.n) & 255,
                        ?1 + sequence.n
                    ),
                    'image/jpeg', 256, 256, ?3, 'ready',
                    printf('%016d', ?1 + sequence.n)
                FROM sequence
                WHERE sequence.n < ?2
                "#,
            )
            .bind(i64::try_from(first_source_id).unwrap())
            .bind(i64::try_from(batch_rows).unwrap())
            .bind(i64::try_from(bytes_per_file).unwrap())
            .execute(&mut *transaction)
            .await
            .unwrap();
            transaction.commit().await.unwrap();
            first_source_id += batch_rows;
        }
        let insert_millis = u64::try_from(insert_started.elapsed().as_millis()).unwrap_or(u64::MAX);

        let capacity_started = Instant::now();
        let capacity_before = cache.capacity().await.unwrap();
        let capacity_query_micros =
            u64::try_from(capacity_started.elapsed().as_micros()).unwrap_or(u64::MAX);
        assert_eq!(capacity_before, (bytes_per_file * rows, rows));
        assert!(capacity_before.0 > high);

        let query_plan = sqlx::query(
            r#"
            EXPLAIN QUERY PLAN
            SELECT id, relative_path, bytes
            FROM derivatives INDEXED BY idx_derivatives_eviction_lru
            WHERE status = ?1 AND bytes > 0
            ORDER BY last_access_at, id
            LIMIT ?2
            "#,
        )
        .bind("ready")
        .bind(EVICTION_BATCH_SIZE as i64)
        .fetch_all(db.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>();
        assert!(query_plan
            .iter()
            .any(|detail| detail.contains("idx_derivatives_eviction_lru")));
        assert!(query_plan
            .iter()
            .all(|detail| !detail.contains("TEMP B-TREE")));

        let eviction_started = Instant::now();
        let evicted_files = cache.evict_to_low_watermark().await.unwrap();
        let eviction_millis =
            u64::try_from(eviction_started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let capacity_after = cache.capacity().await.unwrap();
        assert!(capacity_after.0 <= low);
        assert!(capacity_after.0 > low.saturating_sub(bytes_per_file));
        assert_eq!(capacity_after.1, rows - evicted_files);
        let integrity_check = sqlx::query_scalar::<_, String>("PRAGMA integrity_check")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(integrity_check, "ok");

        let database_path = temp.path().join("library.sqlite");
        let database_bytes = tokio::fs::metadata(&database_path).await.unwrap().len();
        let wal_bytes = tokio::fs::metadata(format!("{}-wal", database_path.to_string_lossy()))
            .await
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let summary = serde_json::json!({
            "artifact_version": 1,
            "rows": rows,
            "bytes_per_file": bytes_per_file,
            "high_watermark_bytes": high,
            "low_watermark_bytes": low,
            "capacity_before": {
                "bytes": capacity_before.0,
                "files": capacity_before.1,
            },
            "capacity_after": {
                "bytes": capacity_after.0,
                "files": capacity_after.1,
            },
            "evicted_files": evicted_files,
            "timings": {
                "insert_millis": insert_millis,
                "capacity_query_micros": capacity_query_micros,
                "eviction_millis": eviction_millis,
            },
            "database_bytes": database_bytes,
            "wal_bytes": wal_bytes,
            "query_plan": query_plan,
            "integrity_check": integrity_check,
        });
        println!("DERIVATIVE_LEDGER_GATE={summary}");

        if let Ok(output) = std::env::var("DERIVATIVE_LEDGER_GATE_OUTPUT") {
            let output = PathBuf::from(output);
            let output = if output.is_absolute() {
                output
            } else {
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../..")
                    .join(output)
            };
            assert!(
                !output.exists(),
                "refusing to overwrite derivative ledger gate artifact"
            );
            if let Some(parent) = output
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(output, serde_json::to_vec_pretty(&summary).unwrap()).unwrap();
        }
    }
}
