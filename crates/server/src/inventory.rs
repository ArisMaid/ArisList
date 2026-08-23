use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::Json;
use futures::TryStreamExt;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use walkdir::WalkDir;

use crate::catalog_writer::{MutationFence, MutationSource, WorkTombstone};
use crate::db::Db;
use crate::error::{AppError, Result};
use crate::resource::{ResourceClass, ResourceGovernor, ResourceLease};
use crate::scanner::audio_grouping::{self, AudioGroupingMode};
use crate::scanner::inspectors::audio::{self as audio_inspector, AudioInspectionRequest};
use crate::scanner::inspectors::comic::{self as comic_inspector, ComicInspectionRequest};
use crate::scanner::inspectors::coser_picture::{
    self as coser_picture_inspector, CoserPictureInspectionRequest,
};
use crate::scanner::inspectors::gallery::{
    self as gallery_inspector, GalleryInspectionRequest, GalleryInventoryAsset,
};
use crate::scanner::inspectors::novel::{self, NovelInspectionRequest};
use crate::settings::AppSettings;
use crate::vfs;
use crate::{AppState, Row};

pub const INVENTORY_BATCH_SIZE: usize = 1024;
const INVENTORY_CHANNEL_BATCHES: usize = 2;
const INVENTORY_PROCESSING_BUDGET_BYTES: u64 = 16 * 1024 * 1024;
const MAX_DIAGNOSTIC_ERROR_BYTES: usize = 2048;
const CHANGED_KEY_LIMIT: i64 = 4096;
const CHANGED_KEY_BYTES_LIMIT: i64 = 16 * 1024 * 1024;
const NOVEL_COORDINATOR_BATCH_SIZE: i64 = 64;
// One bounded coordinator batch must not lose its global scanner lease while
// a slow NAS is reading an EPUB. Startup recovery clears stale leases; the
// long TTL therefore favors safety over speculative parallelism.
const NOVEL_COORDINATOR_LOCK_TTL_SECONDS: i64 = 6 * 60 * 60;
const CATALOG_DELETE_EVENT: &str = "catalog-delete";

#[derive(Debug, Clone)]
struct RootSpec {
    kind: String,
    provider: String,
    root: PathBuf,
    root_text: String,
    scan_depth: Option<usize>,
    device_class: String,
    audio_grouping: AudioGroupingMode,
}

#[derive(Debug, Clone)]
struct InventoryRoot {
    id: i64,
    spec: RootSpec,
}

#[derive(Debug, Clone, Serialize)]
struct InventoryRow {
    #[serde(rename = "p")]
    relative_path: String,
    #[serde(rename = "d")]
    parent_key: String,
    #[serde(rename = "c")]
    media_class: String,
    #[serde(rename = "s")]
    size: i64,
    #[serde(rename = "m")]
    mtime_ns: i64,
    #[serde(rename = "i")]
    file_id: Option<String>,
    #[serde(rename = "f")]
    fast_fingerprint: String,
    #[serde(rename = "w")]
    work_key: String,
}

struct InventoryWalkBatch {
    rows: Vec<InventoryRow>,
    more: bool,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct InventoryBatchStats {
    pub discovered: u64,
    pub inserted: u64,
    pub changed: u64,
    pub unchanged: u64,
    pub serialized_bytes: usize,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct InventoryRootRun {
    pub root_id: i64,
    pub kind: String,
    pub generation: i64,
    pub complete: bool,
    pub discovered: u64,
    pub inserted: u64,
    pub changed: u64,
    pub unchanged: u64,
    pub newly_missing: u64,
    pub traversal_errors: u64,
    pub max_batch_rows: usize,
    pub max_serialized_batch_bytes: usize,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct InventoryRunSummary {
    pub roots: usize,
    pub complete_roots: usize,
    pub incomplete_roots: usize,
    pub discovered: u64,
    pub inserted: u64,
    pub changed: u64,
    pub unchanged: u64,
    pub newly_missing: u64,
    pub max_batch_rows: usize,
    pub max_serialized_batch_bytes: usize,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct InventoryEventSummary {
    pub claimed: usize,
    pub completed: usize,
    pub retried: usize,
    pub failed: usize,
    pub discovered: u64,
    pub inserted: u64,
    pub changed: u64,
    pub newly_missing: u64,
    pub max_batch_rows: usize,
    pub catalog_completed: usize,
    pub catalog_failed: usize,
}

#[derive(Debug, Clone)]
struct PendingEvent {
    seq: i64,
    root_id: i64,
    relative_path: String,
    event_kind: String,
    work_key: String,
    attempts: i64,
    generation: i64,
    root: RootSpec,
}

#[derive(Debug, Clone)]
struct GalleryCatalogAsset {
    relative_path: String,
    size: i64,
    source_version: String,
    file_id: Option<String>,
}

type GalleryIdentitySignature = Vec<(String, i64, String, Option<String>)>;

#[derive(Debug)]
enum EventScope {
    WorkKey(String),
    Prefix(String),
    Exact(String),
}

#[derive(Debug)]
enum EventTargetState {
    File(std::fs::Metadata),
    Directory,
    Missing,
}

#[derive(Debug)]
struct WalkOutcome {
    complete: bool,
    discovered: u64,
    errors: u64,
    first_error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InventoryStatusResponse {
    enabled: bool,
    enabled_kinds: Vec<String>,
    mode: &'static str,
    batch_size: usize,
    channel_batches: usize,
    totals: InventoryTotals,
    roots: Vec<InventoryRootStatus>,
}

#[derive(Debug, Default, Serialize)]
struct InventoryTotals {
    roots: usize,
    scanning_roots: usize,
    needs_reconcile_roots: usize,
    present_files: i64,
    missing_files: i64,
    pending_events: i64,
    pending_catalog_keys: i64,
    failed_catalog_keys: i64,
}

#[derive(Debug, Serialize)]
struct InventoryRootStatus {
    id: i64,
    kind: String,
    provider: String,
    root_key: String,
    root_label: String,
    device_class: String,
    enabled: bool,
    generation: i64,
    completed_generation: i64,
    status: String,
    present_files: i64,
    missing_files: i64,
    last_discovered: i64,
    last_inserted: i64,
    last_changed: i64,
    last_missing: i64,
    scan_started_at: Option<String>,
    last_reconcile_at: Option<String>,
    last_error: Option<String>,
}

pub async fn status(State(state): State<Arc<AppState>>) -> Result<Json<InventoryStatusResponse>> {
    Ok(Json(
        diagnostics(
            &state.db,
            state.config.inventory_any_kind_enabled(),
            &state.config.inventory_enabled_kinds(),
        )
        .await?,
    ))
}

/// Recover persisted inventory/coordinator work after an unclean process exit.
///
/// A root left in `scanning` can no longer satisfy its old generation/token
/// fence, so it is deliberately downgraded to `needs_reconcile`.  Processing
/// events are returned to the durable pending queue; the next bounded pass
/// will re-read the current filesystem state instead of trusting an in-memory
/// task that may have been interrupted halfway through an EPUB inspection.
pub async fn recover_interrupted_coordinator(db: &Db) -> Result<u64> {
    let _write_slot = db.acquire_write_slot(128 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    // A watcher event and the catalog event it spawned may both have been
    // `processing` when the process stopped.  Requeueing both rows at once
    // would violate the partial `(root_id, work_key)` pending index.  Keep
    // only the newest pending/processing event for each affected key; the
    // newest event either contains the complete catalog action or will
    // deterministically enqueue it again after inventory is refreshed.
    sqlx::query(
        r#"
        UPDATE scan_events AS event
        SET status = 'done',
            last_error = COALESCE(last_error, 'coalesced during coordinator restart recovery')
        WHERE event.status IN ('pending', 'processing')
          AND event.work_key IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM scan_events AS processing
              WHERE processing.root_id = event.root_id
                AND processing.work_key = event.work_key
                AND processing.status = 'processing'
          )
          AND EXISTS (
              SELECT 1
              FROM scan_events AS newer
              WHERE newer.root_id = event.root_id
                AND newer.work_key = event.work_key
                AND newer.status IN ('pending', 'processing')
                AND newer.seq > event.seq
          )
        "#,
    )
    .execute(&mut *transaction)
    .await?;
    let now = chrono::Utc::now();
    let recovered_events = sqlx::query(
        r#"
        UPDATE scan_events
        SET status = 'pending',
            last_error = COALESCE(last_error, 'interrupted by server restart')
        WHERE status = 'processing'
        "#,
    )
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    sqlx::query(
        r#"
        UPDATE library_roots
        SET status = CASE WHEN enabled = 1 THEN 'needs_reconcile' ELSE 'disabled' END,
            active_token = NULL,
            scan_started_at = NULL,
            last_error = COALESCE(last_error, 'interrupted by server restart')
        WHERE status = 'scanning'
        "#,
    )
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        UPDATE novel_coordinator_state
        SET phase = 'needs_reconcile',
            updated_at = ?1,
            last_error = COALESCE(last_error, 'interrupted by server restart')
        WHERE phase = 'processing'
        "#,
    )
    .bind(now)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(recovered_events)
}

async fn diagnostics(
    db: &Db,
    enabled: bool,
    enabled_kinds: &BTreeSet<String>,
) -> Result<InventoryStatusResponse> {
    // The response combines root rows with three queue counts.  Keep all of
    // those reads in one short tracked snapshot so health cannot report a
    // mixture of generations while a scan commits, and avoid four separate
    // pool checkouts on the N100 hot diagnostic path.
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let rows = sqlx::query(
        r#"
        SELECT id, kind, provider, root, device_class, enabled, generation,
               completed_generation, status, present_files, missing_files,
               last_discovered, last_inserted, last_changed, last_missing,
               scan_started_at, last_reconcile_at, last_error
        FROM library_roots
        ORDER BY kind, provider, id
        "#,
    )
    .fetch_all(&mut *transaction)
    .await?;
    let roots = rows
        .into_iter()
        .map(|row| {
            let root: String = row.get("root");
            InventoryRootStatus {
                id: row.get("id"),
                kind: row.get("kind"),
                provider: row.get("provider"),
                root_key: redacted_root_key(&root),
                root_label: root_label(&root),
                device_class: row.get("device_class"),
                enabled: row.get::<i64, _>("enabled") != 0,
                generation: row.get("generation"),
                completed_generation: row.get("completed_generation"),
                status: row.get("status"),
                present_files: row.get("present_files"),
                missing_files: row.get("missing_files"),
                last_discovered: row.get("last_discovered"),
                last_inserted: row.get("last_inserted"),
                last_changed: row.get("last_changed"),
                last_missing: row.get("last_missing"),
                scan_started_at: row.get("scan_started_at"),
                last_reconcile_at: row.get("last_reconcile_at"),
                last_error: row.get("last_error"),
            }
        })
        .collect::<Vec<_>>();
    let pending_events =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM scan_events WHERE status = 'pending'")
            .fetch_one(&mut *transaction)
            .await?;
    let pending_catalog_keys = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM scan_events WHERE status IN ('pending', 'processing') AND event_kind IN ('catalog-upsert', 'catalog-delete')",
    )
    .fetch_one(&mut *transaction)
    .await?;
    let failed_catalog_keys = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM scan_events AS failed
        WHERE failed.status = 'failed'
          AND failed.event_kind IN ('catalog-upsert', 'catalog-delete')
          AND failed.work_key IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM scan_events AS newer
              WHERE newer.root_id = failed.root_id
                AND newer.work_key = failed.work_key
                AND newer.event_kind IN ('catalog-upsert', 'catalog-delete')
                AND newer.seq > failed.seq
          )
        "#,
    )
    .fetch_one(&mut *transaction)
    .await?;
    let mut totals = InventoryTotals {
        roots: roots.len(),
        pending_events,
        pending_catalog_keys,
        failed_catalog_keys,
        ..Default::default()
    };
    for root in &roots {
        totals.present_files = totals.present_files.saturating_add(root.present_files);
        totals.missing_files = totals.missing_files.saturating_add(root.missing_files);
        totals.scanning_roots += usize::from(root.status == "scanning");
        totals.needs_reconcile_roots += usize::from(root.status == "needs_reconcile");
    }
    let response = InventoryStatusResponse {
        enabled,
        enabled_kinds: enabled_kinds.iter().cloned().collect(),
        mode: "shadow",
        batch_size: INVENTORY_BATCH_SIZE,
        channel_batches: INVENTORY_CHANNEL_BATCHES,
        totals,
        roots,
    };
    transaction.commit().await?;
    Ok(response)
}

/// Reconcile only the media kinds selected for the current rollout.  This is
/// deliberately separate from `reconcile_kind_shadow`: the latter is an
/// explicit operator request, while this function is the bounded global pass
/// used by the N100 maintenance scan.
pub async fn reconcile_all_shadow_for_kinds(
    db: &Db,
    resources: &ResourceGovernor,
    settings: &AppSettings,
    generated_dir: &Path,
    lease_valid: Arc<AtomicBool>,
    enabled_kinds: &BTreeSet<String>,
) -> Result<InventoryRunSummary> {
    reconcile_shadow(
        db,
        resources,
        settings,
        generated_dir,
        lease_valid,
        None,
        Some(enabled_kinds),
    )
    .await
}

/// Reconcile only roots belonging to one media kind.  Other inventory roots
/// remain enabled and untouched; this is important when ownership is promoted
/// one kind at a time on a small NAS.
pub async fn reconcile_kind_shadow(
    db: &Db,
    resources: &ResourceGovernor,
    settings: &AppSettings,
    generated_dir: &Path,
    lease_valid: Arc<AtomicBool>,
    kind: &str,
) -> Result<InventoryRunSummary> {
    let normalized = kind.trim().to_ascii_lowercase();
    if !matches!(
        normalized.as_str(),
        "comic" | "novel" | "audio" | "gallery" | "coser-picture"
    ) {
        return Err(AppError::BadRequest(format!(
            "unsupported inventory kind {kind:?}"
        )));
    }
    reconcile_shadow(
        db,
        resources,
        settings,
        generated_dir,
        lease_valid,
        Some(normalized.as_str()),
        None,
    )
    .await
}

async fn reconcile_shadow(
    db: &Db,
    resources: &ResourceGovernor,
    settings: &AppSettings,
    generated_dir: &Path,
    lease_valid: Arc<AtomicBool>,
    kind_filter: Option<&str>,
    enabled_kinds: Option<&BTreeSet<String>>,
) -> Result<InventoryRunSummary> {
    let specs = configured_roots(settings)
        .into_iter()
        .filter(|spec| kind_filter.is_none_or(|kind| spec.kind == kind))
        .filter(|spec| enabled_kinds.is_none_or(|kinds| kinds.contains(spec.kind.as_str())))
        .collect();
    let roots = sync_roots(db, specs, kind_filter, enabled_kinds).await?;
    let mut summary = InventoryRunSummary {
        roots: roots.len(),
        ..Default::default()
    };
    for root in roots {
        if !lease_valid.load(Ordering::Acquire) {
            return Err(AppError::Other(
                "library scanner lease was lost before shadow inventory completed".to_string(),
            ));
        }
        let run = reconcile_root(db, resources, root, generated_dir, lease_valid.clone()).await?;
        summary.complete_roots += usize::from(run.complete);
        summary.incomplete_roots += usize::from(!run.complete);
        summary.discovered = summary.discovered.saturating_add(run.discovered);
        summary.inserted = summary.inserted.saturating_add(run.inserted);
        summary.changed = summary.changed.saturating_add(run.changed);
        summary.unchanged = summary.unchanged.saturating_add(run.unchanged);
        summary.newly_missing = summary.newly_missing.saturating_add(run.newly_missing);
        summary.max_batch_rows = summary.max_batch_rows.max(run.max_batch_rows);
        summary.max_serialized_batch_bytes = summary
            .max_serialized_batch_bytes
            .max(run.max_serialized_batch_bytes);
    }
    Ok(summary)
}

async fn reconcile_root(
    db: &Db,
    resources: &ResourceGovernor,
    root: InventoryRoot,
    generated_dir: &Path,
    lease_valid: Arc<AtomicBool>,
) -> Result<InventoryRootRun> {
    let token = uuid::Uuid::new_v4().to_string();
    let generation = begin_root_scan(db, root.id, &token).await?;
    let mut run = InventoryRootRun {
        root_id: root.id,
        kind: root.spec.kind.clone(),
        generation,
        ..Default::default()
    };
    let first_walk_lease = resources
        .reserve_background(ResourceClass::ScanIo, INVENTORY_PROCESSING_BUDGET_BYTES, 0)
        .await?;
    let (sender, mut receiver) = mpsc::channel(INVENTORY_CHANNEL_BATCHES);
    let (permit_sender, permit_receiver) = std::sync::mpsc::channel();
    permit_sender.send(first_walk_lease).map_err(|_| {
        AppError::Other("inventory walker did not accept its initial resource lease".to_string())
    })?;
    let walk_root = root.spec.clone();
    let walk_lease = lease_valid.clone();
    let worker = tokio::task::spawn_blocking(move || {
        walk_root_inventory(walk_root, walk_lease, sender, permit_receiver)
    });

    let mut write_error = None;
    while let Some(walk_batch) = receiver.recv().await {
        let batch = walk_batch.rows;
        run.max_batch_rows = run.max_batch_rows.max(batch.len());
        match upsert_inventory_batch(db, root.id, generation, &token, &batch).await {
            Ok(stats) => {
                run.discovered = run.discovered.saturating_add(stats.discovered);
                run.inserted = run.inserted.saturating_add(stats.inserted);
                run.changed = run.changed.saturating_add(stats.changed);
                run.unchanged = run.unchanged.saturating_add(stats.unchanged);
                run.max_serialized_batch_bytes =
                    run.max_serialized_batch_bytes.max(stats.serialized_bytes);
                if matches!(root.spec.kind.as_str(), "novel" | "comic" | "coser-picture") {
                    if let Err(error) = drain_root_catalog_events_under_fence(
                        db,
                        resources,
                        &root,
                        generation,
                        &token,
                        generated_dir,
                        NOVEL_COORDINATOR_BATCH_SIZE,
                    )
                    .await
                    {
                        mark_root_incomplete(
                            db,
                            root.id,
                            generation,
                            &token,
                            &run,
                            &error.to_string(),
                        )
                        .await?;
                        return Err(error);
                    }
                }
            }
            Err(err) => {
                write_error = Some(err);
                break;
            }
        }
        if walk_batch.more {
            let next_walk_lease = resources
                .reserve_background(ResourceClass::ScanIo, INVENTORY_PROCESSING_BUDGET_BYTES, 0)
                .await?;
            if permit_sender.send(next_walk_lease).is_err() {
                write_error = Some(AppError::Other(
                    "inventory walker stopped before accepting its next resource lease".to_string(),
                ));
                break;
            }
        }
    }
    drop(permit_sender);
    drop(receiver);
    let walk_result = worker
        .await
        .map_err(|err| AppError::Other(format!("inventory walker task failed: {err}")))?;

    if let Some(err) = write_error {
        mark_root_incomplete(db, root.id, generation, &token, &run, &err.to_string()).await?;
        return Err(err);
    }
    let outcome = match walk_result {
        Ok(outcome) => outcome,
        Err(err) => {
            mark_root_incomplete(db, root.id, generation, &token, &run, &err.to_string()).await?;
            return Err(err);
        }
    };
    run.traversal_errors = outcome.errors;
    if outcome.discovered != run.discovered {
        let message = format!(
            "inventory pipeline count mismatch: walker={}, writer={}",
            outcome.discovered, run.discovered
        );
        mark_root_incomplete(db, root.id, generation, &token, &run, &message).await?;
        return Err(AppError::Other(message));
    }
    if !outcome.complete {
        let message = outcome
            .first_error
            .unwrap_or_else(|| "inventory traversal was incomplete".to_string());
        mark_root_incomplete(db, root.id, generation, &token, &run, &message).await?;
        return Ok(run);
    }

    run.newly_missing = mark_root_scan_missing(db, root.id, generation, &token, &run).await?;
    if matches!(
        root.spec.kind.as_str(),
        "novel" | "comic" | "coser-picture" | "audio" | "gallery"
    ) {
        loop {
            let queued = match root.spec.kind.as_str() {
                "novel" => {
                    enqueue_missing_novel_catalog_events(
                        db,
                        root.id,
                        generation,
                        &token,
                        NOVEL_COORDINATOR_BATCH_SIZE,
                    )
                    .await?
                }
                "comic" | "coser-picture" => {
                    enqueue_missing_archive_catalog_events(
                        db,
                        root.id,
                        generation,
                        &token,
                        NOVEL_COORDINATOR_BATCH_SIZE,
                        &root.spec.kind,
                    )
                    .await?
                }
                "audio" => {
                    let upserts = enqueue_audio_reconcile_catalog_events(
                        db,
                        root.id,
                        generation,
                        &token,
                        NOVEL_COORDINATOR_BATCH_SIZE,
                    )
                    .await?;
                    let deletes = enqueue_missing_audio_catalog_events(
                        db,
                        root.id,
                        generation,
                        &token,
                        NOVEL_COORDINATOR_BATCH_SIZE,
                    )
                    .await?;
                    upserts.saturating_add(deletes)
                }
                "gallery" => {
                    let upserts = enqueue_gallery_reconcile_catalog_events(
                        db,
                        root.id,
                        generation,
                        &token,
                        NOVEL_COORDINATOR_BATCH_SIZE,
                    )
                    .await?;
                    let deletes = enqueue_missing_gallery_catalog_events(
                        db,
                        root.id,
                        generation,
                        &token,
                        NOVEL_COORDINATOR_BATCH_SIZE,
                    )
                    .await?;
                    upserts.saturating_add(deletes)
                }
                _ => 0,
            };
            let processed = match drain_root_catalog_events_under_fence(
                db,
                resources,
                &root,
                generation,
                &token,
                generated_dir,
                NOVEL_COORDINATOR_BATCH_SIZE,
            )
            .await
            {
                Ok(processed) => processed,
                Err(error) => {
                    mark_root_incomplete(db, root.id, generation, &token, &run, &error.to_string())
                        .await?;
                    return Err(error);
                }
            };
            if queued == 0 && processed == 0 {
                break;
            }
        }
    }
    if let Err(error) = complete_root_scan(db, root.id, generation, &token, &run).await {
        mark_root_incomplete(db, root.id, generation, &token, &run, &error.to_string()).await?;
        return Err(error);
    }
    run.complete = true;
    Ok(run)
}

fn walk_root_inventory(
    root: RootSpec,
    lease_valid: Arc<AtomicBool>,
    sender: mpsc::Sender<InventoryWalkBatch>,
    permit_receiver: std::sync::mpsc::Receiver<ResourceLease>,
) -> Result<WalkOutcome> {
    let start = root.root.clone();
    walk_inventory_tree(root, start, lease_valid, sender, Some(permit_receiver))
}

fn walk_inventory_tree(
    root: RootSpec,
    start: PathBuf,
    lease_valid: Arc<AtomicBool>,
    sender: mpsc::Sender<InventoryWalkBatch>,
    permit_receiver: Option<std::sync::mpsc::Receiver<ResourceLease>>,
) -> Result<WalkOutcome> {
    let mut resource_lease = permit_receiver
        .as_ref()
        .map(|receiver| {
            receiver.recv().map_err(|_| {
                AppError::Other("inventory walker resource lease channel closed".to_string())
            })
        })
        .transpose()?;
    if !start.is_dir() {
        return Ok(WalkOutcome {
            complete: false,
            discovered: 0,
            errors: 1,
            first_error: Some("inventory traversal target is not a readable directory".to_string()),
        });
    }
    if let Err(err) = std::fs::read_dir(&start) {
        return Ok(WalkOutcome {
            complete: false,
            discovered: 0,
            errors: 1,
            first_error: Some(format!("inventory root read failed: {err}")),
        });
    }
    let mut walker = WalkDir::new(&start).min_depth(1).follow_links(false);
    if let Some(depth) = root.scan_depth {
        walker = walker.max_depth(depth);
    }
    let mut batch = Vec::with_capacity(INVENTORY_BATCH_SIZE);
    let mut outcome = WalkOutcome {
        complete: true,
        discovered: 0,
        errors: 0,
        first_error: None,
    };
    let mut entries = walker.into_iter().peekable();
    while let Some(entry) = entries.next() {
        if !lease_valid.load(Ordering::Acquire) {
            return Err(AppError::Other(
                "library scanner lease was lost during shadow inventory traversal".to_string(),
            ));
        }
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) => {
                record_walk_error(&mut outcome, format!("inventory traversal failed: {err}"));
                continue;
            }
        };
        if !entry.file_type().is_file() {
            continue;
        }
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(err) => {
                record_walk_error(
                    &mut outcome,
                    format!(
                        "inventory metadata failed for {}: {err}",
                        entry.path().display()
                    ),
                );
                continue;
            }
        };
        let row = match inventory_row(&root, entry.path(), &metadata) {
            Ok(row) => row,
            Err(err) => {
                record_walk_error(&mut outcome, err.to_string());
                continue;
            }
        };
        outcome.discovered = outcome.discovered.saturating_add(1);
        batch.push(row);
        if batch.len() >= INVENTORY_BATCH_SIZE {
            let more = entries.peek().is_some();
            drop(resource_lease.take());
            sender
                .blocking_send(InventoryWalkBatch {
                    rows: std::mem::take(&mut batch),
                    more,
                })
                .map_err(|_| AppError::Other("inventory writer stopped receiving".to_string()))?;
            if !more {
                return Ok(outcome);
            }
            resource_lease = permit_receiver
                .as_ref()
                .map(|receiver| {
                    receiver.recv().map_err(|_| {
                        AppError::Other(
                            "inventory walker resource lease channel closed".to_string(),
                        )
                    })
                })
                .transpose()?;
            batch = Vec::with_capacity(INVENTORY_BATCH_SIZE);
        }
    }
    drop(resource_lease);
    if !batch.is_empty() {
        sender
            .blocking_send(InventoryWalkBatch {
                rows: batch,
                more: false,
            })
            .map_err(|_| AppError::Other("inventory writer stopped receiving".to_string()))?;
    }
    Ok(outcome)
}

fn record_walk_error(outcome: &mut WalkOutcome, message: String) {
    outcome.complete = false;
    outcome.errors = outcome.errors.saturating_add(1);
    if outcome.first_error.is_none() {
        outcome.first_error = Some(bounded_error(&message));
    }
}

fn inventory_row(
    root: &RootSpec,
    path: &Path,
    metadata: &std::fs::Metadata,
) -> Result<InventoryRow> {
    let relative = path.strip_prefix(&root.root).map_err(|_| {
        AppError::Other(format!(
            "inventory path escaped configured root: {}",
            path.display()
        ))
    })?;
    let relative_path = portable_path(relative);
    if relative_path.is_empty() {
        return Err(AppError::Other(
            "inventory relative path is empty".to_string(),
        ));
    }
    let parent_key = relative
        .parent()
        .map(portable_path)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| ".".to_string());
    let media_class = media_class(path).to_string();
    let size = i64::try_from(metadata.len()).unwrap_or(i64::MAX);
    let mtime_ns = modified_ns(metadata.modified().map_err(|err| {
        AppError::Other(format!(
            "inventory modified time failed for {}: {err}",
            path.display()
        ))
    })?);
    let file_id = file_id_for_metadata(path, metadata);
    let fast_fingerprint = fast_fingerprint(&media_class, size, mtime_ns, file_id.as_deref());
    let work_key = derive_work_key(root, &relative_path, &parent_key, false);
    Ok(InventoryRow {
        relative_path,
        parent_key,
        media_class,
        size,
        mtime_ns,
        file_id,
        fast_fingerprint,
        work_key,
    })
}

fn derive_work_key(
    root: &RootSpec,
    relative_path: &str,
    parent_key: &str,
    is_directory: bool,
) -> String {
    match root.kind.as_str() {
        "gallery" => parent_key.to_string(),
        "audio" => audio_grouping::derive_work_key(
            root.audio_grouping,
            relative_path,
            parent_key,
            is_directory,
        ),
        "comic" | "novel" | "coser-picture" => relative_path.to_string(),
        _ => parent_key.to_string(),
    }
}

fn media_class(path: &Path) -> &'static str {
    match path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "jpg" | "jpeg" | "png" | "webp" | "gif" | "bmp" | "avif" => "image",
        "mp3" | "wav" | "flac" | "ogg" | "m4a" | "aac" | "opus" => "audio",
        "cbz" => "comic-archive",
        "strm" => "remote-archive",
        "zip" | "rar" | "7z" => "archive",
        "epub" | "mobi" | "azw" | "azw3" | "fb2" | "pdf" => "novel",
        "xml" | "json" | "txt" | "nfo" | "cue" => "metadata",
        _ => "other",
    }
}

fn modified_ns(value: SystemTime) -> i64 {
    value
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

#[cfg(unix)]
fn platform_file_id(metadata: &std::fs::Metadata) -> Option<String> {
    use std::os::unix::fs::MetadataExt;
    Some(format!("{:x}:{:x}", metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
fn platform_file_id(_metadata: &std::fs::Metadata) -> Option<String> {
    None
}

/// Windows' stable std metadata API does not expose a file index. For
/// archive/EPUB work files, use a bounded content/metadata identity as a
/// conservative rename hint. It is deliberately not used for image/audio
/// files, so the 700k-image inventory path does not turn into a hashing pass.
fn weak_catalog_file_id(path: &Path, metadata: &std::fs::Metadata) -> Option<String> {
    if !path
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| {
            value.eq_ignore_ascii_case("epub")
                || value.eq_ignore_ascii_case("cbz")
                || value.eq_ignore_ascii_case("zip")
        })
    {
        return None;
    }
    const SAMPLE_BYTES: usize = 16 * 1024;
    let mut file = std::fs::File::open(path).ok()?;
    let mut sample = Vec::with_capacity(SAMPLE_BYTES * 2);
    let mut prefix = vec![0_u8; SAMPLE_BYTES];
    let prefix_len = std::io::Read::read(&mut file, &mut prefix).ok()?;
    sample.extend_from_slice(&prefix[..prefix_len]);
    if metadata.len() > SAMPLE_BYTES as u64 {
        use std::io::{Seek, SeekFrom};
        file.seek(SeekFrom::End(-(SAMPLE_BYTES as i64))).ok()?;
        let mut suffix = vec![0_u8; SAMPLE_BYTES];
        let suffix_len = std::io::Read::read(&mut file, &mut suffix).ok()?;
        sample.extend_from_slice(&suffix[..suffix_len]);
    }
    let mut hasher = Sha256::new();
    hasher.update(metadata.len().to_le_bytes());
    hasher.update(modified_ns(metadata.modified().ok()?).to_le_bytes());
    hasher.update(&sample);
    Some(format!("weak:{:x}", hasher.finalize()))
}

/// Return the stable file identity used by Inventory rows.
pub(crate) fn file_id_for_metadata(path: &Path, metadata: &std::fs::Metadata) -> Option<String> {
    platform_file_id(metadata).or_else(|| weak_catalog_file_id(path, metadata))
}

/// Compute the bounded metadata-only identity shared by legacy and
/// Inventory-backed scanners.
pub(crate) fn fast_fingerprint(
    media_class: &str,
    size: i64,
    mtime_ns: i64,
    file_id: Option<&str>,
) -> String {
    let mut hasher = Sha256::new();
    for part in [
        media_class.as_bytes(),
        &size.to_le_bytes(),
        &mtime_ns.to_le_bytes(),
        file_id.unwrap_or_default().as_bytes(),
    ] {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    let digest = format!("{:x}", hasher.finalize());
    digest[..32].to_string()
}

async fn sync_roots(
    db: &Db,
    specs: Vec<RootSpec>,
    kind_filter: Option<&str>,
    enabled_kinds: Option<&BTreeSet<String>>,
) -> Result<Vec<InventoryRoot>> {
    let _write_slot = db.acquire_write_slot(256 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    if let Some(kind) = kind_filter {
        sqlx::query(
            r#"
            UPDATE library_roots
            SET enabled = 0,
                status = 'disabled',
                active_token = NULL
            WHERE enabled != 0 AND kind = ?1
            "#,
        )
        .bind(kind)
        .execute(&mut *transaction)
        .await?;
    } else if let Some(kinds) = enabled_kinds {
        // A subset rollout must not disable roots owned by another kind.  The
        // five supported kinds are a fixed, tiny set, so separate bound
        // parameters keep this transaction simple and avoid dynamic SQL.
        for kind in kinds {
            sqlx::query(
                r#"
                UPDATE library_roots
                SET enabled = 0,
                    status = 'disabled',
                    active_token = NULL
                WHERE enabled != 0 AND kind = ?1
                "#,
            )
            .bind(kind)
            .execute(&mut *transaction)
            .await?;
        }
    } else {
        sqlx::query(
            r#"
            UPDATE library_roots
            SET enabled = 0,
                status = 'disabled',
                active_token = NULL
            WHERE enabled != 0
            "#,
        )
        .execute(&mut *transaction)
        .await?;
    }
    let mut roots = Vec::with_capacity(specs.len());
    for spec in specs {
        let id = upsert_root_record(&mut transaction, &spec).await?;
        roots.push(InventoryRoot { id, spec });
    }
    transaction.commit().await?;
    Ok(roots)
}

async fn upsert_root_record(
    transaction: &mut sqlx_core::transaction::Transaction<'_, crate::Sqlite>,
    spec: &RootSpec,
) -> Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO library_roots (
            kind, provider, root, scan_depth, device_class, audio_grouping, enabled, status
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, 'idle')
        ON CONFLICT(kind, provider, root) DO UPDATE SET
            scan_depth = excluded.scan_depth,
            device_class = excluded.device_class,
            audio_grouping = excluded.audio_grouping,
            enabled = 1,
            status = CASE
                WHEN library_roots.status = 'disabled' THEN 'idle'
                ELSE library_roots.status
            END
        RETURNING id
        "#,
    )
    .bind(&spec.kind)
    .bind(&spec.provider)
    .bind(&spec.root_text)
    .bind(spec.scan_depth.and_then(|value| i64::try_from(value).ok()))
    .bind(&spec.device_class)
    .bind(spec.audio_grouping.as_str())
    .fetch_one(&mut **transaction)
    .await?)
}

fn configured_roots(settings: &AppSettings) -> Vec<RootSpec> {
    let mut unique = BTreeSet::new();
    let mut specs = Vec::new();
    for (kind, roots, audio_grouping) in [
        ("comic", settings.comic_roots(), AudioGroupingMode::Auto),
        ("novel", settings.novel_roots(), AudioGroupingMode::Auto),
        ("audio", settings.audio_roots(), settings.audio_grouping),
        ("gallery", settings.gallery_roots(), AudioGroupingMode::Auto),
        (
            "coser-picture",
            settings.coser_picture_roots(),
            AudioGroupingMode::Auto,
        ),
    ] {
        for root in roots {
            let root_text = root.to_string_lossy().to_string();
            let key = (kind.to_string(), "local".to_string(), root_text.clone());
            if root_text.trim().is_empty() || !unique.insert(key) {
                continue;
            }
            specs.push(RootSpec {
                kind: kind.to_string(),
                provider: "local".to_string(),
                root,
                root_text,
                scan_depth: None,
                device_class: "hdd".to_string(),
                audio_grouping,
            });
        }
    }
    // qmediasync archive, CoserPicture, and audio roots participate in
    // bounded discovery.  The provider-aware inspectors only read local
    // `.strm` stubs; the remote media itself remains behind the existing
    // range/cache route and is never downloaded during discovery.
    for kind in ["comic", "coser-picture", "audio"] {
        for source in vfs::qmediasync_scan_sources(settings, kind) {
            let root = PathBuf::from(source.root.trim());
            let root_text = root.to_string_lossy().to_string();
            let provider = vfs::inventory_provider_key(&source);
            let key = (kind.to_string(), provider.clone(), root_text.clone());
            if root_text.trim().is_empty() || !unique.insert(key) {
                continue;
            }
            specs.push(RootSpec {
                kind: kind.to_string(),
                provider,
                root,
                root_text,
                scan_depth: Some(source.scan_depth.clamp(1, 64)),
                device_class: "hdd".to_string(),
                audio_grouping: source.audio_grouping,
            });
        }
    }
    specs
}

pub async fn journal_watcher_paths_for_kinds(
    db: &Db,
    settings: &AppSettings,
    event_kind: &str,
    paths: &[PathBuf],
    enabled_kinds: &BTreeSet<String>,
) -> Result<usize> {
    journal_paths_for_specs_with_kinds(
        db,
        configured_roots(settings),
        event_kind,
        paths,
        Some(enabled_kinds),
    )
    .await
}

/// Tell the watcher whether a filesystem burst still needs the legacy full
/// scanner. Kinds owned by a bounded Catalog v2 coordinator use changed work
/// keys; all other paths remain on the conservative full-scan path.
pub async fn event_requires_legacy_scan_for_kinds(
    db: &Db,
    settings: &AppSettings,
    paths: &[PathBuf],
    enabled_kinds: &BTreeSet<String>,
) -> Result<bool> {
    event_requires_legacy_scan_inner(db, settings, paths, Some(enabled_kinds)).await
}

async fn event_requires_legacy_scan_inner(
    db: &Db,
    settings: &AppSettings,
    paths: &[PathBuf],
    enabled_kinds: Option<&BTreeSet<String>>,
) -> Result<bool> {
    let specs = configured_roots(settings);
    if paths.is_empty() || specs.is_empty() {
        return Ok(true);
    }
    // Ownership is stable for the duration of a watcher burst (a cutover
    // itself marks roots for a full reconcile).  Read all v2 kinds from one
    // short snapshot instead of issuing one pool checkout per kind on a NAS
    // event storm.
    let catalog_v2_kinds = catalog_v2_kinds_snapshot(db).await?;
    for path in paths {
        let matches = specs
            .iter()
            .filter_map(|spec| {
                path.strip_prefix(&spec.root)
                    .ok()
                    .map(|relative| (spec, relative, spec.root.components().count()))
            })
            .collect::<Vec<_>>();
        let Some((spec, relative, _)) = matches.into_iter().max_by_key(|(_, _, depth)| *depth)
        else {
            return Ok(true);
        };
        let is_epub = relative
            .extension()
            .and_then(|value| value.to_str())
            .is_some_and(|value| value.eq_ignore_ascii_case("epub"));
        let is_comic_archive = relative
            .extension()
            .and_then(|value| value.to_str())
            .is_some_and(|value| {
                value.eq_ignore_ascii_case("cbz") || value.eq_ignore_ascii_case("zip")
            });
        let kind_selected = enabled_kinds.is_none_or(|kinds| kinds.contains(spec.kind.as_str()));
        let novel_incremental =
            kind_selected && spec.kind == "novel" && is_epub && catalog_v2_kinds.contains("novel");
        let comic_incremental = spec.kind == "comic"
            && kind_selected
            && (is_comic_archive
                || (vfs::is_qmediasync_provider(&spec.provider)
                    && relative
                        .extension()
                        .and_then(|value| value.to_str())
                        .is_some_and(|value| value.eq_ignore_ascii_case("strm"))))
            && catalog_v2_kinds.contains("comic");
        let coser_picture_incremental = spec.kind == "coser-picture"
            && kind_selected
            && relative
                .extension()
                .and_then(|value| value.to_str())
                .is_some_and(|value| {
                    value.eq_ignore_ascii_case("zip")
                        || (vfs::is_qmediasync_provider(&spec.provider)
                            && value.eq_ignore_ascii_case("strm"))
                })
            && catalog_v2_kinds.contains("coser-picture");
        let audio_incremental =
            kind_selected && spec.kind == "audio" && catalog_v2_kinds.contains("audio");
        let gallery_incremental =
            kind_selected && spec.kind == "gallery" && catalog_v2_kinds.contains("gallery");
        if !novel_incremental
            && !comic_incremental
            && !coser_picture_incremental
            && !audio_incremental
            && !gallery_incremental
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
async fn journal_paths_for_specs(
    db: &Db,
    specs: Vec<RootSpec>,
    event_kind: &str,
    paths: &[PathBuf],
) -> Result<usize> {
    journal_paths_for_specs_with_kinds(db, specs, event_kind, paths, None).await
}

async fn journal_paths_for_specs_with_kinds(
    db: &Db,
    specs: Vec<RootSpec>,
    event_kind: &str,
    paths: &[PathBuf],
    enabled_kinds: Option<&BTreeSet<String>>,
) -> Result<usize> {
    if paths.is_empty() || specs.is_empty() {
        return Ok(0);
    }
    let event_kind = bounded_error(event_kind);
    // See `event_requires_legacy_scan`: the ownership snapshot avoids a
    // per-kind pool round trip and is safe because rollback schedules a
    // complete root reconcile before returning ownership to legacy.
    let catalog_v2_kinds = catalog_v2_kinds_snapshot(db).await?;
    let mut mapped = Vec::new();
    for path in paths {
        let matches = specs
            .iter()
            .filter_map(|spec| {
                path.strip_prefix(&spec.root)
                    .ok()
                    .map(|relative| (spec, relative.to_path_buf(), spec.root.components().count()))
            })
            .collect::<Vec<_>>();
        let Some(max_depth) = matches.iter().map(|(_, _, depth)| *depth).max() else {
            continue;
        };
        for (spec, relative, depth) in matches {
            if depth != max_depth {
                continue;
            }
            if enabled_kinds.is_some_and(|kinds| !kinds.contains(spec.kind.as_str())) {
                continue;
            }
            let relative_path = portable_path(&relative);
            if relative_path.is_empty() {
                continue;
            }
            let parent_key = relative
                .parent()
                .map(portable_path)
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| ".".to_string());
            let is_directory =
                event_kind.ends_with("-folder") || Path::new(&relative_path).extension().is_none();
            let work_key = derive_event_work_key(spec, &relative_path, &parent_key, is_directory);
            mapped.push((spec.clone(), relative_path, work_key));
        }
    }
    if mapped.is_empty() {
        return Ok(0);
    }
    let _write_slot = db.acquire_write_slot(512 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    let mut root_ids = std::collections::BTreeMap::new();
    let mut journaled = 0;
    for (spec, relative_path, work_key) in mapped {
        let root_identity = (
            spec.kind.clone(),
            spec.provider.clone(),
            spec.root_text.clone(),
        );
        let root_id = if let Some(root_id) = root_ids.get(&root_identity) {
            *root_id
        } else {
            let root_id = upsert_root_record(&mut transaction, &spec).await?;
            root_ids.insert(root_identity, root_id);
            root_id
        };
        let kind_selected = enabled_kinds.is_none_or(|kinds| kinds.contains(spec.kind.as_str()));
        let novel_changed_key = kind_selected
            && spec.kind == "novel"
            && Path::new(&relative_path)
                .extension()
                .and_then(|value| value.to_str())
                .is_some_and(|value| value.eq_ignore_ascii_case("epub"))
            && catalog_v2_kinds.contains("novel");
        let comic_changed_key =
            kind_selected && spec.kind == "comic" && catalog_v2_kinds.contains("comic");
        let coser_picture_changed_key = kind_selected
            && spec.kind == "coser-picture"
            && catalog_v2_kinds.contains("coser-picture");
        let audio_changed_key =
            kind_selected && spec.kind == "audio" && catalog_v2_kinds.contains("audio");
        let gallery_changed_key =
            kind_selected && spec.kind == "gallery" && catalog_v2_kinds.contains("gallery");
        if novel_changed_key
            || comic_changed_key
            || coser_picture_changed_key
            || audio_changed_key
            || gallery_changed_key
        {
            let queued = sqlx::query(
                r#"
                SELECT COUNT(*) AS key_count,
                       COALESCE(SUM(length(COALESCE(work_key, ''))), 0) AS key_bytes
                FROM scan_events
                WHERE root_id = ?1 AND status IN ('pending', 'processing')
                "#,
            )
            .bind(root_id)
            .fetch_one(&mut *transaction)
            .await?;
            let key_count = queued.get::<i64, _>("key_count").max(0);
            let key_bytes = queued.get::<i64, _>("key_bytes").max(0);
            if key_count >= CHANGED_KEY_LIMIT
                || key_bytes.saturating_add(i64::try_from(work_key.len()).unwrap_or(i64::MAX))
                    > CHANGED_KEY_BYTES_LIMIT
            {
                sqlx::query(
                    "UPDATE library_roots SET status = CASE WHEN status = 'scanning' THEN status ELSE 'needs_reconcile' END, last_error = ?2 WHERE id = ?1",
                )
                .bind(root_id)
                .bind(format!("{} changed-key journal capacity exceeded", spec.kind))
                .execute(&mut *transaction)
                .await?;
                continue;
            }
        }
        let seq = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO scan_events (
                root_id, relative_path, event_kind, work_key, observed_at, status
            )
            VALUES (
                ?1, ?2, ?3, ?4, strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'pending'
            )
            ON CONFLICT DO UPDATE SET
                relative_path = excluded.relative_path,
                event_kind = excluded.event_kind,
                observed_at = excluded.observed_at,
                attempts = 0,
                last_error = NULL
            RETURNING seq
            "#,
        )
        .bind(root_id)
        .bind(relative_path)
        .bind(&event_kind)
        .bind(work_key)
        .fetch_one(&mut *transaction)
        .await?;
        sqlx::query("UPDATE library_roots SET last_event_seq = ?2 WHERE id = ?1")
            .bind(root_id)
            .bind(seq)
            .execute(&mut *transaction)
            .await?;
        journaled += 1;
    }
    transaction.commit().await?;
    Ok(journaled)
}

pub async fn mark_watcher_gap_for_kinds(
    db: &Db,
    settings: &AppSettings,
    error: &str,
    enabled_kinds: &BTreeSet<String>,
) -> Result<usize> {
    mark_watcher_gap_inner(db, settings, error, Some(enabled_kinds)).await
}

async fn mark_watcher_gap_inner(
    db: &Db,
    settings: &AppSettings,
    error: &str,
    enabled_kinds: Option<&BTreeSet<String>>,
) -> Result<usize> {
    let specs = configured_roots(settings)
        .into_iter()
        .filter(|spec| enabled_kinds.is_none_or(|kinds| kinds.contains(spec.kind.as_str())))
        .collect::<Vec<_>>();
    let _write_slot = db.acquire_write_slot(256 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    let mut affected = 0_usize;
    for spec in specs {
        let root_id = upsert_root_record(&mut transaction, &spec).await?;
        let changed = sqlx::query(
            r#"
            UPDATE library_roots
            SET status = CASE
                    WHEN status = 'scanning' THEN status
                    ELSE 'needs_reconcile'
                END,
                last_error = ?2
            WHERE id = ?1
            "#,
        )
        .bind(root_id)
        .bind(bounded_error(error))
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        affected += usize::from(changed > 0);
    }
    transaction.commit().await?;
    drop(_write_slot);
    Ok(affected)
}

fn derive_event_work_key(
    root: &RootSpec,
    relative_path: &str,
    parent_key: &str,
    is_directory: bool,
) -> String {
    let has_extension = Path::new(relative_path).extension().is_some();
    match root.kind.as_str() {
        "gallery" if !has_extension => relative_path.to_string(),
        _ => derive_work_key(root, relative_path, parent_key, is_directory),
    }
}

fn parse_audio_grouping(value: String) -> AudioGroupingMode {
    match value.trim().to_ascii_lowercase().as_str() {
        "rj" => AudioGroupingMode::Rj,
        "folder" => AudioGroupingMode::Folder,
        _ => AudioGroupingMode::Auto,
    }
}

#[cfg(test)]
async fn process_pending_events(
    db: &Db,
    resources: &ResourceGovernor,
    generated_dir: &Path,
    limit: i64,
) -> Result<InventoryEventSummary> {
    process_pending_events_inner(db, resources, generated_dir, limit, None).await
}

pub async fn process_pending_events_for_kinds(
    db: &Db,
    resources: &ResourceGovernor,
    generated_dir: &Path,
    limit: i64,
    enabled_kinds: &BTreeSet<String>,
) -> Result<InventoryEventSummary> {
    process_pending_events_inner(db, resources, generated_dir, limit, Some(enabled_kinds)).await
}

async fn process_pending_events_inner(
    db: &Db,
    resources: &ResourceGovernor,
    generated_dir: &Path,
    limit: i64,
    enabled_kinds: Option<&BTreeSet<String>>,
) -> Result<InventoryEventSummary> {
    let events = claim_pending_events(db, limit.clamp(1, 256), enabled_kinds).await?;
    let mut summary = InventoryEventSummary {
        claimed: events.len(),
        ..Default::default()
    };
    if !events.is_empty() {
        let resource_lease = resources
            .reserve_background(ResourceClass::ScanIo, INVENTORY_PROCESSING_BUDGET_BYTES, 0)
            .await?;
        for event in events {
            match process_pending_event(db, &event).await {
                Ok(run) => {
                    complete_event(db, &event).await?;
                    summary.completed += 1;
                    summary.discovered = summary.discovered.saturating_add(run.discovered);
                    summary.inserted = summary.inserted.saturating_add(run.inserted);
                    summary.changed = summary.changed.saturating_add(run.changed);
                    summary.newly_missing = summary.newly_missing.saturating_add(run.newly_missing);
                    summary.max_batch_rows = summary.max_batch_rows.max(run.max_batch_rows);
                }
                Err(err) => {
                    let retry = event.attempts < 3;
                    fail_event(db, &event, &err.to_string(), retry).await?;
                    summary.retried += usize::from(retry);
                    summary.failed += usize::from(!retry);
                }
            }
        }
        // Catalog inspectors acquire ScanIo independently. Release the
        // inventory batch permit before entering either coordinator.
        drop(resource_lease);
    }
    let kind_selected = |kind: &str| enabled_kinds.is_none_or(|kinds| kinds.contains(kind));
    // The coordinator cycle can touch all five kinds.  Capture ownership once
    // before dispatching them so this background path does not perform five
    // independent pool checkouts or observe a mixed cutover state.
    let catalog_v2_kinds = if enabled_kinds.is_some_and(BTreeSet::is_empty) {
        BTreeSet::new()
    } else {
        catalog_v2_kinds_snapshot(db).await?
    };
    let (catalog_completed, catalog_failed) = if kind_selected("novel") {
        process_pending_novel_catalog_events(
            db,
            resources,
            generated_dir,
            limit,
            catalog_v2_kinds.contains("novel"),
        )
        .await?
    } else {
        (0, 0)
    };
    let (comic_completed, comic_failed) = if kind_selected("comic") {
        process_pending_archive_catalog_events(
            db,
            resources,
            limit,
            "comic",
            catalog_v2_kinds.contains("comic"),
        )
        .await?
    } else {
        (0, 0)
    };
    let (coser_picture_completed, coser_picture_failed) = if kind_selected("coser-picture") {
        process_pending_archive_catalog_events(
            db,
            resources,
            limit,
            "coser-picture",
            catalog_v2_kinds.contains("coser-picture"),
        )
        .await?
    } else {
        (0, 0)
    };
    let (audio_completed, audio_failed) = if kind_selected("audio") {
        process_pending_audio_catalog_events(
            db,
            resources,
            limit,
            catalog_v2_kinds.contains("audio"),
        )
        .await?
    } else {
        (0, 0)
    };
    let (gallery_completed, gallery_failed) = if kind_selected("gallery") {
        process_pending_gallery_catalog_events(
            db,
            resources,
            limit,
            catalog_v2_kinds.contains("gallery"),
        )
        .await?
    } else {
        (0, 0)
    };
    summary.catalog_completed = catalog_completed
        .saturating_add(comic_completed)
        .saturating_add(coser_picture_completed)
        .saturating_add(audio_completed)
        .saturating_add(gallery_completed);
    summary.catalog_failed = catalog_failed
        .saturating_add(comic_failed)
        .saturating_add(coser_picture_failed)
        .saturating_add(audio_failed)
        .saturating_add(gallery_failed);
    prune_completed_events(db, 10_000, 512).await?;
    Ok(summary)
}

async fn claim_pending_events(
    db: &Db,
    limit: i64,
    enabled_kinds: Option<&BTreeSet<String>>,
) -> Result<Vec<PendingEvent>> {
    if enabled_kinds.is_some_and(BTreeSet::is_empty) {
        return Ok(Vec::new());
    }
    let _write_slot = db.acquire_write_slot(128 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    let kind_clause = enabled_kinds
        .map(|kinds| {
            format!(
                " AND root.kind IN ({})",
                std::iter::repeat_n("?", kinds.len())
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .unwrap_or_default();
    let query = format!(
        r#"
        SELECT event.seq, event.root_id, event.relative_path, event.event_kind,
               event.work_key, event.attempts,
               root.kind, root.provider, root.root, root.scan_depth,
               root.device_class, root.audio_grouping, root.generation
        FROM scan_events AS event
        JOIN library_roots AS root ON root.id = event.root_id
        WHERE event.status = 'pending'
          AND event.event_kind NOT IN ('catalog-upsert', 'catalog-delete')
          AND root.enabled = 1
          AND root.status != 'scanning'
          {kind_clause}
        ORDER BY event.root_id, event.seq
        LIMIT ?
        "#
    );
    let mut rows_query = sqlx::query(&query);
    if let Some(kinds) = enabled_kinds {
        for kind in kinds {
            rows_query = rows_query.bind(kind);
        }
    }
    let rows = rows_query.bind(limit).fetch_all(&mut *transaction).await?;
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let seq: i64 = row.get("seq");
        let claimed = sqlx::query(
            r#"
            UPDATE scan_events
            SET status = 'processing', attempts = attempts + 1, last_error = NULL
            WHERE seq = ?1 AND status = 'pending'
            "#,
        )
        .bind(seq)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if claimed != 1 {
            continue;
        }
        let root_text: String = row.get("root");
        events.push(PendingEvent {
            seq,
            root_id: row.get("root_id"),
            relative_path: row.get("relative_path"),
            event_kind: row.get("event_kind"),
            work_key: row.get::<Option<String>, _>("work_key").unwrap_or_default(),
            attempts: row.get::<i64, _>("attempts") + 1,
            generation: row.get("generation"),
            root: RootSpec {
                kind: row.get("kind"),
                provider: row.get("provider"),
                root: PathBuf::from(&root_text),
                root_text,
                scan_depth: row
                    .get::<Option<i64>, _>("scan_depth")
                    .and_then(|value| usize::try_from(value).ok()),
                device_class: row.get("device_class"),
                audio_grouping: parse_audio_grouping(row.get("audio_grouping")),
            },
        });
    }
    transaction.commit().await?;
    Ok(events)
}

async fn process_pending_event(db: &Db, event: &PendingEvent) -> Result<InventoryRootRun> {
    let (target, scope) = event_target(event);
    let inspected_target = target.clone();
    let target_state =
        tokio::task::spawn_blocking(move || match std::fs::metadata(&inspected_target) {
            Ok(metadata) if metadata.is_file() => Ok(EventTargetState::File(metadata)),
            Ok(metadata) if metadata.is_dir() => Ok(EventTargetState::Directory),
            Ok(_) => Ok(EventTargetState::Missing),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(EventTargetState::Missing),
            Err(err) => Err(AppError::Other(format!(
                "inventory event metadata failed for {}: {err}",
                inspected_target.display()
            ))),
        })
        .await
        .map_err(|err| AppError::Other(format!("inventory event inspector failed: {err}")))??;
    let mut run = InventoryRootRun {
        root_id: event.root_id,
        kind: event.root.kind.clone(),
        generation: event.generation,
        ..Default::default()
    };
    match target_state {
        EventTargetState::Missing => {
            run.newly_missing = mark_event_scope_missing(db, event, &scope, false).await?;
        }
        EventTargetState::File(metadata) => {
            let root = event.root.clone();
            let path = target.clone();
            let row = tokio::task::spawn_blocking(move || inventory_row(&root, &path, &metadata))
                .await
                .map_err(|err| {
                    AppError::Other(format!("inventory event file task failed: {err}"))
                })??;
            let stats = upsert_event_inventory_batch(db, event, &[row]).await?;
            run.discovered = stats.discovered;
            run.inserted = stats.inserted;
            run.changed = stats.changed;
            run.unchanged = stats.unchanged;
            run.max_batch_rows = 1;
            run.max_serialized_batch_bytes = stats.serialized_bytes;
        }
        EventTargetState::Directory => {
            let (sender, mut receiver) = mpsc::channel(INVENTORY_CHANNEL_BATCHES);
            let root = event.root.clone();
            let walk_target = target.clone();
            let worker = tokio::task::spawn_blocking(move || {
                walk_inventory_tree(
                    root,
                    walk_target,
                    Arc::new(AtomicBool::new(true)),
                    sender,
                    None,
                )
            });
            let mut write_error = None;
            while let Some(walk_batch) = receiver.recv().await {
                let batch = walk_batch.rows;
                run.max_batch_rows = run.max_batch_rows.max(batch.len());
                match upsert_event_inventory_batch(db, event, &batch).await {
                    Ok(stats) => {
                        run.discovered = run.discovered.saturating_add(stats.discovered);
                        run.inserted = run.inserted.saturating_add(stats.inserted);
                        run.changed = run.changed.saturating_add(stats.changed);
                        run.unchanged = run.unchanged.saturating_add(stats.unchanged);
                        run.max_serialized_batch_bytes =
                            run.max_serialized_batch_bytes.max(stats.serialized_bytes);
                    }
                    Err(err) => {
                        write_error = Some(err);
                        break;
                    }
                }
            }
            drop(receiver);
            let walk_result = worker.await.map_err(|err| {
                AppError::Other(format!("inventory event walker task failed: {err}"))
            })?;
            if let Some(err) = write_error {
                return Err(err);
            }
            let outcome = walk_result?;
            if !outcome.complete || outcome.discovered != run.discovered {
                return Err(AppError::Other(outcome.first_error.unwrap_or_else(|| {
                    "targeted inventory traversal was incomplete".to_string()
                })));
            }
            run.newly_missing = mark_event_scope_missing(db, event, &scope, true).await?;
        }
    }
    run.complete = true;
    Ok(run)
}

fn event_target(event: &PendingEvent) -> (PathBuf, EventScope) {
    match event.root.kind.as_str() {
        "gallery" => {
            let relative = event.work_key.trim_matches('/');
            let path = if relative.is_empty() || relative == "." {
                event.root.root.clone()
            } else {
                event.root.root.join(relative)
            };
            let scope = if event.relative_path == event.work_key {
                EventScope::Prefix(event.work_key.clone())
            } else {
                EventScope::WorkKey(event.work_key.clone())
            };
            (path, scope)
        }
        "audio" => {
            let relative = Path::new(&event.relative_path);
            let mut prefix = PathBuf::new();
            let mut found = false;
            for component in relative.components() {
                prefix.push(component.as_os_str());
                if component
                    .as_os_str()
                    .to_string_lossy()
                    .eq_ignore_ascii_case(&event.work_key)
                {
                    found = true;
                    break;
                }
            }
            if !found {
                prefix = PathBuf::from(&event.work_key);
            }
            (
                event.root.root.join(prefix),
                EventScope::WorkKey(event.work_key.clone()),
            )
        }
        _ if event.event_kind.ends_with("-folder") => (
            event.root.root.join(&event.relative_path),
            EventScope::Prefix(event.relative_path.clone()),
        ),
        _ => (
            event.root.root.join(&event.relative_path),
            EventScope::Exact(event.relative_path.clone()),
        ),
    }
}

async fn upsert_event_inventory_batch(
    db: &Db,
    event: &PendingEvent,
    batch: &[InventoryRow],
) -> Result<InventoryBatchStats> {
    if batch.is_empty() {
        return Ok(InventoryBatchStats::default());
    }
    if batch.len() > INVENTORY_BATCH_SIZE {
        return Err(AppError::Other(format!(
            "inventory event batch exceeds fixed limit {INVENTORY_BATCH_SIZE}"
        )));
    }
    let payload = serde_json::to_string(batch).map_err(|err| {
        AppError::Other(format!("inventory event batch serialization failed: {err}"))
    })?;
    let _write_slot = db.acquire_write_slot(256 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    ensure_event_fence(&mut transaction, event).await?;
    let counts = sqlx::query(
        r#"
        WITH batch AS (
            SELECT
                json_extract(value, '$.p') AS relative_path,
                json_extract(value, '$.c') AS media_class,
                json_extract(value, '$.f') AS fast_fingerprint,
                json_extract(value, '$.w') AS work_key
            FROM json_each(?2)
        )
        SELECT
            COALESCE(SUM(CASE WHEN inventory.relative_path IS NULL THEN 1 ELSE 0 END), 0) AS inserted,
            COALESCE(SUM(CASE
                WHEN inventory.relative_path IS NOT NULL AND (
                    inventory.fast_fingerprint IS NOT batch.fast_fingerprint
                    OR inventory.media_class IS NOT batch.media_class
                    OR inventory.work_key IS NOT batch.work_key
                    OR inventory.status != 'present'
                ) THEN 1 ELSE 0 END), 0) AS changed,
            COALESCE(SUM(CASE WHEN inventory.status = 'missing' THEN 1 ELSE 0 END), 0) AS revived
        FROM batch
        LEFT JOIN file_inventory AS inventory
          ON inventory.root_id = ?1 AND inventory.relative_path = batch.relative_path
        "#,
    )
    .bind(event.root_id)
    .bind(&payload)
    .fetch_one(&mut *transaction)
    .await?;
    let inserted = counts.get::<i64, _>("inserted").max(0) as u64;
    let changed = counts.get::<i64, _>("changed").max(0) as u64;
    let revived = counts.get::<i64, _>("revived").max(0) as u64;
    let novel_candidates = sqlx::query(
        r#"
        WITH candidates AS (
            SELECT json_extract(input.value, '$.w') AS work_key
            FROM json_each(?2) AS input
            LEFT JOIN file_inventory AS inventory
              ON inventory.root_id = ?1
             AND inventory.relative_path = json_extract(input.value, '$.p')
            WHERE lower(json_extract(input.value, '$.p')) GLOB '*.epub'
              AND EXISTS (
                  SELECT 1 FROM library_roots AS root
                  WHERE root.id = ?1 AND root.kind = 'novel'
              )
              AND (
                    inventory.relative_path IS NULL
                 OR inventory.fast_fingerprint IS NOT json_extract(input.value, '$.f')
                 OR inventory.media_class IS NOT json_extract(input.value, '$.c')
                 OR inventory.work_key IS NOT json_extract(input.value, '$.w')
                 OR inventory.status != 'present'
                 OR EXISTS (
                     SELECT 1 FROM catalog_work_sources AS source
                     JOIN works ON works.id = source.work_id
                     WHERE source.kind = 'novel'
                       AND source.root_id = ?1
                       AND source.work_key = json_extract(input.value, '$.w')
                       AND works.deleted_at IS NOT NULL
                 )
                 OR NOT EXISTS (
                     SELECT 1 FROM catalog_work_sources AS source
                     WHERE source.kind = 'novel'
                       AND source.root_id = ?1
                       AND source.work_key = json_extract(input.value, '$.w')
                 )
              )
              AND EXISTS (
                  SELECT 1 FROM catalog_kind_ownership
                  WHERE kind = 'novel' AND authoritative_writer = 'catalog-v2'
              )
        )
        SELECT COUNT(*) AS candidate_count,
               COALESCE(SUM(length(work_key)), 0) AS candidate_bytes
        FROM candidates
        "#,
    )
    .bind(event.root_id)
    .bind(&payload)
    .fetch_one(&mut *transaction)
    .await?;
    let candidate_count = novel_candidates.get::<i64, _>("candidate_count").max(0);
    let candidate_bytes = novel_candidates.get::<i64, _>("candidate_bytes").max(0);
    if candidate_count > 0 {
        if let Some(message) = ensure_catalog_queue_capacity(
            &mut transaction,
            event.root_id,
            candidate_count,
            candidate_bytes,
        )
        .await?
        {
            transaction.commit().await?;
            return Err(AppError::Other(message));
        }
        sqlx::query(
            r#"
            INSERT INTO scan_events (
                root_id, relative_path, event_kind, work_key, observed_at, status
            )
            SELECT
                ?1, json_extract(input.value, '$.p'), 'catalog-upsert',
                json_extract(input.value, '$.w'), strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'pending'
            FROM json_each(?2) AS input
            LEFT JOIN file_inventory AS inventory
              ON inventory.root_id = ?1
             AND inventory.relative_path = json_extract(input.value, '$.p')
            WHERE lower(json_extract(input.value, '$.p')) GLOB '*.epub'
              AND EXISTS (
                  SELECT 1 FROM library_roots AS root
                  WHERE root.id = ?1 AND root.kind = 'novel'
              )
              AND (
                    inventory.relative_path IS NULL
                 OR inventory.fast_fingerprint IS NOT json_extract(input.value, '$.f')
                 OR inventory.media_class IS NOT json_extract(input.value, '$.c')
                 OR inventory.work_key IS NOT json_extract(input.value, '$.w')
                 OR inventory.status != 'present'
                 OR EXISTS (
                     SELECT 1 FROM catalog_work_sources AS source
                     JOIN works ON works.id = source.work_id
                     WHERE source.kind = 'novel'
                       AND source.root_id = ?1
                       AND source.work_key = json_extract(input.value, '$.w')
                       AND works.deleted_at IS NOT NULL
                 )
                 OR NOT EXISTS (
                     SELECT 1 FROM catalog_work_sources AS source
                     WHERE source.kind = 'novel'
                       AND source.root_id = ?1
                       AND source.work_key = json_extract(input.value, '$.w')
                 )
              )
            ON CONFLICT DO UPDATE SET
                relative_path = excluded.relative_path,
                event_kind = excluded.event_kind,
                observed_at = excluded.observed_at,
                attempts = 0,
                last_error = NULL
            "#,
        )
        .bind(event.root_id)
        .bind(&payload)
        .execute(&mut *transaction)
        .await?;
    }
    if matches!(event.root.kind.as_str(), "comic" | "coser-picture") {
        let (_, archive_overflow) = enqueue_archive_event_catalog_events(
            &mut transaction,
            event,
            &payload,
            &event.root.kind,
        )
        .await?;
        if let Some(message) = archive_overflow {
            transaction.commit().await?;
            return Err(AppError::Other(message));
        }
    }
    if event.root.kind == "audio" {
        let (_, audio_overflow) =
            enqueue_audio_event_catalog_events(&mut transaction, event, &payload).await?;
        if let Some(message) = audio_overflow {
            transaction.commit().await?;
            return Err(AppError::Other(message));
        }
    }
    if event.root.kind == "gallery" {
        let (_, gallery_overflow) =
            enqueue_gallery_event_catalog_events(&mut transaction, event, &payload).await?;
        if let Some(message) = gallery_overflow {
            transaction.commit().await?;
            return Err(AppError::Other(message));
        }
    }
    let affected = sqlx::query(
        r#"
        INSERT INTO file_inventory (
            root_id, relative_path, parent_key, media_class, size, mtime_ns,
            file_id, fast_fingerprint, work_key, seen_generation,
            seen_event_seq, status, last_error
        )
        SELECT
            ?1,
            json_extract(value, '$.p'), json_extract(value, '$.d'),
            json_extract(value, '$.c'), json_extract(value, '$.s'),
            json_extract(value, '$.m'), json_extract(value, '$.i'),
            json_extract(value, '$.f'), json_extract(value, '$.w'),
            ?2, ?3, 'present', NULL
        FROM json_each(?4)
        WHERE EXISTS (
            SELECT 1 FROM library_roots
            WHERE id = ?1 AND generation = ?2 AND enabled = 1 AND status != 'scanning'
        )
        ON CONFLICT(root_id, relative_path) DO UPDATE SET
            parent_key = excluded.parent_key,
            media_class = excluded.media_class,
            size = excluded.size,
            mtime_ns = excluded.mtime_ns,
            file_id = excluded.file_id,
            fast_fingerprint = excluded.fast_fingerprint,
            work_key = excluded.work_key,
            seen_event_seq = excluded.seen_event_seq,
            status = 'present',
            last_error = NULL
        "#,
    )
    .bind(event.root_id)
    .bind(event.generation)
    .bind(event.seq)
    .bind(&payload)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    if affected != batch.len() as u64 {
        return Err(AppError::Other(format!(
            "inventory event fence changed for root {} generation {}",
            event.root_id, event.generation
        )));
    }
    let present_delta = inserted.saturating_add(revived);
    sqlx::query(
        r#"
        UPDATE library_roots
        SET present_files = present_files + ?3,
            missing_files = MAX(0, missing_files - ?4),
            last_event_seq = ?5
        WHERE id = ?1 AND generation = ?2 AND enabled = 1 AND status != 'scanning'
        "#,
    )
    .bind(event.root_id)
    .bind(event.generation)
    .bind(i64::try_from(present_delta).unwrap_or(i64::MAX))
    .bind(i64::try_from(revived).unwrap_or(i64::MAX))
    .bind(event.seq)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    let discovered = batch.len() as u64;
    Ok(InventoryBatchStats {
        discovered,
        inserted,
        changed,
        unchanged: discovered.saturating_sub(inserted).saturating_sub(changed),
        serialized_bytes: payload.len(),
    })
}

async fn mark_event_scope_missing(
    db: &Db,
    event: &PendingEvent,
    scope: &EventScope,
    preserve_seen_in_event: bool,
) -> Result<u64> {
    let _write_slot = db.acquire_write_slot(128 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    ensure_event_fence(&mut transaction, event).await?;
    let (clause, value) = match scope {
        EventScope::WorkKey(work_key) => ("work_key = ?3", work_key.as_str()),
        EventScope::Prefix(prefix) => (
            "(relative_path = ?3 OR substr(relative_path, 1, length(?3) + 1) = ?3 || '/')",
            prefix.as_str(),
        ),
        EventScope::Exact(relative_path) => ("relative_path = ?3", relative_path.as_str()),
    };
    let seen_clause = if preserve_seen_in_event {
        " AND (seen_event_seq IS NULL OR seen_event_seq != ?4)"
    } else {
        ""
    };
    let candidate_sql = format!(
        "SELECT COUNT(*) AS candidate_count, COALESCE(SUM(length(work_key)), 0) AS candidate_bytes \
         FROM file_inventory \
         WHERE root_id = ?1 AND status = 'present' AND lower(relative_path) GLOB '*.epub' \
           AND {clause}{seen_clause} \
           AND EXISTS (SELECT 1 FROM catalog_kind_ownership \
               WHERE kind = 'novel' AND authoritative_writer = 'catalog-v2')"
    );
    let mut candidate_statement = sqlx::query(&candidate_sql)
        .bind(event.root_id)
        .bind(event.generation)
        .bind(value);
    if preserve_seen_in_event {
        candidate_statement = candidate_statement.bind(event.seq);
    }
    let candidates = candidate_statement.fetch_one(&mut *transaction).await?;
    let candidate_count = candidates.get::<i64, _>("candidate_count").max(0);
    let candidate_bytes = candidates.get::<i64, _>("candidate_bytes").max(0);
    if candidate_count > 0 {
        if let Some(message) = ensure_catalog_queue_capacity(
            &mut transaction,
            event.root_id,
            candidate_count,
            candidate_bytes,
        )
        .await?
        {
            transaction.commit().await?;
            return Err(AppError::Other(message));
        }
        let enqueue_sql = format!(
            "INSERT INTO scan_events (root_id, relative_path, event_kind, work_key, observed_at, status) \
             SELECT ?1, relative_path, 'catalog-delete', work_key, strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'pending' \
             FROM file_inventory \
             WHERE root_id = ?1 AND status = 'present' AND lower(relative_path) GLOB '*.epub' \
               AND {clause}{seen_clause} \
             ON CONFLICT DO UPDATE SET \
                 relative_path = excluded.relative_path, event_kind = excluded.event_kind, \
                 observed_at = excluded.observed_at, attempts = 0, last_error = NULL"
        );
        let mut enqueue_statement = sqlx::query(&enqueue_sql)
            .bind(event.root_id)
            .bind(event.generation)
            .bind(value);
        if preserve_seen_in_event {
            enqueue_statement = enqueue_statement.bind(event.seq);
        }
        enqueue_statement.execute(&mut *transaction).await?;
    }
    if matches!(event.root.kind.as_str(), "comic" | "coser-picture") {
        let (archive_filter, archive_kind) = match event.root.kind.as_str() {
            "comic" if vfs::is_qmediasync_provider(&event.root.provider) => (
                "(lower(relative_path) GLOB '*.cbz' OR lower(relative_path) GLOB '*.zip' OR lower(relative_path) GLOB '*.strm')",
                "comic",
            ),
            "comic" => (
                "(lower(relative_path) GLOB '*.cbz' OR lower(relative_path) GLOB '*.zip')",
                "comic",
            ),
            "coser-picture" if vfs::is_qmediasync_provider(&event.root.provider) => (
                "(lower(relative_path) GLOB '*.zip' OR lower(relative_path) GLOB '*.strm')",
                "coser-picture",
            ),
            "coser-picture" => ("lower(relative_path) GLOB '*.zip'", "coser-picture"),
            _ => unreachable!(),
        };
        let archive_candidate_sql = format!(
            "SELECT COUNT(*) AS candidate_count, COALESCE(SUM(length(work_key)), 0) AS candidate_bytes \
             FROM file_inventory \
             WHERE root_id = ?1 AND status = 'present' \
               AND {archive_filter} \
               AND {clause}{seen_clause} \
               AND EXISTS (SELECT 1 FROM catalog_kind_ownership \
                   WHERE kind = '{archive_kind}' AND authoritative_writer = 'catalog-v2')"
        );
        let mut archive_candidate_statement = sqlx::query(&archive_candidate_sql)
            .bind(event.root_id)
            .bind(event.generation)
            .bind(value);
        if preserve_seen_in_event {
            archive_candidate_statement = archive_candidate_statement.bind(event.seq);
        }
        let archive_candidates = archive_candidate_statement
            .fetch_one(&mut *transaction)
            .await?;
        let archive_count = archive_candidates.get::<i64, _>("candidate_count").max(0);
        let archive_bytes = archive_candidates.get::<i64, _>("candidate_bytes").max(0);
        if archive_count > 0 {
            if let Some(message) = ensure_catalog_queue_capacity(
                &mut transaction,
                event.root_id,
                archive_count,
                archive_bytes,
            )
            .await?
            {
                transaction.commit().await?;
                return Err(AppError::Other(message));
            }
            let archive_enqueue_sql = format!(
                "INSERT INTO scan_events (root_id, relative_path, event_kind, work_key, observed_at, status) \
                 SELECT ?1, relative_path, 'catalog-delete', work_key, strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'pending' \
                 FROM file_inventory \
                 WHERE root_id = ?1 AND status = 'present' \
                   AND {archive_filter} \
                   AND {clause}{seen_clause} \
                 ON CONFLICT DO UPDATE SET \
                     relative_path = excluded.relative_path, event_kind = excluded.event_kind, \
                     observed_at = excluded.observed_at, attempts = 0, last_error = NULL"
            );
            let mut archive_enqueue_statement = sqlx::query(&archive_enqueue_sql)
                .bind(event.root_id)
                .bind(event.generation)
                .bind(value);
            if preserve_seen_in_event {
                archive_enqueue_statement = archive_enqueue_statement.bind(event.seq);
            }
            archive_enqueue_statement.execute(&mut *transaction).await?;
        }
    }
    let gallery_source_clause = if event.root.kind == "gallery" {
        match scope {
            EventScope::WorkKey(_) => Some("work_key = ?2"),
            EventScope::Prefix(_) => {
                Some("(work_key = ?2 OR substr(work_key, 1, length(?2) + 1) = ?2 || '/')")
            }
            EventScope::Exact(_) => None,
        }
    } else {
        None
    };
    let mut enqueue_gallery_catalog_events = false;
    if let Some(source_clause) = gallery_source_clause {
        let owns_gallery = sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM catalog_kind_ownership WHERE kind = 'gallery' AND authoritative_writer = 'catalog-v2'",
        )
        .fetch_optional(&mut *transaction)
        .await?
        .is_some();
        if owns_gallery {
            sqlx::query(
                r#"
                CREATE TEMP TABLE IF NOT EXISTS temp_gallery_event_keys (
                    work_key TEXT PRIMARY KEY
                ) WITHOUT ROWID
                "#,
            )
            .execute(&mut *transaction)
            .await?;
            sqlx::query("DELETE FROM temp_gallery_event_keys")
                .execute(&mut *transaction)
                .await?;
            let inventory_key_sql = format!(
                "INSERT OR IGNORE INTO temp_gallery_event_keys(work_key) \
                 SELECT DISTINCT work_key FROM file_inventory \
                 WHERE root_id = ?1 AND status = 'present' AND work_key IS NOT NULL \
                   AND length(work_key) > 0 AND {clause}{seen_clause} \
                   AND (lower(relative_path) GLOB '*.jpg' \
                     OR lower(relative_path) GLOB '*.jpeg' \
                     OR lower(relative_path) GLOB '*.png' \
                     OR lower(relative_path) GLOB '*.webp' \
                     OR lower(relative_path) GLOB '*.gif' \
                     OR lower(relative_path) GLOB '*.avif' \
                     OR lower(relative_path) GLOB '*.bmp')"
            );
            let mut inventory_key_statement = sqlx::query(&inventory_key_sql)
                .bind(event.root_id)
                .bind(event.generation)
                .bind(value);
            if preserve_seen_in_event {
                inventory_key_statement = inventory_key_statement.bind(event.seq);
            }
            inventory_key_statement.execute(&mut *transaction).await?;
            let source_key_sql = format!(
                "INSERT OR IGNORE INTO temp_gallery_event_keys(work_key) \
                 SELECT work_key FROM catalog_work_sources \
                 WHERE kind = 'gallery' AND root_id = ?1 AND {source_clause}"
            );
            sqlx::query(&source_key_sql)
                .bind(event.root_id)
                .bind(value)
                .execute(&mut *transaction)
                .await?;
            let candidates = sqlx::query(
                r#"
                SELECT
                    COALESCE(SUM(CASE WHEN NOT EXISTS (
                        SELECT 1 FROM scan_events AS queued
                        WHERE queued.root_id = ?1
                          AND queued.work_key = keys.work_key
                          AND queued.status = 'pending'
                          AND queued.event_kind IN ('catalog-upsert', 'catalog-delete')
                    ) THEN 1 ELSE 0 END), 0) AS candidate_count,
                    COALESCE(SUM(CASE WHEN NOT EXISTS (
                        SELECT 1 FROM scan_events AS queued
                        WHERE queued.root_id = ?1
                          AND queued.work_key = keys.work_key
                          AND queued.status = 'pending'
                          AND queued.event_kind IN ('catalog-upsert', 'catalog-delete')
                    ) THEN length(keys.work_key) ELSE 0 END), 0) AS candidate_bytes,
                    COUNT(*) AS total_keys
                FROM temp_gallery_event_keys AS keys
                "#,
            )
            .bind(event.root_id)
            .fetch_one(&mut *transaction)
            .await?;
            let candidate_count = candidates.get::<i64, _>("candidate_count").max(0);
            let candidate_bytes = candidates.get::<i64, _>("candidate_bytes").max(0);
            enqueue_gallery_catalog_events = candidates.get::<i64, _>("total_keys") > 0;
            if let Some(message) = ensure_catalog_queue_capacity(
                &mut transaction,
                event.root_id,
                candidate_count,
                candidate_bytes,
            )
            .await?
            {
                transaction.commit().await?;
                return Err(AppError::Other(message));
            }
        }
    }
    let audio_work_key = if event.root.kind == "audio" {
        match scope {
            EventScope::WorkKey(work_key) => Some(work_key.as_str()),
            _ => None,
        }
    } else {
        None
    };
    let mut enqueue_audio_catalog_event = false;
    if let Some(work_key) = audio_work_key {
        let affected_sql = if preserve_seen_in_event {
            r#"
            SELECT EXISTS(
                SELECT 1 FROM file_inventory
                WHERE root_id = ?1 AND work_key = ?2 AND status = 'present'
                  AND (seen_event_seq IS NULL OR seen_event_seq != ?3)
            ) AND EXISTS(
                SELECT 1 FROM catalog_kind_ownership
                WHERE kind = 'audio' AND authoritative_writer = 'catalog-v2'
            )
            "#
        } else {
            r#"
            SELECT EXISTS(
                SELECT 1 FROM file_inventory
                WHERE root_id = ?1 AND work_key = ?2 AND status = 'present'
            ) AND EXISTS(
                SELECT 1 FROM catalog_kind_ownership
                WHERE kind = 'audio' AND authoritative_writer = 'catalog-v2'
            )
            "#
        };
        let mut affected_statement = sqlx::query_scalar::<_, i64>(affected_sql)
            .bind(event.root_id)
            .bind(work_key);
        if preserve_seen_in_event {
            affected_statement = affected_statement.bind(event.seq);
        }
        enqueue_audio_catalog_event = affected_statement.fetch_one(&mut *transaction).await? != 0;
        if enqueue_audio_catalog_event {
            let already_pending = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT 1 FROM scan_events
                WHERE root_id = ?1 AND work_key = ?2 AND status = 'pending'
                  AND event_kind IN ('catalog-upsert', 'catalog-delete')
                LIMIT 1
                "#,
            )
            .bind(event.root_id)
            .bind(work_key)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
            if let Some(message) = ensure_catalog_queue_capacity(
                &mut transaction,
                event.root_id,
                i64::from(!already_pending),
                if already_pending {
                    0
                } else {
                    i64::try_from(work_key.len()).unwrap_or(i64::MAX)
                },
            )
            .await?
            {
                transaction.commit().await?;
                return Err(AppError::Other(message));
            }
        }
    }
    let sql = format!(
        "UPDATE file_inventory SET status = 'missing', last_error = NULL \
         WHERE root_id = ?1 AND status = 'present' AND {clause}{seen_clause} \
         AND EXISTS (SELECT 1 FROM library_roots \
             WHERE id = ?1 AND generation = ?2 AND enabled = 1 AND status != 'scanning')"
    );
    let mut statement = sqlx::query(&sql)
        .bind(event.root_id)
        .bind(event.generation)
        .bind(value);
    if preserve_seen_in_event {
        statement = statement.bind(event.seq);
    }
    let missing = statement.execute(&mut *transaction).await?.rows_affected();
    if enqueue_gallery_catalog_events {
        sqlx::query(
            r#"
            INSERT INTO scan_events (
                root_id, relative_path, event_kind, work_key, observed_at, status
            )
            SELECT
                ?1,
                keys.work_key,
                CASE WHEN EXISTS (
                    SELECT 1 FROM file_inventory AS inventory
                    WHERE inventory.root_id = ?1
                      AND inventory.work_key = keys.work_key
                      AND inventory.status = 'present'
                      AND (
                            lower(inventory.relative_path) GLOB '*.jpg'
                         OR lower(inventory.relative_path) GLOB '*.jpeg'
                         OR lower(inventory.relative_path) GLOB '*.png'
                         OR lower(inventory.relative_path) GLOB '*.webp'
                         OR lower(inventory.relative_path) GLOB '*.gif'
                         OR lower(inventory.relative_path) GLOB '*.avif'
                         OR lower(inventory.relative_path) GLOB '*.bmp'
                      )
                ) THEN 'catalog-upsert' ELSE 'catalog-delete' END,
                keys.work_key,
                strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                'pending'
            FROM temp_gallery_event_keys AS keys
            WHERE 1
            ON CONFLICT DO UPDATE SET
                relative_path = excluded.relative_path,
                event_kind = excluded.event_kind,
                observed_at = excluded.observed_at,
                attempts = 0,
                last_error = NULL
            "#,
        )
        .bind(event.root_id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query("DELETE FROM temp_gallery_event_keys")
            .execute(&mut *transaction)
            .await?;
    }
    if enqueue_audio_catalog_event {
        let work_key = audio_work_key.expect("audio event work key was checked above");
        let has_track = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT 1 FROM file_inventory
            WHERE root_id = ?1 AND work_key = ?2 AND status = 'present'
              AND (
                    lower(relative_path) GLOB '*.mp3'
                 OR lower(relative_path) GLOB '*.wav'
                 OR lower(relative_path) GLOB '*.flac'
                 OR lower(relative_path) GLOB '*.ogg'
                 OR lower(relative_path) GLOB '*.m4a'
                 OR lower(relative_path) GLOB '*.aac'
                 OR lower(relative_path) GLOB '*.opus'
                 OR (lower(relative_path) GLOB '*.strm' AND EXISTS (
                        SELECT 1 FROM library_roots AS qms_root
                        WHERE qms_root.id = file_inventory.root_id
                          AND qms_root.provider LIKE 'qmediasync%'
                    ))
              )
            LIMIT 1
            "#,
        )
        .bind(event.root_id)
        .bind(work_key)
        .fetch_optional(&mut *transaction)
        .await?
        .is_some();
        sqlx::query(
            r#"
            INSERT INTO scan_events (
                root_id, relative_path, event_kind, work_key, observed_at, status
            )
            VALUES (
                ?1, ?2, ?3, ?4, strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'pending'
            )
            ON CONFLICT DO UPDATE SET
                relative_path = excluded.relative_path,
                event_kind = excluded.event_kind,
                observed_at = excluded.observed_at,
                attempts = 0,
                last_error = NULL
            "#,
        )
        .bind(event.root_id)
        .bind(&event.relative_path)
        .bind(if has_track {
            "catalog-upsert"
        } else {
            CATALOG_DELETE_EVENT
        })
        .bind(work_key)
        .execute(&mut *transaction)
        .await?;
    }
    sqlx::query(
        r#"
        UPDATE library_roots
        SET present_files = MAX(0, present_files - ?3),
            missing_files = missing_files + ?3,
            last_event_seq = ?4
        WHERE id = ?1 AND generation = ?2 AND enabled = 1 AND status != 'scanning'
        "#,
    )
    .bind(event.root_id)
    .bind(event.generation)
    .bind(i64::try_from(missing).unwrap_or(i64::MAX))
    .bind(event.seq)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(missing)
}

async fn ensure_event_fence(
    transaction: &mut sqlx_core::transaction::Transaction<'_, crate::Sqlite>,
    event: &PendingEvent,
) -> Result<()> {
    let valid = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT 1 FROM library_roots
        WHERE id = ?1 AND generation = ?2 AND enabled = 1 AND status != 'scanning'
        "#,
    )
    .bind(event.root_id)
    .bind(event.generation)
    .fetch_optional(&mut **transaction)
    .await?;
    if valid.is_none() {
        return Err(AppError::Other(format!(
            "inventory event fence rejected root {} generation {}",
            event.root_id, event.generation
        )));
    }
    Ok(())
}

async fn complete_event(db: &Db, event: &PendingEvent) -> Result<()> {
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    sqlx::query(
        r#"
        UPDATE scan_events SET status = 'done', last_error = NULL
        WHERE seq = ?1 AND status = 'processing'
        "#,
    )
    .bind(event.seq)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

async fn fail_event(db: &Db, event: &PendingEvent, error: &str, retry: bool) -> Result<()> {
    let status = if retry { "pending" } else { "failed" };
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    sqlx::query(
        r#"
        UPDATE scan_events SET status = ?2, last_error = ?3
        WHERE seq = ?1 AND status = 'processing'
        "#,
    )
    .bind(event.seq)
    .bind(status)
    .bind(bounded_error(error))
    .execute(&mut *transaction)
    .await?;
    if !retry {
        sqlx::query(
            r#"
            UPDATE library_roots
            SET status = CASE WHEN status = 'scanning' THEN status ELSE 'needs_reconcile' END,
                last_error = ?2
            WHERE id = ?1
            "#,
        )
        .bind(event.root_id)
        .bind(bounded_error(error))
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;
    Ok(())
}

async fn prune_completed_events(db: &Db, retain: i64, batch: i64) -> Result<()> {
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    sqlx::query(
        r#"
        DELETE FROM scan_events
        WHERE seq IN (
            SELECT seq FROM scan_events
            WHERE status = 'done'
              AND seq < (SELECT COALESCE(MAX(seq), 0) - ?1 FROM scan_events)
            ORDER BY seq
            LIMIT ?2
        )
        "#,
    )
    .bind(retain.max(0))
    .bind(batch.clamp(1, 4096))
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

async fn ensure_catalog_queue_capacity(
    transaction: &mut sqlx_core::transaction::Transaction<'_, crate::Sqlite>,
    root_id: i64,
    additional_keys: i64,
    additional_bytes: i64,
) -> Result<Option<String>> {
    let queued = sqlx::query(
        r#"
        SELECT COUNT(*) AS key_count,
               COALESCE(SUM(length(COALESCE(work_key, ''))), 0) AS key_bytes
        FROM scan_events
        WHERE root_id = ?1
          AND status IN ('pending', 'processing')
          AND event_kind IN ('catalog-upsert', 'catalog-delete')
        "#,
    )
    .bind(root_id)
    .fetch_one(&mut **transaction)
    .await?;
    let key_count = queued.get::<i64, _>("key_count").max(0);
    let key_bytes = queued.get::<i64, _>("key_bytes").max(0);
    if key_count.saturating_add(additional_keys) <= CHANGED_KEY_LIMIT
        && key_bytes.saturating_add(additional_bytes) <= CHANGED_KEY_BYTES_LIMIT
    {
        return Ok(None);
    }
    let message = format!(
        "catalog changed-key queue capacity exceeded for root {root_id} (keys={}, bytes={})",
        key_count.saturating_add(additional_keys),
        key_bytes.saturating_add(additional_bytes)
    );
    sqlx::query(
        r#"
        UPDATE library_roots
        SET status = CASE WHEN status = 'scanning' THEN status ELSE 'needs_reconcile' END,
            last_error = ?2
        WHERE id = ?1
        "#,
    )
    .bind(root_id)
    .bind(bounded_error(&message))
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO novel_coordinator_state (
            root_id, generation, phase, checkpoint_seq, pending_keys,
            failed_keys, last_error, updated_at
        )
        SELECT id, generation, 'needs_reconcile', 0, ?2, 0, ?3, strftime('%Y-%m-%dT%H:%M:%fZ','now')
        FROM library_roots WHERE id = ?1
        ON CONFLICT(root_id) DO UPDATE SET
            generation = excluded.generation,
            phase = excluded.phase,
            pending_keys = excluded.pending_keys,
            last_error = excluded.last_error,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(root_id)
    .bind(key_count.saturating_add(additional_keys))
    .bind(bounded_error(&message))
    .execute(&mut **transaction)
    .await?;
    Ok(Some(message))
}

/// Enqueue changed archive keys from the bounded full-reconcile temp table.
/// The root-kind and ownership checks prevent a ZIP under one media root from
/// being claimed by another archive coordinator.
async fn enqueue_archive_temp_catalog_events(
    transaction: &mut sqlx_core::transaction::Transaction<'_, crate::Sqlite>,
    root_id: i64,
    kind: &str,
) -> Result<(usize, Option<String>)> {
    if !matches!(kind, "comic" | "coser-picture") {
        return Err(AppError::BadRequest(format!(
            "unsupported archive catalog kind {kind}"
        )));
    }
    let candidates = sqlx::query(
        r#"
        WITH candidates AS (
            SELECT batch.relative_path, batch.work_key
            FROM temp_inventory_batch AS batch
            LEFT JOIN file_inventory AS inventory
              ON inventory.root_id = ?1 AND inventory.relative_path = batch.relative_path
            WHERE (
                    (?2 = 'comic' AND (
                        lower(batch.relative_path) GLOB '*.cbz'
                        OR lower(batch.relative_path) GLOB '*.zip'
                        OR (lower(batch.relative_path) GLOB '*.strm' AND EXISTS (
                            SELECT 1 FROM library_roots AS root
                            WHERE root.id = ?1
                              AND (root.provider = 'qmediasync' OR root.provider LIKE 'qmediasync:%')
                        ))
                    ))
                    OR (?2 = 'coser-picture' AND (
                        lower(batch.relative_path) GLOB '*.zip'
                        OR (lower(batch.relative_path) GLOB '*.strm' AND EXISTS (
                            SELECT 1 FROM library_roots AS root
                            WHERE root.id = ?1
                              AND (root.provider = 'qmediasync' OR root.provider LIKE 'qmediasync:%')
                        ))
                    ))
                  )
              AND EXISTS (
                  SELECT 1 FROM library_roots AS root
                  WHERE root.id = ?1 AND root.kind = ?2
              )
              AND (
                    inventory.relative_path IS NULL
                 OR inventory.fast_fingerprint IS NOT batch.fast_fingerprint
                 OR inventory.media_class IS NOT batch.media_class
                 OR inventory.work_key IS NOT batch.work_key
                 OR inventory.status != 'present'
                 OR EXISTS (
                     SELECT 1 FROM catalog_work_sources AS source
                     JOIN works ON works.id = source.work_id
                     WHERE source.kind = ?2
                       AND source.root_id = ?1
                       AND source.work_key = batch.work_key
                       AND works.deleted_at IS NOT NULL
                 )
                 OR NOT EXISTS (
                     SELECT 1 FROM catalog_work_sources AS source
                     WHERE source.kind = ?2
                       AND source.root_id = ?1
                       AND source.work_key = batch.work_key
                 )
              )
              AND EXISTS (
                  SELECT 1 FROM catalog_kind_ownership
                  WHERE kind = ?2 AND authoritative_writer = 'catalog-v2'
              )
        )
        SELECT COUNT(*) AS candidate_count,
               COALESCE(SUM(length(work_key)), 0) AS candidate_bytes
        FROM candidates
        "#,
    )
    .bind(root_id)
    .bind(kind)
    .fetch_one(&mut **transaction)
    .await?;
    let candidate_count = candidates.get::<i64, _>("candidate_count").max(0);
    let candidate_bytes = candidates.get::<i64, _>("candidate_bytes").max(0);
    if candidate_count == 0 {
        return Ok((0, None));
    }
    let overflow =
        ensure_catalog_queue_capacity(transaction, root_id, candidate_count, candidate_bytes)
            .await?;
    if overflow.is_some() {
        return Ok((0, overflow));
    }
    let inserted = sqlx::query(
        r#"
        INSERT INTO scan_events (
            root_id, relative_path, event_kind, work_key, observed_at, status
        )
        SELECT
            ?1, batch.relative_path, 'catalog-upsert', batch.work_key,
            strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'pending'
        FROM temp_inventory_batch AS batch
        LEFT JOIN file_inventory AS inventory
          ON inventory.root_id = ?1 AND inventory.relative_path = batch.relative_path
        WHERE (
                (?2 = 'comic' AND (
                    lower(batch.relative_path) GLOB '*.cbz'
                    OR lower(batch.relative_path) GLOB '*.zip'
                    OR (lower(batch.relative_path) GLOB '*.strm' AND EXISTS (
                        SELECT 1 FROM library_roots AS root
                        WHERE root.id = ?1
                          AND (root.provider = 'qmediasync' OR root.provider LIKE 'qmediasync:%')
                    ))
                ))
                OR (?2 = 'coser-picture' AND (
                    lower(batch.relative_path) GLOB '*.zip'
                    OR (lower(batch.relative_path) GLOB '*.strm' AND EXISTS (
                        SELECT 1 FROM library_roots AS root
                        WHERE root.id = ?1
                          AND (root.provider = 'qmediasync' OR root.provider LIKE 'qmediasync:%')
                    ))
                ))
              )
          AND EXISTS (
              SELECT 1 FROM library_roots AS root
              WHERE root.id = ?1 AND root.kind = ?2
          )
          AND (
                inventory.relative_path IS NULL
             OR inventory.fast_fingerprint IS NOT batch.fast_fingerprint
             OR inventory.media_class IS NOT batch.media_class
             OR inventory.work_key IS NOT batch.work_key
             OR inventory.status != 'present'
             OR EXISTS (
                 SELECT 1 FROM catalog_work_sources AS source
                 JOIN works ON works.id = source.work_id
                 WHERE source.kind = ?2
                   AND source.root_id = ?1
                   AND source.work_key = batch.work_key
                   AND works.deleted_at IS NOT NULL
             )
             OR NOT EXISTS (
                 SELECT 1 FROM catalog_work_sources AS source
                 WHERE source.kind = ?2
                   AND source.root_id = ?1
                   AND source.work_key = batch.work_key
             )
          )
          AND EXISTS (
              SELECT 1 FROM catalog_kind_ownership
              WHERE kind = ?2 AND authoritative_writer = 'catalog-v2'
          )
        ON CONFLICT DO UPDATE SET
            relative_path = excluded.relative_path,
            event_kind = excluded.event_kind,
            observed_at = excluded.observed_at,
            attempts = 0,
            last_error = NULL
        "#,
    )
    .bind(root_id)
    .bind(kind)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    Ok((usize::try_from(inserted).unwrap_or(usize::MAX), None))
}

async fn enqueue_archive_event_catalog_events(
    transaction: &mut sqlx_core::transaction::Transaction<'_, crate::Sqlite>,
    event: &PendingEvent,
    payload: &str,
    kind: &str,
) -> Result<(usize, Option<String>)> {
    if !matches!(kind, "comic" | "coser-picture") || event.root.kind != kind {
        return Ok((0, None));
    }
    let candidates = sqlx::query(
        r#"
        WITH candidates AS (
            SELECT json_extract(input.value, '$.p') AS relative_path,
                   json_extract(input.value, '$.w') AS work_key
            FROM json_each(?2) AS input
            LEFT JOIN file_inventory AS inventory
              ON inventory.root_id = ?1
             AND inventory.relative_path = json_extract(input.value, '$.p')
            WHERE (
                    (?3 = 'comic' AND (
                        lower(json_extract(input.value, '$.p')) GLOB '*.cbz'
                        OR lower(json_extract(input.value, '$.p')) GLOB '*.zip'
                        OR (lower(json_extract(input.value, '$.p')) GLOB '*.strm' AND EXISTS (
                            SELECT 1 FROM library_roots AS root
                            WHERE root.id = ?1
                              AND (root.provider = 'qmediasync' OR root.provider LIKE 'qmediasync:%')
                        ))
                    ))
                    OR (?3 = 'coser-picture'
                        AND (
                            lower(json_extract(input.value, '$.p')) GLOB '*.zip'
                            OR (lower(json_extract(input.value, '$.p')) GLOB '*.strm' AND EXISTS (
                                SELECT 1 FROM library_roots AS root
                                WHERE root.id = ?1
                                  AND (root.provider = 'qmediasync' OR root.provider LIKE 'qmediasync:%')
                            ))
                        ))
                  )
              AND (
                    inventory.relative_path IS NULL
                 OR inventory.fast_fingerprint IS NOT json_extract(input.value, '$.f')
                 OR inventory.media_class IS NOT json_extract(input.value, '$.c')
                 OR inventory.work_key IS NOT json_extract(input.value, '$.w')
                 OR inventory.status != 'present'
                 OR EXISTS (
                     SELECT 1 FROM catalog_work_sources AS source
                     JOIN works ON works.id = source.work_id
                     WHERE source.kind = ?3
                       AND source.root_id = ?1
                       AND source.work_key = json_extract(input.value, '$.w')
                       AND works.deleted_at IS NOT NULL
                 )
                 OR NOT EXISTS (
                     SELECT 1 FROM catalog_work_sources AS source
                     WHERE source.kind = ?3
                       AND source.root_id = ?1
                       AND source.work_key = json_extract(input.value, '$.w')
                 )
              )
              AND EXISTS (
                  SELECT 1 FROM catalog_kind_ownership
                  WHERE kind = ?3 AND authoritative_writer = 'catalog-v2'
              )
        )
        SELECT COUNT(*) AS candidate_count,
               COALESCE(SUM(length(work_key)), 0) AS candidate_bytes
        FROM candidates
        "#,
    )
    .bind(event.root_id)
    .bind(payload)
    .bind(kind)
    .fetch_one(&mut **transaction)
    .await?;
    let candidate_count = candidates.get::<i64, _>("candidate_count").max(0);
    let candidate_bytes = candidates.get::<i64, _>("candidate_bytes").max(0);
    if candidate_count == 0 {
        return Ok((0, None));
    }
    let overflow =
        ensure_catalog_queue_capacity(transaction, event.root_id, candidate_count, candidate_bytes)
            .await?;
    if overflow.is_some() {
        return Ok((0, overflow));
    }
    let inserted = sqlx::query(
        r#"
        INSERT INTO scan_events (
            root_id, relative_path, event_kind, work_key, observed_at, status
        )
        SELECT
            ?1, json_extract(input.value, '$.p'), 'catalog-upsert',
            json_extract(input.value, '$.w'), strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'pending'
        FROM json_each(?2) AS input
        LEFT JOIN file_inventory AS inventory
          ON inventory.root_id = ?1
         AND inventory.relative_path = json_extract(input.value, '$.p')
        WHERE (
                (?3 = 'comic' AND (
                    lower(json_extract(input.value, '$.p')) GLOB '*.cbz'
                    OR lower(json_extract(input.value, '$.p')) GLOB '*.zip'
                    OR (lower(json_extract(input.value, '$.p')) GLOB '*.strm' AND EXISTS (
                        SELECT 1 FROM library_roots AS root
                        WHERE root.id = ?1
                          AND (root.provider = 'qmediasync' OR root.provider LIKE 'qmediasync:%')
                    ))
                ))
                OR (?3 = 'coser-picture'
                    AND (
                        lower(json_extract(input.value, '$.p')) GLOB '*.zip'
                        OR (lower(json_extract(input.value, '$.p')) GLOB '*.strm' AND EXISTS (
                            SELECT 1 FROM library_roots AS root
                            WHERE root.id = ?1
                              AND (root.provider = 'qmediasync' OR root.provider LIKE 'qmediasync:%')
                        ))
                    ))
              )
          AND (
                inventory.relative_path IS NULL
             OR inventory.fast_fingerprint IS NOT json_extract(input.value, '$.f')
             OR inventory.media_class IS NOT json_extract(input.value, '$.c')
             OR inventory.work_key IS NOT json_extract(input.value, '$.w')
             OR inventory.status != 'present'
             OR EXISTS (
                 SELECT 1 FROM catalog_work_sources AS source
                 JOIN works ON works.id = source.work_id
                 WHERE source.kind = ?3
                   AND source.root_id = ?1
                   AND source.work_key = json_extract(input.value, '$.w')
                   AND works.deleted_at IS NOT NULL
             )
             OR NOT EXISTS (
                 SELECT 1 FROM catalog_work_sources AS source
                 WHERE source.kind = ?3
                   AND source.root_id = ?1
                   AND source.work_key = json_extract(input.value, '$.w')
             )
          )
          AND EXISTS (
              SELECT 1 FROM catalog_kind_ownership
              WHERE kind = ?3 AND authoritative_writer = 'catalog-v2'
          )
        ON CONFLICT DO UPDATE SET
            relative_path = excluded.relative_path,
            event_kind = excluded.event_kind,
            observed_at = excluded.observed_at,
            attempts = 0,
            last_error = NULL
        "#,
    )
    .bind(event.root_id)
    .bind(payload)
    .bind(kind)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    Ok((usize::try_from(inserted).unwrap_or(usize::MAX), None))
}

async fn enqueue_audio_event_catalog_events(
    transaction: &mut sqlx_core::transaction::Transaction<'_, crate::Sqlite>,
    event: &PendingEvent,
    payload: &str,
) -> Result<(usize, Option<String>)> {
    if event.root.kind != "audio" {
        return Ok((0, None));
    }
    let candidates = sqlx::query(
        r#"
        WITH candidates AS (
            SELECT
                json_extract(input.value, '$.w') AS work_key,
                MIN(json_extract(input.value, '$.p')) AS relative_path
            FROM json_each(?2) AS input
            LEFT JOIN file_inventory AS inventory
              ON inventory.root_id = ?1
             AND inventory.relative_path = json_extract(input.value, '$.p')
            WHERE json_extract(input.value, '$.w') IS NOT NULL
              AND length(json_extract(input.value, '$.w')) > 0
              AND (
                    lower(json_extract(input.value, '$.p')) GLOB '*.mp3'
                 OR lower(json_extract(input.value, '$.p')) GLOB '*.wav'
                 OR lower(json_extract(input.value, '$.p')) GLOB '*.flac'
                 OR lower(json_extract(input.value, '$.p')) GLOB '*.ogg'
                 OR lower(json_extract(input.value, '$.p')) GLOB '*.m4a'
                 OR lower(json_extract(input.value, '$.p')) GLOB '*.aac'
                 OR lower(json_extract(input.value, '$.p')) GLOB '*.opus'
                 OR (lower(json_extract(input.value, '$.p')) GLOB '*.strm' AND EXISTS (
                        SELECT 1 FROM library_roots AS qms_root
                        WHERE qms_root.id = ?1
                          AND qms_root.provider LIKE 'qmediasync%'
                    ))
                 OR lower(json_extract(input.value, '$.p')) GLOB '*.jpg'
                 OR lower(json_extract(input.value, '$.p')) GLOB '*.jpeg'
                 OR lower(json_extract(input.value, '$.p')) GLOB '*.png'
                 OR lower(json_extract(input.value, '$.p')) GLOB '*.webp'
                 OR lower(json_extract(input.value, '$.p')) GLOB '*.txt'
              )
              AND (
                    inventory.relative_path IS NULL
                 OR inventory.fast_fingerprint IS NOT json_extract(input.value, '$.f')
                 OR inventory.media_class IS NOT json_extract(input.value, '$.c')
                 OR inventory.work_key IS NOT json_extract(input.value, '$.w')
                 OR inventory.status != 'present'
                 OR EXISTS (
                     SELECT 1 FROM catalog_work_sources AS source
                     JOIN works ON works.id = source.work_id
                     WHERE source.kind = 'audio'
                       AND source.root_id = ?1
                       AND source.work_key = json_extract(input.value, '$.w')
                       AND works.deleted_at IS NOT NULL
                 )
                 OR NOT EXISTS (
                     SELECT 1 FROM catalog_work_sources AS source
                     WHERE source.kind = 'audio'
                       AND source.root_id = ?1
                       AND source.work_key = json_extract(input.value, '$.w')
                 )
              )
              AND EXISTS (
                  SELECT 1 FROM library_roots AS root
                  WHERE root.id = ?1 AND root.kind = 'audio'
              )
              AND EXISTS (
                  SELECT 1 FROM catalog_kind_ownership
                  WHERE kind = 'audio' AND authoritative_writer = 'catalog-v2'
              )
            GROUP BY json_extract(input.value, '$.w')
        )
        SELECT
            COALESCE(SUM(CASE WHEN NOT EXISTS (
                SELECT 1 FROM scan_events AS queued
                WHERE queued.root_id = ?1
                  AND queued.work_key = candidates.work_key
                  AND queued.status = 'pending'
                  AND queued.event_kind IN ('catalog-upsert', 'catalog-delete')
            ) THEN 1 ELSE 0 END), 0) AS candidate_count,
            COALESCE(SUM(CASE WHEN NOT EXISTS (
                SELECT 1 FROM scan_events AS queued
                WHERE queued.root_id = ?1
                  AND queued.work_key = candidates.work_key
                  AND queued.status = 'pending'
                  AND queued.event_kind IN ('catalog-upsert', 'catalog-delete')
            ) THEN length(candidates.work_key) ELSE 0 END), 0) AS candidate_bytes
        FROM candidates
        "#,
    )
    .bind(event.root_id)
    .bind(payload)
    .fetch_one(&mut **transaction)
    .await?;
    let candidate_count = candidates.get::<i64, _>("candidate_count").max(0);
    let candidate_bytes = candidates.get::<i64, _>("candidate_bytes").max(0);
    if let Some(overflow) =
        ensure_catalog_queue_capacity(transaction, event.root_id, candidate_count, candidate_bytes)
            .await?
    {
        return Ok((0, Some(overflow)));
    }
    let inserted = sqlx::query(
        r#"
        INSERT INTO scan_events (
            root_id, relative_path, event_kind, work_key, observed_at, status
        )
        SELECT
            ?1,
            MIN(json_extract(input.value, '$.p')),
            'catalog-upsert',
            json_extract(input.value, '$.w'),
            strftime('%Y-%m-%dT%H:%M:%fZ','now'),
            'pending'
        FROM json_each(?2) AS input
        LEFT JOIN file_inventory AS inventory
          ON inventory.root_id = ?1
         AND inventory.relative_path = json_extract(input.value, '$.p')
        WHERE json_extract(input.value, '$.w') IS NOT NULL
          AND length(json_extract(input.value, '$.w')) > 0
          AND (
                lower(json_extract(input.value, '$.p')) GLOB '*.mp3'
             OR lower(json_extract(input.value, '$.p')) GLOB '*.wav'
             OR lower(json_extract(input.value, '$.p')) GLOB '*.flac'
             OR lower(json_extract(input.value, '$.p')) GLOB '*.ogg'
             OR lower(json_extract(input.value, '$.p')) GLOB '*.m4a'
             OR lower(json_extract(input.value, '$.p')) GLOB '*.aac'
             OR lower(json_extract(input.value, '$.p')) GLOB '*.opus'
             OR (lower(json_extract(input.value, '$.p')) GLOB '*.strm' AND EXISTS (
                    SELECT 1 FROM library_roots AS qms_root
                    WHERE qms_root.id = ?1
                      AND qms_root.provider LIKE 'qmediasync%'
                ))
             OR lower(json_extract(input.value, '$.p')) GLOB '*.jpg'
             OR lower(json_extract(input.value, '$.p')) GLOB '*.jpeg'
             OR lower(json_extract(input.value, '$.p')) GLOB '*.png'
             OR lower(json_extract(input.value, '$.p')) GLOB '*.webp'
             OR lower(json_extract(input.value, '$.p')) GLOB '*.txt'
          )
          AND (
                inventory.relative_path IS NULL
             OR inventory.fast_fingerprint IS NOT json_extract(input.value, '$.f')
             OR inventory.media_class IS NOT json_extract(input.value, '$.c')
             OR inventory.work_key IS NOT json_extract(input.value, '$.w')
             OR inventory.status != 'present'
             OR EXISTS (
                 SELECT 1 FROM catalog_work_sources AS source
                 JOIN works ON works.id = source.work_id
                 WHERE source.kind = 'audio'
                   AND source.root_id = ?1
                   AND source.work_key = json_extract(input.value, '$.w')
                   AND works.deleted_at IS NOT NULL
             )
             OR NOT EXISTS (
                 SELECT 1 FROM catalog_work_sources AS source
                 WHERE source.kind = 'audio'
                   AND source.root_id = ?1
                   AND source.work_key = json_extract(input.value, '$.w')
             )
          )
          AND EXISTS (
              SELECT 1 FROM library_roots AS root
              WHERE root.id = ?1 AND root.kind = 'audio'
          )
          AND EXISTS (
              SELECT 1 FROM catalog_kind_ownership
              WHERE kind = 'audio' AND authoritative_writer = 'catalog-v2'
          )
        GROUP BY json_extract(input.value, '$.w')
        ON CONFLICT DO UPDATE SET
            relative_path = excluded.relative_path,
            event_kind = excluded.event_kind,
            observed_at = excluded.observed_at,
            attempts = 0,
            last_error = NULL
        "#,
    )
    .bind(event.root_id)
    .bind(payload)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    Ok((usize::try_from(inserted).unwrap_or(usize::MAX), None))
}

async fn enqueue_gallery_event_catalog_events(
    transaction: &mut sqlx_core::transaction::Transaction<'_, crate::Sqlite>,
    event: &PendingEvent,
    payload: &str,
) -> Result<(usize, Option<String>)> {
    if event.root.kind != "gallery" {
        return Ok((0, None));
    }
    let candidates = sqlx::query(
        r#"
        WITH candidates AS (
            SELECT
                json_extract(input.value, '$.w') AS work_key,
                MIN(json_extract(input.value, '$.p')) AS relative_path
            FROM json_each(?2) AS input
            LEFT JOIN file_inventory AS inventory
              ON inventory.root_id = ?1
             AND inventory.relative_path = json_extract(input.value, '$.p')
            WHERE json_extract(input.value, '$.w') IS NOT NULL
              AND length(json_extract(input.value, '$.w')) > 0
              AND (
                    lower(json_extract(input.value, '$.p')) GLOB '*.jpg'
                 OR lower(json_extract(input.value, '$.p')) GLOB '*.jpeg'
                 OR lower(json_extract(input.value, '$.p')) GLOB '*.png'
                 OR lower(json_extract(input.value, '$.p')) GLOB '*.webp'
                 OR lower(json_extract(input.value, '$.p')) GLOB '*.gif'
                 OR lower(json_extract(input.value, '$.p')) GLOB '*.avif'
                 OR lower(json_extract(input.value, '$.p')) GLOB '*.bmp'
              )
              AND (
                    inventory.relative_path IS NULL
                 OR inventory.fast_fingerprint IS NOT json_extract(input.value, '$.f')
                 OR inventory.media_class IS NOT json_extract(input.value, '$.c')
                 OR inventory.work_key IS NOT json_extract(input.value, '$.w')
                 OR inventory.status != 'present'
                 OR EXISTS (
                     SELECT 1 FROM catalog_work_sources AS source
                     JOIN works ON works.id = source.work_id
                     WHERE source.kind = 'gallery'
                       AND source.root_id = ?1
                       AND source.work_key = json_extract(input.value, '$.w')
                       AND works.deleted_at IS NOT NULL
                 )
                 OR NOT EXISTS (
                     SELECT 1 FROM catalog_work_sources AS source
                     WHERE source.kind = 'gallery'
                       AND source.root_id = ?1
                       AND source.work_key = json_extract(input.value, '$.w')
                 )
              )
              AND EXISTS (
                  SELECT 1 FROM catalog_kind_ownership
                  WHERE kind = 'gallery' AND authoritative_writer = 'catalog-v2'
              )
            GROUP BY json_extract(input.value, '$.w')
        )
        SELECT
            COALESCE(SUM(CASE WHEN NOT EXISTS (
                SELECT 1 FROM scan_events AS queued
                WHERE queued.root_id = ?1
                  AND queued.work_key = candidates.work_key
                  AND queued.status = 'pending'
                  AND queued.event_kind IN ('catalog-upsert', 'catalog-delete')
            ) THEN 1 ELSE 0 END), 0) AS candidate_count,
            COALESCE(SUM(CASE WHEN NOT EXISTS (
                SELECT 1 FROM scan_events AS queued
                WHERE queued.root_id = ?1
                  AND queued.work_key = candidates.work_key
                  AND queued.status = 'pending'
                  AND queued.event_kind IN ('catalog-upsert', 'catalog-delete')
            ) THEN length(candidates.work_key) ELSE 0 END), 0) AS candidate_bytes
        FROM candidates
        "#,
    )
    .bind(event.root_id)
    .bind(payload)
    .fetch_one(&mut **transaction)
    .await?;
    let candidate_count = candidates.get::<i64, _>("candidate_count").max(0);
    let candidate_bytes = candidates.get::<i64, _>("candidate_bytes").max(0);
    if let Some(overflow) =
        ensure_catalog_queue_capacity(transaction, event.root_id, candidate_count, candidate_bytes)
            .await?
    {
        return Ok((0, Some(overflow)));
    }
    let inserted = sqlx::query(
        r#"
        INSERT INTO scan_events (
            root_id, relative_path, event_kind, work_key, observed_at, status
        )
        SELECT
            ?1,
            MIN(json_extract(input.value, '$.p')),
            'catalog-upsert',
            json_extract(input.value, '$.w'),
            strftime('%Y-%m-%dT%H:%M:%fZ','now'),
            'pending'
        FROM json_each(?2) AS input
        LEFT JOIN file_inventory AS inventory
          ON inventory.root_id = ?1
         AND inventory.relative_path = json_extract(input.value, '$.p')
        WHERE json_extract(input.value, '$.w') IS NOT NULL
          AND length(json_extract(input.value, '$.w')) > 0
          AND (
                lower(json_extract(input.value, '$.p')) GLOB '*.jpg'
             OR lower(json_extract(input.value, '$.p')) GLOB '*.jpeg'
             OR lower(json_extract(input.value, '$.p')) GLOB '*.png'
             OR lower(json_extract(input.value, '$.p')) GLOB '*.webp'
             OR lower(json_extract(input.value, '$.p')) GLOB '*.gif'
             OR lower(json_extract(input.value, '$.p')) GLOB '*.avif'
             OR lower(json_extract(input.value, '$.p')) GLOB '*.bmp'
          )
          AND (
                inventory.relative_path IS NULL
             OR inventory.fast_fingerprint IS NOT json_extract(input.value, '$.f')
             OR inventory.media_class IS NOT json_extract(input.value, '$.c')
             OR inventory.work_key IS NOT json_extract(input.value, '$.w')
             OR inventory.status != 'present'
             OR EXISTS (
                 SELECT 1 FROM catalog_work_sources AS source
                 JOIN works ON works.id = source.work_id
                 WHERE source.kind = 'gallery'
                   AND source.root_id = ?1
                   AND source.work_key = json_extract(input.value, '$.w')
                   AND works.deleted_at IS NOT NULL
             )
             OR NOT EXISTS (
                 SELECT 1 FROM catalog_work_sources AS source
                 WHERE source.kind = 'gallery'
                   AND source.root_id = ?1
                   AND source.work_key = json_extract(input.value, '$.w')
             )
          )
          AND EXISTS (
              SELECT 1 FROM catalog_kind_ownership
              WHERE kind = 'gallery' AND authoritative_writer = 'catalog-v2'
          )
        GROUP BY json_extract(input.value, '$.w')
        ON CONFLICT DO UPDATE SET
            relative_path = excluded.relative_path,
            event_kind = excluded.event_kind,
            observed_at = excluded.observed_at,
            attempts = 0,
            last_error = NULL
        "#,
    )
    .bind(event.root_id)
    .bind(payload)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    Ok((usize::try_from(inserted).unwrap_or(usize::MAX), None))
}

async fn enqueue_missing_novel_catalog_events(
    db: &Db,
    root_id: i64,
    generation: i64,
    token: &str,
    limit: i64,
) -> Result<usize> {
    let _write_slot = db.acquire_write_slot(256 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    ensure_generation_fence(&mut transaction, root_id, generation, token).await?;
    let candidates = sqlx::query(
        r#"
        WITH candidates AS (
            SELECT source.work_key
            FROM catalog_work_sources AS source
            JOIN works ON works.id = source.work_id
            WHERE source.kind = 'novel'
              AND source.root_id = ?1
              AND works.deleted_at IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM file_inventory AS inventory
                  WHERE inventory.root_id = ?1
                    AND inventory.work_key = source.work_key
                    AND inventory.status = 'present'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM scan_events AS event
                  WHERE event.root_id = ?1
                    AND event.work_key = source.work_key
                    AND event.event_kind = 'catalog-delete'
                    AND event.status IN ('pending', 'processing', 'failed')
              )
            ORDER BY source.work_key
            LIMIT ?2
        )
        SELECT COUNT(*) AS candidate_count,
               COALESCE(SUM(length(work_key)), 0) AS candidate_bytes
        FROM candidates
        "#,
    )
    .bind(root_id)
    .bind(limit.clamp(1, NOVEL_COORDINATOR_BATCH_SIZE))
    .fetch_one(&mut *transaction)
    .await?;
    let candidate_count = candidates.get::<i64, _>("candidate_count").max(0);
    let candidate_bytes = candidates.get::<i64, _>("candidate_bytes").max(0);
    if candidate_count == 0 {
        transaction.commit().await?;
        return Ok(0);
    }
    if let Some(message) =
        ensure_catalog_queue_capacity(&mut transaction, root_id, candidate_count, candidate_bytes)
            .await?
    {
        transaction.commit().await?;
        return Err(AppError::Other(message));
    }
    let inserted = sqlx::query(
        r#"
        INSERT INTO scan_events (
            root_id, relative_path, event_kind, work_key, observed_at, status
        )
        SELECT
            ?1, candidates.work_key, 'catalog-delete', candidates.work_key,
            strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'pending'
        FROM (
            SELECT source.work_key
            FROM catalog_work_sources AS source
            JOIN works ON works.id = source.work_id
            WHERE source.kind = 'novel'
              AND source.root_id = ?1
              AND works.deleted_at IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM file_inventory AS inventory
                  WHERE inventory.root_id = ?1
                    AND inventory.work_key = source.work_key
                    AND inventory.status = 'present'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM scan_events AS event
                  WHERE event.root_id = ?1
                    AND event.work_key = source.work_key
                    AND event.event_kind = 'catalog-delete'
                    AND event.status IN ('pending', 'processing', 'failed')
              )
            ORDER BY source.work_key
            LIMIT ?2
        ) AS candidates
        "#,
    )
    .bind(root_id)
    .bind(limit.clamp(1, NOVEL_COORDINATOR_BATCH_SIZE))
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    transaction.commit().await?;
    Ok(usize::try_from(inserted).unwrap_or(usize::MAX))
}

async fn enqueue_missing_archive_catalog_events(
    db: &Db,
    root_id: i64,
    generation: i64,
    token: &str,
    limit: i64,
    kind: &str,
) -> Result<usize> {
    if !matches!(kind, "comic" | "coser-picture") {
        return Err(AppError::BadRequest(format!(
            "unsupported archive catalog kind {kind}"
        )));
    }
    let _write_slot = db.acquire_write_slot(256 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    ensure_generation_fence(&mut transaction, root_id, generation, token).await?;
    let limit = limit.clamp(1, NOVEL_COORDINATOR_BATCH_SIZE);
    let candidates = sqlx::query(
        r#"
        WITH candidates AS (
            SELECT source.work_key
            FROM catalog_work_sources AS source
            JOIN works ON works.id = source.work_id
            WHERE source.kind = ?3
              AND source.root_id = ?1
              AND works.deleted_at IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM file_inventory AS inventory
                  WHERE inventory.root_id = ?1
                    AND inventory.work_key = source.work_key
                    AND inventory.status = 'present'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM scan_events AS event
                  WHERE event.root_id = ?1
                    AND event.work_key = source.work_key
                    AND event.event_kind = 'catalog-delete'
                    AND event.status IN ('pending', 'processing', 'failed')
              )
            ORDER BY source.work_key
            LIMIT ?2
        )
        SELECT COUNT(*) AS candidate_count,
               COALESCE(SUM(length(work_key)), 0) AS candidate_bytes
        FROM candidates
        "#,
    )
    .bind(root_id)
    .bind(limit)
    .bind(kind)
    .fetch_one(&mut *transaction)
    .await?;
    let candidate_count = candidates.get::<i64, _>("candidate_count").max(0);
    let candidate_bytes = candidates.get::<i64, _>("candidate_bytes").max(0);
    if candidate_count == 0 {
        transaction.commit().await?;
        return Ok(0);
    }
    if let Some(message) =
        ensure_catalog_queue_capacity(&mut transaction, root_id, candidate_count, candidate_bytes)
            .await?
    {
        transaction.commit().await?;
        return Err(AppError::Other(message));
    }
    let inserted = sqlx::query(
        r#"
        INSERT INTO scan_events (
            root_id, relative_path, event_kind, work_key, observed_at, status
        )
        SELECT
            ?1, candidates.work_key, 'catalog-delete', candidates.work_key,
            strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'pending'
        FROM (
            SELECT source.work_key
            FROM catalog_work_sources AS source
            JOIN works ON works.id = source.work_id
            WHERE source.kind = ?3
              AND source.root_id = ?1
              AND works.deleted_at IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM file_inventory AS inventory
                  WHERE inventory.root_id = ?1
                    AND inventory.work_key = source.work_key
                    AND inventory.status = 'present'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM scan_events AS event
                  WHERE event.root_id = ?1
                    AND event.work_key = source.work_key
                    AND event.event_kind = 'catalog-delete'
                    AND event.status IN ('pending', 'processing', 'failed')
              )
            ORDER BY source.work_key
            LIMIT ?2
        ) AS candidates
        "#,
    )
    .bind(root_id)
    .bind(limit)
    .bind(kind)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    transaction.commit().await?;
    Ok(usize::try_from(inserted).unwrap_or(usize::MAX))
}

async fn enqueue_audio_reconcile_catalog_events(
    db: &Db,
    root_id: i64,
    generation: i64,
    token: &str,
    limit: i64,
) -> Result<usize> {
    let _write_slot = db.acquire_write_slot(256 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    ensure_generation_fence(&mut transaction, root_id, generation, token).await?;
    let limit = limit.clamp(1, NOVEL_COORDINATOR_BATCH_SIZE);
    let candidates = sqlx::query(
        r#"
        WITH candidates AS (
            SELECT inventory.work_key
            FROM file_inventory AS inventory
            LEFT JOIN catalog_work_sources AS source
              ON source.kind = 'audio'
             AND source.root_id = inventory.root_id
             AND source.work_key = inventory.work_key
            LEFT JOIN works ON works.id = source.work_id
            WHERE inventory.root_id = ?1
              AND inventory.status = 'present'
              AND inventory.work_key IS NOT NULL
              AND length(inventory.work_key) > 0
              AND (
                    lower(inventory.relative_path) GLOB '*.mp3'
                 OR lower(inventory.relative_path) GLOB '*.wav'
                 OR lower(inventory.relative_path) GLOB '*.flac'
                 OR lower(inventory.relative_path) GLOB '*.ogg'
                 OR lower(inventory.relative_path) GLOB '*.m4a'
                 OR lower(inventory.relative_path) GLOB '*.aac'
                 OR lower(inventory.relative_path) GLOB '*.opus'
                 OR (lower(inventory.relative_path) GLOB '*.strm' AND EXISTS (
                        SELECT 1 FROM library_roots AS qms_root
                        WHERE qms_root.id = inventory.root_id
                          AND qms_root.provider LIKE 'qmediasync%'
                    ))
              )
              AND (
                    source.work_id IS NULL
                 OR source.seen_generation != ?2
                 OR works.deleted_at IS NOT NULL
              )
              AND NOT EXISTS (
                  SELECT 1 FROM scan_events AS event
                  WHERE event.root_id = ?1
                    AND event.work_key = inventory.work_key
                    AND event.event_kind IN ('catalog-upsert', 'catalog-delete')
                    AND event.status IN ('pending', 'processing', 'failed')
              )
              AND EXISTS (
                  SELECT 1 FROM library_roots AS root
                  WHERE root.id = ?1 AND root.kind = 'audio'
              )
              AND EXISTS (
                  SELECT 1 FROM catalog_kind_ownership
                  WHERE kind = 'audio' AND authoritative_writer = 'catalog-v2'
              )
            GROUP BY inventory.work_key
            ORDER BY inventory.work_key
            LIMIT ?3
        )
        SELECT COUNT(*) AS candidate_count,
               COALESCE(SUM(length(work_key)), 0) AS candidate_bytes
        FROM candidates
        "#,
    )
    .bind(root_id)
    .bind(generation)
    .bind(limit)
    .fetch_one(&mut *transaction)
    .await?;
    let candidate_count = candidates.get::<i64, _>("candidate_count").max(0);
    let candidate_bytes = candidates.get::<i64, _>("candidate_bytes").max(0);
    if candidate_count == 0 {
        transaction.commit().await?;
        return Ok(0);
    }
    if let Some(message) =
        ensure_catalog_queue_capacity(&mut transaction, root_id, candidate_count, candidate_bytes)
            .await?
    {
        transaction.commit().await?;
        return Err(AppError::Other(message));
    }
    let inserted = sqlx::query(
        r#"
        INSERT INTO scan_events (
            root_id, relative_path, event_kind, work_key, observed_at, status
        )
        SELECT
            ?1, candidates.work_key, 'catalog-upsert', candidates.work_key,
            strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'pending'
        FROM (
            SELECT inventory.work_key
            FROM file_inventory AS inventory
            LEFT JOIN catalog_work_sources AS source
              ON source.kind = 'audio'
             AND source.root_id = inventory.root_id
             AND source.work_key = inventory.work_key
            LEFT JOIN works ON works.id = source.work_id
            WHERE inventory.root_id = ?1
              AND inventory.status = 'present'
              AND inventory.work_key IS NOT NULL
              AND length(inventory.work_key) > 0
              AND (
                    lower(inventory.relative_path) GLOB '*.mp3'
                 OR lower(inventory.relative_path) GLOB '*.wav'
                 OR lower(inventory.relative_path) GLOB '*.flac'
                 OR lower(inventory.relative_path) GLOB '*.ogg'
                 OR lower(inventory.relative_path) GLOB '*.m4a'
                 OR lower(inventory.relative_path) GLOB '*.aac'
                 OR lower(inventory.relative_path) GLOB '*.opus'
                 OR (lower(inventory.relative_path) GLOB '*.strm' AND EXISTS (
                        SELECT 1 FROM library_roots AS qms_root
                        WHERE qms_root.id = inventory.root_id
                          AND qms_root.provider LIKE 'qmediasync%'
                    ))
              )
              AND (
                    source.work_id IS NULL
                 OR source.seen_generation != ?2
                 OR works.deleted_at IS NOT NULL
              )
              AND NOT EXISTS (
                  SELECT 1 FROM scan_events AS event
                  WHERE event.root_id = ?1
                    AND event.work_key = inventory.work_key
                    AND event.event_kind IN ('catalog-upsert', 'catalog-delete')
                    AND event.status IN ('pending', 'processing', 'failed')
              )
              AND EXISTS (
                  SELECT 1 FROM library_roots AS root
                  WHERE root.id = ?1 AND root.kind = 'audio'
              )
              AND EXISTS (
                  SELECT 1 FROM catalog_kind_ownership
                  WHERE kind = 'audio' AND authoritative_writer = 'catalog-v2'
              )
            GROUP BY inventory.work_key
            ORDER BY inventory.work_key
            LIMIT ?3
        ) AS candidates
        "#,
    )
    .bind(root_id)
    .bind(generation)
    .bind(limit)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    transaction.commit().await?;
    Ok(usize::try_from(inserted).unwrap_or(usize::MAX))
}

async fn enqueue_missing_audio_catalog_events(
    db: &Db,
    root_id: i64,
    generation: i64,
    token: &str,
    limit: i64,
) -> Result<usize> {
    let _write_slot = db.acquire_write_slot(256 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    ensure_generation_fence(&mut transaction, root_id, generation, token).await?;
    let limit = limit.clamp(1, NOVEL_COORDINATOR_BATCH_SIZE);
    let candidates = sqlx::query(
        r#"
        WITH candidates AS (
            SELECT source.work_key
            FROM catalog_work_sources AS source
            JOIN works ON works.id = source.work_id
            WHERE source.kind = 'audio'
              AND source.root_id = ?1
              AND works.deleted_at IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM file_inventory AS inventory
                  WHERE inventory.root_id = ?1
                    AND inventory.work_key = source.work_key
                    AND inventory.status = 'present'
                    AND (
                          lower(inventory.relative_path) GLOB '*.mp3'
                       OR lower(inventory.relative_path) GLOB '*.wav'
                       OR lower(inventory.relative_path) GLOB '*.flac'
                       OR lower(inventory.relative_path) GLOB '*.ogg'
                       OR lower(inventory.relative_path) GLOB '*.m4a'
                       OR lower(inventory.relative_path) GLOB '*.aac'
                       OR lower(inventory.relative_path) GLOB '*.opus'
                       OR (lower(inventory.relative_path) GLOB '*.strm' AND EXISTS (
                              SELECT 1 FROM library_roots AS qms_root
                              WHERE qms_root.id = inventory.root_id
                                AND qms_root.provider LIKE 'qmediasync%'
                          ))
                    )
              )
              AND NOT EXISTS (
                  SELECT 1 FROM scan_events AS event
                  WHERE event.root_id = ?1
                    AND event.work_key = source.work_key
                    AND event.event_kind IN ('catalog-upsert', 'catalog-delete')
                    AND event.status IN ('pending', 'processing', 'failed')
              )
              AND EXISTS (
                  SELECT 1 FROM catalog_kind_ownership
                  WHERE kind = 'audio' AND authoritative_writer = 'catalog-v2'
              )
            ORDER BY source.work_key
            LIMIT ?2
        )
        SELECT COUNT(*) AS candidate_count,
               COALESCE(SUM(length(work_key)), 0) AS candidate_bytes
        FROM candidates
        "#,
    )
    .bind(root_id)
    .bind(limit)
    .fetch_one(&mut *transaction)
    .await?;
    let candidate_count = candidates.get::<i64, _>("candidate_count").max(0);
    let candidate_bytes = candidates.get::<i64, _>("candidate_bytes").max(0);
    if candidate_count == 0 {
        transaction.commit().await?;
        return Ok(0);
    }
    if let Some(message) =
        ensure_catalog_queue_capacity(&mut transaction, root_id, candidate_count, candidate_bytes)
            .await?
    {
        transaction.commit().await?;
        return Err(AppError::Other(message));
    }
    let inserted = sqlx::query(
        r#"
        INSERT INTO scan_events (
            root_id, relative_path, event_kind, work_key, observed_at, status
        )
        SELECT
            ?1, candidates.work_key, 'catalog-delete', candidates.work_key,
            strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'pending'
        FROM (
            SELECT source.work_key
            FROM catalog_work_sources AS source
            JOIN works ON works.id = source.work_id
            WHERE source.kind = 'audio'
              AND source.root_id = ?1
              AND works.deleted_at IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM file_inventory AS inventory
                  WHERE inventory.root_id = ?1
                    AND inventory.work_key = source.work_key
                    AND inventory.status = 'present'
                    AND (
                          lower(inventory.relative_path) GLOB '*.mp3'
                       OR lower(inventory.relative_path) GLOB '*.wav'
                       OR lower(inventory.relative_path) GLOB '*.flac'
                       OR lower(inventory.relative_path) GLOB '*.ogg'
                       OR lower(inventory.relative_path) GLOB '*.m4a'
                       OR lower(inventory.relative_path) GLOB '*.aac'
                       OR lower(inventory.relative_path) GLOB '*.opus'
                       OR (lower(inventory.relative_path) GLOB '*.strm' AND EXISTS (
                              SELECT 1 FROM library_roots AS qms_root
                              WHERE qms_root.id = inventory.root_id
                                AND qms_root.provider LIKE 'qmediasync%'
                          ))
                    )
              )
              AND NOT EXISTS (
                  SELECT 1 FROM scan_events AS event
                  WHERE event.root_id = ?1
                    AND event.work_key = source.work_key
                    AND event.event_kind IN ('catalog-upsert', 'catalog-delete')
                    AND event.status IN ('pending', 'processing', 'failed')
              )
              AND EXISTS (
                  SELECT 1 FROM catalog_kind_ownership
                  WHERE kind = 'audio' AND authoritative_writer = 'catalog-v2'
              )
            ORDER BY source.work_key
            LIMIT ?2
        ) AS candidates
        "#,
    )
    .bind(root_id)
    .bind(limit)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    transaction.commit().await?;
    Ok(usize::try_from(inserted).unwrap_or(usize::MAX))
}

async fn enqueue_gallery_reconcile_catalog_events(
    db: &Db,
    root_id: i64,
    generation: i64,
    token: &str,
    limit: i64,
) -> Result<usize> {
    let _write_slot = db.acquire_write_slot(256 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    ensure_generation_fence(&mut transaction, root_id, generation, token).await?;
    let limit = limit.clamp(1, NOVEL_COORDINATOR_BATCH_SIZE);
    let candidates = sqlx::query(
        r#"
        WITH candidates AS (
            SELECT inventory.work_key
            FROM file_inventory AS inventory
            LEFT JOIN catalog_work_sources AS source
              ON source.kind = 'gallery'
             AND source.root_id = inventory.root_id
             AND source.work_key = inventory.work_key
            LEFT JOIN works ON works.id = source.work_id
            WHERE inventory.root_id = ?1
              AND inventory.status = 'present'
              AND inventory.work_key IS NOT NULL
              AND length(inventory.work_key) > 0
              AND (
                    lower(inventory.relative_path) GLOB '*.jpg'
                 OR lower(inventory.relative_path) GLOB '*.jpeg'
                 OR lower(inventory.relative_path) GLOB '*.png'
                 OR lower(inventory.relative_path) GLOB '*.webp'
                 OR lower(inventory.relative_path) GLOB '*.gif'
                 OR lower(inventory.relative_path) GLOB '*.avif'
                 OR lower(inventory.relative_path) GLOB '*.bmp'
              )
              AND (
                    source.work_id IS NULL
                 OR source.seen_generation != ?2
                 OR works.deleted_at IS NOT NULL
              )
              AND NOT EXISTS (
                  SELECT 1 FROM scan_events AS event
                  WHERE event.root_id = ?1
                    AND event.work_key = inventory.work_key
                    AND event.event_kind IN ('catalog-upsert', 'catalog-delete')
                    AND event.status IN ('pending', 'processing', 'failed')
              )
              AND EXISTS (
                  SELECT 1 FROM library_roots AS root
                  WHERE root.id = ?1 AND root.kind = 'gallery'
              )
              AND EXISTS (
                  SELECT 1 FROM catalog_kind_ownership
                  WHERE kind = 'gallery' AND authoritative_writer = 'catalog-v2'
              )
            GROUP BY inventory.work_key
            ORDER BY inventory.work_key
            LIMIT ?3
        )
        SELECT COUNT(*) AS candidate_count,
               COALESCE(SUM(length(work_key)), 0) AS candidate_bytes
        FROM candidates
        "#,
    )
    .bind(root_id)
    .bind(generation)
    .bind(limit)
    .fetch_one(&mut *transaction)
    .await?;
    let candidate_count = candidates.get::<i64, _>("candidate_count").max(0);
    let candidate_bytes = candidates.get::<i64, _>("candidate_bytes").max(0);
    if candidate_count == 0 {
        transaction.commit().await?;
        return Ok(0);
    }
    if let Some(message) =
        ensure_catalog_queue_capacity(&mut transaction, root_id, candidate_count, candidate_bytes)
            .await?
    {
        transaction.commit().await?;
        return Err(AppError::Other(message));
    }
    let inserted = sqlx::query(
        r#"
        INSERT INTO scan_events (
            root_id, relative_path, event_kind, work_key, observed_at, status
        )
        SELECT
            ?1, candidates.work_key, 'catalog-upsert', candidates.work_key,
            strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'pending'
        FROM (
            SELECT inventory.work_key
            FROM file_inventory AS inventory
            LEFT JOIN catalog_work_sources AS source
              ON source.kind = 'gallery'
             AND source.root_id = inventory.root_id
             AND source.work_key = inventory.work_key
            LEFT JOIN works ON works.id = source.work_id
            WHERE inventory.root_id = ?1
              AND inventory.status = 'present'
              AND inventory.work_key IS NOT NULL
              AND length(inventory.work_key) > 0
              AND (
                    lower(inventory.relative_path) GLOB '*.jpg'
                 OR lower(inventory.relative_path) GLOB '*.jpeg'
                 OR lower(inventory.relative_path) GLOB '*.png'
                 OR lower(inventory.relative_path) GLOB '*.webp'
                 OR lower(inventory.relative_path) GLOB '*.gif'
                 OR lower(inventory.relative_path) GLOB '*.avif'
                 OR lower(inventory.relative_path) GLOB '*.bmp'
              )
              AND (
                    source.work_id IS NULL
                 OR source.seen_generation != ?2
                 OR works.deleted_at IS NOT NULL
              )
              AND NOT EXISTS (
                  SELECT 1 FROM scan_events AS event
                  WHERE event.root_id = ?1
                    AND event.work_key = inventory.work_key
                    AND event.event_kind IN ('catalog-upsert', 'catalog-delete')
                    AND event.status IN ('pending', 'processing', 'failed')
              )
              AND EXISTS (
                  SELECT 1 FROM library_roots AS root
                  WHERE root.id = ?1 AND root.kind = 'gallery'
              )
              AND EXISTS (
                  SELECT 1 FROM catalog_kind_ownership
                  WHERE kind = 'gallery' AND authoritative_writer = 'catalog-v2'
              )
            GROUP BY inventory.work_key
            ORDER BY inventory.work_key
            LIMIT ?3
        ) AS candidates
        "#,
    )
    .bind(root_id)
    .bind(generation)
    .bind(limit)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    transaction.commit().await?;
    Ok(usize::try_from(inserted).unwrap_or(usize::MAX))
}

async fn enqueue_missing_gallery_catalog_events(
    db: &Db,
    root_id: i64,
    generation: i64,
    token: &str,
    limit: i64,
) -> Result<usize> {
    let _write_slot = db.acquire_write_slot(256 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    ensure_generation_fence(&mut transaction, root_id, generation, token).await?;
    let limit = limit.clamp(1, NOVEL_COORDINATOR_BATCH_SIZE);
    let candidates = sqlx::query(
        r#"
        WITH candidates AS (
            SELECT source.work_key
            FROM catalog_work_sources AS source
            JOIN works ON works.id = source.work_id
            WHERE source.kind = 'gallery'
              AND source.root_id = ?1
              AND works.deleted_at IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM file_inventory AS inventory
                  WHERE inventory.root_id = ?1
                    AND inventory.work_key = source.work_key
                    AND inventory.status = 'present'
                    AND (
                          lower(inventory.relative_path) GLOB '*.jpg'
                       OR lower(inventory.relative_path) GLOB '*.jpeg'
                       OR lower(inventory.relative_path) GLOB '*.png'
                       OR lower(inventory.relative_path) GLOB '*.webp'
                       OR lower(inventory.relative_path) GLOB '*.gif'
                       OR lower(inventory.relative_path) GLOB '*.avif'
                       OR lower(inventory.relative_path) GLOB '*.bmp'
                    )
              )
              AND NOT EXISTS (
                  SELECT 1 FROM scan_events AS event
                  WHERE event.root_id = ?1
                    AND event.work_key = source.work_key
                    AND event.event_kind IN ('catalog-upsert', 'catalog-delete')
                    AND event.status IN ('pending', 'processing', 'failed')
              )
              AND EXISTS (
                  SELECT 1 FROM catalog_kind_ownership
                  WHERE kind = 'gallery' AND authoritative_writer = 'catalog-v2'
              )
            ORDER BY source.work_key
            LIMIT ?2
        )
        SELECT COUNT(*) AS candidate_count,
               COALESCE(SUM(length(work_key)), 0) AS candidate_bytes
        FROM candidates
        "#,
    )
    .bind(root_id)
    .bind(limit)
    .fetch_one(&mut *transaction)
    .await?;
    let candidate_count = candidates.get::<i64, _>("candidate_count").max(0);
    let candidate_bytes = candidates.get::<i64, _>("candidate_bytes").max(0);
    if candidate_count == 0 {
        transaction.commit().await?;
        return Ok(0);
    }
    if let Some(message) =
        ensure_catalog_queue_capacity(&mut transaction, root_id, candidate_count, candidate_bytes)
            .await?
    {
        transaction.commit().await?;
        return Err(AppError::Other(message));
    }
    let inserted = sqlx::query(
        r#"
        INSERT INTO scan_events (
            root_id, relative_path, event_kind, work_key, observed_at, status
        )
        SELECT
            ?1, candidates.work_key, 'catalog-delete', candidates.work_key,
            strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'pending'
        FROM (
            SELECT source.work_key
            FROM catalog_work_sources AS source
            JOIN works ON works.id = source.work_id
            WHERE source.kind = 'gallery'
              AND source.root_id = ?1
              AND works.deleted_at IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM file_inventory AS inventory
                  WHERE inventory.root_id = ?1
                    AND inventory.work_key = source.work_key
                    AND inventory.status = 'present'
                    AND (
                          lower(inventory.relative_path) GLOB '*.jpg'
                       OR lower(inventory.relative_path) GLOB '*.jpeg'
                       OR lower(inventory.relative_path) GLOB '*.png'
                       OR lower(inventory.relative_path) GLOB '*.webp'
                       OR lower(inventory.relative_path) GLOB '*.gif'
                       OR lower(inventory.relative_path) GLOB '*.avif'
                       OR lower(inventory.relative_path) GLOB '*.bmp'
                    )
              )
              AND NOT EXISTS (
                  SELECT 1 FROM scan_events AS event
                  WHERE event.root_id = ?1
                    AND event.work_key = source.work_key
                    AND event.event_kind IN ('catalog-upsert', 'catalog-delete')
                    AND event.status IN ('pending', 'processing', 'failed')
              )
              AND EXISTS (
                  SELECT 1 FROM catalog_kind_ownership
                  WHERE kind = 'gallery' AND authoritative_writer = 'catalog-v2'
              )
            ORDER BY source.work_key
            LIMIT ?2
        ) AS candidates
        "#,
    )
    .bind(root_id)
    .bind(limit)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    transaction.commit().await?;
    Ok(usize::try_from(inserted).unwrap_or(usize::MAX))
}

async fn update_novel_coordinator_state(
    db: &Db,
    root_id: i64,
    generation: i64,
    phase: &str,
    checkpoint_seq: Option<i64>,
    error: Option<&str>,
) -> Result<()> {
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    update_novel_coordinator_state_in_transaction(
        &mut transaction,
        root_id,
        generation,
        phase,
        checkpoint_seq,
        error,
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

/// Update coordinator state while the caller already owns the SQLite writer
/// slot.  This is used by lease release so the root fence and coordinator
/// phase cannot interleave with another inventory writer.
async fn update_novel_coordinator_state_unlocked(
    db: &Db,
    root_id: i64,
    generation: i64,
    phase: &str,
    checkpoint_seq: Option<i64>,
    error: Option<&str>,
) -> Result<()> {
    let mut transaction = db.begin_tracked_transaction().await?;
    update_novel_coordinator_state_in_transaction(
        &mut transaction,
        root_id,
        generation,
        phase,
        checkpoint_seq,
        error,
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

async fn update_novel_coordinator_state_in_transaction(
    transaction: &mut sqlx_core::transaction::Transaction<'_, crate::Sqlite>,
    root_id: i64,
    generation: i64,
    phase: &str,
    checkpoint_seq: Option<i64>,
    error: Option<&str>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO novel_coordinator_state (
            root_id, generation, phase, checkpoint_seq,
            pending_keys, failed_keys, last_error, updated_at
        )
        VALUES (
            ?1, ?2, ?3,
            COALESCE(?4, 0),
            (SELECT COUNT(*) FROM scan_events
             WHERE root_id = ?1 AND status IN ('pending', 'processing')
               AND event_kind IN ('catalog-upsert', 'catalog-delete')),
            (SELECT COUNT(*) FROM scan_events AS failed
             WHERE failed.root_id = ?1 AND failed.status = 'failed'
               AND failed.event_kind IN ('catalog-upsert', 'catalog-delete')
               AND failed.work_key IS NOT NULL
               AND NOT EXISTS (
                   SELECT 1 FROM scan_events AS newer
                   WHERE newer.root_id = failed.root_id
                     AND newer.work_key = failed.work_key
                     AND newer.event_kind IN ('catalog-upsert', 'catalog-delete')
                     AND newer.seq > failed.seq
               )),
            ?5, strftime('%Y-%m-%dT%H:%M:%fZ','now')
        )
        ON CONFLICT(root_id) DO UPDATE SET
            generation = excluded.generation,
            phase = excluded.phase,
            checkpoint_seq = CASE
                WHEN excluded.generation != novel_coordinator_state.generation
                THEN excluded.checkpoint_seq
                WHEN excluded.checkpoint_seq > novel_coordinator_state.checkpoint_seq
                THEN excluded.checkpoint_seq
                ELSE novel_coordinator_state.checkpoint_seq
            END,
            pending_keys = excluded.pending_keys,
            failed_keys = excluded.failed_keys,
            last_error = excluded.last_error,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(root_id)
    .bind(generation)
    .bind(phase)
    .bind(checkpoint_seq)
    .bind(error.map(bounded_error))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NovelCatalogOutcome {
    Applied,
    Preserved,
}

#[derive(Debug, Clone)]
struct NovelCoordinatorLease {
    root_id: i64,
    generation: i64,
    token: String,
    root: RootSpec,
}

async fn claim_novel_catalog_events_under_fence(
    db: &Db,
    root_id: i64,
    generation: i64,
    token: &str,
    limit: i64,
) -> Result<Vec<PendingEvent>> {
    let _write_slot = db.acquire_write_slot(128 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    let rows = sqlx::query(
        r#"
        SELECT event.seq, event.root_id, event.relative_path, event.event_kind,
               event.work_key, event.attempts,
               root.kind, root.provider, root.root, root.scan_depth,
               root.device_class, root.audio_grouping, root.generation
        FROM scan_events AS event
        JOIN library_roots AS root ON root.id = event.root_id
        WHERE event.status = 'pending'
          AND event.event_kind IN ('catalog-upsert', 'catalog-delete')
          AND root.id = ?1
          AND root.generation = ?2
          AND root.active_token = ?3
          AND root.enabled = 1
          AND root.status = 'scanning'
          AND EXISTS (
              SELECT 1
              FROM catalog_kind_ownership
              WHERE kind = root.kind AND authoritative_writer = 'catalog-v2'
          )
        ORDER BY event.seq
        LIMIT ?4
        "#,
    )
    .bind(root_id)
    .bind(generation)
    .bind(token)
    .bind(limit.clamp(1, NOVEL_COORDINATOR_BATCH_SIZE))
    .fetch_all(&mut *transaction)
    .await?;
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let seq: i64 = row.get("seq");
        let claimed = sqlx::query(
            r#"
            UPDATE scan_events
            SET status = 'processing', attempts = attempts + 1, last_error = NULL
            WHERE seq = ?1 AND status = 'pending'
            "#,
        )
        .bind(seq)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if claimed != 1 {
            continue;
        }
        let root_text: String = row.get("root");
        events.push(PendingEvent {
            seq,
            root_id: row.get("root_id"),
            relative_path: row.get("relative_path"),
            event_kind: row.get("event_kind"),
            work_key: row.get::<Option<String>, _>("work_key").unwrap_or_default(),
            attempts: row.get::<i64, _>("attempts") + 1,
            generation: row.get("generation"),
            root: RootSpec {
                kind: row.get("kind"),
                provider: row.get("provider"),
                root: PathBuf::from(&root_text),
                root_text,
                scan_depth: row
                    .get::<Option<i64>, _>("scan_depth")
                    .and_then(|value| usize::try_from(value).ok()),
                device_class: row.get("device_class"),
                audio_grouping: parse_audio_grouping(row.get("audio_grouping")),
            },
        });
    }
    transaction.commit().await?;
    if !events.is_empty() {
        update_novel_coordinator_state_unlocked(db, root_id, generation, "processing", None, None)
            .await?;
    }
    Ok(events)
}

fn relative_work_key_is_safe(work_key: &str) -> bool {
    let path = Path::new(work_key);
    !work_key.trim().is_empty()
        && !path.is_absolute()
        && !path.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
}

async fn previous_novel_work_key(db: &Db, event: &PendingEvent) -> Result<Option<String>> {
    let file_id = sqlx::query_scalar::<_, Option<String>>(
        r#"
        SELECT file_id
        FROM file_inventory
        WHERE root_id = ?1 AND work_key = ?2 AND status = 'present'
        ORDER BY relative_path
        LIMIT 1
        "#,
    )
    .bind(event.root_id)
    .bind(&event.work_key)
    .fetch_optional(db.pool())
    .await?
    .flatten();
    let Some(file_id) = file_id.filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let keys = sqlx::query_scalar::<_, String>(
        r#"
        SELECT DISTINCT inventory.work_key
        FROM file_inventory AS inventory
        JOIN catalog_work_sources AS source
          ON source.kind = 'novel'
         AND source.root_id = inventory.root_id
         AND source.work_key = inventory.work_key
        WHERE inventory.root_id = ?1
          AND inventory.file_id = ?2
          AND inventory.work_key IS NOT ?3
          AND lower(inventory.work_key) GLOB '*.epub'
          AND inventory.status IN ('present', 'missing')
          AND (inventory.status = 'missing' OR inventory.seen_generation != ?4)
        ORDER BY inventory.work_key
        LIMIT 2
        "#,
    )
    .bind(event.root_id)
    .bind(file_id)
    .bind(&event.work_key)
    .bind(event.generation)
    .fetch_all(db.pool())
    .await?;
    if keys.len() == 1 {
        Ok(keys.into_iter().next())
    } else {
        // Multiple hard links/duplicate identities are intentionally not
        // merged. A later complete reconcile can tombstone stale keys safely.
        Ok(None)
    }
}

fn archive_work_key_is_safe(kind: &str, work_key: &str, provider: &str) -> bool {
    let path = Path::new(work_key);
    !work_key.trim().is_empty()
        && !path.is_absolute()
        && !path.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
        && path
            .extension()
            .and_then(|value| value.to_str())
            .is_some_and(|value| match kind {
                "comic" => {
                    value.eq_ignore_ascii_case("cbz")
                        || value.eq_ignore_ascii_case("zip")
                        || (vfs::is_qmediasync_provider(provider)
                            && value.eq_ignore_ascii_case("strm"))
                }
                "coser-picture" => {
                    value.eq_ignore_ascii_case("zip")
                        || (vfs::is_qmediasync_provider(provider)
                            && value.eq_ignore_ascii_case("strm"))
                }
                _ => false,
            })
}

async fn previous_archive_work_key(
    db: &Db,
    event: &PendingEvent,
    kind: &str,
) -> Result<Option<String>> {
    if !matches!(kind, "comic" | "coser-picture") || event.root.kind != kind {
        return Ok(None);
    }
    let file_id = sqlx::query_scalar::<_, Option<String>>(
        r#"
        SELECT file_id
        FROM file_inventory
        WHERE root_id = ?1 AND work_key = ?2 AND status = 'present'
        ORDER BY relative_path
        LIMIT 1
        "#,
    )
    .bind(event.root_id)
    .bind(&event.work_key)
    .fetch_optional(db.pool())
    .await?
    .flatten();
    let Some(file_id) = file_id.filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let keys = sqlx::query_scalar::<_, String>(
        r#"
        SELECT DISTINCT inventory.work_key
        FROM file_inventory AS inventory
        JOIN catalog_work_sources AS source
          ON source.kind = ?4
         AND source.root_id = inventory.root_id
         AND source.work_key = inventory.work_key
        WHERE inventory.root_id = ?1
          AND inventory.file_id = ?2
          AND inventory.work_key IS NOT ?3
          AND (
                (?4 = 'comic' AND (
                    lower(inventory.work_key) GLOB '*.cbz'
                    OR lower(inventory.work_key) GLOB '*.zip'
                ))
                OR (?4 = 'coser-picture' AND lower(inventory.work_key) GLOB '*.zip')
              )
          AND inventory.status IN ('present', 'missing')
          AND (inventory.status = 'missing' OR inventory.seen_generation != ?5)
        ORDER BY inventory.work_key
        LIMIT 2
        "#,
    )
    .bind(event.root_id)
    .bind(file_id)
    .bind(&event.work_key)
    .bind(kind)
    .bind(event.generation)
    .fetch_all(db.pool())
    .await?;
    if keys.len() == 1 {
        Ok(keys.into_iter().next())
    } else {
        Ok(None)
    }
}

fn gallery_work_key_is_safe(work_key: &str) -> bool {
    relative_work_key_is_safe(work_key)
}

async fn load_gallery_catalog_assets(
    db: &Db,
    root_id: i64,
    work_key: &str,
    status: &str,
) -> Result<Vec<GalleryCatalogAsset>> {
    let mut rows = sqlx::query(
        r#"
        SELECT relative_path, size, fast_fingerprint, file_id
        FROM file_inventory
        WHERE root_id = ?1 AND work_key = ?2 AND status = ?3
          AND (
                lower(relative_path) GLOB '*.jpg'
             OR lower(relative_path) GLOB '*.jpeg'
             OR lower(relative_path) GLOB '*.png'
             OR lower(relative_path) GLOB '*.webp'
             OR lower(relative_path) GLOB '*.gif'
             OR lower(relative_path) GLOB '*.avif'
             OR lower(relative_path) GLOB '*.bmp'
          )
        ORDER BY relative_path
        LIMIT ?4
        "#,
    )
    .bind(root_id)
    .bind(work_key)
    .bind(status)
    .bind(i64::try_from(gallery_inspector::MAX_GALLERY_FILES_PER_WORK + 1).unwrap_or(i64::MAX))
    .fetch(db.pool());
    let mut assets = Vec::with_capacity(256);
    while let Some(row) = rows.try_next().await? {
        if assets.len() >= gallery_inspector::MAX_GALLERY_FILES_PER_WORK {
            return Err(AppError::Other(format!(
                "gallery work {work_key} exceeds the {} file safety limit",
                gallery_inspector::MAX_GALLERY_FILES_PER_WORK
            )));
        }
        let relative_path = row.get::<String, _>("relative_path");
        let parent_key = Path::new(&relative_path)
            .parent()
            .map(portable_path)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| ".".to_string());
        if parent_key != work_key {
            return Err(AppError::Other(format!(
                "gallery inventory path {relative_path} does not belong to work key {work_key}"
            )));
        }
        let size = row.get::<i64, _>("size");
        let source_version = row.get::<String, _>("fast_fingerprint");
        if size < 0 || source_version.trim().is_empty() {
            return Err(AppError::Other(format!(
                "gallery inventory metadata is incomplete for {relative_path}"
            )));
        }
        assets.push(GalleryCatalogAsset {
            relative_path,
            size,
            source_version,
            file_id: row
                .get::<Option<String>, _>("file_id")
                .filter(|value| !value.is_empty()),
        });
    }
    Ok(assets)
}

async fn load_audio_catalog_paths(db: &Db, root_id: i64, work_key: &str) -> Result<Vec<String>> {
    let mut rows = sqlx::query_scalar::<_, String>(
        r#"
        SELECT relative_path
        FROM file_inventory
        WHERE root_id = ?1 AND work_key = ?2 AND status = 'present'
          AND (
                lower(relative_path) GLOB '*.mp3'
             OR lower(relative_path) GLOB '*.wav'
             OR lower(relative_path) GLOB '*.flac'
             OR lower(relative_path) GLOB '*.ogg'
             OR lower(relative_path) GLOB '*.m4a'
             OR lower(relative_path) GLOB '*.aac'
             OR lower(relative_path) GLOB '*.opus'
             OR (lower(relative_path) GLOB '*.strm' AND EXISTS (
                    SELECT 1 FROM library_roots AS qms_root
                    WHERE qms_root.id = file_inventory.root_id
                      AND qms_root.provider LIKE 'qmediasync%'
                ))
             OR lower(relative_path) GLOB '*.jpg'
             OR lower(relative_path) GLOB '*.jpeg'
             OR lower(relative_path) GLOB '*.png'
             OR lower(relative_path) GLOB '*.webp'
             OR lower(relative_path) GLOB '*.txt'
          )
        ORDER BY relative_path
        LIMIT ?3
        "#,
    )
    .bind(root_id)
    .bind(work_key)
    .bind(i64::try_from(audio_inspector::MAX_AUDIO_FILES_PER_WORK + 1).unwrap_or(i64::MAX))
    .fetch(db.pool());
    let mut paths = Vec::with_capacity(256);
    let mut path_bytes = 0_usize;
    while let Some(relative_path) = rows.try_next().await? {
        if paths.len() >= audio_inspector::MAX_AUDIO_FILES_PER_WORK {
            return Err(AppError::Other(format!(
                "audio work {work_key} exceeds the {} file safety limit",
                audio_inspector::MAX_AUDIO_FILES_PER_WORK
            )));
        }
        path_bytes = path_bytes.saturating_add(relative_path.len());
        if path_bytes > audio_inspector::MAX_AUDIO_PATH_BYTES {
            return Err(AppError::Other(format!(
                "audio work {work_key} exceeds the {} byte path budget",
                audio_inspector::MAX_AUDIO_PATH_BYTES
            )));
        }
        paths.push(relative_path);
    }
    Ok(paths)
}

fn gallery_identity_signature(
    work_key: &str,
    assets: &[GalleryCatalogAsset],
) -> Option<GalleryIdentitySignature> {
    let mut signature = Vec::with_capacity(assets.len());
    for asset in assets {
        let relative = Path::new(&asset.relative_path);
        let parent_key = relative
            .parent()
            .map(portable_path)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| ".".to_string());
        if parent_key != work_key {
            return None;
        }
        let file_name = relative.file_name()?.to_string_lossy().to_string();
        signature.push((
            file_name,
            asset.size,
            asset.source_version.clone(),
            asset.file_id.clone(),
        ));
    }
    signature.sort();
    Some(signature)
}

async fn previous_gallery_work_key(
    db: &Db,
    event: &PendingEvent,
    current_assets: &[GalleryCatalogAsset],
) -> Result<Option<String>> {
    if current_assets.is_empty() {
        return Ok(None);
    }
    let strong_identity = current_assets.iter().all(|asset| asset.file_id.is_some());
    if current_assets.len() == 1 && !strong_identity {
        return Ok(None);
    }
    let Some(current_signature) = gallery_identity_signature(&event.work_key, current_assets)
    else {
        return Ok(None);
    };
    let anchor = &current_assets[0];
    let candidates = sqlx::query_scalar::<_, String>(
        r#"
        SELECT DISTINCT inventory.work_key
        FROM file_inventory AS inventory
        JOIN catalog_work_sources AS source
          ON source.kind = 'gallery'
         AND source.root_id = inventory.root_id
         AND source.work_key = inventory.work_key
        WHERE inventory.root_id = ?1
          AND inventory.work_key IS NOT ?2
          AND inventory.status = 'missing'
          AND inventory.fast_fingerprint = ?3
          AND inventory.size = ?4
          AND (?5 IS NULL OR inventory.file_id = ?5)
          AND (
                lower(inventory.relative_path) GLOB '*.jpg'
             OR lower(inventory.relative_path) GLOB '*.jpeg'
             OR lower(inventory.relative_path) GLOB '*.png'
             OR lower(inventory.relative_path) GLOB '*.webp'
             OR lower(inventory.relative_path) GLOB '*.gif'
             OR lower(inventory.relative_path) GLOB '*.avif'
             OR lower(inventory.relative_path) GLOB '*.bmp'
          )
        ORDER BY inventory.work_key
        LIMIT 17
        "#,
    )
    .bind(event.root_id)
    .bind(&event.work_key)
    .bind(&anchor.source_version)
    .bind(anchor.size)
    .bind(
        strong_identity
            .then_some(anchor.file_id.as_deref())
            .flatten(),
    )
    .fetch_all(db.pool())
    .await?;
    if candidates.len() >= 17 {
        return Ok(None);
    }
    let mut matched = None;
    for candidate in candidates {
        if !gallery_work_key_is_safe(&candidate) {
            continue;
        }
        let previous_assets =
            load_gallery_catalog_assets(db, event.root_id, &candidate, "missing").await?;
        if gallery_identity_signature(&candidate, &previous_assets).as_ref()
            != Some(&current_signature)
        {
            continue;
        }
        if matched.is_some() {
            return Ok(None);
        }
        matched = Some(candidate);
    }
    Ok(matched)
}

async fn apply_novel_catalog_event(
    db: &Db,
    resources: &ResourceGovernor,
    event: &PendingEvent,
    generated_dir: &Path,
    fence_token: &str,
) -> Result<std::result::Result<NovelCatalogOutcome, String>> {
    if event.root.kind != "novel" || !relative_work_key_is_safe(&event.work_key) {
        return Ok(Ok(NovelCatalogOutcome::Preserved));
    }
    if !catalog_kind_is_v2(db, "novel").await? {
        return Ok(Ok(NovelCatalogOutcome::Preserved));
    }
    let fence = MutationFence {
        root_generation: event.generation,
        scan_token: fence_token.to_string(),
        complete_snapshot: true,
    };
    let source = MutationSource {
        kind: "novel".to_string(),
        root_id: event.root_id,
        work_key: event.work_key.clone(),
        provider: event.root.provider.clone(),
    };
    if event.event_kind == CATALOG_DELETE_EVENT {
        if !event.work_key.to_ascii_lowercase().ends_with(".epub") {
            return Ok(Ok(NovelCatalogOutcome::Preserved));
        }
        db.apply_catalog_work_tombstone(WorkTombstone {
            source,
            fence,
            reason: Some("filesystem-remove-or-complete-reconcile".to_string()),
        })
        .await?;
        return Ok(Ok(NovelCatalogOutcome::Applied));
    }
    if !event.work_key.to_ascii_lowercase().ends_with(".epub") {
        return Ok(Ok(NovelCatalogOutcome::Preserved));
    }
    let present = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT 1 FROM file_inventory
        WHERE root_id = ?1 AND work_key = ?2 AND status = 'present'
          AND lower(relative_path) GLOB '*.epub'
        LIMIT 1
        "#,
    )
    .bind(event.root_id)
    .bind(&event.work_key)
    .fetch_optional(db.pool())
    .await?
    .is_some();
    if !present {
        return Ok(Err(
            "novel source was not confirmed present; preserving the previous work".to_string(),
        ));
    }
    let previous_work_key = previous_novel_work_key(db, event).await?;
    let request = NovelInspectionRequest {
        root_id: event.root_id,
        root_generation: event.generation,
        scan_token: fence_token.to_string(),
        provider: event.root.provider.clone(),
        root: event.root.root.clone(),
        work_key: event.work_key.clone(),
        previous_work_key,
        generated_dir: generated_dir.to_path_buf(),
    };
    let mutation = match novel::inspect(resources, request).await {
        Ok(mutation) => mutation,
        Err(AppError::Overloaded { .. }) => {
            return Err(AppError::Other(
                "novel inspector admission was overloaded; retrying durable event".to_string(),
            ))
        }
        Err(error) => return Ok(Err(bounded_error(&error.to_string()))),
    };
    db.apply_catalog_work_mutation(mutation).await?;
    Ok(Ok(NovelCatalogOutcome::Applied))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArchiveCatalogOutcome {
    Applied,
    Preserved,
}

async fn apply_comic_catalog_event(
    db: &Db,
    resources: &ResourceGovernor,
    event: &PendingEvent,
    fence_token: &str,
) -> Result<std::result::Result<ArchiveCatalogOutcome, String>> {
    if event.root.kind != "comic"
        || !archive_work_key_is_safe("comic", &event.work_key, &event.root.provider)
    {
        return Ok(Ok(ArchiveCatalogOutcome::Preserved));
    }
    if !catalog_kind_is_v2(db, "comic").await? {
        return Ok(Ok(ArchiveCatalogOutcome::Preserved));
    }
    let fence = MutationFence {
        root_generation: event.generation,
        scan_token: fence_token.to_string(),
        complete_snapshot: true,
    };
    let source = MutationSource {
        kind: "comic".to_string(),
        root_id: event.root_id,
        work_key: event.work_key.clone(),
        provider: event.root.provider.clone(),
    };
    if event.event_kind == CATALOG_DELETE_EVENT {
        db.apply_catalog_work_tombstone(WorkTombstone {
            source,
            fence,
            reason: Some("filesystem-remove-or-complete-reconcile".to_string()),
        })
        .await?;
        return Ok(Ok(ArchiveCatalogOutcome::Applied));
    }
    let present = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT 1 FROM file_inventory
        WHERE root_id = ?1 AND work_key = ?2 AND status = 'present'
          AND (lower(relative_path) GLOB '*.cbz'
               OR lower(relative_path) GLOB '*.zip'
               OR lower(relative_path) GLOB '*.strm')
        LIMIT 1
        "#,
    )
    .bind(event.root_id)
    .bind(&event.work_key)
    .fetch_optional(db.pool())
    .await?
    .is_some();
    if !present {
        return Ok(Err(
            "comic source was not confirmed present; preserving the previous work".to_string(),
        ));
    }
    let request = ComicInspectionRequest {
        root_id: event.root_id,
        root_generation: event.generation,
        scan_token: fence_token.to_string(),
        provider: event.root.provider.clone(),
        qmediasync_mount_name: vfs::qmediasync_mount_name(&event.root.provider).map(str::to_string),
        root: event.root.root.clone(),
        work_key: event.work_key.clone(),
        previous_work_key: previous_archive_work_key(db, event, "comic").await?,
    };
    let mutation = match comic_inspector::inspect(resources, request).await {
        Ok(mutation) => mutation,
        Err(AppError::Overloaded { .. }) => {
            return Err(AppError::Other(
                "comic inspector admission was overloaded; retrying durable event".to_string(),
            ))
        }
        Err(error) => return Ok(Err(bounded_error(&error.to_string()))),
    };
    db.apply_catalog_work_mutation(mutation).await?;
    Ok(Ok(ArchiveCatalogOutcome::Applied))
}

async fn apply_coser_picture_catalog_event(
    db: &Db,
    resources: &ResourceGovernor,
    event: &PendingEvent,
    fence_token: &str,
) -> Result<std::result::Result<ArchiveCatalogOutcome, String>> {
    if event.root.kind != "coser-picture"
        || !archive_work_key_is_safe("coser-picture", &event.work_key, &event.root.provider)
    {
        return Ok(Ok(ArchiveCatalogOutcome::Preserved));
    }
    if !catalog_kind_is_v2(db, "coser-picture").await? {
        return Ok(Ok(ArchiveCatalogOutcome::Preserved));
    }
    let fence = MutationFence {
        root_generation: event.generation,
        scan_token: fence_token.to_string(),
        complete_snapshot: true,
    };
    let source = MutationSource {
        kind: "coser-picture".to_string(),
        root_id: event.root_id,
        work_key: event.work_key.clone(),
        provider: event.root.provider.clone(),
    };
    if event.event_kind == CATALOG_DELETE_EVENT {
        db.apply_catalog_work_tombstone(WorkTombstone {
            source,
            fence,
            reason: Some("filesystem-remove-or-complete-reconcile".to_string()),
        })
        .await?;
        return Ok(Ok(ArchiveCatalogOutcome::Applied));
    }
    let present = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT 1 FROM file_inventory
        WHERE root_id = ?1 AND work_key = ?2 AND status = 'present'
          AND (lower(relative_path) GLOB '*.zip' OR lower(relative_path) GLOB '*.strm')
        LIMIT 1
        "#,
    )
    .bind(event.root_id)
    .bind(&event.work_key)
    .fetch_optional(db.pool())
    .await?
    .is_some();
    if !present {
        return Ok(Err(
            "CoserPicture source was not confirmed present; preserving the previous work"
                .to_string(),
        ));
    }
    let request = CoserPictureInspectionRequest {
        root_id: event.root_id,
        root_generation: event.generation,
        scan_token: fence_token.to_string(),
        provider: event.root.provider.clone(),
        qmediasync_mount_name: vfs::qmediasync_mount_name(&event.root.provider).map(str::to_string),
        root: event.root.root.clone(),
        work_key: event.work_key.clone(),
        previous_work_key: previous_archive_work_key(db, event, "coser-picture").await?,
    };
    let mutation = match coser_picture_inspector::inspect(resources, request).await {
        Ok(mutation) => mutation,
        Err(AppError::Overloaded { .. }) => {
            return Err(AppError::Other(
                "CoserPicture inspector admission was overloaded; retrying durable event"
                    .to_string(),
            ))
        }
        Err(error) => return Ok(Err(bounded_error(&error.to_string()))),
    };
    db.apply_catalog_work_mutation(mutation).await?;
    Ok(Ok(ArchiveCatalogOutcome::Applied))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AudioCatalogOutcome {
    Applied,
    Preserved,
}

async fn apply_audio_catalog_event(
    db: &Db,
    resources: &ResourceGovernor,
    event: &PendingEvent,
    fence_token: &str,
) -> Result<std::result::Result<AudioCatalogOutcome, String>> {
    if event.root.kind != "audio" || !relative_work_key_is_safe(&event.work_key) {
        return Ok(Ok(AudioCatalogOutcome::Preserved));
    }
    if !catalog_kind_is_v2(db, "audio").await? {
        return Ok(Ok(AudioCatalogOutcome::Preserved));
    }
    let fence = MutationFence {
        root_generation: event.generation,
        scan_token: fence_token.to_string(),
        complete_snapshot: true,
    };
    let source = MutationSource {
        kind: "audio".to_string(),
        root_id: event.root_id,
        work_key: event.work_key.clone(),
        provider: event.root.provider.clone(),
    };
    if event.event_kind == CATALOG_DELETE_EVENT {
        db.apply_catalog_work_tombstone(WorkTombstone {
            source,
            fence,
            reason: Some("filesystem-remove-or-complete-reconcile".to_string()),
        })
        .await?;
        return Ok(Ok(AudioCatalogOutcome::Applied));
    }
    let relative_paths = match load_audio_catalog_paths(db, event.root_id, &event.work_key).await {
        Ok(paths) => paths,
        Err(AppError::Other(error)) => return Ok(Err(bounded_error(&error))),
        Err(error) => return Err(error),
    };
    let qmediasync_audio = vfs::is_qmediasync_provider(&event.root.provider);
    let has_track = relative_paths.iter().any(|relative_path| {
        Path::new(relative_path)
            .extension()
            .and_then(|value| value.to_str())
            .is_some_and(|value| {
                matches!(
                    value.to_ascii_lowercase().as_str(),
                    "mp3" | "wav" | "flac" | "ogg" | "m4a" | "aac" | "opus"
                ) || (qmediasync_audio && value.eq_ignore_ascii_case("strm"))
            })
    });
    if !has_track {
        return Ok(Err(
            "audio source was not confirmed present; preserving the previous work".to_string(),
        ));
    }
    let request = AudioInspectionRequest {
        root_id: event.root_id,
        root_generation: event.generation,
        scan_token: fence_token.to_string(),
        provider: event.root.provider.clone(),
        qmediasync_mount_name: vfs::qmediasync_mount_name(&event.root.provider).map(str::to_string),
        root: event.root.root.clone(),
        work_key: event.work_key.clone(),
        previous_work_key: None,
        relative_paths,
        grouping: event.root.audio_grouping,
    };
    let mut stream = match audio_inspector::inspect(resources, request).await {
        Ok(stream) => stream,
        Err(AppError::Overloaded { .. }) => {
            return Err(AppError::Other(
                "audio inspector admission was overloaded; retrying durable event".to_string(),
            ))
        }
        Err(error) => return Ok(Err(bounded_error(&error.to_string()))),
    };
    let mut finalized = false;
    while let Some(mutation) = stream.next().await {
        finalized |= mutation.fence.complete_snapshot;
        db.apply_catalog_work_mutation(mutation).await?;
    }
    if let Err(error) = stream.finish().await {
        return Ok(Err(bounded_error(&error.to_string())));
    }
    if !finalized {
        return Ok(Err(
            "audio inspector ended without a complete snapshot marker".to_string(),
        ));
    }
    Ok(Ok(AudioCatalogOutcome::Applied))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GalleryCatalogOutcome {
    Applied,
    Preserved,
}

async fn apply_gallery_catalog_event(
    db: &Db,
    resources: &ResourceGovernor,
    event: &PendingEvent,
    fence_token: &str,
) -> Result<std::result::Result<GalleryCatalogOutcome, String>> {
    if event.root.kind != "gallery" || !gallery_work_key_is_safe(&event.work_key) {
        return Ok(Ok(GalleryCatalogOutcome::Preserved));
    }
    if !catalog_kind_is_v2(db, "gallery").await? {
        return Ok(Ok(GalleryCatalogOutcome::Preserved));
    }
    let fence = MutationFence {
        root_generation: event.generation,
        scan_token: fence_token.to_string(),
        complete_snapshot: true,
    };
    let source = MutationSource {
        kind: "gallery".to_string(),
        root_id: event.root_id,
        work_key: event.work_key.clone(),
        provider: event.root.provider.clone(),
    };
    if event.event_kind == CATALOG_DELETE_EVENT {
        db.apply_catalog_work_tombstone(WorkTombstone {
            source,
            fence,
            reason: Some("filesystem-remove-or-complete-reconcile".to_string()),
        })
        .await?;
        return Ok(Ok(GalleryCatalogOutcome::Applied));
    }
    let catalog_assets =
        match load_gallery_catalog_assets(db, event.root_id, &event.work_key, "present").await {
            Ok(assets) => assets,
            Err(AppError::Other(error)) => return Ok(Err(bounded_error(&error))),
            Err(error) => return Err(error),
        };
    if catalog_assets.is_empty() {
        return Ok(Err(
            "gallery source was not confirmed present; preserving the previous work".to_string(),
        ));
    }
    let previous_work_key = previous_gallery_work_key(db, event, &catalog_assets).await?;
    let request = GalleryInspectionRequest {
        root_id: event.root_id,
        root_generation: event.generation,
        scan_token: fence_token.to_string(),
        provider: event.root.provider.clone(),
        root: event.root.root.clone(),
        work_key: event.work_key.clone(),
        previous_work_key,
        assets: catalog_assets
            .into_iter()
            .map(|asset| GalleryInventoryAsset {
                relative_path: asset.relative_path,
                size: asset.size,
                source_version: asset.source_version,
            })
            .collect(),
    };
    let mut stream = match gallery_inspector::inspect(resources, request).await {
        Ok(stream) => stream,
        Err(AppError::Overloaded { .. }) => {
            return Err(AppError::Other(
                "gallery inspector admission was overloaded; retrying durable event".to_string(),
            ))
        }
        Err(error) => return Ok(Err(bounded_error(&error.to_string()))),
    };
    let mut finalized = false;
    while let Some(mutation) = stream.next().await {
        finalized |= mutation.fence.complete_snapshot;
        db.apply_catalog_work_mutation(mutation).await?;
    }
    if let Err(error) = stream.finish().await {
        return Ok(Err(bounded_error(&error.to_string())));
    }
    if !finalized {
        return Ok(Err(
            "gallery inspector ended without a complete snapshot marker".to_string(),
        ));
    }
    Ok(Ok(GalleryCatalogOutcome::Applied))
}

async fn fail_novel_catalog_event(
    db: &Db,
    event: &PendingEvent,
    error: &str,
    retry: bool,
) -> Result<()> {
    let status = if retry { "pending" } else { "failed" };
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    sqlx::query(
        "UPDATE scan_events SET status = ?2, last_error = ?3 WHERE seq = ?1 AND status = 'processing'",
    )
    .bind(event.seq)
    .bind(status)
    .bind(bounded_error(error))
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

async fn drain_novel_catalog_events_under_fence(
    db: &Db,
    resources: &ResourceGovernor,
    root: &InventoryRoot,
    generation: i64,
    token: &str,
    generated_dir: &Path,
    limit: i64,
) -> Result<usize> {
    let events =
        claim_novel_catalog_events_under_fence(db, root.id, generation, token, limit).await?;
    let mut handled = 0;
    for event in events {
        match apply_novel_catalog_event(db, resources, &event, generated_dir, token).await? {
            Ok(NovelCatalogOutcome::Applied) | Ok(NovelCatalogOutcome::Preserved) => {
                complete_event(db, &event).await?;
                update_novel_coordinator_state(
                    db,
                    root.id,
                    generation,
                    "processing",
                    Some(event.seq),
                    None,
                )
                .await?;
            }
            Err(error) => {
                let retry = event.attempts < 3;
                fail_novel_catalog_event(db, &event, &error.to_string(), retry).await?;
                update_novel_coordinator_state(
                    db,
                    root.id,
                    generation,
                    "degraded",
                    Some(event.seq),
                    Some(&error.to_string()),
                )
                .await?;
            }
        }
        handled += 1;
    }
    if handled == 0 {
        let phase = catalog_event_queue_phase(db, root.id).await?;
        update_novel_coordinator_state(db, root.id, generation, phase, None, None).await?;
    }
    Ok(handled)
}

async fn drain_archive_catalog_events_under_fence(
    db: &Db,
    resources: &ResourceGovernor,
    root: &InventoryRoot,
    generation: i64,
    token: &str,
    limit: i64,
) -> Result<usize> {
    let events =
        claim_novel_catalog_events_under_fence(db, root.id, generation, token, limit).await?;
    let mut handled = 0;
    for event in events {
        let outcome = match root.spec.kind.as_str() {
            "comic" => apply_comic_catalog_event(db, resources, &event, token).await?,
            "coser-picture" => {
                apply_coser_picture_catalog_event(db, resources, &event, token).await?
            }
            other => {
                return Err(AppError::BadRequest(format!(
                    "unsupported archive coordinator kind {other}"
                )))
            }
        };
        match outcome {
            Ok(ArchiveCatalogOutcome::Applied) | Ok(ArchiveCatalogOutcome::Preserved) => {
                complete_event(db, &event).await?;
                update_novel_coordinator_state(
                    db,
                    root.id,
                    generation,
                    "processing",
                    Some(event.seq),
                    None,
                )
                .await?;
            }
            Err(error) => {
                let retry = event.attempts < 3;
                fail_novel_catalog_event(db, &event, &error.to_string(), retry).await?;
                update_novel_coordinator_state(
                    db,
                    root.id,
                    generation,
                    "degraded",
                    Some(event.seq),
                    Some(&error.to_string()),
                )
                .await?;
            }
        }
        handled += 1;
    }
    if handled == 0 {
        let phase = catalog_event_queue_phase(db, root.id).await?;
        update_novel_coordinator_state(db, root.id, generation, phase, None, None).await?;
    }
    Ok(handled)
}

async fn drain_audio_catalog_events_under_fence(
    db: &Db,
    resources: &ResourceGovernor,
    root: &InventoryRoot,
    generation: i64,
    token: &str,
    limit: i64,
) -> Result<usize> {
    if root.spec.kind != "audio" {
        return Ok(0);
    }
    let events =
        claim_novel_catalog_events_under_fence(db, root.id, generation, token, limit).await?;
    let mut handled = 0;
    for event in events {
        match apply_audio_catalog_event(db, resources, &event, token).await? {
            Ok(AudioCatalogOutcome::Applied) | Ok(AudioCatalogOutcome::Preserved) => {
                complete_event(db, &event).await?;
                update_novel_coordinator_state(
                    db,
                    root.id,
                    generation,
                    "processing",
                    Some(event.seq),
                    None,
                )
                .await?;
            }
            Err(error) => {
                let retry = event.attempts < 3;
                fail_novel_catalog_event(db, &event, &error, retry).await?;
                update_novel_coordinator_state(
                    db,
                    root.id,
                    generation,
                    "degraded",
                    Some(event.seq),
                    Some(&error),
                )
                .await?;
            }
        }
        handled += 1;
    }
    if handled == 0 {
        let phase = catalog_event_queue_phase(db, root.id).await?;
        update_novel_coordinator_state(db, root.id, generation, phase, None, None).await?;
    }
    Ok(handled)
}

async fn drain_gallery_catalog_events_under_fence(
    db: &Db,
    resources: &ResourceGovernor,
    root: &InventoryRoot,
    generation: i64,
    token: &str,
    limit: i64,
) -> Result<usize> {
    if root.spec.kind != "gallery" {
        return Ok(0);
    }
    let events =
        claim_novel_catalog_events_under_fence(db, root.id, generation, token, limit).await?;
    let mut handled = 0;
    for event in events {
        match apply_gallery_catalog_event(db, resources, &event, token).await? {
            Ok(GalleryCatalogOutcome::Applied) | Ok(GalleryCatalogOutcome::Preserved) => {
                complete_event(db, &event).await?;
                update_novel_coordinator_state(
                    db,
                    root.id,
                    generation,
                    "processing",
                    Some(event.seq),
                    None,
                )
                .await?;
            }
            Err(error) => {
                let retry = event.attempts < 3;
                fail_novel_catalog_event(db, &event, &error, retry).await?;
                update_novel_coordinator_state(
                    db,
                    root.id,
                    generation,
                    "degraded",
                    Some(event.seq),
                    Some(&error),
                )
                .await?;
            }
        }
        handled += 1;
    }
    if handled == 0 {
        let phase = catalog_event_queue_phase(db, root.id).await?;
        update_novel_coordinator_state(db, root.id, generation, phase, None, None).await?;
    }
    Ok(handled)
}

async fn drain_root_catalog_events_under_fence(
    db: &Db,
    resources: &ResourceGovernor,
    root: &InventoryRoot,
    generation: i64,
    token: &str,
    generated_dir: &Path,
    limit: i64,
) -> Result<usize> {
    let mut total = 0_usize;
    loop {
        let handled = if root.spec.kind == "novel" {
            drain_novel_catalog_events_under_fence(
                db,
                resources,
                root,
                generation,
                token,
                generated_dir,
                limit,
            )
            .await?
        } else if root.spec.kind == "comic" || root.spec.kind == "coser-picture" {
            drain_archive_catalog_events_under_fence(db, resources, root, generation, token, limit)
                .await?
        } else if root.spec.kind == "audio" {
            drain_audio_catalog_events_under_fence(db, resources, root, generation, token, limit)
                .await?
        } else if root.spec.kind == "gallery" {
            drain_gallery_catalog_events_under_fence(db, resources, root, generation, token, limit)
                .await?
        } else {
            return Ok(0);
        };
        total = total.saturating_add(handled);
        if handled == 0 {
            return Ok(total);
        }
    }
}

async fn acquire_novel_coordinator_lease(
    db: &Db,
    root_id: i64,
    token: &str,
    kind: &str,
) -> Result<Option<NovelCoordinatorLease>> {
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    let row = sqlx::query(
        r#"
        SELECT id, kind, provider, root, scan_depth, device_class, audio_grouping, generation, status
        FROM library_roots
        WHERE id = ?1 AND enabled = 1 AND kind = ?2 AND status = 'idle'
          AND EXISTS (
              SELECT 1
              FROM catalog_kind_ownership
              WHERE kind = ?2 AND authoritative_writer = 'catalog-v2'
          )
        "#,
    )
    .bind(root_id)
    .bind(kind)
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(row) = row else {
        transaction.commit().await?;
        return Ok(None);
    };
    let root_text: String = row.get("root");
    let generation: i64 = row.get("generation");
    let updated = sqlx::query(
        r#"
        UPDATE library_roots
        SET status = 'scanning', active_token = ?2,
            scan_started_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
            last_error = NULL
        WHERE id = ?1 AND enabled = 1 AND kind = ?3
          AND generation = ?4 AND status = 'idle'
        "#,
    )
    .bind(root_id)
    .bind(token)
    .bind(kind)
    .bind(generation)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    if updated != 1 {
        transaction.commit().await?;
        return Ok(None);
    }
    sqlx::query(
        r#"
        INSERT INTO novel_coordinator_state (root_id, generation, phase, updated_at)
        VALUES (?1, ?2, 'processing', strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        ON CONFLICT(root_id) DO UPDATE SET
            generation = excluded.generation,
            phase = 'processing',
            updated_at = excluded.updated_at,
            last_error = NULL
        "#,
    )
    .bind(root_id)
    .bind(generation)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(Some(NovelCoordinatorLease {
        root_id,
        generation,
        token: token.to_string(),
        root: RootSpec {
            kind: row.get("kind"),
            provider: row.get("provider"),
            root: PathBuf::from(&root_text),
            root_text,
            scan_depth: row
                .get::<Option<i64>, _>("scan_depth")
                .and_then(|value| usize::try_from(value).ok()),
            device_class: row.get("device_class"),
            audio_grouping: parse_audio_grouping(row.get("audio_grouping")),
        },
    }))
}

async fn release_novel_coordinator_lease(
    db: &Db,
    lease: &NovelCoordinatorLease,
    needs_reconcile: bool,
    phase_override: Option<&str>,
    error: Option<&str>,
) -> Result<()> {
    let status = if needs_reconcile {
        "needs_reconcile"
    } else {
        "idle"
    };
    let _write_slot = db.acquire_write_slot(128 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    let updated = sqlx::query(
        r#"
        UPDATE library_roots
        SET status = ?2,
            active_token = NULL,
            scan_started_at = NULL,
            last_reconcile_at = CASE WHEN ?2 = 'idle' THEN strftime('%Y-%m-%dT%H:%M:%fZ','now') ELSE last_reconcile_at END
        WHERE id = ?1 AND generation = ?3 AND active_token = ?4 AND status = 'scanning'
        "#,
    )
    .bind(lease.root_id)
    .bind(status)
    .bind(lease.generation)
    .bind(&lease.token)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    if updated != 1 {
        transaction.rollback().await?;
        return Err(AppError::Other(format!(
            "novel coordinator lease release fence rejected root {} generation {}",
            lease.root_id, lease.generation
        )));
    }
    let phase = if needs_reconcile {
        "needs_reconcile"
    } else {
        phase_override.unwrap_or("idle")
    };
    update_novel_coordinator_state_in_transaction(
        &mut transaction,
        lease.root_id,
        lease.generation,
        phase,
        None,
        error,
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

async fn catalog_kind_is_v2(db: &Db, kind: &str) -> Result<bool> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM catalog_kind_ownership WHERE kind = ?1 AND authoritative_writer = 'catalog-v2'",
    )
    .bind(kind)
    .fetch_optional(db.pool())
    .await?
    .is_some())
}

async fn pending_catalog_event_root_ids(db: &Db, kind: &str) -> Result<Vec<i64>> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let root_ids = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT DISTINCT root.id
        FROM scan_events AS event
        JOIN library_roots AS root ON root.id = event.root_id
        WHERE event.status = 'pending'
          AND event.event_kind IN ('catalog-upsert', 'catalog-delete')
          AND root.kind = ?1
          AND root.enabled = 1
          AND root.status = 'idle'
        ORDER BY root.id
        LIMIT 16
        "#,
    )
    .bind(kind)
    .fetch_all(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(root_ids)
}

async fn terminal_catalog_event_status(db: &Db, root_id: i64) -> Result<(i64, Option<String>)> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let terminal = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM scan_events AS failed
        WHERE failed.root_id = ?1
          AND failed.status = 'failed'
          AND failed.event_kind IN ('catalog-upsert', 'catalog-delete')
          AND failed.work_key IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM scan_events AS newer
              WHERE newer.root_id = failed.root_id
                AND newer.work_key = failed.work_key
                AND newer.event_kind IN ('catalog-upsert', 'catalog-delete')
                AND newer.seq > failed.seq
          )
        "#,
    )
    .bind(root_id)
    .fetch_one(&mut *transaction)
    .await?;
    let terminal_error = if terminal > 0 {
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT failed.last_error
            FROM scan_events AS failed
            WHERE failed.root_id = ?1
              AND failed.status = 'failed'
              AND failed.event_kind IN ('catalog-upsert', 'catalog-delete')
              AND failed.work_key IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM scan_events AS newer
                  WHERE newer.root_id = failed.root_id
                    AND newer.work_key = failed.work_key
                    AND newer.event_kind IN ('catalog-upsert', 'catalog-delete')
                    AND newer.seq > failed.seq
              )
            ORDER BY failed.seq DESC
            LIMIT 1
            "#,
        )
        .bind(root_id)
        .fetch_optional(&mut *transaction)
        .await?
    } else {
        None
    };
    transaction.commit().await?;
    Ok((terminal, terminal_error))
}

async fn catalog_event_queue_phase(db: &Db, root_id: i64) -> Result<&'static str> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let failed = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM scan_events AS failed
        WHERE failed.root_id = ?1
          AND failed.status = 'failed'
          AND failed.event_kind IN ('catalog-upsert', 'catalog-delete')
          AND failed.work_key IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM scan_events AS newer
              WHERE newer.root_id = failed.root_id
                AND newer.work_key = failed.work_key
                AND newer.event_kind IN ('catalog-upsert', 'catalog-delete')
                AND newer.seq > failed.seq
          )
        "#,
    )
    .bind(root_id)
    .fetch_one(&mut *transaction)
    .await?;
    let pending = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM scan_events WHERE root_id = ?1 AND status IN ('pending', 'processing') AND event_kind IN ('catalog-upsert', 'catalog-delete')",
    )
    .bind(root_id)
    .fetch_one(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(if failed > 0 {
        "degraded"
    } else if pending > 0 {
        "processing"
    } else {
        "idle"
    })
}

/// Read the current Catalog v2 ownership set once for a watcher burst.
///
/// Watcher paths can arrive in batches containing several media kinds.  The
/// decision to journal a changed work key must use one ownership generation,
/// while five independent pool queries would add avoidable checkout latency
/// and could observe a mixed cutover state.  The snapshot is committed before
/// any event-journal write or filesystem work begins.
async fn catalog_v2_kinds_snapshot(db: &Db) -> Result<BTreeSet<String>> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let kinds = sqlx::query_scalar::<_, String>(
        "SELECT kind FROM catalog_kind_ownership WHERE authoritative_writer = 'catalog-v2' ORDER BY kind",
    )
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .collect();
    transaction.commit().await?;
    Ok(kinds)
}

async fn process_pending_novel_catalog_events(
    db: &Db,
    resources: &ResourceGovernor,
    generated_dir: &Path,
    limit: i64,
    catalog_v2_owned: bool,
) -> Result<(usize, usize)> {
    if !catalog_v2_owned {
        return Ok((0, 0));
    }
    let root_ids = pending_catalog_event_root_ids(db, "novel").await?;
    let mut completed = 0_usize;
    let mut failed = 0_usize;
    for root_id in root_ids {
        if completed.saturating_add(failed) >= usize::try_from(limit.max(1)).unwrap_or(usize::MAX) {
            break;
        }
        let token = uuid::Uuid::new_v4().to_string();
        if !db
            .try_acquire_scanner_lock("library", &token, NOVEL_COORDINATOR_LOCK_TTL_SECONDS)
            .await?
        {
            continue;
        }
        let lease = acquire_novel_coordinator_lease(db, root_id, &token, "novel").await?;
        let Some(lease) = lease else {
            let _ = db.release_scanner_lock("library", &token).await;
            continue;
        };
        let inventory_root = InventoryRoot {
            id: lease.root_id,
            spec: lease.root.clone(),
        };
        let remaining = limit
            .max(1)
            .saturating_sub(i64::try_from(completed.saturating_add(failed)).unwrap_or(i64::MAX));
        let drain = drain_novel_catalog_events_under_fence(
            db,
            resources,
            &inventory_root,
            lease.generation,
            &lease.token,
            generated_dir,
            remaining.min(NOVEL_COORDINATOR_BATCH_SIZE),
        )
        .await;
        match drain {
            Ok(handled) => {
                // The queue records terminal inspector failures separately;
                // count them for diagnostics without treating a bad EPUB as a
                // reason to delete the previous catalog row.
                let (terminal, terminal_error) = terminal_catalog_event_status(db, root_id).await?;
                completed = completed.saturating_add(handled);
                failed = failed.saturating_add(usize::try_from(terminal).unwrap_or(usize::MAX));
                release_novel_coordinator_lease(
                    db,
                    &lease,
                    false,
                    (terminal > 0).then_some("degraded"),
                    terminal_error.as_deref(),
                )
                .await?;
            }
            Err(error) => {
                failed = failed.saturating_add(1);
                let error_text = error.to_string();
                let _ = release_novel_coordinator_lease(
                    db,
                    &lease,
                    true,
                    Some("needs_reconcile"),
                    Some(&error_text),
                )
                .await;
            }
        }
        let _ = db.release_scanner_lock("library", &token).await;
    }
    Ok((completed, failed))
}

async fn process_pending_archive_catalog_events(
    db: &Db,
    resources: &ResourceGovernor,
    limit: i64,
    kind: &str,
    catalog_v2_owned: bool,
) -> Result<(usize, usize)> {
    if !matches!(kind, "comic" | "coser-picture") {
        return Err(AppError::BadRequest(format!(
            "unsupported archive coordinator kind {kind}"
        )));
    }
    if !catalog_v2_owned {
        return Ok((0, 0));
    }
    let root_ids = pending_catalog_event_root_ids(db, kind).await?;
    let mut completed = 0_usize;
    let mut failed = 0_usize;
    for root_id in root_ids {
        if completed.saturating_add(failed) >= usize::try_from(limit.max(1)).unwrap_or(usize::MAX) {
            break;
        }
        let token = uuid::Uuid::new_v4().to_string();
        if !db
            .try_acquire_scanner_lock("library", &token, NOVEL_COORDINATOR_LOCK_TTL_SECONDS)
            .await?
        {
            continue;
        }
        let lease = acquire_novel_coordinator_lease(db, root_id, &token, kind).await?;
        let Some(lease) = lease else {
            let _ = db.release_scanner_lock("library", &token).await;
            continue;
        };
        let inventory_root = InventoryRoot {
            id: lease.root_id,
            spec: lease.root.clone(),
        };
        let remaining = limit
            .max(1)
            .saturating_sub(i64::try_from(completed.saturating_add(failed)).unwrap_or(i64::MAX));
        let drain = drain_archive_catalog_events_under_fence(
            db,
            resources,
            &inventory_root,
            lease.generation,
            &lease.token,
            remaining.min(NOVEL_COORDINATOR_BATCH_SIZE),
        )
        .await;
        match drain {
            Ok(handled) => {
                let (terminal, terminal_error) = terminal_catalog_event_status(db, root_id).await?;
                completed = completed.saturating_add(handled);
                failed = failed.saturating_add(usize::try_from(terminal).unwrap_or(usize::MAX));
                release_novel_coordinator_lease(
                    db,
                    &lease,
                    false,
                    (terminal > 0).then_some("degraded"),
                    terminal_error.as_deref(),
                )
                .await?;
            }
            Err(error) => {
                failed = failed.saturating_add(1);
                let error_text = error.to_string();
                let _ = release_novel_coordinator_lease(
                    db,
                    &lease,
                    true,
                    Some("needs_reconcile"),
                    Some(&error_text),
                )
                .await;
            }
        }
        let _ = db.release_scanner_lock("library", &token).await;
    }
    Ok((completed, failed))
}

async fn process_pending_audio_catalog_events(
    db: &Db,
    resources: &ResourceGovernor,
    limit: i64,
    catalog_v2_owned: bool,
) -> Result<(usize, usize)> {
    if !catalog_v2_owned {
        return Ok((0, 0));
    }
    let root_ids = pending_catalog_event_root_ids(db, "audio").await?;
    let mut completed = 0_usize;
    let mut failed = 0_usize;
    for root_id in root_ids {
        if completed.saturating_add(failed) >= usize::try_from(limit.max(1)).unwrap_or(usize::MAX) {
            break;
        }
        let token = uuid::Uuid::new_v4().to_string();
        if !db
            .try_acquire_scanner_lock("library", &token, NOVEL_COORDINATOR_LOCK_TTL_SECONDS)
            .await?
        {
            continue;
        }
        let lease = acquire_novel_coordinator_lease(db, root_id, &token, "audio").await?;
        let Some(lease) = lease else {
            let _ = db.release_scanner_lock("library", &token).await;
            continue;
        };
        let inventory_root = InventoryRoot {
            id: lease.root_id,
            spec: lease.root.clone(),
        };
        let remaining = limit
            .max(1)
            .saturating_sub(i64::try_from(completed.saturating_add(failed)).unwrap_or(i64::MAX));
        let drain = drain_audio_catalog_events_under_fence(
            db,
            resources,
            &inventory_root,
            lease.generation,
            &lease.token,
            remaining.min(NOVEL_COORDINATOR_BATCH_SIZE),
        )
        .await;
        match drain {
            Ok(handled) => {
                let (terminal, terminal_error) = terminal_catalog_event_status(db, root_id).await?;
                completed = completed.saturating_add(handled);
                failed = failed.saturating_add(usize::try_from(terminal).unwrap_or(usize::MAX));
                release_novel_coordinator_lease(
                    db,
                    &lease,
                    false,
                    (terminal > 0).then_some("degraded"),
                    terminal_error.as_deref(),
                )
                .await?;
            }
            Err(error) => {
                failed = failed.saturating_add(1);
                let error_text = error.to_string();
                let _ = release_novel_coordinator_lease(
                    db,
                    &lease,
                    true,
                    Some("needs_reconcile"),
                    Some(&error_text),
                )
                .await;
            }
        }
        let _ = db.release_scanner_lock("library", &token).await;
    }
    Ok((completed, failed))
}

async fn process_pending_gallery_catalog_events(
    db: &Db,
    resources: &ResourceGovernor,
    limit: i64,
    catalog_v2_owned: bool,
) -> Result<(usize, usize)> {
    if !catalog_v2_owned {
        return Ok((0, 0));
    }
    let root_ids = pending_catalog_event_root_ids(db, "gallery").await?;
    let mut completed = 0_usize;
    let mut failed = 0_usize;
    for root_id in root_ids {
        if completed.saturating_add(failed) >= usize::try_from(limit.max(1)).unwrap_or(usize::MAX) {
            break;
        }
        let token = uuid::Uuid::new_v4().to_string();
        if !db
            .try_acquire_scanner_lock("library", &token, NOVEL_COORDINATOR_LOCK_TTL_SECONDS)
            .await?
        {
            continue;
        }
        let lease = acquire_novel_coordinator_lease(db, root_id, &token, "gallery").await?;
        let Some(lease) = lease else {
            let _ = db.release_scanner_lock("library", &token).await;
            continue;
        };
        let inventory_root = InventoryRoot {
            id: lease.root_id,
            spec: lease.root.clone(),
        };
        let remaining = limit
            .max(1)
            .saturating_sub(i64::try_from(completed.saturating_add(failed)).unwrap_or(i64::MAX));
        let drain = drain_gallery_catalog_events_under_fence(
            db,
            resources,
            &inventory_root,
            lease.generation,
            &lease.token,
            remaining.min(NOVEL_COORDINATOR_BATCH_SIZE),
        )
        .await;
        match drain {
            Ok(handled) => {
                let (terminal, terminal_error) = terminal_catalog_event_status(db, root_id).await?;
                completed = completed.saturating_add(handled);
                failed = failed.saturating_add(usize::try_from(terminal).unwrap_or(usize::MAX));
                release_novel_coordinator_lease(
                    db,
                    &lease,
                    false,
                    (terminal > 0).then_some("degraded"),
                    terminal_error.as_deref(),
                )
                .await?;
            }
            Err(error) => {
                failed = failed.saturating_add(1);
                let error_text = error.to_string();
                let _ = release_novel_coordinator_lease(
                    db,
                    &lease,
                    true,
                    Some("needs_reconcile"),
                    Some(&error_text),
                )
                .await;
            }
        }
        let _ = db.release_scanner_lock("library", &token).await;
    }
    Ok((completed, failed))
}

async fn begin_root_scan(db: &Db, root_id: i64, token: &str) -> Result<i64> {
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    sqlx::query_scalar::<_, i64>(
        r#"
        UPDATE library_roots
        SET generation = generation + 1,
            status = 'scanning',
            active_token = ?2,
            scan_started_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
            last_error = NULL
        WHERE id = ?1 AND enabled = 1
        RETURNING generation
        "#,
    )
    .bind(root_id)
    .bind(token)
    .fetch_optional(db.pool())
    .await?
    .ok_or_else(|| AppError::Other(format!("inventory root {root_id} is disabled or missing")))
}

async fn upsert_inventory_batch(
    db: &Db,
    root_id: i64,
    generation: i64,
    token: &str,
    batch: &[InventoryRow],
) -> Result<InventoryBatchStats> {
    if batch.is_empty() {
        return Ok(InventoryBatchStats::default());
    }
    if batch.len() > INVENTORY_BATCH_SIZE {
        return Err(AppError::Other(format!(
            "inventory batch exceeds fixed limit {INVENTORY_BATCH_SIZE}"
        )));
    }
    let payload = serde_json::to_string(batch)
        .map_err(|err| AppError::Other(format!("inventory batch serialization failed: {err}")))?;
    let _write_slot = db.acquire_write_slot(512 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    let root_kind = sqlx::query_scalar::<_, String>(
        r#"
        SELECT kind FROM library_roots
        WHERE id = ?1 AND generation = ?2 AND active_token = ?3 AND status = 'scanning'
        "#,
    )
    .bind(root_id)
    .bind(generation)
    .bind(token)
    .fetch_optional(&mut *transaction)
    .await?;
    let root_kind = root_kind.ok_or_else(|| {
        AppError::Other(format!(
            "inventory generation fence rejected root {root_id} generation {generation}"
        ))
    })?;
    sqlx::query(
        r#"
        CREATE TEMP TABLE IF NOT EXISTS temp_inventory_batch (
            relative_path TEXT PRIMARY KEY,
            parent_key TEXT NOT NULL,
            media_class TEXT NOT NULL,
            size INTEGER NOT NULL,
            mtime_ns INTEGER NOT NULL,
            file_id TEXT,
            fast_fingerprint TEXT NOT NULL,
            work_key TEXT NOT NULL
        ) WITHOUT ROWID
        "#,
    )
    .execute(&mut *transaction)
    .await?;
    sqlx::query("DELETE FROM temp_inventory_batch")
        .execute(&mut *transaction)
        .await?;
    sqlx::query(
        r#"
        INSERT INTO temp_inventory_batch (
            relative_path, parent_key, media_class, size, mtime_ns,
            file_id, fast_fingerprint, work_key
        )
        SELECT
            json_extract(value, '$.p'), json_extract(value, '$.d'),
            json_extract(value, '$.c'), json_extract(value, '$.s'),
            json_extract(value, '$.m'), json_extract(value, '$.i'),
            json_extract(value, '$.f'), json_extract(value, '$.w')
        FROM json_each(?1)
        "#,
    )
    .bind(&payload)
    .execute(&mut *transaction)
    .await?;
    let counts = sqlx::query(
        r#"
        SELECT
            COALESCE(SUM(CASE WHEN inventory.relative_path IS NULL THEN 1 ELSE 0 END), 0) AS inserted,
            COALESCE(SUM(CASE
                WHEN inventory.relative_path IS NOT NULL AND (
                    inventory.fast_fingerprint IS NOT batch.fast_fingerprint
                    OR inventory.media_class IS NOT batch.media_class
                    OR inventory.work_key IS NOT batch.work_key
                    OR inventory.status != 'present'
                ) THEN 1 ELSE 0 END), 0) AS changed
        FROM temp_inventory_batch AS batch
        LEFT JOIN file_inventory AS inventory
          ON inventory.root_id = ?1 AND inventory.relative_path = batch.relative_path
        "#,
    )
    .bind(root_id)
    .fetch_one(&mut *transaction)
    .await?;
    let inserted = counts.get::<i64, _>("inserted").max(0) as u64;
    let changed = counts.get::<i64, _>("changed").max(0) as u64;
    let novel_candidates = sqlx::query(
        r#"
        WITH candidates AS (
            SELECT batch.work_key
            FROM temp_inventory_batch AS batch
            LEFT JOIN file_inventory AS inventory
              ON inventory.root_id = ?1 AND inventory.relative_path = batch.relative_path
            WHERE lower(batch.relative_path) GLOB '*.epub'
              AND EXISTS (
                  SELECT 1 FROM library_roots AS root
                  WHERE root.id = ?1 AND root.kind = 'novel'
              )
              AND (
                    inventory.relative_path IS NULL
                 OR inventory.fast_fingerprint IS NOT batch.fast_fingerprint
                 OR inventory.media_class IS NOT batch.media_class
                 OR inventory.work_key IS NOT batch.work_key
                 OR inventory.status != 'present'
                 OR EXISTS (
                     SELECT 1 FROM catalog_work_sources AS source
                     JOIN works ON works.id = source.work_id
                     WHERE source.kind = 'novel'
                       AND source.root_id = ?1
                       AND source.work_key = batch.work_key
                       AND works.deleted_at IS NOT NULL
                 )
                 OR NOT EXISTS (
                     SELECT 1 FROM catalog_work_sources AS source
                     WHERE source.kind = 'novel'
                       AND source.root_id = ?1
                       AND source.work_key = batch.work_key
                 )
              )
              AND EXISTS (
                  SELECT 1 FROM catalog_kind_ownership
                  WHERE kind = 'novel' AND authoritative_writer = 'catalog-v2'
              )
        )
        SELECT COUNT(*) AS candidate_count,
               COALESCE(SUM(length(work_key)), 0) AS candidate_bytes
        FROM candidates
        "#,
    )
    .bind(root_id)
    .fetch_one(&mut *transaction)
    .await?;
    let candidate_count = novel_candidates.get::<i64, _>("candidate_count").max(0);
    let candidate_bytes = novel_candidates.get::<i64, _>("candidate_bytes").max(0);
    if candidate_count > 0 {
        if let Some(message) = ensure_catalog_queue_capacity(
            &mut transaction,
            root_id,
            candidate_count,
            candidate_bytes,
        )
        .await?
        {
            transaction.commit().await?;
            return Err(AppError::Other(message));
        }
        sqlx::query(
            r#"
            INSERT INTO scan_events (
                root_id, relative_path, event_kind, work_key, observed_at, status
            )
            SELECT
                ?1, batch.relative_path, 'catalog-upsert', batch.work_key,
                strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'pending'
            FROM temp_inventory_batch AS batch
            LEFT JOIN file_inventory AS inventory
              ON inventory.root_id = ?1 AND inventory.relative_path = batch.relative_path
            WHERE lower(batch.relative_path) GLOB '*.epub'
              AND EXISTS (
                  SELECT 1 FROM library_roots AS root
                  WHERE root.id = ?1 AND root.kind = 'novel'
              )
              AND (
                    inventory.relative_path IS NULL
                 OR inventory.fast_fingerprint IS NOT batch.fast_fingerprint
                 OR inventory.media_class IS NOT batch.media_class
                 OR inventory.work_key IS NOT batch.work_key
                 OR inventory.status != 'present'
                 OR EXISTS (
                     SELECT 1 FROM catalog_work_sources AS source
                     JOIN works ON works.id = source.work_id
                     WHERE source.kind = 'novel'
                       AND source.root_id = ?1
                       AND source.work_key = batch.work_key
                       AND works.deleted_at IS NOT NULL
                 )
                 OR NOT EXISTS (
                     SELECT 1 FROM catalog_work_sources AS source
                     WHERE source.kind = 'novel'
                       AND source.root_id = ?1
                       AND source.work_key = batch.work_key
                 )
              )
            ON CONFLICT DO UPDATE SET
                relative_path = excluded.relative_path,
                event_kind = excluded.event_kind,
                observed_at = excluded.observed_at,
                attempts = 0,
                last_error = NULL
            "#,
        )
        .bind(root_id)
        .execute(&mut *transaction)
        .await?;
    }
    if matches!(root_kind.as_str(), "comic" | "coser-picture") {
        let (_, archive_overflow) =
            enqueue_archive_temp_catalog_events(&mut transaction, root_id, &root_kind).await?;
        if let Some(message) = archive_overflow {
            transaction.commit().await?;
            return Err(AppError::Other(message));
        }
    }
    let affected = sqlx::query(
        r#"
        INSERT INTO file_inventory (
            root_id, relative_path, parent_key, media_class, size, mtime_ns,
            file_id, fast_fingerprint, work_key, seen_generation, status, last_error
        )
        SELECT
            ?1, batch.relative_path, batch.parent_key, batch.media_class,
            batch.size, batch.mtime_ns, batch.file_id, batch.fast_fingerprint,
            batch.work_key, ?2, 'present', NULL
        FROM temp_inventory_batch AS batch
        WHERE EXISTS (
            SELECT 1 FROM library_roots
            WHERE id = ?1 AND generation = ?2 AND active_token = ?3 AND status = 'scanning'
        )
        ON CONFLICT(root_id, relative_path) DO UPDATE SET
            parent_key = excluded.parent_key,
            media_class = excluded.media_class,
            size = excluded.size,
            mtime_ns = excluded.mtime_ns,
            file_id = excluded.file_id,
            fast_fingerprint = excluded.fast_fingerprint,
            work_key = excluded.work_key,
            seen_generation = excluded.seen_generation,
            status = 'present',
            last_error = NULL
        "#,
    )
    .bind(root_id)
    .bind(generation)
    .bind(token)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    if affected != batch.len() as u64 {
        return Err(AppError::Other(format!(
            "inventory generation fence changed while writing root {root_id} generation {generation}"
        )));
    }
    transaction.commit().await?;
    let discovered = batch.len() as u64;
    Ok(InventoryBatchStats {
        discovered,
        inserted,
        changed,
        unchanged: discovered.saturating_sub(inserted).saturating_sub(changed),
        serialized_bytes: payload.len(),
    })
}

async fn mark_root_scan_missing(
    db: &Db,
    root_id: i64,
    generation: i64,
    token: &str,
    run: &InventoryRootRun,
) -> Result<u64> {
    let _write_slot = db.acquire_write_slot(256 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    ensure_generation_fence(&mut transaction, root_id, generation, token).await?;
    let newly_missing = sqlx::query(
        r#"
        UPDATE file_inventory
        SET status = 'missing', last_error = NULL
        WHERE root_id = ?1 AND seen_generation != ?2 AND status = 'present'
        "#,
    )
    .bind(root_id)
    .bind(generation)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    let counts = sqlx::query(
        r#"
        SELECT
            COALESCE(SUM(CASE WHEN status = 'present' THEN 1 ELSE 0 END), 0) AS present_files,
            COALESCE(SUM(CASE WHEN status = 'missing' THEN 1 ELSE 0 END), 0) AS missing_files
        FROM file_inventory WHERE root_id = ?1
        "#,
    )
    .bind(root_id)
    .fetch_one(&mut *transaction)
    .await?;
    let updated = sqlx::query(
        r#"
        UPDATE library_roots
        SET present_files = ?4,
            missing_files = ?5,
            last_reconcile_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
            last_discovered = ?6,
            last_inserted = ?7,
            last_changed = ?8,
            last_missing = ?9,
            last_error = NULL
        WHERE id = ?1 AND generation = ?2 AND active_token = ?3 AND status = 'scanning'
        "#,
    )
    .bind(root_id)
    .bind(generation)
    .bind(token)
    .bind(counts.get::<i64, _>("present_files"))
    .bind(counts.get::<i64, _>("missing_files"))
    .bind(i64::try_from(run.discovered).unwrap_or(i64::MAX))
    .bind(i64::try_from(run.inserted).unwrap_or(i64::MAX))
    .bind(i64::try_from(run.changed).unwrap_or(i64::MAX))
    .bind(i64::try_from(newly_missing).unwrap_or(i64::MAX))
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    if updated != 1 {
        return Err(AppError::Other(format!(
            "inventory generation fence rejected missing finalize for root {root_id} generation {generation}"
        )));
    }
    transaction.commit().await?;
    Ok(newly_missing)
}

async fn complete_root_scan(
    db: &Db,
    root_id: i64,
    generation: i64,
    token: &str,
    _run: &InventoryRootRun,
) -> Result<()> {
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    let updated = sqlx::query(
        r#"
        UPDATE library_roots
        SET completed_generation = ?2,
            status = 'idle',
            active_token = NULL,
            scan_started_at = NULL,
            last_reconcile_at = COALESCE(last_reconcile_at, strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            last_error = NULL
        WHERE id = ?1 AND generation = ?2 AND active_token = ?3 AND status = 'scanning'
        "#,
    )
    .bind(root_id)
    .bind(generation)
    .bind(token)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    if updated != 1 {
        transaction.rollback().await?;
        return Err(AppError::Other(format!(
            "inventory generation fence rejected completion for root {root_id} generation {generation}"
        )));
    }
    transaction.commit().await?;
    Ok(())
}

// Kept for the shadow-inventory test helpers and callers that do not run the
// novel coordinator. The coordinator path uses the split missing/complete
// phases above so catalog writes remain inside the active root fence.
#[cfg(test)]
async fn finish_root_scan(
    db: &Db,
    root_id: i64,
    generation: i64,
    token: &str,
    run: &InventoryRootRun,
) -> Result<u64> {
    let newly_missing = mark_root_scan_missing(db, root_id, generation, token, run).await?;
    complete_root_scan(db, root_id, generation, token, run).await?;
    Ok(newly_missing)
}

async fn ensure_generation_fence(
    transaction: &mut sqlx_core::transaction::Transaction<'_, crate::Sqlite>,
    root_id: i64,
    generation: i64,
    token: &str,
) -> Result<()> {
    let valid = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT 1 FROM library_roots
        WHERE id = ?1 AND generation = ?2 AND active_token = ?3 AND status = 'scanning'
        "#,
    )
    .bind(root_id)
    .bind(generation)
    .bind(token)
    .fetch_optional(&mut **transaction)
    .await?;
    if valid.is_none() {
        return Err(AppError::Other(format!(
            "inventory generation fence rejected root {root_id} generation {generation}"
        )));
    }
    Ok(())
}

async fn mark_root_incomplete(
    db: &Db,
    root_id: i64,
    generation: i64,
    token: &str,
    run: &InventoryRootRun,
    error: &str,
) -> Result<()> {
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    sqlx::query(
        r#"
        UPDATE library_roots
        SET status = 'needs_reconcile',
            active_token = NULL,
            last_discovered = ?4,
            last_inserted = ?5,
            last_changed = ?6,
            last_missing = 0,
            last_error = ?7
        WHERE id = ?1 AND generation = ?2 AND active_token = ?3 AND status = 'scanning'
        "#,
    )
    .bind(root_id)
    .bind(generation)
    .bind(token)
    .bind(i64::try_from(run.discovered).unwrap_or(i64::MAX))
    .bind(i64::try_from(run.inserted).unwrap_or(i64::MAX))
    .bind(i64::try_from(run.changed).unwrap_or(i64::MAX))
    .bind(bounded_error(error))
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

fn bounded_error(value: &str) -> String {
    value.chars().take(MAX_DIAGNOSTIC_ERROR_BYTES).collect()
}

fn portable_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn redacted_root_key(root: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(root.as_bytes());
    let digest = format!("{:x}", hasher.finalize());
    digest[..24].to_string()
}

fn root_label(root: &str) -> String {
    Path::new(root)
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("media-root")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[tokio::test]
    async fn kind_scoped_root_sync_preserves_other_kinds() {
        let (temp, db) = test_db().await;
        let gallery_root = temp.path().join("gallery");
        let audio_root = temp.path().join("audio");
        let specs = vec![
            RootSpec {
                kind: "gallery".to_string(),
                provider: "local".to_string(),
                root: gallery_root.clone(),
                root_text: gallery_root.to_string_lossy().to_string(),
                scan_depth: None,
                device_class: "hdd".to_string(),
                audio_grouping: AudioGroupingMode::Auto,
            },
            RootSpec {
                kind: "audio".to_string(),
                provider: "local".to_string(),
                root: audio_root.clone(),
                root_text: audio_root.to_string_lossy().to_string(),
                scan_depth: None,
                device_class: "hdd".to_string(),
                audio_grouping: AudioGroupingMode::Auto,
            },
        ];
        let roots = sync_roots(&db, specs.clone(), None, None).await.unwrap();
        assert_eq!(roots.len(), 2);

        let gallery = specs.into_iter().next().unwrap();
        let scoped = sync_roots(&db, vec![gallery], Some("gallery"), None)
            .await
            .unwrap();
        assert_eq!(scoped.len(), 1);
        let statuses = sqlx::query_as::<_, (String, i64)>(
            "SELECT kind, enabled FROM library_roots ORDER BY kind",
        )
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(
            statuses,
            vec![("audio".to_string(), 1), ("gallery".to_string(), 1)]
        );
    }

    fn settings_for_local_roots(
        novel_root: &Path,
        comic_root: &Path,
        coser_picture_root: &Path,
        audio_root: &Path,
        gallery_root: &Path,
    ) -> AppSettings {
        serde_json::from_value(serde_json::json!({
            "theme": "light",
            "media_dirs": {
                "comics": [comic_root.to_string_lossy()],
                "novels": [novel_root.to_string_lossy()],
                "audio": [audio_root.to_string_lossy()],
                "gallery": [gallery_root.to_string_lossy()],
                "coser-picture": [coser_picture_root.to_string_lossy()]
            },
            "scan": {
                "enqueue_enrichment": false,
                "file_watcher": false,
                "enrichment_concurrency": 1
            },
            "openai": {
                "image_model": "test",
                "image_configured": false
            }
        }))
        .unwrap()
    }

    async fn insert_pending_event(db: &Db, root_id: i64, relative_path: &str) {
        sqlx::query(
            r#"
            INSERT INTO scan_events (
                root_id, relative_path, event_kind, work_key, observed_at, status
            ) VALUES (
                ?1, ?2, 'modify', ?3, strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'pending'
            )
            "#,
        )
        .bind(root_id)
        .bind(relative_path)
        .bind(relative_path)
        .execute(db.pool())
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn inventory_short_status_writes_use_the_single_writer_gate() {
        let (temp, db) = test_db().await;
        let root_id = add_root(&db, temp.path()).await;

        insert_pending_event(&db, root_id, "first.jpg").await;
        let claimed = claim_pending_events(&db, 8, None).await.unwrap();
        assert_eq!(claimed.len(), 1);
        let before_complete = db.write_snapshot();
        complete_event(&db, &claimed[0]).await.unwrap();
        let after_complete = db.write_snapshot();
        assert_eq!(after_complete.completed, before_complete.completed + 1);

        insert_pending_event(&db, root_id, "second.jpg").await;
        let claimed = claim_pending_events(&db, 8, None).await.unwrap();
        assert_eq!(claimed.len(), 1);
        let before_fail = db.write_snapshot();
        fail_event(&db, &claimed[0], "fixture failure", false)
            .await
            .unwrap();
        let after_fail = db.write_snapshot();
        assert_eq!(after_fail.completed, before_fail.completed + 1);

        let before_coordinator = db.write_snapshot();
        update_novel_coordinator_state(
            &db,
            root_id,
            4,
            "processing",
            Some(12),
            Some("fixture state"),
        )
        .await
        .unwrap();
        let after_coordinator = db.write_snapshot();
        assert_eq!(
            after_coordinator.completed,
            before_coordinator.completed + 1
        );
        let coordinator = sqlx::query_as::<_, (i64, String, i64, Option<String>)>(
            "SELECT generation, phase, checkpoint_seq, last_error FROM novel_coordinator_state WHERE root_id = ?1",
        )
        .bind(root_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(
            coordinator,
            (
                4,
                "processing".to_string(),
                12,
                Some("fixture state".to_string())
            )
        );

        let generation = begin_root_scan(&db, root_id, "status-write-test")
            .await
            .unwrap();
        let before_complete_root = db.write_snapshot();
        complete_root_scan(
            &db,
            root_id,
            generation,
            "status-write-test",
            &InventoryRootRun {
                root_id,
                kind: "gallery".to_string(),
                generation,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let after_complete_root = db.write_snapshot();
        assert_eq!(
            after_complete_root.completed,
            before_complete_root.completed + 1
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM library_roots WHERE id = ?1")
                .bind(root_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "idle"
        );
    }

    #[tokio::test]
    async fn kind_scoped_shadow_reconcile_does_not_touch_unselected_roots() {
        let (temp, db) = test_db().await;
        let novel_root = temp.path().join("novel");
        let comic_root = temp.path().join("comic");
        let coser_picture_root = temp.path().join("coser-picture");
        let audio_root = temp.path().join("audio");
        let gallery_root = temp.path().join("gallery");
        for root in [
            &novel_root,
            &comic_root,
            &coser_picture_root,
            &audio_root,
            &gallery_root,
        ] {
            std::fs::create_dir_all(root).unwrap();
        }
        let settings = settings_for_local_roots(
            &novel_root,
            &comic_root,
            &coser_picture_root,
            &audio_root,
            &gallery_root,
        );
        sqlx::query(
            "INSERT INTO library_roots(kind, provider, root, enabled, status) VALUES ('comic', 'local', ?1, 1, 'idle')",
        )
        .bind(comic_root.to_string_lossy().to_string())
        .execute(db.pool())
        .await
        .unwrap();
        let enabled = ["novel".to_string()].into_iter().collect();
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let summary = reconcile_all_shadow_for_kinds(
            &db,
            &resources,
            &settings,
            &temp.path().join("generated"),
            Arc::new(AtomicBool::new(true)),
            &enabled,
        )
        .await
        .unwrap();

        assert_eq!(summary.roots, 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM library_roots")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT kind FROM library_roots WHERE kind = 'novel'")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "novel"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT enabled FROM library_roots WHERE kind = 'comic'",)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            1,
            "a kind-scoped shadow pass must preserve other inventory roots"
        );
    }

    #[tokio::test]
    async fn pending_event_claim_is_limited_to_enabled_kinds() {
        let (temp, db) = test_db().await;
        let novel_root = temp.path().join("novel");
        let comic_root = temp.path().join("comic");
        std::fs::create_dir_all(&novel_root).unwrap();
        std::fs::create_dir_all(&comic_root).unwrap();
        let novel_id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO library_roots(kind, provider, root, enabled, status) VALUES ('novel', 'local', ?1, 1, 'idle') RETURNING id",
        )
        .bind(novel_root.to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
        let comic_id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO library_roots(kind, provider, root, enabled, status) VALUES ('comic', 'local', ?1, 1, 'idle') RETURNING id",
        )
        .bind(comic_root.to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
        insert_pending_event(&db, novel_id, "book.epub").await;
        insert_pending_event(&db, comic_id, "book.cbz").await;

        let enabled = ["novel".to_string()].into_iter().collect();
        let claimed = claim_pending_events(&db, 8, Some(&enabled)).await.unwrap();

        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].root.kind, "novel");
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM scan_events WHERE event_kind = 'modify' AND status = 'processing'",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM scan_events AS event JOIN library_roots AS root ON root.id = event.root_id WHERE root.kind = 'comic' AND event.status = 'pending'",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn coordinator_queue_status_helpers_use_bounded_read_snapshots() {
        let (temp, db) = test_db().await;
        let root = temp.path().join("queue-status-novel");
        std::fs::create_dir_all(&root).unwrap();
        let root_id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO library_roots(kind, provider, root, enabled, status) VALUES ('novel', 'local', ?1, 1, 'idle') RETURNING id",
        )
        .bind(root.to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
        let before = db.runtime_snapshot().await.read_snapshot;

        let roots = pending_catalog_event_root_ids(&db, "novel").await.unwrap();
        let (terminal, error) = terminal_catalog_event_status(&db, root_id).await.unwrap();
        let phase = catalog_event_queue_phase(&db, root_id).await.unwrap();

        let after = db.runtime_snapshot().await.read_snapshot;
        assert!(roots.is_empty());
        assert_eq!(terminal, 0);
        assert!(error.is_none());
        assert_eq!(phase, "idle");
        assert_eq!(after.active, 0);
        assert_eq!(after.samples, before.samples + 3);
        assert_eq!(after.completed, before.completed + 3);
        assert_eq!(after.implicit_rollbacks, before.implicit_rollbacks);
    }

    #[tokio::test]
    async fn coordinator_dispatch_uses_one_ownership_snapshot_for_all_kinds() {
        let (temp, db) = test_db().await;
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let before = db.runtime_snapshot().await.read_snapshot;
        let summary = process_pending_events(&db, &resources, &temp.path().join("generated"), 16)
            .await
            .unwrap();
        let after = db.runtime_snapshot().await.read_snapshot;

        assert_eq!(summary.claimed, 0);
        assert_eq!(summary.catalog_completed, 0);
        assert_eq!(summary.catalog_failed, 0);
        assert_eq!(
            after.completed.saturating_sub(before.completed),
            1,
            "coordinator dispatch must read all five ownership flags in one snapshot"
        );
        assert_eq!(
            after.implicit_rollbacks, before.implicit_rollbacks,
            "coordinator ownership snapshot must commit explicitly"
        );
    }

    #[tokio::test]
    async fn unselected_events_stay_on_the_legacy_scan_path() {
        let (temp, db) = test_db().await;
        let novel_root = temp.path().join("novel");
        let comic_root = temp.path().join("comic");
        let coser_picture_root = temp.path().join("coser-picture");
        let audio_root = temp.path().join("audio");
        let gallery_root = temp.path().join("gallery");
        let settings = settings_for_local_roots(
            &novel_root,
            &comic_root,
            &coser_picture_root,
            &audio_root,
            &gallery_root,
        );
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind IN ('novel', 'comic')",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let enabled = ["novel".to_string()].into_iter().collect();

        assert!(!event_requires_legacy_scan_for_kinds(
            &db,
            &settings,
            std::slice::from_ref(&novel_root.join("book.epub")),
            &enabled,
        )
        .await
        .unwrap());
        assert!(event_requires_legacy_scan_for_kinds(
            &db,
            &settings,
            std::slice::from_ref(&comic_root.join("book.cbz")),
            &enabled,
        )
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn watcher_ownership_check_uses_one_tracked_snapshot_for_all_kinds() {
        let (temp, db) = test_db().await;
        let novel_root = temp.path().join("novel");
        let comic_root = temp.path().join("comic");
        let coser_picture_root = temp.path().join("coser-picture");
        let audio_root = temp.path().join("audio");
        let gallery_root = temp.path().join("gallery");
        let settings = settings_for_local_roots(
            &novel_root,
            &comic_root,
            &coser_picture_root,
            &audio_root,
            &gallery_root,
        );
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind IN ('novel', 'comic', 'coser-picture', 'audio', 'gallery')",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let before = db.runtime_snapshot().await.read_snapshot;
        let enabled = [
            "novel".to_string(),
            "comic".to_string(),
            "coser-picture".to_string(),
            "audio".to_string(),
            "gallery".to_string(),
        ]
        .into_iter()
        .collect();
        assert!(!event_requires_legacy_scan_for_kinds(
            &db,
            &settings,
            std::slice::from_ref(&novel_root.join("book.epub")),
            &enabled,
        )
        .await
        .unwrap());
        let after = db.runtime_snapshot().await.read_snapshot;
        assert_eq!(
            after.completed.saturating_sub(before.completed),
            1,
            "watcher ownership must use one read snapshot for the five kinds"
        );
        assert_eq!(
            after.implicit_rollbacks, before.implicit_rollbacks,
            "watcher ownership snapshot must commit explicitly"
        );
    }

    #[tokio::test]
    async fn present_work_inventory_covering_index_is_usable_for_reads() {
        let (_temp, db) = test_db().await;
        let keyset_plan = sqlx::query(
            r#"
            EXPLAIN QUERY PLAN
            SELECT DISTINCT work_key
            FROM file_inventory INDEXED BY idx_inventory_present_work_cover
            WHERE root_id = ?1
              AND status = 'present'
              AND work_key IS NOT NULL
              AND lower(relative_path) GLOB '*.jpg'
              AND (?2 IS NULL OR work_key > ?2)
            ORDER BY work_key
            LIMIT 256
            "#,
        )
        .bind(1_i64)
        .bind(Option::<String>::None)
        .fetch_all(db.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>()
        .join("\n");
        assert!(
            keyset_plan.contains("COVERING INDEX idx_inventory_present_work_cover"),
            "unexpected inventory keyset plan: {keyset_plan}"
        );

        let asset_plan = sqlx::query(
            r#"
            EXPLAIN QUERY PLAN
            SELECT relative_path, size, fast_fingerprint, file_id
            FROM file_inventory INDEXED BY idx_inventory_present_work_cover
            WHERE root_id = ?1 AND work_key = ?2 AND status = 'present'
            ORDER BY relative_path
            LIMIT 20001
            "#,
        )
        .bind(1_i64)
        .bind("author-0")
        .fetch_all(db.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>()
        .join("\n");
        assert!(
            asset_plan.contains("COVERING INDEX idx_inventory_present_work_cover"),
            "unexpected inventory asset plan: {asset_plan}"
        );
    }

    #[tokio::test]
    async fn catalog_event_inventory_loaders_fail_closed_at_per_work_limits() {
        let (temp, db) = test_db().await;
        let audio_root = sqlx::query_scalar::<_, i64>(
            "INSERT INTO library_roots(kind, provider, root) VALUES ('audio', 'local', ?1) RETURNING id",
        )
        .bind(temp.path().join("audio").to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
        let gallery_root = sqlx::query_scalar::<_, i64>(
            "INSERT INTO library_roots(kind, provider, root) VALUES ('gallery', 'local', ?1) RETURNING id",
        )
        .bind(temp.path().join("gallery").to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();

        let mut transaction = db.pool().begin().await.unwrap();
        for index in 0..=audio_inspector::MAX_AUDIO_FILES_PER_WORK {
            let relative_path = format!("RJ000001/track-{index:05}.mp3");
            sqlx::query(
                r#"
                INSERT INTO file_inventory (
                    root_id, relative_path, parent_key, media_class, size, mtime_ns,
                    fast_fingerprint, work_key, seen_generation, status
                ) VALUES (?1, ?2, 'RJ000001', 'audio', 1024, ?3, ?4, 'RJ000001', 1, 'present')
                "#,
            )
            .bind(audio_root)
            .bind(relative_path)
            .bind(i64::try_from(index).unwrap())
            .bind(format!("audio-{index}"))
            .execute(&mut *transaction)
            .await
            .unwrap();
        }
        for index in 0..=gallery_inspector::MAX_GALLERY_FILES_PER_WORK {
            let relative_path = format!("author/image-{index:05}.jpg");
            sqlx::query(
                r#"
                INSERT INTO file_inventory (
                    root_id, relative_path, parent_key, media_class, size, mtime_ns,
                    fast_fingerprint, work_key, seen_generation, status
                ) VALUES (?1, ?2, 'author', 'image', 1024, ?3, ?4, 'author', 1, 'present')
                "#,
            )
            .bind(gallery_root)
            .bind(relative_path)
            .bind(i64::try_from(index).unwrap())
            .bind(format!("gallery-{index}"))
            .execute(&mut *transaction)
            .await
            .unwrap();
        }
        transaction.commit().await.unwrap();

        let audio_error = load_audio_catalog_paths(&db, audio_root, "RJ000001")
            .await
            .unwrap_err();
        assert!(matches!(
            audio_error,
            AppError::Other(message) if message.contains("exceeds the 20000 file safety limit")
        ));

        let gallery_error = load_gallery_catalog_assets(&db, gallery_root, "author", "present")
            .await
            .unwrap_err();
        assert!(matches!(
            gallery_error,
            AppError::Other(message) if message.contains("exceeds the 20000 file safety limit")
        ));
    }

    async fn add_root(db: &Db, root: &Path) -> i64 {
        sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots(kind, provider, root, enabled, status)
            VALUES ('gallery', 'local', ?1, 1, 'idle')
            RETURNING id
            "#,
        )
        .bind(root.to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap()
    }

    fn synthetic_row(index: usize, changed: bool) -> InventoryRow {
        let relative_path = format!("author-{}/image-{index:07}.jpg", index / 1000);
        let parent_key = format!("author-{}", index / 1000);
        let size = 1024 + index as i64 + i64::from(changed);
        let mtime_ns = 1_700_000_000_000_000_000_i64 + index as i64;
        InventoryRow {
            fast_fingerprint: fast_fingerprint("image", size, mtime_ns, None),
            relative_path,
            parent_key: parent_key.clone(),
            media_class: "image".to_string(),
            size,
            mtime_ns,
            file_id: None,
            work_key: parent_key,
        }
    }

    #[test]
    fn configured_roots_register_qmediasync_archive_and_audio_sources() {
        let settings: AppSettings = serde_json::from_value(serde_json::json!({
            "theme": "light",
            "media_dirs": {
                "comics": [],
                "novels": [],
                "audio": [],
                "gallery": [],
                "coser-picture": []
            },
            "media_sources": [
                {
                    "kind": "comic",
                    "provider": "qmediasync",
                    "root": "/nas/qms-comics",
                    "mount_name": "comics",
                    "enabled": true,
                    "scan_depth": 12
                },
                {
                    "kind": "coser-picture",
                    "provider": "qmediasync",
                    "root": "/nas/qms-coser",
                    "mount_name": "coser",
                    "enabled": true,
                    "scan_depth": 8
                },
                {
                    "kind": "audio",
                    "provider": "qmediasync",
                    "root": "/nas/qms-audio",
                    "mount_name": "audio",
                    "enabled": true,
                    "scan_depth": 8
                }
            ],
            "scan": {
                "enqueue_enrichment": false,
                "file_watcher": false,
                "enrichment_concurrency": 1
            },
            "openai": {
                "image_model": "test",
                "image_configured": false
            }
        }))
        .unwrap();

        let roots = configured_roots(&settings);

        assert_eq!(roots.len(), 3);
        assert!(roots.iter().any(|root| {
            root.kind == "comic"
                && root.provider == "qmediasync:comics"
                && root.scan_depth == Some(12)
        }));
        assert!(roots.iter().any(|root| {
            root.kind == "coser-picture"
                && root.provider == "qmediasync:coser"
                && root.scan_depth == Some(8)
        }));
        assert!(roots.iter().any(|root| {
            root.kind == "audio"
                && root.provider == "qmediasync:audio"
                && root.scan_depth == Some(8)
        }));
    }

    #[tokio::test]
    async fn coordinator_fails_closed_when_catalog_ownership_rolls_back() {
        let (temp, db) = test_db().await;
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots(kind, provider, root, enabled, status)
            VALUES ('novel', 'local', ?1, 1, 'idle')
            RETURNING id
            "#,
        )
        .bind(temp.path().to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
        let token = "ownership-rollback";

        // Legacy ownership must not be able to acquire a Catalog v2 lease.
        assert!(
            acquire_novel_coordinator_lease(&db, root_id, token, "novel")
                .await
                .unwrap()
                .is_none()
        );

        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'novel'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let lease = acquire_novel_coordinator_lease(&db, root_id, token, "novel")
            .await
            .unwrap()
            .expect("Catalog v2 ownership should allow a lease");

        sqlx::query(
            r#"
            INSERT INTO scan_events(
                root_id, relative_path, event_kind, work_key, observed_at, status
            )
            VALUES (?1, 'book.epub', 'catalog-upsert', 'book.epub',
                    strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'pending')
            "#,
        )
        .bind(root_id)
        .execute(db.pool())
        .await
        .unwrap();

        // Simulate an administrative rollback that happened after lease
        // acquisition. The claim fence must refuse the queued v2 event.
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'legacy' WHERE kind = 'novel'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let claimed = claim_novel_catalog_events_under_fence(
            &db,
            root_id,
            lease.generation,
            &lease.token,
            16,
        )
        .await
        .unwrap();
        assert!(claimed.is_empty());
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT status FROM scan_events WHERE root_id = ?1 AND event_kind = 'catalog-upsert'",
            )
            .bind(root_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "pending"
        );

        release_novel_coordinator_lease(&db, &lease, false, None, None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn incomplete_generation_never_marks_unseen_files_missing() {
        let (temp, db) = test_db().await;
        let root_id = add_root(&db, temp.path()).await;
        let first_token = "first";
        let first_generation = begin_root_scan(&db, root_id, first_token).await.unwrap();
        let first_rows = vec![synthetic_row(1, false), synthetic_row(2, false)];
        let first_stats =
            upsert_inventory_batch(&db, root_id, first_generation, first_token, &first_rows)
                .await
                .unwrap();
        let first_run = InventoryRootRun {
            discovered: first_stats.discovered,
            inserted: first_stats.inserted,
            changed: first_stats.changed,
            ..Default::default()
        };
        finish_root_scan(&db, root_id, first_generation, first_token, &first_run)
            .await
            .unwrap();

        let interrupted_token = "interrupted";
        let interrupted_generation = begin_root_scan(&db, root_id, interrupted_token)
            .await
            .unwrap();
        let partial = vec![synthetic_row(1, false)];
        let partial_stats = upsert_inventory_batch(
            &db,
            root_id,
            interrupted_generation,
            interrupted_token,
            &partial,
        )
        .await
        .unwrap();
        let partial_run = InventoryRootRun {
            discovered: partial_stats.discovered,
            inserted: partial_stats.inserted,
            changed: partial_stats.changed,
            ..Default::default()
        };
        mark_root_incomplete(
            &db,
            root_id,
            interrupted_generation,
            interrupted_token,
            &partial_run,
            "simulated interruption",
        )
        .await
        .unwrap();
        let statuses = sqlx::query_scalar::<_, String>(
            "SELECT status FROM file_inventory WHERE root_id = ?1 ORDER BY relative_path",
        )
        .bind(root_id)
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(statuses, vec!["present", "present"]);

        let final_token = "final";
        let final_generation = begin_root_scan(&db, root_id, final_token).await.unwrap();
        let final_stats =
            upsert_inventory_batch(&db, root_id, final_generation, final_token, &partial)
                .await
                .unwrap();
        let final_run = InventoryRootRun {
            discovered: final_stats.discovered,
            inserted: final_stats.inserted,
            changed: final_stats.changed,
            ..Default::default()
        };
        let missing = finish_root_scan(&db, root_id, final_generation, final_token, &final_run)
            .await
            .unwrap();
        assert_eq!(missing, 1);
    }

    #[tokio::test]
    async fn interrupted_coordinator_recovery_requeues_events_and_invalidates_the_lease() {
        let (temp, db) = test_db().await;
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots(
                kind, provider, root, enabled, status, generation,
                completed_generation, active_token
            )
            VALUES ('novel', 'local', ?1, 1, 'scanning', 7, 6, 'stale-token')
            RETURNING id
            "#,
        )
        .bind(temp.path().to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
        let catalog_seq = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO scan_events(
                root_id, relative_path, event_kind, work_key,
                observed_at, status, attempts
            )
            VALUES (?1, 'book.epub', 'catalog-upsert', 'book.epub',
                    strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'processing', 2)
            RETURNING seq
            "#,
        )
        .bind(root_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        let watcher_seq = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO scan_events(
                root_id, relative_path, event_kind, work_key,
                observed_at, status, attempts
            )
            VALUES (?1, 'book.epub', 'modify', 'book.epub',
                    strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'processing', 1)
            RETURNING seq
            "#,
        )
        .bind(root_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO novel_coordinator_state(root_id, generation, phase, checkpoint_seq) VALUES (?1, 7, 'processing', ?2)",
        )
        .bind(root_id)
        .bind(catalog_seq)
        .execute(db.pool())
        .await
        .unwrap();

        // The newer watcher event supersedes the catalog event that was
        // already in flight; only that newest row is requeued.
        assert_eq!(recover_interrupted_coordinator(&db).await.unwrap(), 1);
        let events = sqlx::query(
            "SELECT seq, status, attempts FROM scan_events WHERE root_id = ?1 ORDER BY seq",
        )
        .bind(root_id)
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].get::<i64, _>("seq"), catalog_seq);
        assert_eq!(events[0].get::<String, _>("status"), "done");
        assert_eq!(events[0].get::<i64, _>("attempts"), 2);
        assert_eq!(events[1].get::<i64, _>("seq"), watcher_seq);
        assert_eq!(events[1].get::<String, _>("status"), "pending");

        let root_status: (String, Option<String>) =
            sqlx::query_as("SELECT status, active_token FROM library_roots WHERE id = ?1")
                .bind(root_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(root_status.0, "needs_reconcile");
        assert!(root_status.1.is_none());
        let coordinator: (String, i64) = sqlx::query_as(
            "SELECT phase, checkpoint_seq FROM novel_coordinator_state WHERE root_id = ?1",
        )
        .bind(root_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(coordinator.0, "needs_reconcile");
        assert_eq!(coordinator.1, catalog_seq);

        // Recovery is idempotent; a second startup must not increment retry
        // attempts or duplicate the durable queue.
        assert_eq!(recover_interrupted_coordinator(&db).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn catalog_queue_overflow_commits_needs_reconcile_instead_of_rolling_it_back() {
        let (temp, db) = test_db().await;
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots(kind, provider, root, enabled, status)
            VALUES ('novel', 'local', ?1, 1, 'idle')
            RETURNING id
            "#,
        )
        .bind(temp.path().to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
        let mut transaction = db.pool().begin().await.unwrap();
        let overflow =
            ensure_catalog_queue_capacity(&mut transaction, root_id, CHANGED_KEY_LIMIT + 1, 0)
                .await
                .unwrap();
        let message = overflow.expect("capacity check must report the overflow");
        transaction.commit().await.unwrap();
        assert!(message.contains("capacity exceeded"));

        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM library_roots WHERE id = ?1")
                .bind(root_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "needs_reconcile"
        );
        let coordinator: (String, i64, String) = sqlx::query_as(
            "SELECT phase, pending_keys, last_error FROM novel_coordinator_state WHERE root_id = ?1",
        )
        .bind(root_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(coordinator.0, "needs_reconcile");
        assert_eq!(coordinator.1, CHANGED_KEY_LIMIT + 1);
        assert!(coordinator.2.contains("capacity exceeded"));
    }

    #[tokio::test]
    async fn coordinator_checkpoint_resets_at_a_new_root_generation() {
        let (temp, db) = test_db().await;
        let root_id = add_root(&db, temp.path()).await;
        update_novel_coordinator_state(&db, root_id, 4, "processing", Some(99), None)
            .await
            .unwrap();
        update_novel_coordinator_state(&db, root_id, 5, "processing", None, None)
            .await
            .unwrap();
        let state: (i64, i64) = sqlx::query_as(
            "SELECT generation, checkpoint_seq FROM novel_coordinator_state WHERE root_id = ?1",
        )
        .bind(root_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(state, (5, 0));
    }

    #[tokio::test]
    async fn stale_generation_cannot_write_or_finish() {
        let (temp, db) = test_db().await;
        let root_id = add_root(&db, temp.path()).await;
        let old_generation = begin_root_scan(&db, root_id, "old").await.unwrap();
        let new_generation = begin_root_scan(&db, root_id, "new").await.unwrap();
        assert!(new_generation > old_generation);
        let stale = upsert_inventory_batch(
            &db,
            root_id,
            old_generation,
            "old",
            &[synthetic_row(1, false)],
        )
        .await;
        assert!(stale.unwrap_err().to_string().contains("generation fence"));
    }

    #[tokio::test]
    async fn bounded_batches_report_unchanged_rows_on_the_second_generation() {
        let (temp, db) = test_db().await;
        let root_id = add_root(&db, temp.path()).await;
        let total = 20_000_usize;
        let mut max_batch = 0;
        let mut max_serialized = 0;
        let first_generation = begin_root_scan(&db, root_id, "first").await.unwrap();
        let mut first_run = InventoryRootRun::default();
        for start in (0..total).step_by(INVENTORY_BATCH_SIZE) {
            let end = (start + INVENTORY_BATCH_SIZE).min(total);
            let batch = (start..end)
                .map(|index| synthetic_row(index, false))
                .collect::<Vec<_>>();
            max_batch = max_batch.max(batch.len());
            let stats = upsert_inventory_batch(&db, root_id, first_generation, "first", &batch)
                .await
                .unwrap();
            max_serialized = max_serialized.max(stats.serialized_bytes);
            first_run.discovered += stats.discovered;
            first_run.inserted += stats.inserted;
            first_run.changed += stats.changed;
        }
        finish_root_scan(&db, root_id, first_generation, "first", &first_run)
            .await
            .unwrap();
        assert_eq!(first_run.inserted, total as u64);

        let second_generation = begin_root_scan(&db, root_id, "second").await.unwrap();
        let mut unchanged = 0_u64;
        for start in (0..total).step_by(INVENTORY_BATCH_SIZE) {
            let end = (start + INVENTORY_BATCH_SIZE).min(total);
            let batch = (start..end)
                .map(|index| synthetic_row(index, false))
                .collect::<Vec<_>>();
            let stats = upsert_inventory_batch(&db, root_id, second_generation, "second", &batch)
                .await
                .unwrap();
            unchanged += stats.unchanged;
        }
        assert_eq!(unchanged, total as u64);
        assert_eq!(max_batch, INVENTORY_BATCH_SIZE);
        assert!(max_serialized < 1024 * 1024);
    }

    #[tokio::test]
    async fn watcher_journal_collapses_duplicate_work_events_and_ignores_other_roots() {
        let (temp, db) = test_db().await;
        let gallery_root = temp.path().join("gallery");
        let other_root = temp.path().join("other");
        std::fs::create_dir_all(gallery_root.join("alice")).unwrap();
        std::fs::create_dir_all(&other_root).unwrap();
        let spec = RootSpec {
            kind: "gallery".to_string(),
            provider: "local".to_string(),
            root_text: gallery_root.to_string_lossy().to_string(),
            root: gallery_root.clone(),
            scan_depth: None,
            device_class: "hdd".to_string(),
            audio_grouping: AudioGroupingMode::Auto,
        };
        let first = gallery_root.join("alice").join("1.jpg");
        let second = gallery_root.join("alice").join("2.jpg");
        let outside = other_root.join("ignored.jpg");
        assert_eq!(
            journal_paths_for_specs(&db, vec![spec.clone()], "create", &[first, outside],)
                .await
                .unwrap(),
            1
        );
        journal_paths_for_specs(&db, vec![spec], "remove", &[second])
            .await
            .unwrap();
        let events = sqlx::query(
            "SELECT relative_path, event_kind, work_key FROM scan_events WHERE status = 'pending'",
        )
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].get::<String, _>("relative_path"), "alice/2.jpg");
        assert_eq!(events[0].get::<String, _>("event_kind"), "remove");
        assert_eq!(events[0].get::<String, _>("work_key"), "alice");
    }

    #[tokio::test]
    async fn targeted_gallery_events_handle_rename_delete_and_do_not_walk_other_authors() {
        let (temp, db) = test_db().await;
        let gallery_root = temp.path().join("gallery");
        let alice_dir = gallery_root.join("alice");
        let bob_dir = gallery_root.join("bob");
        std::fs::create_dir_all(&alice_dir).unwrap();
        std::fs::create_dir_all(&bob_dir).unwrap();
        let first = alice_dir.join("1.jpg");
        let renamed = alice_dir.join("2.jpg");
        let bob = bob_dir.join("never-visited.jpg");
        std::fs::write(&first, b"alice").unwrap();
        std::fs::write(&bob, b"bob").unwrap();
        let spec = RootSpec {
            kind: "gallery".to_string(),
            provider: "local".to_string(),
            root_text: gallery_root.to_string_lossy().to_string(),
            root: gallery_root.clone(),
            scan_depth: None,
            device_class: "hdd".to_string(),
            audio_grouping: AudioGroupingMode::Auto,
        };
        let resources = ResourceGovernor::standard();

        journal_paths_for_specs(
            &db,
            vec![spec.clone()],
            "create",
            std::slice::from_ref(&first),
        )
        .await
        .unwrap();
        let generated_dir = temp.path().join("generated");
        let first_summary = process_pending_events(&db, &resources, &generated_dir, 16)
            .await
            .unwrap();
        assert_eq!(first_summary.completed, 1);
        assert_eq!(first_summary.discovered, 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM file_inventory")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            1,
            "the alice event must not walk bob"
        );

        std::fs::rename(&first, &renamed).unwrap();
        journal_paths_for_specs(
            &db,
            vec![spec.clone()],
            "rename",
            &[first.clone(), renamed.clone()],
        )
        .await
        .unwrap();
        let rename_summary = process_pending_events(&db, &resources, &generated_dir, 16)
            .await
            .unwrap();
        assert_eq!(rename_summary.completed, 1);
        assert_eq!(rename_summary.newly_missing, 1);
        let rows =
            sqlx::query("SELECT relative_path, status FROM file_inventory ORDER BY relative_path")
                .fetch_all(db.pool())
                .await
                .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get::<String, _>("relative_path"), "alice/1.jpg");
        assert_eq!(rows[0].get::<String, _>("status"), "missing");
        assert_eq!(rows[1].get::<String, _>("relative_path"), "alice/2.jpg");
        assert_eq!(rows[1].get::<String, _>("status"), "present");

        std::fs::remove_file(&renamed).unwrap();
        std::fs::remove_dir(&alice_dir).unwrap();
        journal_paths_for_specs(&db, vec![spec], "remove-folder", &[alice_dir])
            .await
            .unwrap();
        let remove_summary = process_pending_events(&db, &resources, &generated_dir, 16)
            .await
            .unwrap();
        assert_eq!(remove_summary.completed, 1);
        assert_eq!(remove_summary.newly_missing, 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM file_inventory WHERE status = 'present'",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn gallery_parent_folder_delete_tombstones_nested_work_keys() {
        let (temp, db) = test_db().await;
        let gallery_root = temp.path().join("gallery-nested");
        let author_dir = gallery_root.join("author");
        let first_dir = author_dir.join("set-a");
        let second_dir = author_dir.join("set-b");
        std::fs::create_dir_all(&first_dir).unwrap();
        std::fs::create_dir_all(&second_dir).unwrap();
        let first = first_dir.join("001.jpg");
        let second = second_dir.join("001.jpg");
        std::fs::write(&first, b"first").unwrap();
        std::fs::write(&second, b"second").unwrap();
        let spec = RootSpec {
            kind: "gallery".to_string(),
            provider: "local".to_string(),
            root_text: gallery_root.to_string_lossy().to_string(),
            root: gallery_root.clone(),
            scan_depth: None,
            device_class: "hdd".to_string(),
            audio_grouping: AudioGroupingMode::Auto,
        };
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'gallery'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let generated = temp.path().join("generated");

        journal_paths_for_specs(
            &db,
            vec![spec.clone()],
            "create-folder",
            std::slice::from_ref(&author_dir),
        )
        .await
        .unwrap();
        let created = process_pending_events(&db, &resources, &generated, 32)
            .await
            .unwrap();
        assert_eq!(created.completed, 1);
        assert_eq!(created.catalog_completed, 2);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM works WHERE kind = 'gallery' AND deleted_at IS NULL",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            2
        );

        std::fs::remove_file(&first).unwrap();
        std::fs::remove_file(&second).unwrap();
        std::fs::remove_dir(&first_dir).unwrap();
        std::fs::remove_dir(&second_dir).unwrap();
        std::fs::remove_dir(&author_dir).unwrap();
        let journaled = journal_paths_for_specs(
            &db,
            vec![spec],
            "remove-folder",
            std::slice::from_ref(&author_dir),
        )
        .await
        .unwrap();
        assert_eq!(journaled, 1);
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT status FROM library_roots WHERE kind = 'gallery' AND root = ?1",
            )
            .bind(gallery_root.to_string_lossy().to_string())
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "idle"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM scan_events WHERE status = 'pending' AND event_kind = 'remove-folder' AND work_key = 'author'",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1
        );
        let removed = process_pending_events(&db, &resources, &generated, 32)
            .await
            .unwrap();
        let remove_error = sqlx::query_scalar::<_, Option<String>>(
            "SELECT last_error FROM scan_events WHERE event_kind = 'remove-folder' AND work_key = 'author' ORDER BY seq DESC LIMIT 1",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(
            removed.completed, 1,
            "summary={removed:?}, error={remove_error:?}"
        );
        assert_eq!(removed.newly_missing, 2);
        assert_eq!(removed.catalog_completed, 2);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM works WHERE kind = 'gallery' AND deleted_at IS NOT NULL",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            2
        );
        assert_eq!(resources.snapshot().pools["scan_io"].used, 0);
    }

    #[tokio::test]
    async fn full_gallery_reconcile_upserts_and_tombstones_catalog_work() {
        let (temp, db) = test_db().await;
        let gallery_root = temp.path().join("gallery-full");
        let artist_dir = gallery_root.join("artist");
        std::fs::create_dir_all(&artist_dir).unwrap();
        let first = artist_dir.join("001.jpg");
        let second = artist_dir.join("002.png");
        std::fs::write(&first, b"first-image").unwrap();
        std::fs::write(&second, b"second-image").unwrap();
        let root_id = add_root(&db, &gallery_root).await;
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'gallery'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let inventory_root = InventoryRoot {
            id: root_id,
            spec: RootSpec {
                kind: "gallery".to_string(),
                provider: "local".to_string(),
                root: gallery_root.clone(),
                root_text: gallery_root.to_string_lossy().to_string(),
                scan_depth: None,
                device_class: "hdd".to_string(),
                audio_grouping: AudioGroupingMode::Auto,
            },
        };
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let generated = temp.path().join("generated");

        let first_run = reconcile_root(
            &db,
            &resources,
            inventory_root.clone(),
            &generated,
            Arc::new(AtomicBool::new(true)),
        )
        .await
        .unwrap();
        assert!(first_run.complete);
        assert_eq!(first_run.discovered, 2);
        let work_id = sqlx::query_scalar::<_, i64>(
            "SELECT work_id FROM catalog_work_sources WHERE kind = 'gallery' AND root_id = ?1 AND work_key = 'artist'",
        )
        .bind(root_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM assets WHERE work_id = ?1 AND role = 'image'",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            2
        );

        std::fs::remove_file(&first).unwrap();
        std::fs::remove_file(&second).unwrap();
        std::fs::remove_dir(&artist_dir).unwrap();
        let second_run = reconcile_root(
            &db,
            &resources,
            inventory_root,
            &generated,
            Arc::new(AtomicBool::new(true)),
        )
        .await
        .unwrap();
        assert!(second_run.complete);
        assert_eq!(second_run.newly_missing, 2);
        assert!(sqlx::query_scalar::<_, Option<String>>(
            "SELECT deleted_at FROM works WHERE id = ?1",
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap()
        .is_some());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM scanner_assets WHERE work_id = ?1",)
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            0
        );
        assert_eq!(resources.snapshot().pools["scan_io"].used, 0);
        assert_eq!(resources.snapshot().pools["processing_memory"].used, 0);
    }

    #[tokio::test]
    async fn targeted_gallery_catalog_preserves_identity_and_old_work_on_failure() {
        let (temp, db) = test_db().await;
        let gallery_root = temp.path().join("gallery-catalog");
        let old_dir = gallery_root.join("old-artist");
        let new_dir = gallery_root.join("new-artist");
        std::fs::create_dir_all(&old_dir).unwrap();
        let first = old_dir.join("001.jpg");
        let second = old_dir.join("002.jpg");
        std::fs::write(&first, b"alpha").unwrap();
        std::fs::write(&second, b"beta").unwrap();
        let spec = RootSpec {
            kind: "gallery".to_string(),
            provider: "local".to_string(),
            root_text: gallery_root.to_string_lossy().to_string(),
            root: gallery_root.clone(),
            scan_depth: None,
            device_class: "hdd".to_string(),
            audio_grouping: AudioGroupingMode::Auto,
        };
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'gallery'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let generated = temp.path().join("generated");

        journal_paths_for_specs(
            &db,
            vec![spec.clone()],
            "create",
            std::slice::from_ref(&first),
        )
        .await
        .unwrap();
        let created = process_pending_events(&db, &resources, &generated, 32)
            .await
            .unwrap();
        assert_eq!(created.completed, 1);
        assert_eq!(created.catalog_completed, 1);
        let root_id = sqlx::query_scalar::<_, i64>(
            "SELECT id FROM library_roots WHERE kind = 'gallery' AND root = ?1",
        )
        .bind(gallery_root.to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
        let work_id = sqlx::query_scalar::<_, i64>(
            "SELECT work_id FROM catalog_work_sources WHERE kind = 'gallery' AND root_id = ?1 AND work_key = 'old-artist'",
        )
        .bind(root_id)
        .fetch_one(db.pool())
        .await
        .unwrap();

        std::fs::write(&first, b"alpha-expanded").unwrap();
        journal_paths_for_specs(
            &db,
            vec![spec.clone()],
            "modify",
            std::slice::from_ref(&first),
        )
        .await
        .unwrap();
        process_pending_events(&db, &resources, &generated, 32)
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT work_id FROM catalog_work_sources WHERE kind = 'gallery' AND root_id = ?1 AND work_key = 'old-artist'",
            )
            .bind(root_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            work_id
        );

        std::fs::rename(&old_dir, &new_dir).unwrap();
        journal_paths_for_specs(
            &db,
            vec![spec.clone()],
            "rename",
            &[old_dir.clone(), new_dir.clone()],
        )
        .await
        .unwrap();
        let renamed = process_pending_events(&db, &resources, &generated, 32)
            .await
            .unwrap();
        assert_eq!(renamed.completed, 2);
        assert_eq!(renamed.catalog_completed, 2);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT work_id FROM catalog_work_sources WHERE kind = 'gallery' AND root_id = ?1 AND work_key = 'new-artist'",
            )
            .bind(root_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            work_id
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM catalog_work_sources WHERE kind = 'gallery' AND root_id = ?1 AND work_key = 'old-artist'",
            )
            .bind(root_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            0
        );

        let new_first = new_dir.join("001.jpg");
        let new_second = new_dir.join("002.jpg");
        std::fs::remove_file(&new_first).unwrap();
        std::fs::remove_file(&new_second).unwrap();
        std::fs::remove_dir(&new_dir).unwrap();
        journal_paths_for_specs(
            &db,
            vec![spec.clone()],
            "remove-folder",
            std::slice::from_ref(&new_dir),
        )
        .await
        .unwrap();
        process_pending_events(&db, &resources, &generated, 32)
            .await
            .unwrap();
        assert!(sqlx::query_scalar::<_, Option<String>>(
            "SELECT deleted_at FROM works WHERE id = ?1",
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap()
        .is_some());

        std::fs::create_dir_all(&new_dir).unwrap();
        std::fs::write(&new_first, b"revived-alpha").unwrap();
        std::fs::write(&new_second, b"revived-beta").unwrap();
        journal_paths_for_specs(
            &db,
            vec![spec],
            "create-folder",
            std::slice::from_ref(&new_dir),
        )
        .await
        .unwrap();
        process_pending_events(&db, &resources, &generated, 32)
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT work_id FROM catalog_work_sources WHERE kind = 'gallery' AND root_id = ?1 AND work_key = 'new-artist'",
            )
            .bind(root_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            work_id
        );
        assert!(sqlx::query_scalar::<_, Option<String>>(
            "SELECT deleted_at FROM works WHERE id = ?1",
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap()
        .is_none());

        sqlx::query(
            "UPDATE file_inventory SET status = 'missing' WHERE root_id = ?1 AND work_key = 'new-artist'",
        )
        .bind(root_id)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO scan_events (
                root_id, relative_path, event_kind, work_key, observed_at, status
            )
            VALUES (
                ?1, 'new-artist', 'catalog-upsert', 'new-artist',
                strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'pending'
            )
            "#,
        )
        .bind(root_id)
        .execute(db.pool())
        .await
        .unwrap();
        for _ in 0..3 {
            process_pending_gallery_catalog_events(&db, &resources, 8, true)
                .await
                .unwrap();
        }
        let failed = sqlx::query(
            "SELECT status, attempts, last_error FROM scan_events WHERE root_id = ?1 AND work_key = 'new-artist' AND event_kind = 'catalog-upsert' ORDER BY seq DESC LIMIT 1",
        )
        .bind(root_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(failed.get::<String, _>("status"), "failed");
        assert_eq!(failed.get::<i64, _>("attempts"), 3);
        assert!(failed
            .get::<Option<String>, _>("last_error")
            .unwrap_or_default()
            .contains("preserving the previous work"));
        assert!(sqlx::query_scalar::<_, Option<String>>(
            "SELECT deleted_at FROM works WHERE id = ?1",
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap()
        .is_none());
        assert_eq!(resources.snapshot().pools["scan_io"].used, 0);
        assert_eq!(resources.snapshot().pools["processing_memory"].used, 0);
    }

    #[tokio::test]
    #[ignore = "explicit 700k-row NAS inventory acceptance benchmark"]
    async fn synthetic_700k_inventory_uses_fixed_batches() {
        let (temp, db) = test_db().await;
        let root_id = add_root(&db, temp.path()).await;
        let generation = begin_root_scan(&db, root_id, "acceptance").await.unwrap();
        let total = 700_000_usize;
        let started = std::time::Instant::now();
        let mut run = InventoryRootRun::default();
        let mut max_serialized = 0;
        for start in (0..total).step_by(INVENTORY_BATCH_SIZE) {
            let end = (start + INVENTORY_BATCH_SIZE).min(total);
            let batch = (start..end)
                .map(|index| synthetic_row(index, false))
                .collect::<Vec<_>>();
            assert!(batch.len() <= INVENTORY_BATCH_SIZE);
            let stats = upsert_inventory_batch(&db, root_id, generation, "acceptance", &batch)
                .await
                .unwrap();
            max_serialized = max_serialized.max(stats.serialized_bytes);
            run.discovered += stats.discovered;
            run.inserted += stats.inserted;
            run.changed += stats.changed;
        }
        finish_root_scan(&db, root_id, generation, "acceptance", &run)
            .await
            .unwrap();
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM file_inventory WHERE root_id = ?1 AND status = 'present'",
        )
        .bind(root_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        let elapsed_millis = started.elapsed().as_millis();
        let summary = serde_json::json!({
            "artifact_version": 1,
            "rows": total,
            "batch_rows": INVENTORY_BATCH_SIZE,
            "elapsed_millis": elapsed_millis,
            "max_batch_rows": INVENTORY_BATCH_SIZE,
            "max_batch_bytes": max_serialized,
            "discovered": run.discovered,
            "inserted": run.inserted,
            "changed": run.changed,
            "present_rows": count,
        });
        eprintln!(
            "synthetic inventory rows={total} elapsed_ms={elapsed_millis} max_batch_rows={} max_batch_bytes={max_serialized}",
            INVENTORY_BATCH_SIZE
        );
        println!("INVENTORY_GATE={summary}");
        if let Ok(output) = std::env::var("INVENTORY_GATE_OUTPUT") {
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
                "refusing to overwrite inventory gate artifact"
            );
            if let Some(parent) = output
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(output, serde_json::to_vec_pretty(&summary).unwrap()).unwrap();
        }
        assert_eq!(count, total as i64);
        assert_eq!(run.inserted, total as u64);
        assert!(max_serialized < 1024 * 1024);
    }

    #[test]
    fn work_keys_are_kind_specific_and_paths_are_not_exposed_by_diagnostics() {
        let gallery_root = RootSpec {
            kind: "gallery".to_string(),
            provider: "local".to_string(),
            root: PathBuf::from("gallery"),
            root_text: "gallery".to_string(),
            scan_depth: None,
            device_class: "hdd".to_string(),
            audio_grouping: AudioGroupingMode::Auto,
        };
        assert_eq!(
            derive_work_key(&gallery_root, "alice/1.jpg", "alice", false),
            "alice"
        );
        let audio_root = RootSpec {
            kind: "audio".to_string(),
            provider: "local".to_string(),
            root: PathBuf::from("audio"),
            root_text: "audio".to_string(),
            scan_depth: None,
            device_class: "hdd".to_string(),
            audio_grouping: AudioGroupingMode::Auto,
        };
        assert_eq!(
            derive_work_key(
                &audio_root,
                "circle/rj123456/disc/1.flac",
                "circle/rj123456/disc",
                false,
            ),
            "RJ123456"
        );
        let comic_root = RootSpec {
            kind: "comic".to_string(),
            provider: "local".to_string(),
            root: PathBuf::from("comic"),
            root_text: "comic".to_string(),
            scan_depth: None,
            device_class: "hdd".to_string(),
            audio_grouping: AudioGroupingMode::Auto,
        };
        assert_eq!(
            derive_work_key(&comic_root, "alice/book.cbz", "alice", false),
            "alice/book.cbz"
        );
        let root = "D:/private/library/gallery";
        assert_eq!(root_label(root), "gallery");
        assert!(!redacted_root_key(root).contains("private"));
        let gallery_root = RootSpec {
            kind: "gallery".to_string(),
            provider: "local".to_string(),
            root: PathBuf::from("gallery"),
            root_text: "gallery".to_string(),
            scan_depth: None,
            device_class: "hdd".to_string(),
            audio_grouping: AudioGroupingMode::Auto,
        };
        assert_eq!(
            derive_event_work_key(&gallery_root, "alice", ".", true),
            "alice"
        );
    }

    #[tokio::test]
    async fn n100_reconcile_hands_scan_io_between_inventory_and_catalog_batches() {
        let (temp, db) = test_db().await;
        let root_path = temp.path().join("novels-batched");
        std::fs::create_dir_all(&root_path).unwrap();
        let epub_count = usize::try_from(NOVEL_COORDINATOR_BATCH_SIZE).unwrap() + 1;
        for index in 0..epub_count {
            write_test_epub(&root_path.join(format!("book-{index:03}.epub")));
        }
        let total_files = INVENTORY_BATCH_SIZE + 1;
        for index in 0..total_files.saturating_sub(epub_count) {
            std::fs::write(
                root_path.join(format!("padding-{index:04}.txt")),
                b"padding",
            )
            .unwrap();
        }
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots(kind, provider, root, enabled, status)
            VALUES ('novel', 'local', ?1, 1, 'idle')
            RETURNING id
            "#,
        )
        .bind(root_path.to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'novel'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let inventory_root = InventoryRoot {
            id: root_id,
            spec: RootSpec {
                kind: "novel".to_string(),
                provider: "local".to_string(),
                root: root_path.clone(),
                root_text: root_path.to_string_lossy().to_string(),
                scan_depth: None,
                device_class: "hdd".to_string(),
                audio_grouping: AudioGroupingMode::Auto,
            },
        };
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let generated = temp.path().join("generated");

        let run = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            reconcile_root(
                &db,
                &resources,
                inventory_root,
                &generated,
                Arc::new(AtomicBool::new(true)),
            ),
        )
        .await
        .expect("N100 batched reconcile must keep making progress")
        .unwrap();

        assert!(run.complete);
        assert_eq!(run.discovered, total_files as u64);
        assert_eq!(run.max_batch_rows, INVENTORY_BATCH_SIZE);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM works WHERE kind = 'novel' AND deleted_at IS NULL",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            epub_count as i64
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM scan_events WHERE root_id = ?1 AND status IN ('pending', 'processing')",
            )
            .bind(root_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            0
        );
        let snapshot = resources.snapshot();
        assert_eq!(snapshot.pools["scan_io"].used, 0);
        assert_eq!(snapshot.pools["processing_memory"].used, 0);
    }

    #[tokio::test]
    async fn comic_coordinator_reconciles_cbz_delete_and_revive_with_a_stable_id() {
        let (temp, db) = test_db().await;
        let root_path = temp.path().join("comics");
        std::fs::create_dir_all(&root_path).unwrap();
        let archive_path = root_path.join("book.cbz");
        write_test_comic_archive(&archive_path);
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots(kind, provider, root, enabled, status)
            VALUES ('comic', 'local', ?1, 1, 'idle')
            RETURNING id
            "#,
        )
        .bind(root_path.to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'comic'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let inventory_root = InventoryRoot {
            id: root_id,
            spec: RootSpec {
                kind: "comic".to_string(),
                provider: "local".to_string(),
                root: root_path.clone(),
                root_text: root_path.to_string_lossy().to_string(),
                scan_depth: None,
                device_class: "hdd".to_string(),
                audio_grouping: AudioGroupingMode::Auto,
            },
        };
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let generated = temp.path().join("generated");

        let first = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_root(
                &db,
                &resources,
                inventory_root.clone(),
                &generated,
                Arc::new(AtomicBool::new(true)),
            ),
        )
        .await
        .expect("N100 single-permit reconcile must not deadlock")
        .unwrap();
        assert!(first.complete);
        let (work_id, deleted): (i64, Option<String>) = sqlx::query_as(
            "SELECT id, deleted_at FROM works WHERE kind = 'comic' AND source_path LIKE '%book.cbz'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert!(deleted.is_none());
        sqlx::query(
            "INSERT INTO reading_history (work_id, progress, position, update_token) VALUES (?1, 0.4, 'page-2', 'comic-history')",
        )
        .bind(work_id)
        .execute(db.pool())
        .await
        .unwrap();

        std::fs::remove_file(&archive_path).unwrap();
        let second = reconcile_root(
            &db,
            &resources,
            inventory_root.clone(),
            &generated,
            Arc::new(AtomicBool::new(true)),
        )
        .await
        .unwrap();
        assert!(second.complete);
        assert!(sqlx::query_scalar::<_, Option<String>>(
            "SELECT deleted_at FROM works WHERE id = ?1"
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap()
        .is_some());
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
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT position FROM reading_history WHERE work_id = ?1",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "page-2"
        );

        write_test_comic_archive(&archive_path);
        let third = reconcile_root(
            &db,
            &resources,
            inventory_root,
            &generated,
            Arc::new(AtomicBool::new(true)),
        )
        .await
        .unwrap();
        assert!(third.complete);
        let revived: (i64, Option<String>) = sqlx::query_as(
            "SELECT id, deleted_at FROM works WHERE kind = 'comic' AND source_path LIKE '%book.cbz'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(revived.0, work_id);
        assert!(revived.1.is_none());
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT position FROM reading_history WHERE work_id = ?1",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "page-2"
        );
    }

    #[tokio::test]
    async fn qmediasync_comic_reconcile_inspects_strm_without_downloading_archive() {
        let (temp, db) = test_db().await;
        let root_path = temp.path().join("qms-comics");
        let book_dir = root_path.join("Author");
        std::fs::create_dir_all(&book_dir).unwrap();
        std::fs::write(
            book_dir.join("ComicInfo.xml"),
            r#"<ComicInfo><Series>Remote Fixture</Series><PageCount>17</PageCount></ComicInfo>"#,
        )
        .unwrap();
        std::fs::write(book_dir.join("cover.jpg"), [0xff_u8, 0xd8, 0xff, 0xd9]).unwrap();
        let strm_path = book_dir.join("book.cbz.strm");
        std::fs::write(&strm_path, "https://qmediasync.example/book.cbz\n").unwrap();
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots(kind, provider, root, enabled, status)
            VALUES ('comic', 'qmediasync:qms', ?1, 1, 'idle')
            RETURNING id
            "#,
        )
        .bind(root_path.to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'comic'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let inventory_root = InventoryRoot {
            id: root_id,
            spec: RootSpec {
                kind: "comic".to_string(),
                provider: "qmediasync:qms".to_string(),
                root: root_path.clone(),
                root_text: root_path.to_string_lossy().to_string(),
                scan_depth: Some(12),
                device_class: "hdd".to_string(),
                audio_grouping: AudioGroupingMode::Auto,
            },
        };
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let generated = temp.path().join("generated");
        let run = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_root(
                &db,
                &resources,
                inventory_root,
                &generated,
                Arc::new(AtomicBool::new(true)),
            ),
        )
        .await
        .expect("qmediasync bounded reconcile must not deadlock")
        .unwrap();

        assert!(run.complete);
        assert_eq!(run.discovered, 3);
        let work: (String, Option<String>) = sqlx::query_as(
            "SELECT source_path, deleted_at FROM works WHERE kind = 'comic' AND source_path LIKE 'qms-strm://qms/%'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(work.0, "qms-strm://qms/Author/book.cbz.strm");
        assert!(work.1.is_none());
        let asset_path: String = sqlx::query_scalar(
            "SELECT path FROM assets WHERE role = 'archive' AND path LIKE 'qms-strm://qms/%'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(asset_path, "qms-strm://qms/Author/book.cbz.strm");
        let snapshot = resources.snapshot();
        assert_eq!(snapshot.pools["scan_io"].used, 0);
        assert_eq!(snapshot.pools["processing_memory"].used, 0);
    }

    #[tokio::test]
    async fn qmediasync_audio_reconcile_streams_strm_tracks_without_downloading() {
        let (temp, db) = test_db().await;
        let root_path = temp.path().join("qms-audio");
        let work_dir = root_path.join("RJ123456").join("Disc");
        std::fs::create_dir_all(&work_dir).unwrap();
        std::fs::write(
            work_dir.join("01.mp3.strm"),
            "https://qmediasync.example/audio/01.mp3\n",
        )
        .unwrap();
        std::fs::write(
            work_dir.join("cover.jpg.strm"),
            "https://qmediasync.example/audio/cover.jpg\n",
        )
        .unwrap();
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots(kind, provider, root, enabled, status, audio_grouping)
            VALUES ('audio', 'qmediasync:audio', ?1, 1, 'idle', 'auto')
            RETURNING id
            "#,
        )
        .bind(root_path.to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'audio'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let run = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_root(
                &db,
                &resources,
                InventoryRoot {
                    id: root_id,
                    spec: RootSpec {
                        kind: "audio".to_string(),
                        provider: "qmediasync:audio".to_string(),
                        root: root_path.clone(),
                        root_text: root_path.to_string_lossy().to_string(),
                        scan_depth: Some(12),
                        device_class: "hdd".to_string(),
                        audio_grouping: AudioGroupingMode::Auto,
                    },
                },
                &temp.path().join("generated"),
                Arc::new(AtomicBool::new(true)),
            ),
        )
        .await
        .expect("qmediasync audio bounded reconcile must not deadlock")
        .unwrap();

        assert!(run.complete);
        assert_eq!(run.discovered, 2);
        let work: (String, Option<String>) = sqlx::query_as(
            "SELECT source_path, deleted_at FROM works WHERE kind = 'audio' AND source_path LIKE 'qms-strm://audio/%'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(work.0, "qms-strm://audio/RJ123456");
        assert!(work.1.is_none());
        let track: (String, String, Option<i64>) = sqlx::query_as(
            "SELECT path, mime, size FROM assets WHERE role = 'track' AND path LIKE 'qms-strm://audio/%'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(track.0, "qms-strm://audio/RJ123456/Disc/01.mp3.strm");
        assert_eq!(track.1, "audio/mpeg");
        assert!(track.2.is_none());
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'legacy' WHERE kind = 'audio'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let evidence = crate::catalog_reconciliation::reconcile_audio_with(&db, &resources)
            .await
            .unwrap();
        assert_eq!(evidence.status, "failed");
        assert_eq!(evidence.expected_works, 1);
        assert_eq!(evidence.matched_works, 0);
        assert_eq!(evidence.mismatch_works, 1);
        assert_eq!(evidence.error_works, 0);
        let snapshot = resources.snapshot();
        assert_eq!(snapshot.pools["scan_io"].used, 0);
        assert_eq!(snapshot.pools["processing_memory"].used, 0);
    }

    #[tokio::test]
    async fn qmediasync_coser_picture_reconcile_keeps_strm_asset_bounded() {
        let (temp, db) = test_db().await;
        let root_path = temp.path().join("qms-coser");
        let author_dir = root_path.join("Alice");
        std::fs::create_dir_all(&author_dir).unwrap();
        let strm_path = author_dir.join("set.zip.strm");
        std::fs::write(&strm_path, "https://qmediasync.example/set.zip\n").unwrap();
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots(kind, provider, root, enabled, status)
            VALUES ('coser-picture', 'qmediasync:coser', ?1, 1, 'idle')
            RETURNING id
            "#,
        )
        .bind(root_path.to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'coser-picture'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let run = reconcile_root(
            &db,
            &resources,
            InventoryRoot {
                id: root_id,
                spec: RootSpec {
                    kind: "coser-picture".to_string(),
                    provider: "qmediasync:coser".to_string(),
                    root: root_path.clone(),
                    root_text: root_path.to_string_lossy().to_string(),
                    scan_depth: Some(12),
                    device_class: "hdd".to_string(),
                    audio_grouping: AudioGroupingMode::Auto,
                },
            },
            &temp.path().join("generated"),
            Arc::new(AtomicBool::new(true)),
        )
        .await
        .unwrap();

        assert!(run.complete);
        let source_path: String = sqlx::query_scalar(
            "SELECT source_path FROM works WHERE kind = 'coser-picture' AND source_path LIKE 'qms-strm://coser/%'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(source_path, "qms-strm://coser/Alice/set.zip.strm");
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT path FROM assets WHERE role = 'archive' AND path LIKE 'qms-strm://coser/%'",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "qms-strm://coser/Alice/set.zip.strm"
        );
        let snapshot = resources.snapshot();
        assert_eq!(snapshot.pools["scan_io"].used, 0);
        assert_eq!(snapshot.pools["processing_memory"].used, 0);
    }

    #[tokio::test]
    async fn coser_picture_coordinator_handles_modify_delete_and_revive_on_one_n100_permit() {
        let (temp, db) = test_db().await;
        let root_path = temp.path().join("coser-picture");
        let author_dir = root_path.join("Alice");
        std::fs::create_dir_all(&author_dir).unwrap();
        let archive_path = author_dir.join("set.zip");
        write_test_coser_picture_archive(&archive_path, 2);
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots(kind, provider, root, enabled, status)
            VALUES ('coser-picture', 'local', ?1, 1, 'idle')
            RETURNING id
            "#,
        )
        .bind(root_path.to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'coser-picture'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let inventory_root = InventoryRoot {
            id: root_id,
            spec: RootSpec {
                kind: "coser-picture".to_string(),
                provider: "local".to_string(),
                root: root_path.clone(),
                root_text: root_path.to_string_lossy().to_string(),
                scan_depth: None,
                device_class: "hdd".to_string(),
                audio_grouping: AudioGroupingMode::Auto,
            },
        };
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let generated = temp.path().join("generated");

        let first = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_root(
                &db,
                &resources,
                inventory_root.clone(),
                &generated,
                Arc::new(AtomicBool::new(true)),
            ),
        )
        .await
        .expect("N100 single-permit CoserPicture reconcile must not deadlock")
        .unwrap();
        assert!(first.complete);
        let (work_id, page_count, deleted): (i64, i64, Option<String>) = sqlx::query_as(
            "SELECT id, CAST(json_extract(meta_json, '$.page_count') AS INTEGER), deleted_at FROM works WHERE kind = 'coser-picture'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(page_count, 2);
        assert!(deleted.is_none());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM assets WHERE work_id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM work_tags WHERE work_id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            3
        );
        sqlx::query(
            "INSERT INTO reading_history (work_id, progress, position, update_token) VALUES (?1, 0.5, 'page-2', 'coser-history')",
        )
        .bind(work_id)
        .execute(db.pool())
        .await
        .unwrap();

        write_test_coser_picture_archive(&archive_path, 3);
        let modified = reconcile_root(
            &db,
            &resources,
            inventory_root.clone(),
            &generated,
            Arc::new(AtomicBool::new(true)),
        )
        .await
        .unwrap();
        assert!(modified.complete);
        let updated: (i64, i64) = sqlx::query_as(
            "SELECT id, CAST(json_extract(meta_json, '$.page_count') AS INTEGER) FROM works WHERE kind = 'coser-picture' AND deleted_at IS NULL",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(updated, (work_id, 3));

        std::fs::remove_file(&archive_path).unwrap();
        let removed = reconcile_root(
            &db,
            &resources,
            inventory_root.clone(),
            &generated,
            Arc::new(AtomicBool::new(true)),
        )
        .await
        .unwrap();
        assert!(removed.complete);
        assert!(sqlx::query_scalar::<_, Option<String>>(
            "SELECT deleted_at FROM works WHERE id = ?1"
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap()
        .is_some());
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
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT position FROM reading_history WHERE work_id = ?1",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "page-2"
        );

        write_test_coser_picture_archive(&archive_path, 2);
        let revived = reconcile_root(
            &db,
            &resources,
            inventory_root,
            &generated,
            Arc::new(AtomicBool::new(true)),
        )
        .await
        .unwrap();
        assert!(revived.complete);
        let active: (i64, Option<String>) =
            sqlx::query_as("SELECT id, deleted_at FROM works WHERE kind = 'coser-picture'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(active.0, work_id);
        assert!(active.1.is_none());
        let snapshot = resources.snapshot();
        assert_eq!(snapshot.pools["scan_io"].used, 0);
        assert_eq!(snapshot.pools["processing_memory"].used, 0);
    }

    #[tokio::test]
    async fn coser_picture_watcher_rename_preserves_identity_and_bad_zip_preserves_work() {
        let (temp, db) = test_db().await;
        let root_path = temp.path().join("coser-picture-events");
        let old_dir = root_path.join("Alice");
        let new_dir = root_path.join("Bob");
        std::fs::create_dir_all(&old_dir).unwrap();
        std::fs::create_dir_all(&new_dir).unwrap();
        let old_path = old_dir.join("old.zip");
        let new_path = new_dir.join("new.zip");
        write_test_coser_picture_archive(&old_path, 2);
        let spec = RootSpec {
            kind: "coser-picture".to_string(),
            provider: "local".to_string(),
            root: root_path.clone(),
            root_text: root_path.to_string_lossy().to_string(),
            scan_depth: None,
            device_class: "hdd".to_string(),
            audio_grouping: AudioGroupingMode::Auto,
        };
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'coser-picture'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let generated = temp.path().join("generated");

        journal_paths_for_specs(
            &db,
            vec![spec.clone()],
            "create",
            std::slice::from_ref(&old_path),
        )
        .await
        .unwrap();
        let first = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            process_pending_events(&db, &resources, &generated, 16),
        )
        .await
        .expect("N100 single-permit CoserPicture watcher drain must not deadlock")
        .unwrap();
        assert_eq!(first.completed, 1);
        assert_eq!(first.catalog_completed, 1);
        let work_id: i64 = sqlx::query_scalar(
            "SELECT id FROM works WHERE kind = 'coser-picture' AND deleted_at IS NULL",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();

        std::fs::rename(&old_path, &new_path).unwrap();
        journal_paths_for_specs(
            &db,
            vec![spec.clone()],
            "rename",
            &[old_path.clone(), new_path.clone()],
        )
        .await
        .unwrap();
        let renamed = process_pending_events(&db, &resources, &generated, 16)
            .await
            .unwrap();
        assert_eq!(renamed.completed, 2);
        let active: (i64, String, Option<String>) = sqlx::query_as(
            "SELECT id, source_path, deleted_at FROM works WHERE kind = 'coser-picture'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(active.0, work_id);
        assert!(active.1.ends_with("Bob/new.zip"));
        assert!(active.2.is_none());
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM works WHERE kind = 'coser-picture' AND deleted_at IS NULL",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1
        );

        std::fs::write(&new_path, b"not a zip").unwrap();
        journal_paths_for_specs(
            &db,
            vec![spec.clone()],
            "modify",
            std::slice::from_ref(&new_path),
        )
        .await
        .unwrap();
        for _ in 0..3 {
            process_pending_events(&db, &resources, &generated, 16)
                .await
                .unwrap();
        }
        assert!(sqlx::query_scalar::<_, Option<String>>(
            "SELECT deleted_at FROM works WHERE id = ?1"
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap()
        .is_none());
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM scan_events AS event JOIN library_roots AS root ON root.id = event.root_id WHERE root.kind = 'coser-picture' AND event.event_kind = 'catalog-upsert' AND event.status = 'failed'",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT phase FROM novel_coordinator_state WHERE root_id = (SELECT id FROM library_roots WHERE kind = 'coser-picture' AND root = ?1)",
            )
            .bind(root_path.to_string_lossy().to_string())
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "degraded"
        );

        write_test_coser_picture_archive(&new_path, 2);
        journal_paths_for_specs(&db, vec![spec], "modify", std::slice::from_ref(&new_path))
            .await
            .unwrap();
        let repaired = process_pending_events(&db, &resources, &generated, 16)
            .await
            .unwrap();
        assert_eq!(repaired.catalog_completed, 1);
        let repaired_work: (i64, Option<String>) =
            sqlx::query_as("SELECT id, deleted_at FROM works WHERE id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(repaired_work.0, work_id);
        assert!(repaired_work.1.is_none());
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT phase FROM novel_coordinator_state WHERE root_id = (SELECT id FROM library_roots WHERE kind = 'coser-picture' AND root = ?1)",
            )
            .bind(root_path.to_string_lossy().to_string())
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "idle"
        );
    }

    #[tokio::test]
    async fn incomplete_coser_picture_generation_never_enqueues_a_tombstone() {
        let (temp, db) = test_db().await;
        let root_path = temp.path().join("coser-picture-incomplete");
        std::fs::create_dir_all(&root_path).unwrap();
        let archive_path = root_path.join("set.zip");
        write_test_coser_picture_archive(&archive_path, 1);
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots(kind, provider, root, enabled, status)
            VALUES ('coser-picture', 'local', ?1, 1, 'idle')
            RETURNING id
            "#,
        )
        .bind(root_path.to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'coser-picture'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let root = InventoryRoot {
            id: root_id,
            spec: RootSpec {
                kind: "coser-picture".to_string(),
                provider: "local".to_string(),
                root: root_path.clone(),
                root_text: root_path.to_string_lossy().to_string(),
                scan_depth: None,
                device_class: "hdd".to_string(),
                audio_grouping: AudioGroupingMode::Auto,
            },
        };
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let generated = temp.path().join("generated");
        reconcile_root(
            &db,
            &resources,
            root,
            &generated,
            Arc::new(AtomicBool::new(true)),
        )
        .await
        .unwrap();
        let work_id: i64 = sqlx::query_scalar(
            "SELECT id FROM works WHERE kind = 'coser-picture' AND deleted_at IS NULL",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();

        std::fs::remove_file(&archive_path).unwrap();
        let token = "incomplete-coser-picture";
        let generation = begin_root_scan(&db, root_id, token).await.unwrap();
        mark_root_incomplete(
            &db,
            root_id,
            generation,
            token,
            &InventoryRootRun {
                root_id,
                kind: "coser-picture".to_string(),
                generation,
                ..Default::default()
            },
            "simulated directory read failure",
        )
        .await
        .unwrap();

        assert!(sqlx::query_scalar::<_, Option<String>>(
            "SELECT deleted_at FROM works WHERE id = ?1"
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap()
        .is_none());
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM scan_events WHERE root_id = ?1 AND event_kind = 'catalog-delete'",
            )
            .bind(root_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn novel_coordinator_reconciles_create_delete_and_revive_with_a_stable_id() {
        let (temp, db) = test_db().await;
        let root_path = temp.path().join("novels");
        std::fs::create_dir_all(&root_path).unwrap();
        write_test_epub(&root_path.join("book.epub"));
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots(kind, provider, root, enabled, status)
            VALUES ('novel', 'local', ?1, 1, 'idle')
            RETURNING id
            "#,
        )
        .bind(root_path.to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'novel'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let inventory_root = InventoryRoot {
            id: root_id,
            spec: RootSpec {
                kind: "novel".to_string(),
                provider: "local".to_string(),
                root: root_path.clone(),
                root_text: root_path.to_string_lossy().to_string(),
                scan_depth: None,
                device_class: "hdd".to_string(),
                audio_grouping: AudioGroupingMode::Auto,
            },
        };
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let generated = temp.path().join("generated");
        let first = reconcile_root(
            &db,
            &resources,
            inventory_root.clone(),
            &generated,
            Arc::new(AtomicBool::new(true)),
        )
        .await
        .unwrap();
        assert!(first.complete);
        let (work_id, deleted): (i64, Option<String>) = sqlx::query_as(
            "SELECT id, deleted_at FROM works WHERE kind = 'novel' AND source_path LIKE '%book.epub'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert!(deleted.is_none());

        std::fs::remove_file(root_path.join("book.epub")).unwrap();
        let second = reconcile_root(
            &db,
            &resources,
            inventory_root.clone(),
            &generated,
            Arc::new(AtomicBool::new(true)),
        )
        .await
        .unwrap();
        assert!(second.complete);
        let tombstoned: Option<String> =
            sqlx::query_scalar("SELECT deleted_at FROM works WHERE id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert!(tombstoned.is_some());
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM search_outbox WHERE work_id = ?1 AND operation = 'delete'",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1
        );

        write_test_epub(&root_path.join("book.epub"));
        let third = reconcile_root(
            &db,
            &resources,
            inventory_root,
            &generated,
            Arc::new(AtomicBool::new(true)),
        )
        .await
        .unwrap();
        assert!(third.complete);
        let revived: (i64, Option<String>) = sqlx::query_as(
            "SELECT id, deleted_at FROM works WHERE kind = 'novel' AND source_path LIKE '%book.epub'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(revived.0, work_id);
        assert!(revived.1.is_none());
    }

    #[tokio::test]
    async fn novel_watcher_events_use_changed_keys_and_preserve_identity_on_rename() {
        let (temp, db) = test_db().await;
        let root_path = temp.path().join("novels");
        std::fs::create_dir_all(&root_path).unwrap();
        let old_path = root_path.join("old.epub");
        let new_path = root_path.join("new.epub");
        write_test_epub(&old_path);
        let spec = RootSpec {
            kind: "novel".to_string(),
            provider: "local".to_string(),
            root: root_path.clone(),
            root_text: root_path.to_string_lossy().to_string(),
            scan_depth: None,
            device_class: "hdd".to_string(),
            audio_grouping: AudioGroupingMode::Auto,
        };
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'novel'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let generated = temp.path().join("generated");
        journal_paths_for_specs(
            &db,
            vec![spec.clone()],
            "create",
            std::slice::from_ref(&old_path),
        )
        .await
        .unwrap();
        let first = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            process_pending_events(&db, &resources, &generated, 16),
        )
        .await
        .expect("N100 single-permit watcher drain must not deadlock")
        .unwrap();
        assert_eq!(first.completed, 1);
        let work_id: i64 = sqlx::query_scalar(
            "SELECT id FROM works WHERE kind = 'novel' AND source_path LIKE '%old.epub'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();

        std::fs::rename(&old_path, &new_path).unwrap();
        journal_paths_for_specs(
            &db,
            vec![spec.clone()],
            "rename",
            &[old_path.clone(), new_path.clone()],
        )
        .await
        .unwrap();
        let rename = process_pending_events(&db, &resources, &generated, 16)
            .await
            .unwrap();
        assert_eq!(rename.completed, 2);
        let renamed: (i64, Option<String>, String) = sqlx::query_as(
            "SELECT id, deleted_at, source_path FROM works WHERE kind = 'novel' AND source_path LIKE '%new.epub'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(renamed.0, work_id);
        assert!(renamed.1.is_none());
        assert!(renamed.2.ends_with("new.epub"));
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM works WHERE kind = 'novel' AND deleted_at IS NULL",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn bad_epub_event_is_degraded_without_tombstoning_the_previous_work() {
        let (temp, db) = test_db().await;
        let root_path = temp.path().join("novels");
        std::fs::create_dir_all(&root_path).unwrap();
        let book = root_path.join("book.epub");
        write_test_epub(&book);
        let spec = RootSpec {
            kind: "novel".to_string(),
            provider: "local".to_string(),
            root: root_path.clone(),
            root_text: root_path.to_string_lossy().to_string(),
            scan_depth: None,
            device_class: "hdd".to_string(),
            audio_grouping: AudioGroupingMode::Auto,
        };
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'novel'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let resources = ResourceGovernor::standard();
        let generated = temp.path().join("generated");
        journal_paths_for_specs(
            &db,
            vec![spec.clone()],
            "create",
            std::slice::from_ref(&book),
        )
        .await
        .unwrap();
        process_pending_events(&db, &resources, &generated, 16)
            .await
            .unwrap();
        let work_id: i64 = sqlx::query_scalar(
            "SELECT id FROM works WHERE kind = 'novel' AND source_path LIKE '%book.epub'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        std::fs::write(&book, b"not an epub").unwrap();
        journal_paths_for_specs(
            &db,
            vec![spec.clone()],
            "modify",
            std::slice::from_ref(&book),
        )
        .await
        .unwrap();
        for _ in 0..3 {
            process_pending_events(&db, &resources, &generated, 16)
                .await
                .unwrap();
        }
        let deleted: Option<String> =
            sqlx::query_scalar("SELECT deleted_at FROM works WHERE id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert!(deleted.is_none());
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM scan_events WHERE event_kind = 'catalog-upsert' AND status = 'failed'",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT phase FROM novel_coordinator_state WHERE root_id = ?1",
            )
            .bind(
                sqlx::query_scalar::<_, i64>(
                    "SELECT id FROM library_roots WHERE kind = 'novel' AND root = ?1",
                )
                .bind(root_path.to_string_lossy().to_string())
                .fetch_one(db.pool())
                .await
                .unwrap(),
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "degraded"
        );

        // A later watcher event supersedes the terminal failure.  Repairing
        // the EPUB must revive the same work and clear the coordinator's
        // degraded phase without requiring a full-library scan.
        write_test_epub(&book);
        journal_paths_for_specs(&db, vec![spec], "modify", std::slice::from_ref(&book))
            .await
            .unwrap();
        let repaired = process_pending_events(&db, &resources, &generated, 16)
            .await
            .unwrap();
        assert_eq!(repaired.catalog_completed, 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM scan_events AS failed WHERE failed.root_id = (SELECT id FROM library_roots WHERE kind = 'novel' AND root = ?1) AND failed.status = 'failed' AND failed.event_kind IN ('catalog-upsert', 'catalog-delete') AND NOT EXISTS (SELECT 1 FROM scan_events AS newer WHERE newer.root_id = failed.root_id AND newer.work_key = failed.work_key AND newer.event_kind IN ('catalog-upsert', 'catalog-delete') AND newer.seq > failed.seq)",
            )
            .bind(root_path.to_string_lossy().to_string())
            .fetch_one(db.pool())
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT phase FROM novel_coordinator_state WHERE root_id = (SELECT id FROM library_roots WHERE kind = 'novel' AND root = ?1)",
            )
            .bind(root_path.to_string_lossy().to_string())
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "idle"
        );
        let revived: (i64, Option<String>) =
            sqlx::query_as("SELECT id, deleted_at FROM works WHERE id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(revived.0, work_id);
        assert!(revived.1.is_none());
    }

    #[tokio::test]
    async fn audio_reconcile_streams_large_work_on_one_n100_permit() {
        let (temp, db) = test_db().await;
        let root_path = temp.path().join("audio-large");
        let work_path = root_path.join("RJ123456").join("disc");
        std::fs::create_dir_all(&work_path).unwrap();
        let track_count = crate::catalog_writer::WORK_MUTATION_ASSET_LIMIT + 33;
        for index in 0..track_count {
            std::fs::write(
                work_path.join(format!("track-{index:04}.mp3")),
                b"not-real-mp3",
            )
            .unwrap();
        }
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots(kind, provider, root, enabled, status)
            VALUES ('audio', 'local', ?1, 1, 'idle')
            RETURNING id
            "#,
        )
        .bind(root_path.to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'audio'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let inventory_root = InventoryRoot {
            id: root_id,
            spec: RootSpec {
                kind: "audio".to_string(),
                provider: "local".to_string(),
                root: root_path.clone(),
                root_text: root_path.to_string_lossy().to_string(),
                scan_depth: None,
                device_class: "hdd".to_string(),
                audio_grouping: AudioGroupingMode::Auto,
            },
        };
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let generated = temp.path().join("generated");
        let run = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            reconcile_root(
                &db,
                &resources,
                inventory_root,
                &generated,
                Arc::new(AtomicBool::new(true)),
            ),
        )
        .await
        .expect("large audio reconcile must not deadlock on one ScanIo permit")
        .unwrap();
        assert!(run.complete);
        let work_id: i64 =
            sqlx::query_scalar("SELECT id FROM works WHERE kind = 'audio' AND deleted_at IS NULL")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM assets WHERE work_id = ?1 AND role = 'track'",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            track_count as i64
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM scan_events WHERE root_id = ?1 AND status IN ('pending', 'processing')",
            )
            .bind(root_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            0
        );
        let snapshot = resources.snapshot();
        assert_eq!(snapshot.pools["scan_io"].used, 0);
        assert_eq!(snapshot.pools["processing_memory"].used, 0);
    }

    #[tokio::test]
    async fn audio_reconcile_normalizes_nested_rj_and_supports_aac_opus() {
        let (temp, db) = test_db().await;
        let root_path = temp.path().join("audio-formats");
        let product = root_path
            .join("Creator")
            .join("rj234567")
            .join("Product Name");
        let aac_dir = product.join("AAC");
        let opus_dir = product.join("Opus");
        std::fs::create_dir_all(&aac_dir).unwrap();
        std::fs::create_dir_all(&opus_dir).unwrap();
        std::fs::write(aac_dir.join("01 AAC.aac"), b"not-real-aac").unwrap();
        std::fs::write(opus_dir.join("01 Opus.opus"), b"not-real-opus").unwrap();
        std::fs::write(product.join("cover.jpg"), b"cover").unwrap();

        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots(kind, provider, root, enabled, status)
            VALUES ('audio', 'local', ?1, 1, 'idle')
            RETURNING id
            "#,
        )
        .bind(root_path.to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'audio'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let run = reconcile_root(
            &db,
            &resources,
            InventoryRoot {
                id: root_id,
                spec: RootSpec {
                    kind: "audio".to_string(),
                    provider: "local".to_string(),
                    root: root_path,
                    root_text: temp
                        .path()
                        .join("audio-formats")
                        .to_string_lossy()
                        .to_string(),
                    scan_depth: None,
                    device_class: "hdd".to_string(),
                    audio_grouping: AudioGroupingMode::Auto,
                },
            },
            &temp.path().join("generated"),
            Arc::new(AtomicBool::new(true)),
        )
        .await
        .unwrap();
        assert!(run.complete);

        let work: (i64, String, String) = sqlx::query_as(
            "SELECT id, title, source_path FROM works WHERE kind = 'audio' AND deleted_at IS NULL",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(work.1, "Product Name");
        assert!(work
            .2
            .replace('\\', "/")
            .ends_with("/Creator/rj234567/Product Name"));
        let tracks: Vec<(String, Option<String>, Option<i64>)> = sqlx::query_as(
            "SELECT path, variant, position FROM assets WHERE work_id = ?1 AND role = 'track' ORDER BY path",
        )
        .bind(work.0)
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(tracks.len(), 2);
        assert!(tracks.iter().all(|track| track.2 == Some(0)));
        assert_eq!(
            tracks
                .iter()
                .filter_map(|track| track.1.as_deref())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["AAC", "Opus"])
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM assets WHERE work_id = ?1 AND role = 'cover'",
            )
            .bind(work.0)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM work_tags AS wt JOIN tags ON tags.id = wt.tag_id WHERE wt.work_id = ?1 AND tags.namespace = 'audio' AND tags.key IN ('aac', 'opus')",
            )
            .bind(work.0)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            2
        );
        let snapshot = resources.snapshot();
        assert_eq!(snapshot.pools["scan_io"].used, 0);
        assert_eq!(snapshot.pools["processing_memory"].used, 0);
    }

    #[tokio::test]
    async fn audio_inspection_failure_preserves_previous_work_and_history() {
        let (temp, db) = test_db().await;
        let root_path = temp.path().join("audio-failure");
        let work_path = root_path.join("RJ765432");
        std::fs::create_dir_all(&work_path).unwrap();
        let track = work_path.join("01.mp3");
        std::fs::write(&track, b"first").unwrap();
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots(kind, provider, root, enabled, status)
            VALUES ('audio', 'local', ?1, 1, 'idle')
            RETURNING id
            "#,
        )
        .bind(root_path.to_string_lossy().to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'audio'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let inventory_root = InventoryRoot {
            id: root_id,
            spec: RootSpec {
                kind: "audio".to_string(),
                provider: "local".to_string(),
                root: root_path.clone(),
                root_text: root_path.to_string_lossy().to_string(),
                scan_depth: None,
                device_class: "hdd".to_string(),
                audio_grouping: AudioGroupingMode::Auto,
            },
        };
        reconcile_root(
            &db,
            &resources,
            inventory_root,
            &temp.path().join("generated"),
            Arc::new(AtomicBool::new(true)),
        )
        .await
        .unwrap();
        let work_id: i64 =
            sqlx::query_scalar("SELECT id FROM works WHERE kind = 'audio' AND deleted_at IS NULL")
                .fetch_one(db.pool())
                .await
                .unwrap();
        sqlx::query(
            "INSERT INTO reading_history (work_id, progress, position, update_token) VALUES (?1, 0.25, 'track:0', 'audio-failure-history')",
        )
        .bind(work_id)
        .execute(db.pool())
        .await
        .unwrap();

        std::fs::remove_file(&track).unwrap();
        sqlx::query(
            r#"
            INSERT INTO scan_events (
                root_id, relative_path, event_kind, work_key, observed_at, status, attempts
            )
            VALUES (?1, 'RJ765432', 'catalog-upsert', 'RJ765432',
                    strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'pending', 2)
            "#,
        )
        .bind(root_id)
        .execute(db.pool())
        .await
        .unwrap();
        let failed = process_pending_events(&db, &resources, &temp.path().join("generated"), 16)
            .await
            .unwrap();
        assert_eq!(failed.catalog_failed, 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM assets WHERE work_id = ?1 AND role = 'track'",
            )
            .bind(work_id)
            .fetch_one(db.pool())
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
        .is_none());
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT position FROM reading_history WHERE work_id = ?1",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "track:0"
        );

        std::fs::write(&track, b"repaired").unwrap();
        sqlx::query(
            r#"
            INSERT INTO scan_events (
                root_id, relative_path, event_kind, work_key, observed_at, status
            )
            VALUES (?1, 'RJ765432', 'catalog-upsert', 'RJ765432',
                    strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'pending')
            "#,
        )
        .bind(root_id)
        .execute(db.pool())
        .await
        .unwrap();
        let repaired = process_pending_events(&db, &resources, &temp.path().join("generated"), 16)
            .await
            .unwrap();
        assert_eq!(repaired.catalog_completed, 1);
        let revived: (i64, Option<String>) =
            sqlx::query_as("SELECT id, deleted_at FROM works WHERE id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(revived.0, work_id);
        assert!(revived.1.is_none());
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT position FROM reading_history WHERE work_id = ?1",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "track:0"
        );
        let snapshot = resources.snapshot();
        assert_eq!(snapshot.pools["scan_io"].used, 0);
        assert_eq!(snapshot.pools["processing_memory"].used, 0);
    }

    #[tokio::test]
    async fn audio_watcher_updates_one_group_and_preserves_history_across_tombstone() {
        let (temp, db) = test_db().await;
        let root_path = temp.path().join("audio-events");
        let work_path = root_path.join("rj654321");
        std::fs::create_dir_all(&work_path).unwrap();
        let first_track = work_path.join("01.mp3");
        let second_track = work_path.join("02.flac");
        std::fs::write(&first_track, b"first").unwrap();
        std::fs::write(&second_track, b"second").unwrap();
        let spec = RootSpec {
            kind: "audio".to_string(),
            provider: "local".to_string(),
            root: root_path.clone(),
            root_text: root_path.to_string_lossy().to_string(),
            scan_depth: None,
            device_class: "hdd".to_string(),
            audio_grouping: AudioGroupingMode::Auto,
        };
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'audio'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let generated = temp.path().join("generated");

        journal_paths_for_specs(
            &db,
            vec![spec.clone()],
            "create",
            std::slice::from_ref(&first_track),
        )
        .await
        .unwrap();
        let created = process_pending_events(&db, &resources, &generated, 16)
            .await
            .unwrap();
        assert_eq!(created.completed, 1);
        assert_eq!(created.catalog_completed, 1);
        let work_id: i64 =
            sqlx::query_scalar("SELECT id FROM works WHERE kind = 'audio' AND deleted_at IS NULL")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM assets WHERE work_id = ?1 AND role = 'track'",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            2
        );
        sqlx::query(
            "INSERT INTO reading_history (work_id, progress, position, update_token) VALUES (?1, 0.5, 'track-2', 'audio-history')",
        )
        .bind(work_id)
        .execute(db.pool())
        .await
        .unwrap();

        std::fs::remove_file(&second_track).unwrap();
        journal_paths_for_specs(
            &db,
            vec![spec.clone()],
            "remove",
            std::slice::from_ref(&second_track),
        )
        .await
        .unwrap();
        let reduced = process_pending_events(&db, &resources, &generated, 16)
            .await
            .unwrap();
        assert_eq!(reduced.catalog_completed, 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM assets WHERE work_id = ?1 AND role = 'track'",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1
        );
        assert!(sqlx::query_scalar::<_, Option<String>>(
            "SELECT deleted_at FROM works WHERE id = ?1"
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap()
        .is_none());

        std::fs::remove_file(&first_track).unwrap();
        journal_paths_for_specs(
            &db,
            vec![spec.clone()],
            "remove",
            std::slice::from_ref(&first_track),
        )
        .await
        .unwrap();
        let removed = process_pending_events(&db, &resources, &generated, 16)
            .await
            .unwrap();
        assert_eq!(removed.catalog_completed, 1);
        assert!(sqlx::query_scalar::<_, Option<String>>(
            "SELECT deleted_at FROM works WHERE id = ?1"
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap()
        .is_some());
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT position FROM reading_history WHERE work_id = ?1",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "track-2"
        );

        std::fs::write(&first_track, b"revived").unwrap();
        journal_paths_for_specs(
            &db,
            vec![spec],
            "create",
            std::slice::from_ref(&first_track),
        )
        .await
        .unwrap();
        let revived = process_pending_events(&db, &resources, &generated, 16)
            .await
            .unwrap();
        assert_eq!(revived.catalog_completed, 1);
        let revived_work: (i64, Option<String>) = sqlx::query_as(
            "SELECT id, deleted_at FROM works WHERE kind = 'audio' AND source_path LIKE '%rj654321%'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(revived_work.0, work_id);
        assert!(revived_work.1.is_none());
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT position FROM reading_history WHERE work_id = ?1",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "track-2"
        );
    }

    #[tokio::test]
    async fn audio_folder_event_stays_inside_its_non_rj_work_key() {
        let (temp, db) = test_db().await;
        let root_path = temp.path().join("audio-folders");
        let first_album = root_path.join("Author").join("Album A");
        let sibling_album = root_path.join("Author").join("Album B");
        std::fs::create_dir_all(&first_album).unwrap();
        std::fs::create_dir_all(&sibling_album).unwrap();
        let first_track = first_album.join("01.mp3");
        let sibling_track = sibling_album.join("01.mp3");
        std::fs::write(&first_track, b"first").unwrap();
        std::fs::write(&sibling_track, b"sibling").unwrap();
        let spec = RootSpec {
            kind: "audio".to_string(),
            provider: "local".to_string(),
            root: root_path.clone(),
            root_text: root_path.to_string_lossy().to_string(),
            scan_depth: None,
            device_class: "hdd".to_string(),
            audio_grouping: AudioGroupingMode::Folder,
        };
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'audio'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        journal_paths_for_specs(
            &db,
            vec![spec],
            "create",
            std::slice::from_ref(&first_track),
        )
        .await
        .unwrap();
        let summary = process_pending_events(&db, &resources, &temp.path().join("generated"), 16)
            .await
            .unwrap();
        assert_eq!(summary.catalog_completed, 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM works WHERE kind = 'audio' AND deleted_at IS NULL",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT work_key FROM catalog_work_sources WHERE kind = 'audio'",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "Author/Album A"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM file_inventory")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM file_inventory WHERE relative_path = 'Author/Album B/01.mp3'",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn audio_folder_rename_keeps_work_identity_and_history() {
        let (temp, db) = test_db().await;
        let root_path = temp.path().join("audio-rename");
        let album = root_path.join("Author").join("Album");
        std::fs::create_dir_all(&album).unwrap();
        let old_track = album.join("01.mp3");
        let new_track = album.join("02.mp3");
        std::fs::write(&old_track, b"old").unwrap();
        let spec = RootSpec {
            kind: "audio".to_string(),
            provider: "local".to_string(),
            root: root_path.clone(),
            root_text: root_path.to_string_lossy().to_string(),
            scan_depth: None,
            device_class: "hdd".to_string(),
            audio_grouping: AudioGroupingMode::Folder,
        };
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'audio'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let generated = temp.path().join("generated");
        journal_paths_for_specs(
            &db,
            vec![spec.clone()],
            "create",
            std::slice::from_ref(&old_track),
        )
        .await
        .unwrap();
        process_pending_events(&db, &resources, &generated, 16)
            .await
            .unwrap();
        let work_id: i64 =
            sqlx::query_scalar("SELECT id FROM works WHERE kind = 'audio' AND deleted_at IS NULL")
                .fetch_one(db.pool())
                .await
                .unwrap();
        sqlx::query(
            "INSERT INTO reading_history (work_id, progress, position, update_token) VALUES (?1, 0.4, 'track:0', 'audio-rename-history')",
        )
        .bind(work_id)
        .execute(db.pool())
        .await
        .unwrap();

        std::fs::rename(&old_track, &new_track).unwrap();
        journal_paths_for_specs(
            &db,
            vec![spec],
            "rename",
            &[old_track.clone(), new_track.clone()],
        )
        .await
        .unwrap();
        process_pending_events(&db, &resources, &generated, 16)
            .await
            .unwrap();

        let renamed: (i64, Option<String>) = sqlx::query_as(
            "SELECT id, deleted_at FROM works WHERE kind = 'audio' AND source_path LIKE '%Author/Album%'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(renamed.0, work_id);
        assert!(renamed.1.is_none());
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT path FROM assets WHERE work_id = ?1 AND role = 'track'",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap()
            .replace('\\', "/"),
            new_track.to_string_lossy().replace('\\', "/")
        );
        assert_eq!(
            sqlx::query_scalar::<_, f64>(
                "SELECT progress FROM reading_history WHERE work_id = ?1",
            )
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            0.4
        );
    }

    fn write_test_epub(path: &Path) {
        use std::fs::File;
        use std::io::Write;
        let file = File::create(path).unwrap();
        let mut archive = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();
        archive
            .start_file("META-INF/container.xml", options)
            .unwrap();
        archive
            .write_all(
                br#"<?xml version="1.0"?><container><rootfiles><rootfile full-path="OPS/content.opf" media-type="application/oebps-package+xml"/></rootfiles></container>"#,
            )
            .unwrap();
        archive.start_file("OPS/content.opf", options).unwrap();
        archive
            .write_all(
                br#"<?xml version="1.0"?><package><metadata><dc:title>Coordinator Novel</dc:title><dc:creator>Author</dc:creator><dc:language>en</dc:language></metadata><manifest><item id="chapter" href="chapter.xhtml" media-type="application/xhtml+xml"/></manifest><spine><itemref idref="chapter"/></spine></package>"#,
            )
            .unwrap();
        archive.start_file("OPS/chapter.xhtml", options).unwrap();
        archive
            .write_all(b"<html><body>chapter</body></html>")
            .unwrap();
        archive.finish().unwrap();
    }

    fn write_test_comic_archive(path: &Path) {
        use std::fs::File;
        use std::io::Write;
        let file = File::create(path).unwrap();
        let mut archive = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();
        archive.start_file("001.jpg", options).unwrap();
        archive.write_all(&[0xff, 0xd8, 0xff, 0xd9]).unwrap();
        archive.start_file("002.png", options).unwrap();
        archive.write_all(&[0x89, b'P', b'N', b'G']).unwrap();
        archive.finish().unwrap();
    }

    fn write_test_coser_picture_archive(path: &Path, pages: usize) {
        use std::fs::File;
        use std::io::Write;
        let file = File::create(path).unwrap();
        let mut archive = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();
        for index in 0..pages {
            archive
                .start_file(format!("{index:03}.jpg"), options)
                .unwrap();
            archive.write_all(&[0xff, 0xd8, index as u8, 0xd9]).unwrap();
        }
        archive.finish().unwrap();
    }
}
