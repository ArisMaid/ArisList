//! Recoverable incremental Tantivy updates for a shadow index.
//!
//! The production reader remains on the existing full-rebuild index. This
//! consumer is deliberately callable but not spawned until shadow-query
//! reconciliation and N100 gates are complete.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::Row;
use sqlx_sqlite::SqliteConnection;
use tantivy::{doc, Term};
use tokio::sync::Mutex;
use uuid::Uuid;

use super::{
    build_search_index_snapshot, open_or_create_index, open_search_writer, search_error,
    SearchIndexRow, SearchShadowFactReport,
};
use crate::db::Db;
use crate::error::{AppError, Result};
use crate::resource::{ResourceClass, ResourceGovernor};
use crate::AppState;

const MAX_OUTBOX_BATCH: i64 = 256;
pub(crate) const OUTBOX_PAYLOAD_VERSION: i64 = 1;
const CLAIM_STALE_AFTER_SECONDS: i64 = 5 * 60;
const MAX_ERROR_BYTES: usize = 2048;
const SHADOW_INDEX_NAME: &str = "shadow-v3";
const SHADOW_SCHEMA_VERSION: i64 = 1;
const SHADOW_IDLE_POLL: Duration = Duration::from_secs(2);
const SHADOW_ACTIVE_POLL: Duration = Duration::from_millis(100);
const SHADOW_ERROR_POLL: Duration = Duration::from_secs(10);
pub(crate) const REBUILD_SHADOW_SEARCH_JOB_TYPE: &str = "rebuild-shadow-search-index";

static SHADOW_OUTBOX_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

async fn acquire_lock_with_timeout<'a>(
    lock: &'a Mutex<()>,
    lock_timeout: Duration,
) -> Result<tokio::sync::MutexGuard<'a, ()>> {
    tokio::time::timeout(lock_timeout, lock.lock())
        .await
        .map_err(|_| {
            AppError::Other(format!(
                "incremental search reader gate lock wait exceeded {}ms",
                lock_timeout.as_millis()
            ))
        })
}

#[derive(Debug, Clone)]
struct ClaimedOutboxItem {
    work_id: i64,
    operation: String,
    catalog_revision: i64,
    search_revision: i64,
    payload_version: i64,
    attempts: i64,
}

#[derive(Debug, Clone)]
struct IndexMutation {
    work_id: i64,
    document: Option<SearchIndexRow>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ConsumeSummary {
    pub claimed: usize,
    pub committed: usize,
    pub acknowledged: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct OutboxStatus {
    pub pending: i64,
    pub claimed: i64,
    pub committed: i64,
    pub failed: i64,
    pub retrying: i64,
    pub max_attempts: i64,
    pub stale_claims: i64,
    pub oldest_failed_at: Option<String>,
    pub oldest_pending_at: Option<String>,
    pub latest_committed_at: Option<String>,
    pub oldest_pending_revision: Option<i64>,
    pub oldest_pending_search_revision: Option<i64>,
    pub catalog_revision: i64,
    pub shadow_applied_revision: i64,
    pub revision_lag: i64,
    pub search_revision: i64,
    pub shadow_applied_search_revision: i64,
    pub search_revision_lag: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct ShadowReconciliationSnapshot {
    pub outbox: OutboxStatus,
    pub shadow: ShadowIndexStatus,
}

/// Search facts used by the Catalog ownership promotion boundary.
///
/// All three records are read through one SQLite connection and transaction so
/// a promotion decision cannot combine an older reconciliation row with a
/// newer outbox revision (or vice versa).
#[derive(Debug, Clone)]
pub(crate) struct SearchPromotionSnapshot {
    pub outbox: OutboxStatus,
    pub shadow: ShadowIndexStatus,
    pub reconciliation: ShadowReconciliationStatus,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ShadowIndexStatus {
    pub index_name: String,
    pub schema_version: i64,
    pub baseline_revision: i64,
    pub applied_revision: i64,
    pub baseline_search_revision: i64,
    pub applied_search_revision: i64,
    pub indexed_documents: i64,
    pub ready: bool,
    pub status: String,
    pub last_error: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ShadowReconciliationStatus {
    pub index_name: String,
    pub status: String,
    pub catalog_revision: i64,
    pub catalog_revision_after: i64,
    pub applied_revision: i64,
    pub applied_revision_after: i64,
    pub search_revision: i64,
    pub search_revision_after: i64,
    pub applied_search_revision: i64,
    pub applied_search_revision_after: i64,
    pub sqlite_work_count: i64,
    pub index_document_count: i64,
    pub index_unique_work_count: i64,
    pub missing_work_ids: i64,
    pub unexpected_work_ids: i64,
    pub duplicate_documents: i64,
    pub invalid_documents: i64,
    pub sqlite_ids_sha256: Option<String>,
    pub index_ids_sha256: Option<String>,
    pub consecutive_passes: i64,
    pub passed_since: Option<String>,
    pub last_checked_at: Option<String>,
    pub took_millis: Option<i64>,
    pub cutover_armed: bool,
}

fn matching_id_hashes(sqlite: &Option<String>, index: &Option<String>) -> bool {
    matches!((sqlite, index), (Some(sqlite), Some(index))
        if sqlite == index
            && sqlite.len() == 64
            && sqlite.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()))
}

/// Return whether the persisted shadow index is safe to use as evidence for a
/// per-kind Catalog v2 ownership promotion.
///
/// This is intentionally weaker than the final all-kind incremental-reader
/// gate: the first kind may be promoted while the shadow index is still in
/// `shadow` (rather than `ready`) state.  The evidence must nevertheless be
/// revision-consistent, schema-compatible, and caught up with the outbox.
/// Revision zero is valid for an empty catalog and must not be treated as an
/// uninitialised baseline.
pub(crate) fn search_promotion_gate_ready(
    shadow: &ShadowIndexStatus,
    reconciliation: &ShadowReconciliationStatus,
    outbox: &OutboxStatus,
) -> bool {
    shadow.schema_version == SHADOW_SCHEMA_VERSION
        && shadow.baseline_revision >= 0
        && shadow.baseline_revision <= shadow.applied_revision
        && shadow.baseline_search_revision >= 0
        && shadow.baseline_search_revision <= shadow.applied_search_revision
        && matches!(shadow.status.as_str(), "shadow" | "ready")
        && shadow.applied_revision == outbox.shadow_applied_revision
        && shadow.applied_search_revision == outbox.shadow_applied_search_revision
        && reconciliation.status == "passed"
        && reconciliation.catalog_revision == outbox.catalog_revision
        && reconciliation.catalog_revision_after == outbox.catalog_revision
        && reconciliation.applied_revision == outbox.shadow_applied_revision
        && reconciliation.applied_revision_after == outbox.shadow_applied_revision
        && reconciliation.search_revision == outbox.search_revision
        && reconciliation.search_revision_after == outbox.search_revision
        && reconciliation.applied_search_revision == outbox.shadow_applied_search_revision
        && reconciliation.applied_search_revision_after == outbox.shadow_applied_search_revision
        // Do not rely on the persisted status string alone.  A partially
        // written or manually repaired reconciliation row must not be enough
        // to promote Catalog ownership when its document counts disagree.
        && reconciliation.sqlite_work_count >= 0
        && shadow.indexed_documents == reconciliation.index_document_count
        && reconciliation.index_document_count == reconciliation.sqlite_work_count
        && reconciliation.index_unique_work_count == reconciliation.sqlite_work_count
        && reconciliation.missing_work_ids == 0
        && reconciliation.unexpected_work_ids == 0
        && reconciliation.duplicate_documents == 0
        && reconciliation.invalid_documents == 0
        && matching_id_hashes(
            &reconciliation.sqlite_ids_sha256,
            &reconciliation.index_ids_sha256,
        )
        && outbox.pending == 0
}

pub(crate) async fn status(db: &Db) -> Result<OutboxStatus> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let result = status_in(transaction.connection()).await?;
    transaction.commit().await?;
    Ok(result)
}

pub(crate) async fn status_in(connection: &mut SqliteConnection) -> Result<OutboxStatus> {
    let stale_before_text = (Utc::now() - ChronoDuration::seconds(CLAIM_STALE_AFTER_SECONDS))
        .to_rfc3339_opts(SecondsFormat::Millis, true);
    let row = sqlx::query(
        r#"
        SELECT
            COALESCE(SUM(CASE WHEN committed_at IS NULL THEN 1 ELSE 0 END), 0) AS pending,
            COALESCE(SUM(CASE WHEN committed_at IS NULL AND claimed_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS claimed,
            COALESCE(SUM(CASE WHEN committed_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS committed,
            COALESCE(SUM(CASE WHEN committed_at IS NULL AND last_error IS NOT NULL THEN 1 ELSE 0 END), 0) AS failed,
            COALESCE(SUM(CASE WHEN committed_at IS NULL AND last_error IS NOT NULL
                 AND julianday(available_at) > julianday('now') THEN 1 ELSE 0 END), 0) AS retrying,
            COALESCE(MAX(CASE WHEN committed_at IS NULL THEN attempts END), 0) AS max_attempts,
            COALESCE(SUM(CASE WHEN committed_at IS NULL AND claimed_at IS NOT NULL
                 AND julianday(claimed_at) < julianday(?2) THEN 1 ELSE 0 END), 0) AS stale_claims,
            MIN(CASE WHEN committed_at IS NULL AND last_error IS NOT NULL THEN updated_at END) AS oldest_failed_at,
            MIN(CASE WHEN committed_at IS NULL THEN created_at END) AS oldest_pending_at,
            MAX(committed_at) AS latest_committed_at,
            MIN(CASE WHEN committed_at IS NULL THEN catalog_revision END) AS oldest_pending_revision,
            MIN(CASE WHEN committed_at IS NULL THEN search_revision END) AS oldest_pending_search_revision,
            -- Search only needs to fence revisions that are represented by a
            -- baseline or a searchable outbox mutation. Catalog-only
            -- maintenance (for example facet-count backfills) may advance
            -- catalog_state without changing any Tantivy document.
            MAX(
                COALESCE((
                    SELECT baseline_revision
                    FROM search_index_state
                    WHERE index_name = ?1
                ), 0),
                COALESCE((SELECT MAX(catalog_revision) FROM search_outbox), 0)
            ) AS catalog_revision,
            (SELECT applied_revision FROM search_index_state WHERE index_name = ?1) AS shadow_applied_revision,
            (SELECT revision FROM search_source_state WHERE singleton = 1) AS search_revision,
            (SELECT applied_search_revision FROM search_index_state WHERE index_name = ?1) AS shadow_applied_search_revision
        FROM search_outbox
        "#,
    )
    .bind(SHADOW_INDEX_NAME)
    .bind(&stale_before_text)
    .fetch_one(&mut *connection)
    .await?;
    let catalog_revision: i64 = row.get("catalog_revision");
    let shadow_applied_revision: i64 = row.get("shadow_applied_revision");
    let search_revision: i64 = row.get("search_revision");
    let shadow_applied_search_revision: i64 = row.get("shadow_applied_search_revision");
    Ok(OutboxStatus {
        pending: row.get("pending"),
        claimed: row.get("claimed"),
        committed: row.get("committed"),
        failed: row.get("failed"),
        retrying: row.get("retrying"),
        max_attempts: row.get("max_attempts"),
        stale_claims: row.get("stale_claims"),
        oldest_failed_at: row.get("oldest_failed_at"),
        oldest_pending_at: row.get("oldest_pending_at"),
        latest_committed_at: row.get("latest_committed_at"),
        oldest_pending_revision: row.get("oldest_pending_revision"),
        oldest_pending_search_revision: row.get("oldest_pending_search_revision"),
        catalog_revision,
        shadow_applied_revision,
        revision_lag: catalog_revision
            .saturating_sub(shadow_applied_revision)
            .max(0),
        search_revision,
        shadow_applied_search_revision,
        search_revision_lag: search_revision
            .saturating_sub(shadow_applied_search_revision)
            .max(0),
    })
}

pub(crate) async fn shadow_index_status(db: &Db) -> Result<ShadowIndexStatus> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let result = shadow_index_status_in(transaction.connection()).await?;
    transaction.commit().await?;
    Ok(result)
}

pub(crate) async fn shadow_index_status_in(
    connection: &mut SqliteConnection,
) -> Result<ShadowIndexStatus> {
    let row = sqlx::query(
        r#"SELECT index_name, schema_version, baseline_revision, applied_revision,
                  baseline_search_revision, applied_search_revision,
                  indexed_documents, ready, status, last_error, updated_at
           FROM search_index_state WHERE index_name = ?1"#,
    )
    .bind(SHADOW_INDEX_NAME)
    .fetch_one(&mut *connection)
    .await?;
    Ok(ShadowIndexStatus {
        index_name: row.get("index_name"),
        schema_version: row.get("schema_version"),
        baseline_revision: row.get("baseline_revision"),
        applied_revision: row.get("applied_revision"),
        baseline_search_revision: row.get("baseline_search_revision"),
        applied_search_revision: row.get("applied_search_revision"),
        indexed_documents: row.get("indexed_documents"),
        ready: row.get::<i64, _>("ready") != 0,
        status: row.get("status"),
        last_error: row.get("last_error"),
        updated_at: row.get("updated_at"),
    })
}

pub(crate) async fn reconciliation_status(db: &Db) -> Result<ShadowReconciliationStatus> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let result = reconciliation_status_in(transaction.connection()).await?;
    transaction.commit().await?;
    Ok(result)
}

pub(crate) async fn reconciliation_status_in(
    connection: &mut SqliteConnection,
) -> Result<ShadowReconciliationStatus> {
    let row = sqlx::query(
        r#"SELECT index_name, status, catalog_revision, catalog_revision_after,
                  applied_revision, applied_revision_after,
                  search_revision, search_revision_after,
                  applied_search_revision, applied_search_revision_after,
                  sqlite_work_count, index_document_count, index_unique_work_count,
                  missing_work_ids, unexpected_work_ids, duplicate_documents,
                  invalid_documents, sqlite_ids_sha256, index_ids_sha256,
                  consecutive_passes, passed_since, last_checked_at, took_millis,
                  cutover_armed
           FROM search_reconciliation_state WHERE index_name = ?1"#,
    )
    .bind(SHADOW_INDEX_NAME)
    .fetch_one(&mut *connection)
    .await?;
    Ok(ShadowReconciliationStatus {
        index_name: row.get("index_name"),
        status: row.get("status"),
        catalog_revision: row.get("catalog_revision"),
        catalog_revision_after: row.get("catalog_revision_after"),
        applied_revision: row.get("applied_revision"),
        applied_revision_after: row.get("applied_revision_after"),
        search_revision: row.get("search_revision"),
        search_revision_after: row.get("search_revision_after"),
        applied_search_revision: row.get("applied_search_revision"),
        applied_search_revision_after: row.get("applied_search_revision_after"),
        sqlite_work_count: row.get("sqlite_work_count"),
        index_document_count: row.get("index_document_count"),
        index_unique_work_count: row.get("index_unique_work_count"),
        missing_work_ids: row.get("missing_work_ids"),
        unexpected_work_ids: row.get("unexpected_work_ids"),
        duplicate_documents: row.get("duplicate_documents"),
        invalid_documents: row.get("invalid_documents"),
        sqlite_ids_sha256: row.get("sqlite_ids_sha256"),
        index_ids_sha256: row.get("index_ids_sha256"),
        consecutive_passes: row.get("consecutive_passes"),
        passed_since: row.get("passed_since"),
        last_checked_at: row.get("last_checked_at"),
        took_millis: row.get("took_millis"),
        cutover_armed: row.get::<i64, _>("cutover_armed") != 0,
    })
}

/// Read the outbox and shadow-index readiness facts from one short SQLite
/// snapshot. The snapshot ends before any Tantivy or filesystem work begins.
pub(crate) async fn shadow_reconciliation_snapshot(
    db: &Db,
) -> Result<ShadowReconciliationSnapshot> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let outbox = status_in(transaction.connection()).await?;
    let shadow = shadow_index_status_in(transaction.connection()).await?;
    transaction.commit().await?;
    Ok(ShadowReconciliationSnapshot { outbox, shadow })
}

/// Read the complete Search promotion evidence from one short tracked
/// snapshot. The caller may use the `_in` variant while already inside the
/// ownership writer transaction to make the gate and ownership update atomic.
pub(crate) async fn search_promotion_snapshot(db: &Db) -> Result<SearchPromotionSnapshot> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let snapshot = search_promotion_snapshot_in(transaction.connection()).await?;
    transaction.commit().await?;
    Ok(snapshot)
}

pub(crate) async fn search_promotion_snapshot_in(
    connection: &mut SqliteConnection,
) -> Result<SearchPromotionSnapshot> {
    let outbox = status_in(connection).await?;
    let shadow = shadow_index_status_in(connection).await?;
    let reconciliation = reconciliation_status_in(connection).await?;
    Ok(SearchPromotionSnapshot {
        outbox,
        shadow,
        reconciliation,
    })
}

async fn incremental_reader_fast_state(db: &Db) -> Result<(bool, String)> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let state = incremental_reader_fast_state_in(transaction.connection()).await?;
    transaction.commit().await?;
    Ok(state)
}

async fn incremental_reader_fast_state_in(
    connection: &mut SqliteConnection,
) -> Result<(bool, String)> {
    let row = sqlx::query(
        r#"
        SELECT ready, status
        FROM search_index_state
        WHERE index_name = ?1
        "#,
    )
    .bind(SHADOW_INDEX_NAME)
    .fetch_one(&mut *connection)
    .await?;
    Ok((
        row.get::<i64, _>("ready") != 0,
        row.get::<String, _>("status"),
    ))
}

#[derive(Debug)]
struct IncrementalReaderGateSnapshot {
    shadow_ready: bool,
    shadow_status: String,
    applied_revision: i64,
    baseline_revision: i64,
    applied_search_revision: i64,
    baseline_search_revision: i64,
    indexed_documents: i64,
    reconciliation_status: String,
    reconciled_catalog_revision: i64,
    reconciled_catalog_revision_after: i64,
    reconciled_applied_revision: i64,
    reconciled_applied_revision_after: i64,
    reconciled_search_revision: i64,
    reconciled_search_revision_after: i64,
    reconciled_applied_search_revision: i64,
    reconciled_applied_search_revision_after: i64,
    reconciled_sqlite_work_count: i64,
    reconciled_index_document_count: i64,
    reconciled_index_unique_work_count: i64,
    reconciled_sqlite_ids_sha256: Option<String>,
    reconciled_index_ids_sha256: Option<String>,
    reconciled_missing_work_ids: i64,
    reconciled_unexpected_work_ids: i64,
    reconciled_duplicate_documents: i64,
    reconciled_invalid_documents: i64,
    cutover_armed: bool,
    catalog_revision: i64,
    search_revision: i64,
    pending: i64,
    legacy_kinds: i64,
}

async fn load_incremental_reader_gate_snapshot(
    connection: &mut SqliteConnection,
) -> Result<IncrementalReaderGateSnapshot> {
    let row = sqlx::query(
        r#"
        SELECT
            state.ready AS shadow_ready,
            state.status AS shadow_status,
            state.applied_revision AS applied_revision,
            state.baseline_revision AS baseline_revision,
            state.applied_search_revision AS applied_search_revision,
            state.baseline_search_revision AS baseline_search_revision,
            state.indexed_documents AS indexed_documents,
            reconciliation.status AS reconciliation_status,
            reconciliation.catalog_revision AS reconciled_catalog_revision,
            reconciliation.catalog_revision_after AS reconciled_catalog_revision_after,
            reconciliation.applied_revision AS reconciled_applied_revision,
            reconciliation.applied_revision_after AS reconciled_applied_revision_after,
            reconciliation.search_revision AS reconciled_search_revision,
            reconciliation.search_revision_after AS reconciled_search_revision_after,
            reconciliation.applied_search_revision AS reconciled_applied_search_revision,
            reconciliation.applied_search_revision_after AS reconciled_applied_search_revision_after,
            reconciliation.sqlite_work_count AS reconciled_sqlite_work_count,
            reconciliation.index_document_count AS reconciled_index_document_count,
            reconciliation.index_unique_work_count AS reconciled_index_unique_work_count,
            reconciliation.sqlite_ids_sha256 AS reconciled_sqlite_ids_sha256,
            reconciliation.index_ids_sha256 AS reconciled_index_ids_sha256,
            reconciliation.missing_work_ids AS reconciled_missing_work_ids,
            reconciliation.unexpected_work_ids AS reconciled_unexpected_work_ids,
            reconciliation.duplicate_documents AS reconciled_duplicate_documents,
            reconciliation.invalid_documents AS reconciled_invalid_documents,
            reconciliation.cutover_armed AS cutover_armed,
            MAX(
                COALESCE((
                    SELECT baseline_revision
                    FROM search_index_state
                    WHERE index_name = ?1
                ), 0),
                COALESCE((SELECT MAX(catalog_revision) FROM search_outbox), 0)
            ) AS catalog_revision,
            (SELECT revision FROM search_source_state WHERE singleton = 1) AS search_revision,
            (SELECT COUNT(*) FROM search_outbox WHERE committed_at IS NULL) AS pending,
            (SELECT COUNT(*) FROM catalog_kind_ownership
             WHERE authoritative_writer != 'catalog-v2') AS legacy_kinds
        FROM search_index_state AS state
        JOIN search_reconciliation_state AS reconciliation
          ON reconciliation.index_name = state.index_name
        WHERE state.index_name = ?1
        "#,
    )
    .bind(SHADOW_INDEX_NAME)
    .fetch_one(&mut *connection)
    .await?;

    Ok(IncrementalReaderGateSnapshot {
        shadow_ready: row.get::<i64, _>("shadow_ready") != 0,
        shadow_status: row.get("shadow_status"),
        applied_revision: row.get("applied_revision"),
        baseline_revision: row.get("baseline_revision"),
        applied_search_revision: row.get("applied_search_revision"),
        baseline_search_revision: row.get("baseline_search_revision"),
        indexed_documents: row.get("indexed_documents"),
        reconciliation_status: row.get("reconciliation_status"),
        reconciled_catalog_revision: row.get("reconciled_catalog_revision"),
        reconciled_catalog_revision_after: row.get("reconciled_catalog_revision_after"),
        reconciled_applied_revision: row.get("reconciled_applied_revision"),
        reconciled_applied_revision_after: row.get("reconciled_applied_revision_after"),
        reconciled_search_revision: row.get("reconciled_search_revision"),
        reconciled_search_revision_after: row.get("reconciled_search_revision_after"),
        reconciled_applied_search_revision: row.get("reconciled_applied_search_revision"),
        reconciled_applied_search_revision_after: row
            .get("reconciled_applied_search_revision_after"),
        reconciled_sqlite_work_count: row.get("reconciled_sqlite_work_count"),
        reconciled_index_document_count: row.get("reconciled_index_document_count"),
        reconciled_index_unique_work_count: row.get("reconciled_index_unique_work_count"),
        reconciled_sqlite_ids_sha256: row.get("reconciled_sqlite_ids_sha256"),
        reconciled_index_ids_sha256: row.get("reconciled_index_ids_sha256"),
        reconciled_missing_work_ids: row.get("reconciled_missing_work_ids"),
        reconciled_unexpected_work_ids: row.get("reconciled_unexpected_work_ids"),
        reconciled_duplicate_documents: row.get("reconciled_duplicate_documents"),
        reconciled_invalid_documents: row.get("reconciled_invalid_documents"),
        cutover_armed: row.get::<i64, _>("cutover_armed") != 0,
        catalog_revision: row.get("catalog_revision"),
        search_revision: row.get("search_revision"),
        pending: row.get("pending"),
        legacy_kinds: row.get("legacy_kinds"),
    })
}

/// Validate a gate snapshot without touching SQLite.  The return value tells
/// the first-arm caller whether it must issue the conditional arm update.
fn validate_incremental_reader_gate_snapshot(
    snapshot: &IncrementalReaderGateSnapshot,
    arm_if_ready: bool,
) -> Result<bool> {
    let progress_ready = snapshot.shadow_ready
        && snapshot.shadow_status == "ready"
        && snapshot.reconciliation_status != "failed"
        // Revision zero is a valid persisted baseline for an empty catalog;
        // the schema already constrains this value to be non-negative.
        && snapshot.baseline_revision >= 0
        && snapshot.baseline_search_revision >= 0
        && snapshot.legacy_kinds == 0
        && snapshot.pending == 0
        && snapshot.applied_revision == snapshot.catalog_revision
        && snapshot.applied_search_revision == snapshot.search_revision;

    if snapshot.cutover_armed {
        if !progress_ready
            || !matching_id_hashes(
                &snapshot.reconciled_sqlite_ids_sha256,
                &snapshot.reconciled_index_ids_sha256,
            )
        {
            return Err(AppError::Other(format!(
                "incremental search reader progress gate is not satisfied: shadow_status={}, shadow_ready={}, baseline_revision={}, applied_revision={}, catalog_revision={}, baseline_search_revision={}, applied_search_revision={}, search_revision={}, pending={}, legacy_kinds={}",
                snapshot.shadow_status,
                snapshot.shadow_ready,
                snapshot.baseline_revision,
                snapshot.applied_revision,
                snapshot.catalog_revision,
                snapshot.baseline_search_revision,
                snapshot.applied_search_revision,
                snapshot.search_revision,
                snapshot.pending,
                snapshot.legacy_kinds,
            )));
        }
        return Ok(false);
    }

    if !arm_if_ready {
        return Err(AppError::Other(
            "incremental search reader cutover is not armed".to_string(),
        ));
    }

    let reconciliation_ready = snapshot.reconciliation_status == "passed"
        && snapshot.reconciled_catalog_revision == snapshot.catalog_revision
        && snapshot.reconciled_catalog_revision_after == snapshot.catalog_revision
        && snapshot.reconciled_applied_revision == snapshot.applied_revision
        && snapshot.reconciled_applied_revision_after == snapshot.applied_revision
        && snapshot.reconciled_search_revision == snapshot.search_revision
        && snapshot.reconciled_search_revision_after == snapshot.search_revision
        && snapshot.reconciled_applied_search_revision == snapshot.applied_search_revision
        && snapshot.reconciled_applied_search_revision_after == snapshot.applied_search_revision
        && snapshot.reconciled_sqlite_work_count >= 0
        && snapshot.indexed_documents == snapshot.reconciled_index_document_count
        && snapshot.reconciled_index_document_count == snapshot.reconciled_sqlite_work_count
        && snapshot.reconciled_index_unique_work_count == snapshot.reconciled_sqlite_work_count
        && snapshot.reconciled_missing_work_ids == 0
        && snapshot.reconciled_unexpected_work_ids == 0
        && snapshot.reconciled_duplicate_documents == 0
        && snapshot.reconciled_invalid_documents == 0
        && matching_id_hashes(
            &snapshot.reconciled_sqlite_ids_sha256,
            &snapshot.reconciled_index_ids_sha256,
        );
    if !progress_ready || !reconciliation_ready {
        return Err(AppError::Other(format!(
            "incremental search reader reconciliation gate (first arm) is not satisfied: shadow_status={}, shadow_ready={}, baseline_revision={}, applied_revision={}, catalog_revision={}, baseline_search_revision={}, applied_search_revision={}, search_revision={}, pending={}, legacy_kinds={}, reconciliation_status={}, reconciled_catalog_revision={}..{}, reconciled_applied_revision={}..{}, reconciled_search_revision={}..{}, reconciled_applied_search_revision={}..{}",
            snapshot.shadow_status,
            snapshot.shadow_ready,
            snapshot.baseline_revision,
            snapshot.applied_revision,
            snapshot.catalog_revision,
            snapshot.baseline_search_revision,
            snapshot.applied_search_revision,
            snapshot.search_revision,
            snapshot.pending,
            snapshot.legacy_kinds,
            snapshot.reconciliation_status,
            snapshot.reconciled_catalog_revision,
            snapshot.reconciled_catalog_revision_after,
            snapshot.reconciled_applied_revision,
            snapshot.reconciled_applied_revision_after,
            snapshot.reconciled_search_revision,
            snapshot.reconciled_search_revision_after,
            snapshot.reconciled_applied_search_revision,
            snapshot.reconciled_applied_search_revision_after,
        )));
    }

    Ok(true)
}

/// Check (and, on the first successful check, arm) the incremental search
/// reader cutover.
///
/// The first arm is deliberately strict: it requires a persisted, stable
/// facts reconciliation for the current catalog and shadow revisions. Once
/// armed, normal catalog revisions do not require another full facts scan;
/// the shadow worker only has to catch up with the outbox. A baseline rebuild
/// resets the arm bit and therefore returns to the strict first-arm gate.
pub(crate) async fn ensure_incremental_reader_gate(db: &Db, lock_timeout: Duration) -> Result<()> {
    incremental_reader_gate(db, lock_timeout, true).await
}

/// Validate an already armed incremental reader without changing persisted
/// cutover state. Interactive production requests use this read-only path so
/// an unarmed deployment fails closed instead of implicitly promoting itself
/// on its first query.
pub(crate) async fn validate_incremental_reader_gate(
    db: &Db,
    lock_timeout: Duration,
) -> Result<()> {
    incremental_reader_gate(db, lock_timeout, false).await
}

async fn incremental_reader_gate(
    db: &Db,
    lock_timeout: Duration,
    arm_if_ready: bool,
) -> Result<()> {
    // Baseline construction marks the persisted state as `building` before it
    // takes the consumer lock for the long Tantivy snapshot. Read that small
    // state row first so an interactive request can fail closed immediately
    // instead of waiting behind the whole baseline build. The locked,
    // revision-fenced transaction below remains authoritative for ready and
    // armed states; this is only an early rejection for an index that cannot
    // possibly serve a production reader yet.
    let (fast_ready, fast_status) = incremental_reader_fast_state(db).await?;
    if !fast_ready || fast_status != "ready" {
        return Err(AppError::Other(format!(
            "incremental search reader shadow is not ready: status={fast_status}, ready={fast_ready}"
        )));
    }

    // Serialize with baseline creation and outbox application. This keeps a
    // baseline reset from racing an arm transaction and makes the state
    // transition easy to reason about on SQLite/NAS deployments. The lock is
    // part of the interactive read path, so it must fail closed at the same
    // bounded deadline as the resource governor rather than waiting behind a
    // stalled background index batch indefinitely.
    let _guard = acquire_lock_with_timeout(&SHADOW_OUTBOX_LOCK, lock_timeout).await?;

    if arm_if_ready {
        // First arm must validate and update the persisted bit in one write
        // transaction. This prevents a catalog revision from changing between
        // the facts snapshot and the conditional arm update.
        let _write_slot = db.acquire_write_slot(64 * 1024).await?;
        let mut transaction = db.begin_tracked_transaction().await?;
        let snapshot = load_incremental_reader_gate_snapshot(&mut transaction).await?;
        if validate_incremental_reader_gate_snapshot(&snapshot, true)? {
            sqlx::query(
                r#"
                UPDATE search_reconciliation_state
                SET cutover_armed = 1
                WHERE index_name = ?1 AND cutover_armed = 0
                "#,
            )
            .bind(SHADOW_INDEX_NAME)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        return Ok(());
    }

    // Production requests only validate an already persisted arm. Keep this
    // path out of the single-writer queue so a burst of searches cannot delay
    // an Inventory/Catalog/SearchWriter commit merely by checking readiness.
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let snapshot = load_incremental_reader_gate_snapshot(transaction.connection()).await?;
    let validation = validate_incremental_reader_gate_snapshot(&snapshot, false);
    let commit_result = transaction.commit().await;
    match validation {
        Err(error) => Err(error),
        Ok(_) => commit_result.map_err(Into::into),
    }
}

pub(crate) fn shadow_index_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("search-index-v3-shadow")
}

fn canonical_timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub(crate) fn spawn_shadow_worker(state: Arc<AppState>) -> Option<tokio::task::JoinHandle<()>> {
    if !state.config.search_outbox_shadow_enabled {
        return None;
    }
    Some(tokio::spawn(async move {
        let worker_id = format!("shadow-search-{}", Uuid::new_v4());
        let index_dir = shadow_index_dir(&state.config.data_dir);
        loop {
            let baseline_rebuilt = match ensure_shadow_baseline(
                &state.db,
                &state.resources,
                index_dir.clone(),
            )
            .await
            {
                Ok(rebuilt) => rebuilt,
                Err(error) => {
                    tracing::warn!(error = %error, "shadow search baseline is not ready");
                    tokio::time::sleep(SHADOW_ERROR_POLL).await;
                    continue;
                }
            };
            if baseline_rebuilt {
                // A baseline can replace the schema and all segments. Drop
                // the old reader rather than attempting an in-place reload
                // against an incompatible index, and discard only candidates
                // derived from this directory.
                state.search_runtime.reset_index(&index_dir).await;
            }
            match consume_once(
                &state.db,
                &state.resources,
                index_dir.clone(),
                &worker_id,
                MAX_OUTBOX_BATCH,
            )
            .await
            {
                Ok(summary) => {
                    if summary.committed > 0 {
                        if let Err(error) = state.search_runtime.refresh_index(&index_dir).await {
                            let _ = record_shadow_error(&state.db, &error.to_string()).await;
                            tracing::warn!(
                                error = %error,
                                "shadow search reader reload failed after commit"
                            );
                            tokio::time::sleep(SHADOW_ERROR_POLL).await;
                            continue;
                        }
                    }
                    if let Err(error) = refresh_shadow_progress(&state.db).await {
                        tracing::warn!(error = %error, "failed to refresh shadow search progress");
                        tokio::time::sleep(SHADOW_ERROR_POLL).await;
                        continue;
                    }
                    if summary.committed > 0 {
                        tracing::info!(
                            claimed = summary.claimed,
                            committed = summary.committed,
                            acknowledged = summary.acknowledged,
                            "shadow search outbox batch committed"
                        );
                        tokio::time::sleep(SHADOW_ACTIVE_POLL).await;
                    } else {
                        tokio::time::sleep(SHADOW_IDLE_POLL).await;
                    }
                }
                Err(error) => {
                    let _ = record_shadow_error(&state.db, &error.to_string()).await;
                    tracing::warn!(error = %error, "shadow search outbox batch failed");
                    tokio::time::sleep(SHADOW_ERROR_POLL).await;
                }
            }
        }
    }))
}

async fn ensure_shadow_baseline(
    db: &Db,
    resources: &ResourceGovernor,
    index_dir: PathBuf,
) -> Result<bool> {
    let _consumer = SHADOW_OUTBOX_LOCK.lock().await;
    Ok(
        rebuild_shadow_baseline_locked(db, resources, index_dir, false)
            .await?
            .is_some(),
    )
}

/// Rebuild the shadow index from a consistent SQLite snapshot even when its
/// persisted state is degraded. This is an explicit recovery path: the
/// regular worker intentionally does not overwrite a degraded index in a
/// loop, which keeps repeated Tantivy/IO failures from becoming a rebuild
/// storm on the N100.
pub(crate) async fn rebuild_shadow_index(
    db: &Db,
    resources: &ResourceGovernor,
    index_dir: PathBuf,
) -> Result<usize> {
    let _consumer = SHADOW_OUTBOX_LOCK.lock().await;
    rebuild_shadow_baseline_locked(db, resources, index_dir, true)
        .await?
        .ok_or_else(|| AppError::Other("forced shadow rebuild did not run".to_string()))
}

async fn rebuild_shadow_baseline_locked(
    db: &Db,
    resources: &ResourceGovernor,
    index_dir: PathBuf,
    force: bool,
) -> Result<Option<usize>> {
    let current = shadow_index_status(db).await?;
    // Revision zero is a valid, stable baseline for an empty catalog.  The
    // old `> 0` check rebuilt that index on every worker poll, which is a
    // needless CPU/IO loop on a freshly deployed NAS.  `status` and the
    // persisted Tantivy metadata still distinguish an uninitialised index.
    let baseline_valid = index_dir.join("meta.json").exists()
        && current.schema_version == SHADOW_SCHEMA_VERSION
        && !matches!(current.status.as_str(), "empty" | "building");
    if !force && baseline_valid {
        return Ok(None);
    }
    // Keep the state transition bounded by the SQLite writer gate, but do not
    // hold that gate while the Tantivy snapshot is being built below.
    {
        let _write_slot = db.acquire_write_slot(64 * 1024).await?;
        let mut transaction = db.begin_tracked_transaction().await?;
        sqlx::query(
            r#"
            UPDATE search_index_state
            SET schema_version = ?1,
                ready = 0,
                status = 'building',
                last_error = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE index_name = ?2
            "#,
        )
        .bind(SHADOW_SCHEMA_VERSION)
        .bind(SHADOW_INDEX_NAME)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
    }

    let snapshot = match build_search_index_snapshot(db, resources, index_dir).await {
        Ok(result) => result,
        Err(error) => {
            record_shadow_error(db, &error.to_string()).await?;
            return Err(error);
        }
    };
    let now_text = canonical_timestamp(Utc::now());
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    sqlx::query(
        r#"
        UPDATE search_outbox
        SET committed_at = ?1,
            claimed_by = NULL,
            claimed_at = NULL,
            last_error = NULL,
            updated_at = ?1
        WHERE committed_at IS NULL
          AND search_revision <= ?2
        "#,
    )
    .bind(&now_text)
    .bind(snapshot.search_revision)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        UPDATE search_index_state
        SET schema_version = ?1,
            baseline_revision = ?2,
            applied_revision = ?2,
            baseline_search_revision = ?3,
            applied_search_revision = ?3,
            indexed_documents = ?4,
            ready = 0,
            status = 'shadow',
            last_error = NULL,
            updated_at = ?5
        WHERE index_name = ?6
        "#,
    )
    .bind(SHADOW_SCHEMA_VERSION)
    .bind(snapshot.catalog_revision)
    .bind(snapshot.search_revision)
    .bind(i64::try_from(snapshot.documents).unwrap_or(i64::MAX))
    .bind(&now_text)
    .bind(SHADOW_INDEX_NAME)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        UPDATE search_reconciliation_state
        SET status = 'unknown',
            catalog_revision = ?1,
            catalog_revision_after = ?1,
            applied_revision = ?1,
            applied_revision_after = ?1,
            search_revision = ?2,
            search_revision_after = ?2,
            applied_search_revision = ?2,
            applied_search_revision_after = ?2,
            sqlite_work_count = 0,
            index_document_count = 0,
            index_unique_work_count = 0,
            missing_work_ids = 0,
            unexpected_work_ids = 0,
            duplicate_documents = 0,
            invalid_documents = 0,
            sqlite_ids_sha256 = NULL,
            index_ids_sha256 = NULL,
            consecutive_passes = 0,
            passed_since = NULL,
            last_checked_at = NULL,
            took_millis = NULL,
            cutover_armed = 0
        WHERE index_name = ?3
        "#,
    )
    .bind(snapshot.catalog_revision)
    .bind(snapshot.search_revision)
    .bind(SHADOW_INDEX_NAME)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    tracing::info!(
        documents = snapshot.documents,
        catalog_revision = snapshot.catalog_revision,
        search_revision = snapshot.search_revision,
        "shadow search baseline committed"
    );
    Ok(Some(snapshot.documents))
}

pub(crate) async fn refresh_shadow_progress(db: &Db) -> Result<ShadowIndexStatus> {
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    let row = sqlx::query(
        r#"
        SELECT
            state.baseline_revision,
            state.baseline_search_revision,
            MAX(
                state.baseline_revision,
                COALESCE((
                    SELECT MAX(catalog_revision)
                    FROM search_outbox
                    WHERE committed_at IS NOT NULL
                ), 0)
            ) AS applied_revision,
            MAX(
                state.baseline_search_revision,
                COALESCE((
                    SELECT MAX(search_revision)
                    FROM search_outbox
                    WHERE committed_at IS NOT NULL
                ), 0)
            ) AS applied_search_revision,
            MAX(
                COALESCE((
                    SELECT baseline_revision
                    FROM search_index_state
                    WHERE index_name = ?1
                ), 0),
                COALESCE((SELECT MAX(catalog_revision) FROM search_outbox), 0)
            ) AS catalog_revision,
            (SELECT revision FROM search_source_state WHERE singleton = 1) AS search_revision,
            (SELECT COUNT(*) FROM search_outbox WHERE committed_at IS NULL) AS pending,
            (SELECT COUNT(*) FROM catalog_kind_ownership
             WHERE authoritative_writer != 'catalog-v2') AS legacy_kinds,
            COALESCE((
                SELECT status FROM search_reconciliation_state WHERE index_name = ?1
            ), 'unknown') AS reconciliation_status,
            COALESCE((
                SELECT catalog_revision FROM search_reconciliation_state WHERE index_name = ?1
            ), 0) AS reconciled_catalog_revision,
            COALESCE((
                SELECT applied_revision FROM search_reconciliation_state WHERE index_name = ?1
            ), 0) AS reconciled_applied_revision,
            COALESCE((
                SELECT search_revision FROM search_reconciliation_state WHERE index_name = ?1
            ), 0) AS reconciled_search_revision,
            COALESCE((
                SELECT applied_search_revision FROM search_reconciliation_state WHERE index_name = ?1
            ), 0) AS reconciled_applied_search_revision
        FROM search_index_state AS state
        WHERE state.index_name = ?1
        "#,
    )
    .bind(SHADOW_INDEX_NAME)
    .fetch_one(&mut *transaction)
    .await?;
    let applied_revision: i64 = row.get("applied_revision");
    let applied_search_revision: i64 = row.get("applied_search_revision");
    let catalog_revision: i64 = row.get("catalog_revision");
    let search_revision: i64 = row.get("search_revision");
    let pending: i64 = row.get("pending");
    let legacy_kinds: i64 = row.get("legacy_kinds");
    let mut reconciliation_status: String = row.get("reconciliation_status");
    let reconciled_catalog_revision: i64 = row.get("reconciled_catalog_revision");
    let reconciled_applied_revision: i64 = row.get("reconciled_applied_revision");
    let reconciled_search_revision: i64 = row.get("reconciled_search_revision");
    let reconciled_applied_search_revision: i64 = row.get("reconciled_applied_search_revision");
    // A passed fact check becomes stale after ordinary catalog churn and may
    // be refreshed without treating the churn itself as corruption.  A
    // failed check is different: it records an actual SQLite/Tantivy drift
    // and must remain degraded until an explicit successful reconciliation,
    // even if newer outbox revisions arrive in the meantime.
    if reconciliation_status == "passed"
        && (reconciled_catalog_revision != catalog_revision
            || reconciled_applied_revision != applied_revision
            || reconciled_search_revision != search_revision
            || reconciled_applied_search_revision != applied_search_revision)
    {
        sqlx::query(
            r#"
            UPDATE search_reconciliation_state
            SET status = 'stale',
                consecutive_passes = 0,
                passed_since = NULL
            WHERE index_name = ?1
            "#,
        )
        .bind(SHADOW_INDEX_NAME)
        .execute(&mut *transaction)
        .await?;
        reconciliation_status = "stale".to_string();
    }
    let fact_failed = reconciliation_status == "failed";
    if fact_failed {
        // A failed fact check is a rollback condition.  Clear any historical
        // arm in this same writer transaction so a refresh can never expose
        // the invalid `degraded + armed` combination to readers.
        sqlx::query(
            r#"
            UPDATE search_reconciliation_state
            SET cutover_armed = 0
            WHERE index_name = ?1
            "#,
        )
        .bind(SHADOW_INDEX_NAME)
        .execute(&mut *transaction)
        .await?;
    }
    let ready = !fact_failed
        && legacy_kinds == 0
        && pending == 0
        && applied_revision >= catalog_revision
        && applied_search_revision >= search_revision;
    let status = if fact_failed {
        "degraded"
    } else if ready {
        "ready"
    } else if legacy_kinds > 0 {
        "shadow"
    } else {
        "catching-up"
    };
    sqlx::query(
        r#"
        UPDATE search_index_state
        SET applied_revision = ?1,
            applied_search_revision = ?2,
            ready = ?3,
            status = ?4,
            last_error = CASE
                WHEN ?5 = 1 THEN COALESCE(
                    last_error,
                    'shadow search fact reconciliation failed'
                )
                ELSE NULL
            END,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE index_name = ?6
        "#,
    )
    .bind(applied_revision)
    .bind(applied_search_revision)
    .bind(i64::from(ready))
    .bind(status)
    .bind(i64::from(fact_failed))
    .bind(SHADOW_INDEX_NAME)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    shadow_index_status(db).await
}

pub(crate) async fn record_shadow_reconciliation(
    db: &Db,
    report: &SearchShadowFactReport,
) -> Result<&'static str> {
    if !matches!(report.status, "passed" | "failed" | "stale") {
        return Err(AppError::BadRequest(format!(
            "unsupported shadow reconciliation status {:?}",
            report.status
        )));
    }
    let now = Utc::now();
    let now_text = now.to_rfc3339();
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    let current = sqlx::query(
        r#"
        SELECT
            MAX(
                COALESCE((
                    SELECT baseline_revision
                    FROM search_index_state
                    WHERE index_name = ?1
                ), 0),
                COALESCE((SELECT MAX(catalog_revision) FROM search_outbox), 0)
            ) AS catalog_revision,
            state.applied_revision,
            (SELECT revision FROM search_source_state WHERE singleton = 1) AS search_revision,
            state.applied_search_revision,
            (SELECT COUNT(*) FROM search_outbox WHERE committed_at IS NULL) AS pending,
            (SELECT COUNT(*) FROM catalog_kind_ownership
             WHERE authoritative_writer != 'catalog-v2') AS legacy_kinds
        FROM search_index_state AS state
        WHERE state.index_name = ?1
        "#,
    )
    .bind(SHADOW_INDEX_NAME)
    .fetch_one(&mut *transaction)
    .await?;
    let current_catalog_revision: i64 = current.get("catalog_revision");
    let current_applied_revision: i64 = current.get("applied_revision");
    let current_search_revision: i64 = current.get("search_revision");
    let current_applied_search_revision: i64 = current.get("applied_search_revision");
    let pending: i64 = current.get("pending");
    let legacy_kinds: i64 = current.get("legacy_kinds");
    let effective_status = if report.status != "stale"
        && (current_catalog_revision != report.catalog_revision_after
            || current_applied_revision != report.shadow_applied_revision_after
            || current_search_revision != report.search_revision_after
            || current_applied_search_revision != report.shadow_applied_search_revision_after)
    {
        "stale"
    } else {
        report.status
    };
    let previous = sqlx::query(
        r#"
        SELECT status, consecutive_passes, passed_since
        FROM search_reconciliation_state
        WHERE index_name = ?1
        "#,
    )
    .bind(SHADOW_INDEX_NAME)
    .fetch_one(&mut *transaction)
    .await?;
    let previous_status: String = previous.get("status");
    let previous_passes: i64 = previous.get("consecutive_passes");
    let previous_passed_since: Option<String> = previous.get("passed_since");
    let consecutive_passes = if effective_status == "passed" {
        if previous_status == "passed" {
            previous_passes.saturating_add(1)
        } else {
            1
        }
    } else {
        0
    };
    let passed_since = if effective_status == "passed" {
        previous_passed_since.or_else(|| Some(now_text.clone()))
    } else {
        None
    };

    sqlx::query(
        r#"
        INSERT INTO search_reconciliation_state (
            index_name, status, catalog_revision, catalog_revision_after,
            applied_revision, applied_revision_after, search_revision,
            search_revision_after, applied_search_revision,
            applied_search_revision_after, sqlite_work_count,
            index_document_count, index_unique_work_count, missing_work_ids,
            unexpected_work_ids, duplicate_documents, invalid_documents,
            sqlite_ids_sha256, index_ids_sha256, consecutive_passes,
            passed_since, last_checked_at, took_millis
        )
        VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
            ?21, ?22, ?23
        )
        ON CONFLICT(index_name) DO UPDATE SET
            status = excluded.status,
            catalog_revision = excluded.catalog_revision,
            catalog_revision_after = excluded.catalog_revision_after,
            applied_revision = excluded.applied_revision,
            applied_revision_after = excluded.applied_revision_after,
            search_revision = excluded.search_revision,
            search_revision_after = excluded.search_revision_after,
            applied_search_revision = excluded.applied_search_revision,
            applied_search_revision_after = excluded.applied_search_revision_after,
            sqlite_work_count = excluded.sqlite_work_count,
            index_document_count = excluded.index_document_count,
            index_unique_work_count = excluded.index_unique_work_count,
            missing_work_ids = excluded.missing_work_ids,
            unexpected_work_ids = excluded.unexpected_work_ids,
            duplicate_documents = excluded.duplicate_documents,
            invalid_documents = excluded.invalid_documents,
            sqlite_ids_sha256 = excluded.sqlite_ids_sha256,
            index_ids_sha256 = excluded.index_ids_sha256,
            consecutive_passes = excluded.consecutive_passes,
            passed_since = excluded.passed_since,
            last_checked_at = excluded.last_checked_at,
            took_millis = excluded.took_millis
        "#,
    )
    .bind(SHADOW_INDEX_NAME)
    .bind(effective_status)
    .bind(report.catalog_revision)
    .bind(report.catalog_revision_after)
    .bind(report.shadow_applied_revision)
    .bind(report.shadow_applied_revision_after)
    .bind(report.search_revision)
    .bind(report.search_revision_after)
    .bind(report.shadow_applied_search_revision)
    .bind(report.shadow_applied_search_revision_after)
    .bind(bounded_i64(report.sqlite_work_count))
    .bind(bounded_i64(report.shadow_document_count))
    .bind(bounded_i64(report.shadow_unique_work_count))
    .bind(bounded_i64(report.missing_work_ids))
    .bind(bounded_i64(report.unexpected_work_ids))
    .bind(bounded_i64(report.duplicate_documents))
    .bind(bounded_i64(report.invalid_documents))
    .bind(&report.sqlite_ids_sha256)
    .bind(&report.shadow_ids_sha256)
    .bind(consecutive_passes)
    .bind(passed_since)
    .bind(&now_text)
    .bind(i64::try_from(report.took_ms).unwrap_or(i64::MAX))
    .execute(&mut *transaction)
    .await?;

    match effective_status {
        "failed" => {
            let error = format!(
                "shadow search facts differ: missing={}, unexpected={}, duplicate={}, invalid={}",
                report.missing_work_ids,
                report.unexpected_work_ids,
                report.duplicate_documents,
                report.invalid_documents
            );
            sqlx::query(
                r#"
                UPDATE search_index_state
                SET ready = 0,
                    status = 'degraded',
                    last_error = ?1,
                    updated_at = ?2
                WHERE index_name = ?3
                "#,
            )
            .bind(error.chars().take(MAX_ERROR_BYTES).collect::<String>())
            .bind(&now_text)
            .bind(SHADOW_INDEX_NAME)
            .execute(&mut *transaction)
            .await?;
            // A persisted fact mismatch is a production-reader rollback
            // condition just like a Tantivy I/O/query error.  Keep the arm
            // transition in this transaction so health cannot expose
            // `degraded + armed` after a reconciliation failure.
            sqlx::query(
                r#"
                UPDATE search_reconciliation_state
                SET cutover_armed = 0
                WHERE index_name = ?1
                "#,
            )
            .bind(SHADOW_INDEX_NAME)
            .execute(&mut *transaction)
            .await?;
        }
        "passed" => {
            let ready = pending == 0
                && legacy_kinds == 0
                && current_applied_revision >= current_catalog_revision;
            let ready = ready && current_applied_search_revision >= current_search_revision;
            let status = if ready {
                "ready"
            } else if legacy_kinds > 0 {
                "shadow"
            } else {
                "catching-up"
            };
            sqlx::query(
                r#"
                UPDATE search_index_state
                SET ready = ?1,
                    status = ?2,
                    last_error = NULL,
                    updated_at = ?3
                WHERE index_name = ?4
                "#,
            )
            .bind(i64::from(ready))
            .bind(status)
            .bind(&now_text)
            .bind(SHADOW_INDEX_NAME)
            .execute(&mut *transaction)
            .await?;
        }
        _ => {}
    }
    transaction.commit().await?;
    Ok(effective_status)
}

fn bounded_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

pub(crate) async fn record_shadow_error(db: &Db, error: &str) -> Result<()> {
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    sqlx::query(
        r#"
        UPDATE search_index_state
        SET ready = 0,
            status = 'degraded',
            last_error = ?1,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE index_name = ?2
        "#,
    )
    .bind(error.chars().take(MAX_ERROR_BYTES).collect::<String>())
    .bind(SHADOW_INDEX_NAME)
    .execute(&mut *transaction)
    .await?;
    // A degraded index must never remain an apparently armed production
    // reader.  Persist the rollback in the same transaction as the index
    // error so health snapshots cannot observe `degraded + armed` across a
    // process restart or between recovery attempts.
    sqlx::query(
        r#"
        UPDATE search_reconciliation_state
        SET status = 'failed',
            consecutive_passes = 0,
            passed_since = NULL,
            cutover_armed = 0
        WHERE index_name = ?1
        "#,
    )
    .bind(SHADOW_INDEX_NAME)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn consume_once(
    db: &Db,
    resources: &ResourceGovernor,
    index_dir: PathBuf,
    worker_id: &str,
    limit: i64,
) -> Result<ConsumeSummary> {
    if worker_id.trim().is_empty() {
        return Err(AppError::BadRequest(
            "search outbox worker id must not be empty".to_string(),
        ));
    }
    let writer_heap_bytes = resources.limits().search_writer_heap_bytes;
    let _resource_lease = resources
        .reserve_background(ResourceClass::SearchWriter, writer_heap_bytes as u64, 0)
        .await?;
    // Reserve the bounded worker resource before taking the global consumer
    // lock. Otherwise a second background worker can hold the only resource
    // while this task holds SHADOW_OUTBOX_LOCK waiting for it, blocking the
    // incremental-reader gate and foreground canary requests behind the lock.
    let _consumer = SHADOW_OUTBOX_LOCK.lock().await;
    let claimed = claim_items(db, worker_id, limit).await?;
    if claimed.is_empty() {
        return Ok(ConsumeSummary::default());
    }
    if let Some(item) = claimed
        .iter()
        .find(|item| item.payload_version != OUTBOX_PAYLOAD_VERSION)
    {
        let message = format!(
            "search outbox payload version {} is incompatible with consumer version {}",
            item.payload_version, OUTBOX_PAYLOAD_VERSION
        );
        release_claims(db, worker_id, &claimed, &message).await?;
        return Err(AppError::Other(message));
    }

    let documents = match load_documents(db, &claimed).await {
        Ok(documents) => documents,
        Err(error) => {
            // A transient SQLite/read failure should not leave a claim held
            // for the full stale-claim window.  Crash/kill recovery still
            // relies on the timestamp fence, but ordinary errors can release
            // immediately and preserve interactive search lag.
            release_claims(db, worker_id, &claimed, &error.to_string()).await?;
            return Err(error);
        }
    };
    let mutations = claimed
        .iter()
        .map(|item| IndexMutation {
            work_id: item.work_id,
            document: (item.operation == "upsert")
                .then(|| documents.get(&item.work_id).cloned())
                .flatten(),
        })
        .collect::<Vec<_>>();
    let committed = mutations.len();
    let apply_result = tokio::task::spawn_blocking(move || {
        apply_index_batch(&index_dir, mutations, writer_heap_bytes)
    })
    .await;
    let apply_result = match apply_result {
        Ok(result) => result,
        Err(error) => {
            let panic_error = AppError::Other(format!("search outbox worker panicked: {error}"));
            release_claims(db, worker_id, &claimed, &panic_error.to_string()).await?;
            return Err(panic_error);
        }
    };
    let indexed_documents = match apply_result {
        Ok(count) => count,
        Err(error) => {
            release_claims(db, worker_id, &claimed, &error.to_string()).await?;
            return Err(error);
        }
    };

    // Persist the live Tantivy count after the commit and before the outbox
    // acknowledgement. If the process fails after this point, replaying the
    // claim is idempotent and will refresh the same count again.
    persist_shadow_indexed_documents(db, indexed_documents).await?;

    // Tantivy commit completed before this acknowledgement. If ack fails, the
    // claim expires and delete+add is safely replayed.
    let acknowledged = acknowledge_claims(db, worker_id, &claimed).await?;
    Ok(ConsumeSummary {
        claimed: claimed.len(),
        committed,
        acknowledged,
    })
}

async fn claim_items(db: &Db, worker_id: &str, limit: i64) -> Result<Vec<ClaimedOutboxItem>> {
    let now = Utc::now();
    // SQLite migration defaults use a millisecond UTC `Z` string.  Binding a
    // chrono value directly produces an RFC3339 `+00:00` suffix, which sorts
    // *after* `Z` when both values fall in the same millisecond and can make a
    // freshly-created item look unavailable.  Keep the queue comparison in
    // the same canonical representation as the stored/default values.
    let now_text = now.to_rfc3339_opts(SecondsFormat::Millis, true);
    let stale_before_text = (now - ChronoDuration::seconds(CLAIM_STALE_AFTER_SECONDS))
        .to_rfc3339_opts(SecondsFormat::Millis, true);
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    let rows = sqlx::query(
        r#"
        UPDATE search_outbox
        SET claimed_by = ?1,
            claimed_at = ?2,
            attempts = attempts + 1,
            last_error = NULL,
            updated_at = ?2
        WHERE work_id IN (
            SELECT work_id
            FROM search_outbox
            WHERE committed_at IS NULL
              AND available_at <= ?2
              AND (claimed_at IS NULL OR claimed_at < ?3)
            ORDER BY search_revision, work_id
            LIMIT ?4
        )
        RETURNING work_id, operation, catalog_revision, search_revision, payload_version, attempts
        "#,
    )
    .bind(worker_id)
    .bind(&now_text)
    .bind(&stale_before_text)
    .bind(limit.clamp(1, MAX_OUTBOX_BATCH))
    .fetch_all(&mut *transaction)
    .await?;
    transaction.commit().await?;
    let mut items = rows
        .into_iter()
        .map(|row| ClaimedOutboxItem {
            work_id: row.get("work_id"),
            operation: row.get("operation"),
            catalog_revision: row.get("catalog_revision"),
            search_revision: row.get("search_revision"),
            payload_version: row.get("payload_version"),
            attempts: row.get("attempts"),
        })
        .collect::<Vec<_>>();
    items.sort_by_key(|item| (item.search_revision, item.work_id));
    Ok(items)
}

async fn load_documents(
    db: &Db,
    items: &[ClaimedOutboxItem],
) -> Result<BTreeMap<i64, SearchIndexRow>> {
    let upsert_ids = items
        .iter()
        .filter(|item| item.operation == "upsert")
        .map(|item| item.work_id)
        .collect::<Vec<_>>();
    if upsert_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let ids_json = serde_json::to_string(&upsert_ids)
        .map_err(|error| AppError::Other(format!("failed to encode outbox ids: {error}")))?;
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let rows = sqlx::query(
        r#"
        SELECT
            work.id,
            work.kind,
            work.title,
            work.category,
            work.description,
            work.source_path,
            GROUP_CONCAT(
                DISTINCT tag.namespace || ':' || tag.key || ' ' || tag.label || ' ' ||
                         COALESCE(tag.translated_label, '')
            ) AS tags
        FROM works AS work
        LEFT JOIN work_tags AS work_tag ON work_tag.work_id = work.id
        LEFT JOIN tags AS tag ON tag.id = work_tag.tag_id
        WHERE work.deleted_at IS NULL
          AND work.id IN (
            SELECT CAST(value AS INTEGER) FROM json_each(?1)
        )
        GROUP BY work.id
        "#,
    )
    .bind(ids_json)
    .fetch_all(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let document = SearchIndexRow {
                id: row.get("id"),
                kind: row.get("kind"),
                title: row.get("title"),
                category: row.get("category"),
                description: row.get("description"),
                source_path: row.get("source_path"),
                tags: row.get("tags"),
            };
            (document.id, document)
        })
        .collect())
}

fn apply_index_batch(
    index_dir: &Path,
    mutations: Vec<IndexMutation>,
    writer_heap_bytes: usize,
) -> Result<usize> {
    std::fs::create_dir_all(index_dir)?;
    let (index, fields) = open_or_create_index(index_dir)?;
    let mut writer = open_search_writer(&index, writer_heap_bytes)?;
    for mutation in mutations {
        let work_id = mutation.work_id.to_string();
        writer.delete_term(Term::from_field_text(fields.work_id, &work_id));
        let Some(row) = mutation.document else {
            continue;
        };
        let body = row.body_text();
        writer
            .add_document(doc!(
                fields.work_id => work_id,
                fields.kind => row.kind,
                fields.title => row.title.clone(),
                fields.title_ngram => row.title,
                fields.body => body.clone(),
                fields.body_ngram => body,
            ))
            .map_err(search_error)?;
    }
    writer.commit().map_err(search_error)?;
    writer.wait_merging_threads().map_err(search_error)?;
    let reader = index.reader().map_err(search_error)?;
    reader.reload().map_err(search_error)?;
    usize::try_from(reader.searcher().num_docs()).map_err(|_| {
        AppError::Other("shadow search document count exceeds platform range".to_string())
    })
}

async fn persist_shadow_indexed_documents(db: &Db, indexed_documents: usize) -> Result<()> {
    let indexed_documents = i64::try_from(indexed_documents).map_err(|_| {
        AppError::Other("shadow search document count exceeds SQLite range".to_string())
    })?;
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    sqlx::query(
        r#"
        UPDATE search_index_state
        SET indexed_documents = ?1,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE index_name = ?2
        "#,
    )
    .bind(indexed_documents)
    .bind(SHADOW_INDEX_NAME)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

async fn acknowledge_claims(db: &Db, worker_id: &str, items: &[ClaimedOutboxItem]) -> Result<u64> {
    let claims = claim_json(items)?;
    let now_text = canonical_timestamp(Utc::now());
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    let affected = sqlx::query(
        r#"
        UPDATE search_outbox
        SET committed_at = ?1,
            claimed_by = NULL,
            claimed_at = NULL,
            last_error = NULL,
            updated_at = ?1
        WHERE claimed_by = ?2
          AND EXISTS (
              SELECT 1
              FROM json_each(?3) AS claim
              WHERE CAST(json_extract(claim.value, '$.work_id') AS INTEGER) =
                        search_outbox.work_id
                AND CAST(json_extract(claim.value, '$.search_revision') AS INTEGER) =
                        search_outbox.search_revision
          )
        "#,
    )
    .bind(&now_text)
    .bind(worker_id)
    .bind(claims)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    transaction.commit().await?;
    Ok(affected)
}

async fn release_claims(
    db: &Db,
    worker_id: &str,
    items: &[ClaimedOutboxItem],
    error: &str,
) -> Result<u64> {
    let claims = claim_json(items)?;
    let attempts = items.iter().map(|item| item.attempts).max().unwrap_or(1);
    let exponent = attempts.clamp(0, 8) as u32;
    let delay_seconds = 2_i64.pow(exponent).clamp(2, 300);
    let now_text = canonical_timestamp(Utc::now());
    let retry_at_text = canonical_timestamp(Utc::now() + ChronoDuration::seconds(delay_seconds));
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    let affected = sqlx::query(
        r#"
        UPDATE search_outbox
        SET available_at = ?1,
            claimed_by = NULL,
            claimed_at = NULL,
            last_error = ?2,
            updated_at = ?3
        WHERE claimed_by = ?4
          AND EXISTS (
              SELECT 1
              FROM json_each(?5) AS claim
              WHERE CAST(json_extract(claim.value, '$.work_id') AS INTEGER) =
                        search_outbox.work_id
                AND CAST(json_extract(claim.value, '$.search_revision') AS INTEGER) =
                        search_outbox.search_revision
          )
        "#,
    )
    .bind(&retry_at_text)
    .bind(error.chars().take(MAX_ERROR_BYTES).collect::<String>())
    .bind(&now_text)
    .bind(worker_id)
    .bind(claims)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    transaction.commit().await?;
    Ok(affected)
}

fn claim_json(items: &[ClaimedOutboxItem]) -> Result<String> {
    serde_json::to_string(
        &items
            .iter()
            .map(|item| {
                json!({
                    "work_id": item.work_id,
                    "revision": item.catalog_revision,
                    "search_revision": item.search_revision,
                })
            })
            .collect::<Vec<Value>>(),
    )
    .map_err(|error| AppError::Other(format!("failed to encode outbox claims: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::{ResourceClass, ResourceLimits};

    fn database_url(temp: &tempfile::TempDir) -> String {
        format!("sqlite://{}", temp.path().join("outbox.sqlite").display())
    }

    async fn test_db() -> (tempfile::TempDir, Db) {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        (temp, db)
    }

    #[tokio::test]
    async fn shadow_error_update_uses_the_single_writer_gate() {
        let (_temp, db) = test_db().await;
        sqlx::query(
            r#"
            UPDATE search_reconciliation_state
            SET status = 'passed',
                consecutive_passes = 2,
                passed_since = '2026-08-22T00:00:00.000Z',
                cutover_armed = 1
            WHERE index_name = ?1
            "#,
        )
        .bind(SHADOW_INDEX_NAME)
        .execute(db.pool())
        .await
        .unwrap();
        let before = db.write_snapshot();

        record_shadow_error(&db, "synthetic shadow failure")
            .await
            .unwrap();

        let after = db.write_snapshot();
        assert_eq!(after.completed, before.completed + 1);
        let row = sqlx::query(
            "SELECT ready, status, last_error FROM search_index_state WHERE index_name = ?1",
        )
        .bind(SHADOW_INDEX_NAME)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(row.get::<i64, _>("ready"), 0);
        assert_eq!(row.get::<String, _>("status"), "degraded");
        assert_eq!(
            row.get::<String, _>("last_error"),
            "synthetic shadow failure"
        );
        let reconciliation = reconciliation_status(&db).await.unwrap();
        assert_eq!(reconciliation.status, "failed");
        assert_eq!(reconciliation.consecutive_passes, 0);
        assert!(!reconciliation.cutover_armed);
    }

    #[tokio::test]
    async fn search_status_helpers_commit_tracked_read_snapshots() {
        let (_temp, db) = test_db().await;
        let before = db.runtime_snapshot().await.read_snapshot;

        let _ = status(&db).await.unwrap();
        let _ = shadow_index_status(&db).await.unwrap();
        let _ = reconciliation_status(&db).await.unwrap();

        let after = db.runtime_snapshot().await.read_snapshot;
        assert_eq!(after.active, 0);
        assert_eq!(after.samples, before.samples + 3);
        assert_eq!(after.completed, before.completed + 3);
        assert_eq!(after.implicit_rollbacks, before.implicit_rollbacks);
    }

    #[tokio::test]
    async fn reconciliation_status_snapshot_reads_outbox_and_shadow_together() {
        let (_temp, db) = test_db().await;
        let before = db.runtime_snapshot().await.read_snapshot;

        let snapshot = shadow_reconciliation_snapshot(&db).await.unwrap();

        let after = db.runtime_snapshot().await.read_snapshot;
        assert_eq!(after.active, 0);
        assert_eq!(after.samples, before.samples + 1);
        assert_eq!(after.completed, before.completed + 1);
        assert_eq!(after.implicit_rollbacks, before.implicit_rollbacks);
        assert_eq!(snapshot.shadow.index_name, SHADOW_INDEX_NAME);
        assert_eq!(
            snapshot.outbox.shadow_applied_revision,
            snapshot.shadow.applied_revision
        );
    }

    #[tokio::test]
    async fn search_promotion_snapshot_commits_one_tracked_read() {
        let (_temp, db) = test_db().await;
        let before = db.runtime_snapshot().await.read_snapshot;

        let snapshot = search_promotion_snapshot(&db).await.unwrap();

        let after = db.runtime_snapshot().await.read_snapshot;
        assert_eq!(after.active, 0);
        assert_eq!(after.samples, before.samples + 1);
        assert_eq!(after.completed, before.completed + 1);
        assert_eq!(after.implicit_rollbacks, before.implicit_rollbacks);
        assert_eq!(snapshot.shadow.index_name, SHADOW_INDEX_NAME);
        assert_eq!(snapshot.reconciliation.index_name, SHADOW_INDEX_NAME);
        assert_eq!(
            snapshot.outbox.shadow_applied_revision,
            snapshot.shadow.applied_revision
        );
    }

    #[tokio::test]
    async fn outbox_resource_wait_does_not_hold_the_consumer_lock() {
        let (_temp, db) = test_db().await;
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let writer_heap_bytes = resources.limits().search_writer_heap_bytes;
        let held_lease = resources
            .reserve_background(
                ResourceClass::SearchWriter,
                u64::try_from(writer_heap_bytes).unwrap(),
                0,
            )
            .await
            .unwrap();
        let consume = consume_once(
            &db,
            &resources,
            tempfile::tempdir().unwrap().path().to_path_buf(),
            "blocked-worker",
            32,
        );
        tokio::pin!(consume);

        // The worker is blocked on its bounded SearchWriter resource. It must
        // not acquire SHADOW_OUTBOX_LOCK before that wait, or a foreground
        // incremental-reader gate could be held behind an unrelated resource
        // waiter.
        let lock_result = tokio::select! {
            result = &mut consume => panic!("consume_once completed unexpectedly: {result:?}"),
            lock = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                SHADOW_OUTBOX_LOCK.lock(),
            ) => lock,
        };
        drop(lock_result.expect("resource wait must not hold the consumer lock"));
        drop(held_lease);
    }

    #[tokio::test]
    async fn incremental_reader_gate_lock_wait_is_bounded() {
        let lock = Mutex::new(());
        let _guard = lock.lock().await;
        let started = std::time::Instant::now();

        let error = acquire_lock_with_timeout(&lock, Duration::from_millis(20))
            .await
            .expect_err("a held shadow lock must fail closed at the deadline");

        assert!(error
            .to_string()
            .contains("incremental search reader gate lock wait exceeded"));
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn incremental_reader_gate_rejects_building_shadow_before_lock_wait() {
        let (_temp, db) = test_db().await;
        sqlx::query(
            "UPDATE search_index_state SET ready = 0, status = 'building' WHERE index_name = ?1",
        )
        .bind(SHADOW_INDEX_NAME)
        .execute(db.pool())
        .await
        .unwrap();

        let before = db.runtime_snapshot().await.read_snapshot;
        let started = std::time::Instant::now();
        let error = ensure_incremental_reader_gate(&db, Duration::from_secs(1))
            .await
            .expect_err("building shadow must fail closed before the lock path");
        let after = db.runtime_snapshot().await.read_snapshot;

        assert!(error
            .to_string()
            .contains("incremental search reader shadow is not ready"));
        assert!(started.elapsed() < Duration::from_millis(500));
        assert_eq!(after.active, 0);
        assert_eq!(after.samples, before.samples + 1);
        assert_eq!(after.completed, before.completed + 1);
        assert_eq!(after.implicit_rollbacks, before.implicit_rollbacks);
    }

    async fn work_at_path(db: &Db, title: &str, source_path: &str) -> i64 {
        db.upsert_work(
            "novel",
            title,
            Some(source_path),
            Some("Light Novel"),
            Some("description"),
            None,
            json!({}),
        )
        .await
        .unwrap()
    }

    async fn work(db: &Db, title: &str) -> i64 {
        work_at_path(db, title, &format!("/novels/{title}.epub")).await
    }

    async fn advance_catalog_revision(db: &Db) -> i64 {
        sqlx::query(
            r#"
            UPDATE catalog_state
            SET revision = revision + 1,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE singleton = 1
            "#,
        )
        .execute(db.pool())
        .await
        .unwrap();
        catalog_revision(db).await
    }

    async fn enqueue(db: &Db, work_id: i64, operation: &str, revision: i64, payload_version: i64) {
        let now_text = canonical_timestamp(Utc::now());
        let search_revision = sqlx::query_scalar::<_, i64>(
            r#"
            UPDATE search_source_state
            SET revision = revision + 1,
                updated_at = ?1
            WHERE singleton = 1
            RETURNING revision
            "#,
        )
        .bind(&now_text)
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO search_outbox (
                work_id, operation, catalog_revision, search_revision, payload_version,
                attempts, available_at, committed_at, updated_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, NULL, ?6)
            ON CONFLICT(work_id) DO UPDATE SET
                operation = excluded.operation,
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
            "#,
        )
        .bind(work_id)
        .bind(operation)
        .bind(revision)
        .bind(search_revision)
        .bind(payload_version)
        .bind(now_text)
        .execute(db.pool())
        .await
        .unwrap();
    }

    async fn catalog_revision(db: &Db) -> i64 {
        sqlx::query_scalar("SELECT revision FROM catalog_state WHERE singleton = 1")
            .fetch_one(db.pool())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn shadow_baseline_is_complete_revisioned_and_restart_safe() {
        let (temp, db) = test_db().await;
        let first = work(&db, "BaselineFirst").await;
        let second = work(&db, "BaselineSecond").await;
        let revision = catalog_revision(&db).await;
        enqueue(&db, first, "upsert", revision, 1).await;
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let index_dir = temp.path().join("shadow-index");

        assert!(ensure_shadow_baseline(&db, &resources, index_dir.clone())
            .await
            .unwrap());
        let hits = super::super::query_search_index_blocking(
            index_dir.clone(),
            "Baseline".to_string(),
            10,
        )
        .unwrap()
        .into_iter()
        .map(|hit| hit.work_id)
        .collect::<Vec<_>>();
        assert_eq!(hits.len(), 2);
        assert!(hits.contains(&first));
        assert!(hits.contains(&second));
        let state = shadow_index_status(&db).await.unwrap();
        assert_eq!(state.schema_version, SHADOW_SCHEMA_VERSION);
        assert_eq!(state.baseline_revision, revision);
        assert_eq!(state.applied_revision, revision);
        assert_eq!(state.indexed_documents, 2);
        assert_eq!(state.status, "shadow");
        assert!(!state.ready);
        assert_eq!(status(&db).await.unwrap().pending, 0);

        assert!(!ensure_shadow_baseline(&db, &resources, index_dir)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn forced_shadow_rebuild_recovers_degraded_state_and_resets_cutover() {
        let (temp, db) = test_db().await;
        let work_id = work(&db, "RecoverableShadow").await;
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let index_dir = temp.path().join("shadow-recovery-index");
        assert!(ensure_shadow_baseline(&db, &resources, index_dir.clone())
            .await
            .unwrap());

        sqlx::query(
            "UPDATE search_index_state SET ready = 0, status = 'degraded', last_error = 'simulated' WHERE index_name = 'shadow-v3'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE search_reconciliation_state SET status = 'failed', cutover_armed = 1 WHERE index_name = 'shadow-v3'",
        )
        .execute(db.pool())
        .await
        .unwrap();

        let documents = rebuild_shadow_index(&db, &resources, index_dir.clone())
            .await
            .unwrap();
        assert_eq!(documents, 1);
        let state = shadow_index_status(&db).await.unwrap();
        assert_eq!(state.status, "shadow");
        assert!(!state.ready);
        assert_eq!(state.indexed_documents, 1);
        let reconciliation = reconciliation_status(&db).await.unwrap();
        assert_eq!(reconciliation.status, "unknown");
        assert!(!reconciliation.cutover_armed);
        let hits =
            super::super::query_search_index_blocking(index_dir, "Recoverable".to_string(), 10)
                .unwrap();
        assert_eq!(
            hits.iter().map(|hit| hit.work_id).collect::<Vec<_>>(),
            vec![work_id]
        );
    }

    #[tokio::test]
    async fn empty_shadow_baseline_is_persisted_once_at_revision_zero() {
        let (temp, db) = test_db().await;
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let index_dir = temp.path().join("empty-shadow-index");
        sqlx::query("UPDATE catalog_state SET revision = 0 WHERE singleton = 1")
            .execute(db.pool())
            .await
            .unwrap();

        assert!(ensure_shadow_baseline(&db, &resources, index_dir.clone())
            .await
            .unwrap());
        let first = shadow_index_status(&db).await.unwrap();
        assert_eq!(first.baseline_revision, 0);
        assert_eq!(first.applied_revision, 0);
        assert_eq!(first.indexed_documents, 0);
        assert_eq!(first.status, "shadow");

        // A restart/poll must reuse the empty index rather than rebuilding it
        // indefinitely while the catalog revision remains zero.
        assert!(!ensure_shadow_baseline(&db, &resources, index_dir)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn incremental_consume_persists_live_document_count() {
        let (temp, db) = test_db().await;
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let index_dir = temp.path().join("shadow-index");
        ensure_shadow_baseline(&db, &resources, index_dir.clone())
            .await
            .unwrap();

        let work_id = work(&db, "CountedIncrementalFixture").await;
        consume_once(&db, &resources, index_dir.clone(), "count-worker", 32)
            .await
            .unwrap();

        let state = shadow_index_status(&db).await.unwrap();
        assert_eq!(state.indexed_documents, 1);
        assert_eq!(
            super::super::query_search_index_blocking(
                index_dir,
                "CountedIncrementalFixture".to_string(),
                10,
            )
            .unwrap()
            .into_iter()
            .map(|hit| hit.work_id)
            .collect::<Vec<_>>(),
            vec![work_id]
        );
    }

    #[tokio::test]
    async fn catalog_only_maintenance_revision_does_not_create_search_lag() {
        let (temp, db) = test_db().await;
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let index_dir = temp.path().join("shadow-index");
        work(&db, "CatalogMaintenanceFixture").await;
        ensure_shadow_baseline(&db, &resources, index_dir)
            .await
            .unwrap();
        let baseline = shadow_index_status(&db).await.unwrap().baseline_revision;

        sqlx::query("UPDATE catalog_state SET revision = revision + 1 WHERE singleton = 1")
            .execute(db.pool())
            .await
            .unwrap();

        let current_catalog_revision = catalog_revision(&db).await;
        assert!(current_catalog_revision > baseline);
        let outbox = status(&db).await.unwrap();
        assert_eq!(outbox.catalog_revision, baseline);
        assert_eq!(outbox.shadow_applied_revision, baseline);
        assert_eq!(outbox.revision_lag, 0);
    }

    #[tokio::test]
    async fn incremental_reader_gate_accepts_a_reconciled_empty_revision_zero_catalog() {
        let (temp, db) = test_db().await;
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let index_dir = temp.path().join("empty-reader-gate-index");
        sqlx::query("UPDATE catalog_state SET revision = 0 WHERE singleton = 1")
            .execute(db.pool())
            .await
            .unwrap();
        ensure_shadow_baseline(&db, &resources, index_dir)
            .await
            .unwrap();
        sqlx::query("UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2'")
            .execute(db.pool())
            .await
            .unwrap();
        let progress = refresh_shadow_progress(&db).await.unwrap();
        assert!(progress.ready);
        assert_eq!(progress.applied_revision, 0);
        sqlx::query(
            r#"
            UPDATE search_reconciliation_state
            SET status = 'passed',
                catalog_revision = 0,
                catalog_revision_after = 0,
                applied_revision = 0,
                applied_revision_after = 0,
                sqlite_work_count = 0,
                index_document_count = 0,
                index_unique_work_count = 0,
                sqlite_ids_sha256 = 'af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc',
                index_ids_sha256 = 'af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc',
                consecutive_passes = 1,
                passed_since = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                last_checked_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE index_name = 'shadow-v3'
            "#,
        )
        .execute(db.pool())
        .await
        .unwrap();

        let write_before_read = db.write_snapshot();
        let unarmed = validate_incremental_reader_gate(&db, Duration::from_secs(1))
            .await
            .expect_err("read-only validation must reject an unarmed reader");
        let write_after_unarmed_read = db.write_snapshot();
        assert_eq!(
            write_after_unarmed_read.acquire_samples, write_before_read.acquire_samples,
            "unarmed read-only validation must not enter the write queue"
        );
        assert_eq!(
            write_after_unarmed_read.completed, write_before_read.completed,
            "unarmed read-only validation must not complete a write lease"
        );
        assert!(unarmed.to_string().contains("cutover is not armed"));
        assert!(!reconciliation_status(&db).await.unwrap().cutover_armed);

        ensure_incremental_reader_gate(&db, Duration::from_secs(1))
            .await
            .unwrap();
        assert!(reconciliation_status(&db).await.unwrap().cutover_armed);

        let write_before_armed_read = db.write_snapshot();
        validate_incremental_reader_gate(&db, Duration::from_secs(1))
            .await
            .unwrap();
        let write_after_armed_read = db.write_snapshot();
        assert_eq!(
            write_after_armed_read.acquire_samples, write_before_armed_read.acquire_samples,
            "armed read-only validation must not enter the write queue"
        );
        assert_eq!(
            write_after_armed_read.completed, write_before_armed_read.completed,
            "armed read-only validation must not complete a write lease"
        );
    }

    #[tokio::test]
    async fn shadow_incremental_progress_only_becomes_ready_after_all_kind_cutovers() {
        let (temp, db) = test_db().await;
        let work_id = work_at_path(
            &db,
            "BeforeIncremental",
            "/novels/stable-title-contract.epub",
        )
        .await;
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let index_dir = temp.path().join("shadow-index");
        ensure_shadow_baseline(&db, &resources, index_dir.clone())
            .await
            .unwrap();

        sqlx::query("UPDATE works SET title = 'AfterIncremental' WHERE id = ?1")
            .bind(work_id)
            .execute(db.pool())
            .await
            .unwrap();
        let revision = catalog_revision(&db).await;
        enqueue(&db, work_id, "upsert", revision, 1).await;
        consume_once(&db, &resources, index_dir.clone(), "shadow-progress", 32)
            .await
            .unwrap();
        let shadow = refresh_shadow_progress(&db).await.unwrap();
        assert_eq!(shadow.applied_revision, revision);
        assert_eq!(shadow.status, "shadow");
        assert!(!shadow.ready);
        assert!(super::super::query_search_index_blocking(
            index_dir.clone(),
            "BeforeIncremental".to_string(),
            10,
        )
        .unwrap()
        .is_empty());
        assert_eq!(
            super::super::query_search_index_blocking(
                index_dir,
                "AfterIncremental".to_string(),
                10,
            )
            .unwrap()
            .len(),
            1
        );

        sqlx::query("UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2'")
            .execute(db.pool())
            .await
            .unwrap();
        let ready = refresh_shadow_progress(&db).await.unwrap();
        assert_eq!(ready.status, "ready");
        assert!(ready.ready);
    }

    #[tokio::test]
    async fn incremental_reader_gate_arms_once_and_survives_normal_revision_churn() {
        let (temp, db) = test_db().await;
        let work_id = work_at_path(&db, "ReaderGateInitial", "/novels/reader-gate.epub").await;
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let index_dir = temp.path().join("shadow-index");
        ensure_shadow_baseline(&db, &resources, index_dir.clone())
            .await
            .unwrap();
        sqlx::query("UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2'")
            .execute(db.pool())
            .await
            .unwrap();
        let revision = catalog_revision(&db).await;
        let indexed_documents = sqlx::query_scalar::<_, i64>(
            "SELECT indexed_documents FROM search_index_state WHERE index_name = 'shadow-v3'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(
            r#"
            UPDATE search_index_state
            SET ready = 1, status = 'ready', applied_revision = ?1
            WHERE index_name = 'shadow-v3'
            "#,
        )
        .bind(revision)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            r#"
            UPDATE search_reconciliation_state
            SET status = 'passed',
                catalog_revision = ?1,
                catalog_revision_after = ?1,
                applied_revision = ?1,
                applied_revision_after = ?1,
                sqlite_work_count = 1,
                index_document_count = ?2,
                index_unique_work_count = ?2,
                sqlite_ids_sha256 = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                index_ids_sha256 = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                consecutive_passes = 1,
                passed_since = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                last_checked_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE index_name = 'shadow-v3'
            "#,
        )
        .bind(revision)
        .bind(indexed_documents)
        .execute(db.pool())
        .await
        .unwrap();

        sqlx::query(
            "UPDATE search_reconciliation_state SET index_ids_sha256 = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' WHERE index_name = 'shadow-v3'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        assert!(ensure_incremental_reader_gate(&db, Duration::from_secs(1))
            .await
            .is_err());
        sqlx::query(
            "UPDATE search_reconciliation_state SET index_ids_sha256 = sqlite_ids_sha256 WHERE index_name = 'shadow-v3'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        ensure_incremental_reader_gate(&db, Duration::from_secs(1))
            .await
            .unwrap();
        assert!(reconciliation_status(&db).await.unwrap().cutover_armed);

        sqlx::query(
            "UPDATE search_reconciliation_state SET sqlite_ids_sha256 = NULL WHERE index_name = 'shadow-v3'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        assert!(ensure_incremental_reader_gate(&db, Duration::from_secs(1))
            .await
            .is_err());
        sqlx::query(
            "UPDATE search_reconciliation_state SET sqlite_ids_sha256 = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' WHERE index_name = 'shadow-v3'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        ensure_incremental_reader_gate(&db, Duration::from_secs(1))
            .await
            .unwrap();

        // A normal write creates a newer outbox revision. It deliberately
        // makes the persisted facts report stale; the armed reader may still
        // resume as soon as the shadow index catches up.
        sqlx::query("UPDATE works SET title = 'ReaderGateUpdated' WHERE id = ?1")
            .bind(work_id)
            .execute(db.pool())
            .await
            .unwrap();
        let next_revision = advance_catalog_revision(&db).await;
        sqlx::query(
            r#"
            INSERT INTO search_outbox (
                work_id, operation, catalog_revision, search_revision, payload_version
            )
            VALUES (?1, 'upsert', ?2, ?3, 1)
            ON CONFLICT(work_id) DO UPDATE SET
                operation = excluded.operation,
                catalog_revision = excluded.catalog_revision,
                search_revision = excluded.search_revision,
                payload_version = excluded.payload_version,
                attempts = 0,
                available_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                claimed_by = NULL,
                claimed_at = NULL,
                committed_at = NULL,
                last_error = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            "#,
        )
        .bind(work_id)
        .bind(next_revision)
        .bind(
            sqlx::query_scalar::<_, i64>(
                "UPDATE search_source_state SET revision = revision + 1 WHERE singleton = 1 RETURNING revision",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
        )
        .execute(db.pool())
        .await
        .unwrap();
        assert!(ensure_incremental_reader_gate(&db, Duration::from_secs(1))
            .await
            .is_err());

        consume_once(&db, &resources, index_dir, "reader-gate", 32)
            .await
            .unwrap();
        let progress = refresh_shadow_progress(&db).await.unwrap();
        assert!(progress.ready);
        assert_eq!(progress.applied_revision, next_revision);
        assert_eq!(reconciliation_status(&db).await.unwrap().status, "stale");
        ensure_incremental_reader_gate(&db, Duration::from_secs(1))
            .await
            .unwrap();
        assert!(reconciliation_status(&db).await.unwrap().cutover_armed);
    }

    #[tokio::test]
    async fn failed_reconciliation_stays_degraded_across_revision_churn() {
        let (temp, db) = test_db().await;
        let _work_id = work(&db, "FailedFactsFixture").await;
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let index_dir = temp.path().join("shadow-index");
        ensure_shadow_baseline(&db, &resources, index_dir)
            .await
            .unwrap();
        sqlx::query("UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2'")
            .execute(db.pool())
            .await
            .unwrap();
        let revision = catalog_revision(&db).await;
        sqlx::query(
            r#"
            UPDATE search_index_state
            SET ready = 1, status = 'ready', applied_revision = ?1
            WHERE index_name = 'shadow-v3'
            "#,
        )
        .bind(revision)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            r#"
            UPDATE search_reconciliation_state
            SET status = 'failed',
                catalog_revision = ?1,
                catalog_revision_after = ?1,
                applied_revision = ?1,
                applied_revision_after = ?1,
                missing_work_ids = 1,
                consecutive_passes = 0,
                cutover_armed = 1
            WHERE index_name = 'shadow-v3'
            "#,
        )
        .bind(revision)
        .execute(db.pool())
        .await
        .unwrap();

        advance_catalog_revision(&db).await;
        let refreshed = refresh_shadow_progress(&db).await.unwrap();
        assert!(!refreshed.ready);
        assert_eq!(refreshed.status, "degraded");
        let reconciliation = reconciliation_status(&db).await.unwrap();
        assert_eq!(reconciliation.status, "failed");
        assert!(!reconciliation.cutover_armed);
        sqlx::query("UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2'")
            .execute(db.pool())
            .await
            .unwrap();
        assert!(ensure_incremental_reader_gate(&db, Duration::from_secs(1))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn baseline_reset_clears_incremental_reader_arm() {
        let (temp, db) = test_db().await;
        let _work_id = work(&db, "ReaderGateBaseline").await;
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let index_dir = temp.path().join("shadow-index");
        ensure_shadow_baseline(&db, &resources, index_dir.clone())
            .await
            .unwrap();
        sqlx::query(
            "UPDATE search_reconciliation_state SET cutover_armed = 1 WHERE index_name = 'shadow-v3'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            r#"
            UPDATE search_index_state
            SET baseline_revision = 0,
                applied_revision = 0,
                ready = 0,
                status = 'empty'
            WHERE index_name = 'shadow-v3'
            "#,
        )
        .execute(db.pool())
        .await
        .unwrap();

        assert!(ensure_shadow_baseline(&db, &resources, index_dir)
            .await
            .unwrap());
        let reconciliation = reconciliation_status(&db).await.unwrap();
        assert!(!reconciliation.cutover_armed);
        assert_eq!(reconciliation.status, "unknown");
    }

    #[tokio::test]
    async fn source_path_terms_are_searchable_and_replace_independently_of_title() {
        let (temp, db) = test_db().await;
        let work_id = work_at_path(
            &db,
            "StablePathContractTitle",
            "/novels/PathTokenBefore.epub",
        )
        .await;
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let index_dir = temp.path().join("shadow-index");
        ensure_shadow_baseline(&db, &resources, index_dir.clone())
            .await
            .unwrap();
        assert_eq!(
            super::super::query_search_index_blocking(
                index_dir.clone(),
                "PathTokenBefore".to_string(),
                10,
            )
            .unwrap()
            .len(),
            1
        );

        sqlx::query("UPDATE works SET source_path = '/novels/PathTokenAfter.epub' WHERE id = ?1")
            .bind(work_id)
            .execute(db.pool())
            .await
            .unwrap();
        let revision = advance_catalog_revision(&db).await;
        enqueue(&db, work_id, "upsert", revision, 1).await;
        consume_once(&db, &resources, index_dir.clone(), "path-contract", 32)
            .await
            .unwrap();

        assert!(super::super::query_search_index_blocking(
            index_dir.clone(),
            "PathTokenBefore".to_string(),
            10,
        )
        .unwrap()
        .is_empty());
        assert_eq!(
            super::super::query_search_index_blocking(
                index_dir.clone(),
                "PathTokenAfter".to_string(),
                10,
            )
            .unwrap()
            .len(),
            1
        );
        assert_eq!(
            super::super::query_search_index_blocking(
                index_dir,
                "StablePathContractTitle".to_string(),
                10,
            )
            .unwrap()
            .len(),
            1
        );
    }

    #[tokio::test]
    async fn category_and_description_terms_replace_from_the_latest_snapshot() {
        let (temp, db) = test_db().await;
        let work_id = db
            .upsert_work(
                "novel",
                "StableMetadataContractTitle",
                Some("/novels/stable-metadata-contract.epub"),
                Some("CedarQuasar"),
                Some("AmberFalcon"),
                None,
                json!({}),
            )
            .await
            .unwrap();
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let index_dir = temp.path().join("shadow-index");
        ensure_shadow_baseline(&db, &resources, index_dir.clone())
            .await
            .unwrap();
        for token in ["CedarQuasar", "AmberFalcon"] {
            assert_eq!(
                super::super::query_search_index_blocking(
                    index_dir.clone(),
                    token.to_string(),
                    10,
                )
                .unwrap()
                .len(),
                1
            );
        }

        sqlx::query("UPDATE works SET category = 'VioletMeteor' WHERE id = ?1")
            .bind(work_id)
            .execute(db.pool())
            .await
            .unwrap();
        let revision = advance_catalog_revision(&db).await;
        enqueue(&db, work_id, "upsert", revision, 1).await;
        consume_once(&db, &resources, index_dir.clone(), "category-contract", 32)
            .await
            .unwrap();
        assert!(super::super::query_search_index_blocking(
            index_dir.clone(),
            "CedarQuasar".to_string(),
            10,
        )
        .unwrap()
        .is_empty());
        for token in ["VioletMeteor", "AmberFalcon"] {
            assert_eq!(
                super::super::query_search_index_blocking(
                    index_dir.clone(),
                    token.to_string(),
                    10,
                )
                .unwrap()
                .len(),
                1
            );
        }

        sqlx::query("UPDATE works SET description = 'SilverOrchid' WHERE id = ?1")
            .bind(work_id)
            .execute(db.pool())
            .await
            .unwrap();
        let revision = advance_catalog_revision(&db).await;
        enqueue(&db, work_id, "upsert", revision, 1).await;
        consume_once(
            &db,
            &resources,
            index_dir.clone(),
            "description-contract",
            32,
        )
        .await
        .unwrap();
        assert!(super::super::query_search_index_blocking(
            index_dir.clone(),
            "AmberFalcon".to_string(),
            10,
        )
        .unwrap()
        .is_empty());
        for token in ["VioletMeteor", "SilverOrchid"] {
            assert_eq!(
                super::super::query_search_index_blocking(
                    index_dir.clone(),
                    token.to_string(),
                    10,
                )
                .unwrap()
                .len(),
                1
            );
        }
    }

    #[tokio::test]
    async fn tag_terms_replace_independently_of_title_and_source_path() {
        let (temp, db) = test_db().await;
        let work_id = work_at_path(
            &db,
            "StableTagContractTitle",
            "/novels/stable-tag-contract.epub",
        )
        .await;
        let tag_id = db
            .upsert_tag(
                "contract",
                "SearchKeyToken",
                "TagTokenBefore",
                Some("TranslationTokenBefore"),
                None,
                "test",
                None,
                None,
            )
            .await
            .unwrap();
        db.link_tag(work_id, tag_id).await.unwrap();
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let index_dir = temp.path().join("shadow-index");
        ensure_shadow_baseline(&db, &resources, index_dir.clone())
            .await
            .unwrap();
        for token in ["SearchKeyToken", "TagTokenBefore", "TranslationTokenBefore"] {
            assert_eq!(
                super::super::query_search_index_blocking(
                    index_dir.clone(),
                    token.to_string(),
                    10,
                )
                .unwrap()
                .len(),
                1
            );
        }

        sqlx::query(
            "UPDATE tags SET label = 'TagTokenAfter', translated_label = 'TranslationTokenAfter' WHERE id = ?1",
        )
        .bind(tag_id)
        .execute(db.pool())
        .await
        .unwrap();
        let revision = advance_catalog_revision(&db).await;
        enqueue(&db, work_id, "upsert", revision, 1).await;
        consume_once(&db, &resources, index_dir.clone(), "tag-contract", 32)
            .await
            .unwrap();

        assert!(super::super::query_search_index_blocking(
            index_dir.clone(),
            "TagTokenBefore".to_string(),
            10,
        )
        .unwrap()
        .is_empty());
        assert!(super::super::query_search_index_blocking(
            index_dir.clone(),
            "TranslationTokenBefore".to_string(),
            10,
        )
        .unwrap()
        .is_empty());
        for token in [
            "SearchKeyToken",
            "TagTokenAfter",
            "TranslationTokenAfter",
            "StableTagContractTitle",
        ] {
            assert_eq!(
                super::super::query_search_index_blocking(
                    index_dir.clone(),
                    token.to_string(),
                    10,
                )
                .unwrap()
                .len(),
                1
            );
        }
    }

    #[tokio::test]
    async fn consumer_commits_then_acknowledges_upserts_and_deletes() {
        let (temp, db) = test_db().await;
        let work_id = work(&db, "IncrementalFixture").await;
        enqueue(&db, work_id, "upsert", 10, 1).await;
        let before = status(&db).await.unwrap();
        assert_eq!(before.pending, 1);
        assert_eq!(before.claimed, 0);
        assert_eq!(before.committed, 0);
        assert_eq!(before.oldest_pending_revision, Some(10));
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let index_dir = temp.path().join("shadow-index");
        let summary = consume_once(&db, &resources, index_dir.clone(), "worker-upsert", 32)
            .await
            .unwrap();
        assert_eq!(
            summary,
            ConsumeSummary {
                claimed: 1,
                committed: 1,
                acknowledged: 1,
            }
        );
        assert_eq!(
            super::super::query_search_index_blocking(
                index_dir.clone(),
                "IncrementalFixture".to_string(),
                10,
            )
            .unwrap()
            .into_iter()
            .map(|hit| hit.work_id)
            .collect::<Vec<_>>(),
            vec![work_id]
        );
        assert!(sqlx::query_scalar::<_, String>(
            "SELECT committed_at FROM search_outbox WHERE work_id = ?1",
        )
        .bind(work_id)
        .fetch_optional(db.pool())
        .await
        .unwrap()
        .is_some());
        let after = status(&db).await.unwrap();
        assert_eq!(after.pending, 0);
        assert_eq!(after.committed, 1);
        assert!(after.latest_committed_at.is_some());

        enqueue(&db, work_id, "delete", 11, 1).await;
        consume_once(&db, &resources, index_dir.clone(), "worker-delete", 32)
            .await
            .unwrap();
        assert!(super::super::query_search_index_blocking(
            index_dir,
            "IncrementalFixture".to_string(),
            10,
        )
        .unwrap()
        .is_empty());
    }

    #[tokio::test]
    async fn loading_outbox_documents_commits_a_tracked_read_snapshot() {
        let (_temp, db) = test_db().await;
        let work_id = work(&db, "TrackedOutboxDocumentRead").await;
        enqueue(&db, work_id, "upsert", 50, 1).await;
        let claimed = claim_items(&db, "tracked-read-worker", 32).await.unwrap();
        let before = db.runtime_snapshot().await.read_snapshot;

        let documents = load_documents(&db, &claimed).await.unwrap();

        let after = db.runtime_snapshot().await.read_snapshot;
        assert!(documents.contains_key(&work_id));
        assert_eq!(after.active, 0);
        assert_eq!(after.samples, before.samples + 1);
        assert_eq!(after.completed, before.completed + 1);
        assert_eq!(after.implicit_rollbacks, before.implicit_rollbacks);
    }

    #[tokio::test]
    async fn commit_before_ack_is_replayed_as_idempotent_delete_then_add() {
        let (temp, db) = test_db().await;
        let work_id = work(&db, "ReplayFixture").await;
        enqueue(&db, work_id, "upsert", 20, 1).await;
        let index_dir = temp.path().join("shadow-index");
        let claimed = claim_items(&db, "crashed-worker", 32).await.unwrap();
        let documents = load_documents(&db, &claimed).await.unwrap();
        apply_index_batch(
            &index_dir,
            vec![IndexMutation {
                work_id,
                document: documents.get(&work_id).cloned(),
            }],
            ResourceLimits::nas_n100_4g().search_writer_heap_bytes,
        )
        .unwrap();
        sqlx::query("UPDATE search_outbox SET claimed_at = ?2 WHERE work_id = ?1")
            .bind(work_id)
            .bind(Utc::now() - ChronoDuration::minutes(10))
            .execute(db.pool())
            .await
            .unwrap();

        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let summary = consume_once(&db, &resources, index_dir.clone(), "recovery-worker", 32)
            .await
            .unwrap();
        assert_eq!(summary.acknowledged, 1);
        assert_eq!(
            super::super::query_search_index_blocking(index_dir, "ReplayFixture".to_string(), 10,)
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn stale_ack_cannot_erase_a_newer_revision() {
        let (temp, db) = test_db().await;
        let work_id = work(&db, "OldRevisionUnique").await;
        sqlx::query("UPDATE works SET source_path = '/novels/revision.epub' WHERE id = ?1")
            .bind(work_id)
            .execute(db.pool())
            .await
            .unwrap();
        enqueue(&db, work_id, "upsert", 30, 1).await;
        let old_claim = claim_items(&db, "old-worker", 32).await.unwrap();
        let old_documents = load_documents(&db, &old_claim).await.unwrap();
        let old_search_revision = old_claim[0].search_revision;

        sqlx::query("UPDATE works SET title = 'NewRevisionUnique' WHERE id = ?1")
            .bind(work_id)
            .execute(db.pool())
            .await
            .unwrap();
        enqueue(&db, work_id, "upsert", 31, 1).await;
        let new_search_revision = sqlx::query_scalar::<_, i64>(
            "SELECT search_revision FROM search_outbox WHERE work_id = ?1",
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert!(new_search_revision > old_search_revision);
        let index_dir = temp.path().join("shadow-index");
        apply_index_batch(
            &index_dir,
            vec![IndexMutation {
                work_id,
                document: old_documents.get(&work_id).cloned(),
            }],
            ResourceLimits::nas_n100_4g().search_writer_heap_bytes,
        )
        .unwrap();
        assert_eq!(
            acknowledge_claims(&db, "old-worker", &old_claim)
                .await
                .unwrap(),
            0
        );

        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        consume_once(&db, &resources, index_dir.clone(), "new-worker", 32)
            .await
            .unwrap();
        assert_eq!(
            super::super::query_search_index_blocking(
                index_dir.clone(),
                "NewRevisionUnique".to_string(),
                10,
            )
            .unwrap()
            .len(),
            1
        );
        assert!(super::super::query_search_index_blocking(
            index_dir,
            "OldRevisionUnique".to_string(),
            10,
        )
        .unwrap()
        .is_empty());
    }

    #[tokio::test]
    async fn incompatible_payload_is_released_with_backoff_without_commit() {
        let (temp, db) = test_db().await;
        let work_id = work(&db, "BadPayload").await;
        enqueue(&db, work_id, "upsert", 40, 99).await;
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let error = consume_once(
            &db,
            &resources,
            temp.path().join("shadow-index"),
            "version-worker",
            32,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("incompatible"));
        let row = sqlx::query(
            r#"
            SELECT claimed_by, committed_at, last_error, attempts
            FROM search_outbox WHERE work_id = ?1
            "#,
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert!(row.get::<Option<String>, _>("claimed_by").is_none());
        assert!(row.get::<Option<String>, _>("committed_at").is_none());
        assert!(row
            .get::<Option<String>, _>("last_error")
            .unwrap()
            .contains("incompatible"));
        assert_eq!(row.get::<i64, _>("attempts"), 1);
        assert!(!temp.path().join("shadow-index").exists());

        // A failed item must remain unavailable until its retry deadline. If
        // `+00:00` and `Z` timestamps are mixed, a lexical SQLite comparison
        // can incorrectly make this future item immediately claimable.
        let status_snapshot = status(&db).await.unwrap();
        assert_eq!(status_snapshot.retrying, 1);
        assert_eq!(status_snapshot.max_attempts, 1);
        assert_eq!(status_snapshot.stale_claims, 0);
        assert!(claim_items(&db, "immediate-retry-worker", 32)
            .await
            .unwrap()
            .is_empty());
    }
}
