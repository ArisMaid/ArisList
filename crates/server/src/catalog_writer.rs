//! Transactional catalog mutation core.
//!
//! This module intentionally remains disconnected from the production scanner
//! until a media kind is explicitly switched in `catalog_kind_ownership`.
//! Keeping the API typed and fenced lets fixture/shadow tests exercise the new
//! write path without allowing the legacy scanner and Catalog v2 to co-author
//! the same kind.

#![allow(dead_code)]

use std::collections::HashSet;

use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use serde_json::{json, Map, Value};
use sqlx::Row;
use sqlx_core::transaction::Transaction;

use crate::db::{Db, SearchOutboxPublication};
use crate::error::{AppError, Result};
use crate::Sqlite;

pub(crate) const WORK_MUTATION_ASSET_LIMIT: usize = 512;
pub(crate) const WORK_MUTATION_TAG_LIMIT: usize = 512;
const MAX_EXTERNAL_IDS_PER_MUTATION: usize = 512;
const MAX_STAGED_BYTES_PER_MUTATION: usize = 8 * 1024 * 1024;
const SEARCH_PAYLOAD_VERSION: i64 = 1;

/// The media kinds that may be assigned to the bounded Catalog v2 writer.
/// Keeping this list in one place prevents a typo in an administrative
/// cutover request from creating an ownership row that no scanner understands.
pub const CATALOG_KINDS: [&str; 5] = ["novel", "comic", "coser-picture", "audio", "gallery"];

#[derive(Debug, Clone)]
pub struct MutationSource {
    pub kind: String,
    pub root_id: i64,
    pub work_key: String,
    pub provider: String,
}

#[derive(Debug, Clone)]
pub struct MutationFence {
    pub root_generation: i64,
    pub scan_token: String,
    /// Intermediate chunks keep this false. Only the final mutation for a
    /// work may set it true, which authorizes removal of scanner-owned facts
    /// whose seen token was not refreshed by any preceding chunk.
    pub complete_snapshot: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationOwner {
    Scanner,
    External,
    User,
}

impl MutationOwner {
    fn as_str(self) -> &'static str {
        match self {
            Self::Scanner => "scanner",
            Self::External => "external",
            Self::User => "user",
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkMutationFields {
    pub title: String,
    pub subtitle: Option<String>,
    pub category: Option<String>,
    pub description: Option<String>,
    pub rating: Option<f64>,
    pub source_path: String,
    pub meta: Value,
}

#[derive(Debug, Clone)]
pub struct AssetMutation {
    pub path: String,
    pub mime: String,
    pub role: String,
    pub variant: Option<String>,
    pub position: Option<i64>,
    pub size: Option<i64>,
    pub source_version: String,
    pub meta: Value,
}

#[derive(Debug, Clone)]
pub struct TagMutation {
    pub namespace: String,
    pub key: String,
    pub label: String,
    pub translated_label: Option<String>,
    pub translated_namespace: Option<String>,
    pub source: String,
    pub intro: Option<String>,
    pub links: Option<String>,
    pub owner: MutationOwner,
}

#[derive(Debug, Clone)]
pub struct ExternalIdMutation {
    pub source: String,
    pub external_id: String,
    pub token: Option<String>,
    pub url: Option<String>,
    pub owner: MutationOwner,
}

#[derive(Debug, Clone)]
pub struct WorkMutation {
    pub source: MutationSource,
    /// Previous source key proven to represent the same filesystem identity.
    /// When present, the writer moves the existing source mapping inside the
    /// fenced transaction so rename keeps the stable work/history identity.
    pub previous_work_key: Option<String>,
    pub fence: MutationFence,
    pub fingerprint: String,
    pub work: WorkMutationFields,
    pub assets: Vec<AssetMutation>,
    pub tags: Vec<TagMutation>,
    pub external_ids: Vec<ExternalIdMutation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkMutationResult {
    pub work_id: i64,
    pub asset_ids: Vec<i64>,
    pub catalog_revision: i64,
    pub changed: bool,
}

/// A fenced, authoritative deletion marker. The work row is retained so its
/// stable ID and reading history/progress remain valid; scanner-owned media
/// facts are removed and the search outbox receives an explicit delete.
#[derive(Debug, Clone)]
pub struct WorkTombstone {
    pub source: MutationSource,
    pub fence: MutationFence,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkTombstoneResult {
    pub work_id: Option<i64>,
    pub catalog_revision: i64,
    pub changed: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CatalogKindOwnershipChange {
    pub kind: String,
    pub authoritative_writer: String,
    pub previous_writer: String,
    pub changed: bool,
    pub roots: i64,
    pub roots_requiring_reconcile: i64,
    pub pending_events: i64,
    pub discarded_events: i64,
}

struct StagedMutation {
    work_meta_json: String,
    assets_json: String,
    tags_json: String,
    external_ids_json: String,
}

impl Db {
    /// Atomically change the authoritative writer for one media kind.
    ///
    /// Apply the ownership transition with Catalog/Inventory safety checks.
    /// The HTTP control plane should use
    /// [`Self::change_catalog_kind_ownership_checked`] so Search promotion
    /// evidence is checked inside this same writer transaction as the update.
    /// This lower-level variant remains useful for reconciliation fixtures that
    /// intentionally isolate Catalog ownership from the optional Search shadow.
    pub async fn change_catalog_kind_ownership(
        &self,
        kind: &str,
        target_writer: &str,
        reason: Option<&str>,
    ) -> Result<CatalogKindOwnershipChange> {
        self.change_catalog_kind_ownership_inner(kind, target_writer, reason, false)
            .await
    }

    /// Atomically change ownership after checking Search promotion evidence
    /// inside the same SQLite writer transaction as the ownership update.
    ///
    /// The HTTP control plane uses this checked boundary. Keeping the lower
    /// level method above available preserves fixture/reconciliation tests that
    /// exercise Catalog ownership independently of the optional Search shadow.
    pub async fn change_catalog_kind_ownership_checked(
        &self,
        kind: &str,
        target_writer: &str,
        reason: Option<&str>,
    ) -> Result<CatalogKindOwnershipChange> {
        self.change_catalog_kind_ownership_inner(kind, target_writer, reason, true)
            .await
    }

    async fn change_catalog_kind_ownership_inner(
        &self,
        kind: &str,
        target_writer: &str,
        reason: Option<&str>,
        require_search_promotion_gate: bool,
    ) -> Result<CatalogKindOwnershipChange> {
        if !CATALOG_KINDS.contains(&kind) {
            return Err(AppError::BadRequest(format!(
                "unsupported catalog kind {kind:?}"
            )));
        }
        if !matches!(target_writer, "legacy" | "catalog-v2") {
            return Err(AppError::BadRequest(format!(
                "unsupported catalog writer {target_writer:?}"
            )));
        }
        let reason = reason.unwrap_or("manual catalog ownership change").trim();
        if reason.is_empty() {
            return Err(AppError::BadRequest(
                "catalog ownership change reason must not be empty".to_string(),
            ));
        }
        if reason.chars().count() > 512 {
            return Err(AppError::BadRequest(
                "catalog ownership change reason exceeds 512 characters".to_string(),
            ));
        }

        let _write_slot = self.acquire_write_slot(64 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        // A no-op UPDATE reserves SQLite's writer slot and serializes this
        // decision with a concurrent mutation/scan before any facts are read.
        let ownership = sqlx::query(
            r#"
            UPDATE catalog_kind_ownership
            SET updated_at = updated_at
            WHERE kind = ?1
            RETURNING authoritative_writer
            "#,
        )
        .bind(kind)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(ownership) = ownership else {
            return Err(AppError::Other(format!(
                "catalog ownership row is missing for kind {kind}"
            )));
        };
        let previous_writer: String = ownership.get("authoritative_writer");

        let active_lock = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scanner_locks WHERE name = 'library'",
        )
        .fetch_one(&mut *transaction)
        .await?;
        let scanning_roots = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM library_roots WHERE kind = ?1 AND enabled = 1 AND status = 'scanning'",
        )
        .bind(kind)
        .fetch_one(&mut *transaction)
        .await?;
        if previous_writer != target_writer && (active_lock > 0 || scanning_roots > 0) {
            return Err(AppError::Overloaded {
                message: format!(
                    "catalog ownership for {kind} is locked while a library scan is active"
                ),
                retry_after_seconds: 5,
            });
        }

        if target_writer == "catalog-v2" && require_search_promotion_gate {
            let search =
                crate::search::outbox::search_promotion_snapshot_in(&mut transaction).await?;
            if !crate::search::outbox::search_promotion_gate_ready(
                &search.shadow,
                &search.reconciliation,
                &search.outbox,
            ) {
                return Err(AppError::Other(
                    "shadow search baseline/reconciliation is not stable enough for a kind promotion"
                        .to_string(),
                ));
            }
        }

        let root_facts = sqlx::query(
            r#"
            SELECT
                COUNT(*) AS roots,
                COALESCE(SUM(CASE
                    WHEN enabled = 1
                     AND status = 'idle'
                     AND active_token IS NULL
                     AND completed_generation = generation
                    THEN 1 ELSE 0 END), 0) AS ready_roots,
                COALESCE(SUM(CASE WHEN enabled = 1 THEN 1 ELSE 0 END), 0)
                    AS enabled_roots
            FROM library_roots
            WHERE kind = ?1
            "#,
        )
        .bind(kind)
        .fetch_one(&mut *transaction)
        .await?;
        let roots: i64 = root_facts.get("roots");
        let ready_roots: i64 = root_facts.get("ready_roots");
        let enabled_roots: i64 = root_facts.get("enabled_roots");
        let roots_requiring_reconcile = enabled_roots.saturating_sub(ready_roots);
        let pending_events = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM scan_events AS event
            JOIN library_roots AS root ON root.id = event.root_id
            WHERE root.kind = ?1
              AND event.event_kind IN ('catalog-upsert', 'catalog-delete')
              AND event.status IN ('pending', 'processing')
            "#,
        )
        .bind(kind)
        .fetch_one(&mut *transaction)
        .await?;
        let failed_events = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM scan_events AS event
            JOIN library_roots AS root ON root.id = event.root_id
            WHERE root.kind = ?1
              AND event.event_kind IN ('catalog-upsert', 'catalog-delete')
              AND event.status = 'failed'
              AND event.work_key IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM scan_events AS newer
                  WHERE newer.root_id = event.root_id
                    AND newer.work_key = event.work_key
                    AND newer.event_kind IN ('catalog-upsert', 'catalog-delete')
                    AND newer.seq > event.seq
              )
            "#,
        )
        .bind(kind)
        .fetch_one(&mut *transaction)
        .await?;

        if previous_writer == target_writer {
            transaction.commit().await?;
            return Ok(CatalogKindOwnershipChange {
                kind: kind.to_string(),
                authoritative_writer: previous_writer.clone(),
                previous_writer,
                changed: false,
                roots,
                roots_requiring_reconcile,
                pending_events,
                discarded_events: 0,
            });
        }

        if target_writer == "catalog-v2" {
            if enabled_roots == 0 {
                return Err(AppError::Other(format!(
                    "cannot promote {kind}: no enabled inventory roots are registered"
                )));
            }
            if ready_roots != enabled_roots {
                return Err(AppError::Other(format!(
                    "cannot promote {kind}: {roots_requiring_reconcile} enabled root(s) need a complete inventory reconcile"
                )));
            }
            if failed_events > 0 {
                return Err(AppError::Other(format!(
                    "cannot promote {kind}: {failed_events} catalog event(s) are permanently failed"
                )));
            }
            crate::catalog_reconciliation::require_current_pass(&mut transaction, kind).await?;
        }

        let now = Utc::now();
        sqlx::query(
            r#"
            UPDATE catalog_kind_ownership
            SET authoritative_writer = ?1, updated_at = ?2
            WHERE kind = ?3
            "#,
        )
        .bind(target_writer)
        .bind(now)
        .bind(kind)
        .execute(&mut *transaction)
        .await?;

        let mut discarded_events = 0_i64;
        if target_writer == "legacy" {
            // A legacy scan is the recovery path after rollback.  Do not let
            // stale v2 events remain pending while no v2 coordinator owns the
            // kind; retain the rows for diagnostics and a future re-promotion.
            discarded_events = sqlx::query(
                r#"
                UPDATE scan_events AS event
                SET status = 'done',
                    last_error = ?1,
                    observed_at = ?2
                WHERE event.event_kind IN ('catalog-upsert', 'catalog-delete')
                  AND event.status IN ('pending', 'processing')
                  AND EXISTS (
                      SELECT 1 FROM library_roots AS root
                      WHERE root.id = event.root_id AND root.kind = ?3
                  )
                "#,
            )
            .bind(format!("catalog ownership rolled back: {reason}"))
            .bind(now)
            .bind(kind)
            .execute(&mut *transaction)
            .await?
            .rows_affected() as i64;
            sqlx::query(
                r#"
                UPDATE library_roots
                SET status = CASE WHEN enabled = 1 THEN 'needs_reconcile' ELSE 'disabled' END,
                    last_error = ?1
                WHERE kind = ?2 AND enabled = 1 AND status = 'idle'
                "#,
            )
            .bind(format!(
                "catalog ownership rollback requires legacy reconcile: {reason}"
            ))
            .bind(kind)
            .execute(&mut *transaction)
            .await?;
        }

        let payload = json!({
            "kind": kind,
            "from": previous_writer.clone(),
            "to": target_writer,
            "reason": reason,
            "pending_events": pending_events,
            "discarded_events": discarded_events,
        });
        sqlx::query(
            r#"
            INSERT INTO audit_logs (action, status, payload_json, created_at)
            VALUES ('catalog.ownership', ?1, ?2, ?3)
            "#,
        )
        .bind(if target_writer == "catalog-v2" {
            "promoted"
        } else {
            "rolled-back"
        })
        .bind(payload.to_string())
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        Ok(CatalogKindOwnershipChange {
            kind: kind.to_string(),
            authoritative_writer: target_writer.to_string(),
            previous_writer,
            changed: true,
            roots,
            roots_requiring_reconcile,
            pending_events,
            discarded_events,
        })
    }

    /// Applies one bounded work mutation under kind, root-generation, and
    /// active-token fences. No production caller uses this until a kind is
    /// explicitly assigned to `catalog-v2` in the ownership table.
    pub async fn apply_catalog_work_mutation(
        &self,
        mutation: WorkMutation,
    ) -> Result<WorkMutationResult> {
        validate_mutation(&mutation)?;
        let staged = stage_mutation(&mutation)?;
        let estimated_bytes = (staged.work_meta_json.len()
            + staged.assets_json.len()
            + staged.tags_json.len()
            + staged.external_ids_json.len()) as u64;
        let _write_slot = self.acquire_write_slot(estimated_bytes).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        let base_revision =
            acquire_mutation_fence(&mut transaction, &mutation.source, &mutation.fence).await?;
        let now = Utc::now();

        prepare_temp_tables(&mut transaction).await?;
        let mut mapped_work_id = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT work_id
            FROM catalog_work_sources
            WHERE kind = ?1 AND root_id = ?2 AND work_key = ?3
            "#,
        )
        .bind(&mutation.source.kind)
        .bind(mutation.source.root_id)
        .bind(&mutation.source.work_key)
        .fetch_optional(&mut *transaction)
        .await?;

        let mut source_key_moved = false;
        if mapped_work_id.is_none() {
            if let Some(previous_work_key) = mutation.previous_work_key.as_deref() {
                let previous_work_id = sqlx::query_scalar::<_, i64>(
                    r#"
                    SELECT work_id
                    FROM catalog_work_sources
                    WHERE kind = ?1 AND root_id = ?2 AND work_key = ?3
                    "#,
                )
                .bind(&mutation.source.kind)
                .bind(mutation.source.root_id)
                .bind(previous_work_key)
                .fetch_optional(&mut *transaction)
                .await?;
                if let Some(previous_work_id) = previous_work_id {
                    let conflicting_path_work_id = sqlx::query_scalar::<_, i64>(
                        "SELECT id FROM works WHERE kind = ?1 AND source_path = ?2 AND id != ?3",
                    )
                    .bind(&mutation.source.kind)
                    .bind(&mutation.work.source_path)
                    .bind(previous_work_id)
                    .fetch_optional(&mut *transaction)
                    .await?;
                    if let Some(conflicting_path_work_id) = conflicting_path_work_id {
                        return Err(AppError::Other(format!(
                            "catalog rename target path is already owned by work {conflicting_path_work_id}"
                        )));
                    }
                    let moved = sqlx::query(
                        r#"
                        UPDATE catalog_work_sources
                        SET work_key = ?1,
                            provider = ?2,
                            updated_at = ?3
                        WHERE kind = ?4 AND root_id = ?5 AND work_key = ?6
                          AND NOT EXISTS (
                              SELECT 1 FROM catalog_work_sources AS current
                              WHERE current.kind = ?4
                                AND current.root_id = ?5
                                AND current.work_key = ?1
                          )
                        "#,
                    )
                    .bind(&mutation.source.work_key)
                    .bind(&mutation.source.provider)
                    .bind(now)
                    .bind(&mutation.source.kind)
                    .bind(mutation.source.root_id)
                    .bind(previous_work_key)
                    .execute(&mut *transaction)
                    .await?
                    .rows_affected();
                    if moved != 1 {
                        return Err(AppError::Other(format!(
                            "catalog source rename fence rejected {previous_work_key} -> {}",
                            mutation.source.work_key
                        )));
                    }
                    mapped_work_id = Some(previous_work_id);
                    source_key_moved = true;
                }
            }
        }

        let mut changed = source_key_moved;
        let work_id = if let Some(work_id) = mapped_work_id {
            let updated = update_existing_work(
                &mut transaction,
                work_id,
                &mutation,
                &staged.work_meta_json,
                now,
            )
            .await?;
            changed |= updated;
            work_id
        } else {
            let existing_work_id = sqlx::query_scalar::<_, i64>(
                "SELECT id FROM works WHERE kind = ?1 AND source_path = ?2",
            )
            .bind(&mutation.source.kind)
            .bind(&mutation.work.source_path)
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some(work_id) = existing_work_id {
                let conflicting_source = sqlx::query(
                    r#"
                    SELECT kind, root_id, work_key
                    FROM catalog_work_sources
                    WHERE work_id = ?1
                    "#,
                )
                .bind(work_id)
                .fetch_optional(&mut *transaction)
                .await?;
                if let Some(row) = conflicting_source {
                    return Err(AppError::Other(format!(
                        "work {work_id} is already owned by {}/{}/{}",
                        row.get::<String, _>("kind"),
                        row.get::<i64, _>("root_id"),
                        row.get::<String, _>("work_key")
                    )));
                }
                update_existing_work(
                    &mut transaction,
                    work_id,
                    &mutation,
                    &staged.work_meta_json,
                    now,
                )
                .await?;
                changed = true;
                work_id
            } else {
                changed = true;
                insert_work(&mut transaction, &mutation, &staged.work_meta_json, now).await?
            }
        };

        sqlx::query(
            r#"
            INSERT INTO catalog_work_sources (
                kind, root_id, work_key, provider, work_id,
                seen_generation, seen_token, fingerprint, updated_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(kind, root_id, work_key) DO UPDATE SET
                provider = excluded.provider,
                work_id = excluded.work_id,
                seen_generation = excluded.seen_generation,
                seen_token = excluded.seen_token,
                fingerprint = excluded.fingerprint,
                updated_at = excluded.updated_at
            WHERE catalog_work_sources.provider IS NOT excluded.provider
               OR catalog_work_sources.work_id IS NOT excluded.work_id
               OR catalog_work_sources.seen_generation IS NOT excluded.seen_generation
               OR catalog_work_sources.seen_token IS NOT excluded.seen_token
               OR catalog_work_sources.fingerprint IS NOT excluded.fingerprint
            "#,
        )
        .bind(&mutation.source.kind)
        .bind(mutation.source.root_id)
        .bind(&mutation.source.work_key)
        .bind(&mutation.source.provider)
        .bind(work_id)
        .bind(mutation.fence.root_generation)
        .bind(&mutation.fence.scan_token)
        .bind(&mutation.fingerprint)
        .bind(now)
        .execute(&mut *transaction)
        .await?;

        let (asset_ids, asset_changed) = merge_assets(
            &mut transaction,
            work_id,
            &mutation,
            &staged.assets_json,
            now,
        )
        .await?;
        changed |= asset_changed;
        changed |= merge_tags(&mut transaction, work_id, &mutation, &staged.tags_json).await?;
        changed |= merge_external_ids(
            &mut transaction,
            work_id,
            &mutation,
            &staged.external_ids_json,
        )
        .await?;

        let final_revision = if changed {
            base_revision.checked_add(1).ok_or_else(|| {
                AppError::Other("catalog revision overflowed signed 64-bit range".to_string())
            })?
        } else {
            base_revision
        };
        reconcile_stats(&mut transaction, work_id, final_revision, changed).await?;
        refresh_dirty_tag_counts(&mut transaction).await?;

        if changed {
            let mut publication = SearchOutboxPublication::default();
            sqlx::query(
                r#"
                UPDATE catalog_state
                SET revision = ?1,
                    updated_at = ?2
                WHERE singleton = 1
                "#,
            )
            .bind(final_revision)
            .bind(now)
            .execute(&mut *transaction)
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
            .execute(&mut *transaction)
            .await?;
            enqueue_search_outbox(
                &mut transaction,
                work_id,
                final_revision,
                now,
                &mut publication,
            )
            .await?;
        }

        clear_temp_tables(&mut transaction).await?;
        transaction.commit().await?;
        if changed {
            self.record_catalog_work_revision_delta(base_revision, final_revision, work_id);
        }
        Ok(WorkMutationResult {
            work_id,
            asset_ids,
            catalog_revision: final_revision,
            changed,
        })
    }

    /// Apply a guarded tombstone for a source mapping.
    ///
    /// The operation is intentionally idempotent. A missing source mapping or
    /// an already tombstoned work does not advance the catalog revision or
    /// enqueue another search operation. A successful transition retains the
    /// `works` row and `reading_history`, removes scanner-owned facts, and
    /// writes `operation = 'delete'` to the search outbox in the same SQLite
    /// transaction.
    pub async fn apply_catalog_work_tombstone(
        &self,
        tombstone: WorkTombstone,
    ) -> Result<WorkTombstoneResult> {
        validate_tombstone(&tombstone)?;
        let _write_slot = self.acquire_write_slot(32 * 1024).await?;
        let mut transaction = self.begin_tracked_transaction().await?;
        let base_revision =
            acquire_mutation_fence(&mut transaction, &tombstone.source, &tombstone.fence).await?;
        prepare_temp_tables(&mut transaction).await?;
        let now = Utc::now();
        let mapped = sqlx::query(
            r#"
            SELECT source.work_id, work.deleted_at
            FROM catalog_work_sources AS source
            JOIN works AS work ON work.id = source.work_id
            WHERE source.kind = ?1 AND source.root_id = ?2 AND source.work_key = ?3
            "#,
        )
        .bind(&tombstone.source.kind)
        .bind(tombstone.source.root_id)
        .bind(&tombstone.source.work_key)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(mapped) = mapped else {
            transaction.commit().await?;
            return Ok(WorkTombstoneResult {
                work_id: None,
                catalog_revision: base_revision,
                changed: false,
            });
        };
        let work_id: i64 = mapped.get("work_id");
        let already_deleted: Option<String> = mapped.get("deleted_at");
        if already_deleted.is_some() {
            // Keep the source fence fresh for a later revival, but do not
            // create a second catalog revision or duplicate outbox item.
            sqlx::query(
                r#"
                UPDATE catalog_work_sources
                SET seen_generation = ?1,
                    seen_token = ?2,
                    updated_at = ?3
                WHERE kind = ?4 AND root_id = ?5 AND work_key = ?6
                "#,
            )
            .bind(tombstone.fence.root_generation)
            .bind(&tombstone.fence.scan_token)
            .bind(now)
            .bind(&tombstone.source.kind)
            .bind(tombstone.source.root_id)
            .bind(&tombstone.source.work_key)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return Ok(WorkTombstoneResult {
                work_id: Some(work_id),
                catalog_revision: base_revision,
                changed: false,
            });
        }

        // Capture tags before removing scanner-owned links so their material
        // counts can be refreshed without scanning the entire tag table.
        sqlx::query(
            "INSERT OR IGNORE INTO temp_catalog_dirty_tags(tag_id) SELECT tag_id FROM work_tags WHERE work_id = ?1",
        )
        .bind(work_id)
        .execute(&mut *transaction)
        .await?;

        sqlx::query(
            r#"
            UPDATE works
            SET deleted_at = ?1,
                deleted_reason = ?2,
                cover_asset_id = NULL,
                updated_at = ?1
            WHERE id = ?3 AND deleted_at IS NULL
            "#,
        )
        .bind(now)
        .bind(tombstone.reason.as_deref())
        .bind(work_id)
        .execute(&mut *transaction)
        .await?;

        // `scanner_assets` is the ownership ledger for assets produced by the
        // catalog scanner. Deleting the asset rows cascades the ledger rows;
        // user/external assets that were never claimed by this ledger remain.
        sqlx::query(
            r#"
            DELETE FROM assets
            WHERE id IN (SELECT asset_id FROM scanner_assets WHERE work_id = ?1)
            "#,
        )
        .bind(work_id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query("DELETE FROM work_tag_sources WHERE work_id = ?1 AND owner = 'scanner'")
            .bind(work_id)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            r#"
            DELETE FROM work_tags
            WHERE work_id = ?1
              AND NOT EXISTS (
                  SELECT 1 FROM work_tag_sources
                  WHERE work_id = ?1 AND tag_id = work_tags.tag_id
              )
            "#,
        )
        .bind(work_id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query("DELETE FROM external_id_sources WHERE work_id = ?1 AND owner = 'scanner'")
            .bind(work_id)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            r#"
            DELETE FROM external_ids
            WHERE work_id = ?1
              AND NOT EXISTS (
                  SELECT 1 FROM external_id_sources
                  WHERE external_id_id = external_ids.id
              )
            "#,
        )
        .bind(work_id)
        .execute(&mut *transaction)
        .await?;

        let final_revision = base_revision.checked_add(1).ok_or_else(|| {
            AppError::Other("catalog revision overflowed signed 64-bit range".to_string())
        })?;
        reconcile_stats(&mut transaction, work_id, final_revision, true).await?;
        refresh_dirty_tag_counts(&mut transaction).await?;
        let mut publication = SearchOutboxPublication::default();
        sqlx::query(
            r#"
            UPDATE catalog_state
            SET revision = ?1, updated_at = ?2
            WHERE singleton = 1
            "#,
        )
        .bind(final_revision)
        .bind(now)
        .execute(&mut *transaction)
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
        .execute(&mut *transaction)
        .await?;
        enqueue_search_delete_outbox(
            &mut transaction,
            work_id,
            final_revision,
            now,
            &mut publication,
        )
        .await?;
        sqlx::query(
            r#"
            UPDATE catalog_work_sources
            SET seen_generation = ?1,
                seen_token = ?2,
                updated_at = ?3
            WHERE kind = ?4 AND root_id = ?5 AND work_key = ?6
            "#,
        )
        .bind(tombstone.fence.root_generation)
        .bind(&tombstone.fence.scan_token)
        .bind(now)
        .bind(&tombstone.source.kind)
        .bind(tombstone.source.root_id)
        .bind(&tombstone.source.work_key)
        .execute(&mut *transaction)
        .await?;
        clear_temp_tables(&mut transaction).await?;
        transaction.commit().await?;
        self.record_catalog_work_revision_delta(base_revision, final_revision, work_id);
        Ok(WorkTombstoneResult {
            work_id: Some(work_id),
            catalog_revision: final_revision,
            changed: true,
        })
    }
}

fn validate_tombstone(tombstone: &WorkTombstone) -> Result<()> {
    for (label, value) in [
        ("kind", tombstone.source.kind.as_str()),
        ("work_key", tombstone.source.work_key.as_str()),
        ("provider", tombstone.source.provider.as_str()),
        ("scan_token", tombstone.fence.scan_token.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(AppError::BadRequest(format!(
                "catalog tombstone {label} must not be empty"
            )));
        }
    }
    if tombstone.source.root_id <= 0 {
        return Err(AppError::BadRequest(
            "catalog tombstone root_id must be positive".to_string(),
        ));
    }
    if tombstone.fence.root_generation < 0 {
        return Err(AppError::BadRequest(
            "catalog tombstone root_generation must not be negative".to_string(),
        ));
    }
    if !tombstone.fence.complete_snapshot {
        return Err(AppError::BadRequest(
            "catalog tombstone requires a complete/fenced observation".to_string(),
        ));
    }
    if tombstone
        .reason
        .as_ref()
        .is_some_and(|reason| reason.chars().count() > 512)
    {
        return Err(AppError::BadRequest(
            "catalog tombstone reason exceeds 512 characters".to_string(),
        ));
    }
    Ok(())
}

fn validate_mutation(mutation: &WorkMutation) -> Result<()> {
    for (label, value) in [
        ("kind", mutation.source.kind.as_str()),
        ("work_key", mutation.source.work_key.as_str()),
        ("provider", mutation.source.provider.as_str()),
        ("scan_token", mutation.fence.scan_token.as_str()),
        ("fingerprint", mutation.fingerprint.as_str()),
        ("title", mutation.work.title.as_str()),
        ("source_path", mutation.work.source_path.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(AppError::BadRequest(format!(
                "catalog mutation {label} must not be empty"
            )));
        }
    }
    if mutation.source.root_id <= 0 {
        return Err(AppError::BadRequest(
            "catalog mutation root_id must be positive".to_string(),
        ));
    }
    if mutation.fence.root_generation < 0 {
        return Err(AppError::BadRequest(
            "catalog mutation root_generation must not be negative".to_string(),
        ));
    }
    if let Some(previous_work_key) = mutation.previous_work_key.as_deref() {
        if previous_work_key.trim().is_empty() {
            return Err(AppError::BadRequest(
                "catalog mutation previous_work_key must not be empty".to_string(),
            ));
        }
        if previous_work_key == mutation.source.work_key {
            return Err(AppError::BadRequest(
                "catalog mutation previous_work_key must differ from work_key".to_string(),
            ));
        }
    }
    if mutation.assets.len() > WORK_MUTATION_ASSET_LIMIT {
        return Err(AppError::BadRequest(format!(
            "catalog mutation has {} assets; maximum is {WORK_MUTATION_ASSET_LIMIT}",
            mutation.assets.len()
        )));
    }
    if mutation.tags.len() > WORK_MUTATION_TAG_LIMIT {
        return Err(AppError::BadRequest(format!(
            "catalog mutation has {} tags; maximum is {WORK_MUTATION_TAG_LIMIT}",
            mutation.tags.len()
        )));
    }
    if mutation.external_ids.len() > MAX_EXTERNAL_IDS_PER_MUTATION {
        return Err(AppError::BadRequest(format!(
            "catalog mutation has {} external ids; maximum is {MAX_EXTERNAL_IDS_PER_MUTATION}",
            mutation.external_ids.len()
        )));
    }

    let mut asset_keys = HashSet::with_capacity(mutation.assets.len());
    for asset in &mutation.assets {
        for (label, value) in [
            ("asset path", asset.path.as_str()),
            ("asset mime", asset.mime.as_str()),
            ("asset role", asset.role.as_str()),
            ("asset source_version", asset.source_version.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(AppError::BadRequest(format!(
                    "catalog mutation {label} must not be empty"
                )));
            }
        }
        if asset.size.is_some_and(|size| size < 0) {
            return Err(AppError::BadRequest(
                "catalog mutation asset size must not be negative".to_string(),
            ));
        }
        let identity = (
            asset.path.as_str(),
            asset.role.as_str(),
            asset.variant.as_deref().unwrap_or(""),
        );
        if !asset_keys.insert(identity) {
            return Err(AppError::BadRequest(format!(
                "catalog mutation repeats asset identity {}/{}/{}",
                identity.0, identity.1, identity.2
            )));
        }
    }

    let mut tag_keys = HashSet::with_capacity(mutation.tags.len());
    for tag in &mutation.tags {
        for (label, value) in [
            ("tag namespace", tag.namespace.as_str()),
            ("tag key", tag.key.as_str()),
            ("tag label", tag.label.as_str()),
            ("tag source", tag.source.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(AppError::BadRequest(format!(
                    "catalog mutation {label} must not be empty"
                )));
            }
        }
        if !tag_keys.insert((tag.namespace.as_str(), tag.key.as_str())) {
            return Err(AppError::BadRequest(format!(
                "catalog mutation repeats tag identity {}/{}",
                tag.namespace, tag.key
            )));
        }
    }

    let mut external_keys = HashSet::with_capacity(mutation.external_ids.len());
    for external in &mutation.external_ids {
        for (label, value) in [
            ("external-id source", external.source.as_str()),
            ("external-id value", external.external_id.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(AppError::BadRequest(format!(
                    "catalog mutation {label} must not be empty"
                )));
            }
        }
        if !external_keys.insert((external.source.as_str(), external.external_id.as_str())) {
            return Err(AppError::BadRequest(format!(
                "catalog mutation repeats external-id identity {}/{}",
                external.source, external.external_id
            )));
        }
    }
    Ok(())
}

fn stage_mutation(mutation: &WorkMutation) -> Result<StagedMutation> {
    let mut work_meta = object_value(&mutation.work.meta, "work meta")?;
    work_meta.insert(
        "_scanner_fingerprint".to_string(),
        Value::String(mutation.fingerprint.clone()),
    );
    let work_meta_json = Value::Object(work_meta).to_string();

    let assets = mutation
        .assets
        .iter()
        .enumerate()
        .map(|(ordinal, asset)| {
            let mut meta = object_value(&asset.meta, "asset meta")?;
            meta.insert(
                "_source_version".to_string(),
                Value::String(asset.source_version.clone()),
            );
            Ok(json!({
                "ordinal": ordinal,
                "path": asset.path,
                "mime": asset.mime,
                "role": asset.role,
                "variant": asset.variant.as_deref().unwrap_or(""),
                "position": asset.position.unwrap_or(-1),
                "size": asset.size,
                "meta_json": Value::Object(meta).to_string(),
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let tags = mutation
        .tags
        .iter()
        .enumerate()
        .map(|(ordinal, tag)| {
            json!({
                "ordinal": ordinal,
                "namespace": tag.namespace,
                "key": tag.key,
                "label": tag.label,
                "translated_label": tag.translated_label,
                "translated_namespace": tag.translated_namespace,
                "source": tag.source,
                "intro": tag.intro,
                "links": tag.links,
                "owner": tag.owner.as_str(),
            })
        })
        .collect::<Vec<_>>();
    let external_ids = mutation
        .external_ids
        .iter()
        .enumerate()
        .map(|(ordinal, external)| {
            json!({
                "ordinal": ordinal,
                "source": external.source,
                "external_id": external.external_id,
                "token": external.token,
                "url": external.url,
                "owner": external.owner.as_str(),
            })
        })
        .collect::<Vec<_>>();
    let assets_json = serde_json::to_string(&assets)
        .map_err(|err| AppError::Other(format!("failed to encode asset staging: {err}")))?;
    let tags_json = serde_json::to_string(&tags)
        .map_err(|err| AppError::Other(format!("failed to encode tag staging: {err}")))?;
    let external_ids_json = serde_json::to_string(&external_ids)
        .map_err(|err| AppError::Other(format!("failed to encode external-id staging: {err}")))?;
    let staged_bytes = work_meta_json
        .len()
        .saturating_add(assets_json.len())
        .saturating_add(tags_json.len())
        .saturating_add(external_ids_json.len());
    if staged_bytes > MAX_STAGED_BYTES_PER_MUTATION {
        return Err(AppError::BadRequest(format!(
            "catalog mutation stages {staged_bytes} bytes; maximum is {MAX_STAGED_BYTES_PER_MUTATION}"
        )));
    }
    Ok(StagedMutation {
        work_meta_json,
        assets_json,
        tags_json,
        external_ids_json,
    })
}

fn object_value(value: &Value, label: &str) -> Result<Map<String, Value>> {
    match value {
        Value::Null => Ok(Map::new()),
        Value::Object(object) => Ok(object.clone()),
        _ => Err(AppError::BadRequest(format!(
            "catalog mutation {label} must be a JSON object"
        ))),
    }
}

async fn acquire_mutation_fence(
    transaction: &mut Transaction<'_, Sqlite>,
    source: &MutationSource,
    fence: &MutationFence,
) -> Result<i64> {
    // The first statement is a write so SQLite reserves the writer before any
    // generation reads. That prevents a read/write snapshot race with a root
    // rollover or an ownership cutover.
    let owns_kind = sqlx::query(
        r#"
        UPDATE catalog_kind_ownership
        SET updated_at = updated_at
        WHERE kind = ?1 AND authoritative_writer = 'catalog-v2'
        "#,
    )
    .bind(&source.kind)
    .execute(&mut **transaction)
    .await?
    .rows_affected()
        > 0;
    if !owns_kind {
        return Err(AppError::Other(format!(
            "catalog-v2 does not own authoritative writes for kind {}",
            source.kind
        )));
    }
    let root_is_current = sqlx::query(
        r#"
        UPDATE library_roots
        SET active_token = active_token
        WHERE id = ?1
          AND kind = ?2
          AND provider = ?3
          AND generation = ?4
          AND active_token = ?5
          AND enabled = 1
          AND status = 'scanning'
        "#,
    )
    .bind(source.root_id)
    .bind(&source.kind)
    .bind(&source.provider)
    .bind(fence.root_generation)
    .bind(&fence.scan_token)
    .execute(&mut **transaction)
    .await?
    .rows_affected()
        > 0;
    if !root_is_current {
        return Err(AppError::Other(format!(
            "catalog mutation generation fence rejected root {} generation {}",
            source.root_id, fence.root_generation
        )));
    }
    Ok(
        sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
            .fetch_one(&mut **transaction)
            .await?,
    )
}

async fn prepare_temp_tables(transaction: &mut Transaction<'_, Sqlite>) -> Result<()> {
    for statement in [
        r#"
        CREATE TEMP TABLE IF NOT EXISTS temp_catalog_asset_batch (
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
        r#"
        CREATE TEMP TABLE IF NOT EXISTS temp_catalog_tag_batch (
            ordinal INTEGER PRIMARY KEY,
            namespace TEXT NOT NULL,
            key TEXT NOT NULL,
            label TEXT NOT NULL,
            translated_label TEXT,
            translated_namespace TEXT,
            source TEXT NOT NULL,
            intro TEXT,
            links TEXT,
            owner TEXT NOT NULL
        ) WITHOUT ROWID
        "#,
        r#"
        CREATE TEMP TABLE IF NOT EXISTS temp_catalog_external_batch (
            ordinal INTEGER PRIMARY KEY,
            source TEXT NOT NULL,
            external_id TEXT NOT NULL,
            token TEXT,
            url TEXT,
            owner TEXT NOT NULL
        ) WITHOUT ROWID
        "#,
        "CREATE TEMP TABLE IF NOT EXISTS temp_catalog_dirty_tags (tag_id INTEGER PRIMARY KEY) WITHOUT ROWID",
        "CREATE TEMP TABLE IF NOT EXISTS temp_catalog_changed_tags (tag_id INTEGER PRIMARY KEY) WITHOUT ROWID",
    ] {
        sqlx::query(statement).execute(&mut **transaction).await?;
    }
    clear_temp_tables(transaction).await
}

async fn clear_temp_tables(transaction: &mut Transaction<'_, Sqlite>) -> Result<()> {
    for table in [
        "temp_catalog_asset_batch",
        "temp_catalog_tag_batch",
        "temp_catalog_external_batch",
        "temp_catalog_dirty_tags",
        "temp_catalog_changed_tags",
    ] {
        sqlx::query(&format!("DELETE FROM {table}"))
            .execute(&mut **transaction)
            .await?;
    }
    Ok(())
}

async fn insert_work(
    transaction: &mut Transaction<'_, Sqlite>,
    mutation: &WorkMutation,
    meta_json: &str,
    now: chrono::DateTime<Utc>,
) -> Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO works (
            kind, title, subtitle, category, description, rating,
            source_path, meta_json, updated_at
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        RETURNING id
        "#,
    )
    .bind(&mutation.source.kind)
    .bind(&mutation.work.title)
    .bind(&mutation.work.subtitle)
    .bind(&mutation.work.category)
    .bind(&mutation.work.description)
    .bind(mutation.work.rating)
    .bind(&mutation.work.source_path)
    .bind(meta_json)
    .bind(now)
    .fetch_one(&mut **transaction)
    .await?)
}

async fn update_existing_work(
    transaction: &mut Transaction<'_, Sqlite>,
    work_id: i64,
    mutation: &WorkMutation,
    meta_json: &str,
    now: chrono::DateTime<Utc>,
) -> Result<bool> {
    let result = sqlx::query(
        r#"
        UPDATE works
        SET title = ?1,
            subtitle = COALESCE(?2, subtitle),
            category = ?3,
            description = COALESCE(?4, description),
            rating = COALESCE(?5, rating),
            source_path = ?6,
            meta_json = json_patch(
                CASE WHEN json_valid(meta_json) THEN meta_json ELSE '{}' END,
                ?7
            ),
            deleted_at = NULL,
            deleted_reason = NULL,
            updated_at = ?8
        WHERE id = ?9
          AND kind = ?10
          AND (
                title IS NOT ?1
             OR subtitle IS NOT COALESCE(?2, subtitle)
             OR category IS NOT ?3
             OR description IS NOT COALESCE(?4, description)
             OR rating IS NOT COALESCE(?5, rating)
             OR source_path IS NOT ?6
             OR deleted_at IS NOT NULL
             OR deleted_reason IS NOT NULL
             OR meta_json IS NOT json_patch(
                    CASE WHEN json_valid(meta_json) THEN meta_json ELSE '{}' END,
                    ?7
                )
          )
        "#,
    )
    .bind(&mutation.work.title)
    .bind(&mutation.work.subtitle)
    .bind(&mutation.work.category)
    .bind(&mutation.work.description)
    .bind(mutation.work.rating)
    .bind(&mutation.work.source_path)
    .bind(meta_json)
    .bind(now)
    .bind(work_id)
    .bind(&mutation.source.kind)
    .execute(&mut **transaction)
    .await?;
    if result.rows_affected() == 0 {
        let exists =
            sqlx::query_scalar::<_, i64>("SELECT 1 FROM works WHERE id = ?1 AND kind = ?2")
                .bind(work_id)
                .bind(&mutation.source.kind)
                .fetch_optional(&mut **transaction)
                .await?
                .is_some();
        if !exists {
            return Err(AppError::Other(format!(
                "catalog source maps to missing or mismatched work {work_id}"
            )));
        }
    }
    Ok(result.rows_affected() > 0)
}

async fn merge_assets(
    transaction: &mut Transaction<'_, Sqlite>,
    work_id: i64,
    mutation: &WorkMutation,
    staged_json: &str,
    now: chrono::DateTime<Utc>,
) -> Result<(Vec<i64>, bool)> {
    sqlx::query(
        r#"
        INSERT INTO temp_catalog_asset_batch (
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

    let upserted = sqlx::query(
        r#"
        INSERT INTO assets (
            work_id, path, mime, role, variant, position, size, meta_json
        )
        SELECT ?1, path, mime, role, variant, position, size, meta_json
        FROM temp_catalog_asset_batch
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
    .bind(now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();

    sqlx::query(
        r#"
        INSERT INTO scanner_assets (asset_id, work_id, seen_token)
        SELECT asset.id, ?1, ?2
        FROM temp_catalog_asset_batch AS input
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
    .bind(&mutation.fence.scan_token)
    .execute(&mut **transaction)
    .await?;

    let cover_asset_id = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT asset.id
        FROM temp_catalog_asset_batch AS input
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
    let cover_changed = if let Some(cover_asset_id) = cover_asset_id {
        sqlx::query(
            r#"
            UPDATE works
            SET cover_asset_id = ?1, updated_at = ?2
            WHERE id = ?3 AND cover_asset_id IS NOT ?1
            "#,
        )
        .bind(cover_asset_id)
        .bind(now)
        .bind(work_id)
        .execute(&mut **transaction)
        .await?
        .rows_affected()
            > 0
    } else {
        false
    };

    let mut deleted = 0;
    let mut cleared_cover = false;
    if mutation.fence.complete_snapshot {
        cleared_cover = sqlx::query(
            r#"
            UPDATE works
            SET cover_asset_id = NULL, updated_at = ?1
            WHERE id = ?2
              AND cover_asset_id IN (
                  SELECT asset_id
                  FROM scanner_assets
                  WHERE work_id = ?2 AND seen_token IS NOT ?3
              )
            "#,
        )
        .bind(now)
        .bind(work_id)
        .bind(&mutation.fence.scan_token)
        .execute(&mut **transaction)
        .await?
        .rows_affected()
            > 0;
        deleted = sqlx::query(
            r#"
            DELETE FROM assets
            WHERE id IN (
                SELECT asset_id
                FROM scanner_assets
                WHERE work_id = ?1 AND seen_token IS NOT ?2
            )
            "#,
        )
        .bind(work_id)
        .bind(&mutation.fence.scan_token)
        .execute(&mut **transaction)
        .await?
        .rows_affected();
    }

    let asset_ids = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT asset.id
        FROM temp_catalog_asset_batch AS input
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
    Ok((
        asset_ids,
        upserted > 0 || cover_changed || cleared_cover || deleted > 0,
    ))
}

async fn merge_tags(
    transaction: &mut Transaction<'_, Sqlite>,
    work_id: i64,
    mutation: &WorkMutation,
    staged_json: &str,
) -> Result<bool> {
    sqlx::query(
        "INSERT OR IGNORE INTO temp_catalog_dirty_tags(tag_id) SELECT tag_id FROM work_tags WHERE work_id = ?1",
    )
    .bind(work_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO temp_catalog_tag_batch (
            ordinal, namespace, key, label, translated_label,
            translated_namespace, source, intro, links, owner
        )
        SELECT
            CAST(json_extract(input.value, '$.ordinal') AS INTEGER),
            CAST(json_extract(input.value, '$.namespace') AS TEXT),
            CAST(json_extract(input.value, '$.key') AS TEXT),
            CAST(json_extract(input.value, '$.label') AS TEXT),
            CAST(json_extract(input.value, '$.translated_label') AS TEXT),
            CAST(json_extract(input.value, '$.translated_namespace') AS TEXT),
            CAST(json_extract(input.value, '$.source') AS TEXT),
            CAST(json_extract(input.value, '$.intro') AS TEXT),
            CAST(json_extract(input.value, '$.links') AS TEXT),
            CAST(json_extract(input.value, '$.owner') AS TEXT)
        FROM json_each(?1) AS input
        "#,
    )
    .bind(staged_json)
    .execute(&mut **transaction)
    .await?;

    sqlx::query(
        r#"
        INSERT OR IGNORE INTO temp_catalog_changed_tags(tag_id)
        SELECT tag.id
        FROM temp_catalog_tag_batch AS input
        JOIN tags AS tag
          ON tag.namespace = input.namespace AND tag.key = input.key
        WHERE tag.label IS NOT input.label
           OR tag.translated_label IS NOT COALESCE(input.translated_label, tag.translated_label)
           OR tag.translated_namespace IS NOT COALESCE(input.translated_namespace, tag.translated_namespace)
           OR tag.source IS NOT input.source
           OR tag.intro IS NOT COALESCE(input.intro, tag.intro)
           OR tag.links IS NOT COALESCE(input.links, tag.links)
        "#,
    )
    .execute(&mut **transaction)
    .await?;

    let tag_fact_changes = sqlx::query(
        r#"
        INSERT INTO tags (
            namespace, key, label, translated_label, translated_namespace,
            source, intro, links
        )
        SELECT
            namespace, key, label, translated_label, translated_namespace,
            source, intro, links
        FROM temp_catalog_tag_batch
        WHERE 1
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
        "#,
    )
    .execute(&mut **transaction)
    .await?
    .rows_affected();

    sqlx::query(
        r#"
        INSERT OR IGNORE INTO temp_catalog_dirty_tags(tag_id)
        SELECT tag.id
        FROM temp_catalog_tag_batch AS input
        JOIN tags AS tag
          ON tag.namespace = input.namespace AND tag.key = input.key
        "#,
    )
    .execute(&mut **transaction)
    .await?;
    let links_inserted = sqlx::query(
        r#"
        INSERT OR IGNORE INTO work_tags(work_id, tag_id)
        SELECT ?1, tag.id
        FROM temp_catalog_tag_batch AS input
        JOIN tags AS tag
          ON tag.namespace = input.namespace AND tag.key = input.key
        "#,
    )
    .bind(work_id)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    sqlx::query(
        r#"
        INSERT INTO work_tag_sources(work_id, tag_id, owner, seen_token)
        SELECT
            ?1,
            tag.id,
            input.owner,
            CASE WHEN input.owner = 'scanner' THEN ?2 ELSE NULL END
        FROM temp_catalog_tag_batch AS input
        JOIN tags AS tag
          ON tag.namespace = input.namespace AND tag.key = input.key
        WHERE 1
        ON CONFLICT(work_id, tag_id, owner) DO UPDATE SET
            seen_token = excluded.seen_token
        WHERE work_tag_sources.seen_token IS NOT excluded.seen_token
        "#,
    )
    .bind(work_id)
    .bind(&mutation.fence.scan_token)
    .execute(&mut **transaction)
    .await?;

    let mut links_deleted = 0;
    if mutation.fence.complete_snapshot {
        sqlx::query(
            r#"
            DELETE FROM work_tag_sources
            WHERE work_id = ?1
              AND owner = 'scanner'
              AND seen_token IS NOT ?2
            "#,
        )
        .bind(work_id)
        .bind(&mutation.fence.scan_token)
        .execute(&mut **transaction)
        .await?;
        links_deleted = sqlx::query(
            r#"
            DELETE FROM work_tags
            WHERE work_id = ?1
              AND NOT EXISTS (
                  SELECT 1
                  FROM work_tag_sources AS source
                  WHERE source.work_id = work_tags.work_id
                    AND source.tag_id = work_tags.tag_id
              )
            "#,
        )
        .bind(work_id)
        .execute(&mut **transaction)
        .await?
        .rows_affected();
    }
    Ok(tag_fact_changes > 0 || links_inserted > 0 || links_deleted > 0)
}

async fn merge_external_ids(
    transaction: &mut Transaction<'_, Sqlite>,
    work_id: i64,
    mutation: &WorkMutation,
    staged_json: &str,
) -> Result<bool> {
    sqlx::query(
        r#"
        INSERT INTO temp_catalog_external_batch (
            ordinal, source, external_id, token, url, owner
        )
        SELECT
            CAST(json_extract(input.value, '$.ordinal') AS INTEGER),
            CAST(json_extract(input.value, '$.source') AS TEXT),
            CAST(json_extract(input.value, '$.external_id') AS TEXT),
            CAST(json_extract(input.value, '$.token') AS TEXT),
            CAST(json_extract(input.value, '$.url') AS TEXT),
            CAST(json_extract(input.value, '$.owner') AS TEXT)
        FROM json_each(?1) AS input
        "#,
    )
    .bind(staged_json)
    .execute(&mut **transaction)
    .await?;
    let fact_changes = sqlx::query(
        r#"
        INSERT INTO external_ids(work_id, source, external_id, token, url)
        SELECT ?1, source, external_id, token, url
        FROM temp_catalog_external_batch
        WHERE 1
        ON CONFLICT(work_id, source, external_id) DO UPDATE SET
            token = COALESCE(excluded.token, external_ids.token),
            url = COALESCE(excluded.url, external_ids.url)
        WHERE external_ids.token IS NOT COALESCE(excluded.token, external_ids.token)
           OR external_ids.url IS NOT COALESCE(excluded.url, external_ids.url)
        "#,
    )
    .bind(work_id)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    sqlx::query(
        r#"
        INSERT INTO external_id_sources(external_id_id, work_id, owner, seen_token)
        SELECT
            external.id,
            ?1,
            input.owner,
            CASE WHEN input.owner = 'scanner' THEN ?2 ELSE NULL END
        FROM temp_catalog_external_batch AS input
        JOIN external_ids AS external
          ON external.work_id = ?1
         AND external.source = input.source
         AND external.external_id = input.external_id
        WHERE 1
        ON CONFLICT(external_id_id, owner) DO UPDATE SET
            work_id = excluded.work_id,
            seen_token = excluded.seen_token
        WHERE external_id_sources.work_id IS NOT excluded.work_id
           OR external_id_sources.seen_token IS NOT excluded.seen_token
        "#,
    )
    .bind(work_id)
    .bind(&mutation.fence.scan_token)
    .execute(&mut **transaction)
    .await?;

    let mut facts_deleted = 0;
    if mutation.fence.complete_snapshot {
        sqlx::query(
            r#"
            DELETE FROM external_id_sources
            WHERE work_id = ?1
              AND owner = 'scanner'
              AND seen_token IS NOT ?2
            "#,
        )
        .bind(work_id)
        .bind(&mutation.fence.scan_token)
        .execute(&mut **transaction)
        .await?;
        facts_deleted = sqlx::query(
            r#"
            DELETE FROM external_ids
            WHERE work_id = ?1
              AND NOT EXISTS (
                  SELECT 1
                  FROM external_id_sources AS source
                  WHERE source.external_id_id = external_ids.id
              )
            "#,
        )
        .bind(work_id)
        .execute(&mut **transaction)
        .await?
        .rows_affected();
    }
    Ok(fact_changes > 0 || facts_deleted > 0)
}

async fn reconcile_stats(
    transaction: &mut Transaction<'_, Sqlite>,
    work_id: i64,
    revision: i64,
    invalidate_collection: bool,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO work_stats (
            work_id, asset_count, tag_count, image_count,
            track_count, page_count, catalog_revision, computed_at
        )
        SELECT
            ?1,
            (SELECT COUNT(*) FROM assets WHERE work_id = ?1),
            (SELECT COUNT(*) FROM work_tags WHERE work_id = ?1),
            (SELECT COUNT(*) FROM assets WHERE work_id = ?1 AND mime LIKE 'image/%'),
            (SELECT COUNT(*) FROM assets WHERE work_id = ?1 AND role = 'track')
                +
            (SELECT COUNT(*) FROM assets
             WHERE work_id = ?1
               AND role <> 'track'
               AND lower(mime) LIKE 'audio/%'),
            COALESCE((
                SELECT SUM(CASE
                    WHEN role = 'page' THEN 1
                    WHEN role = 'archive' THEN CAST(
                        COALESCE(json_extract(meta_json, '$.page_count'), 0) AS INTEGER
                    )
                    ELSE 0
                END)
                FROM assets WHERE work_id = ?1
            ), 0),
            ?2,
            NULL
        ON CONFLICT(work_id) DO UPDATE SET
            asset_count = excluded.asset_count,
            tag_count = excluded.tag_count,
            image_count = excluded.image_count,
            track_count = excluded.track_count,
            page_count = excluded.page_count,
            catalog_revision = excluded.catalog_revision,
            computed_at = CASE WHEN ?3 != 0 THEN NULL ELSE work_stats.computed_at END
        "#,
    )
    .bind(work_id)
    .bind(revision)
    .bind(i64::from(invalidate_collection))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn refresh_dirty_tag_counts(transaction: &mut Transaction<'_, Sqlite>) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE tags
        SET count = (
            SELECT COUNT(*) FROM work_tags WHERE tag_id = tags.id
        )
        WHERE id IN (SELECT tag_id FROM temp_catalog_dirty_tags)
          AND count IS NOT (
              SELECT COUNT(*) FROM work_tags WHERE tag_id = tags.id
          )
        "#,
    )
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn enqueue_search_outbox(
    transaction: &mut Transaction<'_, Sqlite>,
    work_id: i64,
    revision: i64,
    now: chrono::DateTime<Utc>,
    publication: &mut SearchOutboxPublication,
) -> Result<()> {
    let search_revision = publication.revision(transaction).await?;
    let now_text = now.to_rfc3339_opts(SecondsFormat::Millis, true);
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
    .bind(SEARCH_PAYLOAD_VERSION)
    .bind(&now_text)
    .execute(&mut **transaction)
    .await?;

    // A global tag-label/source change alters the indexed document of every
    // work linked to that tag, not only the work currently being inspected.
    sqlx::query(
        r#"
        INSERT INTO search_outbox (
            work_id, operation, catalog_revision, search_revision, payload_version,
            attempts, available_at, claimed_by, claimed_at,
            committed_at, last_error, created_at, updated_at
        )
        SELECT DISTINCT
            work_tag.work_id, 'upsert', ?1, ?2, ?3,
            0, ?4, NULL, NULL, NULL, NULL, ?4, ?4
        FROM work_tags AS work_tag
        WHERE work_tag.tag_id IN (SELECT tag_id FROM temp_catalog_changed_tags)
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
    .bind(SEARCH_PAYLOAD_VERSION)
    .bind(&now_text)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn enqueue_search_delete_outbox(
    transaction: &mut Transaction<'_, Sqlite>,
    work_id: i64,
    revision: i64,
    now: chrono::DateTime<Utc>,
    publication: &mut SearchOutboxPublication,
) -> Result<()> {
    let search_revision = publication.revision(transaction).await?;
    let now_text = now.to_rfc3339_opts(SecondsFormat::Millis, true);
    sqlx::query(
        r#"
        INSERT INTO search_outbox (
            work_id, operation, catalog_revision, search_revision, payload_version,
            attempts, available_at, claimed_by, claimed_at,
            committed_at, last_error, created_at, updated_at
        )
        VALUES (?1, 'delete', ?2, ?3, ?4, 0, ?5, NULL, NULL, NULL, NULL, ?5, ?5)
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
    .bind(work_id)
    .bind(revision)
    .bind(search_revision)
    .bind(SEARCH_PAYLOAD_VERSION)
    .bind(&now_text)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database_url(temp: &tempfile::TempDir) -> String {
        format!(
            "sqlite://{}",
            temp.path().join("catalog-writer.sqlite").display()
        )
    }

    async fn test_db() -> (tempfile::TempDir, Db) {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        (temp, db)
    }

    async fn insert_scanning_root(db: &Db, kind: &str, generation: i64, token: &str) -> i64 {
        sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots (
                kind, provider, root, generation, status, active_token
            )
            VALUES (?1, 'local', ?2, ?3, 'scanning', ?4)
            RETURNING id
            "#,
        )
        .bind(kind)
        .bind(format!("/library/{kind}"))
        .bind(generation)
        .bind(token)
        .fetch_one(db.pool())
        .await
        .unwrap()
    }

    async fn assign_catalog_writer(db: &Db, kind: &str) {
        sqlx::query(
            r#"
            UPDATE catalog_kind_ownership
            SET authoritative_writer = 'catalog-v2',
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE kind = ?1
            "#,
        )
        .bind(kind)
        .execute(db.pool())
        .await
        .unwrap();
    }

    async fn advance_root(db: &Db, root_id: i64, generation: i64, token: &str) {
        sqlx::query(
            r#"
            UPDATE library_roots
            SET generation = ?2, active_token = ?3, status = 'scanning'
            WHERE id = ?1
            "#,
        )
        .bind(root_id)
        .bind(generation)
        .bind(token)
        .execute(db.pool())
        .await
        .unwrap();
    }

    fn asset(name: &str, role: &str, position: i64) -> AssetMutation {
        AssetMutation {
            path: format!("/library/gallery/set/{name}.jpg"),
            mime: "image/jpeg".to_string(),
            role: role.to_string(),
            variant: None,
            position: Some(position),
            size: Some(1024 + position),
            source_version: format!("{name}-v1"),
            meta: json!({ "fixture": name }),
        }
    }

    fn tag(key: &str, label: &str, owner: MutationOwner) -> TagMutation {
        TagMutation {
            namespace: "artist".to_string(),
            key: key.to_string(),
            label: label.to_string(),
            translated_label: None,
            translated_namespace: None,
            source: "gallery-folder".to_string(),
            intro: None,
            links: None,
            owner,
        }
    }

    fn external(value: &str, owner: MutationOwner) -> ExternalIdMutation {
        ExternalIdMutation {
            source: "fixture".to_string(),
            external_id: value.to_string(),
            token: None,
            url: Some(format!("https://example.test/{value}")),
            owner,
        }
    }

    fn mutation(
        root_id: i64,
        generation: i64,
        token: &str,
        fingerprint: &str,
        complete_snapshot: bool,
    ) -> WorkMutation {
        WorkMutation {
            source: MutationSource {
                kind: "gallery".to_string(),
                root_id,
                work_key: "set".to_string(),
                provider: "local".to_string(),
            },
            previous_work_key: None,
            fence: MutationFence {
                root_generation: generation,
                scan_token: token.to_string(),
                complete_snapshot,
            },
            fingerprint: fingerprint.to_string(),
            work: WorkMutationFields {
                title: "Fixture Set".to_string(),
                subtitle: None,
                category: Some("Gallery".to_string()),
                description: None,
                rating: None,
                source_path: "/library/gallery/set".to_string(),
                meta: json!({ "fixture": true }),
            },
            assets: vec![asset("cover", "cover", 0), asset("page", "image", 1)],
            tags: vec![tag("shared", "Current Label", MutationOwner::Scanner)],
            external_ids: vec![external("current", MutationOwner::Scanner)],
        }
    }

    async fn canonical_work_snapshot(db: &Db, work_id: i64) -> Value {
        let work = sqlx::query_as::<
            _,
            (
                String,
                String,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<f64>,
                Option<String>,
                String,
                Option<String>,
            ),
        >(
            r#"
            SELECT
                work.kind, work.title, work.subtitle, work.category,
                work.description, work.rating, work.source_path, work.meta_json,
                (SELECT path FROM assets WHERE id = work.cover_asset_id) AS cover_path
            FROM works AS work
            WHERE work.id = ?1
            "#,
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        let assets =
            sqlx::query_as::<_, (String, String, String, String, i64, Option<i64>, String)>(
                r#"
            SELECT path, mime, role, variant, position, size, meta_json
            FROM assets WHERE work_id = ?1
            ORDER BY path, role, variant
            "#,
            )
            .bind(work_id)
            .fetch_all(db.pool())
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                json!({
                    "path": row.0,
                    "mime": row.1,
                    "role": row.2,
                    "variant": row.3,
                    "position": row.4,
                    "size": row.5,
                    "meta": serde_json::from_str::<Value>(&row.6).unwrap(),
                })
            })
            .collect::<Vec<_>>();
        let tags = sqlx::query_as::<_, (String, String, String, String, String, Option<String>)>(
            r#"
            SELECT tag.namespace, tag.key, tag.label, tag.source,
                   source.owner, source.seen_token
            FROM work_tags AS work_tag
            JOIN tags AS tag ON tag.id = work_tag.tag_id
            JOIN work_tag_sources AS source
              ON source.work_id = work_tag.work_id AND source.tag_id = work_tag.tag_id
            WHERE work_tag.work_id = ?1
            ORDER BY tag.namespace, tag.key, source.owner
            "#,
        )
        .bind(work_id)
        .fetch_all(db.pool())
        .await
        .unwrap();
        let external_ids = sqlx::query_as::<
            _,
            (
                String,
                String,
                Option<String>,
                Option<String>,
                String,
                Option<String>,
            ),
        >(
            r#"
            SELECT external.source, external.external_id, external.token, external.url,
                   source.owner, source.seen_token
            FROM external_ids AS external
            JOIN external_id_sources AS source ON source.external_id_id = external.id
            WHERE external.work_id = ?1
            ORDER BY external.source, external.external_id, source.owner
            "#,
        )
        .bind(work_id)
        .fetch_all(db.pool())
        .await
        .unwrap();
        let stats = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
            r#"
            SELECT asset_count, tag_count, image_count, track_count, page_count
            FROM work_stats WHERE work_id = ?1
            "#,
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        json!({
            "work": {
                "kind": work.0,
                "title": work.1,
                "subtitle": work.2,
                "category": work.3,
                "description": work.4,
                "rating": work.5,
                "source_path": work.6,
                "meta": serde_json::from_str::<Value>(&work.7).unwrap(),
                "cover_path": work.8,
            },
            "assets": assets,
            "tags": tags,
            "external_ids": external_ids,
            "stats": stats,
        })
    }

    #[tokio::test]
    async fn typed_writer_matches_the_legacy_scanner_fixture_field_for_field() {
        let (_legacy_temp, legacy) = test_db().await;
        let (_typed_temp, typed) = test_db().await;
        let token = "fixture-scan";
        let fingerprint = "fixture-fingerprint";

        assert!(legacy
            .try_acquire_scanner_lock("library", token, 60)
            .await
            .unwrap());
        let legacy_work_id = legacy
            .upsert_scanner_work(
                "gallery",
                "Fixture Set",
                Some("/library/gallery/set"),
                Some("Gallery"),
                None,
                None,
                json!({ "fixture": true }),
                token,
                fingerprint,
            )
            .await
            .unwrap();
        legacy
            .upsert_scanner_assets(
                legacy_work_id,
                vec![
                    crate::db::ScannerAssetInput {
                        path: "/library/gallery/set/cover.jpg".to_string(),
                        mime: "image/jpeg".to_string(),
                        role: "cover".to_string(),
                        variant: None,
                        position: Some(0),
                        size: Some(1024),
                        meta: json!({ "fixture": "cover", "_source_version": "cover-v1" }),
                    },
                    crate::db::ScannerAssetInput {
                        path: "/library/gallery/set/page.jpg".to_string(),
                        mime: "image/jpeg".to_string(),
                        role: "image".to_string(),
                        variant: None,
                        position: Some(1),
                        size: Some(1025),
                        meta: json!({ "fixture": "page", "_source_version": "page-v1" }),
                    },
                ],
                token,
            )
            .await
            .unwrap();
        legacy
            .upsert_and_link_scanner_tag(
                legacy_work_id,
                "artist",
                "shared",
                "Current Label",
                None,
                None,
                "gallery-folder",
                None,
                None,
                token,
            )
            .await
            .unwrap();
        legacy
            .upsert_scanner_external_id(
                legacy_work_id,
                "fixture",
                "current",
                None,
                Some("https://example.test/current"),
                token,
            )
            .await
            .unwrap();
        legacy
            .finish_scanner_work(
                legacy_work_id,
                "gallery|/library/gallery",
                token,
                fingerprint,
            )
            .await
            .unwrap();

        let root_id = insert_scanning_root(&typed, "gallery", 1, token).await;
        assign_catalog_writer(&typed, "gallery").await;
        let typed_result = typed
            .apply_catalog_work_mutation(mutation(root_id, 1, token, fingerprint, true))
            .await
            .unwrap();

        assert_eq!(
            canonical_work_snapshot(&typed, typed_result.work_id).await,
            canonical_work_snapshot(&legacy, legacy_work_id).await
        );
    }

    #[tokio::test]
    async fn ownership_cutover_requires_a_complete_idle_inventory_root() {
        let (_temp, db) = test_db().await;
        let root_id = insert_scanning_root(&db, "novel", 1, "cutover-scan").await;
        let error = db
            .change_catalog_kind_ownership("novel", "catalog-v2", Some("test"))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("scan is active") || error.contains("locked"));
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT authoritative_writer FROM catalog_kind_ownership WHERE kind = 'novel'",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "legacy"
        );

        sqlx::query(
            "UPDATE library_roots SET status = 'idle', generation = 2, completed_generation = 1 WHERE id = ?1",
        )
        .bind(root_id)
        .execute(db.pool())
        .await
        .unwrap();
        let error = db
            .change_catalog_kind_ownership("novel", "catalog-v2", Some("test"))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("complete inventory reconcile"));
    }

    #[tokio::test]
    async fn checked_ownership_cutover_requires_search_evidence_without_mutating_owner() {
        let (_temp, db) = test_db().await;
        let root_id = insert_scanning_root(&db, "novel", 1, "search-gate").await;
        sqlx::query(
            "UPDATE library_roots SET status = 'idle', completed_generation = generation, active_token = NULL WHERE id = ?1",
        )
        .bind(root_id)
        .execute(db.pool())
        .await
        .unwrap();

        let error = db
            .change_catalog_kind_ownership_checked("novel", "catalog-v2", Some("search gate"))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("shadow search baseline/reconciliation"));
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT authoritative_writer FROM catalog_kind_ownership WHERE kind = 'novel'",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "legacy"
        );
    }

    #[tokio::test]
    async fn ownership_cutover_is_audited_and_rollback_discards_v2_events() {
        let (_temp, db) = test_db().await;
        let root_id = insert_scanning_root(&db, "novel", 3, "cutover-ready").await;
        sqlx::query(
            "UPDATE library_roots SET status = 'idle', completed_generation = generation, active_token = NULL WHERE id = ?1",
        )
        .bind(root_id)
        .execute(db.pool())
        .await
        .unwrap();
        let error = db
            .change_catalog_kind_ownership("novel", "catalog-v2", Some("missing evidence"))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("current passing legacy/Catalog v2 reconciliation"));
        crate::catalog_reconciliation::record_current_pass_for_test(&db, "novel").await;
        let promoted = db
            .change_catalog_kind_ownership("novel", "catalog-v2", Some("ready fixture"))
            .await
            .unwrap();
        assert!(promoted.changed);
        assert_eq!(promoted.previous_writer, "legacy");
        assert_eq!(promoted.authoritative_writer, "catalog-v2");

        sqlx::query(
            r#"
            INSERT INTO scan_events(root_id, relative_path, event_kind, work_key, observed_at)
            VALUES (?1, 'book.epub', 'catalog-upsert', 'book.epub', strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            "#,
        )
        .bind(root_id)
        .execute(db.pool())
        .await
        .unwrap();
        let rolled_back = db
            .change_catalog_kind_ownership("novel", "legacy", Some("rollback fixture"))
            .await
            .unwrap();
        assert!(rolled_back.changed);
        assert_eq!(rolled_back.discarded_events, 1);
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT authoritative_writer FROM catalog_kind_ownership WHERE kind = 'novel'",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "legacy"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM library_roots WHERE id = ?1")
                .bind(root_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "needs_reconcile"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM audit_logs WHERE action = 'catalog.ownership'",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn ownership_cutover_rejects_stale_catalog_reconciliation_evidence() {
        let (_temp, db) = test_db().await;
        let root_id = insert_scanning_root(&db, "novel", 2, "cutover-stale").await;
        sqlx::query(
            "UPDATE library_roots SET status = 'idle', completed_generation = generation, active_token = NULL WHERE id = ?1",
        )
        .bind(root_id)
        .execute(db.pool())
        .await
        .unwrap();
        crate::catalog_reconciliation::record_current_pass_for_test(&db, "novel").await;
        sqlx::query("UPDATE library_roots SET last_event_seq = 9 WHERE id = ?1")
            .bind(root_id)
            .execute(db.pool())
            .await
            .unwrap();
        let error = db
            .change_catalog_kind_ownership("novel", "catalog-v2", Some("stale fixture"))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("current passing legacy/Catalog v2 reconciliation"));
    }

    #[tokio::test]
    async fn mutation_is_idempotent_and_enqueues_every_document_affected_by_tag_metadata() {
        let (_temp, db) = test_db().await;
        let root_id = insert_scanning_root(&db, "gallery", 1, "scan-1").await;

        let other_work_id = db
            .upsert_work(
                "gallery",
                "Other Set",
                Some("/library/gallery/other"),
                Some("Gallery"),
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();
        let shared_tag_id = db
            .upsert_tag(
                "artist",
                "shared",
                "Old Label",
                None,
                None,
                "seed",
                None,
                None,
            )
            .await
            .unwrap();
        db.link_tag(other_work_id, shared_tag_id).await.unwrap();
        assign_catalog_writer(&db, "gallery").await;

        let base_revision =
            sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
                .fetch_one(db.pool())
                .await
                .unwrap();
        let input = mutation(root_id, 1, "scan-1", "fingerprint-1", true);
        let first = db.apply_catalog_work_mutation(input.clone()).await.unwrap();
        assert!(first.changed);
        assert_eq!(first.catalog_revision, base_revision + 1);
        assert_eq!(first.asset_ids.len(), 2);
        assert_eq!(
            sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
                r#"
                SELECT asset_count, tag_count, image_count, track_count, page_count
                FROM work_stats WHERE work_id = ?1
                "#,
            )
            .bind(first.work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            (2, 1, 2, 0, 0)
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT cover_asset_id FROM works WHERE id = ?1")
                .bind(first.work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            first.asset_ids[0]
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT label FROM tags WHERE id = ?1")
                .bind(shared_tag_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "Current Label"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM external_id_sources WHERE work_id = ?1 AND owner = 'scanner' AND seen_token = 'scan-1'",
            )
            .bind(first.work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT work_id FROM search_outbox ORDER BY work_id",)
                .fetch_all(db.pool())
                .await
                .unwrap(),
            {
                let mut ids = vec![first.work_id, other_work_id];
                ids.sort_unstable();
                ids
            }
        );
        let published_search_revisions = sqlx::query_as::<_, (i64, i64)>(
            "SELECT work_id, search_revision FROM search_outbox WHERE work_id IN (?1, ?2) ORDER BY work_id",
        )
        .bind(first.work_id)
        .bind(other_work_id)
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(published_search_revisions.len(), 2);
        assert_eq!(
            published_search_revisions[0].1, published_search_revisions[1].1,
            "all searchable facts from one catalog mutation must share one source revision"
        );

        let outbox_updated_at = sqlx::query_scalar::<_, String>(
            "SELECT updated_at FROM search_outbox WHERE work_id = ?1",
        )
        .bind(first.work_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        let replay = db.apply_catalog_work_mutation(input).await.unwrap();
        assert!(!replay.changed);
        assert_eq!(replay.work_id, first.work_id);
        assert_eq!(replay.asset_ids, first.asset_ids);
        assert_eq!(replay.catalog_revision, first.catalog_revision);
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT updated_at FROM search_outbox WHERE work_id = ?1",
            )
            .bind(first.work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            outbox_updated_at
        );
    }

    #[tokio::test]
    async fn ownership_token_and_generation_fences_reject_stale_mutations() {
        let (_temp, db) = test_db().await;
        let root_id = insert_scanning_root(&db, "gallery", 3, "current").await;
        let valid = mutation(root_id, 3, "current", "fingerprint", false);

        let ownership_error = db
            .apply_catalog_work_mutation(valid.clone())
            .await
            .unwrap_err()
            .to_string();
        assert!(ownership_error.contains("does not own authoritative writes"));
        assign_catalog_writer(&db, "gallery").await;

        let mut stale_token = valid.clone();
        stale_token.fence.scan_token = "stale".to_string();
        assert!(db
            .apply_catalog_work_mutation(stale_token)
            .await
            .unwrap_err()
            .to_string()
            .contains("generation fence"));
        let mut stale_generation = valid.clone();
        stale_generation.fence.root_generation = 2;
        assert!(db
            .apply_catalog_work_mutation(stale_generation)
            .await
            .unwrap_err()
            .to_string()
            .contains("generation fence"));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM works")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            0
        );

        assert!(db.apply_catalog_work_mutation(valid).await.unwrap().changed);
    }

    #[tokio::test]
    async fn only_complete_snapshots_clean_scanner_rows_and_other_owners_survive() {
        let (_temp, db) = test_db().await;
        let root_id = insert_scanning_root(&db, "gallery", 1, "first").await;
        assign_catalog_writer(&db, "gallery").await;

        let mut initial = mutation(root_id, 1, "first", "first-fingerprint", true);
        initial.assets.push(asset("stale", "image", 2));
        initial.tags = vec![
            tag("keep", "Keep", MutationOwner::Scanner),
            tag("shared", "Shared", MutationOwner::Scanner),
            tag("stale", "Stale", MutationOwner::Scanner),
        ];
        initial.external_ids = vec![
            external("keep", MutationOwner::Scanner),
            external("shared", MutationOwner::Scanner),
            external("stale", MutationOwner::Scanner),
        ];
        let first = db.apply_catalog_work_mutation(initial).await.unwrap();

        let shared_tag_id = sqlx::query_scalar::<_, i64>(
            "SELECT id FROM tags WHERE namespace = 'artist' AND key = 'shared'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO work_tag_sources(work_id, tag_id, owner, seen_token) VALUES (?1, ?2, 'external', NULL)",
        )
        .bind(first.work_id)
        .bind(shared_tag_id)
        .execute(db.pool())
        .await
        .unwrap();
        let shared_external_id = sqlx::query_scalar::<_, i64>(
            "SELECT id FROM external_ids WHERE work_id = ?1 AND external_id = 'shared'",
        )
        .bind(first.work_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO external_id_sources(external_id_id, work_id, owner, seen_token) VALUES (?1, ?2, 'external', NULL)",
        )
        .bind(shared_external_id)
        .bind(first.work_id)
        .execute(db.pool())
        .await
        .unwrap();

        advance_root(&db, root_id, 2, "second").await;
        let mut partial = mutation(root_id, 2, "second", "second-fingerprint", false);
        partial.assets = vec![asset("cover", "cover", 0)];
        partial.tags = vec![tag("keep", "Keep", MutationOwner::Scanner)];
        partial.external_ids = vec![external("keep", MutationOwner::Scanner)];
        db.apply_catalog_work_mutation(partial.clone())
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM assets WHERE work_id = ?1")
                .bind(first.work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM work_tags WHERE work_id = ?1")
                .bind(first.work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM external_ids WHERE work_id = ?1")
                .bind(first.work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            3
        );

        partial.fence.complete_snapshot = true;
        let finalized = db.apply_catalog_work_mutation(partial).await.unwrap();
        assert!(finalized.changed);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM assets WHERE work_id = ?1")
                .bind(first.work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT tag.key FROM work_tags JOIN tags AS tag ON tag.id = work_tags.tag_id WHERE work_tags.work_id = ?1 ORDER BY tag.key",
            )
            .bind(first.work_id)
            .fetch_all(db.pool())
            .await
            .unwrap(),
            vec!["keep".to_string(), "shared".to_string()]
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT external_id FROM external_ids WHERE work_id = ?1 ORDER BY external_id",
            )
            .bind(first.work_id)
            .fetch_all(db.pool())
            .await
            .unwrap(),
            vec!["keep".to_string(), "shared".to_string()]
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT owner FROM work_tag_sources WHERE work_id = ?1 AND tag_id = ?2 ORDER BY owner",
            )
            .bind(first.work_id)
            .bind(shared_tag_id)
            .fetch_all(db.pool())
            .await
            .unwrap(),
            vec!["external".to_string()]
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT owner FROM external_id_sources WHERE external_id_id = ?1 ORDER BY owner",
            )
            .bind(shared_external_id)
            .fetch_all(db.pool())
            .await
            .unwrap(),
            vec!["external".to_string()]
        );
    }

    #[tokio::test]
    async fn tombstone_retains_history_cleans_scanner_facts_and_emits_delete_outbox() {
        let (_temp, db) = test_db().await;
        let root_id = insert_scanning_root(&db, "gallery", 1, "tombstone-1").await;
        assign_catalog_writer(&db, "gallery").await;
        let initial = mutation(root_id, 1, "tombstone-1", "tombstone-fingerprint", true);
        let tombstone_source = initial.source.clone();
        let tombstone_fence = initial.fence.clone();
        let created = db.apply_catalog_work_mutation(initial).await.unwrap();
        db.update_work_progress(created.work_id, 0.73, Some("page-73"), 10)
            .await
            .unwrap();
        let scanner_asset_count =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM scanner_assets WHERE work_id = ?1")
                .bind(created.work_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert!(scanner_asset_count > 0);
        let before_tombstone_revision =
            sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
                .fetch_one(db.pool())
                .await
                .unwrap();

        let tombstone = WorkTombstone {
            source: tombstone_source,
            fence: tombstone_fence,
            reason: Some("missing after complete root reconcile".to_string()),
        };
        let removed = db
            .apply_catalog_work_tombstone(tombstone.clone())
            .await
            .unwrap();
        assert!(removed.changed);
        assert_eq!(removed.work_id, Some(created.work_id));
        assert_eq!(
            removed.catalog_revision,
            before_tombstone_revision.saturating_add(1)
        );
        let deleted_at =
            sqlx::query_scalar::<_, Option<String>>("SELECT deleted_at FROM works WHERE id = ?1")
                .bind(created.work_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert!(deleted_at.is_some());
        assert_eq!(
            sqlx::query_as::<_, (f64, Option<String>)>(
                "SELECT progress, position FROM reading_history WHERE work_id = ?1",
            )
            .bind(created.work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            (0.73, Some("page-73".to_string()))
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM scanner_assets WHERE work_id = ?1",)
                .bind(created.work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT operation FROM search_outbox WHERE work_id = ?1",
            )
            .bind(created.work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "delete"
        );

        let replay = db.apply_catalog_work_tombstone(tombstone).await.unwrap();
        assert!(!replay.changed);
        assert_eq!(replay.catalog_revision, removed.catalog_revision);

        // A later complete observation revives the same stable work identity
        // and turns the pending search operation back into an upsert.
        advance_root(&db, root_id, 2, "tombstone-2").await;
        let mut revived = mutation(root_id, 2, "tombstone-2", "revived-fingerprint", true);
        revived.work.title = "Revived Fixture Set".to_string();
        let revived_result = db.apply_catalog_work_mutation(revived).await.unwrap();
        assert!(revived_result.changed);
        assert_eq!(revived_result.work_id, created.work_id);
        assert_eq!(
            sqlx::query_scalar::<_, Option<String>>("SELECT deleted_at FROM works WHERE id = ?1")
                .bind(created.work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT operation FROM search_outbox WHERE work_id = ?1",
            )
            .bind(created.work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            "upsert"
        );
        assert_eq!(
            sqlx::query_scalar::<_, f64>("SELECT progress FROM works WHERE id = ?1")
                .bind(created.work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            0.73
        );
    }

    #[tokio::test]
    async fn outbox_failure_rolls_back_work_assets_ownership_stats_and_revision() {
        let (_temp, db) = test_db().await;
        let root_id = insert_scanning_root(&db, "gallery", 1, "atomic").await;
        assign_catalog_writer(&db, "gallery").await;
        let base_revision =
            sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
                .fetch_one(db.pool())
                .await
                .unwrap();
        sqlx::query(
            r#"
            CREATE TRIGGER fail_catalog_outbox
            BEFORE INSERT ON search_outbox
            BEGIN
                SELECT RAISE(ABORT, 'catalog outbox failpoint');
            END
            "#,
        )
        .execute(db.pool())
        .await
        .unwrap();

        let input = mutation(root_id, 1, "atomic", "atomic-fingerprint", true);
        assert!(db.apply_catalog_work_mutation(input.clone()).await.is_err());
        for table in [
            "works",
            "assets",
            "work_tags",
            "external_ids",
            "catalog_work_sources",
            "search_outbox",
        ] {
            assert_eq!(
                sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM {table}"))
                    .fetch_one(db.pool())
                    .await
                    .unwrap(),
                0,
                "{table} was not rolled back"
            );
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1",)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            base_revision
        );

        sqlx::query("DROP TRIGGER fail_catalog_outbox")
            .execute(db.pool())
            .await
            .unwrap();
        assert!(db.apply_catalog_work_mutation(input).await.unwrap().changed);
    }

    #[tokio::test]
    async fn v8_upgrade_backfills_scanner_and_external_id_ownership_conservatively() {
        let (_temp, db) = test_db().await;
        let audio_work = db
            .upsert_work(
                "audio",
                "Audio",
                Some("/audio/RJ1"),
                Some("Audio"),
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();
        let novel_work = db
            .upsert_work(
                "novel",
                "Novel",
                Some("/novel/book.epub"),
                Some("Light Novel"),
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO scanner_works(work_id, scope, seen_token, fingerprint) VALUES (?1, 'audio|/audio', 'legacy-audio', NULL)",
        )
        .bind(audio_work)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO external_ids(work_id, source, external_id) VALUES (?1, 'asmr', 'RJ1'), (?2, 'lightnovel', 'LN1')",
        )
        .bind(audio_work)
        .bind(novel_work)
        .execute(db.pool())
        .await
        .unwrap();

        for table in [
            "search_outbox",
            "external_id_sources",
            "catalog_work_sources",
            "catalog_kind_ownership",
        ] {
            sqlx::query(&format!("DROP TABLE {table}"))
                .execute(db.pool())
                .await
                .unwrap();
        }
        sqlx::query("DELETE FROM schema_migrations WHERE version = 8")
            .execute(db.pool())
            .await
            .unwrap();
        crate::migrations::apply_pending(db.pool()).await.unwrap();

        assert_eq!(
            sqlx::query_as::<_, (String, Option<String>)>(
                r#"
                SELECT source.owner, source.seen_token
                FROM external_id_sources AS source
                JOIN external_ids AS external ON external.id = source.external_id_id
                WHERE external.work_id = ?1
                "#,
            )
            .bind(audio_work)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            ("scanner".to_string(), Some("legacy-audio".to_string()))
        );
        assert_eq!(
            sqlx::query_as::<_, (String, Option<String>)>(
                r#"
                SELECT source.owner, source.seen_token
                FROM external_id_sources AS source
                JOIN external_ids AS external ON external.id = source.external_id_id
                WHERE external.work_id = ?1
                "#,
            )
            .bind(novel_work)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            ("external".to_string(), None)
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM catalog_kind_ownership WHERE authoritative_writer = 'legacy'",
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            5
        );
    }
}
