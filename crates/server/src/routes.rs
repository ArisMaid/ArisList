use std::convert::Infallible;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use async_stream::stream;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderValue};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::Utc;
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Semaphore;
use walkdir::WalkDir;

use crate::assets;
use crate::catalog;
use crate::catalog_reconciliation;
use crate::catalog_writer::CATALOG_KINDS;
use crate::enrich;
use crate::error::{AppError, Result};
use crate::inventory;
use crate::migrations;
use crate::models::{
    Asset, HistoryRecord, LibraryResponse, ScanRequest, Tag, WorkDetail, WorkDetailAssetMode,
    WorkKind,
};
use crate::resource;
use crate::search;
use crate::settings;
use crate::{AppState, Row};

static FILESYSTEM_INSPECTION_LIMIT: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(2)));

async fn run_filesystem_inspection<T, F>(operation: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let permit = FILESYSTEM_INSPECTION_LIMIT
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| AppError::Other("filesystem inspection worker closed".to_string()))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        operation()
    })
    .await
    .map_err(|err| AppError::Other(format!("filesystem inspection task failed: {err}")))
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/health/resources", get(resource_health))
        .route("/catalog/works", get(catalog::works))
        .route("/catalog/ownership", get(catalog_ownership))
        .route("/catalog/ownership/{kind}", post(change_catalog_ownership))
        .route(
            "/catalog/reconciliation",
            get(catalog_reconciliation_status),
        )
        .route(
            "/catalog/reconciliation/novel",
            post(reconcile_catalog_novel),
        )
        .route(
            "/catalog/reconciliation/comic",
            post(reconcile_catalog_comic),
        )
        .route(
            "/catalog/reconciliation/coser-picture",
            post(reconcile_catalog_coser_picture),
        )
        .route(
            "/catalog/reconciliation/audio",
            post(reconcile_catalog_audio),
        )
        .route(
            "/catalog/reconciliation/gallery",
            post(reconcile_catalog_gallery),
        )
        .route("/catalog/random", get(catalog::random))
        .route("/catalog/collections", get(catalog::collections))
        .route("/catalog/facets/tags", get(catalog::facets_tags))
        .route("/catalog/counts", get(catalog::counts))
        .route("/catalog/history", get(catalog::history))
        .route("/inventory/status", get(inventory::status))
        .route("/library", get(library))
        .route("/jobs", get(jobs))
        .route("/history", get(history))
        .route(
            "/settings",
            get(settings::get_settings).patch(settings::update_settings),
        )
        .route("/search", get(search::search))
        .route(
            "/search/shadow/reconcile",
            get(search::reconcile_shadow_facts),
        )
        .route("/search/rebuild", post(search::enqueue_rebuild))
        .route(
            "/search/shadow/rebuild",
            post(search::enqueue_shadow_rebuild),
        )
        .route("/cloud/status", get(cloud_status))
        .route("/cloud/qmediasync/test-strm-root", post(test_qms_strm_root))
        .route("/works/{id}", get(work_detail))
        .route("/works/{id}/assets", get(catalog::assets))
        .route("/works/{id}/history", get(work_history))
        .route(
            "/works/{id}/cover",
            get(assets::work_cover).head(assets::reject_expensive_head),
        )
        .route("/works/{id}/gallery", get(gallery_assets))
        .route("/works/{id}/progress", patch(update_progress))
        .route(
            "/works/{id}/pages",
            get(assets::comic_pages).head(assets::reject_expensive_head),
        )
        .route(
            "/works/{id}/pages/{page}/stream",
            get(assets::stream_comic_page).head(assets::reject_expensive_head),
        )
        .route(
            "/works/{id}/epub",
            get(assets::epub_manifest).head(assets::reject_expensive_head),
        )
        .route(
            "/works/{id}/epub/{chapter}/html",
            get(assets::epub_chapter_html).head(assets::reject_expensive_head),
        )
        .route(
            "/works/{id}/epub/image",
            get(assets::stream_epub_image).head(assets::reject_expensive_head),
        )
        .route("/scan", post(scan))
        .route("/enrich", post(enrich::enqueue_enrich))
        .route("/tags", get(tags))
        .route(
            "/assets/{id}/stream",
            get(assets::stream_asset).head(assets::head_asset),
        )
        .route("/assets/{id}/route", get(assets::asset_route))
        .route("/assets/{id}/prepare", post(crate::strm::jobs::prepare))
        .route("/assets/{id}/diagnose", post(crate::strm::jobs::diagnose))
        .route("/strm/tasks/{id}", get(crate::strm::jobs::get))
        .route(
            "/strm/tasks/{id}/password",
            post(crate::strm::jobs::set_password),
        )
        .route("/strm/tasks/{id}/cancel", post(crate::strm::jobs::cancel))
        .route(
            "/assets/{id}/thumb",
            get(assets::thumb_asset).head(assets::reject_expensive_head),
        )
        .route("/assets/generate", post(assets::generate_asset_job))
        .route("/events", get(events))
        .with_state(state)
}

#[derive(Debug, Deserialize, Default)]
struct ResourceHealthQuery {
    /// Optional, explicit maintenance probe.  Normal health polling remains
    /// read-only; `?checkpoint=true` runs one passive WAL checkpoint so a
    /// benchmark can capture reader-snapshot evidence without adding that
    /// work to every poll.
    #[serde(default)]
    checkpoint: bool,
}

async fn resource_health(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ResourceHealthQuery>,
) -> Result<Json<serde_json::Value>> {
    let cgroup_memory = resource::cgroup_memory_snapshot().await;
    let cgroup_cpu = resource::cgroup_cpu_snapshot().await;
    if query.checkpoint {
        state.db.checkpoint_wal().await?;
    }
    let sqlite = state.db.runtime_snapshot().await;
    let catalog_queries = catalog::query_metrics_snapshot();
    let catalog_runtime = state.catalog_runtime.snapshot().await;
    let search_runtime = state.search_runtime.snapshot().await;
    // All diagnostic database facts below are intentionally read from one
    // short tracked snapshot.  A one-second sampler must not turn six pool
    // checkouts into a recurring source of connection contention, nor report
    // catalog/search revisions from different generations.
    let mut transaction = state.db.begin_tracked_read_transaction().await?;
    let schema_version = migrations::current_version_in(transaction.connection()).await?;
    let derivatives = state.derivatives.stats_in(transaction.connection()).await?;
    let tag_facets = catalog::tag_kind_count_status_in(transaction.connection()).await?;
    let search_outbox = crate::search::outbox::status_in(transaction.connection()).await?;
    let search_shadow =
        crate::search::outbox::shadow_index_status_in(transaction.connection()).await?;
    let search_reconciliation =
        crate::search::outbox::reconciliation_status_in(transaction.connection()).await?;
    let archive_manifest_cache = sqlx::query(
        r#"
        SELECT resident_bytes, resident_entries, updated_at
        FROM archive_manifest_cache_state
        WHERE singleton = 1
        "#,
    )
    .fetch_one(transaction.connection())
    .await?;
    transaction.commit().await?;
    Ok(Json(json!({
        "status": "ok",
        "schema_version": schema_version,
        "resources": state.resources.snapshot(),
        "cgroup_memory": cgroup_memory,
        "cgroup_cpu": cgroup_cpu,
        "sqlite": sqlite,
        "tag_facets": tag_facets,
        "catalog_queries": catalog_queries,
        "catalog_runtime": catalog_runtime,
        "search_runtime": search_runtime,
        "search_outbox": search_outbox,
        "search_shadow": search_shadow,
        "search_reconciliation": search_reconciliation,
        "jobs": crate::jobs::runtime_snapshot(),
        "archive_manifest_cache": {
            "resident_bytes": archive_manifest_cache.get::<i64, _>("resident_bytes"),
            "resident_entries": archive_manifest_cache.get::<i64, _>("resident_entries"),
            "budget_bytes": state.resources.limits().archive_manifest_cache_bytes / 2,
            "updated_at": archive_manifest_cache.get::<String, _>("updated_at"),
        },
        "search_features": {
            "outbox_shadow": state.config.search_outbox_shadow_enabled,
            "shadow_canary": state.config.search_shadow_canary_enabled,
            "incremental_reader": state.config.search_incremental_reader_enabled,
            "reader_prewarm": state.config.search_reader_prewarm_enabled,
        },
        "derivatives": derivatives,
        "qmediasync": crate::vfs::qms_runtime_snapshot(),
    })))
}

async fn health(State(state): State<Arc<AppState>>) -> Result<Json<serde_json::Value>> {
    let paths = [
        ("comics", state.config.comics_dir.clone()),
        ("novels", state.config.novels_dir.clone()),
        ("audio", state.config.audio_dir.clone()),
        ("gallery", state.config.gallery_dir.clone()),
        ("coser_picture", state.config.coser_picture_dir.clone()),
        ("generated", state.config.generated_dir.clone()),
    ];
    let media = run_filesystem_inspection(move || {
        paths
            .into_iter()
            .map(|(name, path)| {
                let metadata = std::fs::metadata(&path).ok();
                (
                    name.to_string(),
                    json!({
                        "path": path.to_string_lossy(),
                        "exists": metadata.is_some(),
                        "is_dir": metadata.is_some_and(|value| value.is_dir()),
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>()
    })
    .await?;
    // Keep the readiness response coherent and bounded.  These three search
    // status rows describe one deployment generation; reading them through
    // separate pool checkouts could combine state from different revisions
    // and turns a periodic health probe into avoidable connection churn.
    let mut transaction = state.db.begin_tracked_read_transaction().await?;
    let search_outbox = crate::search::outbox::status_in(transaction.connection()).await?;
    let search_shadow =
        crate::search::outbox::shadow_index_status_in(transaction.connection()).await?;
    let search_reconciliation =
        crate::search::outbox::reconciliation_status_in(transaction.connection()).await?;
    transaction.commit().await?;
    Ok(Json(json!({
        "status": "ok",
        "mode": "single-user-private",
        "media": media,
        "search_outbox": search_outbox,
        "search_shadow": search_shadow,
        "search_reconciliation": search_reconciliation,
        "features": {
            "file_watcher": state.config.enable_file_watcher,
            "enrichment_concurrency": state.config.enrichment_concurrency.clamp(1, 8),
            "openai_image_model": state.config.openai_image_model,
            "openai_image_configured": state.config.openai_api_key.is_some(),
            "catalog_v2": state.config.catalog_v2_enabled,
            "facet_bitmap": state.config.facet_bitmap_enabled,
            "inventory_scanner": state.config.inventory_scanner_enabled,
            "inventory_scanner_kinds": state
                .config
                .inventory_enabled_kinds()
                .into_iter()
                .collect::<Vec<_>>(),
            "search_outbox_shadow": state.config.search_outbox_shadow_enabled,
            "search_shadow_canary": state.config.search_shadow_canary_enabled,
            "search_incremental_reader": state.config.search_incremental_reader_enabled,
            "search_reader_prewarm": state.config.search_reader_prewarm_enabled,
            "derivative_cache_v2": state.config.derivative_cache_v2_enabled,
            "jpeg_thumbnail_downscale": state.config.jpeg_thumbnail_downscale_enabled,
        }
    })))
}

#[derive(Debug, Serialize)]
struct CatalogOwnershipItem {
    kind: String,
    authoritative_writer: String,
    updated_at: String,
    roots: i64,
    enabled_roots: i64,
    ready_roots: i64,
    pending_events: i64,
    failed_events: i64,
}

/// Return ownership and coordinator readiness without exposing configured
/// filesystem paths.  This is intentionally read-only so an operator can
/// inspect a NAS before deciding whether to issue the explicit cutover call.
async fn catalog_ownership(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<CatalogOwnershipItem>>> {
    // Ownership status is sampled by operators and Gate tooling. Keep all
    // correlated root/event counts on one short snapshot so a promotion
    // boundary cannot produce a mixed-generation diagnostic response.
    let mut transaction = state.db.begin_tracked_read_transaction().await?;
    let rows = sqlx::query(
        r#"
        SELECT
            ownership.kind,
            ownership.authoritative_writer,
            ownership.updated_at,
            (SELECT COUNT(*) FROM library_roots AS root
             WHERE root.kind = ownership.kind) AS roots,
            (SELECT COUNT(*) FROM library_roots AS root
             WHERE root.kind = ownership.kind AND root.enabled = 1) AS enabled_roots,
            (SELECT COUNT(*) FROM library_roots AS root
             WHERE root.kind = ownership.kind AND root.enabled = 1
               AND root.status = 'idle'
               AND root.active_token IS NULL
               AND root.completed_generation = root.generation) AS ready_roots,
            (SELECT COUNT(*)
             FROM scan_events AS event
             JOIN library_roots AS root ON root.id = event.root_id
             WHERE root.kind = ownership.kind
               AND event.event_kind IN ('catalog-upsert', 'catalog-delete')
               AND event.status IN ('pending', 'processing')) AS pending_events,
            (SELECT COUNT(*)
             FROM scan_events AS event
             JOIN library_roots AS root ON root.id = event.root_id
             WHERE root.kind = ownership.kind
               AND event.event_kind IN ('catalog-upsert', 'catalog-delete')
               AND event.status = 'failed'
               AND event.work_key IS NOT NULL
               AND NOT EXISTS (
                   SELECT 1 FROM scan_events AS newer
                   WHERE newer.root_id = event.root_id
                     AND newer.work_key = event.work_key
                     AND newer.event_kind IN ('catalog-upsert', 'catalog-delete')
                     AND newer.seq > event.seq
               )) AS failed_events
        FROM catalog_kind_ownership AS ownership
        ORDER BY CASE ownership.kind
            WHEN 'novel' THEN 1
            WHEN 'comic' THEN 2
            WHEN 'coser-picture' THEN 3
            WHEN 'audio' THEN 4
            WHEN 'gallery' THEN 5
            ELSE 99 END
        "#,
    )
    .fetch_all(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(Json(
        rows.into_iter()
            .map(|row| CatalogOwnershipItem {
                kind: row.get("kind"),
                authoritative_writer: row.get("authoritative_writer"),
                updated_at: row.get("updated_at"),
                roots: row.get("roots"),
                enabled_roots: row.get("enabled_roots"),
                ready_roots: row.get("ready_roots"),
                pending_events: row.get("pending_events"),
                failed_events: row.get("failed_events"),
            })
            .collect(),
    ))
}

#[derive(Debug, Deserialize)]
struct CatalogOwnershipRequest {
    /// `promote` assigns the kind to the bounded Catalog v2 coordinator;
    /// `rollback` returns it to the legacy scanner.
    action: String,
    reason: Option<String>,
}

async fn change_catalog_ownership(
    State(state): State<Arc<AppState>>,
    Path(kind): Path<String>,
    Json(input): Json<CatalogOwnershipRequest>,
) -> Result<Json<serde_json::Value>> {
    let kind = kind.trim().to_ascii_lowercase();
    if !CATALOG_KINDS.contains(&kind.as_str()) {
        return Err(AppError::BadRequest(format!(
            "unsupported catalog kind {kind:?}"
        )));
    }
    let action = input.action.trim().to_ascii_lowercase();
    let target_writer = match action.as_str() {
        "promote" | "catalog-v2" | "v2" => "catalog-v2",
        "rollback" | "legacy" => "legacy",
        _ => {
            return Err(AppError::BadRequest(
                "catalog ownership action must be promote or rollback".to_string(),
            ))
        }
    };

    if target_writer == "catalog-v2" {
        if !state.config.catalog_v2_enabled {
            return Err(AppError::BadRequest(
                "CATALOG_V2_ENABLED is false; enable the Catalog v2 read/write implementation before promotion"
                    .to_string(),
            ));
        }
        if !state.config.inventory_kind_enabled(&kind) {
            return Err(AppError::BadRequest(
                "INVENTORY_SCANNER_ENABLED and INVENTORY_SCANNER_KINDS must include the target kind before promotion".to_string(),
            ));
        }
        if !state.config.search_outbox_shadow_enabled {
            return Err(AppError::BadRequest(
                "SEARCH_OUTBOX_SHADOW_ENABLED must be true before a kind can be promoted"
                    .to_string(),
            ));
        }
        if matches!(kind.as_str(), "comic" | "coser-picture" | "audio") {
            let runtime_settings = settings::load_settings(&state.config).await?;
            if !crate::vfs::qmediasync_scan_sources(&runtime_settings, &kind).is_empty() {
                return Err(AppError::BadRequest(format!(
                    "cannot promote {kind}: qmediasync roots require provider reconciliation evidence and remain fail-closed"
                )));
            }
        }
    }

    // Promotion rechecks Search evidence inside the same writer transaction as
    // the ownership update. Rollback does not need that gate and retains the
    // existing conservative legacy-reconcile path.
    let change = if target_writer == "catalog-v2" {
        state
            .db
            .change_catalog_kind_ownership_checked(&kind, target_writer, input.reason.as_deref())
            .await?
    } else {
        state
            .db
            .change_catalog_kind_ownership(&kind, target_writer, input.reason.as_deref())
            .await?
    };
    let mut job_id = None;
    let mut job_created = false;
    if change.changed {
        let (id, created) = state
            .db
            .create_job_if_absent(
                "scan-library",
                "queued",
                json!({
                    "source": "catalog-ownership",
                    "kind": kind,
                    "enqueue_enrichment": false,
                }),
            )
            .await?;
        job_id = Some(id);
        job_created = created;
    }
    Ok(Json(json!({
        "status": if change.changed { "changed" } else { "unchanged" },
        "change": change,
        "scan_job_id": job_id,
        "scan_job_created": job_created,
    })))
}

async fn catalog_reconciliation_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<catalog_reconciliation::CatalogReconciliationOverview>> {
    Ok(Json(catalog_reconciliation::overview(&state.db).await?))
}

async fn reconcile_catalog_novel(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>> {
    let (job_id, created) = state
        .db
        .create_job_if_absent(
            catalog_reconciliation::RECONCILE_NOVEL_JOB_TYPE,
            "queued",
            json!({ "source": "catalog-reconciliation-api", "kind": "novel" }),
        )
        .await?;
    state
        .db
        .audit(
            "catalog.reconciliation",
            if created { "queued" } else { "coalesced" },
            json!({ "job_id": job_id, "kind": "novel" }),
        )
        .await?;
    Ok(Json(json!({
        "job_id": job_id,
        "status": if created { "queued" } else { "already-active" },
        "created": created,
    })))
}

async fn reconcile_catalog_comic(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>> {
    let (job_id, created) = state
        .db
        .create_job_if_absent(
            catalog_reconciliation::RECONCILE_COMIC_JOB_TYPE,
            "queued",
            json!({ "source": "catalog-reconciliation-api", "kind": "comic" }),
        )
        .await?;
    state
        .db
        .audit(
            "catalog.reconciliation",
            if created { "queued" } else { "coalesced" },
            json!({ "job_id": job_id, "kind": "comic" }),
        )
        .await?;
    Ok(Json(json!({
        "job_id": job_id,
        "status": if created { "queued" } else { "already-active" },
        "created": created,
    })))
}

async fn reconcile_catalog_coser_picture(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>> {
    let (job_id, created) = state
        .db
        .create_job_if_absent(
            catalog_reconciliation::RECONCILE_COSER_PICTURE_JOB_TYPE,
            "queued",
            json!({ "source": "catalog-reconciliation-api", "kind": "coser-picture" }),
        )
        .await?;
    state
        .db
        .audit(
            "catalog.reconciliation",
            if created { "queued" } else { "coalesced" },
            json!({ "job_id": job_id, "kind": "coser-picture" }),
        )
        .await?;
    Ok(Json(json!({
        "job_id": job_id,
        "status": if created { "queued" } else { "already-active" },
        "created": created,
    })))
}

async fn reconcile_catalog_audio(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>> {
    let (job_id, created) = state
        .db
        .create_job_if_absent(
            catalog_reconciliation::RECONCILE_AUDIO_JOB_TYPE,
            "queued",
            json!({ "source": "catalog-reconciliation-api", "kind": "audio" }),
        )
        .await?;
    state
        .db
        .audit(
            "catalog.reconciliation",
            if created { "queued" } else { "coalesced" },
            json!({ "job_id": job_id, "kind": "audio" }),
        )
        .await?;
    Ok(Json(json!({
        "job_id": job_id,
        "status": if created { "queued" } else { "already-active" },
        "created": created,
    })))
}

async fn reconcile_catalog_gallery(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>> {
    let (job_id, created) = state
        .db
        .create_job_if_absent(
            catalog_reconciliation::RECONCILE_GALLERY_JOB_TYPE,
            "queued",
            json!({ "source": "catalog-reconciliation-api", "kind": "gallery" }),
        )
        .await?;
    state
        .db
        .audit(
            "catalog.reconciliation",
            if created { "queued" } else { "coalesced" },
            json!({ "job_id": job_id, "kind": "gallery" }),
        )
        .await?;
    Ok(Json(json!({
        "job_id": job_id,
        "status": if created { "queued" } else { "already-active" },
        "created": created,
    })))
}

#[derive(Debug, Deserialize)]
struct LibraryQuery {
    cursor: Option<String>,
    limit: Option<i64>,
    include_context: Option<bool>,
}

async fn library(
    State(state): State<Arc<AppState>>,
    Query(query): Query<LibraryQuery>,
) -> Result<Json<LibraryResponse>> {
    let include_context = query
        .include_context
        .unwrap_or_else(|| query.cursor.is_none());
    Ok(Json(
        state
            .db
            .library_page(
                query.cursor.as_deref(),
                query.limit.unwrap_or(100).clamp(1, 500),
                include_context,
            )
            .await?,
    ))
}

async fn jobs(State(state): State<Arc<AppState>>) -> Result<Json<Vec<crate::models::Job>>> {
    Ok(Json(state.db.jobs(100).await?))
}

#[derive(Debug, Deserialize)]
struct WorkDetailQuery {
    /// `legacy` is the compatibility default.  Catalog v2 clients request
    /// `summary` so a large work never embeds its complete asset list.
    asset_mode: Option<String>,
}

fn parse_work_detail_asset_mode(value: Option<&str>) -> Result<WorkDetailAssetMode> {
    match value {
        None | Some("legacy") => Ok(WorkDetailAssetMode::Legacy),
        Some("summary") => Ok(WorkDetailAssetMode::Summary),
        Some(value) => Err(AppError::BadRequest(format!(
            "invalid asset_mode {value:?}; expected legacy or summary"
        ))),
    }
}

async fn work_detail(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Query(query): Query<WorkDetailQuery>,
) -> Result<Json<WorkDetail>> {
    let asset_mode = parse_work_detail_asset_mode(query.asset_mode.as_deref())?;
    state.catalog_runtime.record_work_detail_mode(asset_mode);
    Ok(Json(state.db.work_detail_with_mode(id, asset_mode).await?))
}

#[derive(Debug, Deserialize)]
struct GalleryQuery {
    /// New clients send an opaque `(position,id)` cursor.  A decimal value
    /// is still accepted as a bounded compatibility path for older clients;
    /// those requests use one offset lookup and then receive a keyset cursor.
    cursor: Option<String>,
    limit: Option<i64>,
    v: Option<String>,
}

#[derive(Debug, Serialize)]
struct GalleryAssetsResponse {
    items: Vec<Asset>,
    next_cursor: Option<String>,
    total: i64,
}

#[derive(Debug, Deserialize, Serialize)]
struct GalleryCursor {
    position: i64,
    id: i64,
    #[serde(default)]
    source_version: Option<String>,
    #[serde(default)]
    catalog_revision: Option<i64>,
}

fn encode_gallery_cursor(
    position: i64,
    id: i64,
    source_version: &str,
    catalog_revision: i64,
) -> Result<String> {
    let bytes = serde_json::to_vec(&GalleryCursor {
        position,
        id,
        source_version: Some(source_version.to_string()),
        catalog_revision: Some(catalog_revision),
    })
    .map_err(|err| AppError::Other(format!("encode gallery cursor: {err}")))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_gallery_cursor(cursor: &str) -> Result<GalleryCursor> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| AppError::BadRequest("invalid gallery cursor".to_string()))?;
    serde_json::from_slice(&bytes)
        .map_err(|_| AppError::BadRequest("invalid gallery cursor".to_string()))
}

async fn gallery_assets(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Query(query): Query<GalleryQuery>,
) -> Result<Response> {
    let limit = query.limit.unwrap_or(120).clamp(1, 240);
    let (offset, after, cursor) = match query.cursor.as_deref() {
        None => (Some(0), None, None),
        Some(raw) => match raw.parse::<i64>() {
            Ok(offset) => (Some(offset.max(0)), None, None),
            Err(_) => {
                let cursor = decode_gallery_cursor(raw)?;
                (None, Some((cursor.position, cursor.id)), Some(cursor))
            }
        },
    };
    let page = state
        .db
        .gallery_assets_page(id, offset, after, limit + 1)
        .await?;
    if page.kind != WorkKind::Gallery.as_str() {
        return Err(AppError::BadRequest("work is not a gallery".to_string()));
    }
    if let Some(cursor) = cursor.as_ref() {
        if cursor
            .source_version
            .as_deref()
            .is_some_and(|version| version != page.source_version.to_rfc3339())
        {
            return Err(AppError::BadRequest(
                "gallery cursor expired after the work changed".to_string(),
            ));
        }
        if cursor
            .catalog_revision
            .is_some_and(|revision| revision != page.catalog_revision)
        {
            return Err(AppError::BadRequest(
                "gallery cursor expired after the catalog changed".to_string(),
            ));
        }
    }
    let total = page.total;
    let mut items = page.items;
    let has_more = items.len() as i64 > limit;
    items.truncate(limit as usize);
    let next_cursor = if has_more {
        items
            .last()
            .map(|asset| {
                encode_gallery_cursor(
                    asset.position.unwrap_or(-1),
                    asset.id,
                    &page.source_version.to_rfc3339(),
                    page.catalog_revision,
                )
            })
            .transpose()?
    } else {
        None
    };
    let mut response = Json(GalleryAssetsResponse {
        items,
        next_cursor,
        total,
    })
    .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(assets::media_cache_control(query.v.as_deref())),
    );
    Ok(response)
}

#[derive(Debug, Deserialize)]
struct ProgressRequest {
    progress: f64,
    position: Option<String>,
    update_token: Option<i64>,
}

async fn update_progress(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Json(input): Json<ProgressRequest>,
) -> Result<Json<serde_json::Value>> {
    const MAX_PROGRESS_POSITION_BYTES: usize = 64 * 1024;
    if input
        .position
        .as_ref()
        .is_some_and(|position| position.len() > MAX_PROGRESS_POSITION_BYTES)
    {
        return Err(AppError::BadRequest(format!(
            "progress position exceeds {MAX_PROGRESS_POSITION_BYTES} bytes"
        )));
    }
    let update_token = input
        .update_token
        .filter(|token| *token > 0)
        .unwrap_or_else(|| Utc::now().timestamp_micros());
    let saved = state
        .db
        .update_work_progress(id, input.progress, input.position.as_deref(), update_token)
        .await?;
    Ok(Json(json!({
        "status": if saved.accepted { "saved" } else { "stale" },
        "accepted": saved.accepted,
        "progress": saved.progress,
        "position": saved.position,
    })))
}

async fn tags(State(state): State<Arc<AppState>>) -> Result<Json<Vec<Tag>>> {
    Ok(Json(state.db.tags().await?))
}

async fn history(State(state): State<Arc<AppState>>) -> Result<Json<Vec<HistoryRecord>>> {
    Ok(Json(state.db.history(50).await?))
}

async fn work_history(
    State(state): State<Arc<AppState>>,
    Path(work_id): Path<i64>,
) -> Result<Json<Option<HistoryRecord>>> {
    Ok(Json(state.db.work_history(work_id).await?))
}

#[derive(Debug, Deserialize)]
struct QmsStrmRootTestRequest {
    root: String,
    kind: Option<String>,
    scan_depth: Option<usize>,
}

async fn test_qms_strm_root(
    Json(input): Json<QmsStrmRootTestRequest>,
) -> Result<Json<serde_json::Value>> {
    let root = std::path::PathBuf::from(input.root.trim());
    if input.root.trim().is_empty() {
        return Err(AppError::BadRequest("STRM root is required".to_string()));
    }
    let max_depth = input.scan_depth.unwrap_or(12).clamp(1, 64);
    let kind = input.kind.unwrap_or_else(|| "comic".to_string());
    let scan_root = root.clone();
    let (strm_files, work_count, samples) = run_filesystem_inspection(move || {
        if !scan_root.is_dir() || std::fs::read_dir(&scan_root).is_err() {
            return Err(AppError::BadRequest(format!(
                "STRM root is not a readable directory: {}",
                scan_root.to_string_lossy()
            )));
        }
        let mut strm_files = 0_u64;
        let mut work_dirs = std::collections::BTreeSet::new();
        let mut samples = Vec::new();
        for entry in WalkDir::new(&scan_root)
            .min_depth(1)
            .max_depth(max_depth)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_file())
        {
            let path = entry.path();
            let lower = path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();
            let is_strm = lower.ends_with(".strm");
            let is_kind_match = match kind.as_str() {
                "comic" => lower.ends_with(".cbz") || is_strm,
                "coser-picture" => lower.ends_with(".zip") || is_strm,
                _ => is_strm,
            };
            if !is_kind_match {
                continue;
            }
            if is_strm {
                strm_files += 1;
            }
            if let Some(parent) = path.parent() {
                work_dirs.insert(parent.to_string_lossy().to_string());
            }
            if samples.len() < 5 {
                samples.push(path.to_string_lossy().to_string());
            }
        }
        Ok((strm_files, work_dirs.len(), samples))
    })
    .await??;
    Ok(Json(json!({
        "status": "ok",
        "scope": "local-directory",
        "remote_checked": false,
        "root": root.to_string_lossy(),
        "works": work_count,
        "strm_files": strm_files,
        "samples": samples,
        "message": "仅检查本地 STRM 文件与目录，不发起远程请求",
    })))
}

async fn cloud_status(State(state): State<Arc<AppState>>) -> Result<Json<serde_json::Value>> {
    let settings = settings::load_settings(&state.config).await?;
    let mut source_specs = Vec::new();
    for kind in ["comic", "novel", "audio", "gallery", "coser-picture"] {
        for source in crate::vfs::qmediasync_scan_sources(&settings, kind) {
            let origin = if settings.media_sources.iter().any(|configured| {
                configured.enabled
                    && configured.kind == source.kind
                    && configured.provider == source.provider
                    && configured.mount_name == source.mount_name
                    && configured.root == source.root
            }) {
                "explicit"
            } else if state
                .config
                .qmediasync_strm_dir
                .as_ref()
                .is_some_and(|root| root == std::path::Path::new(&source.root))
            {
                "env"
            } else {
                "legacy"
            };
            source_specs.push((
                source.kind,
                source.provider,
                source.mount_name,
                source.root,
                source.scan_depth,
                origin.to_string(),
            ));
        }
    }
    let source_details = run_filesystem_inspection(move || {
        source_specs
            .into_iter()
            .map(|(kind, provider, mount_name, root, scan_depth, origin)| {
                let path = std::path::PathBuf::from(&root);
                let readable = path.is_dir() && std::fs::read_dir(&path).is_ok();
                let mut discovered = 0_u64;
                if readable {
                    discovered = WalkDir::new(&path)
                        .min_depth(1)
                        .max_depth(scan_depth.clamp(1, 64))
                        .into_iter()
                        .filter_map(|entry| entry.ok())
                        .filter(|entry| entry.file_type().is_file())
                        .filter(|entry| {
                            let name = entry.file_name().to_string_lossy().to_ascii_lowercase();
                            match kind.as_str() {
                                "comic" => name.ends_with(".strm") || name.ends_with(".cbz"),
                                "novel" => name.ends_with(".strm") || name.ends_with(".epub"),
                                "audio" => [
                                    ".strm", ".mp3", ".wav", ".flac", ".ogg", ".m4a", ".aac",
                                    ".opus", ".zip", ".rar", ".7z",
                                ]
                                .iter()
                                .any(|suffix| name.ends_with(suffix)),
                                "gallery" => [
                                    ".strm", ".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif",
                                    ".bmp",
                                ]
                                .iter()
                                .any(|suffix| name.ends_with(suffix)),
                                "coser-picture" => {
                                    name.ends_with(".strm") || name.ends_with(".zip")
                                }
                                _ => false,
                            }
                        })
                        .count() as u64;
                }
                json!({
                    "kind": kind,
                    "provider": provider,
                    "mount_name": mount_name,
                    "root": root,
                    "source": origin,
                    "scan_depth": scan_depth,
                    "readable": readable,
                    "status": if readable { "ready" } else { "failed" },
                    "discovered": discovered,
                    "message": if readable { Value::Null } else { json!("源目录不可读或不存在") },
                })
            })
            .collect::<Vec<_>>()
    })
    .await?;
    let cache_dir = state.config.data_dir.join("cloud-cache");
    let (cache_bytes, cache_files) = run_filesystem_inspection(move || {
        let mut cache_bytes = 0_u64;
        let mut cache_files = 0_u64;
        if cache_dir.exists() {
            for entry in WalkDir::new(&cache_dir)
                .min_depth(1)
                .into_iter()
                .filter_map(|entry| entry.ok())
                .filter(|entry| entry.file_type().is_file())
            {
                if let Ok(meta) = entry.metadata() {
                    cache_bytes = cache_bytes.saturating_add(meta.len());
                    cache_files = cache_files.saturating_add(1);
                }
            }
        }
        (cache_bytes, cache_files)
    })
    .await?;
    Ok(Json(json!({
        "qmediasync": {
            "enabled": settings.qmediasync.enabled,
            "base_url": settings.qmediasync.base_url,
            "configured": settings.qmediasync.enabled
                && (!settings.qmediasync.strm_roots.is_empty()
                    || settings.media_sources.iter().any(|source| source.provider == "qmediasync" && source.enabled)),
            "sources": settings.media_sources.iter().filter(|source| source.provider == "qmediasync" && source.enabled).count(),
            "strm_roots": settings.qmediasync.strm_roots.len(),
            "source_details": source_details,
        },
        "cache": {
            "bytes": cache_bytes,
            "files": cache_files,
            "quota_bytes": state.config.cloud_cache_max_bytes,
        }
    })))
}

async fn scan(
    State(state): State<Arc<AppState>>,
    Json(input): Json<ScanRequest>,
) -> Result<Json<serde_json::Value>> {
    let configured = settings::load_settings(&state.config).await?;
    let enqueue_enrichment = input
        .enqueue_enrichment
        .unwrap_or(configured.scan.enqueue_enrichment);
    let kind = input
        .kind
        .as_deref()
        .map(crate::scanner::normalize_scan_kind)
        .transpose()?;
    let payload = match kind.as_deref() {
        Some(kind) => json!({
            "enqueue_enrichment": enqueue_enrichment,
            "kind": kind,
        }),
        None => json!({ "enqueue_enrichment": enqueue_enrichment }),
    };
    let (job_id, created) = state
        .db
        .create_job_if_absent("scan-library", "queued", payload.clone())
        .await?;
    state
        .db
        .audit(
            "scan",
            if created { "queued" } else { "coalesced" },
            json!({
                "job_id": job_id,
                "enqueue_enrichment": enqueue_enrichment,
                "kind": kind,
            }),
        )
        .await?;
    Ok(Json(json!({
        "job_id": job_id,
        "status": if created { "queued" } else { "already-queued" },
    })))
}

async fn events(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = std::result::Result<Event, Infallible>>> {
    let stream = stream! {
        let mut interval = tokio::time::interval(Duration::from_secs(3));
        loop {
            interval.tick().await;
            let payload = match state.db.jobs(20).await {
                Ok(jobs) => json!({ "jobs": jobs }),
                Err(err) => json!({ "error": err.to_string() }),
            };
            yield Ok(Event::default().event("jobs").data(payload.to_string()));
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_detail_asset_mode_defaults_to_legacy_and_accepts_summary() {
        assert_eq!(
            parse_work_detail_asset_mode(None).unwrap(),
            WorkDetailAssetMode::Legacy
        );
        assert_eq!(
            parse_work_detail_asset_mode(Some("legacy")).unwrap(),
            WorkDetailAssetMode::Legacy
        );
        assert_eq!(
            parse_work_detail_asset_mode(Some("summary")).unwrap(),
            WorkDetailAssetMode::Summary
        );
    }

    #[test]
    fn work_detail_asset_mode_rejects_unknown_values() {
        let error = parse_work_detail_asset_mode(Some("all"))
            .expect_err("unknown detail modes must fail closed");
        assert!(matches!(error, AppError::BadRequest(message) if message.contains("asset_mode")));
    }

    #[test]
    fn search_promotion_gate_accepts_a_revision_zero_empty_catalog() {
        let shadow = crate::search::outbox::ShadowIndexStatus {
            index_name: "shadow-v3".to_string(),
            schema_version: 1,
            baseline_revision: 0,
            applied_revision: 0,
            baseline_search_revision: 0,
            applied_search_revision: 0,
            indexed_documents: 0,
            ready: false,
            status: "shadow".to_string(),
            last_error: None,
            updated_at: "2026-08-18T00:00:00.000Z".to_string(),
        };
        let reconciliation = crate::search::outbox::ShadowReconciliationStatus {
            index_name: "shadow-v3".to_string(),
            status: "passed".to_string(),
            catalog_revision: 0,
            catalog_revision_after: 0,
            applied_revision: 0,
            applied_revision_after: 0,
            search_revision: 0,
            search_revision_after: 0,
            applied_search_revision: 0,
            applied_search_revision_after: 0,
            sqlite_work_count: 0,
            index_document_count: 0,
            index_unique_work_count: 0,
            missing_work_ids: 0,
            unexpected_work_ids: 0,
            duplicate_documents: 0,
            invalid_documents: 0,
            sqlite_ids_sha256: Some(
                "af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc".to_string(),
            ),
            index_ids_sha256: Some(
                "af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc".to_string(),
            ),
            consecutive_passes: 1,
            passed_since: Some("2026-08-18T00:00:00.000Z".to_string()),
            last_checked_at: Some("2026-08-18T00:00:00.000Z".to_string()),
            took_millis: Some(1),
            cutover_armed: false,
        };
        let outbox = crate::search::outbox::OutboxStatus {
            pending: 0,
            claimed: 0,
            committed: 0,
            failed: 0,
            retrying: 0,
            max_attempts: 0,
            stale_claims: 0,
            oldest_failed_at: None,
            oldest_pending_at: None,
            latest_committed_at: None,
            oldest_pending_revision: None,
            oldest_pending_search_revision: None,
            catalog_revision: 0,
            shadow_applied_revision: 0,
            revision_lag: 0,
            search_revision: 0,
            shadow_applied_search_revision: 0,
            search_revision_lag: 0,
        };

        assert!(crate::search::outbox::search_promotion_gate_ready(
            &shadow,
            &reconciliation,
            &outbox,
        ));
    }

    #[test]
    fn search_promotion_gate_rejects_stale_or_incompatible_evidence() {
        let mut shadow = crate::search::outbox::ShadowIndexStatus {
            index_name: "shadow-v3".to_string(),
            schema_version: 1,
            baseline_revision: 0,
            applied_revision: 3,
            baseline_search_revision: 0,
            applied_search_revision: 3,
            indexed_documents: 1,
            ready: true,
            status: "ready".to_string(),
            last_error: None,
            updated_at: "2026-08-18T00:00:00.000Z".to_string(),
        };
        let reconciliation = crate::search::outbox::ShadowReconciliationStatus {
            index_name: "shadow-v3".to_string(),
            status: "passed".to_string(),
            catalog_revision: 3,
            catalog_revision_after: 3,
            applied_revision: 3,
            applied_revision_after: 3,
            search_revision: 3,
            search_revision_after: 3,
            applied_search_revision: 3,
            applied_search_revision_after: 3,
            sqlite_work_count: 1,
            index_document_count: 1,
            index_unique_work_count: 1,
            missing_work_ids: 0,
            unexpected_work_ids: 0,
            duplicate_documents: 0,
            invalid_documents: 0,
            sqlite_ids_sha256: Some("a".repeat(64)),
            index_ids_sha256: Some("a".repeat(64)),
            consecutive_passes: 1,
            passed_since: Some("2026-08-18T00:00:00.000Z".to_string()),
            last_checked_at: Some("2026-08-18T00:00:00.000Z".to_string()),
            took_millis: Some(1),
            cutover_armed: false,
        };
        let outbox = crate::search::outbox::OutboxStatus {
            pending: 0,
            claimed: 0,
            committed: 0,
            failed: 0,
            retrying: 0,
            max_attempts: 0,
            stale_claims: 0,
            oldest_failed_at: None,
            oldest_pending_at: None,
            latest_committed_at: None,
            oldest_pending_revision: None,
            oldest_pending_search_revision: None,
            catalog_revision: 3,
            shadow_applied_revision: 3,
            revision_lag: 0,
            search_revision: 3,
            shadow_applied_search_revision: 3,
            search_revision_lag: 0,
        };

        assert!(crate::search::outbox::search_promotion_gate_ready(
            &shadow,
            &reconciliation,
            &outbox,
        ));
        shadow.schema_version += 1;
        assert!(!crate::search::outbox::search_promotion_gate_ready(
            &shadow,
            &reconciliation,
            &outbox,
        ));
        shadow.schema_version = 1;
        shadow.status = "unknown".to_string();
        assert!(!crate::search::outbox::search_promotion_gate_ready(
            &shadow,
            &reconciliation,
            &outbox,
        ));
    }

    #[test]
    fn search_promotion_gate_rejects_passed_status_with_count_drift() {
        let mut shadow = crate::search::outbox::ShadowIndexStatus {
            index_name: "shadow-v3".to_string(),
            schema_version: 1,
            baseline_revision: 4,
            applied_revision: 4,
            baseline_search_revision: 4,
            applied_search_revision: 4,
            indexed_documents: 2,
            ready: true,
            status: "ready".to_string(),
            last_error: None,
            updated_at: "2026-08-23T00:00:00.000Z".to_string(),
        };
        let mut reconciliation = crate::search::outbox::ShadowReconciliationStatus {
            index_name: "shadow-v3".to_string(),
            status: "passed".to_string(),
            catalog_revision: 4,
            catalog_revision_after: 4,
            applied_revision: 4,
            applied_revision_after: 4,
            search_revision: 4,
            search_revision_after: 4,
            applied_search_revision: 4,
            applied_search_revision_after: 4,
            sqlite_work_count: 2,
            index_document_count: 2,
            index_unique_work_count: 2,
            missing_work_ids: 0,
            unexpected_work_ids: 0,
            duplicate_documents: 0,
            invalid_documents: 0,
            sqlite_ids_sha256: Some("a".repeat(64)),
            index_ids_sha256: Some("a".repeat(64)),
            consecutive_passes: 1,
            passed_since: Some("2026-08-23T00:00:00.000Z".to_string()),
            last_checked_at: Some("2026-08-23T00:00:00.000Z".to_string()),
            took_millis: Some(1),
            cutover_armed: false,
        };
        let outbox = crate::search::outbox::OutboxStatus {
            pending: 0,
            claimed: 0,
            committed: 0,
            failed: 0,
            retrying: 0,
            max_attempts: 0,
            stale_claims: 0,
            oldest_failed_at: None,
            oldest_pending_at: None,
            latest_committed_at: None,
            oldest_pending_revision: None,
            oldest_pending_search_revision: None,
            catalog_revision: 4,
            shadow_applied_revision: 4,
            revision_lag: 0,
            search_revision: 4,
            shadow_applied_search_revision: 4,
            search_revision_lag: 0,
        };

        assert!(crate::search::outbox::search_promotion_gate_ready(
            &shadow,
            &reconciliation,
            &outbox,
        ));

        reconciliation.index_document_count = 1;
        assert!(!crate::search::outbox::search_promotion_gate_ready(
            &shadow,
            &reconciliation,
            &outbox,
        ));

        reconciliation.index_document_count = 2;
        reconciliation.index_ids_sha256 = Some("b".repeat(64));
        assert!(!crate::search::outbox::search_promotion_gate_ready(
            &shadow,
            &reconciliation,
            &outbox,
        ));

        reconciliation.index_ids_sha256 = reconciliation.sqlite_ids_sha256.clone();
        shadow.indexed_documents = 1;
        assert!(!crate::search::outbox::search_promotion_gate_ready(
            &shadow,
            &reconciliation,
            &outbox,
        ));

        shadow.indexed_documents = 2;
        reconciliation.sqlite_ids_sha256 = Some("a".to_string());
        reconciliation.index_ids_sha256 = Some("a".to_string());
        assert!(!crate::search::outbox::search_promotion_gate_ready(
            &shadow,
            &reconciliation,
            &outbox,
        ));
    }
}
