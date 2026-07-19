use std::convert::Infallible;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use async_stream::stream;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use chrono::Utc;
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Semaphore;
use walkdir::WalkDir;

use crate::assets;
use crate::auth;
use crate::enrich;
use crate::error::{AppError, Result};
use crate::models::{
    Asset, HistoryRecord, LibraryResponse, ScanRequest, Tag, WorkDetail, WorkKind,
};
use crate::search;
use crate::settings;
use crate::AppState;

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
        .route("/auth/session", get(auth::session))
        .route("/auth/login", post(auth::login))
        .route("/auth/logout", post(auth::logout))
        .route("/auth/password", patch(auth::change_password))
        .route("/library", get(library))
        .route("/history", get(history))
        .route(
            "/settings",
            get(settings::get_settings).patch(settings::update_settings),
        )
        .route("/search", get(search::search))
        .route("/search/rebuild", post(search::enqueue_rebuild))
        .route("/cloud/status", get(cloud_status))
        .route("/cloud/qmediasync/test-strm-root", post(test_qms_strm_root))
        .route("/works/{id}", get(work_detail))
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
        .route(
            "/assets/{id}/thumb",
            get(assets::thumb_asset).head(assets::reject_expensive_head),
        )
        .route("/assets/generate", post(assets::generate_asset_job))
        .route("/events", get(events))
        .with_state(state)
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
    Ok(Json(json!({
        "status": "ok",
        "mode": "single-user-private",
        "media": media,
        "features": {
            "file_watcher": state.config.enable_file_watcher,
            "enrichment_concurrency": state.config.enrichment_concurrency.clamp(1, 8),
            "openai_image_model": state.config.openai_image_model,
            "openai_image_configured": state.config.openai_api_key.is_some(),
        }
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

async fn work_detail(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<Json<WorkDetail>> {
    Ok(Json(state.db.work_detail(id).await?))
}

#[derive(Debug, Deserialize)]
struct GalleryQuery {
    cursor: Option<i64>,
    limit: Option<i64>,
    v: Option<String>,
}

#[derive(Debug, Serialize)]
struct GalleryAssetsResponse {
    items: Vec<Asset>,
    next_cursor: Option<i64>,
    total: i64,
}

async fn gallery_assets(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Query(query): Query<GalleryQuery>,
) -> Result<Response> {
    let (kind, _) = state.db.work_kind_and_meta(id).await?;
    if kind != WorkKind::Gallery.as_str() {
        return Err(AppError::BadRequest("work is not a gallery".to_string()));
    }
    let offset = query.cursor.unwrap_or(0).max(0);
    let limit = query.limit.unwrap_or(120).clamp(1, 240);
    let total = state.db.gallery_asset_count(id).await?;
    let items = if offset >= total {
        Vec::new()
    } else {
        state.db.gallery_assets(id, offset, limit).await?
    };
    let next = offset + items.len() as i64;
    let mut response = Json(GalleryAssetsResponse {
        items,
        next_cursor: (next < total).then_some(next),
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
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(input): Json<QmsStrmRootTestRequest>,
) -> Result<Json<serde_json::Value>> {
    auth::require_csrf(&state, &headers, "cloud.qmediasync.test-strm-root").await?;
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
        "root": root.to_string_lossy(),
        "works": work_count,
        "strm_files": strm_files,
        "samples": samples,
    })))
}

async fn cloud_status(State(state): State<Arc<AppState>>) -> Result<Json<serde_json::Value>> {
    let settings = settings::load_settings(&state.config).await?;
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
    headers: HeaderMap,
    Json(input): Json<ScanRequest>,
) -> Result<Json<serde_json::Value>> {
    auth::require_csrf(&state, &headers, "scan").await?;
    let configured = settings::load_settings(&state.config).await?;
    let enqueue_enrichment = input
        .enqueue_enrichment
        .unwrap_or(configured.scan.enqueue_enrichment);
    let payload = json!({ "enqueue_enrichment": enqueue_enrichment });
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
