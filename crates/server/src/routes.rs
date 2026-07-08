use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use async_stream::stream;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::json;
use walkdir::WalkDir;

use crate::assets;
use crate::auth;
use crate::enrich;
use crate::error::{AppError, Result};
use crate::models::{
    Asset, HistoryRecord, LibraryResponse, ScanRequest, ScanResponse, Tag, WorkDetail, WorkKind,
};
use crate::scanner;
use crate::search;
use crate::settings;
use crate::AppState;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/auth/session", get(auth::session))
        .route("/auth/login", post(auth::login))
        .route("/auth/logout", post(auth::logout))
        .route("/auth/password", patch(auth::change_password))
        .route("/auth/password/reset", post(auth::reset_password))
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
        .route("/works/{id}/cover", get(assets::work_cover))
        .route("/works/{id}/gallery", get(gallery_assets))
        .route("/works/{id}/progress", patch(update_progress))
        .route("/works/{id}/pages", get(assets::comic_pages))
        .route(
            "/works/{id}/pages/{page}/stream",
            get(assets::stream_comic_page),
        )
        .route("/works/{id}/epub", get(assets::epub_manifest))
        .route(
            "/works/{id}/epub/{chapter}/html",
            get(assets::epub_chapter_html),
        )
        .route("/works/{id}/epub/image", get(assets::stream_epub_image))
        .route("/scan", post(scan))
        .route("/enrich", post(enrich::enqueue_enrich))
        .route("/tags", get(tags))
        .route("/assets/{id}/stream", get(assets::stream_asset))
        .route("/assets/{id}/route", get(assets::asset_route))
        .route("/assets/{id}/thumb", get(assets::thumb_asset))
        .route("/assets/generate", post(assets::generate_asset_job))
        .route("/events", get(events))
        .with_state(state)
}

async fn health(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let dir_state = |path: &std::path::Path| {
        json!({
            "path": path.to_string_lossy(),
            "exists": path.exists(),
            "is_dir": path.is_dir(),
        })
    };
    Json(json!({
        "status": "ok",
        "mode": "single-user-private",
        "media": {
            "comics": dir_state(&state.config.comics_dir),
            "novels": dir_state(&state.config.novels_dir),
            "audio": dir_state(&state.config.audio_dir),
            "gallery": dir_state(&state.config.gallery_dir),
            "coser_picture": dir_state(&state.config.coser_picture_dir),
            "generated": dir_state(&state.config.generated_dir),
        },
        "features": {
            "file_watcher": state.config.enable_file_watcher,
            "enrichment_concurrency": state.config.enrichment_concurrency.clamp(1, 8),
            "openai_image_model": state.config.openai_image_model,
            "openai_image_configured": state.config.openai_api_key.is_some(),
        }
    }))
}

async fn library(State(state): State<Arc<AppState>>) -> Result<Json<LibraryResponse>> {
    Ok(Json(state.db.library().await?))
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
) -> Result<Json<GalleryAssetsResponse>> {
    let (kind, meta_json) = state.db.work_kind_and_meta(id).await?;
    if kind != WorkKind::Gallery.as_str() {
        return Err(AppError::BadRequest("work is not a gallery".to_string()));
    }
    let offset = query.cursor.unwrap_or(0).max(0);
    let limit = query.limit.unwrap_or(120).clamp(1, 240);
    let total_hint = serde_json::from_str::<serde_json::Value>(&meta_json)
        .ok()
        .and_then(|value| value.get("image_count").and_then(|count| count.as_i64()))
        .filter(|count| *count >= 0);
    let total = match total_hint {
        Some(count) => count,
        None => state.db.gallery_asset_count(id).await?,
    };
    let items = if offset >= total {
        Vec::new()
    } else {
        state.db.gallery_assets(id, offset, limit).await?
    };
    let next = offset + items.len() as i64;
    Ok(Json(GalleryAssetsResponse {
        items,
        next_cursor: (next < total).then_some(next),
        total,
    }))
}

#[derive(Debug, Deserialize)]
struct ProgressRequest {
    progress: f64,
    position: Option<String>,
}

async fn update_progress(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Json(input): Json<ProgressRequest>,
) -> Result<Json<serde_json::Value>> {
    state
        .db
        .update_work_progress(id, input.progress, input.position.as_deref())
        .await?;
    state
        .db
        .audit(
            "works.progress",
            "saved",
            json!({ "work_id": id, "progress": input.progress, "position": input.position }),
        )
        .await?;
    Ok(Json(
        json!({ "status": "saved", "progress": input.progress.clamp(0.0, 1.0), "position": input.position }),
    ))
}

async fn tags(State(state): State<Arc<AppState>>) -> Result<Json<Vec<Tag>>> {
    Ok(Json(state.db.tags().await?))
}

async fn history(State(state): State<Arc<AppState>>) -> Result<Json<Vec<HistoryRecord>>> {
    Ok(Json(state.db.history(50).await?))
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
    if !root.exists() || !root.is_dir() {
        return Err(AppError::BadRequest(format!(
            "STRM root is not a readable directory: {}",
            root.to_string_lossy()
        )));
    }
    let max_depth = input.scan_depth.unwrap_or(12).clamp(1, 64);
    let mut strm_files = 0_u64;
    let mut work_dirs = std::collections::BTreeSet::new();
    let mut samples = Vec::new();
    for entry in WalkDir::new(&root)
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
        let is_kind_match = match input.kind.as_deref().unwrap_or("comic") {
            "comic" => lower.ends_with(".cbz") || lower.ends_with(".cbz.strm"),
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
    Ok(Json(json!({
        "status": "ok",
        "root": root.to_string_lossy(),
        "works": work_dirs.len(),
        "strm_files": strm_files,
        "samples": samples,
    })))
}

async fn cloud_status(State(state): State<Arc<AppState>>) -> Result<Json<serde_json::Value>> {
    let settings = settings::load_settings(&state.config).await?;
    let cache_dir = state.config.data_dir.join("cloud-cache");
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
                cache_bytes += meta.len();
                cache_files += 1;
            }
        }
    }
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
        }
    })))
}

async fn scan(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(input): Json<ScanRequest>,
) -> Result<Json<ScanResponse>> {
    auth::require_csrf(&state, &headers, "scan").await?;
    state
        .db
        .audit("scan", "queued", json!({ "enqueue_enrichment": false }))
        .await?;
    Ok(Json(
        scanner::scan_all(&state, input.enqueue_enrichment.unwrap_or(false)).await?,
    ))
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
