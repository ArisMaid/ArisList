use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufWriter, Cursor, Read};
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use image::codecs::jpeg::JpegEncoder;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio::sync::Semaphore;
use tokio_util::io::ReaderStream;
use zip::ZipArchive;

use crate::auth;
use crate::error::{AppError, Result};
use crate::models::WorkDetail;
use crate::scanner::{image_name, naturalish_key};
use crate::security::path_mime;
use crate::vfs;
use crate::AppState;

static THUMBNAIL_WORKERS: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(2));
const COMIC_DIMENSION_SAMPLE_LIMIT: usize = 8;
const COMIC_PAGE_CACHE_LIMIT: usize = 24;

pub type ComicPageCache = tokio::sync::RwLock<HashMap<String, CachedComicPages>>;

#[derive(Clone)]
pub struct CachedComicPages {
    size: u64,
    modified: Option<SystemTime>,
    pages: Arc<Vec<ComicPageInfo>>,
    archive: Arc<Mutex<ZipArchive<File>>>,
}

pub async fn stream_asset(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    headers: HeaderMap,
) -> Result<Response> {
    let asset = state.db.asset(id).await?;
    if vfs::is_qms_strm_uri(&asset.path) {
        return vfs::stream_qms_asset(state, asset, headers).await;
    }
    let path = vfs::local_asset_path(&state, &asset.path).await?;
    let mut file = tokio::fs::File::open(&path).await?;
    let size = file.metadata().await?.len();

    if let Some((start, end)) = parse_byte_range(&headers, size) {
        let length = end - start + 1;
        file.seek(SeekFrom::Start(start)).await?;
        let stream = ReaderStream::new(file.take(length));
        let body = Body::from_stream(stream);
        return Ok(Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header(header::CONTENT_TYPE, asset.mime)
            .header(header::CACHE_CONTROL, "private, max-age=86400")
            .header(header::ACCEPT_RANGES, "bytes")
            .header(header::CONTENT_LENGTH, length.to_string())
            .header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{size}"))
            .body(body)
            .map_err(|e| AppError::Other(e.to_string()))?);
    }

    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, asset.mime)
        .header(header::CACHE_CONTROL, "private, max-age=86400")
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, size.to_string())
        .body(body)
        .map_err(|e| AppError::Other(e.to_string()))?)
}

pub async fn asset_route(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<Json<vfs::AssetRouteInfo>> {
    let asset = state.db.asset(id).await?;
    Ok(Json(vfs::asset_route_info(&state, &asset).await?))
}

#[derive(Debug, Deserialize)]
pub struct ThumbQuery {
    pub size: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct CoverQuery {
    pub size: Option<u32>,
}

pub async fn work_cover(
    State(state): State<Arc<AppState>>,
    Path(work_id): Path<i64>,
    Query(query): Query<CoverQuery>,
) -> Result<Response> {
    let detail = state.db.work_detail(work_id).await?;
    let size = query.size.unwrap_or(480).clamp(96, 960);
    let settings = crate::settings::load_settings(&state.config).await?;
    let cache_dir = settings.cover_cache_dirs.for_work_kind(&detail.work.kind);
    tokio::fs::create_dir_all(&cache_dir).await?;

    if let Some(asset_id) = detail.work.cover_asset_id {
        let asset = state.db.asset(asset_id).await?;
        if asset.mime.starts_with("image/") {
            return cached_image_cover(state, detail.work.id, asset, cache_dir, size).await;
        }
    }

    if matches!(detail.work.kind.as_str(), "comic" | "coser-picture") {
        let archive = detail
            .assets
            .iter()
            .find(|asset| asset.role == "archive")
            .cloned()
            .ok_or_else(|| AppError::NotFound("archive cover source not found".to_string()))?;
        return cached_archive_cover(state, detail.work.id, archive, cache_dir, size).await;
    }

    Err(AppError::NotFound("cover source not found".to_string()))
}

pub async fn thumb_asset(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Query(query): Query<ThumbQuery>,
) -> Result<Response> {
    let asset = state.db.asset(id).await?;
    if !asset.mime.starts_with("image/") {
        return Err(AppError::BadRequest("asset is not an image".to_string()));
    }
    let size = query.size.unwrap_or(360).clamp(96, 960);
    let thumbs_dir = state.config.data_dir.join("thumbs");
    tokio::fs::create_dir_all(&thumbs_dir).await?;
    let (cache_path, source_path) = if vfs::is_qms_strm_uri(&asset.path) {
        (
            thumbs_dir.join(format!(
                "{}-{}-{}.jpg",
                asset.id,
                size,
                vfs::cloud_cache_key(&asset)
            )),
            None,
        )
    } else {
        let source_path = vfs::local_asset_path(&state, &asset.path).await?;
        let source_meta = tokio::fs::metadata(&source_path).await?;
        let modified = source_meta
            .modified()
            .ok()
            .and_then(system_time_key)
            .unwrap_or(0);
        (
            thumbs_dir.join(format!(
                "{}-{}-{}-{}.jpg",
                asset.id,
                size,
                source_meta.len(),
                modified
            )),
            Some(source_path),
        )
    };

    if tokio::fs::try_exists(&cache_path).await? {
        return stream_thumb_cache(cache_path).await;
    }
    let _permit = THUMBNAIL_WORKERS
        .acquire()
        .await
        .map_err(|e| AppError::Other(e.to_string()))?;
    let generated = if let Some(source_path) = source_path.clone() {
        tokio::task::spawn_blocking({
            let cache_path = cache_path.clone();
            move || generate_thumbnail(&source_path, &cache_path, size)
        })
        .await
        .map_err(|e| AppError::Other(e.to_string()))?
    } else {
        vfs::generate_qms_thumbnail(&state, &asset, &cache_path, size)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    };

    match generated {
        Ok(()) => stream_thumb_cache(cache_path).await,
        Err(err) => {
            tracing::warn!(asset_id = asset.id, error = %err, "thumbnail generation failed; streaming original image");
            if let Some(source_path) = source_path {
                stream_original_image(source_path, asset.mime).await
            } else {
                vfs::stream_qms_asset(state, asset, HeaderMap::new()).await
            }
        }
    }
}

async fn cached_image_cover(
    state: Arc<AppState>,
    work_id: i64,
    asset: crate::models::Asset,
    cache_dir: PathBuf,
    size: u32,
) -> Result<Response> {
    let (cache_path, source_path) = if vfs::is_qms_strm_uri(&asset.path) {
        (
            cache_dir.join(format!(
                "work-{work_id}-asset-{}-{size}-{}.jpg",
                asset.id,
                vfs::cloud_cache_key(&asset)
            )),
            None,
        )
    } else {
        let source_path = vfs::local_asset_path(&state, &asset.path).await?;
        let source_meta = tokio::fs::metadata(&source_path).await?;
        let modified = source_meta
            .modified()
            .ok()
            .and_then(system_time_key)
            .unwrap_or(0);
        (
            cache_dir.join(format!(
                "work-{work_id}-asset-{}-{size}-{}-{modified}.jpg",
                asset.id,
                source_meta.len()
            )),
            Some(source_path),
        )
    };

    if tokio::fs::try_exists(&cache_path).await? {
        return stream_thumb_cache(cache_path).await;
    }
    let _permit = THUMBNAIL_WORKERS
        .acquire()
        .await
        .map_err(|e| AppError::Other(e.to_string()))?;
    let generated = if let Some(source_path) = source_path.clone() {
        tokio::task::spawn_blocking({
            let cache_path = cache_path.clone();
            move || generate_thumbnail(&source_path, &cache_path, size)
        })
        .await
        .map_err(|e| AppError::Other(e.to_string()))?
    } else {
        vfs::generate_qms_thumbnail(&state, &asset, &cache_path, size)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    };

    match generated {
        Ok(()) => stream_thumb_cache(cache_path).await,
        Err(err) => Err(AppError::Other(format!(
            "cover thumbnail generation failed: {err}"
        ))),
    }
}

async fn cached_archive_cover(
    state: Arc<AppState>,
    work_id: i64,
    archive: crate::models::Asset,
    cache_dir: PathBuf,
    size: u32,
) -> Result<Response> {
    let path = vfs::asset_local_processing_path(&state, &archive).await?;
    let metadata = tokio::fs::metadata(&path).await?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(system_time_key)
        .unwrap_or(0);
    let cached = cached_cbz_pages(&state, &path).await?;
    let page_name = cached
        .pages
        .first()
        .ok_or_else(|| AppError::NotFound("archive cover page not found".to_string()))?
        .name
        .clone();
    let cache_path = cache_dir.join(format!(
        "work-{work_id}-archive-{}-{size}-{}-{modified}-{}.jpg",
        archive.id,
        metadata.len(),
        short_hash(&page_name)
    ));
    if tokio::fs::try_exists(&cache_path).await? {
        return stream_thumb_cache(cache_path).await;
    }

    let _permit = THUMBNAIL_WORKERS
        .acquire()
        .await
        .map_err(|e| AppError::Other(e.to_string()))?;
    let archive_cache = cached.archive.clone();
    let stream_name = page_name.clone();
    let out = cache_path.clone();
    tokio::task::spawn_blocking(move || {
        let bytes =
            cbz_named_page_bytes(&archive_cache, &stream_name).map_err(|e| e.to_string())?;
        generate_thumbnail_from_bytes(&bytes, &out, size)
    })
    .await
    .map_err(|e| AppError::Other(e.to_string()))?
    .map_err(AppError::Other)?;
    stream_thumb_cache(cache_path).await
}

fn system_time_key(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|value| value.as_secs())
}

fn generate_thumbnail(
    source_path: &FsPath,
    cache_path: &FsPath,
    size: u32,
) -> std::result::Result<(), String> {
    let image = image::ImageReader::open(source_path)
        .map_err(|e| e.to_string())?
        .with_guessed_format()
        .map_err(|e| e.to_string())?
        .decode()
        .map_err(|e| e.to_string())?;
    let thumb = image.thumbnail(size, size).to_rgb8();
    let file = File::create(cache_path).map_err(|e| e.to_string())?;
    let mut writer = BufWriter::new(file);
    let mut encoder = JpegEncoder::new_with_quality(&mut writer, 84);
    encoder.encode_image(&thumb).map_err(|e| e.to_string())
}

fn generate_thumbnail_from_bytes(
    bytes: &[u8],
    cache_path: &FsPath,
    size: u32,
) -> std::result::Result<(), String> {
    let image = image::load_from_memory(bytes).map_err(|e| e.to_string())?;
    let thumb = image.thumbnail(size, size).to_rgb8();
    let file = File::create(cache_path).map_err(|e| e.to_string())?;
    let mut writer = BufWriter::new(file);
    let mut encoder = JpegEncoder::new_with_quality(&mut writer, 84);
    encoder.encode_image(&thumb).map_err(|e| e.to_string())
}

fn short_hash(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())[..16].to_string()
}

async fn stream_thumb_cache(path: PathBuf) -> Result<Response> {
    let file = tokio::fs::File::open(&path).await?;
    let size = file.metadata().await?.len();
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "image/jpeg")
        .header(header::CACHE_CONTROL, "public, max-age=604800, immutable")
        .header(header::CONTENT_LENGTH, size.to_string())
        .body(body)
        .map_err(|e| AppError::Other(e.to_string()))?)
}

async fn stream_original_image(path: PathBuf, mime: String) -> Result<Response> {
    let file = tokio::fs::File::open(&path).await?;
    let size = file.metadata().await?.len();
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime)
        .header(header::CACHE_CONTROL, "private, max-age=86400")
        .header(header::CONTENT_LENGTH, size.to_string())
        .body(body)
        .map_err(|e| AppError::Other(e.to_string()))?)
}

fn parse_byte_range(headers: &HeaderMap, size: u64) -> Option<(u64, u64)> {
    if size == 0 {
        return None;
    }
    let header = headers.get(header::RANGE)?.to_str().ok()?;
    let spec = header.strip_prefix("bytes=")?.split(',').next()?.trim();
    let (start, end) = spec.split_once('-')?;
    if start.is_empty() {
        let suffix = end.parse::<u64>().ok()?.min(size);
        if suffix == 0 {
            return None;
        }
        return Some((size - suffix, size - 1));
    }
    let start = start.parse::<u64>().ok()?;
    if start >= size {
        return None;
    }
    let end = if end.is_empty() {
        size - 1
    } else {
        end.parse::<u64>().ok()?.min(size - 1)
    };
    (end >= start).then_some((start, end))
}

#[derive(Debug, Serialize)]
pub struct ComicPagesResponse {
    pub pages: Vec<ComicPageInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ComicPageInfo {
    pub name: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

pub async fn comic_pages(
    State(state): State<Arc<AppState>>,
    Path(work_id): Path<i64>,
) -> Result<Json<ComicPagesResponse>> {
    let detail = state.db.work_detail(work_id).await?;
    let archive = detail
        .assets
        .iter()
        .find(|asset| asset.role == "archive")
        .ok_or_else(|| AppError::NotFound("comic archive asset not found".to_string()))?;
    let path = vfs::asset_local_processing_path(&state, archive).await?;
    let cached = cached_cbz_pages(&state, &path).await?;
    if let Err(err) = maybe_update_comic_page_count(&state, &detail, cached.pages.len()).await {
        tracing::warn!(
            work_id = detail.work.id,
            page_count = cached.pages.len(),
            error = %err,
            "failed to persist archive page count"
        );
    }
    Ok(Json(ComicPagesResponse {
        pages: (*cached.pages).clone(),
    }))
}

async fn maybe_update_comic_page_count(
    state: &AppState,
    detail: &WorkDetail,
    page_count: usize,
) -> Result<()> {
    if !matches!(detail.work.kind.as_str(), "comic" | "coser-picture") || page_count == 0 {
        return Ok(());
    }
    let mut meta = serde_json::from_str::<serde_json::Value>(&detail.work.meta_json)
        .unwrap_or_else(|_| json!({}));
    let current = meta
        .get("page_count")
        .and_then(|value| value.as_i64())
        .unwrap_or(0);
    if current == page_count as i64 {
        return Ok(());
    }
    if let Some(object) = meta.as_object_mut() {
        object.insert("page_count".to_string(), json!(page_count as i64));
        state.db.update_work_meta(detail.work.id, meta).await?;
    }
    Ok(())
}

fn open_cbz_pages(path: &FsPath) -> Result<CachedComicPages> {
    let metadata = std::fs::metadata(path)?;
    let size = metadata.len();
    let modified = metadata.modified().ok();
    let file = File::open(path)?;
    let mut archive = ZipArchive::new(file).map_err(|e| AppError::Other(e.to_string()))?;
    let mut names = Vec::new();
    for i in 0..archive.len() {
        let entry = archive
            .by_index(i)
            .map_err(|e| AppError::Other(e.to_string()))?;
        if image_name(entry.name()) {
            names.push(entry.name().to_string());
        }
    }
    names.sort_by_cached_key(|name| naturalish_key(name));
    let mut pages = Vec::with_capacity(names.len());
    for (index, name) in names.into_iter().enumerate() {
        let dimensions = if index < COMIC_DIMENSION_SAMPLE_LIMIT {
            archive.by_name(&name).ok().and_then(|mut entry| {
                let mut bytes = Vec::new();
                entry.read_to_end(&mut bytes).ok()?;
                image::ImageReader::new(Cursor::new(bytes))
                    .with_guessed_format()
                    .ok()?
                    .into_dimensions()
                    .ok()
            })
        } else {
            None
        };
        pages.push(ComicPageInfo {
            name,
            width: dimensions.map(|(width, _)| width),
            height: dimensions.map(|(_, height)| height),
        });
    }
    Ok(CachedComicPages {
        size,
        modified,
        pages: Arc::new(pages),
        archive: Arc::new(Mutex::new(archive)),
    })
}

async fn cached_cbz_pages(state: &AppState, path: &FsPath) -> Result<CachedComicPages> {
    let metadata = std::fs::metadata(path)?;
    let size = metadata.len();
    let modified = metadata.modified().ok();
    let key = path.to_string_lossy().to_string();

    {
        let cache = state.comic_page_cache.read().await;
        if let Some(entry) = cache.get(&key) {
            if entry.size == size && entry.modified == modified {
                return Ok(entry.clone());
            }
        }
    }

    let mut cache = state.comic_page_cache.write().await;
    if let Some(entry) = cache.get(&key) {
        if entry.size == size && entry.modified == modified {
            return Ok(entry.clone());
        }
    }

    let path = path.to_path_buf();
    let cached = tokio::task::spawn_blocking(move || open_cbz_pages(&path))
        .await
        .map_err(|e| AppError::Other(e.to_string()))??;
    if cache.len() >= COMIC_PAGE_CACHE_LIMIT {
        cache.clear();
    }
    cache.insert(key, cached.clone());
    Ok(cached)
}

fn cbz_named_page_bytes(archive: &Mutex<ZipArchive<File>>, name: &str) -> Result<Vec<u8>> {
    let mut archive = archive
        .lock()
        .map_err(|_| AppError::Other("comic archive cache lock poisoned".to_string()))?;
    let mut entry = archive
        .by_name(name)
        .map_err(|e| AppError::Other(e.to_string()))?;
    let mut buf = Vec::new();
    entry.read_to_end(&mut buf)?;
    Ok(buf)
}

pub async fn stream_comic_page(
    State(state): State<Arc<AppState>>,
    Path((work_id, page)): Path<(i64, usize)>,
) -> Result<Response> {
    let detail = state.db.work_detail(work_id).await?;
    let archive = detail
        .assets
        .iter()
        .find(|asset| asset.role == "archive")
        .ok_or_else(|| AppError::NotFound("comic archive asset not found".to_string()))?;
    let path = vfs::asset_local_processing_path(&state, archive).await?;
    let cached = cached_cbz_pages(&state, &path).await?;
    let name = cached
        .pages
        .get(page)
        .ok_or_else(|| AppError::NotFound(format!("page {page} not found")))?
        .name
        .clone();
    let archive = cached.archive.clone();
    let stream_name = name.clone();
    let bytes = tokio::task::spawn_blocking(move || cbz_named_page_bytes(&archive, &stream_name))
        .await
        .map_err(|e| AppError::Other(e.to_string()))??;
    let mime = path_mime(std::path::Path::new(&name));
    Ok((
        [
            (header::CONTENT_TYPE, mime),
            (header::CACHE_CONTROL, "private, max-age=86400".to_string()),
        ],
        bytes,
    )
        .into_response())
}

#[derive(Debug, Serialize)]
pub struct EpubManifestResponse {
    pub chapters: Vec<EpubChapter>,
}

#[derive(Debug, Serialize)]
pub struct EpubChapter {
    pub index: usize,
    pub title: String,
    pub href: String,
}

#[derive(Debug, Clone)]
struct EpubItem {
    href: String,
    media_type: String,
    properties: String,
}

pub async fn epub_manifest(
    State(state): State<Arc<AppState>>,
    Path(work_id): Path<i64>,
) -> Result<Json<EpubManifestResponse>> {
    let detail = state.db.work_detail(work_id).await?;
    let book = book_asset_path(&state, &detail).await?;
    let chapters = read_epub_manifest(&book)?;
    Ok(Json(EpubManifestResponse { chapters }))
}

pub async fn epub_chapter_html(
    State(state): State<Arc<AppState>>,
    Path((work_id, chapter)): Path<(i64, usize)>,
) -> Result<Response> {
    let detail = state.db.work_detail(work_id).await?;
    let book = book_asset_path(&state, &detail).await?;
    let html = read_epub_chapter_html(&book, work_id, chapter)?;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(Body::from(html))
        .map_err(|e| AppError::Other(e.to_string()))?)
}

#[derive(Debug, Deserialize)]
pub struct EpubImageQuery {
    pub path: String,
}

pub async fn stream_epub_image(
    State(state): State<Arc<AppState>>,
    Path(work_id): Path<i64>,
    Query(query): Query<EpubImageQuery>,
) -> Result<Response> {
    let detail = state.db.work_detail(work_id).await?;
    let book = book_asset_path(&state, &detail).await?;
    let image_path = normalize_epub_entry_path(&query.path)?;
    let mut archive = open_epub(&book)?;
    let bytes = read_zip_bytes(&mut archive, &image_path)?;
    let mime = path_mime(FsPath::new(&image_path));
    if !mime.starts_with("image/") {
        return Err(AppError::BadRequest(
            "EPUB entry is not an image".to_string(),
        ));
    }
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime)
        .header(header::CACHE_CONTROL, "private, max-age=86400")
        .body(Body::from(bytes))
        .map_err(|e| AppError::Other(e.to_string()))?)
}

async fn book_asset_path(state: &AppState, detail: &WorkDetail) -> Result<PathBuf> {
    let book = detail
        .assets
        .iter()
        .find(|asset| asset.role == "book" && asset.mime == "application/epub+zip")
        .ok_or_else(|| AppError::NotFound("EPUB asset not found".to_string()))?;
    vfs::asset_local_processing_path(state, book).await
}

fn read_epub_manifest(path: &FsPath) -> Result<Vec<EpubChapter>> {
    let mut archive = open_epub(path)?;
    let opf_name = epub_opf_name(&mut archive)?;
    let opf = read_zip_text(&mut archive, &opf_name)?;
    let chapter_entries = epub_chapter_entries(&mut archive, &opf_name, &opf)?;
    let mut chapters = Vec::new();
    for (href, toc_title) in chapter_entries {
        let title = toc_title
            .filter(|title| !title.trim().is_empty())
            .or_else(|| {
                read_zip_text(&mut archive, &href)
                    .ok()
                    .and_then(|html| chapter_title(&html))
            })
            .unwrap_or_else(|| short_zip_name(&href));
        chapters.push(EpubChapter {
            index: chapters.len(),
            title,
            href,
        });
    }
    Ok(chapters)
}

fn read_epub_chapter_html(path: &FsPath, work_id: i64, chapter: usize) -> Result<String> {
    let mut archive = open_epub(path)?;
    let chapters = {
        let opf_name = epub_opf_name(&mut archive)?;
        let opf = read_zip_text(&mut archive, &opf_name)?;
        epub_chapter_entries(&mut archive, &opf_name, &opf)?
            .into_iter()
            .map(|(href, _)| href)
            .collect::<Vec<_>>()
    };
    let chapter_path = chapters
        .get(chapter)
        .ok_or_else(|| AppError::NotFound(format!("EPUB chapter {chapter} not found")))?
        .to_string();
    let raw = read_zip_text(&mut archive, &chapter_path)?;
    let title = chapter_title(&raw).unwrap_or_else(|| short_zip_name(&chapter_path));
    let body = sanitize_epub_html(work_id, &chapter_path, &raw);
    Ok(format!(
        r#"<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>{}</title>
<style>
:root {{ color-scheme: dark; }}
body {{
  margin: 0;
  background: #f7f2e8;
  color: #24211c;
  font-family: "Noto Serif SC", "Songti SC", "Microsoft YaHei", serif;
  line-height: 1.82;
}}
main {{
  max-width: 900px;
  margin: 0 auto;
  padding: clamp(22px, 5vw, 56px);
}}
img, svg {{ max-width: 100%; height: auto; display: block; margin: 18px auto; }}
p {{ margin: 0 0 1em; }}
h1, h2, h3 {{ line-height: 1.35; }}
a {{ color: #8f4d34; }}
@media (prefers-color-scheme: dark) {{
  body {{ background: #151410; color: #eee7d8; }}
  a {{ color: #e0b66c; }}
}}
</style>
</head>
<body><main>{}</main></body>
</html>"#,
        html_escape::encode_text(&title),
        body
    ))
}

fn open_epub(path: &FsPath) -> Result<ZipArchive<File>> {
    let file = File::open(path)?;
    ZipArchive::new(file).map_err(|e| AppError::Other(format!("EPUB open failed: {e}")))
}

fn epub_opf_name(archive: &mut ZipArchive<File>) -> Result<String> {
    if let Ok(container) = read_zip_text(archive, "META-INF/container.xml") {
        if let Some(path) = first_attr(&container, "rootfile", "full-path") {
            return Ok(path);
        }
    }
    for i in 0..archive.len() {
        let entry = archive
            .by_index(i)
            .map_err(|e| AppError::Other(e.to_string()))?;
        if entry.name().ends_with(".opf") {
            return Ok(entry.name().to_string());
        }
    }
    Err(AppError::NotFound(
        "EPUB OPF package file not found".to_string(),
    ))
}

fn read_zip_text(archive: &mut ZipArchive<File>, name: &str) -> Result<String> {
    let mut entry = archive
        .by_name(name)
        .map_err(|e| AppError::NotFound(format!("EPUB entry {name} not found: {e}")))?;
    let mut bytes = Vec::new();
    entry.read_to_end(&mut bytes)?;
    Ok(String::from_utf8_lossy(&bytes).to_string())
}

fn read_zip_bytes(archive: &mut ZipArchive<File>, name: &str) -> Result<Vec<u8>> {
    let mut entry = archive
        .by_name(name)
        .map_err(|e| AppError::NotFound(format!("EPUB entry {name} not found: {e}")))?;
    let mut bytes = Vec::new();
    entry.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn parse_epub_manifest(opf: &str) -> BTreeMap<String, EpubItem> {
    let item_re = Regex::new(r#"(?is)<item\s+[^>]+>"#).unwrap();
    let mut items = BTreeMap::new();
    for item in item_re.find_iter(opf) {
        let tag = item.as_str();
        let Some(id) = attr_value(tag, "id") else {
            continue;
        };
        let Some(href) = attr_value(tag, "href") else {
            continue;
        };
        items.insert(
            id,
            EpubItem {
                href,
                media_type: attr_value(tag, "media-type").unwrap_or_default(),
                properties: attr_value(tag, "properties").unwrap_or_default(),
            },
        );
    }
    items
}

fn epub_chapter_entries(
    archive: &mut ZipArchive<File>,
    opf_name: &str,
    opf: &str,
) -> Result<Vec<(String, Option<String>)>> {
    let base = zip_parent(opf_name);
    let manifest = parse_epub_manifest(opf);
    let spine = parse_epub_spine(opf);
    let toc_entries = epub_toc_entries(archive, &base, &manifest);
    let spine_entries = spine
        .iter()
        .filter_map(|id| manifest.get(id))
        .filter(|item| is_epub_document(&item.media_type, &item.href))
        .map(|item| (join_zip_path(&base, &item.href), None))
        .collect::<Vec<_>>();

    let mut fallback_entries = if spine_entries.is_empty() {
        let mut entries = manifest
            .values()
            .filter(|item| is_epub_document(&item.media_type, &item.href))
            .map(|item| (join_zip_path(&base, &item.href), None))
            .collect::<Vec<_>>();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    } else {
        spine_entries
    };

    let mut entries = if toc_entries.is_empty() {
        std::mem::take(&mut fallback_entries)
    } else {
        toc_entries
    };
    entries = dedupe_epub_entries(entries);
    let filtered = entries
        .iter()
        .cloned()
        .filter(|(href, title)| readable_epub_chapter(href, title.as_deref()))
        .collect::<Vec<_>>();
    if !filtered.is_empty() {
        return Ok(filtered);
    }

    let fallback = dedupe_epub_entries(fallback_entries)
        .into_iter()
        .filter(|(href, title)| readable_epub_chapter(href, title.as_deref()))
        .collect::<Vec<_>>();
    if fallback.is_empty() {
        Ok(entries)
    } else {
        Ok(fallback)
    }
}

fn epub_toc_entries(
    archive: &mut ZipArchive<File>,
    base: &str,
    manifest: &BTreeMap<String, EpubItem>,
) -> Vec<(String, Option<String>)> {
    for item in manifest.values() {
        if item
            .properties
            .split_whitespace()
            .any(|property| property == "nav")
        {
            let path = join_zip_path(base, &item.href);
            if let Ok(html) = read_zip_text(archive, &path) {
                let entries = parse_nav_document(base, &item.href, &html);
                if !entries.is_empty() {
                    return entries;
                }
            }
        }
    }

    for item in manifest.values() {
        let lower = item.href.to_ascii_lowercase();
        if item.media_type.contains("ncx") || lower.ends_with(".ncx") {
            let path = join_zip_path(base, &item.href);
            if let Ok(ncx) = read_zip_text(archive, &path) {
                let entries = parse_ncx_document(base, &item.href, &ncx);
                if !entries.is_empty() {
                    return entries;
                }
            }
        }
    }

    Vec::new()
}

fn parse_nav_document(base: &str, nav_href: &str, html: &str) -> Vec<(String, Option<String>)> {
    let nav_base = zip_parent(&join_zip_path(base, nav_href));
    let section = Regex::new(
        r#"(?is)<nav\b[^>]*(?:epub:type|type)\s*=\s*["'][^"']*toc[^"']*["'][^>]*>(.*?)</nav>"#,
    )
    .ok()
    .and_then(|re| {
        re.captures(html)
            .and_then(|caps| caps.get(1))
            .map(|m| m.as_str().to_string())
    })
    .unwrap_or_else(|| html.to_string());
    let link_re =
        Regex::new(r#"(?is)<a\b[^>]*\shref\s*=\s*("([^"]*)"|'([^']*)')[^>]*>(.*?)</a>"#).unwrap();
    link_re
        .captures_iter(&section)
        .filter_map(|caps| {
            let href = caps.get(2).or_else(|| caps.get(3))?.as_str();
            let title = caps.get(4).map(|m| strip_html(m.as_str()));
            Some((join_zip_path(&nav_base, href), title))
        })
        .collect()
}

fn parse_ncx_document(base: &str, ncx_href: &str, ncx: &str) -> Vec<(String, Option<String>)> {
    let ncx_base = zip_parent(&join_zip_path(base, ncx_href));
    let point_re = Regex::new(r#"(?is)<navPoint\b[^>]*>(.*?)</navPoint>"#).unwrap();
    point_re
        .captures_iter(ncx)
        .filter_map(|caps| {
            let block = caps.get(1)?.as_str();
            let title = Regex::new(r#"(?is)<text[^>]*>(.*?)</text>"#)
                .ok()
                .and_then(|re| re.captures(block))
                .and_then(|caps| caps.get(1))
                .map(|m| strip_html(m.as_str()));
            let href = first_attr(block, "content", "src")?;
            Some((join_zip_path(&ncx_base, &href), title))
        })
        .collect()
}

fn dedupe_epub_entries(entries: Vec<(String, Option<String>)>) -> Vec<(String, Option<String>)> {
    let mut seen = std::collections::BTreeSet::new();
    entries
        .into_iter()
        .filter(|(href, _)| seen.insert(href.clone()))
        .collect()
}

fn readable_epub_chapter(href: &str, title: Option<&str>) -> bool {
    let haystack = format!(
        "{} {}",
        href.to_ascii_lowercase(),
        title.unwrap_or_default().to_ascii_lowercase()
    );
    ![
        "cover",
        "title",
        "toc",
        "nav.",
        "copyright",
        "colophon",
        "封面",
        "标题",
        "制作信息",
        "简介",
        "彩页",
        "目录",
        "书名页",
        "版权",
    ]
    .iter()
    .any(|needle| haystack.contains(needle))
}

fn parse_epub_spine(opf: &str) -> Vec<String> {
    let item_ref_re = Regex::new(r#"(?is)<itemref\s+[^>]+>"#).unwrap();
    item_ref_re
        .find_iter(opf)
        .filter_map(|item| attr_value(item.as_str(), "idref"))
        .collect()
}

fn first_attr(xml: &str, tag_name: &str, attr_name: &str) -> Option<String> {
    let tag_re = Regex::new(&format!(r#"(?is)<{}\s+[^>]+>"#, regex::escape(tag_name))).ok()?;
    tag_re
        .find(xml)
        .and_then(|tag| attr_value(tag.as_str(), attr_name))
}

fn attr_value(tag: &str, attr_name: &str) -> Option<String> {
    let attr_re =
        Regex::new(r#"(?is)([A-Za-z_:][-A-Za-z0-9_:.]*)\s*=\s*("([^"]*)"|'([^']*)')"#).ok()?;
    let result = attr_re.captures_iter(tag).find_map(|caps| {
        let name = caps.get(1)?.as_str();
        if name.eq_ignore_ascii_case(attr_name) {
            caps.get(3)
                .or_else(|| caps.get(4))
                .map(|value| html_escape::decode_html_entities(value.as_str()).to_string())
        } else {
            None
        }
    });
    result
}

fn is_epub_document(media_type: &str, href: &str) -> bool {
    let lower = href.to_ascii_lowercase();
    media_type.contains("html")
        || lower.ends_with(".xhtml")
        || lower.ends_with(".html")
        || lower.ends_with(".htm")
}

fn chapter_title(html: &str) -> Option<String> {
    for tag in ["title", "h1", "h2"] {
        let re = Regex::new(&format!(r#"(?is)<{tag}[^>]*>(.*?)</{tag}>"#)).ok()?;
        if let Some(title) = re
            .captures(html)
            .and_then(|caps| caps.get(1))
            .map(|m| strip_html(m.as_str()))
            .filter(|title| !title.is_empty())
        {
            return Some(title);
        }
    }
    None
}

fn sanitize_epub_html(work_id: i64, chapter_path: &str, raw: &str) -> String {
    let mut html = extract_body(raw).unwrap_or_else(|| raw.to_string());
    for pattern in [
        r#"(?is)<script\b[^>]*>.*?</script>"#,
        r#"(?is)<style\b[^>]*>.*?</style>"#,
        r#"(?is)<link\b[^>]*>"#,
        r#"(?is)<iframe\b[^>]*>.*?</iframe>"#,
        r#"(?is)<object\b[^>]*>.*?</object>"#,
        r#"(?is)<embed\b[^>]*>"#,
        r#"(?is)\s+on[a-z]+\s*=\s*("[^"]*"|'[^']*')"#,
        r#"(?is)(href|src)\s*=\s*("[ ]*javascript:[^"]*"|'[ ]*javascript:[^']*')"#,
    ] {
        html = Regex::new(pattern)
            .unwrap()
            .replace_all(&html, "")
            .to_string();
    }
    rewrite_epub_images(work_id, chapter_path, &html)
}

fn extract_body(raw: &str) -> Option<String> {
    let re = Regex::new(r#"(?is)<body[^>]*>(.*?)</body>"#).ok()?;
    re.captures(raw)
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str().to_string())
}

fn rewrite_epub_images(work_id: i64, chapter_path: &str, html: &str) -> String {
    let media_tag_re = Regex::new(r#"(?is)<(?:img|image|source)\b[^>]*>"#).unwrap();
    let mut output = String::with_capacity(html.len());
    let mut last = 0;
    for tag in media_tag_re.find_iter(html) {
        output.push_str(&html[last..tag.start()]);
        output.push_str(&rewrite_epub_media_tag(work_id, chapter_path, tag.as_str()));
        last = tag.end();
    }
    output.push_str(&html[last..]);
    output
}

fn rewrite_epub_media_tag(work_id: i64, chapter_path: &str, tag: &str) -> String {
    let attr_re =
        Regex::new(r#"(?is)(\s(?:src|href|xlink:href|poster)\s*=\s*)("([^"]*)"|'([^']*)')"#)
            .unwrap();
    let rewritten = attr_re
        .replace_all(tag, |caps: &regex::Captures<'_>| {
            let prefix = caps.get(1).map(|m| m.as_str()).unwrap_or_default();
            let quote = if caps.get(3).is_some() { "\"" } else { "'" };
            let value = caps
                .get(3)
                .or_else(|| caps.get(4))
                .map(|m| m.as_str())
                .unwrap_or_default();
            if let Some(url) = epub_image_url(work_id, chapter_path, value) {
                format!("{prefix}{quote}{url}{quote}")
            } else {
                caps.get(0)
                    .map(|m| m.as_str())
                    .unwrap_or_default()
                    .to_string()
            }
        })
        .to_string();
    let srcset_re = Regex::new(r#"(?is)(\ssrcset\s*=\s*)("([^"]*)"|'([^']*)')"#).unwrap();
    srcset_re
        .replace_all(&rewritten, |caps: &regex::Captures<'_>| {
            let prefix = caps.get(1).map(|m| m.as_str()).unwrap_or_default();
            let quote = if caps.get(3).is_some() { "\"" } else { "'" };
            let value = caps
                .get(3)
                .or_else(|| caps.get(4))
                .map(|m| m.as_str())
                .unwrap_or_default();
            format!(
                "{prefix}{quote}{}{quote}",
                rewrite_epub_srcset(work_id, chapter_path, value)
            )
        })
        .to_string()
}

fn rewrite_epub_srcset(work_id: i64, chapter_path: &str, value: &str) -> String {
    value
        .split(',')
        .map(|candidate| {
            let trimmed = candidate.trim();
            if trimmed.is_empty() {
                return String::new();
            }
            let mut parts = trimmed.split_whitespace();
            let Some(src) = parts.next() else {
                return trimmed.to_string();
            };
            let rest = parts.collect::<Vec<_>>().join(" ");
            let next_src =
                epub_image_url(work_id, chapter_path, src).unwrap_or_else(|| src.to_string());
            if rest.is_empty() {
                next_src
            } else {
                format!("{next_src} {rest}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn epub_image_url(work_id: i64, chapter_path: &str, src: &str) -> Option<String> {
    if src.starts_with("http://")
        || src.starts_with("https://")
        || src.starts_with("data:")
        || src.starts_with('#')
    {
        return None;
    }
    let image_path = join_zip_path(&zip_parent(chapter_path), src);
    if image_path.is_empty() {
        return None;
    }
    let encoded = url::form_urlencoded::byte_serialize(image_path.as_bytes()).collect::<String>();
    Some(format!("/api/works/{work_id}/epub/image?path={encoded}"))
}

fn normalize_epub_entry_path(path: &str) -> Result<String> {
    if path.starts_with("http://")
        || path.starts_with("https://")
        || path.starts_with("data:")
        || path.starts_with('#')
    {
        return Err(AppError::BadRequest("invalid EPUB image path".to_string()));
    }
    let normalized = join_zip_path("", path);
    if normalized.is_empty() {
        return Err(AppError::BadRequest("empty EPUB image path".to_string()));
    }
    Ok(normalized)
}

fn join_zip_path(base: &str, href: &str) -> String {
    let href = decode_percent_escapes(
        href.split(['?', '#'])
            .next()
            .unwrap_or(href)
            .replace('\\', "/")
            .trim_start_matches('/'),
    );
    let combined = if base.is_empty() {
        href
    } else {
        format!("{base}/{href}")
    };
    let mut parts = Vec::new();
    for part in combined.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            other => parts.push(other),
        }
    }
    parts.join("/")
}

fn decode_percent_escapes(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
            {
                output.push((hi << 4) | lo);
                index += 3;
                continue;
            }
        }
        output.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&output).to_string()
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn zip_parent(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(parent, _)| parent.to_string())
        .unwrap_or_default()
}

fn short_zip_name(path: &str) -> String {
    let name = path.rsplit('/').next().unwrap_or(path);
    name.rsplit_once('.')
        .map(|(stem, _)| stem)
        .unwrap_or(name)
        .to_string()
}

fn strip_html(value: &str) -> String {
    let tag_re = Regex::new(r"<[^>]+>").unwrap();
    html_escape::decode_html_entities(tag_re.replace_all(value, "").trim()).to_string()
}

#[derive(Debug, Deserialize)]
pub struct GenerateAssetRequest {
    pub prompt: String,
    pub style: Option<String>,
    pub allow_cover_style: Option<bool>,
    pub sanitized_asset_id: Option<i64>,
}

pub async fn generate_asset_job(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(input): Json<GenerateAssetRequest>,
) -> Result<Json<serde_json::Value>> {
    auth::require_csrf(&state, &headers, "assets.generate").await?;
    if input.prompt.trim().is_empty() {
        return Err(AppError::BadRequest("prompt is required".to_string()));
    }
    if input.sanitized_asset_id.is_some() && input.allow_cover_style != Some(true) {
        return Err(AppError::BadRequest(
            "cover stylization requires explicit allow_cover_style=true and sanitized input"
                .to_string(),
        ));
    }
    let id = state
        .db
        .create_job(
            "generate-image-asset",
            "queued",
            json!({
                "prompt": input.prompt,
                "style": input.style,
                "model": state.config.openai_image_model,
                "sanitized_asset_id": input.sanitized_asset_id
            }),
        )
        .await?;
    state
        .db
        .audit(
            "assets.generate",
            "queued",
            json!({ "job_id": id, "model": state.config.openai_image_model }),
        )
        .await?;
    Ok(Json(json!({ "job_id": id, "status": "queued" })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use crate::config::Config;
    use crate::db::Db;

    const PNG_1X1: &[u8] = &[
        137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1, 8, 6,
        0, 0, 0, 31, 21, 196, 137, 0, 0, 0, 10, 73, 68, 65, 84, 120, 156, 99, 0, 1, 0, 0, 5, 0, 1,
        13, 10, 45, 180, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130,
    ];

    #[tokio::test]
    async fn work_cover_caches_coser_archive_cover_in_kind_directory() {
        let temp = tempfile::tempdir().unwrap();
        let data_dir = temp.path().join("data");
        let generated_dir = temp.path().join("generated");
        let cover_cache_dir = temp.path().join("cover-cache");
        let coser_cover_cache_dir = cover_cache_dir.join("coser-picture");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&generated_dir).unwrap();

        let archive_path = temp.path().join("COS图").join("CoserA").join("set.zip");
        std::fs::create_dir_all(archive_path.parent().unwrap()).unwrap();
        write_test_zip(&archive_path);

        let database_url = format!(
            "sqlite://{}",
            test_path_string(&data_dir.join("library.sqlite"))
        );
        let db = Db::connect(&database_url).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "coser-picture",
                "set",
                Some(&test_path_string(&archive_path)),
                Some("CoserPicture"),
                None,
                None,
                json!({ "page_count": 1 }),
            )
            .await
            .unwrap();
        db.upsert_asset(
            work_id,
            &test_path_string(&archive_path),
            "application/zip",
            "archive",
            Some("zip"),
            None,
            std::fs::metadata(&archive_path)
                .ok()
                .map(|m| m.len() as i64),
            json!({ "page_count": 1 }),
        )
        .await
        .unwrap();

        let state = Arc::new(AppState {
            config: Config {
                bind: "127.0.0.1:0".to_string(),
                database_url,
                data_dir,
                cover_cache_dir: cover_cache_dir.clone(),
                comic_cover_cache_dir: cover_cache_dir.join("comic"),
                novel_cover_cache_dir: cover_cache_dir.join("novel"),
                audio_cover_cache_dir: cover_cache_dir.join("audio"),
                gallery_cover_cache_dir: cover_cache_dir.join("gallery"),
                coser_picture_cover_cache_dir: coser_cover_cache_dir.clone(),
                comics_dir: temp.path().join("漫画"),
                novels_dir: temp.path().join("轻小说"),
                audio_dir: temp.path().join("音声"),
                gallery_dir: temp.path().join("图库"),
                coser_picture_dir: temp.path().join("COS图"),
                generated_dir,
                app_admin_password: "admin".to_string(),
                lightnovel_api_bases: Vec::new(),
                lightnovel_access_token: None,
                enrichment_concurrency: 1,
                ehtt_url: String::new(),
                openai_api_key: None,
                openai_image_model: "gpt-image-2".to_string(),
                qmediasync_base_url: String::new(),
                session_secret: "test-secret".to_string(),
                enable_file_watcher: false,
                watch_debounce_seconds: 20,
            },
            db,
            http: reqwest::Client::new(),
            comic_page_cache: Arc::new(ComicPageCache::default()),
        });

        let response = work_cover(
            State(state.clone()),
            Path(work_id),
            Query(CoverQuery { size: Some(128) }),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "image/jpeg"
        );
        let first_files = cache_files(&coser_cover_cache_dir);
        assert_eq!(first_files.len(), 1);

        let second = work_cover(
            State(state),
            Path(work_id),
            Query(CoverQuery { size: Some(128) }),
        )
        .await
        .unwrap();
        assert_eq!(second.status(), StatusCode::OK);
        assert_eq!(cache_files(&coser_cover_cache_dir), first_files);
        assert!(cache_files(&cover_cache_dir.join("comic")).is_empty());
    }

    fn write_test_zip(path: &FsPath) {
        let file = File::create(path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();
        zip.start_file("1.png", options).unwrap();
        zip.write_all(PNG_1X1).unwrap();
        zip.finish().unwrap();
    }

    fn cache_files(path: &FsPath) -> Vec<String> {
        if !path.exists() {
            return Vec::new();
        }
        let mut files = std::fs::read_dir(path)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        files.sort();
        files
    }

    fn test_path_string(path: &FsPath) -> String {
        path.to_string_lossy().replace('\\', "/")
    }
}
