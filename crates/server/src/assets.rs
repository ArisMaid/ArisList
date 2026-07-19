use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Cursor, Read, Write};
use std::path::{Path as FsPath, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, TryLockError, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tokio_util::io::ReaderStream;
use uuid::Uuid;
use zip::ZipArchive;

use crate::auth;
use crate::error::{AppError, Result};
use crate::models::WorkDetail;
use crate::scanner::{image_name, naturalish_key};
use crate::security::path_mime;
use crate::vfs;
use crate::AppState;

static THUMBNAIL_WORKERS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(2)));
static ARCHIVE_READ_WORKERS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(4)));
static THUMBNAIL_WRITE_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Weak<AsyncMutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static EPUB_MANIFEST_CACHE: LazyLock<AsyncMutex<EpubManifestCacheState>> =
    LazyLock::new(|| AsyncMutex::new(EpubManifestCacheState::default()));
static EPUB_MANIFEST_LOAD_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Weak<AsyncMutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static THUMBNAIL_CACHE_QUOTAS: LazyLock<Mutex<HashMap<PathBuf, ThumbnailCacheQuotaState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static EPUB_ITEM_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?is)<item\s+[^>]+>"#).unwrap());
static EPUB_NAV_SECTION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?is)<nav\b[^>]*(?:epub:type|type)\s*=\s*["'][^"']*toc[^"']*["'][^>]*>(.*?)</nav>"#,
    )
    .unwrap()
});
static EPUB_NAV_LINK_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?is)<a\b[^>]*\shref\s*=\s*("([^"]*)"|'([^']*)')[^>]*>(.*?)</a>"#).unwrap()
});
static EPUB_NAV_POINT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?is)<navPoint\b[^>]*>(.*?)</navPoint>"#).unwrap());
static EPUB_NAV_TEXT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?is)<text[^>]*>(.*?)</text>"#).unwrap());
static EPUB_ITEM_REF_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?is)<itemref\s+[^>]+>"#).unwrap());
static XML_ATTR_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?is)([A-Za-z_:][-A-Za-z0-9_:.]*)\s*=\s*("([^"]*)"|'([^']*)')"#).unwrap()
});
static EPUB_CHAPTER_TITLE_RES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    ["title", "h1", "h2"]
        .into_iter()
        .map(|tag| Regex::new(&format!(r#"(?is)<{tag}[^>]*>(.*?)</{tag}>"#)).unwrap())
        .collect()
});
static EPUB_SANITIZE_RES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        r#"(?is)<script\b[^>]*>.*?</script>"#,
        r#"(?is)<style\b[^>]*>.*?</style>"#,
        r#"(?is)<link\b[^>]*>"#,
        r#"(?is)<iframe\b[^>]*>.*?</iframe>"#,
        r#"(?is)<object\b[^>]*>.*?</object>"#,
        r#"(?is)<embed\b[^>]*>"#,
        r#"(?is)\s+on[a-z]+\s*=\s*("[^"]*"|'[^']*')"#,
        r#"(?is)(href|src)\s*=\s*("[ ]*javascript:[^"]*"|'[ ]*javascript:[^']*')"#,
    ]
    .into_iter()
    .map(|pattern| Regex::new(pattern).unwrap())
    .collect()
});
static EPUB_BODY_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?is)<body[^>]*>(.*?)</body>"#).unwrap());
static EPUB_MEDIA_TAG_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?is)<(?:img|image|source)\b[^>]*>"#).unwrap());
static EPUB_MEDIA_ATTR_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?is)(\s(?:src|href|xlink:href|poster)\s*=\s*)("([^"]*)"|'([^']*)')"#).unwrap()
});
static EPUB_SRCSET_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?is)(\ssrcset\s*=\s*)("([^"]*)"|'([^']*)')"#).unwrap());
static HTML_TAG_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"<[^>]+>").unwrap());
const STREAM_BUFFER_SIZE: usize = 128 * 1024;
const THUMBNAIL_SIZE_BUCKETS: [u32; 5] = [128, 256, 360, 480, 960];
const COMIC_PAGE_CACHE_LIMIT: usize = 8;
const COMIC_ARCHIVE_POOL_SIZE: usize = 2;
const EPUB_MANIFEST_CACHE_LIMIT: usize = 8;
const MAX_COMIC_PAGE_BYTES: u64 = 128 * 1024 * 1024;
const MAX_EPUB_TEXT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_EPUB_IMAGE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_EPUB_TITLE_PROBE_BYTES: u64 = 256 * 1024;
const MAX_EPUB_TITLE_PROBE_TOTAL_BYTES: u64 = 16 * 1024 * 1024;
const MAX_EPUB_CHAPTERS: usize = 10_000;
const MAX_IMAGE_DECODE_ALLOC_BYTES: u64 = 256 * 1024 * 1024;
const MAX_THUMBNAIL_CACHE_FILE_BYTES: u64 = 8 * 1024 * 1024;
const CACHE_USAGE_RESCAN_INTERVAL: Duration = Duration::from_secs(60);
const REMOTE_DERIVED_CACHE_MAX_AGE: Duration = Duration::from_secs(15 * 60);
const MEDIA_NO_CACHE: &str = "private, no-cache";
const MEDIA_IMMUTABLE_CACHE: &str = "private, max-age=31536000, immutable";

#[derive(Default)]
struct ThumbnailCacheQuotaState {
    initialized: bool,
    committed: u64,
    reserved: u64,
    generation: u64,
    observed_dir_modified: Option<SystemTime>,
    last_scan: Option<Instant>,
}

struct ThumbnailCacheReservation {
    cache_dir: PathBuf,
    reserved: u64,
    active: bool,
}

impl ThumbnailCacheReservation {
    fn limit(&self) -> u64 {
        self.reserved
    }

    fn commit(mut self, actual: u64, observed_dir_modified: Option<SystemTime>) {
        let mut quotas = THUMBNAIL_CACHE_QUOTAS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(state) = quotas.get_mut(&self.cache_dir) {
            state.reserved = state.reserved.saturating_sub(self.reserved);
            state.committed = state.committed.saturating_add(actual);
            state.generation = state.generation.wrapping_add(1);
            state.observed_dir_modified = observed_dir_modified;
        }
        self.active = false;
    }
}

impl Drop for ThumbnailCacheReservation {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut quotas = THUMBNAIL_CACHE_QUOTAS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(state) = quotas.get_mut(&self.cache_dir) {
            state.reserved = state.reserved.saturating_sub(self.reserved);
            state.generation = state.generation.wrapping_add(1);
        }
    }
}

async fn archive_blocking<T, F>(task: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    let permit = ARCHIVE_READ_WORKERS
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| AppError::Other("archive reader is closed".to_string()))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        task()
    })
    .await
    .map_err(|err| AppError::Other(format!("archive reader task failed: {err}")))?
}

async fn run_blocking_thumbnail_generation<F>(
    write_guard: tokio::sync::OwnedMutexGuard<()>,
    reservation: ThumbnailCacheReservation,
    cache_path: PathBuf,
    generate: F,
) -> std::result::Result<(), String>
where
    F: FnOnce(&FsPath) -> std::result::Result<(), String> + Send + 'static,
{
    tokio::spawn(async move {
        let _write_guard = write_guard;
        let permit = THUMBNAIL_WORKERS
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| "thumbnail worker is closed".to_string())?;
        let generation_path = cache_path.clone();
        let generated = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            generate(&generation_path)
        })
        .await
        .map_err(|err| format!("thumbnail worker task failed: {err}"))?;
        finalize_thumbnail_generation(reservation, &cache_path, generated).await
    })
    .await
    .map_err(|err| format!("thumbnail generation task failed: {err}"))?
}

async fn run_qms_thumbnail_generation(
    state: Arc<AppState>,
    asset: crate::models::Asset,
    write_guard: tokio::sync::OwnedMutexGuard<()>,
    reservation: ThumbnailCacheReservation,
    cache_path: PathBuf,
    size: u32,
) -> std::result::Result<(), String> {
    tokio::spawn(async move {
        let _write_guard = write_guard;
        let permit = THUMBNAIL_WORKERS
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| "thumbnail worker is closed".to_string())?;
        let generated = vfs::generate_qms_thumbnail(&state, &asset, &cache_path, size)
            .await
            .map_err(|err| err.to_string());
        drop(permit);
        finalize_thumbnail_generation(reservation, &cache_path, generated).await
    })
    .await
    .map_err(|err| format!("thumbnail generation task failed: {err}"))?
}

#[derive(Default)]
pub struct ComicPageCache {
    state: AsyncMutex<ComicPageCacheState>,
    load_locks: Mutex<HashMap<String, Weak<AsyncMutex<()>>>>,
}

#[derive(Default)]
struct ComicPageCacheState {
    entries: HashMap<String, CachedComicPages>,
    lru: VecDeque<String>,
}

#[derive(Default)]
struct EpubManifestCacheState {
    entries: HashMap<PathBuf, CachedEpubManifest>,
    lru: VecDeque<PathBuf>,
}

#[derive(Clone)]
struct CachedEpubManifest {
    size: u64,
    modified: Option<SystemTime>,
    chapters: Arc<Vec<EpubChapter>>,
    archive: Arc<Mutex<ZipArchive<File>>>,
}

#[derive(Clone)]
pub struct CachedComicPages {
    size: u64,
    modified: Option<SystemTime>,
    pages: Arc<Vec<ComicPageInfo>>,
    archive: Arc<ComicArchivePool>,
}

struct ComicArchivePool {
    archives: Vec<Mutex<ZipArchive<File>>>,
    next: AtomicUsize,
}

pub async fn stream_asset(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Query(query): Query<VersionQuery>,
    headers: HeaderMap,
) -> Result<Response> {
    let asset = state.db.asset(id).await?;
    if vfs::is_qms_strm_uri(&asset.path) {
        return vfs::stream_qms_asset(state, asset, headers).await;
    }
    let path = vfs::local_asset_path(&state, &asset.path).await?;
    let mut file = tokio::fs::File::open(&path).await?;
    let size = file.metadata().await?.len();
    let cache_control = media_cache_control(query.v.as_deref());

    match parse_byte_range(&headers, size) {
        ByteRange::Range { start, end } => {
            let length = end - start + 1;
            file.seek(SeekFrom::Start(start)).await?;
            let stream = ReaderStream::with_capacity(file.take(length), STREAM_BUFFER_SIZE);
            let body = Body::from_stream(stream);
            return Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(header::CONTENT_TYPE, asset.mime)
                .header(header::CACHE_CONTROL, cache_control)
                .header(header::ACCEPT_RANGES, "bytes")
                .header(header::CONTENT_LENGTH, length.to_string())
                .header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{size}"))
                .body(body)
                .map_err(|e| AppError::Other(e.to_string()));
        }
        ByteRange::Unsatisfiable => {
            return Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header(header::ACCEPT_RANGES, "bytes")
                .header(header::CONTENT_RANGE, format!("bytes */{size}"))
                .header(header::CONTENT_LENGTH, "0")
                .body(Body::empty())
                .map_err(|e| AppError::Other(e.to_string()));
        }
        ByteRange::None => {}
    }

    let stream = ReaderStream::with_capacity(file, STREAM_BUFFER_SIZE);
    let body = Body::from_stream(stream);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, asset.mime)
        .header(header::CACHE_CONTROL, cache_control)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, size.to_string())
        .body(body)
        .map_err(|e| AppError::Other(e.to_string()))
}

pub async fn head_asset(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Query(query): Query<VersionQuery>,
) -> Result<Response> {
    let asset = state.db.asset(id).await?;
    if vfs::is_qms_strm_uri(&asset.path) {
        return reject_expensive_head().await;
    }
    let path = vfs::local_asset_path(&state, &asset.path).await?;
    let size = tokio::fs::metadata(path).await?.len();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, asset.mime)
        .header(
            header::CACHE_CONTROL,
            media_cache_control(query.v.as_deref()),
        )
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, size.to_string())
        .body(Body::empty())
        .map_err(|err| AppError::Other(err.to_string()))
}

pub async fn reject_expensive_head() -> Result<Response> {
    Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .header(header::ALLOW, "GET")
        .header(header::CONTENT_LENGTH, "0")
        .body(Body::empty())
        .map_err(|err| AppError::Other(err.to_string()))
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
    pub v: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CoverQuery {
    pub size: Option<u32>,
    pub v: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct VersionQuery {
    pub v: Option<String>,
}

pub async fn work_cover(
    State(state): State<Arc<AppState>>,
    Path(work_id): Path<i64>,
    Query(query): Query<CoverQuery>,
) -> Result<Response> {
    let detail = state.db.work_detail(work_id).await?;
    let size = thumbnail_size_bucket(query.size.unwrap_or(480));
    let cache_control = media_cache_control(query.v.as_deref());
    let settings = crate::settings::load_settings(&state.config).await?;
    let cache_dir = settings.cover_cache_dirs.for_work_kind(&detail.work.kind);
    tokio::fs::create_dir_all(&cache_dir).await?;

    if let Some(asset_id) = detail.work.cover_asset_id {
        let asset = state.db.asset(asset_id).await?;
        if asset.mime.starts_with("image/") {
            return cached_image_cover(
                state,
                detail.work.id,
                asset,
                cache_dir,
                size,
                cache_control,
            )
            .await;
        }
    }

    if matches!(detail.work.kind.as_str(), "comic" | "coser-picture") {
        let archive = detail
            .assets
            .iter()
            .find(|asset| asset.role == "archive")
            .cloned()
            .ok_or_else(|| AppError::NotFound("archive cover source not found".to_string()))?;
        if vfs::is_qms_strm_uri(&archive.path) {
            return Err(AppError::NotFound(
                "remote archive has no cached side cover".to_string(),
            ));
        }
        return cached_archive_cover(
            state,
            detail.work.id,
            archive,
            cache_dir,
            size,
            cache_control,
        )
        .await;
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
    let size = thumbnail_size_bucket(query.size.unwrap_or(360));
    let is_qms = vfs::is_qms_strm_uri(&asset.path);
    let cache_control = if is_qms {
        MEDIA_NO_CACHE
    } else {
        media_cache_control(query.v.as_deref())
    };
    let thumbs_dir = state.config.data_dir.join("thumbs");
    tokio::fs::create_dir_all(&thumbs_dir).await?;
    let (cache_path, source_path) = if is_qms {
        let version = vfs::qms_asset_version_key(&state, &asset).await?;
        (
            thumbs_dir.join(format!("{}-{}-{version}.jpg", asset.id, size)),
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

    if valid_thumbnail_cache_for_source(&cache_path, is_qms).await? {
        return stream_thumb_cache(cache_path, cache_control).await;
    }
    let write_lock = thumbnail_write_lock(&cache_path)?;
    let write_guard = write_lock.lock_owned().await;
    if valid_thumbnail_cache_for_source(&cache_path, is_qms).await? {
        return stream_thumb_cache(cache_path, cache_control).await;
    }
    let generated = match reserve_thumbnail_cache_capacity(
        &cache_path,
        state.config.thumbnail_cache_max_bytes_per_dir,
    )
    .await
    {
        Ok(reservation) => {
            if let Some(source_path) = source_path.clone() {
                run_blocking_thumbnail_generation(
                    write_guard,
                    reservation,
                    cache_path.clone(),
                    move |cache_path| generate_thumbnail_atomic(&source_path, cache_path, size),
                )
                .await
            } else {
                run_qms_thumbnail_generation(
                    state.clone(),
                    asset.clone(),
                    write_guard,
                    reservation,
                    cache_path.clone(),
                    size,
                )
                .await
            }
        }
        Err(err) => Err(err.to_string()),
    };

    match generated {
        Ok(()) => stream_thumb_cache(cache_path, cache_control).await,
        Err(err) => {
            tracing::warn!(asset_id = asset.id, error = %err, "thumbnail generation failed; streaming original image");
            if let Some(source_path) = source_path {
                stream_original_image(source_path, asset.mime, cache_control).await
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
    cache_control: &'static str,
) -> Result<Response> {
    let is_qms = vfs::is_qms_strm_uri(&asset.path);
    let cache_control = if is_qms {
        MEDIA_NO_CACHE
    } else {
        cache_control
    };
    let (cache_path, source_path) = if is_qms {
        let version = vfs::qms_asset_version_key(&state, &asset).await?;
        (
            cache_dir.join(format!(
                "work-{work_id}-asset-{}-{size}-{version}.jpg",
                asset.id
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

    if valid_thumbnail_cache_for_source(&cache_path, is_qms).await? {
        return stream_thumb_cache(cache_path, cache_control).await;
    }
    let write_lock = thumbnail_write_lock(&cache_path)?;
    let write_guard = write_lock.lock_owned().await;
    if valid_thumbnail_cache_for_source(&cache_path, is_qms).await? {
        return stream_thumb_cache(cache_path, cache_control).await;
    }
    let generated = match reserve_thumbnail_cache_capacity(
        &cache_path,
        state.config.thumbnail_cache_max_bytes_per_dir,
    )
    .await
    {
        Ok(reservation) => {
            if let Some(source_path) = source_path.clone() {
                run_blocking_thumbnail_generation(
                    write_guard,
                    reservation,
                    cache_path.clone(),
                    move |cache_path| generate_thumbnail_atomic(&source_path, cache_path, size),
                )
                .await
            } else {
                run_qms_thumbnail_generation(
                    state.clone(),
                    asset.clone(),
                    write_guard,
                    reservation,
                    cache_path.clone(),
                    size,
                )
                .await
            }
        }
        Err(err) => Err(err.to_string()),
    };

    match generated {
        Ok(()) => stream_thumb_cache(cache_path, cache_control).await,
        Err(err) => {
            tracing::warn!(asset_id = asset.id, error = %err, "cover thumbnail generation failed; streaming original image");
            if let Some(source_path) = source_path {
                stream_original_image(source_path, asset.mime, cache_control).await
            } else {
                vfs::stream_qms_asset(state, asset, HeaderMap::new()).await
            }
        }
    }
}

async fn cached_archive_cover(
    state: Arc<AppState>,
    work_id: i64,
    archive: crate::models::Asset,
    cache_dir: PathBuf,
    size: u32,
    cache_control: &'static str,
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
    if valid_thumbnail_cache(&cache_path).await? {
        return stream_thumb_cache(cache_path, cache_control).await;
    }
    let write_lock = thumbnail_write_lock(&cache_path)?;
    let write_guard = write_lock.lock_owned().await;
    if valid_thumbnail_cache(&cache_path).await? {
        return stream_thumb_cache(cache_path, cache_control).await;
    }

    let generated = match reserve_thumbnail_cache_capacity(
        &cache_path,
        state.config.thumbnail_cache_max_bytes_per_dir,
    )
    .await
    {
        Ok(reservation) => {
            let cached_archive = cached.archive.clone();
            let stream_name = page_name.clone();
            run_blocking_thumbnail_generation(
                write_guard,
                reservation,
                cache_path.clone(),
                move |cache_path| {
                    let bytes = cbz_named_page_bytes(&cached_archive, &stream_name)
                        .map_err(|e| e.to_string())?;
                    generate_thumbnail_from_bytes_atomic(&bytes, cache_path, size)
                },
            )
            .await
        }
        Err(err) => Err(err.to_string()),
    };
    match generated {
        Ok(()) => stream_thumb_cache(cache_path, cache_control).await,
        Err(err) => {
            tracing::warn!(asset_id = archive.id, error = %err, "archive cover thumbnail generation failed; streaming first page");
            let cached_archive = cached.archive.clone();
            let stream_name = page_name.clone();
            let bytes =
                archive_blocking(move || cbz_named_page_bytes(&cached_archive, &stream_name))
                    .await?;
            let mime = path_mime(FsPath::new(&page_name));
            Ok((
                [
                    (header::CONTENT_TYPE, mime),
                    (header::CACHE_CONTROL, cache_control.to_string()),
                ],
                bytes,
            )
                .into_response())
        }
    }
}

fn system_time_key(time: SystemTime) -> Option<u128> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|value| value.as_nanos())
}

pub(crate) fn media_cache_control(version: Option<&str>) -> &'static str {
    if version.is_some_and(|value| !value.trim().is_empty()) {
        MEDIA_IMMUTABLE_CACHE
    } else {
        MEDIA_NO_CACHE
    }
}

fn thumbnail_size_bucket(requested: u32) -> u32 {
    let requested = requested.clamp(96, 960);
    THUMBNAIL_SIZE_BUCKETS
        .into_iter()
        .find(|size| *size >= requested)
        .unwrap_or(960)
}

fn generate_thumbnail_atomic(
    source_path: &FsPath,
    cache_path: &FsPath,
    size: u32,
) -> std::result::Result<(), String> {
    let mut reader = image::ImageReader::open(source_path)
        .map_err(|e| e.to_string())?
        .with_guessed_format()
        .map_err(|e| e.to_string())?;
    reader.limits(image_decode_limits());
    let image = reader.decode().map_err(|e| e.to_string())?;
    publish_thumbnail(image, cache_path, size)
}

fn generate_thumbnail_from_bytes_atomic(
    bytes: &[u8],
    cache_path: &FsPath,
    size: u32,
) -> std::result::Result<(), String> {
    let mut reader = image::ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|e| e.to_string())?;
    reader.limits(image_decode_limits());
    let image = reader.decode().map_err(|e| e.to_string())?;
    publish_thumbnail(image, cache_path, size)
}

fn image_decode_limits() -> image::Limits {
    let mut limits = image::Limits::default();
    limits.max_alloc = Some(MAX_IMAGE_DECODE_ALLOC_BYTES);
    limits
}

fn publish_thumbnail(
    image: image::DynamicImage,
    cache_path: &FsPath,
    size: u32,
) -> std::result::Result<(), String> {
    let thumb = image.thumbnail(size, size).to_rgb8();
    let temp_path = thumbnail_temp_path(cache_path)?;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .map_err(|e| e.to_string())?;
    let mut writer = BufWriter::new(file);
    let published = (|| -> std::result::Result<(), String> {
        {
            let mut encoder = JpegEncoder::new_with_quality(&mut writer, 84);
            encoder.encode_image(&thumb).map_err(|e| e.to_string())?;
        }
        writer.flush().map_err(|e| e.to_string())?;
        writer.get_ref().sync_all().map_err(|e| e.to_string())?;
        drop(writer);
        image::ImageReader::open(&temp_path)
            .map_err(|e| e.to_string())?
            .with_guessed_format()
            .map_err(|e| e.to_string())?
            .into_dimensions()
            .map_err(|e| e.to_string())?;
        if cache_path.exists() {
            std::fs::remove_file(cache_path).map_err(|e| e.to_string())?;
        }
        std::fs::rename(&temp_path, cache_path).map_err(|e| e.to_string())
    })();
    if published.is_err() {
        if let Err(err) = std::fs::remove_file(&temp_path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(path = %temp_path.display(), error = %err, "failed to remove thumbnail temp file");
            }
        }
    }
    published
}

fn thumbnail_write_lock(path: &FsPath) -> Result<Arc<AsyncMutex<()>>> {
    let mut locks = THUMBNAIL_WRITE_LOCKS
        .lock()
        .map_err(|_| AppError::Other("thumbnail write lock registry poisoned".to_string()))?;
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
        return Ok(lock);
    }
    let lock = Arc::new(AsyncMutex::new(()));
    locks.insert(path.to_path_buf(), Arc::downgrade(&lock));
    Ok(lock)
}

fn thumbnail_temp_path(cache_path: &FsPath) -> std::result::Result<PathBuf, String> {
    let file_name = cache_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| "thumbnail cache path has no valid file name".to_string())?;
    Ok(cache_path.with_file_name(format!(".{file_name}.{}.part", Uuid::new_v4())))
}

async fn valid_thumbnail_cache(path: &FsPath) -> Result<bool> {
    let mut file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err.into()),
    };
    let metadata = file.metadata().await?;
    if !metadata.is_file() || metadata.len() < 4 {
        return Ok(false);
    }
    let mut start = [0_u8; 2];
    file.read_exact(&mut start).await?;
    file.seek(SeekFrom::End(-2)).await?;
    let mut end = [0_u8; 2];
    file.read_exact(&mut end).await?;
    Ok(start == [0xff, 0xd8] && end == [0xff, 0xd9])
}

async fn valid_thumbnail_cache_for_source(path: &FsPath, remote: bool) -> Result<bool> {
    if !valid_thumbnail_cache(path).await? {
        return Ok(false);
    }
    if !remote {
        return Ok(true);
    }
    let modified = tokio::fs::metadata(path).await?.modified().ok();
    Ok(modified.is_some_and(|modified| {
        SystemTime::now()
            .duration_since(modified)
            .unwrap_or_default()
            < REMOTE_DERIVED_CACHE_MAX_AGE
    }))
}

async fn reserve_thumbnail_cache_capacity(
    cache_path: &FsPath,
    quota: u64,
) -> Result<ThumbnailCacheReservation> {
    if tokio::fs::try_exists(cache_path).await? {
        tokio::fs::remove_file(cache_path).await?;
    }
    let cache_dir = cache_path
        .parent()
        .ok_or_else(|| AppError::Other("thumbnail cache path has no parent".to_string()))?
        .to_path_buf();
    let dir_modified = tokio::fs::metadata(&cache_dir)
        .await
        .ok()
        .and_then(|metadata| metadata.modified().ok());
    let should_scan = {
        let mut quotas = THUMBNAIL_CACHE_QUOTAS
            .lock()
            .map_err(|_| AppError::Other("thumbnail cache quota registry poisoned".to_string()))?;
        let state = quotas.entry(cache_dir.clone()).or_default();
        !state.initialized
            || state.observed_dir_modified != dir_modified
            || state
                .last_scan
                .is_none_or(|last_scan| last_scan.elapsed() >= CACHE_USAGE_RESCAN_INTERVAL)
    };
    if should_scan {
        resync_thumbnail_cache_usage(&cache_dir, dir_modified).await?;
    }

    if let Some(reservation) = try_reserve_thumbnail_capacity(&cache_dir, quota)? {
        return Ok(reservation);
    }
    if !should_scan {
        let refreshed_modified = tokio::fs::metadata(&cache_dir)
            .await
            .ok()
            .and_then(|metadata| metadata.modified().ok());
        resync_thumbnail_cache_usage(&cache_dir, refreshed_modified).await?;
        if let Some(reservation) = try_reserve_thumbnail_capacity(&cache_dir, quota)? {
            return Ok(reservation);
        }
    }

    let quotas = THUMBNAIL_CACHE_QUOTAS
        .lock()
        .map_err(|_| AppError::Other("thumbnail cache quota registry poisoned".to_string()))?;
    let state = quotas
        .get(&cache_dir)
        .ok_or_else(|| AppError::Other("thumbnail cache quota state is missing".to_string()))?;
    let used = state.committed.saturating_add(state.reserved);
    Err(AppError::Other(format!(
        "thumbnail cache directory quota reached: {used} bytes used or reserved, {quota} allowed; remove individual cached files manually or raise THUMBNAIL_CACHE_MAX_BYTES_PER_DIR"
    )))
}

async fn resync_thumbnail_cache_usage(
    cache_dir: &FsPath,
    observed_dir_modified: Option<SystemTime>,
) -> Result<()> {
    let cache_dir = cache_dir.to_path_buf();
    let scan_generation = {
        let mut quotas = THUMBNAIL_CACHE_QUOTAS
            .lock()
            .map_err(|_| AppError::Other("thumbnail cache quota registry poisoned".to_string()))?;
        quotas.entry(cache_dir.clone()).or_default().generation
    };
    let usage_dir = cache_dir.clone();
    let current_usage = tokio::task::spawn_blocking(move || thumbnail_cache_usage(&usage_dir))
        .await
        .map_err(|err| AppError::Other(format!("thumbnail cache usage task failed: {err}")))??;
    let mut quotas = THUMBNAIL_CACHE_QUOTAS
        .lock()
        .map_err(|_| AppError::Other("thumbnail cache quota registry poisoned".to_string()))?;
    let state = quotas.entry(cache_dir).or_default();
    if !state.initialized || state.generation == scan_generation {
        state.committed = current_usage;
    } else {
        state.committed = state.committed.max(current_usage);
    }
    state.initialized = true;
    state.observed_dir_modified = observed_dir_modified;
    state.last_scan = Some(Instant::now());
    Ok(())
}

fn try_reserve_thumbnail_capacity(
    cache_dir: &FsPath,
    quota: u64,
) -> Result<Option<ThumbnailCacheReservation>> {
    let mut quotas = THUMBNAIL_CACHE_QUOTAS
        .lock()
        .map_err(|_| AppError::Other("thumbnail cache quota registry poisoned".to_string()))?;
    let state = quotas.entry(cache_dir.to_path_buf()).or_default();
    let used = state.committed.saturating_add(state.reserved);
    let requested = MAX_THUMBNAIL_CACHE_FILE_BYTES.min(quota.saturating_sub(used));
    if requested == 0 {
        return Ok(None);
    }
    state.reserved = state.reserved.saturating_add(requested);
    state.generation = state.generation.wrapping_add(1);
    Ok(Some(ThumbnailCacheReservation {
        cache_dir: cache_dir.to_path_buf(),
        reserved: requested,
        active: true,
    }))
}

async fn finalize_thumbnail_generation(
    reservation: ThumbnailCacheReservation,
    cache_path: &FsPath,
    generated: std::result::Result<(), String>,
) -> std::result::Result<(), String> {
    generated?;
    let size = tokio::fs::metadata(cache_path)
        .await
        .map_err(|err| err.to_string())?
        .len();
    if size > reservation.limit() {
        if let Err(err) = tokio::fs::remove_file(cache_path).await {
            if err.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(path = %cache_path.display(), error = %err, "failed to remove over-quota thumbnail");
            }
        }
        return Err(format!(
            "generated thumbnail requires {size} bytes but only {} were available",
            reservation.limit()
        ));
    }
    let observed_dir_modified = tokio::fs::metadata(&reservation.cache_dir)
        .await
        .ok()
        .and_then(|metadata| metadata.modified().ok());
    reservation.commit(size, observed_dir_modified);
    Ok(())
}

fn thumbnail_cache_usage(cache_dir: &FsPath) -> std::io::Result<u64> {
    let mut total = 0_u64;
    for entry in std::fs::read_dir(cache_dir)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };
        if !file_type.is_file() {
            continue;
        }
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };
        total = total.saturating_add(metadata.len());
    }
    Ok(total)
}

fn short_hash(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())[..16].to_string()
}

async fn stream_thumb_cache(path: PathBuf, cache_control: &'static str) -> Result<Response> {
    let file = tokio::fs::File::open(&path).await?;
    let size = file.metadata().await?.len();
    let stream = ReaderStream::with_capacity(file, STREAM_BUFFER_SIZE);
    let body = Body::from_stream(stream);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "image/jpeg")
        .header(header::CACHE_CONTROL, cache_control)
        .header(header::CONTENT_LENGTH, size.to_string())
        .body(body)
        .map_err(|e| AppError::Other(e.to_string()))
}

async fn stream_original_image(
    path: PathBuf,
    mime: String,
    cache_control: &'static str,
) -> Result<Response> {
    let file = tokio::fs::File::open(&path).await?;
    let size = file.metadata().await?.len();
    let stream = ReaderStream::with_capacity(file, STREAM_BUFFER_SIZE);
    let body = Body::from_stream(stream);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime)
        .header(header::CACHE_CONTROL, cache_control)
        .header(header::CONTENT_LENGTH, size.to_string())
        .body(body)
        .map_err(|e| AppError::Other(e.to_string()))
}

#[derive(Debug, PartialEq, Eq)]
enum ByteRange {
    None,
    Range { start: u64, end: u64 },
    Unsatisfiable,
}

fn parse_byte_range(headers: &HeaderMap, size: u64) -> ByteRange {
    let Some(header) = headers.get(header::RANGE) else {
        return ByteRange::None;
    };
    let Ok(header) = header.to_str() else {
        return ByteRange::Unsatisfiable;
    };
    let Some(specs) = header.strip_prefix("bytes=") else {
        return ByteRange::Unsatisfiable;
    };
    if size == 0 || specs.contains(',') {
        return ByteRange::Unsatisfiable;
    }
    let spec = specs.trim();
    let Some((start, end)) = spec.split_once('-') else {
        return ByteRange::Unsatisfiable;
    };
    if start.is_empty() {
        let Ok(suffix) = end.parse::<u64>() else {
            return ByteRange::Unsatisfiable;
        };
        let suffix = suffix.min(size);
        if suffix == 0 {
            return ByteRange::Unsatisfiable;
        }
        return ByteRange::Range {
            start: size - suffix,
            end: size - 1,
        };
    }
    let Ok(start) = start.parse::<u64>() else {
        return ByteRange::Unsatisfiable;
    };
    if start >= size {
        return ByteRange::Unsatisfiable;
    }
    let end = if end.is_empty() {
        size - 1
    } else {
        let Ok(end) = end.parse::<u64>() else {
            return ByteRange::Unsatisfiable;
        };
        end.min(size - 1)
    };
    if end < start {
        ByteRange::Unsatisfiable
    } else {
        ByteRange::Range { start, end }
    }
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
    Query(query): Query<VersionQuery>,
) -> Result<Response> {
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
    let remote = vfs::is_qms_strm_uri(&archive.path);
    let mut response = Json(ComicPagesResponse {
        pages: (*cached.pages).clone(),
    })
    .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static(if remote {
            MEDIA_NO_CACHE
        } else {
            media_cache_control(query.v.as_deref())
        }),
    );
    Ok(response)
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
    let archive = crate::archive::open_media_zip(file, "comic archive")?;
    let mut names = archive
        .file_names()
        .filter(|name| image_name(name))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    names.sort_by_cached_key(|name| naturalish_key(name));
    let mut pages = Vec::with_capacity(names.len());
    for name in names {
        pages.push(ComicPageInfo {
            name,
            width: None,
            height: None,
        });
    }
    let mut archives = Vec::with_capacity(COMIC_ARCHIVE_POOL_SIZE);
    archives.push(Mutex::new(archive));
    for _ in 1..COMIC_ARCHIVE_POOL_SIZE {
        let extra = File::open(path)
            .map_err(AppError::from)
            .and_then(|file| crate::archive::open_media_zip(file, "comic archive pool"));
        match extra {
            Ok(archive) => archives.push(Mutex::new(archive)),
            Err(err) => {
                tracing::warn!(path = %path.display(), error = %err, "failed to open an extra comic archive handle; using a smaller pool");
                break;
            }
        }
    }
    Ok(CachedComicPages {
        size,
        modified,
        pages: Arc::new(pages),
        archive: Arc::new(ComicArchivePool {
            archives,
            next: AtomicUsize::new(0),
        }),
    })
}

async fn cached_cbz_pages(state: &AppState, path: &FsPath) -> Result<CachedComicPages> {
    let metadata = tokio::fs::metadata(path).await?;
    let size = metadata.len();
    let modified = metadata.modified().ok();
    let key = path.to_string_lossy().to_string();

    {
        let mut cache = state.comic_page_cache.state.lock().await;
        if let Some(entry) = cache.entries.get(&key).cloned() {
            if entry.size == size && entry.modified == modified {
                touch_lru(&mut cache.lru, &key);
                return Ok(entry);
            }
            cache.entries.remove(&key);
            remove_lru_key(&mut cache.lru, &key);
        }
    }

    let load_lock = comic_load_lock(&state.comic_page_cache, &key)?;
    let _load_guard = load_lock.lock().await;
    let metadata = tokio::fs::metadata(path).await?;
    let size = metadata.len();
    let modified = metadata.modified().ok();
    {
        let mut cache = state.comic_page_cache.state.lock().await;
        if let Some(entry) = cache.entries.get(&key).cloned() {
            if entry.size == size && entry.modified == modified {
                touch_lru(&mut cache.lru, &key);
                return Ok(entry);
            }
            cache.entries.remove(&key);
            remove_lru_key(&mut cache.lru, &key);
        }
    }

    let path = path.to_path_buf();
    let cached = archive_blocking(move || open_cbz_pages(&path)).await?;
    let mut cache = state.comic_page_cache.state.lock().await;
    while cache.entries.len() >= COMIC_PAGE_CACHE_LIMIT {
        let Some(evicted) = cache.lru.pop_front() else {
            break;
        };
        cache.entries.remove(&evicted);
    }
    cache.entries.insert(key.clone(), cached.clone());
    touch_lru(&mut cache.lru, &key);
    Ok(cached)
}

fn comic_load_lock(cache: &ComicPageCache, key: &str) -> Result<Arc<AsyncMutex<()>>> {
    let mut locks = cache
        .load_locks
        .lock()
        .map_err(|_| AppError::Other("comic cache load lock registry poisoned".to_string()))?;
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(key).and_then(Weak::upgrade) {
        return Ok(lock);
    }
    let lock = Arc::new(AsyncMutex::new(()));
    locks.insert(key.to_string(), Arc::downgrade(&lock));
    Ok(lock)
}

fn touch_lru(lru: &mut VecDeque<String>, key: &str) {
    remove_lru_key(lru, key);
    lru.push_back(key.to_string());
}

fn remove_lru_key(lru: &mut VecDeque<String>, key: &str) {
    if let Some(index) = lru.iter().position(|candidate| candidate == key) {
        lru.remove(index);
    }
}

fn cbz_named_page_bytes(pool: &ComicArchivePool, name: &str) -> Result<Vec<u8>> {
    let count = pool.archives.len();
    if count == 0 {
        return Err(AppError::Other(
            "comic archive cache has no readable handles".to_string(),
        ));
    }
    let start = pool.next.fetch_add(1, Ordering::Relaxed) % count;
    let mut fallback = None;
    for offset in 0..count {
        let index = (start + offset) % count;
        match pool.archives[index].try_lock() {
            Ok(mut archive) => return read_cbz_page(&mut archive, name),
            Err(TryLockError::WouldBlock) => fallback.get_or_insert(index),
            Err(TryLockError::Poisoned(_)) => continue,
        };
    }
    let index = fallback
        .ok_or_else(|| AppError::Other("comic archive cache handles are poisoned".to_string()))?;
    let mut archive = pool.archives[index]
        .lock()
        .map_err(|_| AppError::Other("comic archive cache lock poisoned".to_string()))?;
    read_cbz_page(&mut archive, name)
}

fn read_cbz_page(archive: &mut ZipArchive<File>, name: &str) -> Result<Vec<u8>> {
    let mut entry = archive
        .by_name(name)
        .map_err(|e| AppError::Other(e.to_string()))?;
    if entry.size() > MAX_COMIC_PAGE_BYTES {
        return Err(AppError::BadRequest(format!(
            "comic page exceeds {MAX_COMIC_PAGE_BYTES} decompressed bytes"
        )));
    }
    read_limited_zip_entry(&mut entry, MAX_COMIC_PAGE_BYTES, "comic page")
}

pub async fn stream_comic_page(
    State(state): State<Arc<AppState>>,
    Path((work_id, page)): Path<(i64, usize)>,
    Query(query): Query<VersionQuery>,
) -> Result<Response> {
    let detail = state.db.work_detail(work_id).await?;
    let archive = detail
        .assets
        .iter()
        .find(|asset| asset.role == "archive")
        .ok_or_else(|| AppError::NotFound("comic archive asset not found".to_string()))?;
    let remote_archive = vfs::is_qms_strm_uri(&archive.path);
    let path = vfs::asset_local_processing_path(&state, archive).await?;
    let cached = cached_cbz_pages(&state, &path).await?;
    let name = cached
        .pages
        .get(page)
        .ok_or_else(|| AppError::NotFound(format!("page {page} not found")))?
        .name
        .clone();
    let cached_archive = cached.archive.clone();
    let stream_name = name.clone();
    let bytes =
        archive_blocking(move || cbz_named_page_bytes(&cached_archive, &stream_name)).await?;
    let mime = path_mime(std::path::Path::new(&name));
    let cache_control = if remote_archive {
        MEDIA_NO_CACHE
    } else {
        media_cache_control(query.v.as_deref())
    };
    Ok((
        [
            (header::CONTENT_TYPE, mime),
            (header::CACHE_CONTROL, cache_control.to_string()),
        ],
        bytes,
    )
        .into_response())
}

#[derive(Debug, Serialize)]
pub struct EpubManifestResponse {
    pub chapters: Vec<EpubChapter>,
}

#[derive(Debug, Clone, Serialize)]
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
    Query(query): Query<VersionQuery>,
) -> Result<Response> {
    let detail = state.db.work_detail(work_id).await?;
    let (book, remote) = book_asset_path(&state, &detail).await?;
    let cached = cached_epub_manifest(&book).await?;
    let mut response = Json(EpubManifestResponse {
        chapters: (*cached.chapters).clone(),
    })
    .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static(if remote {
            MEDIA_NO_CACHE
        } else {
            media_cache_control(query.v.as_deref())
        }),
    );
    Ok(response)
}

pub async fn epub_chapter_html(
    State(state): State<Arc<AppState>>,
    Path((work_id, chapter)): Path<(i64, usize)>,
    Query(query): Query<VersionQuery>,
) -> Result<Response> {
    let detail = state.db.work_detail(work_id).await?;
    let (book, remote_book) = book_asset_path(&state, &detail).await?;
    let cached = cached_epub_manifest(&book).await?;
    let chapter_path = cached
        .chapters
        .get(chapter)
        .ok_or_else(|| AppError::NotFound(format!("EPUB chapter {chapter} not found")))?
        .href
        .clone();
    let version = query.v.clone();
    let archive = cached.archive.clone();
    let html = archive_blocking(move || {
        read_epub_chapter_html(&archive, work_id, &chapter_path, version.as_deref())
    })
    .await?;
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(
            header::CACHE_CONTROL,
            if remote_book {
                MEDIA_NO_CACHE
            } else {
                media_cache_control(query.v.as_deref())
            },
        )
        .body(Body::from(html))
        .map_err(|e| AppError::Other(e.to_string()))
}

#[derive(Debug, Deserialize)]
pub struct EpubImageQuery {
    pub path: String,
    pub v: Option<String>,
}

pub async fn stream_epub_image(
    State(state): State<Arc<AppState>>,
    Path(work_id): Path<i64>,
    Query(query): Query<EpubImageQuery>,
) -> Result<Response> {
    let detail = state.db.work_detail(work_id).await?;
    let (book, remote_book) = book_asset_path(&state, &detail).await?;
    let cached = cached_epub_manifest(&book).await?;
    let image_path = normalize_epub_entry_path(&query.path)?;
    let mime = path_mime(FsPath::new(&image_path));
    if !mime.starts_with("image/") {
        return Err(AppError::BadRequest(
            "EPUB entry is not an image".to_string(),
        ));
    }
    let archive = cached.archive.clone();
    let bytes = archive_blocking(move || {
        let mut archive = archive
            .lock()
            .map_err(|_| AppError::Other("EPUB archive cache lock poisoned".to_string()))?;
        read_zip_bytes(&mut archive, &image_path)
    })
    .await?;
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime)
        .header(
            header::CACHE_CONTROL,
            if remote_book {
                MEDIA_NO_CACHE
            } else {
                media_cache_control(query.v.as_deref())
            },
        )
        .body(Body::from(bytes))
        .map_err(|e| AppError::Other(e.to_string()))
}

async fn book_asset_path(state: &AppState, detail: &WorkDetail) -> Result<(PathBuf, bool)> {
    let book = detail
        .assets
        .iter()
        .find(|asset| asset.role == "book" && asset.mime == "application/epub+zip")
        .ok_or_else(|| AppError::NotFound("EPUB asset not found".to_string()))?;
    let remote = vfs::is_qms_strm_uri(&book.path);
    Ok((vfs::asset_local_processing_path(state, book).await?, remote))
}

fn read_epub_manifest(path: &FsPath) -> Result<(Vec<EpubChapter>, ZipArchive<File>)> {
    let mut archive = open_epub(path)?;
    let opf_name = epub_opf_name(&mut archive)?;
    let opf = read_zip_text(&mut archive, &opf_name)?;
    let chapter_entries = epub_chapter_entries(&mut archive, &opf_name, &opf)?;
    ensure_epub_chapter_limit(chapter_entries.len())?;
    let mut chapters = Vec::new();
    let mut title_probe_budget = MAX_EPUB_TITLE_PROBE_TOTAL_BYTES;
    for (href, toc_title) in chapter_entries {
        let title = toc_title
            .filter(|title| !title.trim().is_empty())
            .or_else(|| probe_epub_chapter_title(&mut archive, &href, &mut title_probe_budget))
            .unwrap_or_else(|| short_zip_name(&href));
        chapters.push(EpubChapter {
            index: chapters.len(),
            title,
            href,
        });
    }
    Ok((chapters, archive))
}

async fn cached_epub_manifest(path: &FsPath) -> Result<CachedEpubManifest> {
    let metadata = tokio::fs::metadata(path).await?;
    let size = metadata.len();
    let modified = metadata.modified().ok();
    let key = path.to_path_buf();
    {
        let mut cache = EPUB_MANIFEST_CACHE.lock().await;
        if let Some(entry) = cache.entries.get(&key).cloned() {
            if entry.size == size && entry.modified == modified {
                touch_path_lru(&mut cache.lru, &key);
                return Ok(entry);
            }
            cache.entries.remove(&key);
            remove_path_lru_key(&mut cache.lru, &key);
        }
    }

    let load_lock = epub_manifest_load_lock(&key)?;
    let _load_guard = load_lock.lock().await;
    let metadata = tokio::fs::metadata(path).await?;
    let size = metadata.len();
    let modified = metadata.modified().ok();
    {
        let mut cache = EPUB_MANIFEST_CACHE.lock().await;
        if let Some(entry) = cache.entries.get(&key).cloned() {
            if entry.size == size && entry.modified == modified {
                touch_path_lru(&mut cache.lru, &key);
                return Ok(entry);
            }
            cache.entries.remove(&key);
            remove_path_lru_key(&mut cache.lru, &key);
        }
    }

    let load_path = key.clone();
    let (chapters, archive) = archive_blocking(move || read_epub_manifest(&load_path)).await?;
    let chapters = Arc::new(chapters);
    let cached = CachedEpubManifest {
        size,
        modified,
        chapters,
        archive: Arc::new(Mutex::new(archive)),
    };
    let mut cache = EPUB_MANIFEST_CACHE.lock().await;
    while cache.entries.len() >= EPUB_MANIFEST_CACHE_LIMIT {
        let Some(evicted) = cache.lru.pop_front() else {
            break;
        };
        cache.entries.remove(&evicted);
    }
    cache.entries.insert(key.clone(), cached.clone());
    touch_path_lru(&mut cache.lru, &key);
    Ok(cached)
}

fn epub_manifest_load_lock(path: &FsPath) -> Result<Arc<AsyncMutex<()>>> {
    let mut locks = EPUB_MANIFEST_LOAD_LOCKS
        .lock()
        .map_err(|_| AppError::Other("EPUB manifest lock registry poisoned".to_string()))?;
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
        return Ok(lock);
    }
    let lock = Arc::new(AsyncMutex::new(()));
    locks.insert(path.to_path_buf(), Arc::downgrade(&lock));
    Ok(lock)
}

fn touch_path_lru(lru: &mut VecDeque<PathBuf>, key: &FsPath) {
    remove_path_lru_key(lru, key);
    lru.push_back(key.to_path_buf());
}

fn remove_path_lru_key(lru: &mut VecDeque<PathBuf>, key: &FsPath) {
    if let Some(index) = lru.iter().position(|candidate| candidate == key) {
        lru.remove(index);
    }
}

fn read_epub_chapter_html(
    archive: &Mutex<ZipArchive<File>>,
    work_id: i64,
    chapter_path: &str,
    version: Option<&str>,
) -> Result<String> {
    let mut archive = archive
        .lock()
        .map_err(|_| AppError::Other("EPUB archive cache lock poisoned".to_string()))?;
    let raw = read_zip_text(&mut archive, chapter_path)?;
    let title = chapter_title(&raw).unwrap_or_else(|| short_zip_name(chapter_path));
    let body = sanitize_epub_html(work_id, chapter_path, &raw, version);
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
    crate::archive::open_media_zip(file, "EPUB")
}

fn epub_opf_name(archive: &mut ZipArchive<File>) -> Result<String> {
    if let Ok(container) = read_zip_text(archive, "META-INF/container.xml") {
        if let Some(path) = first_attr(&container, "rootfile", "full-path") {
            return Ok(path);
        }
    }
    if let Some(name) = archive
        .file_names()
        .find(|name| name.ends_with(".opf"))
        .map(ToOwned::to_owned)
    {
        return Ok(name);
    }
    Err(AppError::NotFound(
        "EPUB OPF package file not found".to_string(),
    ))
}

fn read_zip_text(archive: &mut ZipArchive<File>, name: &str) -> Result<String> {
    let mut entry = archive
        .by_name(name)
        .map_err(|e| AppError::NotFound(format!("EPUB entry {name} not found: {e}")))?;
    if entry.size() > MAX_EPUB_TEXT_BYTES {
        return Err(AppError::BadRequest(format!(
            "EPUB text entry exceeds {MAX_EPUB_TEXT_BYTES} decompressed bytes"
        )));
    }
    let bytes = read_limited_zip_entry(&mut entry, MAX_EPUB_TEXT_BYTES, "EPUB text entry")?;
    Ok(String::from_utf8_lossy(&bytes).to_string())
}

#[cfg(test)]
fn read_zip_text_prefix(archive: &mut ZipArchive<File>, name: &str, limit: u64) -> Result<String> {
    read_zip_text_prefix_with_len(archive, name, limit).map(|(text, _)| text)
}

fn read_zip_text_prefix_with_len(
    archive: &mut ZipArchive<File>,
    name: &str,
    limit: u64,
) -> Result<(String, u64)> {
    let entry = archive
        .by_name(name)
        .map_err(|e| AppError::NotFound(format!("EPUB entry {name} not found: {e}")))?;
    let mut bytes = Vec::with_capacity(entry.size().min(limit) as usize);
    entry.take(limit).read_to_end(&mut bytes)?;
    let read = bytes.len() as u64;
    Ok((String::from_utf8_lossy(&bytes).to_string(), read))
}

fn ensure_epub_chapter_limit(chapter_count: usize) -> Result<()> {
    if chapter_count > MAX_EPUB_CHAPTERS {
        return Err(AppError::BadRequest(format!(
            "EPUB contains {chapter_count} chapters, exceeding the limit of {MAX_EPUB_CHAPTERS}"
        )));
    }
    Ok(())
}

fn probe_epub_chapter_title(
    archive: &mut ZipArchive<File>,
    name: &str,
    remaining_budget: &mut u64,
) -> Option<String> {
    let limit = (*remaining_budget).min(MAX_EPUB_TITLE_PROBE_BYTES);
    if limit == 0 {
        return None;
    }
    *remaining_budget -= limit;
    match read_zip_text_prefix_with_len(archive, name, limit) {
        Ok((html, read)) => {
            *remaining_budget = remaining_budget.saturating_add(limit.saturating_sub(read));
            chapter_title(&html)
        }
        Err(_) => None,
    }
}

fn read_zip_bytes(archive: &mut ZipArchive<File>, name: &str) -> Result<Vec<u8>> {
    let mut entry = archive
        .by_name(name)
        .map_err(|e| AppError::NotFound(format!("EPUB entry {name} not found: {e}")))?;
    if entry.size() > MAX_EPUB_IMAGE_BYTES {
        return Err(AppError::BadRequest(format!(
            "EPUB image entry exceeds {MAX_EPUB_IMAGE_BYTES} decompressed bytes"
        )));
    }
    read_limited_zip_entry(&mut entry, MAX_EPUB_IMAGE_BYTES, "EPUB image entry")
}

fn read_limited_zip_entry<R: Read>(entry: &mut R, limit: u64, label: &str) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    entry.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(AppError::BadRequest(format!(
            "{label} exceeds {limit} decompressed bytes"
        )));
    }
    Ok(bytes)
}

fn parse_epub_manifest(opf: &str) -> BTreeMap<String, EpubItem> {
    let mut items = BTreeMap::new();
    for item in EPUB_ITEM_RE.find_iter(opf) {
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
        .filter(|(href, title)| readable_epub_chapter(href, title.as_deref()))
        .cloned()
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
    let section = EPUB_NAV_SECTION_RE
        .captures(html)
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| html.to_string());
    EPUB_NAV_LINK_RE
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
    EPUB_NAV_POINT_RE
        .captures_iter(ncx)
        .filter_map(|caps| {
            let block = caps.get(1)?.as_str();
            let title = EPUB_NAV_TEXT_RE
                .captures(block)
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
    EPUB_ITEM_REF_RE
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
    let result = XML_ATTR_RE.captures_iter(tag).find_map(|caps| {
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
    for re in EPUB_CHAPTER_TITLE_RES.iter() {
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

fn sanitize_epub_html(
    work_id: i64,
    chapter_path: &str,
    raw: &str,
    version: Option<&str>,
) -> String {
    let mut html = extract_body(raw).unwrap_or_else(|| raw.to_string());
    for pattern in EPUB_SANITIZE_RES.iter() {
        html = pattern.replace_all(&html, "").to_string();
    }
    rewrite_epub_images(work_id, chapter_path, &html, version)
}

fn extract_body(raw: &str) -> Option<String> {
    EPUB_BODY_RE
        .captures(raw)
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str().to_string())
}

fn rewrite_epub_images(
    work_id: i64,
    chapter_path: &str,
    html: &str,
    version: Option<&str>,
) -> String {
    let mut output = String::with_capacity(html.len());
    let mut last = 0;
    for tag in EPUB_MEDIA_TAG_RE.find_iter(html) {
        output.push_str(&html[last..tag.start()]);
        output.push_str(&rewrite_epub_media_tag(
            work_id,
            chapter_path,
            tag.as_str(),
            version,
        ));
        last = tag.end();
    }
    output.push_str(&html[last..]);
    output
}

fn rewrite_epub_media_tag(
    work_id: i64,
    chapter_path: &str,
    tag: &str,
    version: Option<&str>,
) -> String {
    let rewritten = EPUB_MEDIA_ATTR_RE
        .replace_all(tag, |caps: &regex::Captures<'_>| {
            let prefix = caps.get(1).map(|m| m.as_str()).unwrap_or_default();
            let quote = if caps.get(3).is_some() { "\"" } else { "'" };
            let value = caps
                .get(3)
                .or_else(|| caps.get(4))
                .map(|m| m.as_str())
                .unwrap_or_default();
            if let Some(url) = epub_image_url(work_id, chapter_path, value, version) {
                format!("{prefix}{quote}{url}{quote}")
            } else {
                caps.get(0)
                    .map(|m| m.as_str())
                    .unwrap_or_default()
                    .to_string()
            }
        })
        .to_string();
    EPUB_SRCSET_RE
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
                rewrite_epub_srcset(work_id, chapter_path, value, version)
            )
        })
        .to_string()
}

fn rewrite_epub_srcset(
    work_id: i64,
    chapter_path: &str,
    value: &str,
    version: Option<&str>,
) -> String {
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
            let next_src = epub_image_url(work_id, chapter_path, src, version)
                .unwrap_or_else(|| src.to_string());
            if rest.is_empty() {
                next_src
            } else {
                format!("{next_src} {rest}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn epub_image_url(
    work_id: i64,
    chapter_path: &str,
    src: &str,
    version: Option<&str>,
) -> Option<String> {
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
    let version = version
        .map(|value| url::form_urlencoded::byte_serialize(value.as_bytes()).collect::<String>())
        .map(|value| format!("&v={value}"))
        .unwrap_or_default();
    Some(format!(
        "/api/works/{work_id}/epub/image?path={encoded}{version}"
    ))
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
    html_escape::decode_html_entities(HTML_TAG_RE.replace_all(value, "").trim()).to_string()
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
                admin_password_persisted: false,
                admin_password_ephemeral: false,
                lightnovel_api_bases: Vec::new(),
                lightnovel_access_token: None,
                enrichment_concurrency: 1,
                ehtt_url: String::new(),
                openai_api_key: None,
                openai_image_model: "gpt-image-2".to_string(),
                qmediasync_base_url: String::new(),
                cloud_cache_max_bytes: 64 * 1024 * 1024 * 1024,
                thumbnail_cache_max_bytes_per_dir: 8 * 1024 * 1024 * 1024,
                session_secret: "test-secret".to_string(),
                enable_file_watcher: false,
                watch_debounce_seconds: 20,
            },
            db,
            http: reqwest::Client::new(),
            comic_page_cache: Arc::new(ComicPageCache::default()),
            auth_epoch: Arc::new(tokio::sync::RwLock::new("test".to_string())),
            admin_password_persisted: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });

        let response = work_cover(
            State(state.clone()),
            Path(work_id),
            Query(CoverQuery {
                size: Some(128),
                v: None,
            }),
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
            Query(CoverQuery {
                size: Some(128),
                v: None,
            }),
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

    #[test]
    fn comic_cache_keeps_a_small_parallel_archive_pool() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("comic.cbz");
        write_test_zip(&path);

        let cached = open_cbz_pages(&path).unwrap();
        assert_eq!(cached.archive.archives.len(), COMIC_ARCHIVE_POOL_SIZE);
        assert_eq!(
            cbz_named_page_bytes(&cached.archive, "1.png").unwrap(),
            PNG_1X1
        );
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

    #[test]
    fn byte_range_parser_distinguishes_missing_valid_and_unsatisfiable() {
        let headers = HeaderMap::new();
        assert_eq!(parse_byte_range(&headers, 100), ByteRange::None);

        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, "bytes=10-19".parse().unwrap());
        assert_eq!(
            parse_byte_range(&headers, 100),
            ByteRange::Range { start: 10, end: 19 }
        );

        headers.insert(header::RANGE, "bytes=-20".parse().unwrap());
        assert_eq!(
            parse_byte_range(&headers, 100),
            ByteRange::Range { start: 80, end: 99 }
        );

        headers.insert(header::RANGE, "bytes=100-".parse().unwrap());
        assert_eq!(parse_byte_range(&headers, 100), ByteRange::Unsatisfiable);

        headers.insert(header::RANGE, "bytes=0-1,4-5".parse().unwrap());
        assert_eq!(parse_byte_range(&headers, 100), ByteRange::Unsatisfiable);
    }

    #[test]
    fn thumbnail_sizes_use_bounded_cache_buckets() {
        assert_eq!(thumbnail_size_bucket(96), 128);
        assert_eq!(thumbnail_size_bucket(128), 128);
        assert_eq!(thumbnail_size_bucket(129), 256);
        assert_eq!(thumbnail_size_bucket(256), 256);
        assert_eq!(thumbnail_size_bucket(361), 480);
        assert_eq!(thumbnail_size_bucket(10_000), 960);
    }

    #[test]
    fn system_time_cache_key_preserves_subsecond_changes() {
        let first = UNIX_EPOCH + Duration::from_secs(7) + Duration::from_millis(1);
        let second = UNIX_EPOCH + Duration::from_secs(7) + Duration::from_millis(2);
        assert_eq!(system_time_key(first), Some(7_001_000_000));
        assert_eq!(system_time_key(second), Some(7_002_000_000));
        assert_ne!(system_time_key(first), system_time_key(second));
    }

    #[test]
    fn limited_zip_entry_rejects_decompressed_overflow() {
        let mut input = &b"123456"[..];
        let error = read_limited_zip_entry(&mut input, 5, "test entry").unwrap_err();
        assert!(error.to_string().contains("exceeds 5 decompressed bytes"));
    }

    #[test]
    fn epub_title_probe_reads_only_the_prefix() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("book.epub");
        let file = File::create(&path).unwrap();
        let mut writer = zip::ZipWriter::new(file);
        writer
            .start_file("chapter.xhtml", zip::write::SimpleFileOptions::default())
            .unwrap();
        writer.write_all(&vec![b'x'; 1024]).unwrap();
        writer.finish().unwrap();

        let file = File::open(path).unwrap();
        let mut archive = ZipArchive::new(file).unwrap();
        let prefix = read_zip_text_prefix(&mut archive, "chapter.xhtml", 32).unwrap();
        assert_eq!(prefix.len(), 32);
    }

    #[test]
    fn epub_title_probes_share_a_total_budget() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("book.epub");
        let first = b"<title>First</title>";
        let second = b"<title>Second</title>";
        let file = File::create(&path).unwrap();
        let mut writer = zip::ZipWriter::new(file);
        writer
            .start_file("first.xhtml", zip::write::SimpleFileOptions::default())
            .unwrap();
        writer.write_all(first).unwrap();
        writer
            .start_file("second.xhtml", zip::write::SimpleFileOptions::default())
            .unwrap();
        writer.write_all(second).unwrap();
        writer.finish().unwrap();

        let file = File::open(path).unwrap();
        let mut archive = ZipArchive::new(file).unwrap();
        let mut budget = first.len() as u64 + 5;
        assert_eq!(
            probe_epub_chapter_title(&mut archive, "first.xhtml", &mut budget).as_deref(),
            Some("First")
        );
        assert_eq!(budget, 5);
        assert_eq!(
            probe_epub_chapter_title(&mut archive, "second.xhtml", &mut budget),
            None
        );
        assert_eq!(budget, 0);
    }

    #[test]
    fn epub_manifest_rejects_excessive_chapter_counts() {
        assert!(ensure_epub_chapter_limit(MAX_EPUB_CHAPTERS).is_ok());
        let error = ensure_epub_chapter_limit(MAX_EPUB_CHAPTERS + 1).unwrap_err();
        assert!(error.to_string().contains("exceeding the limit"));
    }

    #[test]
    fn versioned_media_is_immutable_and_epub_images_inherit_version() {
        assert_eq!(media_cache_control(None), MEDIA_NO_CACHE);
        assert_eq!(
            media_cache_control(Some("asset-version")),
            MEDIA_IMMUTABLE_CACHE
        );
        let html = sanitize_epub_html(
            7,
            "OPS/chapter.xhtml",
            r#"<body><img src="images/cover.jpg"></body>"#,
            Some("book:1"),
        );
        assert!(html.contains("/api/works/7/epub/image?path=OPS%2Fimages%2Fcover.jpg&v=book%3A1"));
    }

    #[tokio::test]
    async fn thumbnail_quota_counts_files_and_concurrent_reservations() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("existing.jpg"), [0_u8; 8]).unwrap();
        std::fs::write(temp.path().join(".thumbnail.part"), [0_u8; 1]).unwrap();
        let cache_path = temp.path().join("new.jpg");

        assert_eq!(thumbnail_cache_usage(temp.path()).unwrap(), 9);
        let reservation = reserve_thumbnail_cache_capacity(&cache_path, 11)
            .await
            .unwrap();
        assert_eq!(reservation.limit(), 2);
        assert!(reserve_thumbnail_cache_capacity(&cache_path, 11)
            .await
            .is_err());
        drop(reservation);
        assert!(reserve_thumbnail_cache_capacity(&cache_path, 11)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn thumbnail_quota_resyncs_after_explicit_file_removal() {
        let temp = tempfile::tempdir().unwrap();
        let existing = temp.path().join("existing.jpg");
        std::fs::write(&existing, [0_u8; 8]).unwrap();
        let cache_path = temp.path().join("new.jpg");
        let reservation = reserve_thumbnail_cache_capacity(&cache_path, 10)
            .await
            .unwrap();
        drop(reservation);

        std::fs::remove_file(existing).unwrap();
        let reservation = reserve_thumbnail_cache_capacity(&cache_path, 2)
            .await
            .unwrap();
        assert_eq!(reservation.limit(), 2);
    }

    #[tokio::test]
    async fn over_quota_generated_thumbnail_is_removed() {
        let temp = tempfile::tempdir().unwrap();
        let cache_path = temp.path().join("thumb.jpg");
        let reservation = reserve_thumbnail_cache_capacity(&cache_path, 2)
            .await
            .unwrap();
        std::fs::write(&cache_path, [0_u8; 3]).unwrap();

        assert!(
            finalize_thumbnail_generation(reservation, &cache_path, Ok(()))
                .await
                .is_err()
        );
        assert!(!cache_path.exists());
    }

    #[test]
    fn thumbnail_publish_uses_atomic_final_file() {
        let temp = tempfile::tempdir().unwrap();
        let cache_path = temp.path().join("thumb.jpg");
        let image = image::DynamicImage::new_rgb8(4, 4);
        publish_thumbnail(image, &cache_path, 4).unwrap();
        assert!(cache_path.is_file());
        assert_eq!(cache_files(temp.path()), vec!["thumb.jpg".to_string()]);
        image::ImageReader::open(cache_path)
            .unwrap()
            .with_guessed_format()
            .unwrap()
            .into_dimensions()
            .unwrap();
    }
}
