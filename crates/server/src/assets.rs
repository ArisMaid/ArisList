use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::convert::Infallible;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Cursor, Read, Write};
use std::path::{Path as FsPath, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, TryLockError, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures::StreamExt;
use image::codecs::jpeg::JpegEncoder;
use mozjpeg::{DctMethod, Decompress};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};
use tokio_util::io::ReaderStream;
use uuid::Uuid;
use zip::ZipArchive;

use crate::derivative::{DerivativeFile, DerivativeKey};
use crate::error::{AppError, Result};
use crate::models::Asset;
use crate::resource::{ResourceBufferLease, ResourceClass, ResourceGovernor};
use crate::scanner::{image_name, naturalish_key};
use crate::security::path_mime;
use crate::strm::{archive as strm_archive, cache as strm_cache, source as strm_source};
use crate::vfs;
use crate::AppState;

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
const ARCHIVE_CACHE_WEIGHT_MULTIPLIER: u64 = 3;
const ARCHIVE_CACHE_ENTRY_OVERHEAD_BYTES: u64 = 192;
const MAX_COMIC_PAGE_BYTES: u64 = 128 * 1024 * 1024;
const COMIC_MANIFEST_PAGE_SIZE: usize = 200;
const COMIC_MANIFEST_MAX_PAGE_SIZE: usize = 500;
const COMIC_MANIFEST_CACHE_ENTRY_MAX_BYTES: usize = 8 * 1024 * 1024;
const EPUB_MANIFEST_PAGE_SIZE: usize = 200;
const EPUB_MANIFEST_MAX_PAGE_SIZE: usize = 500;
const COMIC_STREAM_QUEUE_CHUNKS: usize = 4;
const READER_SIZE_BUCKETS: [u32; 2] = [1280, 1920];
const MAX_EPUB_TEXT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_EPUB_IMAGE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_EPUB_TITLE_PROBE_BYTES: u64 = 256 * 1024;
const MAX_EPUB_TITLE_PROBE_TOTAL_BYTES: u64 = 16 * 1024 * 1024;
const MAX_EPUB_CHAPTERS: usize = 10_000;
const MAX_IMAGE_DECODE_ALLOC_BYTES: u64 = 256 * 1024 * 1024;
const MAX_THUMBNAIL_CACHE_FILE_BYTES: u64 = 8 * 1024 * 1024;
const THUMBNAIL_PROCESSING_BUDGET_BYTES: u64 =
    MAX_IMAGE_DECODE_ALLOC_BYTES + MAX_COMIC_PAGE_BYTES + MAX_THUMBNAIL_CACHE_FILE_BYTES;
const CACHE_USAGE_RESCAN_INTERVAL: Duration = Duration::from_secs(60);
// A directory mtime changes for every published thumbnail.  Reacting to that
// signal for a large flat legacy cache would turn every cold miss into an
// O(number-of-cached-thumbnails) read_dir walk.  Large directories therefore
// use the bounded interval above; a stale usage estimate can only reject a
// reservation temporarily, never allow the quota to be exceeded.
const CACHE_USAGE_MTIME_RESCAN_MAX_FILES: usize = 4_096;
const REMOTE_DERIVED_CACHE_MAX_AGE: Duration = Duration::from_secs(15 * 60);
const MEDIA_NO_CACHE: &str = "private, no-cache";
const MEDIA_IMMUTABLE_CACHE: &str = "private, max-age=31536000, immutable";
const DERIVATIVE_PLACEHOLDER_CACHE: &str = "private, no-store";

#[derive(Default)]
struct ThumbnailCacheQuotaState {
    initialized: bool,
    committed: u64,
    reserved: u64,
    generation: u64,
    file_count: usize,
    observed_dir_modified: Option<SystemTime>,
    last_scan: Option<Instant>,
}

#[derive(Debug, Clone, Copy, Default)]
struct ThumbnailCacheUsage {
    bytes: u64,
    files: usize,
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

async fn archive_blocking<T, F>(
    resources: &ResourceGovernor,
    processing_bytes: u64,
    task: F,
) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    let lease = resources
        .reserve(ResourceClass::ArchiveStream, processing_bytes, 0)
        .await?;
    tokio::task::spawn_blocking(move || {
        let _lease = lease;
        task()
    })
    .await
    .map_err(|err| AppError::Other(format!("archive reader task failed: {err}")))?
}

async fn archive_bytes_blocking<F>(
    resources: &ResourceGovernor,
    processing_bytes: u64,
    inflight_bytes: u64,
    task: F,
) -> Result<(Vec<u8>, ResourceBufferLease)>
where
    F: FnOnce() -> Result<Vec<u8>> + Send + 'static,
{
    let lease = resources
        .reserve(
            ResourceClass::ArchiveStream,
            processing_bytes,
            inflight_bytes,
        )
        .await?;
    tokio::task::spawn_blocking(move || {
        // Keep both the work and inflight reservations inside the blocking
        // task. If the HTTP future is cancelled, spawn_blocking continues;
        // returning the buffer lease from this closure keeps that detached
        // work accounted until it actually exits.
        let (work_lease, buffer_lease) = lease.split();
        let _work_lease = work_lease;
        let bytes = task()?;
        Ok((bytes, buffer_lease))
    })
    .await
    .map_err(|err| AppError::Other(format!("archive reader task failed: {err}")))?
}

fn leased_bytes_body(bytes: Vec<u8>, lease: ResourceBufferLease) -> Body {
    let bytes = Bytes::from(bytes);
    Body::from_stream(async_stream::stream! {
        let _lease = lease;
        let mut offset = 0;
        while offset < bytes.len() {
            let end = (offset + STREAM_BUFFER_SIZE).min(bytes.len());
            yield Ok::<Bytes, Infallible>(bytes.slice(offset..end));
            offset = end;
        }
    })
}

async fn run_blocking_thumbnail_generation<F>(
    resources: ResourceGovernor,
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
        let lease = resources
            .reserve(
                ResourceClass::ThumbnailDecode,
                THUMBNAIL_PROCESSING_BUDGET_BYTES,
                0,
            )
            .await
            .map_err(|err| err.to_string())?;
        let generation_path = cache_path.clone();
        let generated = tokio::task::spawn_blocking(move || {
            let _lease = lease;
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
    resources: ResourceGovernor,
    state: Arc<AppState>,
    asset: crate::models::Asset,
    write_guard: tokio::sync::OwnedMutexGuard<()>,
    reservation: ThumbnailCacheReservation,
    cache_path: PathBuf,
    size: u32,
) -> std::result::Result<(), String> {
    tokio::spawn(async move {
        let _write_guard = write_guard;
        let lease = resources
            .reserve(
                ResourceClass::ThumbnailDecode,
                THUMBNAIL_PROCESSING_BUDGET_BYTES,
                0,
            )
            .await
            .map_err(|err| err.to_string())?;
        let generated = vfs::generate_qms_thumbnail(&state, &asset, &cache_path, size)
            .await
            .map_err(|err| err.to_string());
        drop(lease);
        finalize_thumbnail_generation(reservation, &cache_path, generated).await
    })
    .await
    .map_err(|err| format!("thumbnail generation task failed: {err}"))?
}

async fn generate_local_derivative_thumbnail(
    resources: ResourceGovernor,
    source_path: PathBuf,
    cache_path: PathBuf,
    size: u32,
    jpeg_downscale_enabled: bool,
) -> Result<()> {
    let lease = resources
        .reserve(
            ResourceClass::ThumbnailDecode,
            THUMBNAIL_PROCESSING_BUDGET_BYTES,
            0,
        )
        .await?;
    tokio::task::spawn_blocking(move || {
        let _lease = lease;
        generate_thumbnail_atomic(&source_path, &cache_path, size, jpeg_downscale_enabled)
            .map_err(AppError::Other)
    })
    .await
    .map_err(|err| AppError::Other(format!("derivative thumbnail worker failed: {err}")))?
}

async fn generate_qms_derivative_thumbnail(
    resources: ResourceGovernor,
    state: Arc<AppState>,
    asset: crate::models::Asset,
    cache_path: PathBuf,
    size: u32,
) -> Result<()> {
    let lease = resources
        .reserve(
            ResourceClass::ThumbnailDecode,
            THUMBNAIL_PROCESSING_BUDGET_BYTES,
            0,
        )
        .await?;
    let generated = vfs::generate_qms_thumbnail(&state, &asset, &cache_path, size).await;
    drop(lease);
    generated
}

async fn generate_archive_derivative_thumbnail(
    resources: ResourceGovernor,
    archive: Arc<ComicArchivePool>,
    entry_name: String,
    cache_path: PathBuf,
    size: u32,
    jpeg_downscale_enabled: bool,
) -> Result<()> {
    let lease = resources
        .reserve(
            ResourceClass::ThumbnailDecode,
            THUMBNAIL_PROCESSING_BUDGET_BYTES,
            0,
        )
        .await?;
    tokio::task::spawn_blocking(move || {
        let _lease = lease;
        generate_archive_thumbnail_atomic(
            &archive,
            &entry_name,
            &cache_path,
            size,
            jpeg_downscale_enabled,
        )
        .map_err(AppError::Other)
    })
    .await
    .map_err(|err| AppError::Other(format!("archive derivative worker failed: {err}")))?
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
    total_weight_bytes: u64,
}

#[derive(Default)]
struct EpubManifestCacheState {
    entries: HashMap<PathBuf, CachedEpubManifest>,
    lru: VecDeque<PathBuf>,
    total_weight_bytes: u64,
}

#[derive(Clone)]
struct CachedEpubManifest {
    size: u64,
    modified: Option<SystemTime>,
    weight_bytes: u64,
    chapters: Arc<Vec<EpubChapter>>,
    archive: Arc<Mutex<ZipArchive<File>>>,
}

#[derive(Clone)]
pub struct CachedComicPages {
    size: u64,
    modified: Option<SystemTime>,
    weight_bytes: u64,
    pages: Arc<Vec<ComicPageInfo>>,
    archive: Arc<ComicArchivePool>,
}

fn leased_local_file_stream<R>(
    file: R,
    lease: crate::resource::ResourceLease,
) -> impl futures::Stream<Item = std::io::Result<Bytes>> + Send + 'static
where
    R: AsyncRead + Unpin + Send + 'static,
{
    async_stream::stream! {
        // Keep the class and one small in-flight buffer permit alive until the
        // response body is dropped, including client disconnects.  The file
        // size itself is never reserved as memory.
        let _lease = lease;
        let mut reader = ReaderStream::with_capacity(file, STREAM_BUFFER_SIZE);
        while let Some(chunk) = reader.next().await {
            yield chunk;
        }
    }
}

struct ComicArchivePool {
    archives: Vec<Mutex<ZipArchive<File>>>,
    next: AtomicUsize,
}

async fn stream_local_file(
    state: &AppState,
    path: &FsPath,
    mime: &str,
    headers: &HeaderMap,
    cache_control: &str,
) -> Result<Response> {
    let mut file = tokio::fs::File::open(path).await?;
    let size = file.metadata().await?.len();

    match parse_byte_range(headers, size) {
        ByteRange::Range { start, end } => {
            let length = end - start + 1;
            file.seek(SeekFrom::Start(start)).await?;
            let lease = state
                .resources
                .reserve(
                    ResourceClass::LocalMediaStream,
                    0,
                    STREAM_BUFFER_SIZE as u64,
                )
                .await?;
            let body = Body::from_stream(leased_local_file_stream(file.take(length), lease));
            return Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(header::CONTENT_TYPE, mime)
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

    let lease = state
        .resources
        .reserve(
            ResourceClass::LocalMediaStream,
            0,
            STREAM_BUFFER_SIZE as u64,
        )
        .await?;
    let body = Body::from_stream(leased_local_file_stream(file, lease));
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime)
        .header(header::CACHE_CONTROL, cache_control)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, size.to_string())
        .body(body)
        .map_err(|e| AppError::Other(e.to_string()))
}

pub async fn stream_asset(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Query(query): Query<VersionQuery>,
    headers: HeaderMap,
) -> Result<Response> {
    let asset = state.db.asset(id).await?;
    if is_strm_entry_asset(&asset) {
        return stream_strm_entry_asset(&state, &asset, &headers, &query).await;
    }
    if vfs::is_qms_strm_uri(&asset.path) {
        return vfs::stream_qms_asset(state, asset, headers).await;
    }
    let path = vfs::local_asset_path(&state, &asset.path).await?;
    let cache_control = media_cache_control(query.v.as_deref());
    stream_local_file(&state, &path, &asset.mime, &headers, cache_control).await
}

fn is_strm_entry_asset(asset: &Asset) -> bool {
    serde_json::from_str::<serde_json::Value>(&asset.meta_json)
        .ok()
        .and_then(|meta| meta.get("strm_entry").and_then(|value| value.as_bool()))
        .unwrap_or(false)
}

async fn stream_strm_entry_asset(
    state: &AppState,
    asset: &Asset,
    headers: &HeaderMap,
    query: &VersionQuery,
) -> Result<Response> {
    let meta: serde_json::Value = serde_json::from_str(&asset.meta_json)
        .map_err(|error| AppError::Other(format!("invalid STRM entry metadata: {error}")))?;
    let parent_id = meta
        .get("archive_asset_id")
        .and_then(|value| value.as_i64())
        .ok_or_else(|| AppError::Other("STRM entry has no parent archive".to_string()))?;
    let entry_name = meta
        .get("entry_name")
        .and_then(|value| value.as_str())
        .ok_or_else(|| AppError::Other("STRM entry has no archive path".to_string()))?;
    let parent = state.db.asset(parent_id).await?;
    let relation = state
        .db
        .archive_asset_entry(parent.id, entry_name)
        .await?
        .ok_or_else(|| AppError::NotFound("STRM entry relation not found".to_string()))?;
    let destination = FsPath::new(&relation.entry_path).to_path_buf();
    let prepared = if let Some((target_url, manifest)) =
        remote_archive_manifest(state, &parent).await?
    {
        let entry = manifest
            .entries
            .iter()
            .find(|entry| entry.name == entry_name)
            .ok_or_else(|| {
                AppError::NotFound(format!("remote archive entry {entry_name} not found"))
            })?;
        if entry.encrypted {
            let archive_path = vfs::ensure_qms_asset_cached(state, &parent).await?;
            let entries = strm_archive::list_seven_zip_entries(&archive_path).await?;
            let entry = entries
                .into_iter()
                .find(|entry| entry.name == entry_name)
                .ok_or_else(|| {
                    AppError::NotFound(format!("archive entry {entry_name} not found"))
                })?;
            let password = crate::strm::jobs::password_for_asset(parent.id);
            strm_archive::prepare_seven_zip_entry(
                &archive_path,
                &entry,
                &destination,
                password.as_deref(),
                None,
            )
            .await?
        } else {
            prepare_remote_strm_entry(state, &target_url, &manifest, entry, &destination).await?
        }
    } else {
        let archive_path = vfs::ensure_qms_asset_cached(state, &parent).await?;
        let target_url = vfs::qms_target_url_for_asset(state, &parent).await?;
        let extension = strm_source::extension_from_target(&target_url).unwrap_or("zip");
        match strm_archive::support_for_extension(extension) {
            strm_archive::ArchiveSupport::Zip => {
                match strm_archive::prepare_local_zip_entry(&archive_path, entry_name, &destination)
                    .await
                {
                    Ok(path) => path,
                    Err(_zip_error) if strm_archive::seven_zip_available().await => {
                        let entry = strm_archive::list_seven_zip_entries(&archive_path)
                            .await?
                            .into_iter()
                            .find(|entry| entry.name == entry_name)
                            .ok_or_else(|| {
                                AppError::NotFound(format!("archive entry {entry_name} not found"))
                            })?;
                        let password = crate::strm::jobs::password_for_asset(parent.id);
                        strm_archive::prepare_seven_zip_entry(
                            &archive_path,
                            &entry,
                            &destination,
                            password.as_deref(),
                            None,
                        )
                        .await?
                    }
                    Err(zip_error) => return Err(zip_error),
                }
            }
            strm_archive::ArchiveSupport::SevenZip | strm_archive::ArchiveSupport::Rar => {
                let entries = strm_archive::list_seven_zip_entries(&archive_path).await?;
                let entry = entries
                    .into_iter()
                    .find(|entry| entry.name == entry_name)
                    .ok_or_else(|| {
                        AppError::NotFound(format!("archive entry {entry_name} not found"))
                    })?;
                let password = crate::strm::jobs::password_for_asset(parent.id);
                strm_archive::prepare_seven_zip_entry(
                    &archive_path,
                    &entry,
                    &destination,
                    password.as_deref(),
                    None,
                )
                .await?
            }
            strm_archive::ArchiveSupport::Unsupported => {
                return Err(strm_archive::unsupported_archive_error(extension));
            }
        }
    };
    stream_local_file(
        state,
        &prepared,
        &asset.mime,
        headers,
        media_cache_control(query.v.as_deref()),
    )
    .await
}

async fn prepare_remote_strm_entry(
    state: &AppState,
    target_url: &str,
    manifest: &strm_archive::RemoteArchiveManifest,
    entry: &strm_archive::RemoteArchiveEntry,
    destination: &FsPath,
) -> Result<PathBuf> {
    if tokio::fs::try_exists(destination).await? {
        return strm_archive::prepare_entry(target_url, manifest, entry, destination).await;
    }
    let reservation =
        vfs::reserve_strm_entry_cache_capacity(state, entry.uncompressed_size).await?;
    let prepared = strm_archive::prepare_entry(target_url, manifest, entry, destination).await?;
    let actual = tokio::fs::metadata(&prepared).await?.len();
    let observed_dir_modified = destination
        .parent()
        .and_then(|path| std::fs::metadata(path).ok())
        .and_then(|metadata| metadata.modified().ok());
    reservation.commit(actual, 0, observed_dir_modified);
    Ok(prepared)
}

pub async fn head_asset(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Query(query): Query<VersionQuery>,
) -> Result<Response> {
    let asset = state.db.asset(id).await?;
    if is_strm_entry_asset(&asset) {
        return reject_expensive_head().await;
    }
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

#[derive(Debug, Deserialize)]
pub struct ComicPageQuery {
    pub v: Option<String>,
    /// Requested longest edge for an optional reader derivative.  The value
    /// is bucketed to 1280/1920 and ignored while Derivative Cache v2 is off.
    pub size: Option<u32>,
}

pub async fn work_cover(
    State(state): State<Arc<AppState>>,
    Path(work_id): Path<i64>,
    Query(query): Query<CoverQuery>,
) -> Result<Response> {
    let crate::db::WorkCoverSource {
        kind,
        image_cover,
        archive,
    } = state.db.work_cover_source(work_id).await?;
    let size = thumbnail_size_bucket(query.size.unwrap_or(480));
    let cache_control = media_cache_control(query.v.as_deref());
    let settings = crate::settings::load_settings(&state.config).await?;
    let cache_dir = settings.cover_cache_dirs.for_work_kind(&kind);
    tokio::fs::create_dir_all(&cache_dir).await?;

    if let Some(asset) = image_cover {
        return cached_image_cover(state, work_id, asset, cache_dir, size, cache_control).await;
    }

    if matches!(kind.as_str(), "comic" | "coser-picture") {
        let archive = archive
            .ok_or_else(|| AppError::NotFound("archive cover source not found".to_string()))?;
        if vfs::is_qms_strm_uri(&archive.path) {
            return Err(AppError::NotFound(
                "remote archive has no cached side cover".to_string(),
            ));
        }
        return cached_archive_cover(state, work_id, archive, cache_dir, size, cache_control).await;
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
    let (cache_path, source_path, source_version) = if is_qms {
        let version = vfs::qms_asset_version_key(&state, &asset).await?;
        (
            thumbs_dir.join(format!("{}-{}-{version}.jpg", asset.id, size)),
            None,
            format!("qms:{version}"),
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
            format!("local:{}:{modified}", source_meta.len()),
        )
    };

    if valid_thumbnail_cache_for_source(&cache_path, is_qms).await? {
        return stream_thumb_cache(cache_path, cache_control).await;
    }
    if size == 256 && state.derivatives.enabled() {
        let key = DerivativeKey::new("asset", asset.id, "thumb-256", source_version);
        let resources = state.resources.clone();
        let generation_state = state.clone();
        let generation_asset = asset.clone();
        let jpeg_downscale_enabled = state.config.jpeg_thumbnail_downscale_enabled;
        let generated = state
            .derivatives
            .get_or_generate(key, "image/jpeg", move |derivative_path| async move {
                if let Some(source_path) = source_path {
                    generate_local_derivative_thumbnail(
                        resources,
                        source_path,
                        derivative_path,
                        size,
                        jpeg_downscale_enabled,
                    )
                    .await
                } else {
                    generate_qms_derivative_thumbnail(
                        resources,
                        generation_state,
                        generation_asset,
                        derivative_path,
                        size,
                    )
                    .await
                }
            })
            .await;
        return match generated {
            Ok(Some(file)) => stream_derivative_cache(file, cache_control).await,
            Ok(None) => Err(AppError::Other(
                "derivative cache was disabled during generation".to_string(),
            )),
            Err(err) => {
                tracing::warn!(asset_id = asset.id, error = %err, "v2 thumbnail generation failed; serving a retryable placeholder");
                thumbnail_placeholder_response(size)
            }
        };
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
                let jpeg_downscale_enabled = state.config.jpeg_thumbnail_downscale_enabled;
                run_blocking_thumbnail_generation(
                    state.resources.clone(),
                    write_guard,
                    reservation,
                    cache_path.clone(),
                    move |cache_path| {
                        generate_thumbnail_atomic(
                            &source_path,
                            cache_path,
                            size,
                            jpeg_downscale_enabled,
                        )
                    },
                )
                .await
            } else {
                run_qms_thumbnail_generation(
                    state.resources.clone(),
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
            tracing::warn!(asset_id = asset.id, error = %err, "thumbnail generation failed; serving a retryable placeholder");
            thumbnail_placeholder_response(size)
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
    let (cache_path, source_path, source_version) = if is_qms {
        let version = vfs::qms_asset_version_key(&state, &asset).await?;
        (
            cache_dir.join(format!(
                "work-{work_id}-asset-{}-{size}-{version}.jpg",
                asset.id
            )),
            None,
            format!("asset:{}:qms:{version}", asset.id),
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
            format!("asset:{}:local:{}:{modified}", asset.id, source_meta.len()),
        )
    };

    if valid_thumbnail_cache_for_source(&cache_path, is_qms).await? {
        return stream_thumb_cache(cache_path, cache_control).await;
    }
    if size == 480 && state.derivatives.enabled() {
        let key = DerivativeKey::new("work-cover", work_id, "cover-480", source_version);
        let resources = state.resources.clone();
        let generation_state = state.clone();
        let generation_asset = asset.clone();
        let jpeg_downscale_enabled = state.config.jpeg_thumbnail_downscale_enabled;
        let generated = state
            .derivatives
            .get_or_generate(key, "image/jpeg", move |derivative_path| async move {
                if let Some(source_path) = source_path {
                    generate_local_derivative_thumbnail(
                        resources,
                        source_path,
                        derivative_path,
                        size,
                        jpeg_downscale_enabled,
                    )
                    .await
                } else {
                    generate_qms_derivative_thumbnail(
                        resources,
                        generation_state,
                        generation_asset,
                        derivative_path,
                        size,
                    )
                    .await
                }
            })
            .await;
        return match generated {
            Ok(Some(file)) => stream_derivative_cache(file, cache_control).await,
            Ok(None) => Err(AppError::Other(
                "derivative cache was disabled during generation".to_string(),
            )),
            Err(err) => {
                tracing::warn!(asset_id = asset.id, error = %err, "v2 cover generation failed; serving a retryable placeholder");
                thumbnail_placeholder_response(size)
            }
        };
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
                let jpeg_downscale_enabled = state.config.jpeg_thumbnail_downscale_enabled;
                run_blocking_thumbnail_generation(
                    state.resources.clone(),
                    write_guard,
                    reservation,
                    cache_path.clone(),
                    move |cache_path| {
                        generate_thumbnail_atomic(
                            &source_path,
                            cache_path,
                            size,
                            jpeg_downscale_enabled,
                        )
                    },
                )
                .await
            } else {
                run_qms_thumbnail_generation(
                    state.resources.clone(),
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
            tracing::warn!(asset_id = asset.id, error = %err, "cover thumbnail generation failed; serving a retryable placeholder");
            thumbnail_placeholder_response(size)
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
    if vfs::is_qms_strm_uri(&archive.path) {
        if let Some(response) =
            cached_remote_archive_cover(&state, work_id, &archive, &cache_dir, size, cache_control)
                .await?
        {
            return Ok(response);
        }
    }
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
    if size == 480 && state.derivatives.enabled() {
        let source_version = format!(
            "archive:{}:{}:{modified}:{}",
            archive.id,
            metadata.len(),
            short_hash(&page_name)
        );
        let key = DerivativeKey::new("work-cover", work_id, "cover-480", source_version);
        let resources = state.resources.clone();
        let generation_archive = cached.archive.clone();
        let generation_name = page_name.clone();
        let jpeg_downscale_enabled = state.config.jpeg_thumbnail_downscale_enabled;
        let generated = state
            .derivatives
            .get_or_generate(key, "image/jpeg", move |derivative_path| async move {
                generate_archive_derivative_thumbnail(
                    resources,
                    generation_archive,
                    generation_name,
                    derivative_path,
                    size,
                    jpeg_downscale_enabled,
                )
                .await
            })
            .await;
        return match generated {
            Ok(Some(file)) => stream_derivative_cache(file, cache_control).await,
            Ok(None) => Err(AppError::Other(
                "derivative cache was disabled during generation".to_string(),
            )),
            Err(err) => {
                tracing::warn!(asset_id = archive.id, error = %err, "v2 archive cover generation failed; serving a retryable placeholder");
                thumbnail_placeholder_response(size)
            }
        };
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
            let jpeg_downscale_enabled = state.config.jpeg_thumbnail_downscale_enabled;
            run_blocking_thumbnail_generation(
                state.resources.clone(),
                write_guard,
                reservation,
                cache_path.clone(),
                move |cache_path| {
                    generate_archive_thumbnail_atomic(
                        &cached_archive,
                        &stream_name,
                        cache_path,
                        size,
                        jpeg_downscale_enabled,
                    )
                },
            )
            .await
        }
        Err(err) => Err(err.to_string()),
    };
    match generated {
        Ok(()) => stream_thumb_cache(cache_path, cache_control).await,
        Err(err) => {
            tracing::warn!(asset_id = archive.id, error = %err, "archive cover thumbnail generation failed; serving a retryable placeholder");
            thumbnail_placeholder_response(size)
        }
    }
}

async fn cached_remote_archive_cover(
    state: &AppState,
    work_id: i64,
    archive: &Asset,
    cache_dir: &FsPath,
    size: u32,
    cache_control: &'static str,
) -> Result<Option<Response>> {
    let Some((target_url, manifest)) = remote_archive_manifest(state, archive).await? else {
        return Ok(None);
    };
    let mut entries = manifest
        .entries
        .iter()
        .filter(|entry| {
            !entry.directory && strm_source::classify(FsPath::new(&entry.name), None).is_image()
        })
        .collect::<Vec<_>>();
    entries.sort_by_cached_key(|entry| naturalish_key(&entry.name));
    let Some(entry) = entries.first() else {
        return Err(AppError::NotFound(
            "remote archive cover page not found".to_string(),
        ));
    };
    let entry_path = strm_cache::entry_cache_path(
        &state.config.data_dir,
        &archive.id.to_string(),
        &manifest.source_version,
        &entry.name,
    );
    let entry_path =
        prepare_remote_strm_entry(state, &target_url, &manifest, entry, &entry_path).await?;
    let cache_path = cache_dir.join(format!(
        "work-{work_id}-archive-{}-{size}-{}.jpg",
        archive.id,
        short_hash(&format!("{}:{}", manifest.source_version, entry.name))
    ));
    if valid_thumbnail_cache(&cache_path).await? {
        return Ok(Some(stream_thumb_cache(cache_path, cache_control).await?));
    }
    let write_lock = thumbnail_write_lock(&cache_path)?;
    let write_guard = write_lock.lock_owned().await;
    if valid_thumbnail_cache(&cache_path).await? {
        return Ok(Some(stream_thumb_cache(cache_path, cache_control).await?));
    }
    let generated = match reserve_thumbnail_cache_capacity(
        &cache_path,
        state.config.thumbnail_cache_max_bytes_per_dir,
    )
    .await
    {
        Ok(reservation) => {
            let jpeg_downscale_enabled = state.config.jpeg_thumbnail_downscale_enabled;
            run_blocking_thumbnail_generation(
                state.resources.clone(),
                write_guard,
                reservation,
                cache_path.clone(),
                move |cache_path| {
                    generate_thumbnail_atomic(&entry_path, cache_path, size, jpeg_downscale_enabled)
                },
            )
            .await
            .map_err(|error| AppError::Other(error.to_string()))
        }
        Err(error) => Err(error),
    };
    match generated {
        Ok(()) => Ok(Some(stream_thumb_cache(cache_path, cache_control).await?)),
        Err(error) => Err(error),
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

fn reader_size_bucket(requested: Option<u32>) -> Option<u32> {
    let requested = requested?;
    if requested == 0 {
        return None;
    }
    let requested = requested.clamp(READER_SIZE_BUCKETS[0], *READER_SIZE_BUCKETS.last()?);
    Some(
        READER_SIZE_BUCKETS
            .into_iter()
            .find(|size| *size >= requested)
            .unwrap_or(*READER_SIZE_BUCKETS.last().unwrap()),
    )
}

fn archive_page_source_version(
    archive: &Asset,
    cached: &CachedComicPages,
    entry_name: &str,
) -> String {
    let modified = cached
        .modified
        .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
        .map(|value| value.as_nanos().to_string())
        .unwrap_or_else(|| "0".to_string());
    let material = format!(
        "reader-page-v1|{}|{}|{}|{}|{}",
        archive.id, archive.path, archive.meta_json, cached.size, modified
    );
    let mut hasher = Sha256::new();
    hasher.update(material.as_bytes());
    hasher.update([0]);
    hasher.update(entry_name.as_bytes());
    format!("reader-page-v1:{:x}", hasher.finalize())
}

fn generate_thumbnail_atomic(
    source_path: &FsPath,
    cache_path: &FsPath,
    size: u32,
    jpeg_downscale_enabled: bool,
) -> std::result::Result<(), String> {
    if jpeg_downscale_enabled && is_jpeg_path(source_path) {
        match decode_jpeg_thumbnail_image_from_path(source_path, size) {
            Ok(image) => return publish_thumbnail(image, cache_path, size),
            Err(error) => {
                tracing::debug!(
                    path = %source_path.display(),
                    error = %error,
                    "native JPEG thumbnail decode failed; falling back to image crate"
                );
            }
        }
    }
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
    jpeg_downscale_enabled: bool,
) -> std::result::Result<(), String> {
    if jpeg_downscale_enabled && looks_like_jpeg(bytes) {
        match decode_jpeg_thumbnail_image_from_bytes(bytes, size) {
            Ok(image) => return publish_thumbnail(image, cache_path, size),
            Err(error) => {
                tracing::debug!(
                    error = %error,
                    "native JPEG thumbnail decode failed; falling back to image crate"
                );
            }
        }
    }
    let mut reader = image::ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|e| e.to_string())?;
    reader.limits(image_decode_limits());
    let image = reader.decode().map_err(|e| e.to_string())?;
    publish_thumbnail(image, cache_path, size)
}

const JPEG_DCT_SCALE_NUMERATORS: [u8; 4] = [1, 2, 4, 8];

fn is_jpeg_path(path: &FsPath) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "jpg" | "jpeg" | "jpe"
            )
        })
}

fn looks_like_jpeg(bytes: &[u8]) -> bool {
    bytes.len() >= 2 && bytes[0] == 0xff && bytes[1] == 0xd8
}

fn jpeg_dct_scale_for_target(width: usize, height: usize, target: u32) -> u8 {
    let longest = width.max(height) as u128;
    let target = target as u128;
    JPEG_DCT_SCALE_NUMERATORS
        .into_iter()
        .find(|numerator| {
            longest.saturating_mul(*numerator as u128).saturating_add(7) / 8 >= target
        })
        .unwrap_or(8)
}

fn jpeg_rgb_buffer_size(width: usize, height: usize) -> std::result::Result<usize, String> {
    width
        .checked_mul(height)
        .and_then(|pixels| pixels.checked_mul(3))
        .ok_or_else(|| "JPEG RGB buffer size overflowed".to_string())
}

fn decode_jpeg_thumbnail_image_from_path(
    source_path: &FsPath,
    size: u32,
) -> std::result::Result<image::DynamicImage, String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let file = File::open(source_path).map_err(|error| error.to_string())?;
        let decoder = Decompress::builder()
            .from_reader(BufReader::new(file))
            .map_err(|error| error.to_string())?;
        decode_jpeg_thumbnail_image(decoder, size)
    }))
    .map_err(|_| "native JPEG decoder panicked".to_string())?
}

fn decode_jpeg_thumbnail_image_from_bytes(
    bytes: &[u8],
    size: u32,
) -> std::result::Result<image::DynamicImage, String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let decoder = Decompress::builder()
            .from_mem(bytes)
            .map_err(|error| error.to_string())?;
        decode_jpeg_thumbnail_image(decoder, size)
    }))
    .map_err(|_| "native JPEG decoder panicked".to_string())?
}

fn decode_archive_jpeg_thumbnail_image(
    pool: &ComicArchivePool,
    name: &str,
    size: u32,
) -> std::result::Result<image::DynamicImage, String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        decode_archive_jpeg_thumbnail_image_unchecked(pool, name, size)
    }))
    .map_err(|_| "native JPEG decoder panicked".to_string())?
}

fn decode_archive_jpeg_thumbnail_image_unchecked(
    pool: &ComicArchivePool,
    name: &str,
    size: u32,
) -> std::result::Result<image::DynamicImage, String> {
    let count = pool.archives.len();
    if count == 0 {
        return Err("comic archive cache has no readable handles".to_string());
    }
    let start = pool.next.fetch_add(1, Ordering::Relaxed) % count;
    let mut fallback = None;
    for offset in 0..count {
        let index = (start + offset) % count;
        match pool.archives[index].try_lock() {
            Ok(mut archive) => {
                return decode_archive_jpeg_thumbnail_from_locked_archive(&mut archive, name, size);
            }
            Err(TryLockError::WouldBlock) => fallback.get_or_insert(index),
            Err(TryLockError::Poisoned(_)) => continue,
        };
    }
    let index = fallback.ok_or_else(|| "comic archive cache handles are poisoned".to_string())?;
    let mut archive = pool.archives[index]
        .lock()
        .map_err(|_| "comic archive cache lock poisoned".to_string())?;
    decode_archive_jpeg_thumbnail_from_locked_archive(&mut archive, name, size)
}

struct SharedZipEntryReader<'a> {
    entry: Rc<RefCell<zip::read::ZipFile<'a>>>,
}

impl Read for SharedZipEntryReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.entry
            .try_borrow_mut()
            .map_err(|_| std::io::Error::other("ZIP entry reader is already borrowed"))?
            .read(buffer)
    }
}

fn decode_archive_jpeg_thumbnail_from_locked_archive(
    archive: &mut ZipArchive<File>,
    name: &str,
    size: u32,
) -> std::result::Result<image::DynamicImage, String> {
    let entry = Rc::new(RefCell::new(
        archive.by_name(name).map_err(|error| error.to_string())?,
    ));
    let reader = SharedZipEntryReader {
        entry: entry.clone(),
    };
    let decoder = Decompress::builder()
        .from_reader(BufReader::new(reader))
        .map_err(|error| error.to_string())?;
    let image = decode_jpeg_thumbnail_image(decoder, size)?;
    std::io::copy(
        &mut *entry
            .try_borrow_mut()
            .map_err(|_| "ZIP entry reader is already borrowed".to_string())?,
        &mut std::io::sink(),
    )
    .map_err(|error| format!("ZIP entry CRC verification failed: {error}"))?;
    Ok(image)
}

fn generate_archive_thumbnail_atomic(
    pool: &ComicArchivePool,
    name: &str,
    cache_path: &FsPath,
    size: u32,
    jpeg_downscale_enabled: bool,
) -> std::result::Result<(), String> {
    let jpeg_entry = is_jpeg_path(FsPath::new(name));
    if jpeg_downscale_enabled && jpeg_entry {
        match decode_archive_jpeg_thumbnail_image(pool, name, size) {
            Ok(image) => return publish_thumbnail(image, cache_path, size),
            Err(error) => {
                tracing::debug!(
                    entry = %name,
                    error = %error,
                    "streaming archive JPEG thumbnail decode failed; falling back to buffered image decode"
                );
            }
        }
    }
    let bytes = cbz_named_page_bytes(pool, name).map_err(|error| error.to_string())?;
    generate_thumbnail_from_bytes_atomic(
        &bytes,
        cache_path,
        size,
        jpeg_downscale_enabled && !jpeg_entry,
    )
}

fn decode_jpeg_thumbnail_image<R: std::io::BufRead>(
    mut decoder: Decompress<R>,
    size: u32,
) -> std::result::Result<image::DynamicImage, String> {
    let scale = jpeg_dct_scale_for_target(decoder.width(), decoder.height(), size);
    decoder.scale(scale);
    decoder.dct_method(DctMethod::IntegerFast);
    let mut decoder = decoder.rgb().map_err(|error| error.to_string())?;
    let width = decoder.width();
    let height = decoder.height();
    let rgb_bytes = jpeg_rgb_buffer_size(width, height)?;
    if rgb_bytes as u64 > MAX_IMAGE_DECODE_ALLOC_BYTES {
        return Err(format!(
            "JPEG RGB buffer {rgb_bytes} bytes exceeds {MAX_IMAGE_DECODE_ALLOC_BYTES} byte limit"
        ));
    }
    let pixels = decoder
        .read_scanlines::<u8>()
        .map_err(|error| error.to_string())?;
    if pixels.len() != rgb_bytes {
        return Err(format!(
            "JPEG decoder returned {} bytes, expected {rgb_bytes}",
            pixels.len()
        ));
    }
    let width = u32::try_from(width).map_err(|_| "JPEG width exceeds u32".to_string())?;
    let height = u32::try_from(height).map_err(|_| "JPEG height exceeds u32".to_string())?;
    let image = image::RgbImage::from_raw(width, height, pixels)
        .ok_or_else(|| "JPEG RGB buffer dimensions are invalid".to_string())?;
    Ok(image::DynamicImage::ImageRgb8(image))
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
            || state
                .last_scan
                .is_none_or(|last_scan| last_scan.elapsed() >= CACHE_USAGE_RESCAN_INTERVAL)
            || (state.file_count <= CACHE_USAGE_MTIME_RESCAN_MAX_FILES
                && state.observed_dir_modified != dir_modified)
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
    let current_usage = tokio::task::spawn_blocking(move || thumbnail_cache_snapshot(&usage_dir))
        .await
        .map_err(|err| AppError::Other(format!("thumbnail cache usage task failed: {err}")))??;
    let mut quotas = THUMBNAIL_CACHE_QUOTAS
        .lock()
        .map_err(|_| AppError::Other("thumbnail cache quota registry poisoned".to_string()))?;
    let state = quotas.entry(cache_dir).or_default();
    if !state.initialized || state.generation == scan_generation {
        state.committed = current_usage.bytes;
    } else {
        state.committed = state.committed.max(current_usage.bytes);
    }
    state.file_count = current_usage.files;
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

#[cfg(test)]
fn thumbnail_cache_usage(cache_dir: &FsPath) -> std::io::Result<u64> {
    Ok(thumbnail_cache_snapshot(cache_dir)?.bytes)
}

fn thumbnail_cache_snapshot(cache_dir: &FsPath) -> std::io::Result<ThumbnailCacheUsage> {
    let mut usage = ThumbnailCacheUsage::default();
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
        usage.bytes = usage.bytes.saturating_add(metadata.len());
        usage.files = usage.files.saturating_add(1);
    }
    Ok(usage)
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

async fn stream_derivative_cache(
    derivative: DerivativeFile,
    cache_control: &'static str,
) -> Result<Response> {
    let file = tokio::fs::File::open(&derivative.path).await?;
    let stream = ReaderStream::with_capacity(file, STREAM_BUFFER_SIZE);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, derivative.mime)
        .header(header::CACHE_CONTROL, cache_control)
        .header(header::CONTENT_LENGTH, derivative.bytes.to_string())
        .header("x-arislist-derivative-cache", "v2")
        .body(Body::from_stream(stream))
        .map_err(|e| AppError::Other(e.to_string()))
}

fn thumbnail_placeholder_response(size: u32) -> Result<Response> {
    let size = size.clamp(96, 960);
    let svg = format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" viewBox="0 0 96 96" role="img" aria-label="preview pending"><rect width="96" height="96" rx="12" fill="#e8e4dc"/><path d="M25 67 41 49l10 11 9-9 12 16H25Z" fill="#a8a196"/><circle cx="34" cy="33" r="7" fill="#bdb5a8"/><path d="M30 78h36" stroke="#8f877b" stroke-width="4" stroke-linecap="round"/></svg>"##
    );
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "image/svg+xml; charset=utf-8")
        .header(header::CACHE_CONTROL, DERIVATIVE_PLACEHOLDER_CACHE)
        .header("x-arislist-derivative-status", "placeholder")
        .header(header::CONTENT_LENGTH, svg.len().to_string())
        .body(Body::from(svg))
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
    /// Total number of image entries in the archive.  `pages` is a bounded
    /// slice when the caller supplies a manifest cursor/limit.
    pub total: usize,
    pub next_cursor: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ComicPageInfo {
    pub name: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

async fn remote_archive_manifest(
    state: &AppState,
    archive: &Asset,
) -> Result<Option<(String, strm_archive::RemoteArchiveManifest)>> {
    if !vfs::is_qms_strm_uri(&archive.path) {
        return Ok(None);
    }
    let target_url = vfs::qms_target_url_for_asset(state, archive).await?;
    let manifest = match strm_archive::cached_manifest(&target_url) {
        Some(manifest) => manifest,
        None => {
            if let Some(manifest) =
                strm_archive::load_persisted_manifest(&state.db, &target_url).await?
            {
                strm_archive::remember_manifest(&target_url, manifest.clone());
                manifest
            } else {
                match strm_archive::probe_manifest(&target_url).await? {
                    strm_archive::ManifestProbe::Ready(manifest) => {
                        strm_archive::remember_manifest(&target_url, manifest.clone());
                        let cache_limit =
                            state.resources.snapshot().archive_manifest_cache_bytes / 2;
                        if let Err(error) = strm_archive::persist_manifest(
                            &state.db,
                            &target_url,
                            &manifest,
                            cache_limit,
                        )
                        .await
                        {
                            tracing::debug!(
                                error = %error,
                                "failed to persist remote STRM archive manifest"
                            );
                        }
                        manifest
                    }
                    strm_archive::ManifestProbe::RequiresFullCache { .. } => return Ok(None),
                }
            }
        }
    };
    if !manifest.range_supported {
        return Ok(None);
    }
    Ok(Some((target_url, manifest)))
}

fn remote_comic_pages(manifest: &strm_archive::RemoteArchiveManifest) -> Vec<ComicPageInfo> {
    let mut names = manifest
        .entries
        .iter()
        .filter(|entry| {
            !entry.directory && strm_source::classify(FsPath::new(&entry.name), None).is_image()
        })
        .map(|entry| entry.name.clone())
        .collect::<Vec<_>>();
    names.sort_by_cached_key(|name| naturalish_key(name));
    names
        .into_iter()
        .map(|name| ComicPageInfo {
            name,
            width: None,
            height: None,
        })
        .collect()
}

#[derive(Debug, Deserialize)]
pub struct ComicPagesQuery {
    pub cursor: Option<usize>,
    pub limit: Option<usize>,
    pub v: Option<String>,
}

pub async fn comic_pages(
    State(state): State<Arc<AppState>>,
    Path(work_id): Path<i64>,
    Query(query): Query<ComicPagesQuery>,
) -> Result<Response> {
    let crate::db::WorkArchiveSource {
        archive,
        kind,
        meta_json,
    } = state.db.work_archive_and_meta(work_id).await?;
    let pages = if let Some((_, manifest)) = remote_archive_manifest(&state, &archive).await? {
        Arc::new(remote_comic_pages(&manifest))
    } else {
        let path = vfs::asset_local_processing_path(&state, &archive).await?;
        cached_cbz_page_manifest(&state, &path).await?
    };
    if let Err(err) =
        maybe_update_comic_page_count(&state, work_id, &kind, &meta_json, pages.len()).await
    {
        tracing::warn!(
            work_id,
            page_count = pages.len(),
            error = %err,
            "failed to persist archive page count"
        );
    }
    let remote = vfs::is_qms_strm_uri(&archive.path);
    let total = pages.len();
    // Keep small no-query responses compatible for older clients.  Once a
    // manifest exceeds the bounded page size, even an accidental no-query
    // request must stay paginated; large clients continue with next_cursor.
    let bounded =
        query.cursor.is_some() || query.limit.is_some() || total > COMIC_MANIFEST_MAX_PAGE_SIZE;
    let start = query.cursor.unwrap_or(0).min(total);
    let limit = if bounded {
        query
            .limit
            .unwrap_or(COMIC_MANIFEST_PAGE_SIZE)
            .clamp(1, COMIC_MANIFEST_MAX_PAGE_SIZE)
    } else {
        total
    };
    let end = start.saturating_add(limit).min(total);
    let pages = pages[start..end].to_vec();
    let mut response = Json(ComicPagesResponse {
        pages,
        total,
        next_cursor: (end < total).then_some(end),
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
    work_id: i64,
    kind: &str,
    meta_json: &str,
    page_count: usize,
) -> Result<()> {
    if !matches!(kind, "comic" | "coser-picture") || page_count == 0 {
        return Ok(());
    }
    let mut meta =
        serde_json::from_str::<serde_json::Value>(meta_json).unwrap_or_else(|_| json!({}));
    let current = meta
        .get("page_count")
        .and_then(|value| value.as_i64())
        .unwrap_or(0);
    if current == page_count as i64 {
        return Ok(());
    }
    if let Some(object) = meta.as_object_mut() {
        object.insert("page_count".to_string(), json!(page_count as i64));
        state.db.update_work_meta(work_id, meta).await?;
    }
    Ok(())
}

fn estimated_archive_cache_weight(
    archive_size: u64,
    central_directory_start: u64,
    archive_handles: usize,
    archive_entries: usize,
    retained_string_bytes: usize,
) -> u64 {
    let handles = archive_handles.max(1) as u64;
    let central_tail = archive_size.saturating_sub(central_directory_start);
    central_tail
        .saturating_mul(ARCHIVE_CACHE_WEIGHT_MULTIPLIER)
        .saturating_mul(handles)
        .saturating_add(
            (archive_entries as u64)
                .saturating_mul(ARCHIVE_CACHE_ENTRY_OVERHEAD_BYTES)
                .saturating_mul(handles),
        )
        .saturating_add(retained_string_bytes as u64)
        .max(1)
}

fn archive_manifest_cache_limit_per_kind(resources: &ResourceGovernor) -> u64 {
    (resources.limits().archive_manifest_cache_bytes / 2).max(1)
}

fn insert_comic_cache_entry(
    cache: &mut ComicPageCacheState,
    key: String,
    cached: CachedComicPages,
    cache_limit: u64,
) -> bool {
    if cached.weight_bytes > cache_limit {
        return false;
    }
    if let Some(replaced) = cache.entries.remove(&key) {
        cache.total_weight_bytes = cache
            .total_weight_bytes
            .saturating_sub(replaced.weight_bytes);
    }
    remove_lru_key(&mut cache.lru, &key);
    while cache.entries.len() >= COMIC_PAGE_CACHE_LIMIT
        || (!cache.entries.is_empty()
            && cache.total_weight_bytes.saturating_add(cached.weight_bytes) > cache_limit)
    {
        let Some(evicted) = cache.lru.pop_front() else {
            break;
        };
        if let Some(removed) = cache.entries.remove(&evicted) {
            cache.total_weight_bytes = cache
                .total_weight_bytes
                .saturating_sub(removed.weight_bytes);
        }
    }
    cache.total_weight_bytes = cache.total_weight_bytes.saturating_add(cached.weight_bytes);
    cache.entries.insert(key.clone(), cached);
    touch_lru(&mut cache.lru, &key);
    true
}

fn comic_manifest_cache_key(source_path: &FsPath) -> String {
    let mut hasher = Sha256::new();
    hasher.update(source_path.to_string_lossy().as_bytes());
    format!("comic:{:x}", hasher.finalize())
}

fn comic_manifest_modified_key(modified: Option<SystemTime>) -> Option<String> {
    modified
        .and_then(system_time_key)
        .map(|value| value.to_string())
}

fn valid_comic_manifest_pages(pages: &[String]) -> bool {
    pages.len() <= crate::archive::MAX_ARCHIVE_ENTRIES as usize
        && pages
            .iter()
            .all(|name| !name.is_empty() && image_name(name))
}

async fn load_comic_manifest_cache(
    db: &crate::db::Db,
    source_path: &FsPath,
    size: u64,
    modified: Option<SystemTime>,
) -> Result<Option<Vec<String>>> {
    let row = sqlx::query(
        r#"
        SELECT source_size, source_modified_nanos, pages_json, bytes
        FROM archive_manifest_cache
        WHERE cache_key = ?1
        "#,
    )
    .bind(comic_manifest_cache_key(source_path))
    .fetch_optional(db.pool())
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let source_size: i64 = sqlx::Row::get(&row, "source_size");
    let source_modified_nanos: Option<String> = sqlx::Row::get(&row, "source_modified_nanos");
    let pages_json: String = sqlx::Row::get(&row, "pages_json");
    let bytes: i64 = sqlx::Row::get(&row, "bytes");
    if source_size < 0
        || source_size as u64 != size
        || source_modified_nanos != comic_manifest_modified_key(modified)
        || bytes < 0
        || usize::try_from(bytes).unwrap_or(usize::MAX) > COMIC_MANIFEST_CACHE_ENTRY_MAX_BYTES
    {
        return Ok(None);
    }
    let pages = match serde_json::from_str::<Vec<String>>(&pages_json) {
        Ok(pages) if valid_comic_manifest_pages(&pages) => pages,
        Ok(_) => return Ok(None),
        Err(error) => {
            tracing::debug!(
                source = %source_path.display(),
                error = %error,
                "ignoring malformed comic manifest cache row"
            );
            return Ok(None);
        }
    };
    Ok(Some(pages))
}

async fn persist_comic_manifest_cache(
    db: &crate::db::Db,
    source_path: &FsPath,
    cached: &CachedComicPages,
    cache_limit: u64,
) -> Result<bool> {
    let pages = cached
        .pages
        .iter()
        .map(|page| page.name.clone())
        .collect::<Vec<_>>();
    let pages_json = match serde_json::to_string(&pages) {
        Ok(bytes) => bytes,
        Err(error) => {
            tracing::warn!(
                path = %source_path.display(),
                error = %error,
                "failed to serialize comic manifest cache"
            );
            return Ok(false);
        }
    };
    if pages_json.len() > COMIC_MANIFEST_CACHE_ENTRY_MAX_BYTES {
        tracing::debug!(
            path = %source_path.display(),
            bytes = pages_json.len(),
            limit = COMIC_MANIFEST_CACHE_ENTRY_MAX_BYTES,
            "comic manifest is too large for the persistent cache entry budget"
        );
        return Ok(false);
    }
    let bytes = u64::try_from(pages_json.len()).unwrap_or(u64::MAX);
    let _write_slot = db
        .acquire_write_slot(bytes.saturating_add(16 * 1024))
        .await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    sqlx::query(
        r#"
        INSERT INTO archive_manifest_cache (
            cache_key, source_size, source_modified_nanos, pages_json, bytes, created_at
        )
        VALUES (?1, ?2, ?3, ?4, ?5, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        ON CONFLICT(cache_key) DO UPDATE SET
            source_size = excluded.source_size,
            source_modified_nanos = excluded.source_modified_nanos,
            pages_json = excluded.pages_json,
            bytes = excluded.bytes,
            created_at = excluded.created_at
        "#,
    )
    .bind(comic_manifest_cache_key(source_path))
    .bind(i64::try_from(cached.size).unwrap_or(i64::MAX))
    .bind(comic_manifest_modified_key(cached.modified))
    .bind(&pages_json)
    .bind(i64::try_from(bytes).unwrap_or(i64::MAX))
    .execute(&mut *transaction)
    .await?;

    loop {
        let resident_bytes: i64 = sqlx::query_scalar(
            "SELECT resident_bytes FROM archive_manifest_cache_state WHERE singleton = 1",
        )
        .fetch_one(&mut *transaction)
        .await?;
        if resident_bytes <= i64::try_from(cache_limit).unwrap_or(i64::MAX) {
            break;
        }
        let oldest: Option<String> = sqlx::query_scalar(
            "SELECT cache_key FROM archive_manifest_cache ORDER BY created_at, cache_key LIMIT 1",
        )
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(oldest) = oldest else {
            break;
        };
        sqlx::query("DELETE FROM archive_manifest_cache WHERE cache_key = ?1")
            .bind(oldest)
            .execute(&mut *transaction)
            .await?;
    }
    transaction.commit().await?;
    Ok(true)
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
    open_cbz_pages_with_archive(path, size, modified, archive, names)
}

fn open_cbz_pages_with_names(path: &FsPath, names: Vec<String>) -> Result<CachedComicPages> {
    if !valid_comic_manifest_pages(&names) {
        return Err(AppError::BadRequest(
            "comic manifest contains invalid page names".to_string(),
        ));
    }
    let metadata = std::fs::metadata(path)?;
    let size = metadata.len();
    let modified = metadata.modified().ok();
    let file = File::open(path)?;
    let archive = crate::archive::open_media_zip(file, "comic archive")?;
    open_cbz_pages_with_archive(path, size, modified, archive, names)
}

fn open_cbz_pages_with_archive(
    path: &FsPath,
    size: u64,
    modified: Option<SystemTime>,
    archive: ZipArchive<File>,
    names: Vec<String>,
) -> Result<CachedComicPages> {
    let central_directory_start = archive.central_directory_start();
    let archive_entries = archive.len();
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
    let retained_string_bytes = pages.iter().map(|page| page.name.len()).sum::<usize>();
    let weight_bytes = estimated_archive_cache_weight(
        size,
        central_directory_start,
        archives.len(),
        archive_entries,
        retained_string_bytes,
    );
    Ok(CachedComicPages {
        size,
        modified,
        weight_bytes,
        pages: Arc::new(pages),
        archive: Arc::new(ComicArchivePool {
            archives,
            next: AtomicUsize::new(0),
        }),
    })
}

/// Return only the page manifest for the manifest endpoint.  A valid
/// a valid persistent cache row is enough for this route; opening ZIP handles is left to
/// cover/page streaming, so a restart does not pay the archive-pool cost just
/// to render the bounded page list.
async fn cached_cbz_page_manifest(
    state: &AppState,
    path: &FsPath,
) -> Result<Arc<Vec<ComicPageInfo>>> {
    let metadata = tokio::fs::metadata(path).await?;
    let size = metadata.len();
    let modified = metadata.modified().ok();
    let key = path.to_string_lossy().to_string();

    {
        let mut cache = state.comic_page_cache.state.lock().await;
        if let Some(entry) = cache.entries.get(&key).cloned() {
            if entry.size == size && entry.modified == modified {
                touch_lru(&mut cache.lru, &key);
                return Ok(entry.pages);
            }
        }
    }

    if let Some(names) = load_comic_manifest_cache(&state.db, path, size, modified).await? {
        let pages = names
            .into_iter()
            .map(|name| ComicPageInfo {
                name,
                width: None,
                height: None,
            })
            .collect();
        return Ok(Arc::new(pages));
    }

    Ok(cached_cbz_pages(state, path).await?.pages)
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
            if let Some(removed) = cache.entries.remove(&key) {
                cache.total_weight_bytes = cache
                    .total_weight_bytes
                    .saturating_sub(removed.weight_bytes);
            }
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
            if let Some(removed) = cache.entries.remove(&key) {
                cache.total_weight_bytes = cache
                    .total_weight_bytes
                    .saturating_sub(removed.weight_bytes);
            }
            remove_lru_key(&mut cache.lru, &key);
        }
    }

    let path = path.to_path_buf();
    let persisted_names = load_comic_manifest_cache(&state.db, &path, size, modified).await?;
    let used_persisted_manifest = persisted_names.is_some();
    let open_path = path.clone();
    let cached = archive_blocking(
        &state.resources,
        crate::archive::MAX_CENTRAL_DIRECTORY_BYTES
            .saturating_mul(COMIC_ARCHIVE_POOL_SIZE as u64)
            .saturating_mul(2),
        move || match persisted_names {
            Some(names) => open_cbz_pages_with_names(&open_path, names),
            None => open_cbz_pages(&open_path),
        },
    )
    .await?;
    if !used_persisted_manifest {
        let _ = persist_comic_manifest_cache(
            &state.db,
            &path,
            &cached,
            archive_manifest_cache_limit_per_kind(&state.resources),
        )
        .await;
    }
    let mut cache = state.comic_page_cache.state.lock().await;
    let cache_limit = archive_manifest_cache_limit_per_kind(&state.resources);
    if !insert_comic_cache_entry(&mut cache, key.clone(), cached.clone(), cache_limit) {
        tracing::warn!(
            path = %key,
            estimated_bytes = cached.weight_bytes,
            cache_limit_bytes = cache_limit,
            "comic archive manifest exceeds its byte cache budget; serving without retaining it"
        );
    }
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
    let declared_size = entry.size();
    read_limited_zip_entry_sized(
        &mut entry,
        declared_size,
        MAX_COMIC_PAGE_BYTES,
        "comic page",
    )
}

/// Decompress a CBZ entry into a bounded async channel instead of first
/// materializing the complete page.  The blocking worker owns the archive and
/// processing lease until the reader reaches EOF (or the client disconnects),
/// while the response retains only a small inflight chunk budget.
async fn stream_cbz_page_chunks(
    resources: &ResourceGovernor,
    pool: Arc<ComicArchivePool>,
    name: String,
) -> Result<(
    u64,
    ResourceBufferLease,
    mpsc::Receiver<std::io::Result<Bytes>>,
)> {
    let lease = resources
        .reserve(
            ResourceClass::ArchiveStream,
            MAX_COMIC_PAGE_BYTES,
            (STREAM_BUFFER_SIZE * COMIC_STREAM_QUEUE_CHUNKS) as u64,
        )
        .await?;
    let (work_lease, buffer_lease) = lease.split();
    let (ready_tx, ready_rx) = oneshot::channel::<Result<u64>>();
    let (chunk_tx, chunk_rx) = mpsc::channel(COMIC_STREAM_QUEUE_CHUNKS);

    tokio::task::spawn_blocking(move || {
        let _work_lease = work_lease;
        let mut ready_tx = Some(ready_tx);
        let result = (|| -> Result<()> {
            let count = pool.archives.len();
            if count == 0 {
                return Err(AppError::Other(
                    "comic archive cache has no readable handles".to_string(),
                ));
            }
            let start = pool.next.fetch_add(1, Ordering::Relaxed) % count;
            let mut fallback = None;
            let mut archive_guard = None;
            for offset in 0..count {
                let index = (start + offset) % count;
                match pool.archives[index].try_lock() {
                    Ok(guard) => {
                        archive_guard = Some(guard);
                        break;
                    }
                    Err(TryLockError::WouldBlock) => fallback.get_or_insert(index),
                    Err(TryLockError::Poisoned(_)) => continue,
                };
            }
            let mut archive_guard = if let Some(guard) = archive_guard {
                guard
            } else {
                let index = fallback.ok_or_else(|| {
                    AppError::Other("comic archive cache handles are poisoned".to_string())
                })?;
                pool.archives[index]
                    .lock()
                    .map_err(|_| AppError::Other("comic archive cache lock poisoned".to_string()))?
            };
            let mut entry = archive_guard
                .by_name(&name)
                .map_err(|err| AppError::Other(err.to_string()))?;
            let size = entry.size();
            if size > MAX_COMIC_PAGE_BYTES {
                return Err(AppError::BadRequest(format!(
                    "comic page exceeds {MAX_COMIC_PAGE_BYTES} decompressed bytes"
                )));
            }
            if let Some(sender) = ready_tx.take() {
                let _ = sender.send(Ok(size));
            }
            let mut buffer = vec![0_u8; STREAM_BUFFER_SIZE];
            let mut emitted = 0_u64;
            loop {
                let read = entry
                    .read(&mut buffer)
                    .map_err(|err| AppError::Other(err.to_string()))?;
                if read == 0 {
                    break;
                }
                emitted = emitted.saturating_add(read as u64);
                if emitted > MAX_COMIC_PAGE_BYTES {
                    return Err(AppError::BadRequest(format!(
                        "comic page exceeds {MAX_COMIC_PAGE_BYTES} decompressed bytes"
                    )));
                }
                if chunk_tx
                    .blocking_send(Ok(Bytes::copy_from_slice(&buffer[..read])))
                    .is_err()
                {
                    // The client disconnected.  Dropping the worker-owned
                    // leases here is the cancellation boundary.
                    break;
                }
            }
            Ok(())
        })();
        if let Err(err) = result {
            if let Some(sender) = ready_tx.take() {
                let _ = sender.send(Err(err));
            } else {
                let _ = chunk_tx.blocking_send(Err(std::io::Error::other(err.to_string())));
            }
        }
    });

    let size = ready_rx
        .await
        .map_err(|_| AppError::Other("comic page stream worker exited early".to_string()))??;
    Ok((size, buffer_lease, chunk_rx))
}

pub async fn stream_comic_page(
    State(state): State<Arc<AppState>>,
    Path((work_id, page)): Path<(i64, usize)>,
    Query(query): Query<ComicPageQuery>,
) -> Result<Response> {
    let archive = state
        .db
        .work_asset_by_role(work_id, "archive", None)
        .await?;
    let remote_archive = vfs::is_qms_strm_uri(&archive.path);
    if remote_archive {
        if let Some(response) = stream_remote_comic_page(&state, &archive, page).await? {
            return Ok(response);
        }
    }
    let path = vfs::asset_local_processing_path(&state, &archive).await?;
    let cached = cached_cbz_pages(&state, &path).await?;
    let name = cached
        .pages
        .get(page)
        .ok_or_else(|| AppError::NotFound(format!("page {page} not found")))?
        .name
        .clone();
    let cached_archive = cached.archive.clone();
    let requested_size = reader_size_bucket(query.size);
    let cache_control = if remote_archive {
        MEDIA_NO_CACHE
    } else {
        media_cache_control(query.v.as_deref())
    };
    let mut queued_derivative: Option<(
        DerivativeKey,
        ResourceGovernor,
        Arc<ComicArchivePool>,
        String,
        u32,
        bool,
    )> = None;
    if let Some(size) = requested_size.filter(|_| state.derivatives.enabled()) {
        let key = DerivativeKey::new(
            "archive-page",
            archive.id,
            format!("reader-{size}-{}", short_hash(&name)),
            archive_page_source_version(&archive, &cached, &name),
        );
        match state.derivatives.lookup_ready(&key).await {
            Ok(Some(file)) => return stream_derivative_cache(file, cache_control).await,
            Ok(None) => {
                // Start the source stream first.  The ZIP worker acquires an
                // archive handle before returning, so a background decoder
                // cannot monopolize the only handle needed for the first
                // interactive page response.
                queued_derivative = Some((
                    key,
                    state.resources.clone(),
                    cached_archive.clone(),
                    name.clone(),
                    size,
                    state.config.jpeg_thumbnail_downscale_enabled,
                ));
            }
            Err(error) => tracing::debug!(
                work_id,
                page,
                size,
                error = %error,
                "reader derivative lookup unavailable; serving original page"
            ),
        }
    }
    let stream_name = name.clone();
    let (content_length, lease, mut chunks) =
        stream_cbz_page_chunks(&state.resources, cached_archive, stream_name).await?;
    if let Some((
        key,
        resources,
        generation_archive,
        generation_name,
        size,
        jpeg_downscale_enabled,
    )) = queued_derivative
    {
        if let Err(error) = state
            .derivatives
            .queue_generation(key, "image/jpeg", move |derivative_path| async move {
                generate_archive_derivative_thumbnail(
                    resources,
                    generation_archive,
                    generation_name,
                    derivative_path,
                    size,
                    jpeg_downscale_enabled,
                )
                .await
            })
            .await
        {
            tracing::debug!(
                work_id,
                page,
                size,
                error = %error,
                "reader derivative queue unavailable; serving original page"
            );
        }
    }
    let mime = path_mime(std::path::Path::new(&name));
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime)
        .header(header::CACHE_CONTROL, cache_control)
        .header(header::CONTENT_LENGTH, content_length.to_string())
        .body(Body::from_stream(async_stream::stream! {
            let _lease = lease;
            while let Some(chunk) = chunks.recv().await {
                yield chunk;
            }
        }))
        .map_err(|err| AppError::Other(err.to_string()))
}

async fn stream_remote_comic_page(
    state: &AppState,
    archive: &Asset,
    page: usize,
) -> Result<Option<Response>> {
    let Some((target_url, manifest)) = remote_archive_manifest(state, archive).await? else {
        return Ok(None);
    };
    let mut entries = manifest
        .entries
        .iter()
        .filter(|entry| {
            !entry.directory && strm_source::classify(FsPath::new(&entry.name), None).is_image()
        })
        .collect::<Vec<_>>();
    entries.sort_by_cached_key(|entry| naturalish_key(&entry.name));
    let Some(entry) = entries.get(page) else {
        return Err(AppError::NotFound(format!("page {page} not found")));
    };
    let cache_path = strm_cache::entry_cache_path(
        &state.config.data_dir,
        &archive.id.to_string(),
        &manifest.source_version,
        &entry.name,
    );
    let page_directory = state
        .config
        .data_dir
        .join("cloud-cache")
        .join("reader-pages");
    let cache_path = page_directory.join(
        cache_path
            .file_name()
            .ok_or_else(|| AppError::Other("invalid comic page cache path".to_string()))?,
    );
    let mime = path_mime(FsPath::new(&entry.name));
    // Warm pages must not wait behind an unrelated prefetch download.
    if strm_cache::touch_page(&cache_path).await.is_ok() {
        if let Ok(response) =
            stream_local_file(state, &cache_path, &mime, &HeaderMap::new(), MEDIA_NO_CACHE).await
        {
            return Ok(Some(response));
        }
    }
    let _page_gate = strm_cache::PAGE_CACHE_GATE.lock().await;
    let exists = tokio::fs::try_exists(&cache_path).await?;
    strm_cache::reserve_page_space(
        &page_directory,
        if exists { 0 } else { entry.uncompressed_size },
        &cache_path,
    )
    .await?;
    let prepared = strm_archive::prepare_entry(&target_url, &manifest, entry, &cache_path).await?;
    strm_cache::touch_page(&prepared).await?;
    let mime = path_mime(FsPath::new(&entry.name));
    let response =
        stream_local_file(state, &prepared, &mime, &HeaderMap::new(), MEDIA_NO_CACHE).await?;
    Ok(Some(response))
}

#[derive(Debug, Serialize)]
pub struct EpubManifestResponse {
    pub chapters: Vec<EpubChapter>,
    pub total: usize,
    pub next_cursor: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EpubChapter {
    pub index: usize,
    pub title: String,
    pub href: String,
}

#[derive(Debug, Deserialize)]
pub struct EpubManifestQuery {
    pub cursor: Option<usize>,
    pub limit: Option<usize>,
    pub v: Option<String>,
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
    Query(query): Query<EpubManifestQuery>,
) -> Result<Response> {
    let (book, remote) = book_asset_path(&state, work_id).await?;
    let cached = cached_epub_manifest(&state, &book).await?;
    let total = cached.chapters.len();
    let (start, end) = epub_manifest_bounds(total, query.cursor, query.limit);
    let mut response = Json(EpubManifestResponse {
        chapters: cached.chapters[start..end].to_vec(),
        total,
        next_cursor: (end < total).then_some(end),
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
    let (book, remote_book) = book_asset_path(&state, work_id).await?;
    let cached = cached_epub_manifest(&state, &book).await?;
    let chapter_path = cached
        .chapters
        .get(chapter)
        .ok_or_else(|| AppError::NotFound(format!("EPUB chapter {chapter} not found")))?
        .href
        .clone();
    let version = query.v.clone();
    let archive = cached.archive.clone();
    let html = archive_blocking(&state.resources, MAX_EPUB_TEXT_BYTES, move || {
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
    let (book, remote_book) = book_asset_path(&state, work_id).await?;
    let cached = cached_epub_manifest(&state, &book).await?;
    let image_path = normalize_epub_entry_path(&query.path)?;
    let mime = path_mime(FsPath::new(&image_path));
    if !mime.starts_with("image/") {
        return Err(AppError::BadRequest(
            "EPUB entry is not an image".to_string(),
        ));
    }
    let archive = cached.archive.clone();
    let (bytes, lease) = archive_bytes_blocking(
        &state.resources,
        MAX_EPUB_IMAGE_BYTES,
        MAX_EPUB_IMAGE_BYTES,
        move || {
            let mut archive = archive
                .lock()
                .map_err(|_| AppError::Other("EPUB archive cache lock poisoned".to_string()))?;
            read_zip_bytes(&mut archive, &image_path)
        },
    )
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
        .header(header::CONTENT_LENGTH, bytes.len().to_string())
        .body(leased_bytes_body(bytes, lease))
        .map_err(|e| AppError::Other(e.to_string()))
}

async fn book_asset_path(state: &AppState, work_id: i64) -> Result<(PathBuf, bool)> {
    let book = state
        .db
        .work_asset_by_role(work_id, "book", Some("application/epub+zip"))
        .await?;
    let remote = vfs::is_qms_strm_uri(&book.path);
    Ok((
        vfs::asset_local_processing_path(state, &book).await?,
        remote,
    ))
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

fn epub_manifest_bounds(
    total: usize,
    cursor: Option<usize>,
    requested_limit: Option<usize>,
) -> (usize, usize) {
    // Preserve the old compact response for small books while ensuring that
    // accidental no-query requests cannot materialize a very large chapter
    // directory in one JSON response.
    let bounded =
        cursor.is_some() || requested_limit.is_some() || total > EPUB_MANIFEST_MAX_PAGE_SIZE;
    let start = cursor.unwrap_or(0).min(total);
    let limit = if bounded {
        requested_limit
            .unwrap_or(EPUB_MANIFEST_PAGE_SIZE)
            .clamp(1, EPUB_MANIFEST_MAX_PAGE_SIZE)
    } else {
        total
    };
    (start, start.saturating_add(limit).min(total))
}

async fn cached_epub_manifest(state: &AppState, path: &FsPath) -> Result<CachedEpubManifest> {
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
            remove_epub_manifest_cache_entry(&mut cache, &key);
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
            remove_epub_manifest_cache_entry(&mut cache, &key);
        }
    }

    let load_path = key.clone();
    let (chapters, archive) = archive_blocking(
        &state.resources,
        crate::archive::MAX_CENTRAL_DIRECTORY_BYTES.saturating_mul(2),
        move || read_epub_manifest(&load_path),
    )
    .await?;
    let retained_string_bytes = chapters
        .iter()
        .map(|chapter| chapter.title.len().saturating_add(chapter.href.len()))
        .sum::<usize>();
    let weight_bytes = estimated_archive_cache_weight(
        size,
        archive.central_directory_start(),
        1,
        archive.len(),
        retained_string_bytes,
    );
    let chapters = Arc::new(chapters);
    let cached = CachedEpubManifest {
        size,
        modified,
        weight_bytes,
        chapters,
        archive: Arc::new(Mutex::new(archive)),
    };
    let mut cache = EPUB_MANIFEST_CACHE.lock().await;
    let cache_limit = archive_manifest_cache_limit_per_kind(&state.resources);
    if !insert_epub_manifest_cache_entry(&mut cache, key.clone(), cached.clone(), cache_limit) {
        tracing::warn!(
            path = %key.display(),
            estimated_bytes = cached.weight_bytes,
            cache_limit_bytes = cache_limit,
            "EPUB manifest exceeds its byte cache budget; serving without retaining it"
        );
    }
    Ok(cached)
}

fn remove_epub_manifest_cache_entry(cache: &mut EpubManifestCacheState, key: &FsPath) {
    if let Some(removed) = cache.entries.remove(key) {
        cache.total_weight_bytes = cache
            .total_weight_bytes
            .saturating_sub(removed.weight_bytes);
    }
    remove_path_lru_key(&mut cache.lru, key);
}

fn insert_epub_manifest_cache_entry(
    cache: &mut EpubManifestCacheState,
    key: PathBuf,
    cached: CachedEpubManifest,
    cache_limit: u64,
) -> bool {
    if cached.weight_bytes > cache_limit {
        return false;
    }
    remove_epub_manifest_cache_entry(cache, &key);
    while cache.entries.len() >= EPUB_MANIFEST_CACHE_LIMIT
        || (!cache.entries.is_empty()
            && cache.total_weight_bytes.saturating_add(cached.weight_bytes) > cache_limit)
    {
        let Some(evicted) = cache.lru.front().cloned() else {
            break;
        };
        remove_epub_manifest_cache_entry(cache, &evicted);
    }
    cache.total_weight_bytes = cache.total_weight_bytes.saturating_add(cached.weight_bytes);
    cache.entries.insert(key.clone(), cached);
    touch_path_lru(&mut cache.lru, &key);
    true
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
    let declared_size = entry.size();
    let bytes = read_limited_zip_entry_sized(
        &mut entry,
        declared_size,
        MAX_EPUB_TEXT_BYTES,
        "EPUB text entry",
    )?;
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
    let declared_size = entry.size();
    read_limited_zip_entry_sized(
        &mut entry,
        declared_size,
        MAX_EPUB_IMAGE_BYTES,
        "EPUB image entry",
    )
}

#[cfg(test)]
fn read_limited_zip_entry<R: Read>(entry: &mut R, limit: u64, label: &str) -> Result<Vec<u8>> {
    // Generic readers do not expose a declared decompressed length. Reserve
    // only a small bounded prefix in that case; ZIP readers use the sized
    // variant below to avoid repeated Vec growth and copying for large pages.
    let initial_capacity = usize::try_from(limit.min(64 * 1024)).unwrap_or(64 * 1024);
    let mut bytes = Vec::with_capacity(initial_capacity);
    entry.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(AppError::BadRequest(format!(
            "{label} exceeds {limit} decompressed bytes"
        )));
    }
    Ok(bytes)
}

fn read_limited_zip_entry_sized<R: Read>(
    entry: &mut R,
    declared_size: u64,
    limit: u64,
    label: &str,
) -> Result<Vec<u8>> {
    if declared_size > limit {
        return Err(AppError::BadRequest(format!(
            "{label} exceeds {limit} decompressed bytes"
        )));
    }
    let capacity = usize::try_from(declared_size)
        .map_err(|_| AppError::BadRequest(format!("{label} declared size cannot fit in memory")))?;
    let mut bytes = Vec::with_capacity(capacity);
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
    Json(input): Json<GenerateAssetRequest>,
) -> Result<Json<serde_json::Value>> {
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
    use futures::StreamExt;
    use sqlx::Row;

    const PNG_1X1: &[u8] = &[
        137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1, 8, 6,
        0, 0, 0, 31, 21, 196, 137, 0, 0, 0, 10, 73, 68, 65, 84, 120, 156, 99, 0, 1, 0, 0, 5, 0, 1,
        13, 10, 45, 180, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130,
    ];

    fn derivative_test_config(
        temp: &tempfile::TempDir,
        database_url: String,
        data_dir: PathBuf,
    ) -> Config {
        let cover_cache_dir = temp.path().join("cover-cache");
        Config {
            bind: "127.0.0.1:0".to_string(),
            database_url,
            data_dir,
            cover_cache_dir: cover_cache_dir.clone(),
            comic_cover_cache_dir: cover_cache_dir.join("comic"),
            novel_cover_cache_dir: cover_cache_dir.join("novel"),
            audio_cover_cache_dir: cover_cache_dir.join("audio"),
            gallery_cover_cache_dir: cover_cache_dir.join("gallery"),
            coser_picture_cover_cache_dir: cover_cache_dir.join("coser-picture"),
            comics_dir: temp.path().join("comics"),
            novels_dir: temp.path().join("novels"),
            audio_dir: temp.path().join("audio"),
            gallery_dir: temp.path().join("gallery"),
            coser_picture_dir: temp.path().join("coser-picture"),
            generated_dir: temp.path().join("generated"),
            lightnovel_api_bases: Vec::new(),
            lightnovel_access_token: None,
            enrichment_concurrency: 1,
            ehtt_url: String::new(),
            openai_api_key: None,
            openai_image_model: "gpt-image-2".to_string(),
            qmediasync_base_url: String::new(),
            qmediasync_strm_dir: None,
            cloud_cache_max_bytes: 64 * 1024 * 1024 * 1024,
            thumbnail_cache_max_bytes_per_dir: 8 * 1024 * 1024 * 1024,
            catalog_v2_enabled: true,
            facet_bitmap_enabled: false,
            inventory_scanner_enabled: false,
            inventory_scanner_kinds: std::collections::BTreeSet::new(),
            search_outbox_shadow_enabled: false,
            search_shadow_canary_enabled: false,
            search_incremental_reader_enabled: false,
            search_reader_prewarm_enabled: false,
            derivative_cache_v2_enabled: true,
            jpeg_thumbnail_downscale_enabled: true,
            derivative_cache_dir: temp.path().join("derivatives"),
            derivative_cache_max_bytes: 1024 * 1024,
            derivative_cache_low_watermark_bytes: 512 * 1024,
            enable_file_watcher: false,
            watch_debounce_seconds: 20,
        }
    }

    #[tokio::test]
    async fn standard_thumb_and_cover_use_v2_without_writing_legacy_caches() {
        let temp = tempfile::tempdir().unwrap();
        let data_dir = temp.path().join("data");
        let gallery_dir = temp.path().join("gallery");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&gallery_dir).unwrap();
        let source = gallery_dir.join("image.png");
        std::fs::write(&source, PNG_1X1).unwrap();
        let database_url = format!(
            "sqlite://{}",
            test_path_string(&data_dir.join("library.sqlite"))
        );
        let db = Db::connect(&database_url).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "gallery",
                "image",
                Some(&test_path_string(&gallery_dir)),
                None,
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();
        let asset_id = db
            .upsert_asset(
                work_id,
                &test_path_string(&source),
                "image/png",
                "cover",
                None,
                None,
                Some(PNG_1X1.len() as i64),
                json!({}),
            )
            .await
            .unwrap();
        let config = derivative_test_config(&temp, database_url, data_dir.clone());
        let derivatives = crate::derivative::DerivativeCache::new(
            db.clone(),
            true,
            config.derivative_cache_dir.clone(),
            config.derivative_cache_max_bytes,
            config.derivative_cache_low_watermark_bytes,
        )
        .unwrap();
        derivatives.recover_startup().await.unwrap();
        let state = Arc::new(AppState {
            config,
            db,
            http: reqwest::Client::new(),
            resources: crate::resource::ResourceGovernor::standard(),
            derivatives: derivatives.clone(),
            catalog_runtime: crate::catalog::CatalogRuntime::default(),
            search_runtime: crate::search::SearchRuntime::default(),
            comic_page_cache: Arc::new(ComicPageCache::default()),
        });

        let response = thumb_asset(
            State(state.clone()),
            Path(asset_id),
            Query(ThumbQuery {
                size: Some(256),
                v: Some("v1".to_string()),
            }),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-arislist-derivative-cache"], "v2");
        assert_eq!(response.headers()[header::CONTENT_TYPE], "image/jpeg");
        assert!(cache_files(&data_dir.join("thumbs")).is_empty());

        let cover = work_cover(
            State(state),
            Path(work_id),
            Query(CoverQuery {
                size: Some(480),
                v: Some("v1".to_string()),
            }),
        )
        .await
        .unwrap();
        assert_eq!(cover.status(), StatusCode::OK);
        assert_eq!(cover.headers()["x-arislist-derivative-cache"], "v2");
        assert!(cache_files(&temp.path().join("cover-cache/gallery")).is_empty());
        let stats = derivatives.stats().await.unwrap();
        assert_eq!(stats.resident_files, 2);
        assert!(stats.resident_bytes > 0);
        assert_eq!(stats.statuses.get("ready"), Some(&2));
    }

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
                lightnovel_api_bases: Vec::new(),
                lightnovel_access_token: None,
                enrichment_concurrency: 1,
                ehtt_url: String::new(),
                openai_api_key: None,
                openai_image_model: "gpt-image-2".to_string(),
                qmediasync_base_url: String::new(),
                qmediasync_strm_dir: None,
                cloud_cache_max_bytes: 64 * 1024 * 1024 * 1024,
                thumbnail_cache_max_bytes_per_dir: 8 * 1024 * 1024 * 1024,
                catalog_v2_enabled: true,
                facet_bitmap_enabled: false,
                inventory_scanner_enabled: false,
                inventory_scanner_kinds: std::collections::BTreeSet::new(),
                search_outbox_shadow_enabled: false,
                search_shadow_canary_enabled: false,
                search_incremental_reader_enabled: false,
                search_reader_prewarm_enabled: false,
                derivative_cache_v2_enabled: false,
                jpeg_thumbnail_downscale_enabled: false,
                derivative_cache_dir: temp.path().join("derivatives"),
                derivative_cache_max_bytes: 1024,
                derivative_cache_low_watermark_bytes: 512,
                enable_file_watcher: false,
                watch_debounce_seconds: 20,
            },
            derivatives: crate::derivative::DerivativeCache::disabled(
                db.clone(),
                temp.path().join("derivatives"),
            ),
            db,
            http: reqwest::Client::new(),
            resources: crate::resource::ResourceGovernor::standard(),
            catalog_runtime: crate::catalog::CatalogRuntime::default(),
            search_runtime: crate::search::SearchRuntime::default(),
            comic_page_cache: Arc::new(ComicPageCache::default()),
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
        write_test_zip_entries(path, 1);
    }

    fn test_jpeg_bytes(width: u32, height: u32) -> Vec<u8> {
        let source = image::RgbImage::from_pixel(width, height, image::Rgb([96, 128, 192]));
        let mut bytes = Vec::new();
        JpegEncoder::new_with_quality(&mut bytes, 84)
            .encode_image(&image::DynamicImage::ImageRgb8(source))
            .unwrap();
        bytes
    }

    fn write_test_zip_entries(path: &FsPath, count: usize) {
        let file = File::create(path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();
        for index in 1..=count {
            zip.start_file(format!("{index}.png"), options).unwrap();
            zip.write_all(PNG_1X1).unwrap();
        }
        zip.finish().unwrap();
    }

    fn write_test_zip_payload(path: &FsPath, name: &str, payload: &[u8]) {
        let file = File::create(path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        zip.start_file(name, options).unwrap();
        zip.write_all(payload).unwrap();
        zip.finish().unwrap();
    }

    async fn comic_test_state(temp: &tempfile::TempDir, page_count: usize) -> (Arc<AppState>, i64) {
        let data_dir = temp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let comic_dir = temp.path().join("comics");
        std::fs::create_dir_all(&comic_dir).unwrap();
        let archive_path = comic_dir.join("comic.cbz");
        write_test_zip_entries(&archive_path, page_count);
        let database_url = format!(
            "sqlite://{}",
            test_path_string(&data_dir.join("library.sqlite"))
        );
        let db = Db::connect(&database_url).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "comic",
                "comic",
                Some(&test_path_string(&archive_path)),
                Some("Doujinshi"),
                None,
                None,
                json!({ "page_count": page_count }),
            )
            .await
            .unwrap();
        db.upsert_asset(
            work_id,
            &test_path_string(&archive_path),
            "application/vnd.comicbook+zip",
            "archive",
            Some("cbz"),
            None,
            std::fs::metadata(&archive_path)
                .ok()
                .map(|metadata| metadata.len() as i64),
            json!({ "page_count": page_count }),
        )
        .await
        .unwrap();
        let mut config = derivative_test_config(temp, database_url, data_dir);
        config.derivative_cache_v2_enabled = false;
        let derivatives = crate::derivative::DerivativeCache::disabled(
            db.clone(),
            config.derivative_cache_dir.clone(),
        );
        let state = Arc::new(AppState {
            config,
            db,
            http: reqwest::Client::new(),
            resources: crate::resource::ResourceGovernor::standard(),
            derivatives,
            catalog_runtime: crate::catalog::CatalogRuntime::default(),
            search_runtime: crate::search::SearchRuntime::default(),
            comic_page_cache: Arc::new(ComicPageCache::default()),
        });
        (state, work_id)
    }

    #[tokio::test]
    async fn comic_manifest_query_is_bounded_and_legacy_response_remains_complete() {
        let temp = tempfile::tempdir().unwrap();
        let (state, work_id) = comic_test_state(&temp, 4).await;

        let first = comic_pages(
            State(state.clone()),
            Path(work_id),
            Query(ComicPagesQuery {
                cursor: Some(0),
                limit: Some(2),
                v: None,
            }),
        )
        .await
        .unwrap();
        let first = axum::body::to_bytes(first.into_body(), 16 * 1024)
            .await
            .unwrap();
        let first: serde_json::Value = serde_json::from_slice(&first).unwrap();
        assert_eq!(first["total"], 4);
        assert_eq!(first["next_cursor"], 2);
        assert_eq!(first["pages"].as_array().unwrap().len(), 2);
        assert_eq!(first["pages"][0]["name"], "1.png");
        assert_eq!(first["pages"][1]["name"], "2.png");

        let second = comic_pages(
            State(state.clone()),
            Path(work_id),
            Query(ComicPagesQuery {
                cursor: Some(2),
                limit: Some(2),
                v: None,
            }),
        )
        .await
        .unwrap();
        let second = axum::body::to_bytes(second.into_body(), 16 * 1024)
            .await
            .unwrap();
        let second: serde_json::Value = serde_json::from_slice(&second).unwrap();
        assert_eq!(second["next_cursor"], serde_json::Value::Null);
        assert_eq!(second["pages"].as_array().unwrap().len(), 2);
        assert_eq!(second["pages"][0]["name"], "3.png");
        assert_eq!(second["pages"][1]["name"], "4.png");

        let legacy = comic_pages(
            State(state),
            Path(work_id),
            Query(ComicPagesQuery {
                cursor: None,
                limit: None,
                v: None,
            }),
        )
        .await
        .unwrap();
        let legacy = axum::body::to_bytes(legacy.into_body(), 16 * 1024)
            .await
            .unwrap();
        let legacy: serde_json::Value = serde_json::from_slice(&legacy).unwrap();
        assert_eq!(legacy["total"], 4);
        assert_eq!(legacy["next_cursor"], serde_json::Value::Null);
        assert_eq!(legacy["pages"].as_array().unwrap().len(), 4);
    }

    #[tokio::test]
    async fn large_no_query_comic_manifest_falls_back_to_a_bounded_page() {
        let temp = tempfile::tempdir().unwrap();
        let (state, work_id) = comic_test_state(&temp, COMIC_MANIFEST_MAX_PAGE_SIZE + 1).await;

        let response = comic_pages(
            State(state),
            Path(work_id),
            Query(ComicPagesQuery {
                cursor: None,
                limit: None,
                v: None,
            }),
        )
        .await
        .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 256 * 1024)
            .await
            .unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["total"], (COMIC_MANIFEST_MAX_PAGE_SIZE + 1));
        assert_eq!(payload["next_cursor"], COMIC_MANIFEST_PAGE_SIZE);
        assert_eq!(
            payload["pages"].as_array().unwrap().len(),
            COMIC_MANIFEST_PAGE_SIZE
        );
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

    #[tokio::test]
    async fn comic_page_stream_decompresses_in_bounded_chunks_and_releases_budget() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("comic.cbz");
        write_test_zip(&path);
        let cached = open_cbz_pages(&path).unwrap();
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let (size, lease, mut chunks) =
            stream_cbz_page_chunks(&resources, cached.archive.clone(), "1.png".to_string())
                .await
                .unwrap();
        assert_eq!(size, PNG_1X1.len() as u64);
        let mut body = Vec::new();
        while let Some(chunk) = chunks.recv().await {
            body.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(body, PNG_1X1);
        drop(lease);
        let snapshot = resources.snapshot();
        assert_eq!(snapshot.pools["archive_stream"].used, 0);
        assert_eq!(snapshot.pools["processing_memory"].used, 0);
        assert_eq!(snapshot.pools["inflight_media"].used, 0);
    }

    #[tokio::test]
    async fn dropping_comic_stream_releases_worker_and_all_reserved_resources() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("comic.cbz");
        let payload = vec![7_u8; STREAM_BUFFER_SIZE * (COMIC_STREAM_QUEUE_CHUNKS + 2)];
        write_test_zip_payload(&path, "1.png", &payload);
        let cached = open_cbz_pages(&path).unwrap();
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let (size, lease, chunks) =
            stream_cbz_page_chunks(&resources, cached.archive.clone(), "1.png".to_string())
                .await
                .unwrap();
        assert_eq!(size, payload.len() as u64);
        assert_eq!(resources.snapshot().pools["archive_stream"].used, 1);

        drop(chunks);
        drop(lease);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = resources.snapshot();
                if snapshot.pools["archive_stream"].used == 0
                    && snapshot.pools["processing_memory"].used == 0
                    && snapshot.pools["inflight_media"].used == 0
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn comic_cache_evicts_by_estimated_bytes_and_skips_oversized_entries() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("comic.cbz");
        write_test_zip(&path);
        let mut first = open_cbz_pages(&path).unwrap();
        first.weight_bytes = 60;
        let mut second = first.clone();
        second.weight_bytes = 60;
        let mut oversized = first.clone();
        oversized.weight_bytes = 101;
        let mut cache = ComicPageCacheState::default();

        assert!(insert_comic_cache_entry(
            &mut cache,
            "first".to_string(),
            first,
            100,
        ));
        assert!(insert_comic_cache_entry(
            &mut cache,
            "second".to_string(),
            second,
            100,
        ));
        assert!(!cache.entries.contains_key("first"));
        assert!(cache.entries.contains_key("second"));
        assert_eq!(cache.total_weight_bytes, 60);

        assert!(!insert_comic_cache_entry(
            &mut cache,
            "oversized".to_string(),
            oversized,
            100,
        ));
        assert!(cache.entries.contains_key("second"));
        assert!(!cache.entries.contains_key("oversized"));
        assert_eq!(cache.total_weight_bytes, 60);
    }

    #[tokio::test]
    async fn comic_manifest_cache_round_trip_is_bounded_and_source_fenced() {
        let temp = tempfile::tempdir().unwrap();
        let (state, work_id) = comic_test_state(&temp, 3).await;
        let detail = state.db.work_detail(work_id).await.unwrap();
        let archive_path = detail
            .assets
            .iter()
            .find(|asset| asset.role == "archive")
            .map(|asset| FsPath::new(&asset.path))
            .unwrap();
        let cached = open_cbz_pages(archive_path).unwrap();
        assert!(
            persist_comic_manifest_cache(&state.db, archive_path, &cached, 1024 * 1024)
                .await
                .unwrap()
        );

        let metadata = std::fs::metadata(archive_path).unwrap();
        let pages = load_comic_manifest_cache(
            &state.db,
            archive_path,
            metadata.len(),
            metadata.modified().ok(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            pages,
            vec![
                "1.png".to_string(),
                "2.png".to_string(),
                "3.png".to_string()
            ]
        );
        let resident = sqlx::query(
            "SELECT resident_bytes, resident_entries FROM archive_manifest_cache_state WHERE singleton = 1",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert!(resident.get::<i64, _>("resident_bytes") > 0);
        assert_eq!(resident.get::<i64, _>("resident_entries"), 1);

        sqlx::query("UPDATE archive_manifest_cache SET pages_json = ?1 WHERE cache_key = ?2")
            .bind("[\"not-an-image.txt\"]")
            .bind(comic_manifest_cache_key(archive_path))
            .execute(state.db.pool())
            .await
            .unwrap();
        assert!(load_comic_manifest_cache(
            &state.db,
            archive_path,
            metadata.len(),
            metadata.modified().ok(),
        )
        .await
        .unwrap()
        .is_none());

        let second_path = temp.path().join("second.cbz");
        write_test_zip_entries(&second_path, 1);
        let second = open_cbz_pages(&second_path).unwrap();
        let first_bytes = sqlx::query_scalar::<_, i64>(
            "SELECT bytes FROM archive_manifest_cache WHERE cache_key = ?1",
        )
        .bind(comic_manifest_cache_key(archive_path))
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert!(persist_comic_manifest_cache(
            &state.db,
            &second_path,
            &second,
            u64::try_from(first_bytes).unwrap(),
        )
        .await
        .unwrap());
        let resident_after_eviction = sqlx::query(
            "SELECT resident_bytes, resident_entries FROM archive_manifest_cache_state WHERE singleton = 1",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert!(resident_after_eviction.get::<i64, _>("resident_bytes") <= first_bytes);
        assert_eq!(resident_after_eviction.get::<i64, _>("resident_entries"), 1);

        let pages = cached_cbz_page_manifest(&state, archive_path)
            .await
            .unwrap();
        assert_eq!(pages.len(), 3);
        assert_eq!(pages[0].name, "1.png");
        assert_eq!(state.resources.snapshot().pools["archive_stream"].used, 0);
    }

    #[test]
    fn epub_cache_evicts_by_estimated_bytes_and_tracks_removal() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("book.epub");
        write_test_zip(&path);
        let manifest = |weight_bytes| CachedEpubManifest {
            size: 1,
            modified: None,
            weight_bytes,
            chapters: Arc::new(Vec::new()),
            archive: Arc::new(Mutex::new(
                ZipArchive::new(File::open(&path).unwrap()).unwrap(),
            )),
        };
        let first = temp.path().join("first.epub");
        let second = temp.path().join("second.epub");
        let oversized = temp.path().join("oversized.epub");
        let mut cache = EpubManifestCacheState::default();

        assert!(insert_epub_manifest_cache_entry(
            &mut cache,
            first.clone(),
            manifest(60),
            100,
        ));
        assert!(insert_epub_manifest_cache_entry(
            &mut cache,
            second.clone(),
            manifest(60),
            100,
        ));
        assert!(!cache.entries.contains_key(&first));
        assert!(cache.entries.contains_key(&second));
        assert_eq!(cache.total_weight_bytes, 60);

        assert!(!insert_epub_manifest_cache_entry(
            &mut cache,
            oversized.clone(),
            manifest(101),
            100,
        ));
        assert!(cache.entries.contains_key(&second));
        assert!(!cache.entries.contains_key(&oversized));
        remove_epub_manifest_cache_entry(&mut cache, &second);
        assert_eq!(cache.total_weight_bytes, 0);
        assert!(cache.lru.is_empty());
    }

    #[tokio::test]
    async fn thumbnail_placeholder_is_a_no_store_svg_not_source_media() {
        let response = thumbnail_placeholder_response(360).unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "image/svg+xml; charset=utf-8"
        );
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            DERIVATIVE_PLACEHOLDER_CACHE
        );
        assert_eq!(
            response
                .headers()
                .get("x-arislist-derivative-status")
                .unwrap(),
            "placeholder"
        );
        let body = axum::body::to_bytes(response.into_body(), 16 * 1024)
            .await
            .unwrap();
        assert!(body.starts_with(b"<svg"));
        assert!(body.ends_with(b"</svg>"));
        assert!(!body.starts_with(PNG_1X1));
    }

    #[tokio::test]
    async fn dropping_a_partial_response_releases_its_inflight_budget() {
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let lease = resources
            .reserve(ResourceClass::ArchiveStream, 0, 8 * 1024 * 1024)
            .await
            .unwrap();
        let (work_lease, buffer_lease) = lease.split();
        drop(work_lease);
        let body = leased_bytes_body(vec![7_u8; STREAM_BUFFER_SIZE * 2], buffer_lease);
        let mut stream = body.into_data_stream();
        assert_eq!(
            stream.next().await.unwrap().unwrap().len(),
            STREAM_BUFFER_SIZE
        );
        assert_eq!(
            resources.snapshot().pools["inflight_media"].used_bytes,
            Some(8 * 1024 * 1024)
        );
        drop(stream);
        assert_eq!(resources.snapshot().pools["inflight_media"].used, 0);
    }

    #[tokio::test]
    async fn cancelled_archive_request_keeps_budget_until_blocking_work_exits() {
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker_resources = resources.clone();
        let request = tokio::spawn(async move {
            archive_bytes_blocking(
                &worker_resources,
                16 * 1024 * 1024,
                8 * 1024 * 1024,
                move || {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(vec![1_u8])
                },
            )
            .await
        });
        tokio::task::spawn_blocking(move || started_rx.recv())
            .await
            .unwrap()
            .unwrap();
        request.abort();
        let _ = request.await;
        let snapshot = resources.snapshot();
        assert_eq!(snapshot.pools["archive_stream"].used, 1);
        assert_eq!(
            snapshot.pools["processing_memory"].used_bytes,
            Some(16 * 1024 * 1024)
        );
        assert_eq!(
            snapshot.pools["inflight_media"].used_bytes,
            Some(8 * 1024 * 1024)
        );

        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = resources.snapshot();
                if snapshot.pools["archive_stream"].used == 0
                    && snapshot.pools["processing_memory"].used == 0
                    && snapshot.pools["inflight_media"].used == 0
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn panicking_archive_worker_releases_all_reserved_resources() {
        let resources = ResourceGovernor::new(crate::resource::ResourceLimits::nas_n100_4g());
        let result = archive_bytes_blocking(
            &resources,
            16 * 1024 * 1024,
            8 * 1024 * 1024,
            || -> Result<Vec<u8>> { panic!("injected archive worker panic") },
        )
        .await;
        assert!(result.is_err());
        let snapshot = resources.snapshot();
        assert_eq!(snapshot.pools["archive_stream"].used, 0);
        assert_eq!(snapshot.pools["processing_memory"].used, 0);
        assert_eq!(snapshot.pools["inflight_media"].used, 0);
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
    fn jpeg_dct_scale_chooses_smallest_decode_that_covers_target() {
        assert_eq!(jpeg_dct_scale_for_target(6_000, 4_000, 256), 1);
        assert_eq!(jpeg_dct_scale_for_target(10_000, 6_000, 1_920), 2);
        assert_eq!(jpeg_dct_scale_for_target(1_500, 1_000, 256), 2);
        assert_eq!(jpeg_dct_scale_for_target(200, 100, 960), 8);
    }

    #[test]
    fn native_jpeg_thumbnail_decode_scales_before_rgb_allocation() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("source.jpg");
        let cache_path = temp.path().join("thumbnail.jpg");
        let bytes = test_jpeg_bytes(3_000, 2_000);
        std::fs::write(&source_path, bytes).unwrap();

        generate_thumbnail_atomic(&source_path, &cache_path, 256, true).unwrap();
        let dimensions = image::ImageReader::open(&cache_path)
            .unwrap()
            .with_guessed_format()
            .unwrap()
            .into_dimensions()
            .unwrap();
        assert_eq!(dimensions, (256, 171));
    }

    #[test]
    fn non_jpeg_thumbnail_keeps_existing_decode_path_when_flag_is_enabled() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("source.jpg");
        let cache_path = temp.path().join("thumbnail.jpg");
        std::fs::write(&source_path, PNG_1X1).unwrap();

        generate_thumbnail_atomic(&source_path, &cache_path, 256, true).unwrap();
        let dimensions = image::ImageReader::open(&cache_path)
            .unwrap()
            .with_guessed_format()
            .unwrap()
            .into_dimensions()
            .unwrap();
        assert_eq!(dimensions, (256, 256));
    }

    #[test]
    fn archive_jpeg_thumbnail_uses_entry_stream_before_buffered_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let archive_path = temp.path().join("comic.cbz");
        let cache_path = temp.path().join("thumbnail.jpg");
        write_test_zip_payload(&archive_path, "1.jpg", &test_jpeg_bytes(3_000, 2_000));
        let cached = open_cbz_pages(&archive_path).unwrap();

        generate_archive_thumbnail_atomic(&cached.archive, "1.jpg", &cache_path, 256, true)
            .unwrap();
        let dimensions = image::ImageReader::open(&cache_path)
            .unwrap()
            .with_guessed_format()
            .unwrap()
            .into_dimensions()
            .unwrap();
        assert_eq!(dimensions, (256, 171));
    }

    #[test]
    fn archive_non_jpeg_thumbnail_keeps_buffered_compatibility_path() {
        let temp = tempfile::tempdir().unwrap();
        let archive_path = temp.path().join("comic.cbz");
        let cache_path = temp.path().join("thumbnail.jpg");
        write_test_zip(&archive_path);
        let cached = open_cbz_pages(&archive_path).unwrap();

        generate_archive_thumbnail_atomic(&cached.archive, "1.png", &cache_path, 256, true)
            .unwrap();
        let dimensions = image::ImageReader::open(&cache_path)
            .unwrap()
            .with_guessed_format()
            .unwrap()
            .into_dimensions()
            .unwrap();
        assert_eq!(dimensions, (256, 256));
    }

    #[test]
    fn jpeg_rgb_buffer_limit_is_checked_before_decode_allocation() {
        let bytes = jpeg_rgb_buffer_size(10_000, 10_000).unwrap();
        assert!(bytes as u64 > MAX_IMAGE_DECODE_ALLOC_BYTES);
        assert!(jpeg_rgb_buffer_size(usize::MAX, 2).is_err());
    }

    #[test]
    fn reader_sizes_use_only_the_two_n100_buckets_and_allow_raw_fallback() {
        assert_eq!(reader_size_bucket(None), None);
        assert_eq!(reader_size_bucket(Some(0)), None);
        assert_eq!(reader_size_bucket(Some(960)), Some(1280));
        assert_eq!(reader_size_bucket(Some(1280)), Some(1280));
        assert_eq!(reader_size_bucket(Some(1281)), Some(1920));
        assert_eq!(reader_size_bucket(Some(10_000)), Some(1920));
    }

    #[test]
    fn reader_derivative_source_version_binds_archive_revision_and_page_name() {
        let archive = Asset {
            id: 7,
            work_id: 3,
            path: "/library/comics/book.cbz".to_string(),
            mime: "application/vnd.comicbook+zip".to_string(),
            role: "archive".to_string(),
            variant: Some("cbz".to_string()),
            position: Some(-1),
            size: Some(100),
            meta_json: "{\"_source_version\":\"v1\"}".to_string(),
            created_at: chrono::Utc::now(),
        };
        let cached = CachedComicPages {
            size: 100,
            modified: Some(UNIX_EPOCH + Duration::from_secs(10)),
            weight_bytes: 1,
            pages: Arc::new(Vec::new()),
            archive: Arc::new(ComicArchivePool {
                archives: Vec::new(),
                next: AtomicUsize::new(0),
            }),
        };
        let first = archive_page_source_version(&archive, &cached, "pages/001.jpg");
        assert_ne!(
            first,
            archive_page_source_version(&archive, &cached, "pages/002.jpg")
        );
        let changed_archive = Asset {
            meta_json: "{\"_source_version\":\"v2\"}".to_string(),
            ..archive
        };
        assert_ne!(
            first,
            archive_page_source_version(&changed_archive, &cached, "pages/001.jpg")
        );
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
    fn epub_manifest_bounds_keep_small_legacy_responses_and_cap_large_pages() {
        assert_eq!(epub_manifest_bounds(4, None, None), (0, 4));
        assert_eq!(
            epub_manifest_bounds(10_000, None, None),
            (0, EPUB_MANIFEST_PAGE_SIZE)
        );
        assert_eq!(
            epub_manifest_bounds(10_000, Some(200), Some(900)),
            (200, 700)
        );
        assert_eq!(
            epub_manifest_bounds(10_000, Some(9_999), Some(0)),
            (9_999, 10_000)
        );
        assert_eq!(
            epub_manifest_bounds(10_000, Some(20_000), None),
            (10_000, 10_000)
        );
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
    async fn large_thumbnail_cache_directories_do_not_rescan_on_each_mtime_change() {
        let temp = tempfile::tempdir().unwrap();
        let cache_path = temp.path().join("new.jpg");
        {
            let mut quotas = THUMBNAIL_CACHE_QUOTAS.lock().unwrap();
            quotas.insert(
                temp.path().to_path_buf(),
                ThumbnailCacheQuotaState {
                    initialized: true,
                    committed: 0,
                    reserved: 0,
                    generation: 0,
                    file_count: CACHE_USAGE_MTIME_RESCAN_MAX_FILES + 1,
                    observed_dir_modified: Some(UNIX_EPOCH),
                    last_scan: Some(Instant::now()),
                },
            );
        }

        let reservation = reserve_thumbnail_cache_capacity(&cache_path, 1)
            .await
            .unwrap();
        drop(reservation);

        let quotas = THUMBNAIL_CACHE_QUOTAS.lock().unwrap();
        let state = quotas.get(temp.path()).unwrap();
        assert_eq!(state.file_count, CACHE_USAGE_MTIME_RESCAN_MAX_FILES + 1);
        assert_eq!(state.observed_dir_modified, Some(UNIX_EPOCH));
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
