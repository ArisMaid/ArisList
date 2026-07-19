use std::collections::HashMap;
use std::io::{BufWriter, Cursor, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;
use futures::StreamExt;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use url::Url;
use uuid::Uuid;

use crate::error::{AppError, Result};
use crate::models::Asset;
use crate::security::ensure_asset_path_allowed_with_roots_async;
use crate::settings::{self, AppSettings, MediaSourceSettings};
use crate::AppState;

const MAX_CLOUD_CACHE_FILE_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const MAX_REMOTE_THUMBNAIL_SOURCE_BYTES: u64 = 128 * 1024 * 1024;
const MAX_STRM_FILE_BYTES: u64 = 64 * 1024;
const MAX_IMAGE_DECODE_ALLOC_BYTES: u64 = 256 * 1024 * 1024;
const VALIDATED_CLOUD_CACHE_LIMIT: usize = 2_048;
const STRM_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const STRM_REQUEST_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const STRM_DNS_TIMEOUT: Duration = Duration::from_secs(10);
const CACHE_USAGE_RESCAN_INTERVAL: Duration = Duration::from_secs(60);
const REMOTE_DERIVED_CACHE_MAX_AGE: Duration = Duration::from_secs(15 * 60);
const CLOUD_CACHE_REVALIDATE_INTERVAL: Duration = Duration::from_secs(15 * 60);
const STRM_CLIENT_CACHE_LIMIT: usize = 32;

static CACHE_WRITE_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Weak<AsyncMutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
type CloudCacheValidationMap = HashMap<PathBuf, (u64, Option<SystemTime>)>;
static VALIDATED_CLOUD_CACHES: LazyLock<Mutex<CloudCacheValidationMap>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static CLOUD_CACHE_QUOTAS: LazyLock<Mutex<HashMap<PathBuf, CloudCacheQuotaState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static CLOUD_CACHE_DOWNLOADS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(2)));
static CLOUD_CACHE_VALIDATORS: LazyLock<Mutex<HashMap<PathBuf, CloudCacheValidator>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static STRM_CLIENTS: LazyLock<Mutex<HashMap<String, (reqwest::Client, Instant)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone)]
struct CloudCacheValidator {
    etag: Option<String>,
    last_modified: Option<String>,
    checked_at: Instant,
}

#[derive(Default)]
struct CloudCacheQuotaState {
    initialized: bool,
    committed: u64,
    reserved: u64,
    generation: u64,
    observed_dir_modified: Option<SystemTime>,
    last_scan: Option<Instant>,
}

struct CloudCacheReservation {
    cache_dir: PathBuf,
    reserved: u64,
    active: bool,
}

impl CloudCacheReservation {
    fn limit(&self) -> u64 {
        self.reserved
    }

    fn commit(mut self, actual: u64, replaced: u64, observed_dir_modified: Option<SystemTime>) {
        let mut quotas = CLOUD_CACHE_QUOTAS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(state) = quotas.get_mut(&self.cache_dir) {
            state.reserved = state.reserved.saturating_sub(self.reserved);
            state.committed = state
                .committed
                .saturating_sub(replaced)
                .saturating_add(actual);
            state.generation = state.generation.wrapping_add(1);
            state.observed_dir_modified = observed_dir_modified;
        }
        self.active = false;
    }
}

impl Drop for CloudCacheReservation {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut quotas = CLOUD_CACHE_QUOTAS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(state) = quotas.get_mut(&self.cache_dir) {
            state.reserved = state.reserved.saturating_sub(self.reserved);
            state.generation = state.generation.wrapping_add(1);
        }
    }
}

struct TempFileCleanup {
    path: Option<PathBuf>,
}

impl TempFileCleanup {
    fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn disarm(&mut self) {
        self.path = None;
    }
}

impl Drop for TempFileCleanup {
    fn drop(&mut self) {
        let Some(path) = self.path.take() else {
            return;
        };
        if let Err(err) = std::fs::remove_file(&path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(path = %path.display(), error = %err, "failed to clean abandoned cache temp file");
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AssetRouteInfo {
    pub asset_id: i64,
    pub provider: String,
    pub policy: String,
    pub policy_label: String,
    pub transfer: String,
    pub route_label: String,
    pub via_qmediasync: bool,
    pub via_app: bool,
    pub qmediasync_host: Option<String>,
    pub target_host: Option<String>,
    pub note: Option<String>,
}

pub fn is_qms_strm_uri(value: &str) -> bool {
    value.starts_with("qms-strm://")
}

pub fn qms_strm_uri(mount_name: &str, relative_path: &str) -> String {
    let relative_path = normalize_remote_path(relative_path);
    let tail = relative_path.trim_start_matches('/');
    format!("qms-strm://{mount_name}/{tail}")
}

pub fn parse_qms_strm_uri(value: &str) -> Result<(String, String)> {
    let Some(rest) = value.strip_prefix("qms-strm://") else {
        return Err(AppError::BadRequest(
            "asset is not a qmediasync STRM URI".to_string(),
        ));
    };
    let (mount, path) = rest
        .split_once('/')
        .map(|(mount, path)| (mount.to_string(), format!("/{path}")))
        .unwrap_or_else(|| (rest.to_string(), "/".to_string()));
    if mount.trim().is_empty() {
        return Err(AppError::BadRequest(
            "qmediasync STRM URI mount is empty".to_string(),
        ));
    }
    Ok((mount, normalize_remote_path(&path)))
}

pub fn normalize_remote_path(path: &str) -> String {
    let trimmed = path.trim().replace('\\', "/");
    if trimmed.is_empty() || trimmed == "/" {
        return "/".to_string();
    }
    let mut out = String::from("/");
    out.push_str(trimmed.trim_matches('/'));
    out
}

pub fn qmediasync_sources<'a>(
    settings: &'a AppSettings,
    kind: &'a str,
) -> impl Iterator<Item = &'a MediaSourceSettings> + 'a {
    settings.media_sources.iter().filter(move |source| {
        source.enabled && source.provider == "qmediasync" && source.kind == kind
    })
}

pub fn qmediasync_scan_sources(settings: &AppSettings, kind: &str) -> Vec<MediaSourceSettings> {
    let mut sources = qmediasync_sources(settings, kind)
        .cloned()
        .collect::<Vec<_>>();
    if kind != "comic" || !settings.qmediasync.enabled {
        return sources;
    }

    let mut used_mounts = sources
        .iter()
        .map(|source| source.mount_name.clone())
        .collect::<Vec<_>>();
    for root in &settings.qmediasync.strm_roots {
        let root = root.trim().trim_end_matches(['/', '\\']);
        if root.is_empty()
            || sources
                .iter()
                .any(|source| source.root.trim_end_matches(['/', '\\']) == root)
        {
            continue;
        }
        let mount_name = unique_qmediasync_mount_name(&mut used_mounts);
        sources.push(MediaSourceSettings {
            kind: "comic".to_string(),
            provider: "qmediasync".to_string(),
            root: root.to_string(),
            mount_name,
            enabled: true,
            scan_depth: 12,
        });
    }
    sources
}

fn unique_qmediasync_mount_name(used: &mut Vec<String>) -> String {
    let mut index = 1;
    loop {
        let candidate = if index == 1 {
            "qms".to_string()
        } else {
            format!("qms{index}")
        };
        if !used.iter().any(|name| name == &candidate) {
            used.push(candidate.clone());
            return candidate;
        }
        index += 1;
    }
}

pub async fn qms_strm_meta_json(
    mount_name: &str,
    source_root: &Path,
    strm_path: &Path,
    relative_path: &str,
    target_url: &str,
) -> Value {
    let metadata = tokio::fs::metadata(strm_path).await.ok();
    serde_json::json!({
        "provider": "qmediasync",
        "mount_name": mount_name,
        "source_root": source_root.to_string_lossy(),
        "strm_path": strm_path.to_string_lossy(),
        "relative_path": normalize_remote_path(relative_path),
        "strm_mtime": metadata
            .as_ref()
            .and_then(|meta| meta.modified().ok())
            .and_then(system_time_key),
        "strm_size": metadata.map(|meta| meta.len()),
        "target_url_hash": short_hash(target_url),
    })
}

pub fn cloud_cache_key(asset: &Asset) -> String {
    let mut hasher = Sha256::new();
    hasher.update(asset.path.as_bytes());
    hasher.update(asset.size.unwrap_or_default().to_le_bytes());
    hasher.update(asset.meta_json.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub async fn qms_asset_version_key(state: &AppState, asset: &Asset) -> Result<String> {
    let strm_path = qms_strm_path_for_asset(state, &asset.path).await?;
    let raw_url = read_qms_strm_url_async(strm_path.clone()).await?;
    let metadata = tokio::fs::metadata(&strm_path).await?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    Ok(short_hash(&format!(
        "{}\0{raw_url}\0{}\0{modified}",
        cloud_cache_key(asset),
        metadata.len()
    )))
}

pub fn short_hash(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub async fn local_asset_path(state: &AppState, raw: &str) -> Result<PathBuf> {
    let settings = settings::load_settings(&state.config).await?;
    ensure_asset_path_allowed_with_roots_async(&state.config, raw, &settings.all_media_roots())
        .await
}

pub async fn asset_local_processing_path(state: &AppState, asset: &Asset) -> Result<PathBuf> {
    if is_qms_strm_uri(&asset.path) {
        ensure_qms_asset_cached(state, asset).await
    } else {
        local_asset_path(state, &asset.path).await
    }
}

pub async fn qms_strm_path_for_asset(state: &AppState, asset_path: &str) -> Result<PathBuf> {
    let (mount_name, relative_path) = parse_qms_strm_uri(asset_path)?;
    let settings = settings::load_settings(&state.config).await?;
    let source = settings
        .media_sources
        .iter()
        .find(|source| {
            source.enabled && source.provider == "qmediasync" && source.mount_name == mount_name
        })
        .cloned()
        .or_else(|| {
            qmediasync_scan_sources(&settings, "comic")
                .into_iter()
                .find(|source| source.mount_name == mount_name)
        })
        .ok_or_else(|| AppError::NotFound(format!("qmediasync source {mount_name} not found")))?;
    let strm_path = PathBuf::from(&source.root).join(relative_path.trim_start_matches('/'));
    ensure_asset_path_allowed_with_roots_async(
        &state.config,
        &strm_path.to_string_lossy(),
        &settings.all_media_roots(),
    )
    .await
}

pub fn read_qms_strm_url(strm_path: &Path) -> Result<String> {
    let file = std::fs::File::open(strm_path)?;
    let mut bytes = Vec::new();
    file.take(MAX_STRM_FILE_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_STRM_FILE_BYTES {
        return Err(AppError::BadRequest(format!(
            "STRM file exceeds {MAX_STRM_FILE_BYTES} bytes"
        )));
    }
    let raw = std::str::from_utf8(&bytes)
        .map_err(|err| AppError::BadRequest(format!("STRM file is not valid UTF-8: {err}")))?;
    let url = raw
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .ok_or_else(|| AppError::BadRequest("STRM file does not contain a URL".to_string()))?;
    let parsed =
        Url::parse(url).map_err(|err| AppError::BadRequest(format!("invalid STRM URL: {err}")))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(AppError::BadRequest(
            "STRM URL must use http or https".to_string(),
        ));
    }
    Ok(url.to_string())
}

pub async fn qms_target_url_for_asset(state: &AppState, asset: &Asset) -> Result<String> {
    let path = qms_strm_path_for_asset(state, &asset.path).await?;
    let target = read_qms_strm_url_async(path).await?;
    Ok(target)
}

pub async fn asset_route_info(state: &AppState, asset: &Asset) -> Result<AssetRouteInfo> {
    if !is_qms_strm_uri(&asset.path) {
        return Ok(AssetRouteInfo {
            asset_id: asset.id,
            provider: "local".to_string(),
            policy: "local".to_string(),
            policy_label: "local".to_string(),
            transfer: "app-proxy".to_string(),
            route_label: "local -> app -> browser".to_string(),
            via_qmediasync: false,
            via_app: true,
            qmediasync_host: None,
            target_host: None,
            note: None,
        });
    }

    let settings = settings::load_settings(&state.config).await?;
    let target_url = qms_target_url_for_asset(state, asset).await?;
    let parsed = Url::parse(&target_url)
        .map_err(|err| AppError::BadRequest(format!("invalid STRM URL: {err}")))?;
    resolve_public_target(&parsed).await?;
    let qms_host = url_host(&settings.qmediasync.base_url);
    let target_host = url_host(&target_url);
    let via_qmediasync = qms_host.is_some() && qms_host == target_host;
    Ok(AssetRouteInfo {
        asset_id: asset.id,
        provider: "qmediasync".to_string(),
        policy: "qmediasync-strm".to_string(),
        policy_label: "qmediasync STRM".to_string(),
        transfer: "app-proxy".to_string(),
        route_label: "115 -> qmediasync -> STRM -> app-cache -> browser".to_string(),
        via_qmediasync,
        via_app: true,
        qmediasync_host: qms_host,
        target_host,
        note: Some("qmediasync-strm-link".to_string()),
    })
}

fn url_host(value: &str) -> Option<String> {
    Url::parse(value)
        .ok()
        .and_then(|url| url.host_str().map(ToOwned::to_owned))
}

pub async fn stream_qms_asset(
    state: Arc<AppState>,
    asset: Asset,
    headers: HeaderMap,
) -> Result<Response> {
    let target_url = qms_target_url_for_asset(&state, &asset).await?;
    let range = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let if_range = headers
        .get(header::IF_RANGE)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let response =
        send_strm_get_with_range(&target_url, range.as_deref(), if_range.as_deref()).await?;
    let status = response.status();
    if !(status.is_success() || status == StatusCode::RANGE_NOT_SATISFIABLE) {
        return Err(AppError::Other(format!(
            "qmediasync stream request failed: {status}"
        )));
    }
    let mut builder = Response::builder()
        .status(status)
        .header(header::CACHE_CONTROL, "private, no-store");
    for name in [
        header::CONTENT_TYPE,
        header::CONTENT_LENGTH,
        header::CONTENT_RANGE,
        header::ACCEPT_RANGES,
        header::ETAG,
        header::LAST_MODIFIED,
    ] {
        if let Some(value) = response.headers().get(&name) {
            builder = builder.header(name, value);
        }
    }
    builder
        .body(Body::from_stream(response.bytes_stream()))
        .map_err(|e| AppError::Other(e.to_string()))
}

pub async fn generate_qms_thumbnail(
    state: &AppState,
    asset: &Asset,
    cache_path: &Path,
    size: u32,
) -> Result<()> {
    let cache_path = cache_path.to_path_buf();
    let lock = cache_write_lock(&cache_path)?;
    let _guard = lock.lock().await;
    if valid_remote_jpeg_cache(&cache_path).await? {
        return Ok(());
    }

    let raw_url = qms_target_url_for_asset(state, asset).await?;
    let response = send_strm_get(&raw_url).await?;
    if !response.status().is_success() {
        return Err(AppError::Other(format!(
            "qmediasync thumbnail download failed: {}",
            response.status()
        )));
    }
    reject_oversized_response(&response, MAX_REMOTE_THUMBNAIL_SOURCE_BYTES, "thumbnail")?;
    let expected_length = response.content_length();
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if bytes.len().saturating_add(chunk.len()) > MAX_REMOTE_THUMBNAIL_SOURCE_BYTES as usize {
            return Err(AppError::Other(format!(
                "qmediasync thumbnail source exceeds {} bytes",
                MAX_REMOTE_THUMBNAIL_SOURCE_BYTES
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    if let Some(expected) = expected_length {
        if bytes.len() as u64 != expected {
            return Err(AppError::Other(format!(
                "qmediasync thumbnail source length mismatch: expected {expected}, received {}",
                bytes.len()
            )));
        }
    }

    let temp_path = unique_temp_path(&cache_path)?;
    let generated = match tokio::task::spawn_blocking({
        let temp_path = temp_path.clone();
        move || -> std::result::Result<(), String> {
            let generated = (|| -> std::result::Result<(), String> {
                let mut reader = image::ImageReader::new(Cursor::new(&bytes))
                    .with_guessed_format()
                    .map_err(|e| e.to_string())?;
                let mut limits = image::Limits::default();
                limits.max_alloc = Some(MAX_IMAGE_DECODE_ALLOC_BYTES);
                reader.limits(limits);
                let image = reader.decode().map_err(|e| e.to_string())?;
                let thumb = image.thumbnail(size, size).to_rgb8();
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&temp_path)
                    .map_err(|e| e.to_string())?;
                let mut writer = BufWriter::new(file);
                {
                    let mut encoder =
                        image::codecs::jpeg::JpegEncoder::new_with_quality(&mut writer, 84);
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
                    std::fs::remove_file(&cache_path).map_err(|e| e.to_string())?;
                }
                std::fs::rename(&temp_path, &cache_path).map_err(|e| e.to_string())
            })();
            if generated.is_err() {
                if let Err(err) = std::fs::remove_file(&temp_path) {
                    if err.kind() != std::io::ErrorKind::NotFound {
                        tracing::warn!(path = %temp_path.display(), error = %err, "failed to clean thumbnail temp file");
                    }
                }
            }
            generated
        }
    })
    .await
    {
        Ok(generated) => generated,
        Err(err) => {
            remove_temp_file(&temp_path).await;
            return Err(AppError::Other(err.to_string()));
        }
    };
    if let Err(err) = generated {
        remove_temp_file(&temp_path).await;
        return Err(AppError::Other(err));
    }
    Ok(())
}

async fn reserve_cloud_cache_capacity(
    cache_dir: &Path,
    quota: u64,
    expected_length: Option<u64>,
) -> Result<CloudCacheReservation> {
    let cache_dir = cache_dir.to_path_buf();
    if expected_length.is_some_and(|length| length > MAX_CLOUD_CACHE_FILE_BYTES) {
        return Err(AppError::Other(format!(
            "qmediasync cloud cache file exceeds {} bytes",
            MAX_CLOUD_CACHE_FILE_BYTES
        )));
    }
    let dir_modified = tokio::fs::metadata(&cache_dir)
        .await
        .ok()
        .and_then(|metadata| metadata.modified().ok());
    let should_scan = {
        let mut quotas = CLOUD_CACHE_QUOTAS
            .lock()
            .map_err(|_| AppError::Other("cloud cache quota registry poisoned".to_string()))?;
        let state = quotas.entry(cache_dir.clone()).or_default();
        !state.initialized
            || state.observed_dir_modified != dir_modified
            || state
                .last_scan
                .is_none_or(|last_scan| last_scan.elapsed() >= CACHE_USAGE_RESCAN_INTERVAL)
    };
    if should_scan {
        resync_cloud_cache_usage(&cache_dir, dir_modified).await?;
    }

    if let Some(reservation) = try_reserve_cloud_capacity(&cache_dir, quota, expected_length)? {
        return Ok(reservation);
    }
    if !should_scan {
        let refreshed_modified = tokio::fs::metadata(&cache_dir)
            .await
            .ok()
            .and_then(|metadata| metadata.modified().ok());
        resync_cloud_cache_usage(&cache_dir, refreshed_modified).await?;
        if let Some(reservation) = try_reserve_cloud_capacity(&cache_dir, quota, expected_length)? {
            return Ok(reservation);
        }
    }

    let quotas = CLOUD_CACHE_QUOTAS
        .lock()
        .map_err(|_| AppError::Other("cloud cache quota registry poisoned".to_string()))?;
    let state = quotas
        .get(&cache_dir)
        .ok_or_else(|| AppError::Other("cloud cache quota state is missing".to_string()))?;
    let used = state.committed.saturating_add(state.reserved);
    let available = quota.saturating_sub(used);
    let requested = expected_length
        .unwrap_or_else(|| MAX_CLOUD_CACHE_FILE_BYTES.min(available))
        .max(1);
    Err(AppError::Other(format!(
        "cloud cache quota exceeded: {used} bytes used or reserved, {requested} requested, {quota} allowed; remove individual cached files manually or raise CLOUD_CACHE_MAX_BYTES"
    )))
}

async fn resync_cloud_cache_usage(
    cache_dir: &Path,
    observed_dir_modified: Option<SystemTime>,
) -> Result<()> {
    let cache_dir = cache_dir.to_path_buf();
    let scan_generation = {
        let mut quotas = CLOUD_CACHE_QUOTAS
            .lock()
            .map_err(|_| AppError::Other("cloud cache quota registry poisoned".to_string()))?;
        quotas.entry(cache_dir.clone()).or_default().generation
    };
    let usage_dir = cache_dir.clone();
    let current_usage = tokio::task::spawn_blocking(move || cloud_cache_usage(&usage_dir))
        .await
        .map_err(|err| AppError::Other(format!("cloud cache usage task failed: {err}")))??;
    let mut quotas = CLOUD_CACHE_QUOTAS
        .lock()
        .map_err(|_| AppError::Other("cloud cache quota registry poisoned".to_string()))?;
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

fn try_reserve_cloud_capacity(
    cache_dir: &Path,
    quota: u64,
    expected_length: Option<u64>,
) -> Result<Option<CloudCacheReservation>> {
    let mut quotas = CLOUD_CACHE_QUOTAS
        .lock()
        .map_err(|_| AppError::Other("cloud cache quota registry poisoned".to_string()))?;
    let state = quotas.entry(cache_dir.to_path_buf()).or_default();
    let used = state.committed.saturating_add(state.reserved);
    let available = quota.saturating_sub(used);
    let requested = expected_length
        .unwrap_or_else(|| MAX_CLOUD_CACHE_FILE_BYTES.min(available))
        .max(1);
    if requested > MAX_CLOUD_CACHE_FILE_BYTES {
        return Err(AppError::Other(format!(
            "qmediasync cloud cache file exceeds {} bytes",
            MAX_CLOUD_CACHE_FILE_BYTES
        )));
    }
    if requested > available {
        return Ok(None);
    }
    state.reserved = state.reserved.saturating_add(requested);
    state.generation = state.generation.wrapping_add(1);
    Ok(Some(CloudCacheReservation {
        cache_dir: cache_dir.to_path_buf(),
        reserved: requested,
        active: true,
    }))
}

fn cloud_cache_usage(cache_dir: &Path) -> std::io::Result<u64> {
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

fn cloud_cache_needs_revalidation(cache_path: &Path) -> Result<bool> {
    let mut validators = CLOUD_CACHE_VALIDATORS
        .lock()
        .map_err(|_| AppError::Other("cloud cache validator registry poisoned".to_string()))?;
    if let Some(validator) = validators.get(cache_path) {
        return Ok(validator.checked_at.elapsed() >= CLOUD_CACHE_REVALIDATE_INTERVAL);
    }
    if validators.len() >= VALIDATED_CLOUD_CACHE_LIMIT {
        if let Some(oldest) = validators
            .iter()
            .min_by_key(|(_, validator)| validator.checked_at)
            .map(|(path, _)| path.clone())
        {
            validators.remove(&oldest);
        }
    }
    validators.insert(
        cache_path.to_path_buf(),
        CloudCacheValidator {
            etag: None,
            last_modified: None,
            checked_at: Instant::now(),
        },
    );
    Ok(false)
}

fn cloud_cache_validator(cache_path: &Path) -> Result<Option<CloudCacheValidator>> {
    Ok(CLOUD_CACHE_VALIDATORS
        .lock()
        .map_err(|_| AppError::Other("cloud cache validator registry poisoned".to_string()))?
        .get(cache_path)
        .cloned())
}

fn remember_cloud_cache_validator(
    cache_path: &Path,
    headers: &reqwest::header::HeaderMap,
) -> Result<()> {
    let mut validators = CLOUD_CACHE_VALIDATORS
        .lock()
        .map_err(|_| AppError::Other("cloud cache validator registry poisoned".to_string()))?;
    let previous = validators.get(cache_path).cloned();
    let header_value = |name: reqwest::header::HeaderName| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned)
    };
    validators.insert(
        cache_path.to_path_buf(),
        CloudCacheValidator {
            etag: header_value(reqwest::header::ETAG)
                .or_else(|| previous.as_ref().and_then(|value| value.etag.clone())),
            last_modified: header_value(reqwest::header::LAST_MODIFIED).or_else(|| {
                previous
                    .as_ref()
                    .and_then(|value| value.last_modified.clone())
            }),
            checked_at: Instant::now(),
        },
    );
    Ok(())
}

pub async fn ensure_qms_asset_cached(state: &AppState, asset: &Asset) -> Result<PathBuf> {
    let strm_path = qms_strm_path_for_asset(state, &asset.path).await?;
    let cache_dir = state.config.data_dir.join("cloud-cache");
    tokio::fs::create_dir_all(&cache_dir).await?;
    for _attempt in 0..3 {
        let raw_url = read_qms_strm_url_async(strm_path.clone()).await?;
        let ext = qms_cache_extension(&strm_path, &raw_url);
        let cache_key = short_hash(&format!("{}\0{raw_url}", cloud_cache_key(asset)));
        let cache_path = cache_dir.join(format!("{cache_key}.{ext}"));
        if valid_cloud_cache(&cache_path, &ext).await?
            && !cloud_cache_needs_revalidation(&cache_path)?
        {
            return Ok(cache_path);
        }

        let lock = cache_write_lock(&cache_path)?;
        let _guard = lock.lock().await;
        if valid_cloud_cache(&cache_path, &ext).await?
            && !cloud_cache_needs_revalidation(&cache_path)?
        {
            return Ok(cache_path);
        }
        let _download_permit = CLOUD_CACHE_DOWNLOADS
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| AppError::Other("cloud cache downloader is closed".to_string()))?;
        if valid_cloud_cache(&cache_path, &ext).await?
            && !cloud_cache_needs_revalidation(&cache_path)?
        {
            return Ok(cache_path);
        }
        let fresh_url = read_qms_strm_url_async(strm_path.clone()).await?;
        if fresh_url != raw_url {
            continue;
        }
        let existing_valid = valid_cloud_cache(&cache_path, &ext).await?;
        let replaced_bytes = tokio::fs::metadata(&cache_path)
            .await
            .ok()
            .filter(|metadata| metadata.is_file())
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let validator = existing_valid
            .then(|| cloud_cache_validator(&cache_path))
            .transpose()?
            .flatten();
        let response = send_strm_get_conditional(&fresh_url, validator.as_ref()).await?;
        if response.status() == StatusCode::NOT_MODIFIED && existing_valid {
            remember_cloud_cache_validator(&cache_path, response.headers())?;
            return Ok(cache_path);
        }
        if !response.status().is_success() {
            return Err(AppError::Other(format!(
                "qmediasync cache download failed: {}",
                response.status()
            )));
        }
        reject_oversized_response(&response, MAX_CLOUD_CACHE_FILE_BYTES, "cloud cache")?;
        let response_headers = response.headers().clone();
        let expected_length = response.content_length();
        let reservation = reserve_cloud_cache_capacity(
            &cache_dir,
            state.config.cloud_cache_max_bytes,
            expected_length,
        )
        .await?;
        let download_limit = reservation.limit();
        let temp_path = unique_temp_path(&cache_path)?;
        let mut temp_cleanup = TempFileCleanup::new(temp_path.clone());
        let mut file = tokio::fs::File::create_new(&temp_path).await?;
        let mut stream = response.bytes_stream();
        let mut downloaded = 0_u64;
        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(err) => {
                    drop(file);
                    remove_temp_file(&temp_path).await;
                    return Err(err.into());
                }
            };
            downloaded = downloaded.saturating_add(chunk.len() as u64);
            if downloaded > download_limit {
                drop(file);
                remove_temp_file(&temp_path).await;
                return Err(AppError::Other(format!(
                    "qmediasync cloud cache download exceeds its reserved {download_limit} bytes"
                )));
            }
            if let Err(err) = file.write_all(&chunk).await {
                drop(file);
                remove_temp_file(&temp_path).await;
                return Err(err.into());
            }
        }
        if downloaded == 0 {
            drop(file);
            remove_temp_file(&temp_path).await;
            return Err(AppError::Other(
                "qmediasync cloud cache download was empty".to_string(),
            ));
        }
        if let Some(expected) = expected_length {
            if downloaded != expected {
                drop(file);
                remove_temp_file(&temp_path).await;
                return Err(AppError::Other(format!(
                    "qmediasync cloud cache length mismatch: expected {expected}, received {downloaded}"
                )));
            }
        }
        if let Err(err) = file.flush().await {
            drop(file);
            remove_temp_file(&temp_path).await;
            return Err(err.into());
        }
        if let Err(err) = file.sync_all().await {
            drop(file);
            remove_temp_file(&temp_path).await;
            return Err(err.into());
        }
        drop(file);
        if let Err(err) = validate_cloud_cache(&temp_path, &ext).await {
            remove_temp_file(&temp_path).await;
            return Err(err);
        }
        if tokio::fs::try_exists(&cache_path).await? {
            tokio::fs::remove_file(&cache_path).await?;
        }
        if let Err(err) = tokio::fs::rename(&temp_path, &cache_path).await {
            remove_temp_file(&temp_path).await;
            return Err(err.into());
        }
        temp_cleanup.disarm();
        let observed_dir_modified = tokio::fs::metadata(&cache_dir)
            .await
            .ok()
            .and_then(|metadata| metadata.modified().ok());
        reservation.commit(downloaded, replaced_bytes, observed_dir_modified);
        remember_validated_cloud_cache(&cache_path).await?;
        remember_cloud_cache_validator(&cache_path, &response_headers)?;
        return Ok(cache_path);
    }
    Err(AppError::Other(
        "STRM target changed repeatedly while waiting for a cache download slot".to_string(),
    ))
}

async fn read_qms_strm_url_async(path: PathBuf) -> Result<String> {
    tokio::task::spawn_blocking(move || read_qms_strm_url(&path))
        .await
        .map_err(|err| AppError::Other(err.to_string()))?
}

async fn send_strm_get(raw_url: &str) -> Result<reqwest::Response> {
    send_strm_request(raw_url, None, None, None).await
}

async fn send_strm_get_with_range(
    raw_url: &str,
    range: Option<&str>,
    if_range: Option<&str>,
) -> Result<reqwest::Response> {
    send_strm_request(raw_url, range, if_range, None).await
}

async fn send_strm_get_conditional(
    raw_url: &str,
    validator: Option<&CloudCacheValidator>,
) -> Result<reqwest::Response> {
    send_strm_request(raw_url, None, None, validator).await
}

async fn send_strm_request(
    raw_url: &str,
    range: Option<&str>,
    if_range: Option<&str>,
    validator: Option<&CloudCacheValidator>,
) -> Result<reqwest::Response> {
    let url = Url::parse(raw_url)
        .map_err(|err| AppError::BadRequest(format!("invalid STRM URL: {err}")))?;
    let addresses = resolve_public_target(&url).await?;
    let host = url
        .host_str()
        .ok_or_else(|| AppError::BadRequest("STRM URL has no host".to_string()))?;
    let client = strm_client(&url, host, &addresses)?;
    let mut request = client
        .get(url)
        .header(reqwest::header::ACCEPT_ENCODING, "identity");
    if let Some(range) = range {
        request = request.header(reqwest::header::RANGE, range);
    }
    if let Some(if_range) = if_range {
        request = request.header(reqwest::header::IF_RANGE, if_range);
    }
    if let Some(validator) = validator {
        if let Some(etag) = validator.etag.as_deref() {
            request = request.header(reqwest::header::IF_NONE_MATCH, etag);
        }
        if let Some(last_modified) = validator.last_modified.as_deref() {
            request = request.header(reqwest::header::IF_MODIFIED_SINCE, last_modified);
        }
    }
    let response = request.send().await?;
    if response.status().is_redirection() {
        return Err(AppError::BadRequest(
            "STRM target redirects are disabled".to_string(),
        ));
    }
    Ok(response)
}

fn strm_client(url: &Url, host: &str, addresses: &[SocketAddr]) -> Result<reqwest::Client> {
    let mut address_keys = addresses
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    address_keys.sort();
    let key = format!(
        "{}://{}:{}|{}",
        url.scheme(),
        host,
        url.port_or_known_default().unwrap_or_default(),
        address_keys.join(",")
    );
    let mut clients = STRM_CLIENTS
        .lock()
        .map_err(|_| AppError::Other("STRM client cache poisoned".to_string()))?;
    if let Some((client, used_at)) = clients.get_mut(&key) {
        *used_at = Instant::now();
        return Ok(client.clone());
    }
    let client = reqwest::Client::builder()
        .user_agent("LocalMediaShelf/0.1 (+private local deployment)")
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .connect_timeout(STRM_CONNECT_TIMEOUT)
        .timeout(STRM_REQUEST_TIMEOUT)
        .resolve_to_addrs(host, addresses)
        .build()?;
    if clients.len() >= STRM_CLIENT_CACHE_LIMIT {
        if let Some(oldest) = clients
            .iter()
            .min_by_key(|(_, (_, used_at))| *used_at)
            .map(|(key, _)| key.clone())
        {
            clients.remove(&oldest);
        }
    }
    clients.insert(key, (client.clone(), Instant::now()));
    Ok(client)
}

async fn resolve_public_target(url: &Url) -> Result<Vec<SocketAddr>> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(AppError::BadRequest(
            "STRM URL must use http or https".to_string(),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(AppError::BadRequest(
            "STRM URL credentials are not allowed".to_string(),
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| AppError::BadRequest("STRM URL has no host".to_string()))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| AppError::BadRequest("STRM URL has no valid port".to_string()))?;
    let addresses = tokio::time::timeout(STRM_DNS_TIMEOUT, tokio::net::lookup_host((host, port)))
        .await
        .map_err(|_| AppError::BadRequest("STRM host resolution timed out".to_string()))?
        .map_err(|err| AppError::BadRequest(format!("STRM host resolution failed: {err}")))?
        .collect::<Vec<_>>();
    if addresses.is_empty() {
        return Err(AppError::BadRequest(
            "STRM host resolved to no addresses".to_string(),
        ));
    }
    if let Some(address) = addresses.iter().find(|address| !is_public_ip(address.ip())) {
        return Err(AppError::BadRequest(format!(
            "STRM target address {} is not public",
            address.ip()
        )));
    }
    Ok(addresses)
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_public_ipv4(ip),
        IpAddr::V6(ip) => is_public_ipv6(ip),
    }
}

fn is_public_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_multicast()
        || ip.is_broadcast()
        || a == 0
        || a == 100 && (64..=127).contains(&b)
        || a == 192 && b == 0 && c == 0
        || a == 192 && b == 0 && c == 2
        || a == 198 && b == 18
        || a == 198 && b == 19
        || a == 198 && b == 51 && c == 100
        || a == 203 && b == 0 && c == 113
        || a >= 240)
}

fn is_public_ipv6(ip: Ipv6Addr) -> bool {
    let segments = ip.segments();
    if let Some(ipv4) = ip.to_ipv4() {
        return is_public_ipv4(ipv4);
    }
    if (segments[0] & 0xe000) != 0x2000 {
        return false;
    }
    if segments[0] == 0x2001 && (segments[1] <= 0x01ff || segments[1] == 0x0db8) {
        return false;
    }
    if segments[0] == 0x2002 {
        return false;
    }
    if segments[0] == 0x3fff && segments[1] < 0x1000 {
        return false;
    }
    true
}

fn cache_write_lock(path: &Path) -> Result<Arc<AsyncMutex<()>>> {
    let mut locks = CACHE_WRITE_LOCKS
        .lock()
        .map_err(|_| AppError::Other("cache write lock registry poisoned".to_string()))?;
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
        return Ok(lock);
    }
    let lock = Arc::new(AsyncMutex::new(()));
    locks.insert(path.to_path_buf(), Arc::downgrade(&lock));
    Ok(lock)
}

fn unique_temp_path(final_path: &Path) -> Result<PathBuf> {
    let file_name = final_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| AppError::Other("cache path has no valid file name".to_string()))?;
    Ok(final_path.with_file_name(format!(".{file_name}.{}.part", Uuid::new_v4())))
}

fn reject_oversized_response(response: &reqwest::Response, limit: u64, label: &str) -> Result<()> {
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(AppError::Other(format!(
            "qmediasync {label} exceeds {limit} bytes"
        )));
    }
    Ok(())
}

async fn valid_cloud_cache(path: &Path, extension: &str) -> Result<bool> {
    let metadata = match tokio::fs::metadata(path).await {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err.into()),
    };
    if !metadata.is_file() || metadata.len() == 0 {
        return Ok(false);
    }
    if !matches!(
        extension.to_ascii_lowercase().as_str(),
        "zip" | "cbz" | "epub"
    ) {
        return Ok(true);
    }
    let fingerprint = (metadata.len(), metadata.modified().ok());
    {
        let validated = VALIDATED_CLOUD_CACHES
            .lock()
            .map_err(|_| AppError::Other("cloud cache validation registry poisoned".to_string()))?;
        if validated.get(path) == Some(&fingerprint) {
            return Ok(true);
        }
    }
    if let Err(err) = validate_cloud_cache(path, extension).await {
        tracing::warn!(path = %path.display(), error = %err, "cached cloud archive failed validation");
        return Ok(false);
    }
    let mut validated = VALIDATED_CLOUD_CACHES
        .lock()
        .map_err(|_| AppError::Other("cloud cache validation registry poisoned".to_string()))?;
    if validated.len() >= VALIDATED_CLOUD_CACHE_LIMIT && !validated.contains_key(path) {
        validated.clear();
    }
    validated.insert(path.to_path_buf(), fingerprint);
    Ok(true)
}

async fn validate_cloud_cache(path: &Path, extension: &str) -> Result<()> {
    if matches!(
        extension.to_ascii_lowercase().as_str(),
        "zip" | "cbz" | "epub"
    ) {
        let path = path.to_path_buf();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let file = std::fs::File::open(path)?;
            let _archive = crate::archive::open_media_zip(file, "downloaded archive")?;
            Ok(())
        })
        .await
        .map_err(|err| AppError::Other(err.to_string()))??;
    }
    Ok(())
}

async fn valid_jpeg_cache(path: &Path) -> Result<bool> {
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

async fn valid_remote_jpeg_cache(path: &Path) -> Result<bool> {
    if !valid_jpeg_cache(path).await? {
        return Ok(false);
    }
    let modified = tokio::fs::metadata(path).await?.modified().ok();
    Ok(modified.is_some_and(|modified| {
        SystemTime::now()
            .duration_since(modified)
            .unwrap_or_default()
            < REMOTE_DERIVED_CACHE_MAX_AGE
    }))
}

async fn remember_validated_cloud_cache(path: &Path) -> Result<()> {
    let metadata = tokio::fs::metadata(path).await?;
    let mut validated = VALIDATED_CLOUD_CACHES
        .lock()
        .map_err(|_| AppError::Other("cloud cache validation registry poisoned".to_string()))?;
    if validated.len() >= VALIDATED_CLOUD_CACHE_LIMIT && !validated.contains_key(path) {
        validated.clear();
    }
    validated.insert(
        path.to_path_buf(),
        (metadata.len(), metadata.modified().ok()),
    );
    Ok(())
}

async fn remove_temp_file(path: &Path) {
    if let Err(err) = tokio::fs::remove_file(path).await {
        if err.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(path = %path.display(), error = %err, "failed to remove cache temp file");
        }
    }
}

fn qms_cache_extension(strm_path: &Path, raw_url: &str) -> String {
    let file_name = strm_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if let Some(stem) = file_name.strip_suffix(".strm") {
        if let Some((_, ext)) = stem.rsplit_once('.') {
            if !ext.trim().is_empty() {
                return ext.to_string();
            }
        }
    }
    Url::parse(raw_url)
        .ok()
        .and_then(|url| {
            Path::new(url.path())
                .extension()
                .and_then(|value| value.to_str())
                .map(ToOwned::to_owned)
        })
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "bin".to_string())
}

fn system_time_key(time: SystemTime) -> Option<u128> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|value| value.as_nanos())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn qms_uri_round_trips_mount_and_path() {
        let uri = qms_strm_uri("qms", "漫画/测试.cbz.strm");
        assert_eq!(uri, "qms-strm://qms/漫画/测试.cbz.strm");
        let (mount, path) = parse_qms_strm_uri(&uri).unwrap();
        assert_eq!(mount, "qms");
        assert_eq!(path, "/漫画/测试.cbz.strm");
    }

    #[test]
    fn normalizes_remote_paths() {
        assert_eq!(normalize_remote_path(""), "/");
        assert_eq!(normalize_remote_path("漫画\\合集\\"), "/漫画/合集");
    }

    #[test]
    fn cloud_cache_key_changes_with_meta() {
        let now = Utc::now();
        let base = Asset {
            id: 1,
            work_id: 1,
            path: "qms-strm://m/a.cbz.strm".to_string(),
            mime: "application/vnd.comicbook+zip".to_string(),
            role: "archive".to_string(),
            variant: None,
            position: Some(0),
            size: Some(10),
            meta_json: "{\"target_url_hash\":\"1\"}".to_string(),
            created_at: now,
        };
        let mut changed = base.clone();
        changed.meta_json = "{\"target_url_hash\":\"2\"}".to_string();
        assert_ne!(cloud_cache_key(&base), cloud_cache_key(&changed));
    }

    #[test]
    fn qms_cache_extension_prefers_strm_target_suffix() {
        assert_eq!(
            qms_cache_extension(Path::new("book.cbz.strm"), "https://example.test/download"),
            "cbz"
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
    fn cloud_cache_validator_expires_and_records_response_validators() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("cached.cbz");
        CLOUD_CACHE_VALIDATORS.lock().unwrap().remove(&path);
        assert!(!cloud_cache_needs_revalidation(&path).unwrap());
        CLOUD_CACHE_VALIDATORS
            .lock()
            .unwrap()
            .get_mut(&path)
            .unwrap()
            .checked_at = Instant::now() - CLOUD_CACHE_REVALIDATE_INTERVAL - Duration::from_secs(1);
        assert!(cloud_cache_needs_revalidation(&path).unwrap());

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::ETAG,
            reqwest::header::HeaderValue::from_static("\"v2\""),
        );
        headers.insert(
            reqwest::header::LAST_MODIFIED,
            reqwest::header::HeaderValue::from_static("Wed, 15 Jul 2026 12:00:00 GMT"),
        );
        remember_cloud_cache_validator(&path, &headers).unwrap();
        let validator = cloud_cache_validator(&path).unwrap().unwrap();
        assert_eq!(validator.etag.as_deref(), Some("\"v2\""));
        assert_eq!(
            validator.last_modified.as_deref(),
            Some("Wed, 15 Jul 2026 12:00:00 GMT")
        );
        assert!(!cloud_cache_needs_revalidation(&path).unwrap());
        CLOUD_CACHE_VALIDATORS.lock().unwrap().remove(&path);
    }

    #[test]
    fn strm_ssrf_filter_rejects_non_public_addresses() {
        for address in [
            "127.0.0.1",
            "10.0.0.1",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.1.1",
            "100.64.0.1",
            "224.0.0.1",
            "::1",
            "fc00::1",
            "fe80::1",
            "2001:db8::1",
            "::ffff:127.0.0.1",
            "::127.0.0.1",
            "64:ff9b::c0a8:1",
            "64:ff9b:1::c0a8:1",
            "2001::1",
            "2001:db8::1",
            "2002:c0a8:1::1",
            "3fff::1",
        ] {
            assert!(!is_public_ip(address.parse().unwrap()), "{address}");
        }
        for address in ["1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"] {
            assert!(is_public_ip(address.parse().unwrap()), "{address}");
        }
    }

    #[test]
    fn cache_temp_paths_are_unique_and_sibling_files() {
        let final_path = Path::new("cache/book.cbz");
        let first = unique_temp_path(final_path).unwrap();
        let second = unique_temp_path(final_path).unwrap();
        assert_ne!(first, second);
        assert_eq!(first.parent(), final_path.parent());
        assert!(first
            .file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with(".part"));
    }

    #[test]
    fn strm_reader_rejects_files_over_limit() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("oversized.strm");
        std::fs::write(&path, vec![b'x'; MAX_STRM_FILE_BYTES as usize + 1]).unwrap();

        let result = read_qms_strm_url(&path);
        assert!(
            matches!(result, Err(AppError::BadRequest(message)) if message.contains("exceeds"))
        );
    }

    #[tokio::test]
    async fn cloud_cache_quota_counts_files_and_concurrent_reservations() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("cached.bin"), [0_u8; 8]).unwrap();
        std::fs::write(temp.path().join(".download.part"), [0_u8; 1]).unwrap();
        assert_eq!(cloud_cache_usage(temp.path()).unwrap(), 9);

        let reservation = reserve_cloud_cache_capacity(temp.path(), 11, Some(2))
            .await
            .unwrap();
        assert!(reserve_cloud_cache_capacity(temp.path(), 11, Some(1))
            .await
            .is_err());
        drop(reservation);
        assert!(reserve_cloud_cache_capacity(temp.path(), 11, Some(2))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn cloud_cache_quota_resyncs_after_explicit_file_removal() {
        let temp = tempfile::tempdir().unwrap();
        let cached = temp.path().join("cached.bin");
        std::fs::write(&cached, [0_u8; 8]).unwrap();
        let reservation = reserve_cloud_cache_capacity(temp.path(), 10, Some(2))
            .await
            .unwrap();
        drop(reservation);

        std::fs::remove_file(cached).unwrap();
        assert!(reserve_cloud_cache_capacity(temp.path(), 2, Some(2))
            .await
            .is_ok());
    }

    #[test]
    fn temp_file_cleanup_removes_only_its_explicit_path() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(".cache.unique.part");
        std::fs::write(&path, b"partial").unwrap();

        drop(TempFileCleanup::new(path.clone()));
        assert!(!path.exists());
    }

    #[test]
    fn disarmed_temp_cleanup_preserves_published_file() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("cache.bin");
        std::fs::write(&path, b"published").unwrap();
        let mut cleanup = TempFileCleanup::new(path.clone());
        cleanup.disarm();
        drop(cleanup);

        assert!(path.exists());
    }
}
