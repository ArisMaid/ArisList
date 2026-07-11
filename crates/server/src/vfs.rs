use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;
use futures::StreamExt;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use url::Url;

use crate::error::{AppError, Result};
use crate::models::Asset;
use crate::security::ensure_asset_path_allowed_with_roots;
use crate::settings::{self, AppSettings, MediaSourceSettings};
use crate::AppState;

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

pub fn qms_strm_meta_json(
    mount_name: &str,
    source_root: &Path,
    strm_path: &Path,
    relative_path: &str,
    target_url: &str,
) -> Value {
    let metadata = std::fs::metadata(strm_path).ok();
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

pub fn short_hash(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub async fn local_asset_path(state: &AppState, raw: &str) -> Result<PathBuf> {
    let settings = settings::load_settings(&state.config).await?;
    ensure_asset_path_allowed_with_roots(&state.config, raw, &settings.all_media_roots())
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
    ensure_asset_path_allowed_with_roots(
        &state.config,
        &strm_path.to_string_lossy(),
        &settings.all_media_roots(),
    )
}

pub fn read_qms_strm_url(strm_path: &Path) -> Result<String> {
    let raw = std::fs::read_to_string(strm_path)?;
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
    read_qms_strm_url(&path)
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
    _headers: HeaderMap,
) -> Result<Response> {
    let target_url = qms_target_url_for_asset(&state, &asset).await?;
    Ok(Response::builder()
        .status(StatusCode::FOUND)
        .header(header::LOCATION, target_url)
        .header(header::CACHE_CONTROL, "private, max-age=300")
        .body(Body::empty())
        .map_err(|e| AppError::Other(e.to_string()))?)
}

pub async fn generate_qms_thumbnail(
    state: &AppState,
    asset: &Asset,
    cache_path: &Path,
    size: u32,
) -> Result<()> {
    let raw_url = qms_target_url_for_asset(state, asset).await?;
    let bytes = state.http.get(raw_url).send().await?.bytes().await?;
    let cache_path = cache_path.to_path_buf();
    tokio::task::spawn_blocking(move || -> std::result::Result<(), String> {
        let image = image::load_from_memory(&bytes).map_err(|e| e.to_string())?;
        let thumb = image.thumbnail(size, size).to_rgb8();
        let file = std::fs::File::create(cache_path).map_err(|e| e.to_string())?;
        let mut writer = std::io::BufWriter::new(file);
        let mut encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut writer, 84);
        encoder.encode_image(&thumb).map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| AppError::Other(e.to_string()))?
    .map_err(AppError::Other)
}

pub async fn ensure_qms_asset_cached(state: &AppState, asset: &Asset) -> Result<PathBuf> {
    let strm_path = qms_strm_path_for_asset(state, &asset.path).await?;
    let raw_url = read_qms_strm_url(&strm_path)?;
    let ext = qms_cache_extension(&strm_path, &raw_url);
    let cache_dir = state.config.data_dir.join("cloud-cache");
    tokio::fs::create_dir_all(&cache_dir).await?;
    let cache_path = cache_dir.join(format!("{}.{}", cloud_cache_key(asset), ext));
    if tokio::fs::try_exists(&cache_path).await? {
        return Ok(cache_path);
    }

    let response = state.http.get(raw_url).send().await?;
    if !response.status().is_success() {
        return Err(AppError::Other(format!(
            "qmediasync cache download failed: {}",
            response.status()
        )));
    }
    let mut file = tokio::fs::File::create(&cache_path).await?;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        file.write_all(&chunk?).await?;
    }
    file.flush().await?;
    Ok(cache_path)
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

fn system_time_key(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|value| value.as_secs())
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
}
