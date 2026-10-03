//! Remote archive manifest and lazy-entry preparation.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};

use flate2::read::DeflateDecoder;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use crate::error::{AppError, Result};
use crate::strm::cache::stable_key;
use crate::strm::http;

const MAX_CENTRAL_DIRECTORY_BYTES: u64 = 64 * 1024 * 1024;
const MAX_SUFFIX_PROBE_BYTES: u64 = 65_535 + 22;
const MAX_ARCHIVE_ENTRIES: usize = 50_000;
const MAX_ARCHIVE_ENTRY_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const ZIP_LOCAL_HEADER_BYTES: u64 = 30;
const MANIFEST_CACHE_LIMIT: usize = 256;

static REMOTE_MANIFESTS: LazyLock<Mutex<HashMap<String, RemoteArchiveManifest>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveSupport {
    Zip,
    SevenZip,
    Rar,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteArchiveEntry {
    pub name: String,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub compression: u16,
    pub local_header_offset: u64,
    pub encrypted: bool,
    pub directory: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteArchiveManifest {
    pub total_size: u64,
    pub source_version: String,
    pub range_supported: bool,
    pub entries: Vec<RemoteArchiveEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestProbe {
    Ready(RemoteArchiveManifest),
    RequiresFullCache { reason: String },
}

pub fn cached_manifest(raw_url: &str) -> Option<RemoteArchiveManifest> {
    REMOTE_MANIFESTS
        .lock()
        .ok()
        .and_then(|manifests| manifests.get(raw_url).cloned())
}

pub fn remember_manifest(raw_url: &str, manifest: RemoteArchiveManifest) {
    if let Ok(mut manifests) = REMOTE_MANIFESTS.lock() {
        if manifests.len() >= MANIFEST_CACHE_LIMIT && !manifests.contains_key(raw_url) {
            if let Some(key) = manifests.keys().next().cloned() {
                manifests.remove(&key);
            }
        }
        manifests.insert(raw_url.to_string(), manifest);
    }
}

fn persistent_manifest_key(raw_url: &str) -> String {
    format!("strm:manifest:{}", stable_key(&[raw_url]))
}

pub async fn load_persisted_manifest(
    db: &crate::db::Db,
    raw_url: &str,
) -> Result<Option<RemoteArchiveManifest>> {
    let row = sqlx::query(
        r#"
        SELECT source_size, pages_json
        FROM archive_manifest_cache
        WHERE cache_key = ?1
          AND created_at >= strftime('%Y-%m-%dT%H:%M:%fZ','now','-15 minutes')
        "#,
    )
    .bind(persistent_manifest_key(raw_url))
    .fetch_optional(db.pool())
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let source_size: i64 = row.get("source_size");
    if source_size < 0 {
        return Ok(None);
    }
    let pages_json: String = row.get("pages_json");
    let manifest = serde_json::from_str::<RemoteArchiveManifest>(&pages_json)
        .map_err(|error| AppError::Other(format!("stored STRM manifest is invalid: {error}")))?;
    if manifest.total_size != source_size as u64 || !manifest.range_supported {
        return Ok(None);
    }
    Ok(Some(manifest))
}

pub async fn persist_manifest(
    db: &crate::db::Db,
    raw_url: &str,
    manifest: &RemoteArchiveManifest,
    cache_limit: u64,
) -> Result<()> {
    if !manifest.range_supported {
        return Ok(());
    }
    let pages_json = serde_json::to_string(manifest)
        .map_err(|error| AppError::Other(format!("failed to serialize STRM manifest: {error}")))?;
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
    .bind(persistent_manifest_key(raw_url))
    .bind(i64::try_from(manifest.total_size).unwrap_or(i64::MAX))
    .bind(&manifest.source_version)
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
        if resident_bytes <= i64::try_from(cache_limit.max(1)).unwrap_or(i64::MAX) {
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
    Ok(())
}

pub fn support_for_extension(extension: &str) -> ArchiveSupport {
    match extension
        .trim_start_matches('.')
        .to_ascii_lowercase()
        .as_str()
    {
        "zip" | "cbz" => ArchiveSupport::Zip,
        "7z" | "cb7" => ArchiveSupport::SevenZip,
        "rar" => ArchiveSupport::Rar,
        _ => ArchiveSupport::Unsupported,
    }
}

pub fn unsupported_archive_error(extension: &str) -> AppError {
    AppError::BadRequest(format!("remote archive type .{extension} is not supported"))
}

pub async fn probe_manifest(raw_url: &str) -> Result<ManifestProbe> {
    let mut response = http::send_get(
        raw_url,
        Some(&format!("bytes=-{}", MAX_SUFFIX_PROBE_BYTES)),
        None,
        None,
        None,
    )
    .await?;
    let status = response.status();
    if status == StatusCode::RANGE_NOT_SATISFIABLE {
        return Ok(ManifestProbe::RequiresFullCache {
            reason: "remote source rejected the suffix range".to_string(),
        });
    }
    if !status.is_success() {
        return Err(remote_response_error("archive manifest probe", &response));
    }

    let total_size = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_content_range)
        .map(|(_, _, total)| total)
        .or_else(|| response.content_length())
        .ok_or_else(|| {
            AppError::Other("archive manifest response has no usable size".to_string())
        })?;
    let range_supported = status == StatusCode::PARTIAL_CONTENT
        && response
            .headers()
            .contains_key(reqwest::header::CONTENT_RANGE);
    if !range_supported {
        return Ok(ManifestProbe::RequiresFullCache {
            reason: "remote source does not provide byte ranges".to_string(),
        });
    }
    let (range_start, _, _) = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_content_range)
        .ok_or_else(|| {
            AppError::Other("archive manifest response has invalid Content-Range".to_string())
        })?;
    let mut suffix = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if suffix.len() as u64 + chunk.len() as u64 > MAX_SUFFIX_PROBE_BYTES {
            return Err(AppError::Other(
                "archive suffix probe exceeded its limit".to_string(),
            ));
        }
        suffix.extend_from_slice(&chunk);
    }
    let eocd = find_end_of_central_directory(&suffix).ok_or_else(|| {
        AppError::Other("remote source is not a readable ZIP archive".to_string())
    })?;
    let central_size = read_u32(&suffix, eocd + 12).unwrap_or_default() as u64;
    let central_offset = read_u32(&suffix, eocd + 16).unwrap_or_default() as u64;
    let entry_count = read_u16(&suffix, eocd + 10).unwrap_or_default() as usize;
    if central_size > MAX_CENTRAL_DIRECTORY_BYTES
        || entry_count > MAX_ARCHIVE_ENTRIES
        || central_size == u32::MAX as u64
        || central_offset == u32::MAX as u64
    {
        return Ok(ManifestProbe::RequiresFullCache {
            reason: "ZIP64 or oversized central directory requires full cache".to_string(),
        });
    }

    let central = if central_offset >= range_start
        && central_offset.saturating_add(central_size)
            <= range_start.saturating_add(suffix.len() as u64)
    {
        let start = (central_offset - range_start) as usize;
        suffix
            .get(start..start.saturating_add(central_size as usize))
            .ok_or_else(|| AppError::Other("ZIP central directory is truncated".to_string()))?
            .to_vec()
    } else {
        if central_size == 0 {
            Vec::new()
        } else {
            let end = central_offset
                .checked_add(central_size - 1)
                .ok_or_else(|| {
                    AppError::Other("ZIP central directory range overflow".to_string())
                })?;
            read_exact_range(raw_url, central_offset, end, total_size).await?
        }
    };
    if central.len() as u64 != central_size {
        return Err(AppError::Other(format!(
            "ZIP central directory length mismatch: expected {central_size}, received {}",
            central.len()
        )));
    }
    let entries = parse_central_directory(&central, entry_count)?;
    if entries
        .iter()
        .any(|entry| entry.encrypted || !matches!(entry.compression, 0 | 8))
    {
        return Ok(ManifestProbe::RequiresFullCache {
            reason: "encrypted or non-Stored/Deflate ZIP entries require local 7-Zip extraction"
                .to_string(),
        });
    }
    let source_version = manifest_source_version(raw_url, total_size, &suffix);
    Ok(ManifestProbe::Ready(RemoteArchiveManifest {
        total_size,
        source_version,
        range_supported: true,
        entries,
    }))
}

async fn read_exact_range(raw_url: &str, start: u64, end: u64, total: u64) -> Result<Vec<u8>> {
    let mut response = http::send_get(
        raw_url,
        Some(&format!("bytes={start}-{end}")),
        None,
        None,
        None,
    )
    .await?;
    if response.status() != StatusCode::PARTIAL_CONTENT {
        return Err(remote_response_error("archive range request", &response));
    }
    let range = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_content_range);
    if range != Some((start, end, total)) {
        return Err(AppError::Other(
            "archive response Content-Range mismatch".to_string(),
        ));
    }
    let expected = end - start + 1;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if (bytes.len() as u64).saturating_add(chunk.len() as u64) > expected {
            return Err(AppError::Other(
                "archive range response exceeded its limit".to_string(),
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() as u64 != expected {
        return Err(AppError::Other(
            "archive range response is truncated".to_string(),
        ));
    }
    Ok(bytes)
}

pub async fn prepare_entry(
    raw_url: &str,
    manifest: &RemoteArchiveManifest,
    entry: &RemoteArchiveEntry,
    destination: &Path,
) -> Result<PathBuf> {
    if entry.directory {
        return Err(AppError::BadRequest(
            "cannot prepare a directory entry".to_string(),
        ));
    }
    if entry.encrypted {
        return Err(AppError::BadRequest(
            "encrypted ZIP entries require a password preparation job".to_string(),
        ));
    }
    if entry.uncompressed_size > MAX_ARCHIVE_ENTRY_BYTES {
        return Err(AppError::BadRequest(
            "archive entry exceeds the safety limit".to_string(),
        ));
    }
    if tokio::fs::try_exists(destination).await? {
        return Ok(destination.to_path_buf());
    }
    if let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // A local header has two u16-sized variable fields. Include their maximum
    // extent in one bounded request, stopping before the next entry when known.
    let next_offset = manifest
        .entries
        .iter()
        .map(|other| other.local_header_offset)
        .filter(|offset| *offset > entry.local_header_offset)
        .min()
        .unwrap_or(manifest.total_size);
    let end_exclusive = entry
        .local_header_offset
        .checked_add(ZIP_LOCAL_HEADER_BYTES + 2 * u16::MAX as u64)
        .and_then(|value| value.checked_add(entry.compressed_size))
        .ok_or_else(|| AppError::Other("ZIP entry range overflow".to_string()))?
        .min(next_offset)
        .min(manifest.total_size);
    if end_exclusive <= entry.local_header_offset {
        return Err(AppError::Other(
            "ZIP entry offset is outside the archive".to_string(),
        ));
    }
    let bytes = read_exact_range(
        raw_url,
        entry.local_header_offset,
        end_exclusive - 1,
        manifest.total_size,
    )
    .await?;
    if bytes.len() < ZIP_LOCAL_HEADER_BYTES as usize || bytes.get(..4) != Some(b"PK\x03\x04") {
        return Err(AppError::Other("ZIP local header is invalid".to_string()));
    }
    let data_start = ZIP_LOCAL_HEADER_BYTES as usize
        + read_u16(&bytes, 26).unwrap_or_default() as usize
        + read_u16(&bytes, 28).unwrap_or_default() as usize;
    let data_end = data_start
        .checked_add(
            usize::try_from(entry.compressed_size)
                .map_err(|_| AppError::Other("ZIP entry is too large".to_string()))?,
        )
        .ok_or_else(|| AppError::Other("ZIP entry range overflow".to_string()))?;
    let compressed = bytes
        .get(data_start..data_end)
        .ok_or_else(|| AppError::Other("ZIP entry range length mismatch".to_string()))?
        .to_vec();

    let temporary = destination.with_extension(format!("tmp-{}", Uuid::new_v4().simple()));
    let destination_for_blocking = temporary.clone();
    let expected_size = entry.uncompressed_size;
    let compression = entry.compression;
    let write_result = match tokio::task::spawn_blocking(move || {
        write_entry_file(
            &destination_for_blocking,
            &compressed,
            compression,
            expected_size,
        )
    })
    .await
    {
        Ok(result) => result,
        Err(error) => {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(AppError::Other(format!(
                "archive entry extraction task failed: {error}"
            )));
        }
    };
    let written = match write_result {
        Ok(written) => written,
        Err(error) => {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(error);
        }
    };
    if written != expected_size {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(AppError::Other(format!(
            "archive entry length mismatch: expected {expected_size}, received {written}"
        )));
    }
    if let Err(error) = tokio::fs::rename(&temporary, destination).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error.into());
    }
    Ok(destination.to_path_buf())
}

pub async fn prepare_local_zip_entry(
    archive_path: &Path,
    entry_name: &str,
    destination: &Path,
) -> Result<PathBuf> {
    if tokio::fs::try_exists(destination).await? {
        return Ok(destination.to_path_buf());
    }
    if let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let archive_path = archive_path.to_path_buf();
    let entry_name = entry_name.to_string();
    let temporary = destination.with_extension(format!("tmp-{}", Uuid::new_v4().simple()));
    let temporary_for_blocking = temporary.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<u64> {
        let file = std::fs::File::open(archive_path)?;
        let mut archive = zip::ZipArchive::new(file)
            .map_err(|error| AppError::Other(format!("local ZIP entry open failed: {error}")))?;
        let mut entry = archive.by_name(&entry_name).map_err(|error| {
            AppError::NotFound(format!(
                "local archive entry {entry_name} not found: {error}"
            ))
        })?;
        if entry.is_dir() {
            return Err(AppError::BadRequest(
                "cannot prepare a directory entry".to_string(),
            ));
        }
        let declared_size = entry.size();
        if declared_size > MAX_ARCHIVE_ENTRY_BYTES {
            return Err(AppError::BadRequest(
                "archive entry exceeds the safety limit".to_string(),
            ));
        }
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(temporary_for_blocking)?;
        let mut buffer = [0_u8; 64 * 1024];
        let mut written = 0_u64;
        loop {
            let read = entry.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            written = written.saturating_add(read as u64);
            if written > MAX_ARCHIVE_ENTRY_BYTES {
                return Err(AppError::BadRequest(
                    "archive entry expanded beyond the safety limit".to_string(),
                ));
            }
            output.write_all(&buffer[..read])?;
        }
        output.flush()?;
        output.sync_all()?;
        if written != declared_size {
            return Err(AppError::Other(
                "local ZIP entry length mismatch".to_string(),
            ));
        }
        Ok(written)
    })
    .await
    .map_err(|error| AppError::Other(format!("local archive extraction task failed: {error}")))?;
    if let Err(error) = result {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error);
    }
    if let Err(error) = tokio::fs::rename(&temporary, destination).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error.into());
    }
    Ok(destination.to_path_buf())
}

pub async fn list_local_zip_entries(archive_path: &Path) -> Result<Vec<RemoteArchiveEntry>> {
    let archive_path = archive_path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&archive_path)?;
        let mut archive = zip::ZipArchive::new(file)
            .map_err(|error| AppError::Other(format!("local ZIP manifest open failed: {error}")))?;
        if archive.len() > MAX_ARCHIVE_ENTRIES {
            return Err(AppError::BadRequest(
                "ZIP archive contains too many entries".to_string(),
            ));
        }
        let mut entries = Vec::with_capacity(archive.len());
        for index in 0..archive.len() {
            let entry = archive.by_index(index).map_err(|error| {
                AppError::Other(format!("local ZIP manifest entry read failed: {error}"))
            })?;
            let name = entry.name().replace('\\', "/");
            if !safe_entry_name(&name) {
                return Err(AppError::BadRequest(format!(
                    "unsafe ZIP entry path: {name}"
                )));
            }
            entries.push(RemoteArchiveEntry {
                directory: entry.is_dir(),
                name,
                compressed_size: entry.compressed_size(),
                uncompressed_size: entry.size(),
                compression: 0,
                local_header_offset: 0,
                encrypted: false,
            });
        }
        Ok(entries)
    })
    .await
    .map_err(|error| AppError::Other(format!("local ZIP manifest task failed: {error}")))?
}

pub async fn seven_zip_available() -> bool {
    let path = std::env::var("STRM_7ZIP_PATH").unwrap_or_else(|_| "7z".to_string());
    tokio::process::Command::new(path)
        .arg("--help")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .await
        .map(|output| output.status.success())
        .unwrap_or(false)
}

pub async fn list_seven_zip_entries(archive_path: &Path) -> Result<Vec<RemoteArchiveEntry>> {
    let path = std::env::var("STRM_7ZIP_PATH").unwrap_or_else(|_| "7z".to_string());
    let output = tokio::process::Command::new(path)
        .arg("l")
        .arg("-slt")
        .arg("-ba")
        .arg(archive_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .await
        .map_err(|error| AppError::Other(format!("7-Zip listing failed to start: {error}")))?;
    if !output.status.success() {
        return Err(AppError::BadRequest(format!(
            "7-Zip could not list the archive (exit {})",
            output.status.code().unwrap_or(-1)
        )));
    }
    parse_seven_zip_listing(&output.stdout)
}

pub async fn prepare_seven_zip_entry(
    archive_path: &Path,
    entry: &RemoteArchiveEntry,
    destination: &Path,
    password: Option<&str>,
    cancel: Option<&AtomicBool>,
) -> Result<PathBuf> {
    if entry.directory {
        return Err(AppError::BadRequest(
            "cannot prepare a directory entry".to_string(),
        ));
    }
    if tokio::fs::try_exists(destination).await? {
        return Ok(destination.to_path_buf());
    }
    if let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let path = std::env::var("STRM_7ZIP_PATH").unwrap_or_else(|_| "7z".to_string());
    let mut command = tokio::process::Command::new(path);
    command.kill_on_drop(true);
    command
        .arg("x")
        .arg(archive_path)
        .arg("-so")
        .arg("-y")
        .arg("-bd")
        .arg(&entry.name)
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if let Some(password) = password {
        command.arg(format!("-p{password}"));
    }
    let mut child = command
        .spawn()
        .map_err(|error| AppError::Other(format!("7-Zip extraction failed to start: {error}")))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| AppError::Other("7-Zip extraction has no stdout pipe".to_string()))?;
    let temporary = destination.with_extension(format!("tmp-{}", Uuid::new_v4().simple()));
    let mut file = tokio::fs::File::create_new(&temporary).await?;
    let mut buffer = [0_u8; 64 * 1024];
    let mut written = 0_u64;
    loop {
        if cancel.is_some_and(|flag| flag.load(Ordering::Acquire)) {
            let _ = child.kill().await;
            drop(file);
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(AppError::BadRequest(
                "STRM archive entry preparation was cancelled".to_string(),
            ));
        }
        let read = match stdout.read(&mut buffer).await {
            Ok(read) => read,
            Err(error) => {
                let _ = child.kill().await;
                drop(file);
                let _ = tokio::fs::remove_file(&temporary).await;
                return Err(error.into());
            }
        };
        if read == 0 {
            break;
        }
        written = written.saturating_add(read as u64);
        if written > MAX_ARCHIVE_ENTRY_BYTES {
            drop(file);
            let _ = tokio::fs::remove_file(&temporary).await;
            let _ = child.kill().await;
            return Err(AppError::BadRequest(
                "archive entry exceeds the safety limit".to_string(),
            ));
        }
        if let Err(error) = file.write_all(&buffer[..read]).await {
            let _ = child.kill().await;
            drop(file);
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(error.into());
        }
    }
    if let Err(error) = file.flush().await {
        let _ = child.kill().await;
        drop(file);
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error.into());
    }
    if let Err(error) = file.sync_all().await {
        let _ = child.kill().await;
        drop(file);
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error.into());
    }
    drop(file);
    let status = child.wait().await?;
    if !status.success() {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(AppError::BadRequest(format!(
            "7-Zip could not extract archive entry {} (exit {})",
            entry.name,
            status.code().unwrap_or(-1)
        )));
    }
    if entry.uncompressed_size > 0 && written != entry.uncompressed_size {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(AppError::Other(
            "7-Zip archive entry length mismatch".to_string(),
        ));
    }
    if let Err(error) = tokio::fs::rename(&temporary, destination).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error.into());
    }
    Ok(destination.to_path_buf())
}

fn parse_seven_zip_listing(output: &[u8]) -> Result<Vec<RemoteArchiveEntry>> {
    let text = String::from_utf8_lossy(output);
    let mut entries = Vec::new();
    let mut block = HashMap::<String, String>::new();
    let flush = |block: &mut HashMap<String, String>, entries: &mut Vec<RemoteArchiveEntry>| {
        let Some(name) = block.remove("Path") else {
            block.clear();
            return;
        };
        if name == ".." || name == "." || !safe_entry_name(&name) {
            block.clear();
            return;
        }
        let directory = block
            .get("Attributes")
            .is_some_and(|value| value.starts_with('D'))
            || name.ends_with('/');
        let uncompressed_size = block
            .get("Size")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_default();
        let compressed_size = block
            .get("Packed Size")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_default();
        let encrypted = block
            .get("Encrypted")
            .is_some_and(|value| value == "+" || value.eq_ignore_ascii_case("true"));
        entries.push(RemoteArchiveEntry {
            name,
            compressed_size,
            uncompressed_size,
            compression: 0,
            local_header_offset: 0,
            encrypted,
            directory,
        });
        block.clear();
    };
    for line in text.lines() {
        if line.trim().is_empty() {
            flush(&mut block, &mut entries);
            continue;
        }
        if let Some((key, value)) = line.split_once(" = ") {
            block.insert(key.trim().to_string(), value.trim().to_string());
        }
    }
    flush(&mut block, &mut entries);
    if entries.len() > MAX_ARCHIVE_ENTRIES {
        return Err(AppError::BadRequest(
            "archive contains too many entries".to_string(),
        ));
    }
    Ok(entries)
}

fn write_entry_file(
    destination: &Path,
    compressed: &[u8],
    compression: u16,
    expected_size: u64,
) -> Result<u64> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    let mut written = 0_u64;
    let mut write_chunk = |chunk: &[u8]| -> Result<()> {
        written = written.saturating_add(chunk.len() as u64);
        if written > expected_size || written > MAX_ARCHIVE_ENTRY_BYTES {
            return Err(AppError::BadRequest(
                "archive entry expanded beyond its declared size".to_string(),
            ));
        }
        file.write_all(chunk)?;
        Ok(())
    };

    match compression {
        0 => write_chunk(compressed)?,
        8 => {
            let mut decoder = DeflateDecoder::new(compressed);
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                let read = decoder.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                write_chunk(&buffer[..read])?;
            }
        }
        method => {
            return Err(AppError::BadRequest(format!(
                "ZIP compression method {method} is not supported"
            )));
        }
    }
    file.flush()?;
    file.sync_all()?;
    Ok(written)
}

fn parse_central_directory(
    data: &[u8],
    expected_entries: usize,
) -> Result<Vec<RemoteArchiveEntry>> {
    let mut entries = Vec::with_capacity(expected_entries.min(MAX_ARCHIVE_ENTRIES));
    let mut cursor = 0_usize;
    while cursor < data.len() {
        if data.get(cursor..cursor + 4) != Some(b"PK\x01\x02") {
            return Err(AppError::Other(
                "ZIP central directory entry signature is invalid".to_string(),
            ));
        }
        if entries.len() >= MAX_ARCHIVE_ENTRIES {
            return Err(AppError::BadRequest(
                "ZIP archive contains too many entries".to_string(),
            ));
        }
        let flags = read_u16(data, cursor + 8).unwrap_or_default();
        let compression = read_u16(data, cursor + 10).unwrap_or_default();
        let compressed_size = read_u32(data, cursor + 20).unwrap_or_default() as u64;
        let uncompressed_size = read_u32(data, cursor + 24).unwrap_or_default() as u64;
        let name_len = read_u16(data, cursor + 28).unwrap_or_default() as usize;
        let extra_len = read_u16(data, cursor + 30).unwrap_or_default() as usize;
        let comment_len = read_u16(data, cursor + 32).unwrap_or_default() as usize;
        let local_header_offset = read_u32(data, cursor + 42).unwrap_or_default() as u64;
        let record_len = 46_usize
            .checked_add(name_len)
            .and_then(|value| value.checked_add(extra_len))
            .and_then(|value| value.checked_add(comment_len))
            .ok_or_else(|| {
                AppError::Other("ZIP central directory entry is too large".to_string())
            })?;
        let record = data
            .get(cursor..cursor.saturating_add(record_len))
            .ok_or_else(|| {
                AppError::Other("ZIP central directory entry is truncated".to_string())
            })?;
        let name_bytes = record
            .get(46..46 + name_len)
            .ok_or_else(|| AppError::Other("ZIP entry name is truncated".to_string()))?;
        let name = String::from_utf8_lossy(name_bytes).replace('\\', "/");
        let directory = name.ends_with('/');
        if !safe_entry_name(&name) {
            return Err(AppError::BadRequest(format!(
                "unsafe ZIP entry path: {name}"
            )));
        }
        if compressed_size == u32::MAX as u64
            || uncompressed_size == u32::MAX as u64
            || local_header_offset == u32::MAX as u64
        {
            return Err(AppError::BadRequest(
                "ZIP64 archive entries require full-cache extraction".to_string(),
            ));
        }
        entries.push(RemoteArchiveEntry {
            name,
            compressed_size,
            uncompressed_size,
            compression,
            local_header_offset,
            encrypted: flags & 1 != 0,
            directory,
        });
        cursor += record_len;
    }
    if entries.len() != expected_entries {
        return Err(AppError::Other(format!(
            "ZIP central directory entry count mismatch: expected {expected_entries}, received {}",
            entries.len()
        )));
    }
    Ok(entries)
}

fn safe_entry_name(name: &str) -> bool {
    if name.is_empty() || name.starts_with('/') || name.starts_with('\\') || name.contains(':') {
        return false;
    }
    let components = name.split('/').collect::<Vec<_>>();
    components.iter().enumerate().all(|(index, component)| {
        (index + 1 == components.len() && component.is_empty())
            || (!component.is_empty() && *component != "..")
    })
}

fn find_end_of_central_directory(data: &[u8]) -> Option<usize> {
    data.windows(4)
        .rposition(|window| window == b"PK\x05\x06")
        .filter(|offset| offset.saturating_add(22) <= data.len())
}

fn read_u16(data: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_le_bytes([
        *data.get(offset)?,
        *data.get(offset + 1)?,
    ]))
}

fn read_u32(data: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes([
        *data.get(offset)?,
        *data.get(offset + 1)?,
        *data.get(offset + 2)?,
        *data.get(offset + 3)?,
    ]))
}

fn parse_content_range(value: &str) -> Option<(u64, u64, u64)> {
    let (range, total) = value.strip_prefix("bytes ")?.split_once('/')?;
    let total = total.parse::<u64>().ok()?;
    let (start, end) = range.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?, total))
}

fn manifest_source_version(raw_url: &str, total_size: u64, suffix: &[u8]) -> String {
    stable_key(&[
        raw_url,
        &total_size.to_string(),
        &stable_key(&[&String::from_utf8_lossy(suffix)]),
    ])
}

fn remote_response_error(context: &str, response: &reqwest::Response) -> AppError {
    match response.status() {
        StatusCode::TOO_MANY_REQUESTS => AppError::Overloaded {
            message: format!("{context} failed: remote source returned 429"),
            retry_after_seconds: http::retry_after_seconds(response).unwrap_or(60),
        },
        StatusCode::FORBIDDEN => AppError::Overloaded {
            message: format!("{context} failed: remote source returned 403"),
            retry_after_seconds: http::retry_after_seconds(response).unwrap_or(5 * 60),
        },
        StatusCode::SERVICE_UNAVAILABLE => AppError::Overloaded {
            message: format!("{context} failed: remote source returned 503"),
            retry_after_seconds: http::retry_after_seconds(response).unwrap_or(30).max(30),
        },
        status => AppError::Other(format!("{context} failed: {status}")),
    }
}
