//! In-process STRM preparation tasks.

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use std::time::{Duration, Instant};

use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::error::{AppError, Result};
use crate::models::Asset;
use crate::strm::archive::{self, ArchiveSupport, ManifestProbe, RemoteArchiveManifest};
use crate::strm::cache;
use crate::strm::source;
use crate::vfs;
use crate::AppState;

const PASSWORD_TTL: Duration = Duration::from_secs(15 * 60);
const TASK_RETENTION: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone, Serialize)]
pub struct TaskSnapshot {
    pub id: String,
    pub asset_id: i64,
    pub status: String,
    pub phase: String,
    pub progress: u64,
    pub total: Option<u64>,
    pub message: Option<String>,
    pub cooling_until: Option<String>,
    pub requires_password: bool,
    pub missing_volumes: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct PasswordRequest {
    pub password: String,
}

struct TaskRecord {
    snapshot: Mutex<TaskSnapshot>,
    cancel: AtomicBool,
    password: Mutex<Option<(String, Instant)>>,
    created_at: Instant,
}

static TASKS: LazyLock<Mutex<HashMap<String, Arc<TaskRecord>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static PASSWORDS_BY_ASSET: LazyLock<StdMutex<HashMap<i64, (String, Instant)>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

pub async fn prepare(
    State(state): State<Arc<AppState>>,
    AxumPath(asset_id): AxumPath<i64>,
) -> Result<(StatusCode, Json<TaskSnapshot>)> {
    let asset = state.db.asset(asset_id).await?;
    let task = start_prepare(state, asset).await?;
    Ok((StatusCode::ACCEPTED, Json(snapshot(&task).await)))
}

pub async fn get(AxumPath(task_id): AxumPath<String>) -> Result<Json<TaskSnapshot>> {
    let task = find_task(&task_id).await?;
    Ok(Json(snapshot(&task).await))
}

pub async fn set_password(
    AxumPath(task_id): AxumPath<String>,
    Json(payload): Json<PasswordRequest>,
) -> Result<Json<TaskSnapshot>> {
    if payload.password.is_empty() || payload.password.chars().count() > 512 {
        return Err(AppError::BadRequest(
            "STRM archive password must be between 1 and 512 characters".to_string(),
        ));
    }
    let task = find_task(&task_id).await?;
    let asset_id = task.snapshot.lock().await.asset_id;
    *task.password.lock().await = Some((payload.password, Instant::now() + PASSWORD_TTL));
    let password_state = task.password.lock().await.clone();
    if let Ok(mut passwords) = PASSWORDS_BY_ASSET.lock() {
        if let Some((password, expires_at)) = password_state {
            passwords.insert(asset_id, (password, expires_at));
        }
    }
    {
        let mut state = task.snapshot.lock().await;
        if state.status == "waiting-password" {
            state.status = "done".to_string();
            state.phase = "done".to_string();
            state.requires_password = false;
            state.message = Some(
                "password stored in memory for 15 minutes; the next entry request will extract it"
                    .to_string(),
            );
        }
    }
    Ok(Json(snapshot(&task).await))
}

pub async fn cancel(AxumPath(task_id): AxumPath<String>) -> Result<Json<TaskSnapshot>> {
    let task = find_task(&task_id).await?;
    task.cancel.store(true, Ordering::Release);
    let mut state = task.snapshot.lock().await;
    if !matches!(state.status.as_str(), "done" | "failed" | "cancelled") {
        state.status = "cancelled".to_string();
        state.phase = "cancelled".to_string();
        state.message = Some("preparation cancelled by user".to_string());
    }
    Ok(Json(state.clone()))
}

pub async fn diagnose(
    State(state): State<Arc<AppState>>,
    AxumPath(asset_id): AxumPath<i64>,
) -> Result<Json<serde_json::Value>> {
    let asset = state.db.asset(asset_id).await?;
    if !vfs::is_qms_strm_uri(&asset.path) {
        return Ok(Json(json!({
            "asset_id": asset.id,
            "scope": "local",
            "remote_checked": false,
            "range_supported": true,
            "message": "local asset does not require a remote STRM probe",
        })));
    }
    let target_url = vfs::qms_target_url_for_asset(&state, &asset).await?;
    let response =
        crate::strm::http::send_get(&target_url, Some("bytes=0-1023"), None, None, None).await?;
    let status = response.status();
    let range_supported = status == reqwest::StatusCode::PARTIAL_CONTENT
        || response
            .headers()
            .get(reqwest::header::ACCEPT_RANGES)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.eq_ignore_ascii_case("bytes"));
    let content_range = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let content_length = response.content_length();
    Ok(Json(json!({
        "asset_id": asset.id,
        "scope": "remote-strm",
        "remote_checked": true,
        "target_host": url::Url::parse(&target_url).ok().and_then(|url| url.host_str().map(ToOwned::to_owned)),
        "status": status.as_u16(),
        "range_supported": range_supported,
        "content_range": content_range,
        "content_length": content_length,
        "message": if range_supported { "remote source accepts byte ranges" } else { "remote source does not advertise byte ranges; archive requests fall back to bounded cache" },
    })))
}

async fn start_prepare(state: Arc<AppState>, asset: Asset) -> Result<Arc<TaskRecord>> {
    let mut tasks = TASKS.lock().await;
    let now = Instant::now();
    tasks.retain(|_, task| task.created_at + TASK_RETENTION > now);
    for task in tasks.values() {
        let current = task.snapshot.lock().await;
        if current.asset_id == asset.id
            && !matches!(current.status.as_str(), "done" | "failed" | "cancelled")
        {
            return Ok(task.clone());
        }
    }

    let id = Uuid::new_v4().to_string();
    let missing_volumes = metadata_string_list(&asset, "missing_volumes");
    let task = Arc::new(TaskRecord {
        snapshot: Mutex::new(TaskSnapshot {
            id: id.clone(),
            asset_id: asset.id,
            status: "queued".to_string(),
            phase: "queued".to_string(),
            progress: 0,
            total: None,
            message: None,
            cooling_until: None,
            requires_password: false,
            missing_volumes,
        }),
        cancel: AtomicBool::new(false),
        password: Mutex::new(None),
        created_at: now,
    });
    tasks.insert(id, task.clone());
    drop(tasks);

    let task_for_worker = task.clone();
    tokio::spawn(async move {
        if let Err(error) = run_prepare(state, asset, task_for_worker.clone()).await {
            let mut snapshot = task_for_worker.snapshot.lock().await;
            if snapshot.status != "cancelled" {
                snapshot.status = "failed".to_string();
                snapshot.phase = "failed".to_string();
                snapshot.message = Some(error.to_string());
                snapshot.cooling_until = match error {
                    AppError::Overloaded {
                        retry_after_seconds,
                        ..
                    } => Some(
                        (Utc::now() + chrono::Duration::seconds(retry_after_seconds as i64))
                            .to_rfc3339(),
                    ),
                    _ => None,
                };
            }
        }
    });
    Ok(task)
}

async fn run_prepare(state: Arc<AppState>, asset: Asset, task: Arc<TaskRecord>) -> Result<()> {
    update(&task, |snapshot| {
        snapshot.status = "downloading".to_string();
        snapshot.phase = "downloading".to_string();
        snapshot.message = Some("reading local STRM pointer".to_string());
    })
    .await;
    ensure_not_cancelled(&task).await?;

    let missing_volumes = task.snapshot.lock().await.missing_volumes.clone();
    if !missing_volumes.is_empty() {
        let names = missing_volumes.join(", ");
        update(&task, |snapshot| {
            snapshot.status = "failed".to_string();
            snapshot.phase = "failed".to_string();
            snapshot.message = Some(format!("missing archive volumes: {names}"));
        })
        .await;
        return Ok(());
    }

    if !vfs::is_qms_strm_uri(&asset.path) {
        finish(&task, "local asset is already available").await;
        return Ok(());
    }
    let target_url = vfs::qms_target_url_for_asset(&state, &asset).await?;
    let kind = source::classify(Path::new(&asset.path), Some(&target_url));
    if !kind.is_archive() {
        finish(&task, "direct STRM source requires no archive preparation").await;
        return Ok(());
    }
    let extension = source::extension_from_target(&target_url)
        .or_else(|| {
            Path::new(&asset.path)
                .file_stem()
                .and_then(|value| Path::new(value).extension())
                .and_then(|value| value.to_str())
        })
        .unwrap_or("unknown");
    match archive::support_for_extension(extension) {
        ArchiveSupport::Zip => prepare_zip_archive(&state, &asset, &target_url, &task).await?,
        ArchiveSupport::SevenZip | ArchiveSupport::Rar => {
            prepare_complex_archive(&state, &asset, &target_url, extension, &task).await?
        }
        ArchiveSupport::Unsupported => return Err(archive::unsupported_archive_error(extension)),
    }
    Ok(())
}

async fn prepare_zip_archive(
    state: &Arc<AppState>,
    asset: &Asset,
    target_url: &str,
    task: &Arc<TaskRecord>,
) -> Result<()> {
    update(task, |snapshot| {
        snapshot.phase = "parsing-directory".to_string();
        snapshot.message = Some("reading ZIP central directory through byte ranges".to_string());
    })
    .await;
    let manifest = match archive::cached_manifest(target_url) {
        Some(manifest) => manifest,
        None => match archive::load_persisted_manifest(&state.db, target_url).await? {
            Some(manifest) => {
                archive::remember_manifest(target_url, manifest.clone());
                manifest
            }
            None => match archive::probe_manifest(target_url).await? {
                ManifestProbe::Ready(manifest) => {
                    archive::remember_manifest(target_url, manifest.clone());
                    let cache_limit = state.resources.snapshot().archive_manifest_cache_bytes / 2;
                    if let Err(error) =
                        archive::persist_manifest(&state.db, target_url, &manifest, cache_limit)
                            .await
                    {
                        tracing::debug!(
                            error = %error,
                            "failed to persist remote STRM archive manifest"
                        );
                    }
                    manifest
                }
                ManifestProbe::RequiresFullCache { reason } => {
                    update(task, |snapshot| {
                        snapshot.message = Some(format!(
                            "byte ranges unavailable; falling back to bounded full cache: {reason}"
                        ));
                    })
                    .await;
                    let archive_path = vfs::ensure_qms_asset_cached(state, asset).await?;
                    if state.db.work_kind(asset.work_id).await? == "audio" {
                        let entries = match archive::list_local_zip_entries(&archive_path).await {
                            Ok(entries) => entries,
                            Err(_zip_error) if archive::seven_zip_available().await => {
                                archive::list_seven_zip_entries(&archive_path).await?
                            }
                            Err(zip_error) => return Err(zip_error),
                        };
                        let total_size = tokio::fs::metadata(&archive_path).await?.len();
                        let manifest = RemoteArchiveManifest {
                            total_size,
                            source_version: cache::stable_key(&[
                                target_url,
                                &total_size.to_string(),
                            ]),
                            range_supported: false,
                            entries,
                        };
                        archive::remember_manifest(target_url, manifest.clone());
                        materialize_audio_entries(state, asset, &manifest).await?;
                        if manifest.entries.iter().any(|entry| entry.encrypted) {
                            update(task, |snapshot| {
                                snapshot.status = "waiting-password".to_string();
                                snapshot.phase = "waiting-password".to_string();
                                snapshot.requires_password = true;
                                snapshot.message = Some(
                                    "one or more archive entries require a password".to_string(),
                                );
                            })
                            .await;
                        } else {
                            finish(task, "archive cached locally and track directory is ready")
                                .await;
                        }
                    } else {
                        finish(
                            task,
                            "archive cached locally because range access is unavailable",
                        )
                        .await;
                    }
                    return Ok(());
                }
            },
        },
    };
    ensure_not_cancelled(task).await?;
    update(task, |snapshot| {
        snapshot.progress = 0;
        snapshot.total = Some(manifest.entries.len() as u64);
        snapshot.phase = "done".to_string();
        snapshot.status = "done".to_string();
        snapshot.message = Some(format!(
            "archive directory ready ({} entries)",
            manifest.entries.len()
        ));
    })
    .await;

    if state.db.work_kind(asset.work_id).await? == "audio" {
        materialize_audio_entries(state, asset, &manifest).await?;
    }
    if manifest.entries.iter().any(|entry| entry.encrypted) {
        update(task, |snapshot| {
            snapshot.status = "waiting-password".to_string();
            snapshot.phase = "waiting-password".to_string();
            snapshot.requires_password = true;
            snapshot.message = Some("one or more archive entries require a password".to_string());
        })
        .await;
    }
    Ok(())
}

async fn prepare_complex_archive(
    state: &Arc<AppState>,
    asset: &Asset,
    target_url: &str,
    extension: &str,
    task: &Arc<TaskRecord>,
) -> Result<()> {
    update(task, |snapshot| {
        snapshot.phase = "downloading".to_string();
        snapshot.message = Some(format!("checking 7-Zip support for .{extension} archive"));
    })
    .await;
    if !archive::seven_zip_available().await {
        return Err(AppError::BadRequest(
            "RAR/7z preparation requires the configured 7-Zip CLI; install it or set STRM_7ZIP_PATH"
                .to_string(),
        ));
    }
    let archive_path = vfs::ensure_qms_asset_cached(state, asset).await?;
    let entries = archive::list_seven_zip_entries(&archive_path).await?;
    let source_version = cache::stable_key(&[
        target_url,
        &tokio::fs::metadata(&archive_path).await?.len().to_string(),
    ]);
    let manifest = RemoteArchiveManifest {
        total_size: tokio::fs::metadata(&archive_path).await?.len(),
        source_version,
        range_supported: false,
        entries,
    };
    archive::remember_manifest(target_url, manifest.clone());
    if state.db.work_kind(asset.work_id).await? == "audio" {
        materialize_audio_entries(state, asset, &manifest).await?;
    }
    if manifest.entries.iter().any(|entry| entry.encrypted) {
        update(task, |snapshot| {
            snapshot.status = "waiting-password".to_string();
            snapshot.phase = "waiting-password".to_string();
            snapshot.requires_password = true;
            snapshot.message = Some("one or more archive entries require a password".to_string());
        })
        .await;
    } else {
        finish(task, "complex archive is ready for 7-Zip entry extraction").await;
    }
    Ok(())
}

async fn materialize_audio_entries(
    state: &Arc<AppState>,
    parent: &Asset,
    manifest: &RemoteArchiveManifest,
) -> Result<()> {
    let existing = state.db.archive_asset_entries(parent.id).await?;
    let mut current_keys = HashSet::new();
    let mut position = 0_i64;
    for entry in manifest.entries.iter().filter(|entry| {
        !entry.directory && source::classify(Path::new(&entry.name), None).is_audio()
    }) {
        current_keys.insert(entry.name.clone());
        let extension = Path::new(&entry.name)
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("bin")
            .to_ascii_lowercase();
        let entry_id = cache::stable_key(&[
            &parent.id.to_string(),
            &manifest.source_version,
            &entry.name,
        ]);
        let synthetic_path = format!("strm-entry://{}/{entry_id}", parent.id);
        let meta = json!({
            "strm_entry": true,
            "archive_asset_id": parent.id,
            "entry_name": entry.name,
            "source_version": manifest.source_version,
            "compressed_size": entry.compressed_size,
            "uncompressed_size": entry.uncompressed_size,
        });
        let mime = mime_guess::from_ext(&extension)
            .first_or_octet_stream()
            .to_string();
        let derived_id = state
            .db
            .upsert_asset(
                parent.work_id,
                &synthetic_path,
                &mime,
                "track",
                Some("strm-entry"),
                Some(position),
                Some(entry.uncompressed_size.min(i64::MAX as u64) as i64),
                meta,
            )
            .await?;
        let entry_path = cache::entry_cache_path(
            &state.config.data_dir,
            &parent.id.to_string(),
            &manifest.source_version,
            &entry.name,
        );
        state
            .db
            .upsert_archive_asset_entry(
                parent.id,
                &entry.name,
                derived_id,
                &entry_path.to_string_lossy(),
                &manifest.source_version,
            )
            .await?;
        position += 1;
    }
    for entry in existing {
        if !current_keys.contains(&entry.entry_key) {
            state
                .db
                .delete_archive_asset_entry(parent.id, &entry.entry_key)
                .await?;
        }
    }
    Ok(())
}

pub fn password_for_asset(asset_id: i64) -> Option<String> {
    let mut passwords = PASSWORDS_BY_ASSET.lock().ok()?;
    let (password, expires_at) = passwords.get(&asset_id)?.clone();
    if expires_at <= Instant::now() {
        passwords.remove(&asset_id);
        return None;
    }
    Some(password)
}

fn metadata_string_list(asset: &Asset, key: &str) -> Vec<String> {
    serde_json::from_str::<serde_json::Value>(&asset.meta_json)
        .ok()
        .and_then(|value| value.get(key).cloned())
        .and_then(|value| value.as_array().cloned())
        .map(|values| {
            values
                .into_iter()
                .filter_map(|value| value.as_str().map(ToOwned::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

async fn find_task(task_id: &str) -> Result<Arc<TaskRecord>> {
    TASKS
        .lock()
        .await
        .get(task_id)
        .cloned()
        .ok_or_else(|| AppError::NotFound(format!("STRM task {task_id} not found")))
}

async fn snapshot(task: &TaskRecord) -> TaskSnapshot {
    task.snapshot.lock().await.clone()
}

async fn update<F>(task: &TaskRecord, update: F)
where
    F: FnOnce(&mut TaskSnapshot),
{
    let mut snapshot = task.snapshot.lock().await;
    update(&mut snapshot);
}

async fn finish(task: &TaskRecord, message: &str) {
    update(task, |snapshot| {
        snapshot.status = "done".to_string();
        snapshot.phase = "done".to_string();
        snapshot.message = Some(message.to_string());
    })
    .await;
}

async fn ensure_not_cancelled(task: &TaskRecord) -> Result<()> {
    if task.cancel.load(Ordering::Acquire) {
        return Err(AppError::BadRequest(
            "STRM preparation was cancelled".to_string(),
        ));
    }
    Ok(())
}
