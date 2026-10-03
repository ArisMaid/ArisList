use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use lofty::prelude::*;
use lofty::probe::Probe;
use regex::Regex;
use serde_json::json;
use sha2::{Digest, Sha256};
use walkdir::WalkDir;
use zip::ZipArchive;

use crate::db::{ScannerAssetInput, ScannerExternalIdInput, ScannerTagInput, ScannerWorkSnapshot};
use crate::error::{AppError, Result};
use crate::inventory;
use crate::models::{ScanResponse, ScanSourceResult};
use crate::resource::{ResourceClass, ResourceGovernor};
use crate::settings;
use crate::strm::source;
use crate::vfs;
use crate::AppState;

pub(crate) mod audio_grouping;
pub(crate) mod comic_info;
use comic_info::ComicInfoRead;

const AUDIO_METADATA_VERSION: &str = "audio-v2";
static SCAN_SOURCE_RESULTS: LazyLock<Mutex<Vec<ScanSourceResult>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));
pub(crate) mod inspectors;

static NATURAL_NUMBER_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\d+").unwrap());
static EPUB_MANIFEST_ITEM_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?is)<(?:[A-Za-z_][A-Za-z0-9_.-]*:)?item\s+[^>]+>"#).unwrap());
static EPUB_META_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?is)<(?:[A-Za-z_][A-Za-z0-9_.-]*:)?meta\s+[^>]+>"#).unwrap());
static EPUB_ROOTFILE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?is)<(?:[A-Za-z_][A-Za-z0-9_.-]*:)?rootfile\s+[^>]+>"#).unwrap()
});
static XML_ATTR_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?is)([A-Za-z_:][-A-Za-z0-9_:.]*)\s*=\s*("([^"]*)"|'([^']*)')"#).unwrap()
});
static EPUB_TITLE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?s)<(?:[A-Za-z_][A-Za-z0-9_.-]*:)?title[^>]*>(.*?)</(?:[A-Za-z_][A-Za-z0-9_.-]*:)?title>"#).unwrap()
});
static EPUB_CREATOR_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?s)<(?:[A-Za-z_][A-Za-z0-9_.-]*:)?creator[^>]*>(.*?)</(?:[A-Za-z_][A-Za-z0-9_.-]*:)?creator>"#).unwrap()
});
static EPUB_DESCRIPTION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?s)<(?:[A-Za-z_][A-Za-z0-9_.-]*:)?description[^>]*>(.*?)</(?:[A-Za-z_][A-Za-z0-9_.-]*:)?description>"#).unwrap()
});
static EPUB_LANGUAGE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?s)<(?:[A-Za-z_][A-Za-z0-9_.-]*:)?language[^>]*>(.*?)</(?:[A-Za-z_][A-Za-z0-9_.-]*:)?language>"#).unwrap()
});
static EPUB_IDENTIFIER_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?s)<(?:[A-Za-z_][A-Za-z0-9_.-]*:)?identifier[^>]*>(.*?)</(?:[A-Za-z_][A-Za-z0-9_.-]*:)?identifier>"#).unwrap()
});
static EPUB_SUBJECT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?s)<(?:[A-Za-z_][A-Za-z0-9_.-]*:)?subject[^>]*>(.*?)</(?:[A-Za-z_][A-Za-z0-9_.-]*:)?subject>"#).unwrap()
});
static EPUB_COLLECTION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?s)<(?:[A-Za-z_][A-Za-z0-9_.-]*:)?meta[^>]+property=["']belongs-to-collection["'][^>]*>(.*?)</(?:[A-Za-z_][A-Za-z0-9_.-]*:)?meta>"#).unwrap()
});
static EPUB_GROUP_POSITION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?s)<(?:[A-Za-z_][A-Za-z0-9_.-]*:)?meta[^>]+property=["']group-position["'][^>]*>(.*?)</(?:[A-Za-z_][A-Za-z0-9_.-]*:)?meta>"#).unwrap()
});
static AUDIO_TRACK_NOISE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(mp3|wav|flac|ogg|m4a|aac|opus|効果音なし|seなし|bonus|特典)\b").unwrap()
});
const MAX_COMIC_INFO_BYTES: u64 = 1024 * 1024;
const MAX_EPUB_XML_BYTES: u64 = 2 * 1024 * 1024;
const MAX_TEXT_SUMMARY_BYTES: u64 = 64 * 1024;
const MAX_EPUB_COVER_BYTES: u64 = 12 * 1024 * 1024;
const SCANNER_ASSET_BATCH_SIZE: usize = 512;
const SCANNER_DISCOVERY_BATCH_SIZE: usize = 512;
const SCANNER_PROCESSING_BUDGET_BYTES: u64 = 64 * 1024 * 1024;
const MAX_GALLERY_PARENT_DIRECTORIES: usize = 65_536;
const MAX_AUDIO_DISCOVERY_PATH_BYTES: usize = 64 * 1024 * 1024;

struct ScannerHeartbeat {
    task: tokio::task::JoinHandle<()>,
    lease_valid: Arc<AtomicBool>,
}

impl Drop for ScannerHeartbeat {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn spawn_scanner_heartbeat(state: &AppState, token: String) -> ScannerHeartbeat {
    let db = state.db.clone();
    let lease_valid = Arc::new(AtomicBool::new(true));
    let heartbeat_lease_valid = lease_valid.clone();
    let task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            match db.heartbeat_scanner_lock("library", &token).await {
                Ok(true) => {}
                Ok(false) => {
                    heartbeat_lease_valid.store(false, Ordering::Release);
                    tracing::error!("library scanner lease was lost");
                    break;
                }
                Err(err) => {
                    tracing::warn!(error = %err, "failed to renew library scanner lease");
                }
            }
        }
    });
    ScannerHeartbeat { task, lease_valid }
}

struct ScanContext<'a> {
    state: &'a AppState,
    settings: settings::AppSettings,
    token: String,
    lease_valid: Arc<AtomicBool>,
}

impl ScanContext<'_> {
    fn ensure_lease_valid(&self) -> Result<()> {
        if !self.lease_valid.load(Ordering::Acquire) {
            return Err(AppError::Other(
                "library scanner lease was lost; cancelling scan".to_string(),
            ));
        }
        Ok(())
    }

    fn scope(&self, kind: &str, root: &Path) -> String {
        format!("{kind}|{}", path_string(root))
    }

    async fn prepare_scope(&self, kind: &str, root: &Path) -> Result<String> {
        let scope = self.scope(kind, root);
        self.state
            .db
            .adopt_scanner_scope(kind, &path_string(root), &scope, &self.token)
            .await?;
        Ok(scope)
    }

    async fn finish_work(&self, work_id: i64, scope: &str, fingerprint: &str) -> Result<()> {
        self.state
            .db
            .finish_scanner_work(work_id, scope, &self.token, fingerprint)
            .await
    }

    async fn finish_work_preserving_tags(
        &self,
        work_id: i64,
        scope: &str,
        fingerprint: &str,
    ) -> Result<()> {
        self.state
            .db
            .finish_scanner_work_preserving_tags(work_id, scope, &self.token, fingerprint)
            .await
    }

    fn record_source_result(&self, result: ScanSourceResult) {
        let Ok(mut results) = SCAN_SOURCE_RESULTS.lock() else {
            tracing::warn!("scanner source result lock is poisoned");
            return;
        };
        if let Some(existing) = results
            .iter_mut()
            .find(|existing| source_results_match(existing, &result))
        {
            *existing = result;
        } else {
            results.push(result);
        }
    }
}

#[derive(Debug)]
struct ScanFingerprint {
    value: String,
    paths: BTreeMap<PathBuf, ScannedPath>,
}

#[derive(Debug)]
struct ScannedPath {
    size: Option<i64>,
    source_version: String,
}

/// Gallery directories can contain thousands of images.  Keep their ordered
/// path and metadata in one allocation instead of retaining the directory
/// list, a fingerprint input clone, and a second map key for every image.
#[derive(Debug)]
struct GalleryScannedPath {
    path: PathBuf,
    size: Option<i64>,
    source_version: String,
}

#[derive(Debug)]
struct GalleryFingerprint {
    value: String,
    files: Vec<GalleryScannedPath>,
}

impl ScanFingerprint {
    fn from_paths(mut paths: Vec<PathBuf>) -> Self {
        paths.sort();
        let mut hasher = Sha256::new();
        let mut scanned_paths = BTreeMap::new();
        for path in paths {
            let metadata = std::fs::metadata(&path).ok();
            let size = metadata.as_ref().map(|value| value.len()).unwrap_or(0);
            let stored_size = metadata
                .as_ref()
                .and_then(|value| i64::try_from(value.len()).ok());
            let modified = metadata
                .as_ref()
                .and_then(|value| value.modified().ok())
                .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|value| value.as_nanos())
                .unwrap_or(0);
            let created = metadata
                .as_ref()
                .and_then(|value| value.created().ok())
                .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|value| value.as_nanos())
                .unwrap_or(0);
            hasher.update(path_string(&path).as_bytes());
            hasher.update([0]);
            hasher.update(size.to_le_bytes());
            hasher.update(modified.to_le_bytes());
            hasher.update(created.to_le_bytes());
            let content_sample = sampled_content_key(&path, size);
            if let Some(sample) = content_sample.as_deref() {
                hasher.update(sample.as_bytes());
            }
            if metadata.is_some() {
                scanned_paths.insert(
                    path.clone(),
                    ScannedPath {
                        size: stored_size,
                        source_version: format!(
                            "{size}:{modified}:{created}:{}",
                            content_sample.as_deref().unwrap_or("metadata-only")
                        ),
                    },
                );
            }
        }
        Self {
            value: format!("{:x}", hasher.finalize()),
            paths: scanned_paths,
        }
    }

    fn size(&self, path: &Path) -> Option<i64> {
        self.paths.get(path).and_then(|value| value.size)
    }

    fn source_version(&self, path: &Path) -> Option<&str> {
        self.paths
            .get(path)
            .map(|value| value.source_version.as_str())
    }

    fn asset_meta(&self, path: &Path, mut meta: serde_json::Value) -> serde_json::Value {
        if let (Some(scanned), Some(object)) = (self.paths.get(path), meta.as_object_mut()) {
            object.insert(
                "_source_version".to_string(),
                json!(&scanned.source_version),
            );
        }
        meta
    }
}

impl GalleryFingerprint {
    fn from_ordered_paths(root: PathBuf, paths: Vec<PathBuf>) -> Result<Self> {
        let mut hasher = Sha256::new();
        let mut files = Vec::with_capacity(paths.len());
        for path in paths {
            let metadata = std::fs::metadata(&path).map_err(|error| {
                AppError::Other(format!(
                    "gallery metadata changed during scan for {}: {error}",
                    path.display()
                ))
            })?;
            if !metadata.is_file() {
                return Err(AppError::Other(format!(
                    "gallery entry is no longer a file during scan: {}",
                    path.display()
                )));
            }
            let stored_size = i64::try_from(metadata.len()).unwrap_or(i64::MAX);
            let modified = metadata
                .modified()
                .ok()
                .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|value| value.as_nanos())
                .unwrap_or(0);
            let modified_ns = modified.min(i64::MAX as u128) as i64;
            let file_id = inventory::file_id_for_metadata(&path, &metadata);
            let source_version =
                inventory::fast_fingerprint("image", stored_size, modified_ns, file_id.as_deref());
            let relative_path = path
                .strip_prefix(&root)
                .map_err(|_| {
                    AppError::Other(format!(
                        "gallery path escaped configured root during scan: {}",
                        path.display()
                    ))
                })
                .map(path_string)?;
            inspectors::gallery::update_gallery_fingerprint(
                &mut hasher,
                &relative_path,
                &source_version,
                stored_size,
            );
            files.push(GalleryScannedPath {
                path,
                size: Some(stored_size),
                source_version,
            });
        }
        Ok(Self {
            value: format!("{:x}", hasher.finalize()),
            files,
        })
    }
}

impl GalleryScannedPath {
    fn asset_meta(&self, mut meta: serde_json::Value) -> serde_json::Value {
        if let Some(object) = meta.as_object_mut() {
            object.insert("_source_version".to_string(), json!(&self.source_version));
        }
        meta
    }
}

fn sampled_content_key(path: &Path, size: u64) -> Option<String> {
    if !extension_is(path, &["cbz", "epub", "zip", "strm", "xml", "txt"]) {
        return None;
    }
    const SAMPLE_BYTES: usize = 64 * 1024;
    let mut file = File::open(path).ok()?;
    let mut sample = vec![0_u8; SAMPLE_BYTES];
    let first_len = file.read(&mut sample).ok()?;
    let mut hasher = Sha256::new();
    hasher.update((first_len as u64).to_le_bytes());
    hasher.update(&sample[..first_len]);
    if size > SAMPLE_BYTES as u64 {
        file.seek(SeekFrom::End(-(SAMPLE_BYTES as i64))).ok()?;
        let tail_len = file.read(&mut sample).ok()?;
        hasher.update((tail_len as u64).to_le_bytes());
        hasher.update(&sample[..tail_len]);
    }
    Some(format!("{:x}", hasher.finalize()))
}

struct ArchiveScanState {
    fallback_title: String,
    fingerprint: ScanFingerprint,
    cover: Option<PathBuf>,
}

struct AudioFingerprintState {
    fingerprint: ScanFingerprint,
    root: PathBuf,
    cover: Option<PathBuf>,
}

struct AudioMetadataState {
    summary: Option<String>,
    tracks: Vec<Option<serde_json::Value>>,
    files: Vec<PathBuf>,
}

struct ExtractedCover {
    path: PathBuf,
    size: Option<i64>,
    source_version: String,
}

struct ScanWalk {
    files: Vec<PathBuf>,
    usable: bool,
    complete: bool,
    discovered: usize,
}

struct BatchedWalkState {
    iterator: Option<walkdir::IntoIter>,
    usable: bool,
    complete: bool,
    done: bool,
    discovered: usize,
}

/// Incremental variant of `walk_matching_files`.
///
/// The legacy walker retains every matching path until traversal completes.
/// That is needlessly expensive on a large NAS library and also delays the
/// first database write.  This walker keeps only WalkDir's bounded traversal
/// state and exposes a small batch at a time.  The caller still decides when
/// to commit the scanner scope, so incomplete traversal keeps the same
/// tombstone-safety semantics as the legacy path.
struct BatchedFileWalker {
    resources: ResourceGovernor,
    state: Arc<std::sync::Mutex<BatchedWalkState>>,
    root: PathBuf,
    label: &'static str,
    lease_valid: Arc<AtomicBool>,
    predicate: Arc<dyn Fn(&Path) -> bool + Send + Sync>,
}

impl BatchedFileWalker {
    fn usable(&self) -> bool {
        self.state.lock().map(|state| state.usable).unwrap_or(false)
    }

    async fn next_batch(&self) -> Result<Option<Vec<PathBuf>>> {
        let state = self.state.clone();
        let root = self.root.clone();
        let label = self.label;
        let lease_valid = self.lease_valid.clone();
        let predicate = self.predicate.clone();
        scanner_blocking(&self.resources, move || {
            let mut state = state.lock().map_err(|_| {
                AppError::Other("scanner traversal state lock poisoned".to_string())
            })?;
            if state.done {
                return Ok(None);
            }
            let mut files = Vec::with_capacity(SCANNER_DISCOVERY_BATCH_SIZE);
            let mut discovered = 0_usize;
            let mut traversal_failed = false;
            let iterator = state.iterator.as_mut().ok_or_else(|| {
                AppError::Other("scanner traversal iterator was unexpectedly missing".to_string())
            })?;
            while files.len() < SCANNER_DISCOVERY_BATCH_SIZE {
                if !lease_valid.load(Ordering::Acquire) {
                    return Err(AppError::Other(
                        "library scanner lease was lost during traversal".to_string(),
                    ));
                }
                match iterator.next() {
                    None => {
                        state.done = true;
                        break;
                    }
                    Some(Err(err)) => {
                        tracing::warn!(
                            path = %root.display(),
                            error = %err,
                            "{label} traversal failed"
                        );
                        traversal_failed = true;
                    }
                    Some(Ok(entry)) => {
                        if entry.file_type().is_file() && predicate(entry.path()) {
                            discovered += 1;
                            files.push(entry.into_path());
                        }
                    }
                }
            }
            state.discovered += discovered;
            if traversal_failed {
                state.complete = false;
            }
            if files.is_empty() && state.done {
                Ok(None)
            } else {
                Ok(Some(files))
            }
        })
        .await
    }

    async fn finish(self) -> Result<ScanWalk> {
        let state = self.state.clone();
        scanner_blocking(&self.resources, move || {
            let state = state.lock().map_err(|_| {
                AppError::Other("scanner traversal state lock poisoned".to_string())
            })?;
            Ok(ScanWalk {
                files: Vec::new(),
                usable: state.usable,
                complete: state.complete && state.done,
                discovered: state.discovered,
            })
        })
        .await
    }
}

async fn open_matching_file_batches<F>(
    resources: &ResourceGovernor,
    root: &Path,
    min_depth: usize,
    max_depth: Option<usize>,
    label: &'static str,
    lease_valid: Arc<AtomicBool>,
    predicate: F,
) -> Result<BatchedFileWalker>
where
    F: Fn(&Path) -> bool + Send + Sync + 'static,
{
    let root = root.to_path_buf();
    let state_root = root.clone();
    let state = scanner_blocking(resources, move || {
        if !state_root.is_dir() || std::fs::read_dir(&state_root).is_err() {
            tracing::warn!(path = %state_root.display(), "{label} root is not readable");
            return Ok(BatchedWalkState {
                iterator: None,
                usable: false,
                complete: false,
                done: true,
                discovered: 0,
            });
        }
        let mut walker = WalkDir::new(&state_root).min_depth(min_depth);
        if let Some(max_depth) = max_depth {
            walker = walker.max_depth(max_depth);
        }
        Ok(BatchedWalkState {
            iterator: Some(walker.into_iter()),
            usable: true,
            complete: true,
            done: false,
            discovered: 0,
        })
    })
    .await?;
    Ok(BatchedFileWalker {
        resources: resources.clone(),
        state: Arc::new(std::sync::Mutex::new(state)),
        root,
        label,
        lease_valid,
        predicate: Arc::new(predicate),
    })
}

async fn scanner_blocking<T, F>(resources: &ResourceGovernor, task: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    let lease = resources
        .reserve_background(ResourceClass::ScanIo, SCANNER_PROCESSING_BUDGET_BYTES, 0)
        .await?;
    tokio::task::spawn_blocking(move || {
        let _lease = lease;
        task()
    })
    .await
    .map_err(|err| AppError::Other(format!("scanner blocking task failed: {err}")))?
}

async fn fingerprint_paths(
    resources: &ResourceGovernor,
    paths: Vec<PathBuf>,
) -> Result<ScanFingerprint> {
    scanner_blocking(resources, move || Ok(ScanFingerprint::from_paths(paths))).await
}

async fn fingerprint_gallery_paths(
    resources: &ResourceGovernor,
    root: PathBuf,
    paths: Vec<PathBuf>,
) -> Result<GalleryFingerprint> {
    scanner_blocking(resources, move || {
        GalleryFingerprint::from_ordered_paths(root, paths)
    })
    .await
}

async fn fingerprint_archive(
    resources: &ResourceGovernor,
    archive_path: PathBuf,
    include_comic_info: bool,
    include_cover: bool,
) -> Result<ArchiveScanState> {
    scanner_blocking(resources, move || {
        let dir = archive_path.parent().unwrap_or_else(|| Path::new(""));
        let comic_info_path = dir.join("ComicInfo.xml");
        let cover = include_cover.then(|| find_cover_file(dir)).flatten();
        let fallback_title = if include_comic_info {
            comic_info::fallback_title(&archive_path)
        } else {
            String::new()
        };
        let mut paths = vec![archive_path];
        if include_comic_info && comic_info_path.is_file() {
            paths.push(comic_info_path);
        }
        if let Some(path) = cover.as_ref() {
            paths.push(path.clone());
        }
        Ok(ArchiveScanState {
            fallback_title,
            fingerprint: {
                let mut fingerprint = ScanFingerprint::from_paths(paths);
                if include_comic_info {
                    fingerprint.value =
                        format!("{}:{}", comic_info::METADATA_VERSION, fingerprint.value);
                }
                fingerprint
            },
            cover,
        })
    })
    .await
}

async fn fingerprint_archive_with_sidecar_version(
    resources: &ResourceGovernor,
    archive_path: PathBuf,
    include_cover: bool,
    version: &'static str,
) -> Result<ArchiveScanState> {
    scanner_blocking(resources, move || {
        let dir = archive_path.parent().unwrap_or_else(|| Path::new(""));
        let comic_info_path = dir.join("ComicInfo.xml");
        let cover = include_cover.then(|| find_cover_file(dir)).flatten();
        let fallback_title = archive_path
            .file_stem()
            .and_then(|value| value.to_str())
            .map(clean_title)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "Untitled CoserPicture".to_string());
        let mut paths = vec![archive_path];
        if comic_info_path.is_file() {
            paths.push(comic_info_path);
        }
        if let Some(path) = cover.as_ref() {
            paths.push(path.clone());
        }
        let mut fingerprint = ScanFingerprint::from_paths(paths);
        fingerprint.value = format!("{version}:{}", fingerprint.value);
        Ok(ArchiveScanState {
            fallback_title,
            fingerprint,
            cover,
        })
    })
    .await
}

async fn fingerprint_audio_group(
    resources: &ResourceGovernor,
    audio_dir: PathBuf,
    rj: String,
    files: Vec<PathBuf>,
) -> Result<AudioFingerprintState> {
    scanner_blocking(resources, move || {
        let root = common_rj_root(&audio_dir, &rj, &files);
        let cover = audio_cover_candidate(&files, &root);
        let mut fingerprint_files = files;
        let sidecar = root.join("ComicInfo.xml");
        if sidecar.is_file() {
            fingerprint_files.push(sidecar);
        }
        if let Some(path) = cover.as_ref() {
            if !fingerprint_files.contains(path) {
                fingerprint_files.push(path.clone());
            }
        }
        let mut fingerprint = ScanFingerprint::from_paths(fingerprint_files);
        fingerprint.value = format!("{AUDIO_METADATA_VERSION}:{}", fingerprint.value);
        Ok(AudioFingerprintState {
            fingerprint,
            root,
            cover,
        })
    })
    .await
}

async fn read_audio_group_metadata(
    resources: &ResourceGovernor,
    files: Vec<PathBuf>,
) -> Result<AudioMetadataState> {
    scanner_blocking(resources, move || {
        let summary = read_first_text_summary(&files);
        let tracks = files
            .iter()
            .map(|path| {
                if !audio_track_file(path) {
                    return None;
                }
                let stem = path
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .unwrap_or_default();
                let quality = path
                    .extension()
                    .and_then(|value| value.to_str())
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                Some(read_audio_metadata(path, stem, &quality))
            })
            .collect();
        Ok(AudioMetadataState {
            summary,
            tracks,
            files,
        })
    })
    .await
}

async fn read_comic_info_blocking(
    resources: &ResourceGovernor,
    dir: PathBuf,
) -> Result<ComicInfoRead> {
    scanner_blocking(resources, move || read_comic_info(&dir)).await
}

async fn count_cbz_pages_blocking(resources: &ResourceGovernor, path: PathBuf) -> Result<i64> {
    scanner_blocking(resources, move || count_cbz_pages(&path)).await
}

async fn read_epub_metadata_blocking(
    resources: &ResourceGovernor,
    path: PathBuf,
) -> Result<EpubMetadata> {
    scanner_blocking(resources, move || read_epub_metadata(&path)).await
}

async fn extract_epub_cover_content_addressed_blocking(
    resources: &ResourceGovernor,
    epub_path: PathBuf,
    generated_dir: PathBuf,
) -> Result<Option<ExtractedCover>> {
    scanner_blocking(resources, move || {
        extract_epub_cover_named(&epub_path, &generated_dir, "epub-cover-v2")
    })
    .await
}

pub(crate) async fn read_qms_strm_url_blocking(
    resources: &ResourceGovernor,
    path: PathBuf,
) -> Result<String> {
    scanner_blocking(resources, move || vfs::read_qms_strm_url(&path)).await
}

async fn walk_matching_files<F>(
    resources: &ResourceGovernor,
    root: &Path,
    min_depth: usize,
    max_depth: Option<usize>,
    label: &'static str,
    lease_valid: Arc<AtomicBool>,
    predicate: F,
) -> Result<ScanWalk>
where
    F: Fn(&Path) -> bool + Send + Sync + 'static,
{
    let root = root.to_path_buf();
    scanner_blocking(resources, move || {
        if !root.is_dir() || std::fs::read_dir(&root).is_err() {
            tracing::warn!(path = %root.display(), "{label} root is not readable");
            return Ok(ScanWalk {
                files: Vec::new(),
                usable: false,
                complete: false,
                discovered: 0,
            });
        }
        let mut walker = WalkDir::new(&root).min_depth(min_depth);
        if let Some(max_depth) = max_depth {
            walker = walker.max_depth(max_depth);
        }
        let mut files = Vec::new();
        let mut complete = true;
        for entry in walker {
            if !lease_valid.load(Ordering::Acquire) {
                return Err(AppError::Other(
                    "library scanner lease was lost during traversal".to_string(),
                ));
            }
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    tracing::warn!(path = %root.display(), error = %err, "{label} traversal failed");
                    complete = false;
                    continue;
                }
            };
            if entry.file_type().is_file() && predicate(entry.path()) {
                files.push(entry.into_path());
            }
        }
        let discovered = files.len();
        Ok(ScanWalk {
            files,
            usable: true,
            complete,
            discovered,
        })
    })
    .await
}

/// Discover only the parent directories that contain gallery images.  The
/// legacy gallery scanner used to retain every matching path (70w images can
/// turn that into hundreds of megabytes before the first database write).
/// Directory identities are small and bounded by the number of image sets;
/// each set is enumerated and committed separately by `scan_gallery`.
async fn walk_matching_parent_directories<F>(
    resources: &ResourceGovernor,
    root: &Path,
    min_depth: usize,
    max_depth: Option<usize>,
    label: &'static str,
    lease_valid: Arc<AtomicBool>,
    predicate: F,
) -> Result<ScanWalk>
where
    F: Fn(&Path) -> bool + Send + Sync + 'static,
{
    let root = root.to_path_buf();
    scanner_blocking(resources, move || {
        if !root.is_dir() || std::fs::read_dir(&root).is_err() {
            tracing::warn!(path = %root.display(), "{label} root is not readable");
            return Ok(ScanWalk {
                files: Vec::new(),
                usable: false,
                complete: false,
                discovered: 0,
            });
        }
        let mut walker = WalkDir::new(&root).min_depth(min_depth);
        if let Some(max_depth) = max_depth {
            walker = walker.max_depth(max_depth);
        }
        let mut parents = std::collections::BTreeSet::new();
        let mut complete = true;
        for entry in walker {
            if !lease_valid.load(Ordering::Acquire) {
                return Err(AppError::Other(
                    "library scanner lease was lost during traversal".to_string(),
                ));
            }
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    tracing::warn!(path = %root.display(), error = %err, "{label} traversal failed");
                    complete = false;
                    continue;
                }
            };
            if entry.file_type().is_file() && predicate(entry.path()) {
                if let Some(parent) = entry.path().parent() {
                    parents.insert(parent.to_path_buf());
                    if parents.len() >= MAX_GALLERY_PARENT_DIRECTORIES {
                        tracing::warn!(
                            path = %root.display(),
                            limit = MAX_GALLERY_PARENT_DIRECTORIES,
                            "gallery parent-directory discovery reached its safety limit"
                        );
                        complete = false;
                        break;
                    }
                }
            }
        }
        let discovered = parents.len();
        Ok(ScanWalk {
            files: parents.into_iter().collect(),
            usable: true,
            complete,
            discovered,
        })
    })
    .await
}

async fn list_matching_files_in_directory<F>(
    resources: &ResourceGovernor,
    directory: &Path,
    lease_valid: Arc<AtomicBool>,
    predicate: F,
) -> Result<(Vec<PathBuf>, bool)>
where
    F: Fn(&Path) -> bool + Send + Sync + 'static,
{
    let directory = directory.to_path_buf();
    scanner_blocking(resources, move || {
        let read_dir = match std::fs::read_dir(&directory) {
            Ok(read_dir) => read_dir,
            Err(err) => {
                tracing::warn!(path = %directory.display(), error = %err, "gallery directory is not readable");
                return Ok((Vec::new(), false));
            }
        };
        let mut files = Vec::new();
        let mut complete = true;
        for entry in read_dir {
            if !lease_valid.load(Ordering::Acquire) {
                return Err(AppError::Other(
                    "library scanner lease was lost during gallery directory enumeration"
                        .to_string(),
                ));
            }
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    tracing::warn!(path = %directory.display(), error = %err, "gallery directory entry read failed");
                    complete = false;
                    continue;
                }
            };
            let path = entry.path();
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(err) => {
                    tracing::warn!(path = %path.display(), error = %err, "gallery entry type read failed");
                    complete = false;
                    continue;
                }
            };
            if file_type.is_file() && predicate(&path) {
                if files.len() >= inspectors::gallery::MAX_GALLERY_FILES_PER_WORK {
                    tracing::warn!(
                        path = %directory.display(),
                        limit = inspectors::gallery::MAX_GALLERY_FILES_PER_WORK,
                        "gallery directory reached the per-work file safety limit"
                    );
                    complete = false;
                    break;
                }
                files.push(path);
            }
        }
        Ok((files, complete))
    })
    .await
}

async fn finish_kind_scopes(
    context: &ScanContext<'_>,
    kind: &str,
    active_scopes: &[String],
) -> Result<()> {
    context
        .state
        .db
        .finish_removed_scanner_scopes(kind, active_scopes, &context.token)
        .await?;
    Ok(())
}

async fn preserve_existing_scanner_work(
    context: &ScanContext<'_>,
    kind: &str,
    source_path: &str,
    scope: &str,
) -> Result<bool> {
    let Some(work_id) = context.state.db.scanner_work_id(kind, source_path).await? else {
        return Ok(false);
    };
    context
        .state
        .db
        .touch_scanner_work(work_id, scope, &context.token)
        .await?;
    Ok(true)
}

#[derive(Default)]
pub struct ScanStats {
    pub comics: usize,
    pub novels: usize,
    pub audio: usize,
    pub gallery: usize,
    pub coser_picture: usize,
    pub jobs_created: usize,
}

pub async fn scan_all(state: &AppState, enqueue_enrichment: bool) -> Result<ScanResponse> {
    scan_with_scope(state, enqueue_enrichment, None).await
}

/// Run a maintenance scan for one media kind.  Ownership promotion and
/// operator-triggered repair jobs use this path so a change to (for example)
/// Gallery does not rescan Comic, Novel, Audio, and CoserPicture on the same
/// N100/HDD maintenance window.
pub async fn scan_kind(
    state: &AppState,
    kind: &str,
    enqueue_enrichment: bool,
) -> Result<ScanResponse> {
    let kind = normalize_scan_kind(kind)?;
    scan_with_scope(state, enqueue_enrichment, Some(kind.as_str())).await
}

pub fn normalize_scan_kind(kind: &str) -> Result<String> {
    let normalized = kind.trim().to_ascii_lowercase();
    if matches!(
        normalized.as_str(),
        "comic" | "novel" | "audio" | "gallery" | "coser-picture"
    ) {
        Ok(normalized)
    } else {
        Err(AppError::BadRequest(format!(
            "unsupported scan kind {kind:?}"
        )))
    }
}

async fn scan_with_scope(
    state: &AppState,
    enqueue_enrichment: bool,
    scope: Option<&str>,
) -> Result<ScanResponse> {
    let token = uuid::Uuid::new_v4().to_string();
    if !state
        .db
        .try_acquire_scanner_lock("library", &token, 6 * 60 * 60)
        .await?
    {
        return Err(AppError::Other(
            "a library scan is already running".to_string(),
        ));
    }
    let heartbeat = spawn_scanner_heartbeat(state, token.clone());
    let result = scan_all_locked(
        state,
        token.clone(),
        heartbeat.lease_valid.clone(),
        enqueue_enrichment,
        scope,
    )
    .await;
    if let Err(err) = state.db.release_scanner_lock("library", &token).await {
        tracing::warn!(error = %err, "failed to release library scan lock");
    }
    result
}

async fn scan_all_locked(
    state: &AppState,
    token: String,
    lease_valid: Arc<AtomicBool>,
    enqueue_enrichment: bool,
    scope: Option<&str>,
) -> Result<ScanResponse> {
    if let Ok(mut results) = SCAN_SOURCE_RESULTS.lock() {
        results.clear();
    }
    let revision_before = state.db.revision_fence_snapshot().await?;
    let catalog_revision_before = revision_before.catalog_revision;
    let search_revision_before = revision_before.search_revision;
    let context = ScanContext {
        state,
        settings: settings::load_settings(&state.config).await?,
        token,
        lease_valid,
    };
    if state.config.inventory_any_kind_enabled() {
        let inventory_result = match scope {
            Some(kind) if state.config.inventory_kind_enabled(kind) => {
                inventory::reconcile_kind_shadow(
                    &state.db,
                    &state.resources,
                    &context.settings,
                    &state.config.generated_dir,
                    context.lease_valid.clone(),
                    kind,
                )
                .await
            }
            Some(kind) => {
                tracing::debug!(
                    kind,
                    "skipping shadow inventory for an unselected scan kind"
                );
                Ok(Default::default())
            }
            None => {
                let enabled_kinds = state.config.inventory_enabled_kinds();
                inventory::reconcile_all_shadow_for_kinds(
                    &state.db,
                    &state.resources,
                    &context.settings,
                    &state.config.generated_dir,
                    context.lease_valid.clone(),
                    &enabled_kinds,
                )
                .await
            }
        };
        match inventory_result {
            Ok(summary) => tracing::info!(
                roots = summary.roots,
                complete_roots = summary.complete_roots,
                incomplete_roots = summary.incomplete_roots,
                discovered = summary.discovered,
                inserted = summary.inserted,
                changed = summary.changed,
                unchanged = summary.unchanged,
                newly_missing = summary.newly_missing,
                max_batch_rows = summary.max_batch_rows,
                max_serialized_batch_bytes = summary.max_serialized_batch_bytes,
                "shadow inventory reconcile completed"
            ),
            Err(err) => tracing::warn!(
                error = %err,
                "shadow inventory reconcile failed; authoritative scanner will continue"
            ),
        }
    }
    let mut stats = ScanStats::default();
    if scope.is_none_or(|kind| kind == "comic") {
        stats.comics = scan_comics(&context).await?;
    }
    if scope.is_none_or(|kind| kind == "novel") {
        let (novels, novel_jobs) = scan_novels(&context, enqueue_enrichment).await?;
        stats.novels = novels;
        stats.jobs_created += novel_jobs;
    }
    if scope.is_none_or(|kind| kind == "audio") {
        let (audio, audio_jobs) = scan_audio(&context, enqueue_enrichment).await?;
        stats.audio = audio;
        stats.jobs_created += audio_jobs;
    }
    if scope.is_none_or(|kind| kind == "gallery") {
        stats.gallery = scan_gallery(&context).await?;
    }
    if scope.is_none_or(|kind| kind == "coser-picture") {
        stats.coser_picture = scan_coser_pictures(&context).await?;
    }
    let revision_after_scan = state.db.revision_fence_snapshot().await?;
    let catalog_revision_after_scan = revision_after_scan.catalog_revision;
    let search_revision_after_scan = revision_after_scan.search_revision;
    if catalog_revision_after_scan != catalog_revision_before {
        // A no-op maintenance scan should not rescan the entire work_tags
        // relation merely to rediscover the same global tag counts.  Real
        // catalog mutations still take the existing set-oriented refresh
        // path, while an explicit repair can continue to call
        // `refresh_tag_counts` directly.
        state.db.refresh_tag_counts().await?;
    }
    let catalog_revision_after = state.db.revision_fence_snapshot().await?.catalog_revision;
    let search_index_missing = !state
        .config
        .data_dir
        .join("search-index-v2")
        .join("meta.json")
        .is_file();
    // Once the incremental reader is atomically cut over, the search outbox
    // is the authoritative update path. A full rebuild remains an explicit
    // recovery job, rather than turning every scan into an hours-long index
    // rebuild. Legacy mode only queues it when the catalog changed or the
    // index has not been initialized yet.
    if !state.config.search_incremental_reader_enabled
        && (catalog_revision_after != catalog_revision_before
            || search_revision_after_scan != search_revision_before
            || search_index_missing)
    {
        let (_, created) = state
            .db
            .create_job_if_absent(
                "rebuild-search-index",
                "queued",
                json!({ "source": "scan" }),
            )
            .await?;
        stats.jobs_created += usize::from(created);
    }

    record_unreported_source_results(&context, scope);
    let source_results = SCAN_SOURCE_RESULTS
        .lock()
        .map(|results| results.clone())
        .unwrap_or_default();
    let status = if source_results.is_empty() {
        "skipped".to_string()
    } else if source_results
        .iter()
        .any(|result| result.status == "failed")
    {
        if source_results
            .iter()
            .any(|result| result.status == "success" || result.status == "partial")
        {
            "partial".to_string()
        } else {
            "failed".to_string()
        }
    } else if source_results
        .iter()
        .any(|result| result.status == "partial")
    {
        "partial".to_string()
    } else {
        "success".to_string()
    };

    Ok(ScanResponse {
        comics: stats.comics,
        novels: stats.novels,
        audio: stats.audio,
        gallery: stats.gallery,
        coser_picture: stats.coser_picture,
        jobs_created: stats.jobs_created,
        status,
        source_results,
    })
}

fn record_unreported_source_results(context: &ScanContext<'_>, scope: Option<&str>) {
    let local_sources = [
        ("comic", &context.settings.media_dirs.comics),
        ("novel", &context.settings.media_dirs.novels),
        ("audio", &context.settings.media_dirs.audio),
        ("gallery", &context.settings.media_dirs.gallery),
        ("coser-picture", &context.settings.media_dirs.coser_picture),
    ];
    for (kind, roots) in local_sources {
        if scope.is_some_and(|selected| selected != kind) {
            continue;
        }
        for root in roots {
            record_default_source_result(context, kind, "local", None, Path::new(root));
        }
    }
    for kind in ["comic", "novel", "audio", "gallery", "coser-picture"] {
        if scope.is_some_and(|selected| selected != kind) {
            continue;
        }
        for source in vfs::qmediasync_scan_sources(&context.settings, kind) {
            record_default_source_result(
                context,
                kind,
                &source.provider,
                Some(source.mount_name),
                Path::new(&source.root),
            );
        }
    }
}

fn record_default_source_result(
    context: &ScanContext<'_>,
    kind: &str,
    provider: &str,
    mount_name: Option<String>,
    root: &Path,
) {
    let root_string = path_string(root);
    let readable = root.is_dir() && std::fs::read_dir(root).is_ok();
    let (status, message) = if readable {
        ("success".to_string(), None)
    } else {
        (
            "failed".to_string(),
            Some(format!("源目录不可读或不存在: {root_string}")),
        )
    };
    let result = ScanSourceResult {
        kind: kind.to_string(),
        provider: provider.to_string(),
        mount_name,
        root: root_string,
        status,
        discovered: 0,
        imported: 0,
        skipped: 0,
        failed: usize::from(!readable),
        message,
    };
    let already_reported = SCAN_SOURCE_RESULTS
        .lock()
        .map(|results| {
            results
                .iter()
                .any(|existing| source_results_match(existing, &result))
        })
        .unwrap_or(false);
    if !already_reported {
        context.record_source_result(result);
    }
}

fn source_results_match(left: &ScanSourceResult, right: &ScanSourceResult) -> bool {
    left.kind == right.kind
        && left.provider == right.provider
        && left.mount_name == right.mount_name
        && source_result_root_key(&left.root) == source_result_root_key(&right.root)
}

fn source_result_root_key(root: &str) -> String {
    let normalized = root
        .trim()
        .replace('\\', "/")
        .trim_end_matches('/')
        .to_string();
    if cfg!(windows) {
        normalized.to_ascii_lowercase()
    } else {
        normalized
    }
}

fn scan_source_status(readable: bool, complete: bool, discovered: usize, failed: usize) -> String {
    if !readable {
        "failed".to_string()
    } else if !complete || failed > 0 {
        "partial".to_string()
    } else if discovered == 0 {
        "skipped".to_string()
    } else {
        "success".to_string()
    }
}

fn scan_source_message(
    readable: bool,
    complete: bool,
    discovered: usize,
    failed: usize,
) -> Option<String> {
    if !readable {
        Some("源目录不可读或不存在".to_string())
    } else if !complete {
        Some("源目录遍历未完成，已跳过缺失清理".to_string())
    } else if failed > 0 {
        Some("部分候选文件未能导入；已保留既有作品".to_string())
    } else if discovered == 0 {
        Some("扫描深度内没有可导入媒体".to_string())
    } else {
        None
    }
}

async fn catalog_v2_coordinator_owns_kind(state: &AppState, kind: &str) -> Result<bool> {
    if !state.config.inventory_kind_enabled(kind) {
        return Ok(false);
    }
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT EXISTS (SELECT 1 FROM catalog_kind_ownership WHERE kind = ?1 AND authoritative_writer = 'catalog-v2')",
    )
    .bind(kind)
    .fetch_one(state.db.pool())
    .await?
        != 0)
}

async fn scan_comics(context: &ScanContext<'_>) -> Result<usize> {
    let state = context.state;
    if catalog_v2_coordinator_owns_kind(state, "comic").await? {
        return Ok(0);
    }
    let roots = context.settings.comic_roots();
    let qms_sources = vfs::qmediasync_scan_sources(&context.settings, "comic");
    let mut active_scopes = roots
        .iter()
        .map(|root| context.scope("comic", root))
        .collect::<Vec<_>>();
    active_scopes.extend(
        qms_sources
            .iter()
            .map(|source| context.scope("comic", Path::new(&source.root))),
    );
    let mut count: usize = 0;
    for root in roots {
        let imported_before = count;
        let skipped = 0_usize;
        let mut failed = 0_usize;
        let scope = context.prepare_scope("comic", &root).await?;
        let walk = open_matching_file_batches(
            &state.resources,
            &root,
            1,
            Some(context.settings.media_dirs.comic_scan_depth),
            "comic",
            context.lease_valid.clone(),
            is_local_comic_archive,
        )
        .await?;
        if !walk.usable() {
            let summary = walk.finish().await?;
            context.record_source_result(ScanSourceResult {
                kind: "comic".to_string(),
                provider: "local".to_string(),
                mount_name: None,
                root: path_string(&root),
                status: "failed".to_string(),
                discovered: summary.discovered,
                imported: count.saturating_sub(imported_before),
                skipped,
                failed: 1,
                message: Some("源目录不可读或不存在".to_string()),
            });
            continue;
        }
        while let Some(batch) = walk.next_batch().await? {
            for cbz_path in batch {
                context.ensure_lease_valid()?;
                let dir = cbz_path.parent().unwrap_or(root.as_path()).to_path_buf();
                let ArchiveScanState {
                    fingerprint,
                    cover,
                    fallback_title,
                } = fingerprint_archive(&state.resources, cbz_path.clone(), true, true).await?;
                let source_path = path_string(&cbz_path);
                if let Some((work_id, previous)) = state
                    .db
                    .scanner_work_fingerprint("comic", &source_path)
                    .await?
                {
                    if previous == fingerprint.value.as_str() {
                        state
                            .db
                            .touch_scanner_work(work_id, &scope, &context.token)
                            .await?;
                        count += 1;
                        continue;
                    }
                }
                let existing_work_id = state.db.scanner_work_id("comic", &source_path).await?;
                let comic_info_read = match read_comic_info_blocking(&state.resources, dir.clone())
                    .await
                {
                    Ok(comic_info) => comic_info,
                    Err(err) => {
                        tracing::warn!(path = %dir.display(), error = %err, "preserving comic after ComicInfo.xml read failure");
                        failed += 1;
                        if preserve_existing_scanner_work(context, "comic", &source_path, &scope)
                            .await?
                        {
                            count += 1;
                        }
                        continue;
                    }
                };
                let preserve_scanner_tags =
                    comic_info_read.is_missing() && existing_work_id.is_some();
                let comic_info = comic_info_read.into_info();
                let title = comic_info.display_title(&fallback_title);
                let archive_page_count = match count_cbz_pages_blocking(
                    &state.resources,
                    cbz_path.clone(),
                )
                .await
                {
                    Ok(page_count) => page_count,
                    Err(err) => {
                        tracing::warn!(path = %cbz_path.display(), error = %err, "skipping unreadable comic archive");
                        failed += 1;
                        if preserve_existing_scanner_work(context, "comic", &source_path, &scope)
                            .await?
                        {
                            count += 1;
                        }
                        continue;
                    }
                };
                let page_count = comic_info.page_count.unwrap_or(archive_page_count);
                let rating = comic_info.community_rating;
                let (archive_mime, archive_variant) = local_comic_archive_type(&cbz_path);
                let size = fingerprint.size(&cbz_path);
                let mut assets = vec![ScannerAssetInput {
                    path: path_string(&cbz_path),
                    mime: archive_mime.to_string(),
                    role: "archive".to_string(),
                    variant: Some(archive_variant.to_string()),
                    position: None,
                    size,
                    meta: fingerprint.asset_meta(&cbz_path, json!({ "page_count": page_count })),
                }];
                if let Some(cover) = cover {
                    let mime = mime_guess::from_path(&cover)
                        .first_or_octet_stream()
                        .to_string();
                    let size = fingerprint.size(&cover);
                    assets.push(ScannerAssetInput {
                        path: path_string(&cover),
                        mime,
                        role: "cover".to_string(),
                        variant: None,
                        position: None,
                        size,
                        meta: fingerprint.asset_meta(&cover, json!({})),
                    });
                }

                let tags = comic_info
                    .tags()
                    .into_iter()
                    .map(|tag| ScannerTagInput {
                        namespace: tag.namespace,
                        key: tag.key,
                        label: tag.label,
                        source: "comic-info".to_string(),
                    })
                    .collect();
                let mut comic_meta = comic_info.meta(page_count);
                if preserve_scanner_tags {
                    comic_meta["comic_info"]["sidecar_status"] = json!("missing");
                }
                let snapshot = ScannerWorkSnapshot {
                    kind: "comic".to_string(),
                    title,
                    source_path: Some(source_path.clone()),
                    category: Some("Doujinshi".to_string()),
                    description: comic_info.description(),
                    rating,
                    meta: comic_meta,
                    fingerprint: fingerprint.value.clone(),
                    assets,
                    tags,
                    external_ids: Vec::new(),
                };
                if preserve_scanner_tags {
                    state
                        .db
                        .commit_scanner_work_snapshot_preserving_tags(
                            snapshot,
                            &context.token,
                            &scope,
                        )
                        .await?;
                } else {
                    state
                        .db
                        .commit_scanner_work_snapshot(snapshot, &context.token, &scope)
                        .await?;
                }
                count += 1;
            }
        }
        let summary = walk.finish().await?;
        if summary.complete {
            state
                .db
                .finish_scanner_scope(&scope, &context.token)
                .await?;
        }
        context.record_source_result(ScanSourceResult {
            kind: "comic".to_string(),
            provider: "local".to_string(),
            mount_name: None,
            root: path_string(&root),
            status: scan_source_status(
                summary.usable,
                summary.complete,
                summary.discovered,
                failed,
            ),
            discovered: summary.discovered,
            imported: count.saturating_sub(imported_before),
            skipped,
            failed,
            message: scan_source_message(
                summary.usable,
                summary.complete,
                summary.discovered,
                failed,
            ),
        });
    }
    count += scan_qmediasync_comics(context, &qms_sources).await?;
    finish_kind_scopes(context, "comic", &active_scopes).await?;
    Ok(count)
}

async fn scan_novels(
    context: &ScanContext<'_>,
    enqueue_enrichment: bool,
) -> Result<(usize, usize)> {
    let state = context.state;
    if catalog_v2_coordinator_owns_kind(state, "novel").await? {
        return Ok((0, 0));
    }
    let roots = context.settings.novel_roots();
    let active_scopes = roots
        .iter()
        .map(|root| context.scope("novel", root))
        .collect::<Vec<_>>();
    let mut count: usize = 0;
    let mut jobs_created = 0;
    for root in roots {
        let imported_before = count;
        let mut failed = 0_usize;
        let scope = context.prepare_scope("novel", &root).await?;
        let walk = open_matching_file_batches(
            &state.resources,
            &root,
            1,
            None,
            "novel",
            context.lease_valid.clone(),
            |path| extension_is(path, &["epub"]),
        )
        .await?;
        if !walk.usable() {
            let summary = walk.finish().await?;
            context.record_source_result(ScanSourceResult {
                kind: "novel".to_string(),
                provider: "local".to_string(),
                mount_name: None,
                root: path_string(&root),
                status: "failed".to_string(),
                discovered: summary.discovered,
                imported: count.saturating_sub(imported_before),
                skipped: 0,
                failed: 1,
                message: Some("源目录不可读或不存在".to_string()),
            });
            continue;
        }
        while let Some(batch) = walk.next_batch().await? {
            for epub_path in batch {
                context.ensure_lease_valid()?;
                let fingerprint =
                    fingerprint_paths(&state.resources, vec![epub_path.clone()]).await?;
                let source_path = path_string(&epub_path);
                let existing_scanner_work = state
                    .db
                    .scanner_work_fingerprint("novel", &source_path)
                    .await?;
                if let Some((work_id, previous)) = existing_scanner_work.as_ref() {
                    if previous == fingerprint.value.as_str() {
                        state
                            .db
                            .touch_scanner_work(*work_id, &scope, &context.token)
                            .await?;
                        if enqueue_enrichment {
                            let (_, created) = state
                                .db
                                .create_work_job_once(
                                    "enrich-lightnovel-work",
                                    *work_id,
                                    &fingerprint.value,
                                    json!({
                                        "work_id": *work_id,
                                        "fingerprint": fingerprint.value,
                                    }),
                                )
                                .await?;
                            jobs_created += usize::from(created);
                        }
                        count += 1;
                        continue;
                    }
                }
                let existing_work_id = existing_scanner_work.map(|(work_id, _)| work_id);
                let meta = match read_epub_metadata_blocking(&state.resources, epub_path.clone())
                    .await
                {
                    Ok(meta) => meta,
                    Err(err) => {
                        tracing::warn!(path = %epub_path.display(), error = %err, "skipping unreadable EPUB");
                        failed += 1;
                        if preserve_existing_scanner_work(context, "novel", &source_path, &scope)
                            .await?
                        {
                            count += 1;
                        }
                        continue;
                    }
                };
                let title = meta.title.clone().unwrap_or_else(|| {
                    epub_path
                        .file_stem()
                        .and_then(|v| v.to_str())
                        .unwrap_or("Untitled novel")
                        .to_string()
                });
                let series = meta.series.clone().or_else(|| {
                    epub_path
                        .parent()
                        .and_then(|p| p.file_name())
                        .and_then(|v| v.to_str())
                        .map(|s| s.to_string())
                });

                let previous_epub_cover = if let Some(work_id) = existing_work_id {
                    state
                        .db
                        .work_asset_path(work_id, "cover", Some("epub-extracted"))
                        .await?
                        .map(PathBuf::from)
                } else {
                    None
                };
                let mut current_epub_cover = None;

                let size = fingerprint.size(&epub_path);
                let mut assets = vec![ScannerAssetInput {
                    path: path_string(&epub_path),
                    mime: "application/epub+zip".to_string(),
                    role: "book".to_string(),
                    variant: Some("epub".to_string()),
                    position: None,
                    size,
                    meta: fingerprint.asset_meta(&epub_path, json!({})),
                }];

                match extract_epub_cover_content_addressed_blocking(
                    &state.resources,
                    epub_path.clone(),
                    state.config.generated_dir.clone(),
                )
                .await
                {
                    Ok(Some(ExtractedCover {
                        path: cover,
                        size: cover_size,
                        source_version,
                    })) => {
                        current_epub_cover = Some(cover.clone());
                        let mime = mime_guess::from_path(&cover)
                            .first_or_octet_stream()
                            .to_string();
                        assets.push(ScannerAssetInput {
                            path: path_string(&cover),
                            mime,
                            role: "cover".to_string(),
                            variant: Some("epub-extracted".to_string()),
                            position: None,
                            size: cover_size,
                            meta: json!({
                                "source": "epub",
                                "_source_version": source_version,
                            }),
                        });
                    }
                    Ok(None) => {}
                    Err(err) => {
                        tracing::warn!(path = %epub_path.display(), error = %err, "preserving previous EPUB assets after cover extraction failure");
                        failed += 1;
                        if preserve_existing_scanner_work(context, "novel", &source_path, &scope)
                            .await?
                        {
                            count += 1;
                            continue;
                        }
                    }
                }

                let mut tags = Vec::new();
                if let Some(series) = series.as_deref() {
                    tags.push(ScannerTagInput {
                        namespace: "series".to_string(),
                        key: normalize_key(series),
                        label: series.to_string(),
                        source: "epub".to_string(),
                    });
                }
                if let Some(author) = meta.creator.as_deref() {
                    tags.push(ScannerTagInput {
                        namespace: "artist".to_string(),
                        key: normalize_key(author),
                        label: author.to_string(),
                        source: "epub".to_string(),
                    });
                }
                for subject in &meta.subjects {
                    tags.push(ScannerTagInput {
                        namespace: "ln".to_string(),
                        key: normalize_key(subject),
                        label: subject.clone(),
                        source: "epub".to_string(),
                    });
                }
                if let Some(lang) = meta.language.as_deref() {
                    tags.push(ScannerTagInput {
                        namespace: "language".to_string(),
                        key: normalize_key(lang),
                        label: lang.to_string(),
                        source: "epub".to_string(),
                    });
                }
                let work_id = state
                    .db
                    .commit_scanner_work_snapshot(
                        ScannerWorkSnapshot {
                            kind: "novel".to_string(),
                            title: clean_title(&title),
                            source_path: Some(source_path.clone()),
                            category: Some("Light Novel".to_string()),
                            description: meta.description.clone(),
                            rating: None,
                            meta: json!({
                                "creator": meta.creator.clone(),
                                "language": meta.language.clone(),
                                "series": series.clone(),
                                "volume": meta.volume.clone(),
                                "source": meta.source.clone(),
                            }),
                            fingerprint: fingerprint.value.clone(),
                            assets,
                            tags,
                            external_ids: Vec::new(),
                        },
                        &context.token,
                        &scope,
                    )
                    .await?;
                cleanup_replaced_epub_cover(
                    state,
                    work_id,
                    previous_epub_cover,
                    current_epub_cover.as_ref(),
                )
                .await;
                if enqueue_enrichment {
                    let (_, created) = state
                        .db
                        .create_work_job_once(
                            "enrich-lightnovel-work",
                            work_id,
                            &fingerprint.value,
                            json!({
                                "work_id": work_id,
                                "fingerprint": fingerprint.value,
                                "title": title,
                                "series": series,
                                "creator": meta.creator,
                                "subjects": meta.subjects,
                            }),
                        )
                        .await?;
                    jobs_created += usize::from(created);
                }
                count += 1;
            }
        }
        let summary = walk.finish().await?;
        if summary.complete {
            state
                .db
                .finish_scanner_scope(&scope, &context.token)
                .await?;
        }
        context.record_source_result(ScanSourceResult {
            kind: "novel".to_string(),
            provider: "local".to_string(),
            mount_name: None,
            root: path_string(&root),
            status: scan_source_status(
                summary.usable,
                summary.complete,
                summary.discovered,
                failed,
            ),
            discovered: summary.discovered,
            imported: count.saturating_sub(imported_before),
            skipped: 0,
            failed,
            message: scan_source_message(
                summary.usable,
                summary.complete,
                summary.discovered,
                failed,
            ),
        });
    }
    finish_kind_scopes(context, "novel", &active_scopes).await?;
    Ok((count, jobs_created))
}

async fn scan_audio(context: &ScanContext<'_>, enqueue_enrichment: bool) -> Result<(usize, usize)> {
    let state = context.state;
    if catalog_v2_coordinator_owns_kind(state, "audio").await? {
        return Ok((0, 0));
    }
    let roots = context.settings.audio_roots();
    let active_scopes = roots
        .iter()
        .map(|root| context.scope("audio", root))
        .collect::<Vec<_>>();
    let mut count: usize = 0;
    let mut jobs_created = 0;
    for audio_dir in roots {
        let imported_before = count;
        let mut skipped = 0_usize;
        let mut failed = 0_usize;
        let scope = context.prepare_scope("audio", &audio_dir).await?;
        let walk = open_matching_file_batches(
            &state.resources,
            &audio_dir,
            1,
            None,
            "audio",
            context.lease_valid.clone(),
            |path| {
                audio_track_file(path) || extension_is(path, &["jpg", "jpeg", "png", "webp", "txt"])
            },
        )
        .await?;
        if !walk.usable() {
            let summary = walk.finish().await?;
            context.record_source_result(ScanSourceResult {
                kind: "audio".to_string(),
                provider: "local".to_string(),
                mount_name: None,
                root: path_string(&audio_dir),
                status: "failed".to_string(),
                discovered: summary.discovered,
                imported: count.saturating_sub(imported_before),
                skipped,
                failed: 1,
                message: Some("源目录不可读或不存在".to_string()),
            });
            continue;
        }
        let mut groups: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();
        let mut oversized_groups = BTreeSet::new();
        let mut grouped_path_bytes = 0_usize;
        let mut discovery_within_budget = true;
        while let Some(batch) = walk.next_batch().await? {
            for path in batch {
                let relative = path
                    .strip_prefix(&audio_dir)
                    .map(|value| value.to_string_lossy().replace('\\', "/"))
                    .unwrap_or_else(|_| path_string(&path));
                let parent_key = Path::new(&relative)
                    .parent()
                    .map(|value| value.to_string_lossy().replace('\\', "/"))
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| ".".to_string());
                let work_key = audio_grouping::derive_work_key(
                    context.settings.audio_grouping,
                    &relative,
                    &parent_key,
                    false,
                );
                grouped_path_bytes = grouped_path_bytes.saturating_add(relative.len());
                if grouped_path_bytes > MAX_AUDIO_DISCOVERY_PATH_BYTES {
                    discovery_within_budget = false;
                    continue;
                }
                let files = groups.entry(work_key.clone()).or_default();
                if files.len() >= inspectors::audio::MAX_AUDIO_FILES_PER_WORK {
                    oversized_groups.insert(work_key);
                    continue;
                }
                files.push(path);
            }
        }
        let walk = walk.finish().await?;
        if !walk.complete || !discovery_within_budget {
            // Do not process a partial grouping: the missing file could be a
            // track, cover, or text summary for an existing work.  Waiting for
            // a complete discovery keeps the legacy scanner's tombstone and
            // metadata-preservation fence intact.
            if !discovery_within_budget {
                tracing::warn!(
                    root = %audio_dir.display(),
                    limit_bytes = MAX_AUDIO_DISCOVERY_PATH_BYTES,
                    "preserving audio works after the discovery path budget was reached"
                );
            }
            let message = if !discovery_within_budget {
                Some("音声目录发现路径达到安全上限，已跳过缺失清理".to_string())
            } else {
                Some("源目录遍历未完成，已跳过缺失清理".to_string())
            };
            context.record_source_result(ScanSourceResult {
                kind: "audio".to_string(),
                provider: "local".to_string(),
                mount_name: None,
                root: path_string(&audio_dir),
                status: "partial".to_string(),
                discovered: walk.discovered,
                imported: count.saturating_sub(imported_before),
                skipped,
                failed,
                message,
            });
            continue;
        }

        for (work_key, mut files) in groups {
            if oversized_groups.contains(&work_key) {
                tracing::warn!(
                    work_key,
                    limit = inspectors::audio::MAX_AUDIO_FILES_PER_WORK,
                    "preserving audio work after reaching the per-work file safety limit"
                );
                skipped += 1;
                continue;
            }
            context.ensure_lease_valid()?;
            files.sort();
            let rj = audio_grouping::rj_key(&work_key);
            let grouping_key = rj.clone().unwrap_or_else(|| work_key.clone());
            let AudioFingerprintState {
                fingerprint,
                root,
                cover,
            } = match fingerprint_audio_group(
                &state.resources,
                audio_dir.clone(),
                grouping_key,
                files.clone(),
            )
            .await
            {
                Ok(state) => state,
                Err(err) => {
                    tracing::warn!(work_key, error = %err, "skipping audio work after fingerprint failure");
                    failed += 1;
                    continue;
                }
            };
            let source_path = path_string(&root);
            if let Some((work_id, previous)) = state
                .db
                .scanner_work_fingerprint("audio", &source_path)
                .await?
            {
                if previous == fingerprint.value.as_str() {
                    state
                        .db
                        .touch_scanner_work(work_id, &scope, &context.token)
                        .await?;
                    if enqueue_enrichment && rj.is_some() {
                        let (_, created) = state
                            .db
                            .create_work_job_once(
                                "enrich-asmr-work",
                                work_id,
                                &fingerprint.value,
                                json!({
                                    "work_id": work_id,
                                    "rj": rj,
                                    "fingerprint": fingerprint.value,
                                }),
                            )
                            .await?;
                        jobs_created += usize::from(created);
                    }
                    count += 1;
                    continue;
                }
            }
            let audio_metadata = match read_audio_group_metadata(&state.resources, files).await {
                Ok(metadata) => metadata,
                Err(err) => {
                    tracing::warn!(work_key, error = %err, "preserving audio work after metadata read failure");
                    failed += 1;
                    if preserve_existing_scanner_work(context, "audio", &source_path, &scope)
                        .await?
                    {
                        count += 1;
                    }
                    continue;
                }
            };
            let files = audio_metadata.files;
            let sidecar = match read_comic_info_blocking(&state.resources, root.clone()).await {
                Ok(sidecar) => sidecar,
                Err(err) => {
                    tracing::warn!(path = %root.display(), error = %err, "preserving audio work after ComicInfo.xml read failure");
                    if preserve_existing_scanner_work(context, "audio", &source_path, &scope)
                        .await?
                    {
                        count += 1;
                    }
                    continue;
                }
            };
            let sidecar_missing = sidecar.is_missing();
            let comic_info = sidecar.into_info();
            let title = comic_info
                .title_candidate()
                .map(|(_, value)| clean_title(value))
                .or_else(|| rj.as_deref().map(|rj| infer_audio_title(&root, rj)))
                .unwrap_or_else(|| {
                    root.file_name()
                        .and_then(|value| value.to_str())
                        .map(clean_title)
                        .filter(|value| !value.is_empty())
                        .unwrap_or_else(|| work_key.clone())
                });
            let track_count = files.iter().filter(|p| audio_track_file(p)).count();

            let mut work_meta = comic_info.meta(track_count as i64);
            work_meta["audio"] = json!({
                "rj": rj.clone(),
                "track_count": track_count,
                "grouping": context.settings.audio_grouping.as_str(),
                "metadata_version": AUDIO_METADATA_VERSION,
            });
            if sidecar_missing {
                work_meta["comic_info"]["sidecar_status"] = json!("missing");
            }
            let description = comic_info.description().or(audio_metadata.summary.clone());
            let work_id = state
                .db
                .upsert_scanner_work(
                    "audio",
                    &title,
                    Some(&source_path),
                    Some("Audio"),
                    description.as_deref(),
                    comic_info.community_rating,
                    work_meta,
                    &context.token,
                    &fingerprint.value,
                )
                .await?;

            let mut tags = Vec::new();
            let mut external_ids = Vec::new();
            if let Some(rj) = rj.as_deref() {
                external_ids.push(ScannerExternalIdInput {
                    source: "asmr".to_string(),
                    external_id: rj.to_string(),
                    token: None,
                    url: Some(format!("https://asmr.one/work/{rj}")),
                });
                external_ids.push(ScannerExternalIdInput {
                    source: "dlsite".to_string(),
                    external_id: rj.to_string(),
                    token: None,
                    url: Some(format!(
                        "https://www.dlsite.com/maniax/work/=/product_id/{rj}.html"
                    )),
                });
                tags.push(ScannerTagInput {
                    namespace: "audio".to_string(),
                    key: "asmr".to_string(),
                    label: "ASMR".to_string(),
                    source: "audio-folder".to_string(),
                });
                tags.push(ScannerTagInput {
                    namespace: "source".to_string(),
                    key: rj.to_lowercase(),
                    label: rj.to_string(),
                    source: "audio-folder".to_string(),
                });
            } else {
                tags.push(ScannerTagInput {
                    namespace: "audio".to_string(),
                    key: "local".to_string(),
                    label: "Audio".to_string(),
                    source: "audio-folder".to_string(),
                });
            }
            for tag in comic_info.tags() {
                tags.push(ScannerTagInput {
                    namespace: tag.namespace,
                    key: tag.key,
                    label: tag.label,
                    source: "comic-info".to_string(),
                });
            }

            let mut next_position = 0_i64;
            let mut track_positions = BTreeMap::new();
            let mut seen_track_names = BTreeSet::new();
            let mut scanner_assets =
                Vec::with_capacity(track_count.min(SCANNER_ASSET_BATCH_SIZE).saturating_add(2));
            for (file, track_meta) in files
                .iter()
                .zip(audio_metadata.tracks.iter())
                .filter(|(path, _)| audio_track_file(path))
            {
                let ext = file
                    .extension()
                    .and_then(|v| v.to_str())
                    .unwrap_or("")
                    .to_lowercase();
                let variant = infer_audio_variant(file);
                let stem = file
                    .file_stem()
                    .and_then(|v| v.to_str())
                    .unwrap_or_default()
                    .to_string();
                let track_key = normalize_track_key(&stem);
                let position = *track_positions.entry(track_key.clone()).or_insert_with(|| {
                    let current = next_position;
                    next_position += 1;
                    current
                });
                let size = fingerprint.size(file);
                let mime = mime_guess::from_path(file)
                    .first_or_octet_stream()
                    .to_string();
                let mut meta = track_meta
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| json!({ "title": stem, "quality": ext }));
                if let Some(meta) = meta.as_object_mut() {
                    meta.insert("track_key".to_string(), json!(track_key));
                    meta.insert("format".to_string(), json!(ext.clone()));
                    meta.insert(
                        "preferred_playback".to_string(),
                        json!(mime == "audio/mpeg"),
                    );
                }
                scanner_assets.push(ScannerAssetInput {
                    path: path_string(file),
                    mime,
                    role: "track".to_string(),
                    variant: Some(variant.clone()),
                    position: Some(position),
                    size,
                    meta: fingerprint.asset_meta(file, meta),
                });
                if scanner_assets.len() >= SCANNER_ASSET_BATCH_SIZE {
                    state
                        .db
                        .upsert_scanner_assets(
                            work_id,
                            std::mem::take(&mut scanner_assets),
                            &context.token,
                        )
                        .await?;
                }
                seen_track_names.insert(variant);
            }

            let mut queued_cover_paths = BTreeSet::new();
            for file in files
                .iter()
                .filter(|p| extension_is(p, &["jpg", "jpeg", "png", "webp"]))
            {
                let file_name = file
                    .file_name()
                    .and_then(|v| v.to_str())
                    .unwrap_or_default();
                if file_name.contains("ジャケット")
                    || file_name.to_ascii_lowercase().contains("cover")
                    || file_name.to_ascii_lowercase().contains("jacket")
                {
                    let mime = mime_guess::from_path(file)
                        .first_or_octet_stream()
                        .to_string();
                    let size = fingerprint.size(file);
                    queued_cover_paths.insert(file.clone());
                    scanner_assets.push(ScannerAssetInput {
                        path: path_string(file),
                        mime,
                        role: "cover".to_string(),
                        variant: None,
                        position: None,
                        size,
                        meta: fingerprint.asset_meta(file, json!({})),
                    });
                    break;
                }
            }
            if let Some(cover) = cover.filter(|path| queued_cover_paths.insert(path.clone())) {
                let mime = mime_guess::from_path(&cover)
                    .first_or_octet_stream()
                    .to_string();
                let size = fingerprint.size(&cover);
                scanner_assets.push(ScannerAssetInput {
                    path: path_string(&cover),
                    mime,
                    role: "cover".to_string(),
                    variant: None,
                    position: None,
                    size,
                    meta: fingerprint.asset_meta(&cover, json!({})),
                });
            }
            if !scanner_assets.is_empty() {
                state
                    .db
                    .upsert_scanner_assets(work_id, scanner_assets, &context.token)
                    .await?;
            }

            for variant in seen_track_names {
                tags.push(ScannerTagInput {
                    namespace: "audio".to_string(),
                    key: normalize_key(&variant),
                    label: variant,
                    source: "audio-folder".to_string(),
                });
            }
            if sidecar_missing {
                state
                    .db
                    .commit_scanner_work_metadata_preserving_tags(
                        work_id,
                        tags,
                        external_ids,
                        &context.token,
                        &scope,
                        &fingerprint.value,
                    )
                    .await?;
            } else {
                state
                    .db
                    .commit_scanner_work_metadata(
                        work_id,
                        tags,
                        external_ids,
                        &context.token,
                        &scope,
                        &fingerprint.value,
                    )
                    .await?;
            }
            if enqueue_enrichment && rj.is_some() {
                let (_, created) = state
                    .db
                    .create_work_job_once(
                        "enrich-asmr-work",
                        work_id,
                        &fingerprint.value,
                        json!({
                            "work_id": work_id,
                            "rj": rj,
                            "fingerprint": fingerprint.value,
                        }),
                    )
                    .await?;
                jobs_created += usize::from(created);
            }
            count += 1;
        }
        state
            .db
            .finish_scanner_scope(&scope, &context.token)
            .await?;
        context.record_source_result(ScanSourceResult {
            kind: "audio".to_string(),
            provider: "local".to_string(),
            mount_name: None,
            root: path_string(&audio_dir),
            status: scan_source_status(true, true, walk.discovered, failed),
            discovered: walk.discovered,
            imported: count.saturating_sub(imported_before),
            skipped,
            failed,
            message: scan_source_message(true, true, walk.discovered, failed),
        });
    }
    finish_kind_scopes(context, "audio", &active_scopes).await?;
    Ok((count, jobs_created))
}

async fn scan_gallery(context: &ScanContext<'_>) -> Result<usize> {
    let state = context.state;
    if catalog_v2_coordinator_owns_kind(state, "gallery").await? {
        return Ok(0);
    }
    let roots = context.settings.gallery_roots();
    let active_scopes = roots
        .iter()
        .map(|root| context.scope("gallery", root))
        .collect::<Vec<_>>();
    let mut count: usize = 0;
    for root in roots {
        let imported_before = count;
        let mut skipped = 0_usize;
        let mut failed = 0_usize;
        let scope = context.prepare_scope("gallery", &root).await?;
        let walk = walk_matching_parent_directories(
            &state.resources,
            &root,
            1,
            None,
            "gallery",
            context.lease_valid.clone(),
            gallery_image_name,
        )
        .await?;
        if !walk.usable || !walk.complete {
            let status = scan_source_status(walk.usable, walk.complete, walk.discovered, 1);
            let message = scan_source_message(walk.usable, walk.complete, walk.discovered, 1);
            context.record_source_result(ScanSourceResult {
                kind: "gallery".to_string(),
                provider: "local".to_string(),
                mount_name: None,
                root: path_string(&root),
                status,
                discovered: walk.discovered,
                imported: count.saturating_sub(imported_before),
                skipped,
                failed: 1,
                message,
            });
            continue;
        }
        let mut scope_complete = true;
        for folder in walk.files {
            context.ensure_lease_valid()?;
            let (mut files, folder_complete) = list_matching_files_in_directory(
                &state.resources,
                &folder,
                context.lease_valid.clone(),
                gallery_image_name,
            )
            .await?;
            if !folder_complete {
                scope_complete = false;
                failed += 1;
                // Do not commit a partial folder snapshot.  The next
                // complete reconcile will decide whether entries are truly
                // missing, preserving the scanner's tombstone safety fence.
                continue;
            }
            if files.is_empty() {
                continue;
            }
            files.sort_by_cached_key(|path| naturalish_key(&path_string(path)));
            let fingerprint =
                match fingerprint_gallery_paths(&state.resources, root.clone(), files).await {
                    Ok(fingerprint) => fingerprint,
                    Err(err) => {
                        tracing::warn!(
                            path = %folder.display(),
                            error = %err,
                            "preserving gallery after file metadata changed during scan"
                        );
                        failed += 1;
                        scope_complete = false;
                        continue;
                    }
                };
            if fingerprint.files.is_empty() {
                scope_complete = false;
                skipped += 1;
                continue;
            }
            let title = folder
                .file_name()
                .and_then(|value| value.to_str())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("图库")
                .to_string();
            let relative = folder
                .strip_prefix(&root)
                .ok()
                .map(|value| value.to_string_lossy().to_string())
                .filter(|value| !value.is_empty());
            let source_path = path_string(&folder);
            if let Some((work_id, previous)) = state
                .db
                .scanner_work_fingerprint("gallery", &source_path)
                .await?
            {
                if previous == fingerprint.value.as_str() {
                    state
                        .db
                        .touch_scanner_work(work_id, &scope, &context.token)
                        .await?;
                    count += 1;
                    continue;
                }
            }
            let work_id = state
                .db
                .upsert_scanner_work(
                    "gallery",
                    &clean_title(&title),
                    Some(&source_path),
                    Some("Gallery"),
                    relative.as_deref(),
                    None,
                    json!({
                        "image_count": fingerprint.files.len(),
                        "root": path_string(&root),
                        "folder": path_string(&folder),
                    }),
                    &context.token,
                    &fingerprint.value,
                )
                .await?;

            let cover_file = gallery_cover_candidate(&fingerprint.files);
            let mut scanner_assets = Vec::with_capacity(
                fingerprint
                    .files
                    .len()
                    .min(SCANNER_ASSET_BATCH_SIZE)
                    .saturating_add(usize::from(cover_file.is_some())),
            );
            if let Some(cover) = cover_file {
                let mime = mime_guess::from_path(&cover.path)
                    .first_or_octet_stream()
                    .to_string();
                scanner_assets.push(ScannerAssetInput {
                    path: path_string(&cover.path),
                    mime,
                    role: "cover".to_string(),
                    variant: None,
                    position: None,
                    size: cover.size,
                    meta: cover.asset_meta(json!({ "source": "gallery" })),
                });
            }

            for (index, file) in fingerprint.files.iter().enumerate() {
                let mime = mime_guess::from_path(&file.path)
                    .first_or_octet_stream()
                    .to_string();
                scanner_assets.push(ScannerAssetInput {
                    path: path_string(&file.path),
                    mime,
                    role: "image".to_string(),
                    variant: None,
                    position: Some(index as i64),
                    size: file.size,
                    meta: file.asset_meta(json!({ "source": "gallery" })),
                });
                if scanner_assets.len() >= SCANNER_ASSET_BATCH_SIZE {
                    state
                        .db
                        .upsert_scanner_assets(
                            work_id,
                            std::mem::take(&mut scanner_assets),
                            &context.token,
                        )
                        .await?;
                }
            }
            if !scanner_assets.is_empty() {
                state
                    .db
                    .upsert_scanner_assets(work_id, scanner_assets, &context.token)
                    .await?;
            }

            let mut tags = vec![
                ScannerTagInput {
                    namespace: "gallery".to_string(),
                    key: "image-set".to_string(),
                    label: "图库".to_string(),
                    source: "gallery-folder".to_string(),
                },
                ScannerTagInput {
                    namespace: "folder".to_string(),
                    key: normalize_key(&title),
                    label: title.clone(),
                    source: "gallery-folder".to_string(),
                },
            ];
            if let Some(top) = folder
                .strip_prefix(&root)
                .ok()
                .and_then(|path| path.components().next())
                .and_then(|part| part.as_os_str().to_str())
                .filter(|value| !value.trim().is_empty())
            {
                tags.push(ScannerTagInput {
                    namespace: "artist".to_string(),
                    key: normalize_key(top),
                    label: top.to_string(),
                    source: "gallery-folder".to_string(),
                });
            }
            let mut filename_tags = BTreeSet::new();
            for file in &fingerprint.files {
                filename_tags.extend(gallery_filename_tags(&file.path));
            }
            for tag in filename_tags {
                tags.push(ScannerTagInput {
                    namespace: "gallery".to_string(),
                    key: normalize_key(&tag),
                    label: tag,
                    source: "gallery-filename".to_string(),
                });
            }
            state
                .db
                .commit_scanner_work_metadata(
                    work_id,
                    tags,
                    Vec::new(),
                    &context.token,
                    &scope,
                    &fingerprint.value,
                )
                .await?;
            count += 1;
        }
        if walk.complete && scope_complete {
            state
                .db
                .finish_scanner_scope(&scope, &context.token)
                .await?;
        }
        context.record_source_result(ScanSourceResult {
            kind: "gallery".to_string(),
            provider: "local".to_string(),
            mount_name: None,
            root: path_string(&root),
            status: scan_source_status(
                true,
                walk.complete && scope_complete,
                walk.discovered,
                failed,
            ),
            discovered: walk.discovered,
            imported: count.saturating_sub(imported_before),
            skipped,
            failed,
            message: scan_source_message(
                true,
                walk.complete && scope_complete,
                walk.discovered,
                failed,
            ),
        });
    }
    finish_kind_scopes(context, "gallery", &active_scopes).await?;
    Ok(count)
}

async fn scan_coser_pictures(context: &ScanContext<'_>) -> Result<usize> {
    let state = context.state;
    if catalog_v2_coordinator_owns_kind(state, "coser-picture").await? {
        return Ok(0);
    }
    let roots = context.settings.coser_picture_roots();
    let qms_sources = vfs::qmediasync_scan_sources(&context.settings, "coser-picture");
    let mut active_scopes = roots
        .iter()
        .map(|root| context.scope("coser-picture", root))
        .collect::<Vec<_>>();
    active_scopes.extend(
        qms_sources
            .iter()
            .map(|source| context.scope("coser-picture", Path::new(&source.root))),
    );
    let mut count: usize = 0;
    for root in roots {
        let imported_before = count;
        let mut skipped = 0_usize;
        let mut failed = 0_usize;
        let scope = context.prepare_scope("coser-picture", &root).await?;
        let walk = open_matching_file_batches(
            &state.resources,
            &root,
            1,
            None,
            "CoserPicture",
            context.lease_valid.clone(),
            |path| extension_is(path, &["zip"]),
        )
        .await?;
        if !walk.usable() {
            let summary = walk.finish().await?;
            context.record_source_result(ScanSourceResult {
                kind: "coser-picture".to_string(),
                provider: "local".to_string(),
                mount_name: None,
                root: path_string(&root),
                status: "failed".to_string(),
                discovered: summary.discovered,
                imported: count.saturating_sub(imported_before),
                skipped,
                failed: 1,
                message: Some("源目录不可读或不存在".to_string()),
            });
            continue;
        }
        while let Some(batch) = walk.next_batch().await? {
            for zip_path in batch {
                context.ensure_lease_valid()?;
                let source_path = path_string(&zip_path);
                let ArchiveScanState {
                    fingerprint,
                    cover,
                    fallback_title,
                } = fingerprint_archive_with_sidecar_version(
                    &state.resources,
                    zip_path.clone(),
                    true,
                    "coser-picture-v2",
                )
                .await?;
                if let Some((work_id, previous)) = state
                    .db
                    .scanner_work_fingerprint("coser-picture", &source_path)
                    .await?
                {
                    if previous == fingerprint.value.as_str() {
                        state
                            .db
                            .touch_scanner_work(work_id, &scope, &context.token)
                            .await?;
                        count += 1;
                        continue;
                    }
                }
                let page_count = match count_cbz_pages_blocking(&state.resources, zip_path.clone())
                    .await
                {
                    Ok(page_count) => page_count,
                    Err(err) => {
                        tracing::warn!(path = %zip_path.display(), error = %err, "skipping unreadable CoserPicture archive");
                        failed += 1;
                        if preserve_existing_scanner_work(
                            context,
                            "coser-picture",
                            &source_path,
                            &scope,
                        )
                        .await?
                        {
                            count += 1;
                        }
                        continue;
                    }
                };
                if page_count <= 0 {
                    skipped += 1;
                    if preserve_existing_scanner_work(
                        context,
                        "coser-picture",
                        &source_path,
                        &scope,
                    )
                    .await?
                    {
                        count += 1;
                    }
                    continue;
                }
                let sidecar = match read_comic_info_blocking(
                    &state.resources,
                    zip_path.parent().unwrap_or(root.as_path()).to_path_buf(),
                )
                .await
                {
                    Ok(sidecar) => sidecar,
                    Err(err) => {
                        tracing::warn!(path = %zip_path.display(), error = %err, "preserving CoserPicture after ComicInfo.xml read failure");
                        failed += 1;
                        if preserve_existing_scanner_work(
                            context,
                            "coser-picture",
                            &source_path,
                            &scope,
                        )
                        .await?
                        {
                            count += 1;
                        }
                        continue;
                    }
                };
                let sidecar_missing = sidecar.is_missing();
                let comic_info = sidecar.into_info();
                let title = comic_info.display_title(&fallback_title);
                let coser = zip_path
                    .parent()
                    .and_then(|path| path.file_name())
                    .and_then(|value| value.to_str())
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or("CoserPicture")
                    .to_string();
                let relative = zip_path
                    .strip_prefix(&root)
                    .ok()
                    .map(|value| value.to_string_lossy().to_string())
                    .filter(|value| !value.is_empty());

                let size = fingerprint.size(&zip_path);
                let mut metadata = comic_info.meta(page_count);
                metadata["source"] = json!("coser-picture");
                metadata["archive"] = json!(path_string(&zip_path));
                metadata["coser"] = json!(coser.clone());
                if sidecar_missing {
                    metadata["comic_info"]["sidecar_status"] = json!("missing");
                }
                let mut tags = vec![
                    ScannerTagInput {
                        namespace: "coser-picture".to_string(),
                        key: "image-set".to_string(),
                        label: "CoserPicture".to_string(),
                        source: "coser-picture-zip".to_string(),
                    },
                    ScannerTagInput {
                        namespace: "folder".to_string(),
                        key: normalize_key(&coser),
                        label: coser.clone(),
                        source: "coser-picture-zip".to_string(),
                    },
                    ScannerTagInput {
                        namespace: "artist".to_string(),
                        key: normalize_key(&coser),
                        label: coser.clone(),
                        source: "coser-picture-zip".to_string(),
                    },
                ];
                tags.extend(comic_info.tags().into_iter().map(|tag| ScannerTagInput {
                    namespace: tag.namespace,
                    key: tag.key,
                    label: tag.label,
                    source: "comic-info".to_string(),
                }));
                let snapshot = ScannerWorkSnapshot {
                    kind: "coser-picture".to_string(),
                    title: clean_title(&title),
                    source_path: Some(source_path.clone()),
                    category: Some("CoserPicture".to_string()),
                    description: comic_info.description().or(relative),
                    rating: comic_info.community_rating,
                    meta: metadata,
                    fingerprint: fingerprint.value.clone(),
                    assets: {
                        let mut assets = vec![ScannerAssetInput {
                            path: source_path,
                            mime: "application/zip".to_string(),
                            role: "archive".to_string(),
                            variant: Some("zip".to_string()),
                            position: None,
                            size,
                            meta: fingerprint.asset_meta(
                                &zip_path,
                                json!({
                                    "source": "coser-picture",
                                    "page_count": page_count
                                }),
                            ),
                        }];
                        if let Some(cover) = cover {
                            let cover_mime = mime_guess::from_path(&cover)
                                .first_or_octet_stream()
                                .to_string();
                            assets.push(ScannerAssetInput {
                                path: path_string(&cover),
                                mime: cover_mime,
                                role: "cover".to_string(),
                                variant: None,
                                position: None,
                                size: fingerprint.size(&cover),
                                meta: fingerprint.asset_meta(&cover, json!({})),
                            });
                        }
                        assets
                    },
                    tags,
                    external_ids: Vec::new(),
                };
                if sidecar_missing {
                    state
                        .db
                        .commit_scanner_work_snapshot_preserving_tags(
                            snapshot,
                            &context.token,
                            &scope,
                        )
                        .await?;
                } else {
                    state
                        .db
                        .commit_scanner_work_snapshot(snapshot, &context.token, &scope)
                        .await?;
                }
                count += 1;
            }
        }
        let summary = walk.finish().await?;
        if summary.complete {
            state
                .db
                .finish_scanner_scope(&scope, &context.token)
                .await?;
        }
        context.record_source_result(ScanSourceResult {
            kind: "coser-picture".to_string(),
            provider: "local".to_string(),
            mount_name: None,
            root: path_string(&root),
            status: scan_source_status(
                summary.usable,
                summary.complete,
                summary.discovered,
                failed,
            ),
            discovered: summary.discovered,
            imported: count.saturating_sub(imported_before),
            skipped,
            failed,
            message: scan_source_message(
                summary.usable,
                summary.complete,
                summary.discovered,
                failed,
            ),
        });
    }
    count += scan_qmediasync_coser_pictures(context, &qms_sources).await?;
    finish_kind_scopes(context, "coser-picture", &active_scopes).await?;
    Ok(count)
}

async fn scan_qmediasync_comics(
    context: &ScanContext<'_>,
    sources: &[settings::MediaSourceSettings],
) -> Result<usize> {
    let state = context.state;
    let mut count: usize = 0;
    for source in sources {
        let imported_before = count;
        let mut skipped = 0_usize;
        let mut failed = 0_usize;
        let root = PathBuf::from(&source.root);
        let scope = context.prepare_scope("comic", &root).await?;
        state
            .db
            .adopt_scanner_scope(
                "comic",
                &format!("qms-strm://{}", source.mount_name),
                &scope,
                &context.token,
            )
            .await?;
        let walk = walk_matching_files(
            &state.resources,
            &root,
            1,
            Some(source.scan_depth.clamp(1, 64)),
            "qmediasync comic",
            context.lease_valid.clone(),
            |path| is_strm_file(path) || extension_is(path, &["cbz"]),
        )
        .await?;
        if !walk.usable || !walk.complete {
            context.record_source_result(ScanSourceResult {
                kind: source.kind.clone(),
                provider: source.provider.clone(),
                mount_name: Some(source.mount_name.clone()),
                root: path_string(&root),
                status: if walk.usable { "partial" } else { "failed" }.to_string(),
                discovered: walk.files.len(),
                imported: count.saturating_sub(imported_before),
                skipped,
                failed: if walk.usable { 0 } else { 1 },
                message: Some(if walk.usable {
                    "源目录遍历未完成，已跳过缺失清理".to_string()
                } else {
                    "源目录不可读，已跳过缺失清理".to_string()
                }),
            });
            continue;
        }
        let discovered = walk.files.len();
        let complete = walk.complete;
        for archive_path in walk.files {
            context.ensure_lease_valid()?;
            let is_strm = is_strm_file(&archive_path);
            if is_strm && source::is_secondary_volume(&archive_path) {
                skipped += 1;
                continue;
            }
            let dir = archive_path
                .parent()
                .unwrap_or(root.as_path())
                .to_path_buf();
            let relative = archive_path
                .strip_prefix(&root)
                .unwrap_or(&archive_path)
                .to_string_lossy()
                .replace('\\', "/");
            let archive_uri = if is_strm {
                vfs::qms_strm_uri(&source.mount_name, &relative)
            } else {
                path_string(&archive_path)
            };
            let ArchiveScanState {
                fingerprint,
                cover,
                fallback_title,
            } = fingerprint_archive(&state.resources, archive_path.clone(), true, true).await?;
            if let Some((work_id, previous)) = state
                .db
                .scanner_work_fingerprint("comic", &archive_uri)
                .await?
            {
                if previous == fingerprint.value.as_str() {
                    state
                        .db
                        .touch_scanner_work(work_id, &scope, &context.token)
                        .await?;
                    count += 1;
                    continue;
                }
            }
            let target_url = if is_strm {
                match read_qms_strm_url_blocking(&state.resources, archive_path.clone()).await {
                    Ok(target_url) => Some(target_url),
                    Err(err) => {
                        tracing::warn!(
                            path = %archive_path.to_string_lossy(),
                            error = %err,
                            "skipping invalid qmediasync STRM file"
                        );
                        if preserve_existing_scanner_work(context, "comic", &archive_uri, &scope)
                            .await?
                        {
                            count += 1;
                        }
                        failed += 1;
                        continue;
                    }
                }
            } else {
                None
            };
            if let Some(target_url) = target_url.as_deref() {
                if !source::classify(&archive_path, Some(target_url)).is_archive() {
                    skipped += 1;
                    continue;
                }
            }
            let existing_work_id = state.db.scanner_work_id("comic", &archive_uri).await?;
            let comic_info_read = match read_comic_info_blocking(&state.resources, dir.clone())
                .await
            {
                Ok(comic_info) => comic_info,
                Err(err) => {
                    tracing::warn!(path = %dir.display(), error = %err, "preserving qmediasync comic after ComicInfo.xml read failure");
                    if preserve_existing_scanner_work(context, "comic", &archive_uri, &scope)
                        .await?
                    {
                        count += 1;
                    }
                    failed += 1;
                    continue;
                }
            };
            let preserve_scanner_tags = comic_info_read.is_missing() && existing_work_id.is_some();
            let comic_info = comic_info_read.into_info();
            let title = comic_info.display_title(&fallback_title);
            let archive_page_count = if is_strm {
                None
            } else {
                match count_cbz_pages_blocking(&state.resources, archive_path.clone()).await {
                    Ok(page_count) => Some(page_count),
                    Err(err) => {
                        tracing::warn!(path = %archive_path.display(), error = %err, "skipping unreadable qmediasync comic archive");
                        if preserve_existing_scanner_work(context, "comic", &archive_uri, &scope)
                            .await?
                        {
                            count += 1;
                        }
                        failed += 1;
                        continue;
                    }
                }
            };
            let page_count = comic_info.page_count.or(archive_page_count).unwrap_or(0);
            let work_id = state
                .db
                .upsert_scanner_work(
                    "comic",
                    &title,
                    Some(&archive_uri),
                    Some("Doujinshi"),
                    comic_info.description().as_deref(),
                    comic_info.community_rating,
                    {
                        let mut meta = comic_info.meta(page_count);
                        if preserve_scanner_tags {
                            meta["comic_info"]["sidecar_status"] = json!("missing");
                        }
                        meta["source"] = json!("qmediasync");
                        meta["provider"] = json!("qmediasync");
                        meta["mount_name"] = json!(source.mount_name);
                        meta["strm_root"] = json!(path_string(&root));
                        meta
                    },
                    &context.token,
                    &fingerprint.value,
                )
                .await?;

            let size = fingerprint.size(&archive_path);
            let meta = if let Some(target_url) = target_url.as_deref() {
                let volume_paths = qms_volume_paths(&root, &source.mount_name, &archive_path);
                let missing_volumes = qms_volume_missing_names(&archive_path);
                vfs::qms_strm_meta_json_with_volumes(
                    &source.mount_name,
                    &root,
                    &archive_path,
                    &relative,
                    target_url,
                    &volume_paths,
                    &missing_volumes,
                )
                .await
            } else {
                json!({ "source": "qmediasync", "provider": "qmediasync", "page_count": page_count })
            };
            let (archive_mime, archive_variant) = target_url
                .as_deref()
                .map(qms_remote_archive_type)
                .unwrap_or(("application/vnd.comicbook+zip", "cbz"));
            state
                .db
                .upsert_scanner_asset(
                    work_id,
                    &archive_uri,
                    archive_mime,
                    "archive",
                    Some(qms_archive_variant(archive_variant, is_strm)),
                    None,
                    size,
                    fingerprint.asset_meta(&archive_path, meta),
                    &context.token,
                )
                .await?;

            if let Some(cover) = cover {
                let mime = mime_guess::from_path(&cover)
                    .first_or_octet_stream()
                    .to_string();
                let size = fingerprint.size(&cover);
                state
                    .db
                    .upsert_scanner_asset(
                        work_id,
                        &path_string(&cover),
                        &mime,
                        "cover",
                        None,
                        None,
                        size,
                        fingerprint.asset_meta(&cover, json!({ "source": "qmediasync" })),
                        &context.token,
                    )
                    .await?;
            }

            for tag in comic_info.tags() {
                link_tag(
                    context,
                    work_id,
                    &tag.namespace,
                    &tag.key,
                    &tag.label,
                    "comic-info",
                )
                .await?;
            }
            link_tag(
                context,
                work_id,
                "source",
                "qmediasync",
                "qmediasync",
                "qmediasync",
            )
            .await?;
            if preserve_scanner_tags {
                context
                    .finish_work_preserving_tags(work_id, &scope, &fingerprint.value)
                    .await?;
            } else {
                context
                    .finish_work(work_id, &scope, &fingerprint.value)
                    .await?;
            }
            count += 1;
        }
        if complete {
            state
                .db
                .finish_scanner_scope(&scope, &context.token)
                .await?;
        }
        let imported = count.saturating_sub(imported_before);
        context.record_source_result(ScanSourceResult {
            kind: source.kind.clone(),
            provider: source.provider.clone(),
            mount_name: Some(source.mount_name.clone()),
            root: path_string(&root),
            status: if failed > 0 && imported > 0 {
                "partial"
            } else if failed > 0 {
                "failed"
            } else {
                "success"
            }
            .to_string(),
            discovered,
            imported,
            skipped,
            failed,
            message: (failed > 0).then(|| "部分候选文件未能导入；已保留既有作品".to_string()),
        });
    }
    Ok(count)
}

async fn scan_qmediasync_coser_pictures(
    context: &ScanContext<'_>,
    sources: &[settings::MediaSourceSettings],
) -> Result<usize> {
    let state = context.state;
    let mut count: usize = 0;
    for source in sources {
        let imported_before = count;
        let mut skipped = 0_usize;
        let mut failed = 0_usize;
        let root = PathBuf::from(&source.root);
        let scope = context.prepare_scope("coser-picture", &root).await?;
        state
            .db
            .adopt_scanner_scope(
                "coser-picture",
                &format!("qms-strm://{}", source.mount_name),
                &scope,
                &context.token,
            )
            .await?;
        let walk = walk_matching_files(
            &state.resources,
            &root,
            1,
            Some(source.scan_depth.clamp(1, 64)),
            "qmediasync CoserPicture",
            context.lease_valid.clone(),
            |path| is_strm_file(path) || extension_is(path, &["zip"]),
        )
        .await?;
        if !walk.usable || !walk.complete {
            context.record_source_result(ScanSourceResult {
                kind: source.kind.clone(),
                provider: source.provider.clone(),
                mount_name: Some(source.mount_name.clone()),
                root: path_string(&root),
                status: if walk.usable { "partial" } else { "failed" }.to_string(),
                discovered: walk.discovered,
                imported: count.saturating_sub(imported_before),
                skipped,
                failed: if walk.usable { 1 } else { 1 },
                message: Some(if walk.usable {
                    "源目录遍历未完成，已跳过缺失清理".to_string()
                } else {
                    "源目录不可读，已跳过缺失清理".to_string()
                }),
            });
            continue;
        }
        for archive_path in walk.files {
            context.ensure_lease_valid()?;
            let is_strm = is_strm_file(&archive_path);
            if is_strm && source::is_secondary_volume(&archive_path) {
                skipped += 1;
                continue;
            }
            let dir = archive_path
                .parent()
                .unwrap_or(root.as_path())
                .to_path_buf();
            let relative = archive_path
                .strip_prefix(&root)
                .unwrap_or(&archive_path)
                .to_string_lossy()
                .replace('\\', "/");
            let archive_uri = if is_strm {
                vfs::qms_strm_uri(&source.mount_name, &relative)
            } else {
                path_string(&archive_path)
            };
            let ArchiveScanState {
                fingerprint,
                cover,
                fallback_title,
            } = fingerprint_archive_with_sidecar_version(
                &state.resources,
                archive_path.clone(),
                true,
                "coser-picture-v2",
            )
            .await?;
            if let Some((work_id, previous)) = state
                .db
                .scanner_work_fingerprint("coser-picture", &archive_uri)
                .await?
            {
                if previous == fingerprint.value.as_str() {
                    state
                        .db
                        .touch_scanner_work(work_id, &scope, &context.token)
                        .await?;
                    count += 1;
                    continue;
                }
            }
            let target_url = if is_strm {
                match read_qms_strm_url_blocking(&state.resources, archive_path.clone()).await {
                    Ok(target_url) => Some(target_url),
                    Err(err) => {
                        tracing::warn!(
                            path = %archive_path.to_string_lossy(),
                            error = %err,
                            "skipping invalid qmediasync CoserPicture STRM file"
                        );
                        if preserve_existing_scanner_work(
                            context,
                            "coser-picture",
                            &archive_uri,
                            &scope,
                        )
                        .await?
                        {
                            count += 1;
                        }
                        failed += 1;
                        continue;
                    }
                }
            } else {
                None
            };
            if let Some(target_url) = target_url.as_deref() {
                if !source::classify(&archive_path, Some(target_url)).is_archive() {
                    skipped += 1;
                    continue;
                }
            }
            let page_count = if is_strm {
                0
            } else {
                match count_cbz_pages_blocking(&state.resources, archive_path.clone()).await {
                    Ok(page_count) => page_count,
                    Err(err) => {
                        tracing::warn!(path = %archive_path.display(), error = %err, "skipping unreadable qmediasync CoserPicture archive");
                        if preserve_existing_scanner_work(
                            context,
                            "coser-picture",
                            &archive_uri,
                            &scope,
                        )
                        .await?
                        {
                            count += 1;
                        }
                        failed += 1;
                        continue;
                    }
                }
            };
            let sidecar = match read_comic_info_blocking(&state.resources, dir.clone()).await {
                Ok(sidecar) => sidecar,
                Err(err) => {
                    tracing::warn!(path = %dir.display(), error = %err, "preserving qmediasync CoserPicture after ComicInfo.xml read failure");
                    if preserve_existing_scanner_work(
                        context,
                        "coser-picture",
                        &archive_uri,
                        &scope,
                    )
                    .await?
                    {
                        count += 1;
                    }
                    failed += 1;
                    continue;
                }
            };
            let sidecar_missing = sidecar.is_missing();
            let comic_info = sidecar.into_info();
            let title = comic_info.display_title(&fallback_title);
            let coser = dir
                .file_name()
                .and_then(|value| value.to_str())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("CoserPicture")
                .to_string();
            let mut work_meta = comic_info.meta(page_count);
            work_meta["source"] = json!("qmediasync");
            work_meta["provider"] = json!("qmediasync");
            work_meta["mount_name"] = json!(source.mount_name.clone());
            work_meta["strm_root"] = json!(path_string(&root));
            work_meta["coser"] = json!(coser.clone());
            if sidecar_missing {
                work_meta["comic_info"]["sidecar_status"] = json!("missing");
            }
            let work_id = state
                .db
                .upsert_scanner_work(
                    "coser-picture",
                    &clean_title(&title),
                    Some(&archive_uri),
                    Some("CoserPicture"),
                    comic_info.description().as_deref().or(Some(&relative)),
                    comic_info.community_rating,
                    work_meta,
                    &context.token,
                    &fingerprint.value,
                )
                .await?;

            let size = fingerprint.size(&archive_path);
            let meta = if let Some(target_url) = target_url.as_deref() {
                let volume_paths = qms_volume_paths(&root, &source.mount_name, &archive_path);
                let missing_volumes = qms_volume_missing_names(&archive_path);
                vfs::qms_strm_meta_json_with_volumes(
                    &source.mount_name,
                    &root,
                    &archive_path,
                    &relative,
                    target_url,
                    &volume_paths,
                    &missing_volumes,
                )
                .await
            } else {
                json!({ "source": "qmediasync", "provider": "qmediasync", "page_count": page_count })
            };
            let (archive_mime, archive_variant) = target_url
                .as_deref()
                .map(qms_remote_archive_type)
                .unwrap_or(("application/zip", "zip"));
            state
                .db
                .upsert_scanner_asset(
                    work_id,
                    &archive_uri,
                    archive_mime,
                    "archive",
                    Some(qms_archive_variant(archive_variant, is_strm)),
                    None,
                    size,
                    fingerprint.asset_meta(&archive_path, meta),
                    &context.token,
                )
                .await?;

            if let Some(cover) = cover {
                let mime = mime_guess::from_path(&cover)
                    .first_or_octet_stream()
                    .to_string();
                let size = fingerprint.size(&cover);
                state
                    .db
                    .upsert_scanner_asset(
                        work_id,
                        &path_string(&cover),
                        &mime,
                        "cover",
                        None,
                        None,
                        size,
                        fingerprint.asset_meta(&cover, json!({ "source": "qmediasync" })),
                        &context.token,
                    )
                    .await?;
            }

            link_tag(
                context,
                work_id,
                "coser-picture",
                "image-set",
                "CoserPicture",
                "qmediasync",
            )
            .await?;
            for tag in comic_info.tags() {
                link_tag(
                    context,
                    work_id,
                    &tag.namespace,
                    &tag.key,
                    &tag.label,
                    "comic-info",
                )
                .await?;
            }
            link_tag(
                context,
                work_id,
                "artist",
                &normalize_key(&coser),
                &coser,
                "qmediasync",
            )
            .await?;
            link_tag(
                context,
                work_id,
                "source",
                "qmediasync",
                "qmediasync",
                "qmediasync",
            )
            .await?;
            if sidecar_missing {
                context
                    .finish_work_preserving_tags(work_id, &scope, &fingerprint.value)
                    .await?;
            } else {
                context
                    .finish_work(work_id, &scope, &fingerprint.value)
                    .await?;
            }
            count += 1;
        }
        if walk.complete {
            state
                .db
                .finish_scanner_scope(&scope, &context.token)
                .await?;
        }
        let imported = count.saturating_sub(imported_before);
        context.record_source_result(ScanSourceResult {
            kind: source.kind.clone(),
            provider: source.provider.clone(),
            mount_name: Some(source.mount_name.clone()),
            root: path_string(&root),
            status: scan_source_status(true, walk.complete, walk.discovered, failed),
            discovered: walk.discovered,
            imported,
            skipped,
            failed,
            message: scan_source_message(true, walk.complete, walk.discovered, failed),
        });
    }
    Ok(count)
}

fn is_strm_file(path: &Path) -> bool {
    extension_is(path, &["strm"])
}

fn qms_volume_paths(root: &Path, mount_name: &str, primary: &Path) -> Vec<String> {
    if !source::is_primary_volume(primary) {
        return Vec::new();
    }
    let Some(group_key) = source::volume_group_key(primary) else {
        return Vec::new();
    };
    let Some(parent) = primary.parent() else {
        return Vec::new();
    };
    let mut members = std::fs::read_dir(parent)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| is_strm_file(path) && path.parent() == Some(parent))
        .filter_map(|path| {
            let part = source::volume_part(&path)?;
            (part.group_key == group_key).then_some((part.index, path))
        })
        .collect::<Vec<_>>();
    if members.len() <= 1 {
        return Vec::new();
    }
    members.sort_by_key(|(index, path)| (*index, path.clone()));
    members
        .into_iter()
        .filter_map(|(_, path)| {
            let relative = path
                .strip_prefix(root)
                .ok()?
                .to_string_lossy()
                .replace('\\', "/");
            Some(vfs::qms_strm_uri(mount_name, &relative))
        })
        .collect()
}

pub(crate) fn qms_volume_missing_names(primary: &Path) -> Vec<String> {
    let Some(group_key) = source::volume_group_key(primary) else {
        return Vec::new();
    };
    let Some(parent) = primary.parent() else {
        return Vec::new();
    };
    let members = std::fs::read_dir(parent)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| is_strm_file(path) && path.parent() == Some(parent))
        .filter_map(|path| {
            let part = source::volume_part(&path)?;
            (part.group_key == group_key).then_some((part.index, path))
        })
        .collect::<Vec<_>>();
    let Some(max_index) = members.iter().map(|(index, _)| *index).max() else {
        return Vec::new();
    };
    if members.len() <= 1 || max_index <= 1 {
        return Vec::new();
    }
    let present = members
        .iter()
        .map(|(index, _)| *index)
        .collect::<BTreeSet<_>>();
    (1..=max_index)
        .filter(|index| !present.contains(index))
        .filter_map(|index| expected_volume_name(primary, index))
        .collect()
}

fn expected_volume_name(primary: &Path, index: u32) -> Option<String> {
    let raw = primary.file_name()?.to_str()?.strip_suffix(".strm")?;
    let lower = raw.to_ascii_lowercase();
    if let Some(part_start) = lower.rfind(".part") {
        if lower[part_start + 5..].ends_with(".rar") {
            return Some(format!("{}{:02}.rar.strm", &raw[..part_start + 5], index));
        }
    }
    for archive in ["7z", "zip", "rar"] {
        let marker = format!(".{archive}.");
        if let Some(volume_start) = lower.rfind(&marker) {
            if lower[volume_start + marker.len()..]
                .chars()
                .all(|value| value.is_ascii_digit())
            {
                return Some(format!(
                    "{}.{archive}.{index:03}.strm",
                    &raw[..volume_start]
                ));
            }
        }
    }
    if lower.ends_with(".rar") && index >= 2 {
        return Some(format!(
            "{}.r{:02}.strm",
            &raw[..raw.len().saturating_sub(4)],
            index - 2
        ));
    }
    if lower.ends_with(".zip") && index >= 2 {
        return Some(format!(
            "{}.z{:02}.strm",
            &raw[..raw.len().saturating_sub(4)],
            index - 1
        ));
    }
    None
}

fn qms_remote_archive_type(target_url: &str) -> (&'static str, &'static str) {
    match source::extension_from_target(target_url)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("cbz") | Some("cb7") => ("application/vnd.comicbook+zip", "cbz"),
        Some("rar") | Some("r00") | Some("part01.rar") => ("application/vnd.rar", "rar"),
        Some("7z") => ("application/x-7z-compressed", "7z"),
        _ => ("application/zip", "zip"),
    }
}

fn qms_archive_variant(base: &'static str, is_strm: bool) -> &'static str {
    if !is_strm {
        return base;
    }
    match base {
        "cbz" => "cbz-strm",
        "rar" => "rar-strm",
        "7z" => "7z-strm",
        _ => "zip-strm",
    }
}

async fn link_tag(
    context: &ScanContext<'_>,
    work_id: i64,
    namespace: &str,
    key: &str,
    label: &str,
    source: &str,
) -> Result<()> {
    context
        .state
        .db
        .upsert_and_link_scanner_tag(
            work_id,
            namespace,
            key,
            label,
            None,
            None,
            source,
            None,
            None,
            &context.token,
        )
        .await?;
    Ok(())
}

pub(crate) fn read_comic_info(dir: &Path) -> Result<ComicInfoRead> {
    let path = dir.join("ComicInfo.xml");
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ComicInfoRead::Missing)
        }
        Err(err) => return Err(err.into()),
    };
    if file.metadata()?.len() > MAX_COMIC_INFO_BYTES {
        return Err(AppError::Other(format!(
            "ComicInfo.xml exceeds {MAX_COMIC_INFO_BYTES} bytes"
        )));
    }
    let mut xml = String::new();
    file.take(MAX_COMIC_INFO_BYTES + 1)
        .read_to_string(&mut xml)
        .map_err(AppError::from)?;
    if xml.len() as u64 > MAX_COMIC_INFO_BYTES {
        return Err(AppError::Other(format!(
            "ComicInfo.xml exceeds {MAX_COMIC_INFO_BYTES} bytes"
        )));
    }
    quick_xml::de::from_str(&xml)
        .map(ComicInfoRead::Present)
        .map_err(|err| AppError::Other(format!("invalid ComicInfo.xml: {err}")))
}

fn count_cbz_pages(path: &Path) -> Result<i64> {
    let archive = open_zip_archive(path, "comic archive")?;
    Ok(archive.file_names().filter(|name| image_name(name)).count() as i64)
}

#[derive(Default)]
struct EpubMetadata {
    title: Option<String>,
    creator: Option<String>,
    description: Option<String>,
    language: Option<String>,
    source: Option<String>,
    series: Option<String>,
    volume: Option<String>,
    subjects: Vec<String>,
}

fn read_epub_metadata(path: &Path) -> Result<EpubMetadata> {
    let mut archive = open_zip_archive(path, "EPUB")?;
    let opf_name = epub_opf_name(&mut archive)?;
    let opf = read_zip_text(&mut archive, &opf_name)?;
    validate_epub_opf(&opf)?;
    Ok(EpubMetadata {
        title: capture_xml(&opf, &EPUB_TITLE_RE),
        creator: capture_xml(&opf, &EPUB_CREATOR_RE),
        description: capture_xml(&opf, &EPUB_DESCRIPTION_RE)
            .map(|v| html_escape::decode_html_entities(&v).to_string()),
        language: capture_xml(&opf, &EPUB_LANGUAGE_RE),
        source: capture_xml(&opf, &EPUB_IDENTIFIER_RE),
        series: capture_xml(&opf, &EPUB_COLLECTION_RE),
        volume: capture_xml(&opf, &EPUB_GROUP_POSITION_RE),
        subjects: capture_all_xml(&opf, &EPUB_SUBJECT_RE),
    })
}

#[derive(Debug, Clone)]
struct EpubManifestItem {
    id: String,
    href: String,
    media_type: String,
    properties: String,
}

fn extract_epub_cover_named(
    epub_path: &Path,
    generated_dir: &Path,
    file_prefix: &str,
) -> Result<Option<ExtractedCover>> {
    let mut archive = open_zip_archive(epub_path, "EPUB")?;
    let opf_name = epub_opf_name(&mut archive)?;
    let opf = read_zip_text(&mut archive, &opf_name)?;
    validate_epub_opf(&opf)?;
    let base = zip_parent(&opf_name);
    let items = parse_epub_manifest_items(&opf);

    let mut candidates = Vec::new();
    if let Some(cover_id) = capture_meta_name_content(&opf, "cover") {
        for item in &items {
            if item.id == cover_id || item.href == cover_id {
                candidates.push(join_zip_path(&base, &item.href));
            }
        }
    }
    for item in &items {
        if item
            .properties
            .split_whitespace()
            .any(|property| property == "cover-image")
        {
            candidates.push(join_zip_path(&base, &item.href));
        }
    }
    for item in &items {
        let lower = item.href.to_ascii_lowercase();
        if item.media_type.starts_with("image/")
            && (lower.contains("cover") || lower.contains("thumb") || lower.contains("title"))
        {
            candidates.push(join_zip_path(&base, &item.href));
        }
    }
    for item in &items {
        if item.media_type.starts_with("image/") {
            candidates.push(join_zip_path(&base, &item.href));
        }
    }

    let mut seen = BTreeSet::new();
    for candidate in candidates {
        if !seen.insert(candidate.clone()) {
            continue;
        }
        if !image_name(&candidate) {
            continue;
        }
        let entry = match archive.by_name(&candidate) {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        if entry.size() == 0 || entry.size() > MAX_EPUB_COVER_BYTES {
            continue;
        }
        let mut bytes = Vec::new();
        entry
            .take(MAX_EPUB_COVER_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.is_empty() || bytes.len() as u64 > MAX_EPUB_COVER_BYTES {
            continue;
        }
        let ext = Path::new(&candidate)
            .extension()
            .and_then(|v| v.to_str())
            .unwrap_or("jpg");
        // Content-address the extracted cover. A scanner that loses its lease
        // may still have an in-flight blocking extraction; a fixed work-id path
        // would let that stale task overwrite the current scan's cover bytes
        // even though its later database write is fenced out.
        let digest = format!("{:x}", Sha256::digest(&bytes));
        let out = generated_dir.join(format!("{file_prefix}-{digest}.{ext}"));
        let already_published = std::fs::metadata(&out)
            .ok()
            .is_some_and(|metadata| metadata.is_file() && metadata.len() == bytes.len() as u64);
        if !already_published {
            crate::atomic_file::write_sync(&out, &bytes)?;
        }
        return Ok(Some(ExtractedCover {
            path: out,
            size: i64::try_from(bytes.len()).ok(),
            source_version: digest,
        }));
    }
    Ok(None)
}

async fn cleanup_replaced_epub_cover(
    state: &AppState,
    work_id: i64,
    previous: Option<PathBuf>,
    current: Option<&PathBuf>,
) {
    let Some(previous) = previous else {
        return;
    };
    if current.is_some_and(|current| current == &previous) {
        return;
    }
    let expected_prefix = format!("epub-cover-{work_id}-");
    let legacy_prefix = format!("epub-cover-{work_id}.");
    let content_addressed_prefix = "epub-cover-v2-";
    let safe_generated_file = previous.parent() == Some(state.config.generated_dir.as_path())
        && previous
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                name.starts_with(&expected_prefix)
                    || name.starts_with(&legacy_prefix)
                    || name.starts_with(content_addressed_prefix)
            });
    if !safe_generated_file {
        tracing::warn!(path = %previous.display(), "refusing to remove an unexpected old EPUB cover path");
        return;
    }
    match state
        .db
        .asset_path_reference_count(&path_string(&previous))
        .await
    {
        Ok(0) => match tokio::fs::remove_file(&previous).await {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                tracing::warn!(path = %previous.display(), error = %err, "failed to remove replaced EPUB cover");
            }
        },
        Ok(_) => {}
        Err(err) => {
            tracing::warn!(path = %previous.display(), error = %err, "failed to verify old EPUB cover references");
        }
    }
}

fn open_zip_archive(path: &Path, label: &str) -> Result<ZipArchive<File>> {
    let file = File::open(path)?;
    crate::archive::open_media_zip(file, label)
}

fn epub_opf_name(archive: &mut ZipArchive<File>) -> Result<String> {
    if let Ok(container) = read_zip_text(archive, "META-INF/container.xml") {
        if let Some(path) = EPUB_ROOTFILE_RE
            .find(&container)
            .and_then(|tag| attr_value(tag.as_str(), "full-path"))
        {
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
    let entry = archive
        .by_name(name)
        .map_err(|e| AppError::NotFound(format!("EPUB entry {name} not found: {e}")))?;
    if entry.size() > MAX_EPUB_XML_BYTES {
        return Err(AppError::Other(format!(
            "EPUB entry {name} exceeds {MAX_EPUB_XML_BYTES} bytes"
        )));
    }
    let mut text = String::new();
    entry
        .take(MAX_EPUB_XML_BYTES + 1)
        .read_to_string(&mut text)?;
    if text.len() as u64 > MAX_EPUB_XML_BYTES {
        return Err(AppError::Other(format!(
            "EPUB entry {name} exceeds {MAX_EPUB_XML_BYTES} bytes"
        )));
    }
    Ok(text)
}

fn validate_epub_opf(opf: &str) -> Result<()> {
    let mut reader = quick_xml::Reader::from_str(opf);
    let mut saw_package = false;
    let mut saw_metadata = false;
    loop {
        match reader.read_event() {
            Ok(quick_xml::events::Event::Start(event))
            | Ok(quick_xml::events::Event::Empty(event)) => {
                let qualified = event.name();
                let name = qualified.as_ref();
                let local = name.rsplit(|byte| *byte == b':').next().unwrap_or(name);
                saw_package |= local.eq_ignore_ascii_case(b"package");
                saw_metadata |= local.eq_ignore_ascii_case(b"metadata");
            }
            Ok(quick_xml::events::Event::Eof) => break,
            Ok(_) => {}
            Err(err) => {
                return Err(AppError::Other(format!(
                    "invalid EPUB package document: {err}"
                )));
            }
        }
    }
    if !saw_package || !saw_metadata {
        return Err(AppError::Other(
            "EPUB package document is missing package metadata".to_string(),
        ));
    }
    Ok(())
}

fn parse_epub_manifest_items(opf: &str) -> Vec<EpubManifestItem> {
    EPUB_MANIFEST_ITEM_RE
        .find_iter(opf)
        .filter_map(|item| {
            let tag = item.as_str();
            Some(EpubManifestItem {
                id: attr_value(tag, "id")?,
                href: attr_value(tag, "href")?,
                media_type: attr_value(tag, "media-type").unwrap_or_default(),
                properties: attr_value(tag, "properties").unwrap_or_default(),
            })
        })
        .collect()
}

fn capture_meta_name_content(xml: &str, name: &str) -> Option<String> {
    let result = EPUB_META_RE.find_iter(xml).find_map(|tag| {
        let tag = tag.as_str();
        (attr_value(tag, "name").as_deref() == Some(name))
            .then(|| attr_value(tag, "content"))
            .flatten()
    });
    result
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

fn join_zip_path(base: &str, href: &str) -> String {
    let href = href
        .split(['?', '#'])
        .next()
        .unwrap_or(href)
        .replace('\\', "/")
        .trim_start_matches('/')
        .to_string();
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

fn zip_parent(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(parent, _)| parent.to_string())
        .unwrap_or_default()
}

fn capture_xml(xml: &str, regex: &Regex) -> Option<String> {
    regex
        .captures(xml)
        .and_then(|c| c.get(1))
        .map(|m| html_escape::decode_html_entities(m.as_str().trim()).to_string())
}

fn capture_all_xml(xml: &str, regex: &Regex) -> Vec<String> {
    regex
        .captures_iter(xml)
        .filter_map(|c| c.get(1))
        .map(|m| html_escape::decode_html_entities(m.as_str().trim()).to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

#[derive(Debug, Clone)]
pub struct ParsedTag {
    pub namespace: String,
    pub key: String,
    pub label: String,
}

pub fn parse_comic_genre_tags(genre: &str) -> Vec<ParsedTag> {
    genre
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|raw| {
            let (namespace, label) = if let Some(rest) = raw.strip_prefix("f:") {
                ("female", rest)
            } else if let Some(rest) = raw.strip_prefix("m:") {
                ("male", rest)
            } else if let Some(rest) = raw.strip_prefix("x:") {
                ("mixed", rest)
            } else if let Some((ns, rest)) = raw.split_once(':') {
                (ns, rest)
            } else {
                ("other", raw)
            };
            ParsedTag {
                namespace: namespace.to_string(),
                key: normalize_key(label),
                label: label.to_string(),
            }
        })
        .collect()
}

fn infer_audio_title(root: &Path, rj: &str) -> String {
    root.file_name()
        .and_then(|v| v.to_str())
        .filter(|v| !v.eq_ignore_ascii_case(rj))
        .map(clean_title)
        .unwrap_or_else(|| rj.to_string())
}

fn infer_audio_variant(path: &Path) -> String {
    path.parent()
        .and_then(|p| p.file_name())
        .and_then(|v| v.to_str())
        .unwrap_or("audio")
        .to_string()
}

fn common_rj_root(audio_dir: &Path, rj: &str, files: &[PathBuf]) -> PathBuf {
    let normalized_rj = rj.to_ascii_uppercase();
    let mut marker_roots = BTreeSet::new();
    for file in files {
        let Some(parent) = file.parent() else {
            continue;
        };
        let Ok(relative_parent) = parent.strip_prefix(audio_dir) else {
            continue;
        };
        let mut candidate = audio_dir.to_path_buf();
        for component in relative_parent.components() {
            candidate.push(component.as_os_str());
            if component
                .as_os_str()
                .to_string_lossy()
                .to_ascii_uppercase()
                .contains(&normalized_rj)
            {
                marker_roots.insert(candidate.clone());
                break;
            }
        }
    }

    if marker_roots.len() == 1 {
        let marker_root = marker_roots
            .into_iter()
            .next()
            .expect("one RJ marker root was confirmed");
        let marker_is_exact_rj = marker_root
            .file_name()
            .and_then(|value| value.to_str())
            .is_some_and(|value| value.eq_ignore_ascii_case(rj));
        if !marker_is_exact_rj {
            return marker_root;
        }
        return audio_product_root(marker_root, files);
    }

    let rj_root = audio_dir.join(rj);
    if rj_root.is_dir() {
        return audio_product_root(rj_root, files);
    }
    common_audio_parent(audio_dir, files).unwrap_or(rj_root)
}

fn audio_product_root(rj_root: PathBuf, files: &[PathBuf]) -> PathBuf {
    let mut immediate_parents = BTreeSet::new();
    let mut has_direct_files = false;
    for file in files {
        let Some(parent) = file.parent() else {
            continue;
        };
        let Ok(relative_parent) = parent.strip_prefix(&rj_root) else {
            continue;
        };
        let Some(component) = relative_parent.components().next() else {
            has_direct_files = true;
            continue;
        };
        immediate_parents.insert(rj_root.join(component.as_os_str()));
    }
    if !has_direct_files && immediate_parents.len() == 1 {
        if let Some(path) = immediate_parents.into_iter().next() {
            return path;
        }
    }
    if rj_root.is_dir() {
        return rj_root;
    }
    common_audio_parent(&rj_root, files).unwrap_or(rj_root)
}

fn common_audio_parent(audio_dir: &Path, files: &[PathBuf]) -> Option<PathBuf> {
    let mut common = files.first()?.parent()?.to_path_buf();
    for file in files.iter().skip(1) {
        let parent = file.parent()?;
        while !parent.starts_with(&common) {
            if !common.pop() {
                return None;
            }
        }
    }
    common.starts_with(audio_dir).then_some(common)
}

fn read_first_text_summary(files: &[PathBuf]) -> Option<String> {
    let txt = files.iter().find(|p| extension_is(p, &["txt"]))?;
    let file = File::open(txt).ok()?;
    let mut bytes = Vec::with_capacity(MAX_TEXT_SUMMARY_BYTES as usize);
    file.take(MAX_TEXT_SUMMARY_BYTES)
        .read_to_end(&mut bytes)
        .ok()?;
    let raw = String::from_utf8_lossy(&bytes);
    Some(raw.chars().take(1600).collect())
}

fn read_audio_metadata(path: &Path, fallback_title: &str, quality: &str) -> serde_json::Value {
    let tagged_file = Probe::open(path).and_then(|probe| probe.read()).ok();
    let Some(tagged_file) = tagged_file else {
        return json!({ "title": fallback_title, "quality": quality });
    };
    let properties = tagged_file.properties();
    let tag = tagged_file
        .primary_tag()
        .or_else(|| tagged_file.first_tag());
    json!({
        "title": tag.and_then(|tag| tag.title().map(|value| value.to_string())).unwrap_or_else(|| fallback_title.to_string()),
        "artist": tag.and_then(|tag| tag.artist().map(|value| value.to_string())),
        "album": tag.and_then(|tag| tag.album().map(|value| value.to_string())),
        "genre": tag.and_then(|tag| tag.genre().map(|value| value.to_string())),
        "track": tag.and_then(|tag| tag.track()),
        "quality": quality,
        "duration_seconds": properties.duration().as_secs_f64(),
        "audio_bitrate": properties.audio_bitrate(),
        "sample_rate": properties.sample_rate(),
        "channels": properties.channels(),
    })
}

fn audio_cover_candidate(files: &[PathBuf], root: &Path) -> Option<PathBuf> {
    let images = files
        .iter()
        .filter(|p| extension_is(p, &["jpg", "jpeg", "png", "webp"]))
        .collect::<Vec<_>>();
    images
        .iter()
        .copied()
        .find(|file| {
            let file_name = file
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();
            file_name.contains("cover")
                || file_name.contains("jacket")
                || file_name.contains("thumb")
                || file_name.contains("folder")
                || file_name.contains("ジャケット")
        })
        .or_else(|| images.first().copied())
        .cloned()
        .or_else(|| find_audio_cover_near_root(root))
}

fn find_audio_cover_near_root(root: &Path) -> Option<PathBuf> {
    root.ancestors().take(3).find_map(find_cover_file)
}

fn find_cover_file(dir: &Path) -> Option<PathBuf> {
    for name in [
        "thumb.jpg",
        "thumb.webp",
        "thumb.png",
        "cover.jpg",
        "cover.webp",
        "cover.png",
    ] {
        let path = dir.join(name);
        if path.exists() {
            return Some(path);
        }
    }
    None
}

fn extension_is(path: &Path, extensions: &[&str]) -> bool {
    path.extension()
        .and_then(|v| v.to_str())
        .map(|ext| {
            extensions
                .iter()
                .any(|wanted| ext.eq_ignore_ascii_case(wanted))
        })
        .unwrap_or(false)
}

pub(crate) fn audio_track_file(path: &Path) -> bool {
    extension_is(path, &["mp3", "wav", "flac", "ogg", "m4a", "aac", "opus"])
}

fn is_local_comic_archive(path: &Path) -> bool {
    extension_is(path, &["cbz", "zip"])
}

fn local_comic_archive_type(path: &Path) -> (&'static str, &'static str) {
    if extension_is(path, &["zip"]) {
        ("application/zip", "zip")
    } else {
        ("application/vnd.comicbook+zip", "cbz")
    }
}

pub(crate) fn image_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    [".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif", ".bmp"]
        .iter()
        .any(|ext| lower.ends_with(ext))
}

fn gallery_image_name(path: &Path) -> bool {
    extension_is(path, &["jpg", "jpeg", "png", "webp", "gif", "avif", "bmp"])
}

fn gallery_filename_tags(path: &Path) -> Vec<String> {
    let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
        return Vec::new();
    };
    let mut tags = BTreeSet::new();
    let mut iter = stem.char_indices().peekable();
    while let Some((_, ch)) = iter.next() {
        if ch != '#' {
            continue;
        }
        let mut value = String::new();
        while let Some((_, next)) = iter.peek().copied() {
            if next == '#' || next.is_whitespace() || is_gallery_tag_separator(next) {
                break;
            }
            value.push(next);
            iter.next();
        }
        let tag = value
            .trim_matches(|c: char| c == '#' || c.is_whitespace())
            .trim_matches(|c: char| is_gallery_tag_separator(c) || matches!(c, '.' | '_' | '-'))
            .trim()
            .to_string();
        if !tag.is_empty() {
            tags.insert(tag);
        }
    }
    tags.into_iter().collect()
}

fn is_gallery_tag_separator(ch: char) -> bool {
    matches!(
        ch,
        ',' | '，' | ';' | '；' | '、' | '[' | ']' | '(' | ')' | '（' | '）'
    )
}

fn gallery_cover_candidate(files: &[GalleryScannedPath]) -> Option<&GalleryScannedPath> {
    files
        .iter()
        .find(|file| {
            file.path
                .file_name()
                .and_then(|value| value.to_str())
                .map(|name| {
                    let lower = name.to_ascii_lowercase();
                    lower.contains("cover")
                        || lower.contains("thumb")
                        || lower.contains("jacket")
                        || lower.contains("封面")
                })
                .unwrap_or(false)
        })
        .or_else(|| files.first())
}

pub(crate) fn naturalish_key(value: &str) -> String {
    NATURAL_NUMBER_RE
        .replace_all(value, |caps: &regex::Captures| format!("{:0>12}", &caps[0]))
        .to_string()
}

pub fn normalize_key(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace('_', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn normalize_track_key(value: &str) -> String {
    let value = AUDIO_TRACK_NOISE_RE.replace_all(value, " ");
    normalize_key(&value)
}

fn clean_title(value: &str) -> String {
    value.trim().replace('\u{fffd}', "").replace("  ", " ")
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::Arc;

    use crate::assets;
    use crate::config::Config;
    use crate::db::Db;
    use crate::AppState;

    async fn make_test_state(temp: &tempfile::TempDir) -> AppState {
        let data_dir = temp.path().join("data");
        let generated_dir = temp.path().join("generated");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&generated_dir).unwrap();
        let database_url = format!("sqlite://{}", path_string(&data_dir.join("library.sqlite")));
        let db = Db::connect(&database_url).await.unwrap();
        db.migrate().await.unwrap();
        AppState {
            config: Config {
                bind: "127.0.0.1:0".to_string(),
                database_url,
                data_dir,
                cover_cache_dir: temp.path().join("cover-cache"),
                comic_cover_cache_dir: temp.path().join("cover-cache").join("comic"),
                novel_cover_cache_dir: temp.path().join("cover-cache").join("novel"),
                audio_cover_cache_dir: temp.path().join("cover-cache").join("audio"),
                gallery_cover_cache_dir: temp.path().join("cover-cache").join("gallery"),
                coser_picture_cover_cache_dir: temp
                    .path()
                    .join("cover-cache")
                    .join("coser-picture"),
                comics_dir: temp.path().join("comics"),
                novels_dir: temp.path().join("novels"),
                audio_dir: temp.path().join("audio"),
                gallery_dir: temp.path().join("gallery"),
                coser_picture_dir: temp.path().join("coser-picture"),
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
            comic_page_cache: Arc::new(assets::ComicPageCache::default()),
        }
    }

    #[test]
    fn gallery_fingerprint_keeps_one_ordered_snapshot_and_rejects_missing_files() {
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("2.jpg");
        let second = temp.path().join("10.jpg");
        std::fs::write(&first, b"first").unwrap();
        std::fs::write(&second, b"second").unwrap();
        let mut files = vec![second.clone(), first.clone()];
        files.sort_by_cached_key(|path| naturalish_key(&path_string(path)));

        let fingerprint =
            GalleryFingerprint::from_ordered_paths(temp.path().to_path_buf(), files).unwrap();
        assert_eq!(fingerprint.files.len(), 2);
        assert_eq!(fingerprint.files[0].path, first);
        assert_eq!(fingerprint.files[1].path, second);
        assert!(fingerprint.files.iter().all(|file| file.size.is_some()));
        assert!(fingerprint.files.iter().all(|file| {
            file.source_version.len() == 32
                && file
                    .source_version
                    .chars()
                    .all(|value| value.is_ascii_hexdigit())
        }));

        let missing = temp.path().join("missing.jpg");
        let error =
            GalleryFingerprint::from_ordered_paths(temp.path().to_path_buf(), vec![missing])
                .unwrap_err()
                .to_string();
        assert!(error.contains("gallery metadata changed during scan"));
    }

    #[test]
    fn gallery_legacy_and_inventory_fingerprints_are_identical() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("gallery");
        let folder = root.join("Artist").join("Set");
        std::fs::create_dir_all(&folder).unwrap();
        let first = folder.join("2.jpg");
        let second = folder.join("10.jpg");
        std::fs::write(&first, b"first").unwrap();
        std::fs::write(&second, b"second").unwrap();
        let mut files = vec![second.clone(), first.clone()];
        files.sort_by_cached_key(|path| naturalish_key(&path_string(path)));

        let legacy = GalleryFingerprint::from_ordered_paths(root.clone(), files).unwrap();
        let inventory_entries = legacy
            .files
            .iter()
            .map(|file| {
                (
                    path_string(file.path.strip_prefix(&root).unwrap()),
                    file.source_version.clone(),
                    file.size.unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let inventory = inspectors::gallery::gallery_fingerprint(inventory_entries.iter().map(
            |(relative, source_version, size)| (relative.as_str(), source_version.as_str(), *size),
        ));
        assert_eq!(legacy.value, inventory);
    }

    #[test]
    fn normalize_scan_kind_accepts_supported_kinds_and_rejects_unknown() {
        assert_eq!(normalize_scan_kind(" Gallery ").unwrap(), "gallery");
        assert_eq!(
            normalize_scan_kind("coser-picture").unwrap(),
            "coser-picture"
        );
        assert!(matches!(
            normalize_scan_kind("everything"),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn parses_comic_prefix_tags() {
        let tags = parse_comic_genre_tags("m:bbm, f:ahegao, x:group, full color");
        assert_eq!(tags[0].namespace, "male");
        assert_eq!(tags[1].namespace, "female");
        assert_eq!(tags[2].namespace, "mixed");
        assert_eq!(tags[3].namespace, "other");
        assert_eq!(tags[1].key, "ahegao");
    }

    #[test]
    fn parses_gallery_filename_hash_tags() {
        let tags = gallery_filename_tags(Path::new("artist/set #black #white #girl.png"));
        assert_eq!(
            tags,
            vec!["black".to_string(), "girl".to_string(), "white".to_string()]
        );
    }

    #[test]
    fn finds_audio_cover_near_work_root() {
        let temp = tempfile::tempdir().unwrap();
        let work_root = temp.path().join("RJ123456");
        std::fs::create_dir_all(work_root.join("tracks")).unwrap();
        let track = work_root.join("tracks").join("01.mp3");
        let cover = work_root.join("thumb.jpg");
        std::fs::write(&track, b"audio").unwrap();
        std::fs::write(&cover, b"image").unwrap();

        assert_eq!(audio_cover_candidate(&[track], &work_root), Some(cover));
    }

    #[test]
    fn derives_audio_root_from_matched_files_deterministically() {
        let temp = tempfile::tempdir().unwrap();
        let audio_dir = temp.path().join("audio");
        let rj_root = audio_dir.join("RJ123456");
        let product = rj_root.join("product");
        let bonus = rj_root.join("bonus");
        std::fs::create_dir_all(product.join("tracks")).unwrap();
        std::fs::create_dir_all(&bonus).unwrap();

        let product_files = vec![
            product.join("tracks").join("01.mp3"),
            product.join("cover.jpg"),
        ];
        assert_eq!(
            common_rj_root(&audio_dir, "RJ123456", &product_files),
            product
        );

        let split_files = vec![rj_root.join("product").join("01.mp3"), bonus.join("02.mp3")];
        assert_eq!(
            common_rj_root(&audio_dir, "RJ123456", &split_files),
            rj_root
        );

        let nested_rj_root = audio_dir.join("Creator").join("rj234567");
        let nested_product = nested_rj_root.join("Product Name");
        let nested_files = vec![
            nested_product.join("AAC").join("01.aac"),
            nested_product.join("Opus").join("01.opus"),
        ];
        assert_eq!(
            common_rj_root(&audio_dir, "RJ234567", &nested_files),
            nested_product
        );

        let titled_root = audio_dir.join("Creator").join("[RJ345678] Titled Work");
        let titled_files = vec![titled_root.join("tracks").join("01.flac")];
        assert_eq!(
            common_rj_root(&audio_dir, "RJ345678", &titled_files),
            titled_root
        );
    }

    #[tokio::test]
    async fn legacy_audio_writer_stops_after_catalog_v2_ownership_cutover() {
        let temp = tempfile::tempdir().unwrap();
        let mut state = make_test_state(&temp).await;
        state.config.inventory_scanner_enabled = true;
        std::fs::create_dir_all(&state.config.audio_dir).unwrap();
        std::fs::write(state.config.audio_dir.join("RJ123456-track.mp3"), b"audio").unwrap();
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'audio'",
        )
        .execute(state.db.pool())
        .await
        .unwrap();
        let context = ScanContext {
            state: &state,
            settings: settings::AppSettings::defaults(&state.config),
            token: "audio-catalog-owner".to_string(),
            lease_valid: Arc::new(AtomicBool::new(true)),
        };

        assert_eq!(scan_audio(&context, false).await.unwrap(), (0, 0));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM works WHERE kind = 'audio'")
                .fetch_one(state.db.pool())
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn catalog_v2_ownership_requires_the_inventory_coordinator() {
        let temp = tempfile::tempdir().unwrap();
        let mut state = make_test_state(&temp).await;
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'novel'",
        )
        .execute(state.db.pool())
        .await
        .unwrap();

        assert!(!catalog_v2_coordinator_owns_kind(&state, "novel")
            .await
            .unwrap());
        state.config.inventory_scanner_enabled = true;
        assert!(catalog_v2_coordinator_owns_kind(&state, "novel")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn legacy_gallery_writer_stops_only_while_catalog_v2_coordinator_is_active() {
        let temp = tempfile::tempdir().unwrap();
        let mut state = make_test_state(&temp).await;
        let folder = state.config.gallery_dir.join("artist");
        std::fs::create_dir_all(&folder).unwrap();
        std::fs::write(folder.join("001.jpg"), b"gallery-image").unwrap();
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'gallery'",
        )
        .execute(state.db.pool())
        .await
        .unwrap();

        state.config.inventory_scanner_enabled = true;
        let cutover = ScanContext {
            state: &state,
            settings: settings::AppSettings::defaults(&state.config),
            token: "gallery-catalog-owner".to_string(),
            lease_valid: Arc::new(AtomicBool::new(true)),
        };
        assert_eq!(scan_gallery(&cutover).await.unwrap(), 0);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM works WHERE kind = 'gallery'")
                .fetch_one(state.db.pool())
                .await
                .unwrap(),
            0
        );

        state.config.inventory_scanner_enabled = false;
        let rollback = ScanContext {
            state: &state,
            settings: settings::AppSettings::defaults(&state.config),
            token: "gallery-inventory-disabled".to_string(),
            lease_valid: Arc::new(AtomicBool::new(true)),
        };
        assert!(state
            .db
            .try_acquire_scanner_lock("library", &rollback.token, 60)
            .await
            .unwrap());
        assert_eq!(scan_gallery(&rollback).await.unwrap(), 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM works WHERE kind = 'gallery'")
                .fetch_one(state.db.pool())
                .await
                .unwrap(),
            1
        );
    }

    #[test]
    fn rejects_oversized_comic_info() {
        let temp = tempfile::tempdir().unwrap();
        let xml = format!(
            "<ComicInfo><Series>oversized</Series>{}</ComicInfo>",
            " ".repeat(MAX_COMIC_INFO_BYTES as usize)
        );
        std::fs::write(temp.path().join("ComicInfo.xml"), xml).unwrap();

        assert!(read_comic_info(temp.path()).is_err());
    }

    #[test]
    fn archive_content_sample_detects_same_size_replacement() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("book.epub");
        std::fs::write(&path, b"aaaa").unwrap();
        let first = sampled_content_key(&path, 4).unwrap();
        std::fs::write(&path, b"bbbb").unwrap();
        let second = sampled_content_key(&path, 4).unwrap();
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn traversal_stops_after_scanner_lease_loss() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("book.epub"), b"book").unwrap();
        let lease_valid = Arc::new(AtomicBool::new(false));
        let resources = ResourceGovernor::standard();
        let result = walk_matching_files(
            &resources,
            temp.path(),
            1,
            None,
            "test",
            lease_valid,
            |path| extension_is(path, &["epub"]),
        )
        .await;
        assert!(result.err().unwrap().to_string().contains("lease was lost"));
    }

    #[tokio::test]
    async fn batched_traversal_bounds_discovery_batch_and_completes() {
        let temp = tempfile::tempdir().unwrap();
        for index in 0..1_100 {
            std::fs::write(temp.path().join(format!("{index}.epub")), b"book").unwrap();
        }
        std::fs::write(temp.path().join("ignored.txt"), b"ignored").unwrap();

        let lease_valid = Arc::new(AtomicBool::new(true));
        let resources = ResourceGovernor::standard();
        let walker = open_matching_file_batches(
            &resources,
            temp.path(),
            1,
            None,
            "test-batched",
            lease_valid,
            |path| extension_is(path, &["epub"]),
        )
        .await
        .unwrap();
        assert!(walker.usable());

        let mut batches = 0;
        let mut discovered = 0;
        while let Some(batch) = walker.next_batch().await.unwrap() {
            batches += 1;
            assert!(batch.len() <= SCANNER_DISCOVERY_BATCH_SIZE);
            discovered += batch.len();
        }
        let summary = walker.finish().await.unwrap();
        assert!(batches > 1);
        assert_eq!(discovered, 1_100);
        assert!(summary.usable);
        assert!(summary.complete);
    }

    #[test]
    fn epub_package_parser_accepts_nonstandard_namespace_prefixes() {
        let opf = r#"<?xml version="1.0"?><opf:package xmlns:opf="urn:oasis:names:tc:opendocument:xmlns:container" xmlns:d="http://purl.org/dc/elements/1.1/"><opf:metadata><d:title>Namespaced Book</d:title><d:creator>Author</d:creator><d:subject>Fantasy</d:subject></opf:metadata><opf:manifest><opf:item id="chapter" href="chapter.xhtml" media-type="application/xhtml+xml"/></opf:manifest></opf:package>"#;

        validate_epub_opf(opf).unwrap();
        assert_eq!(
            capture_xml(opf, &EPUB_TITLE_RE).as_deref(),
            Some("Namespaced Book")
        );
        assert_eq!(
            capture_all_xml(opf, &EPUB_SUBJECT_RE),
            vec!["Fantasy".to_string()]
        );
        let items = parse_epub_manifest_items(opf);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].href, "chapter.xhtml");
    }

    #[tokio::test]
    async fn invalid_epub_preserves_existing_work_and_history() {
        let temp = tempfile::tempdir().unwrap();
        let state = make_test_state(&temp).await;
        std::fs::create_dir_all(&state.config.novels_dir).unwrap();
        let epub_path = state.config.novels_dir.join("book.epub");
        write_test_epub(&epub_path, "Current Book");
        let settings = settings::AppSettings::defaults(&state.config);

        let first = ScanContext {
            state: &state,
            settings: settings.clone(),
            token: "novel-first".to_string(),
            lease_valid: Arc::new(AtomicBool::new(true)),
        };
        assert!(state
            .db
            .try_acquire_scanner_lock("library", &first.token, 60)
            .await
            .unwrap());
        assert_eq!(scan_novels(&first, false).await.unwrap().0, 1);
        let work_id = state
            .db
            .library()
            .await
            .unwrap()
            .works
            .into_iter()
            .find(|work| work.kind == "novel")
            .unwrap()
            .id;
        sqlx::query(
            "INSERT INTO reading_history (work_id, progress, position, update_token) VALUES (?1, 0.5, 'chapter-2', 'history-token')",
        )
        .bind(work_id)
        .execute(state.db.pool())
        .await
        .unwrap();
        // Exercise the migration/legacy case too: preserve must not depend on a
        // non-NULL fingerprint being present.
        sqlx::query("UPDATE scanner_works SET fingerprint = NULL WHERE work_id = ?1")
            .bind(work_id)
            .execute(state.db.pool())
            .await
            .unwrap();
        state
            .db
            .release_scanner_lock("library", &first.token)
            .await
            .unwrap();

        std::fs::write(&epub_path, b"temporarily incomplete epub").unwrap();
        let second = ScanContext {
            state: &state,
            settings,
            token: "novel-second".to_string(),
            lease_valid: Arc::new(AtomicBool::new(true)),
        };
        assert!(state
            .db
            .try_acquire_scanner_lock("library", &second.token, 60)
            .await
            .unwrap());
        assert_eq!(scan_novels(&second, false).await.unwrap().0, 1);
        assert!(state
            .db
            .library()
            .await
            .unwrap()
            .works
            .iter()
            .any(|work| work.id == work_id));
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM reading_history WHERE work_id = ?1 AND position = 'chapter-2'",
            )
            .bind(work_id)
            .fetch_one(state.db.pool())
            .await
            .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn gallery_reconciliation_updates_positions_and_removes_stale_rows() {
        let temp = tempfile::tempdir().unwrap();
        let state = make_test_state(&temp).await;
        let folder = state.config.gallery_dir.join("set");
        std::fs::create_dir_all(&folder).unwrap();
        let image_2 = folder.join("2.jpg");
        let image_10 = folder.join("10.jpg");
        std::fs::write(&image_2, b"two").unwrap();
        std::fs::write(&image_10, b"ten").unwrap();

        let settings = settings::AppSettings::defaults(&state.config);
        let first = ScanContext {
            state: &state,
            settings: settings.clone(),
            token: "gallery-first".to_string(),
            lease_valid: Arc::new(AtomicBool::new(true)),
        };
        assert!(state
            .db
            .try_acquire_scanner_lock("library", &first.token, 60)
            .await
            .unwrap());
        assert_eq!(scan_gallery(&first).await.unwrap(), 1);
        let work_id = state
            .db
            .library()
            .await
            .unwrap()
            .works
            .into_iter()
            .find(|work| work.kind == "gallery")
            .unwrap()
            .id;
        let first_images = state.db.gallery_assets(work_id, 0, 100).await.unwrap();
        let first_2 = first_images
            .iter()
            .find(|asset| asset.path == path_string(&image_2))
            .unwrap();
        let first_10 = first_images
            .iter()
            .find(|asset| asset.path == path_string(&image_10))
            .unwrap();
        let first_2_id = first_2.id;
        let first_10_id = first_10.id;
        assert_eq!(first_2.position, Some(0));
        assert_eq!(first_10.position, Some(1));

        let image_1 = folder.join("1.jpg");
        std::fs::write(&image_1, b"one").unwrap();
        let second = ScanContext {
            state: &state,
            settings: settings.clone(),
            token: "gallery-second".to_string(),
            lease_valid: Arc::new(AtomicBool::new(true)),
        };
        state
            .db
            .release_scanner_lock("library", &first.token)
            .await
            .unwrap();
        assert!(state
            .db
            .try_acquire_scanner_lock("library", &second.token, 60)
            .await
            .unwrap());
        assert_eq!(scan_gallery(&second).await.unwrap(), 1);
        let images = state.db.gallery_assets(work_id, 0, 100).await.unwrap();
        assert_eq!(images.len(), 3);
        let second_2 = images
            .iter()
            .find(|asset| asset.path == path_string(&image_2))
            .unwrap();
        let second_10 = images
            .iter()
            .find(|asset| asset.path == path_string(&image_10))
            .unwrap();
        assert_eq!(second_2.id, first_2_id);
        assert_eq!(second_10.id, first_10_id);
        assert_eq!(second_2.position, Some(1));
        assert_eq!(second_10.position, Some(2));

        std::fs::remove_file(&image_2).unwrap();
        let third = ScanContext {
            state: &state,
            settings: settings.clone(),
            token: "gallery-third".to_string(),
            lease_valid: Arc::new(AtomicBool::new(true)),
        };
        state
            .db
            .release_scanner_lock("library", &second.token)
            .await
            .unwrap();
        assert!(state
            .db
            .try_acquire_scanner_lock("library", &third.token, 60)
            .await
            .unwrap());
        assert_eq!(scan_gallery(&third).await.unwrap(), 1);
        let third_images = state.db.gallery_assets(work_id, 0, 100).await.unwrap();
        assert_eq!(third_images.len(), 2);
        assert!(!third_images
            .iter()
            .any(|asset| asset.path == path_string(&image_2)));

        std::fs::remove_file(&image_1).unwrap();
        std::fs::remove_file(&image_10).unwrap();
        let fourth = ScanContext {
            state: &state,
            settings,
            token: "gallery-fourth".to_string(),
            lease_valid: Arc::new(AtomicBool::new(true)),
        };
        state
            .db
            .release_scanner_lock("library", &third.token)
            .await
            .unwrap();
        assert!(state
            .db
            .try_acquire_scanner_lock("library", &fourth.token, 60)
            .await
            .unwrap());
        assert_eq!(scan_gallery(&fourth).await.unwrap(), 0);
        assert!(!state
            .db
            .library()
            .await
            .unwrap()
            .works
            .iter()
            .any(|work| work.id == work_id));
    }

    #[tokio::test]
    async fn scans_plain_zip_comics_through_the_cbz_safety_path() {
        let temp = tempfile::tempdir().unwrap();
        let state = make_test_state(&temp).await;
        let work_dir = state.config.comics_dir.join("Author A");
        std::fs::create_dir_all(&work_dir).unwrap();
        let archive_path = work_dir.join("book.ZIP");
        write_test_zip(&archive_path, &["2.jpg", "10.jpg", "1.jpg"]);

        let settings = settings::AppSettings::defaults(&state.config);
        let context = ScanContext {
            state: &state,
            settings,
            token: "plain-zip-comic".to_string(),
            lease_valid: Arc::new(AtomicBool::new(true)),
        };
        assert!(state
            .db
            .try_acquire_scanner_lock("library", &context.token, 60)
            .await
            .unwrap());

        assert_eq!(scan_comics(&context).await.unwrap(), 1);
        let library = state.db.library().await.unwrap();
        let work = library
            .works
            .iter()
            .find(|work| work.kind == "comic")
            .expect("plain ZIP comic work");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&work.meta_json)
                .unwrap()
                .get("page_count")
                .and_then(|value| value.as_i64()),
            Some(3)
        );
        let detail = state.db.work_detail(work.id).await.unwrap();
        let archive = detail
            .assets
            .iter()
            .find(|asset| asset.role == "archive")
            .expect("plain ZIP archive asset");
        assert_eq!(archive.path, path_string(&archive_path));
        assert_eq!(archive.mime, "application/zip");
        assert_eq!(archive.variant.as_deref(), Some("zip"));
        assert_eq!(count_cbz_pages(&archive_path).unwrap(), 3);
    }

    #[tokio::test]
    async fn scans_coser_picture_zip_archives() {
        let temp = tempfile::tempdir().unwrap();
        let coser_root = temp.path().join("COS图");
        let coser_dir = coser_root.join("CoserA");
        std::fs::create_dir_all(&coser_dir).unwrap();
        let archive_path = coser_dir.join("set.zip");
        write_test_zip(&archive_path, &["2.jpg", "10.jpg", "1.jpg"]);

        let data_dir = temp.path().join("data");
        let generated_dir = temp.path().join("generated");
        let db_path = data_dir.join("library.sqlite");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&generated_dir).unwrap();
        let database_url = format!("sqlite://{}", path_string(&db_path));
        let db = Db::connect(&database_url).await.unwrap();
        db.migrate().await.unwrap();
        let state = AppState {
            config: Config {
                bind: "127.0.0.1:0".to_string(),
                database_url,
                data_dir,
                cover_cache_dir: temp.path().join("cover-cache"),
                comic_cover_cache_dir: temp.path().join("cover-cache").join("comic"),
                novel_cover_cache_dir: temp.path().join("cover-cache").join("novel"),
                audio_cover_cache_dir: temp.path().join("cover-cache").join("audio"),
                gallery_cover_cache_dir: temp.path().join("cover-cache").join("gallery"),
                coser_picture_cover_cache_dir: temp
                    .path()
                    .join("cover-cache")
                    .join("coser-picture"),
                comics_dir: temp.path().join("漫画"),
                novels_dir: temp.path().join("轻小说"),
                audio_dir: temp.path().join("音声"),
                gallery_dir: temp.path().join("图库"),
                coser_picture_dir: coser_root,
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
            comic_page_cache: Arc::new(assets::ComicPageCache::default()),
        };

        let response = scan_all(&state, false).await.unwrap();
        assert_eq!(response.coser_picture, 1);

        let library = state.db.library().await.unwrap();
        let work = library
            .works
            .iter()
            .find(|work| work.kind == "coser-picture")
            .expect("coser-picture work");
        assert_eq!(work.title, "set");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&work.meta_json)
                .unwrap()
                .get("page_count")
                .and_then(|value| value.as_i64()),
            Some(3)
        );
        assert!(work
            .tag_keys
            .as_deref()
            .unwrap_or_default()
            .contains("artist:cosera"));
        assert!(work
            .tag_keys
            .as_deref()
            .unwrap_or_default()
            .contains("coser-picture:image-set"));

        let detail = state.db.work_detail(work.id).await.unwrap();
        let archive = detail
            .assets
            .iter()
            .find(|asset| asset.role == "archive")
            .expect("archive asset");
        assert_eq!(archive.mime, "application/zip");
        assert_eq!(archive.variant.as_deref(), Some("zip"));
    }

    #[tokio::test]
    async fn unchanged_scan_does_not_requeue_search_rebuild_when_index_exists() {
        let temp = tempfile::tempdir().unwrap();
        let state = make_test_state(&temp).await;
        let search_index_dir = state.config.data_dir.join("search-index-v2");
        std::fs::create_dir_all(&search_index_dir).unwrap();
        std::fs::write(search_index_dir.join("meta.json"), b"{}").unwrap();

        let first = scan_all(&state, false).await.unwrap();
        let second = scan_all(&state, false).await.unwrap();
        assert_eq!(first.jobs_created, 0);
        assert_eq!(second.jobs_created, 0);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM jobs WHERE job_type = 'rebuild-search-index'"
            )
            .fetch_one(state.db.pool())
            .await
            .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn scan_job_kind_scope_only_processes_requested_media_kind() {
        let temp = tempfile::tempdir().unwrap();
        let mut state = make_test_state(&temp).await;
        state.config.inventory_scanner_enabled = true;
        state.config.inventory_scanner_kinds = ["novel".to_string()].into_iter().collect();
        let state = Arc::new(state);

        let novel_path = state.config.novels_dir.join("scoped-book.epub");
        std::fs::create_dir_all(&state.config.novels_dir).unwrap();
        write_test_epub(&novel_path, "Scoped novel");

        let comic_dir = state.config.comics_dir.join("Scoped Author");
        std::fs::create_dir_all(&comic_dir).unwrap();
        let comic_path = comic_dir.join("scoped-book.cbz");
        write_test_zip(&comic_path, &["001.jpg", "002.jpg"]);

        // Exercise the same dispatch used by the recovery worker instead of
        // calling scan_kind directly.  A novel-scoped payload must not walk
        // or write the comic root.
        crate::enrich::run_job(
            state.clone(),
            0,
            "scan-library",
            json!({
                "kind": "novel",
                "enqueue_enrichment": false
            }),
        )
        .await
        .unwrap();

        let counts_after_novel = sqlx::query_as::<_, (i64, i64)>(
            "SELECT\n+                 (SELECT COUNT(*) FROM works WHERE kind = 'novel'),\n+                 (SELECT COUNT(*) FROM works WHERE kind = 'comic')",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(counts_after_novel, (1, 0));
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM library_roots WHERE kind = 'novel'",
            )
            .fetch_one(state.db.pool())
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM library_roots WHERE kind = 'comic'",
            )
            .fetch_one(state.db.pool())
            .await
            .unwrap(),
            0,
            "an unselected scoped kind must not create a shadow inventory root"
        );

        // The second scoped job can then import the comic independently;
        // this also proves the first job did not leave the global scan lock
        // held or create an unusable successor state.
        crate::enrich::run_job(
            state.clone(),
            0,
            "scan-library",
            json!({
                "kind": "comic",
                "enqueue_enrichment": false
            }),
        )
        .await
        .unwrap();

        let counts_after_comic = sqlx::query_as::<_, (i64, i64)>(
            "SELECT\n+                 (SELECT COUNT(*) FROM works WHERE kind = 'novel'),\n+                 (SELECT COUNT(*) FROM works WHERE kind = 'comic')",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(counts_after_comic, (1, 1));
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM library_roots WHERE kind = 'comic'",
            )
            .fetch_one(state.db.pool())
            .await
            .unwrap(),
            0,
            "the legacy comic scan must remain independent of the novel shadow rollout"
        );
    }

    #[tokio::test]
    async fn scans_qmediasync_plain_strm_comics() {
        let temp = tempfile::tempdir().unwrap();
        let qms_root = temp.path().join("qms");
        let work_dir = qms_root.join("Remote Book");
        std::fs::create_dir_all(&work_dir).unwrap();
        std::fs::write(
            work_dir.join("book.strm"),
            "https://example.test/book.cbz\n",
        )
        .unwrap();
        std::fs::write(
            work_dir.join("ComicInfo.xml"),
            "<ComicInfo><Series>Remote Book</Series><PageCount>12</PageCount><Penciller>Artist A</Penciller></ComicInfo>",
        )
        .unwrap();
        std::fs::write(work_dir.join("thumb.jpg"), b"cover").unwrap();

        let data_dir = temp.path().join("data");
        let generated_dir = temp.path().join("generated");
        let db_path = data_dir.join("library.sqlite");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&generated_dir).unwrap();
        let database_url = format!("sqlite://{}", path_string(&db_path));
        let db = Db::connect(&database_url).await.unwrap();
        db.migrate().await.unwrap();
        let config = Config {
            bind: "127.0.0.1:0".to_string(),
            database_url,
            data_dir,
            cover_cache_dir: temp.path().join("cover-cache"),
            comic_cover_cache_dir: temp.path().join("cover-cache").join("comic"),
            novel_cover_cache_dir: temp.path().join("cover-cache").join("novel"),
            audio_cover_cache_dir: temp.path().join("cover-cache").join("audio"),
            gallery_cover_cache_dir: temp.path().join("cover-cache").join("gallery"),
            coser_picture_cover_cache_dir: temp.path().join("cover-cache").join("coser-picture"),
            comics_dir: temp.path().join("comics"),
            novels_dir: temp.path().join("novels"),
            audio_dir: temp.path().join("audio"),
            gallery_dir: temp.path().join("gallery"),
            coser_picture_dir: temp.path().join("coser-picture"),
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
        };
        let mut app_settings = settings::AppSettings::defaults(&config);
        app_settings.qmediasync.enabled = true;
        app_settings
            .qmediasync
            .strm_roots
            .push(path_string(&qms_root));
        let state = AppState {
            config,
            derivatives: crate::derivative::DerivativeCache::disabled(
                db.clone(),
                temp.path().join("derivatives"),
            ),
            db,
            http: reqwest::Client::new(),
            resources: crate::resource::ResourceGovernor::standard(),
            catalog_runtime: crate::catalog::CatalogRuntime::default(),
            search_runtime: crate::search::SearchRuntime::default(),
            comic_page_cache: Arc::new(assets::ComicPageCache::default()),
        };

        let context = ScanContext {
            state: &state,
            settings: app_settings,
            token: "qms-valid".to_string(),
            lease_valid: Arc::new(AtomicBool::new(true)),
        };
        assert!(state
            .db
            .try_acquire_scanner_lock("library", &context.token, 60)
            .await
            .unwrap());
        let count = scan_comics(&context).await.unwrap();
        assert_eq!(count, 1);

        let library = state.db.library().await.unwrap();
        let work = library
            .works
            .iter()
            .find(|work| work.kind == "comic")
            .expect("comic work");
        assert_eq!(work.title, "Remote Book");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&work.meta_json)
                .unwrap()
                .get("page_count")
                .and_then(|value| value.as_i64()),
            Some(12)
        );
        assert!(work
            .tag_keys
            .as_deref()
            .unwrap_or_default()
            .contains("artist:artist a"));

        let detail = state.db.work_detail(work.id).await.unwrap();
        let archive = detail
            .assets
            .iter()
            .find(|asset| asset.role == "archive")
            .expect("archive asset");
        assert_eq!(archive.path, "qms-strm://qms/Remote Book/book.strm");
        assert_eq!(archive.mime, "application/vnd.comicbook+zip");
        assert_eq!(archive.variant.as_deref(), Some("cbz-strm"));
        assert!(detail.assets.iter().any(|asset| asset.role == "cover"));
    }

    #[tokio::test]
    async fn invalid_qmediasync_strm_does_not_create_work() {
        let temp = tempfile::tempdir().unwrap();
        let state = make_test_state(&temp).await;
        let qms_root = temp.path().join("qms-invalid");
        std::fs::create_dir_all(&qms_root).unwrap();
        std::fs::write(qms_root.join("broken.strm"), "not a URL\n").unwrap();

        let mut app_settings = settings::AppSettings::defaults(&state.config);
        app_settings.qmediasync.enabled = true;
        app_settings
            .qmediasync
            .strm_roots
            .push(path_string(&qms_root));
        let sources = vfs::qmediasync_scan_sources(&app_settings, "comic");
        let context = ScanContext {
            state: &state,
            settings: app_settings,
            token: "qms-invalid".to_string(),
            lease_valid: Arc::new(AtomicBool::new(true)),
        };
        assert!(state
            .db
            .try_acquire_scanner_lock("library", &context.token, 60)
            .await
            .unwrap());

        assert_eq!(scan_qmediasync_comics(&context, &sources).await.unwrap(), 0);
        assert!(!state
            .db
            .library()
            .await
            .unwrap()
            .works
            .iter()
            .any(|work| work.kind == "comic"));
    }

    fn write_test_zip(path: &Path, entries: &[&str]) {
        let file = File::create(path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();
        for entry in entries {
            zip.start_file(entry, options).unwrap();
            zip.write_all(b"test-image-bytes").unwrap();
        }
        zip.finish().unwrap();
    }

    fn write_test_epub(path: &Path, title: &str) {
        let file = File::create(path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();
        zip.start_file("META-INF/container.xml", options).unwrap();
        zip.write_all(
            br#"<?xml version="1.0"?><container><rootfiles><rootfile full-path="OPS/content.opf" media-type="application/oebps-package+xml"/></rootfiles></container>"#,
        )
        .unwrap();
        zip.start_file("OPS/content.opf", options).unwrap();
        zip.write_all(
            format!(
                r#"<?xml version="1.0"?><package><metadata><dc:title>{title}</dc:title><dc:creator>Author</dc:creator></metadata><manifest><item id="chapter" href="chapter.xhtml" media-type="application/xhtml+xml"/></manifest><spine><itemref idref="chapter"/></spine></package>"#
            )
            .as_bytes(),
        )
        .unwrap();
        zip.start_file("OPS/chapter.xhtml", options).unwrap();
        zip.write_all(b"<html><body>chapter</body></html>").unwrap();
        zip.finish().unwrap();
    }
}
