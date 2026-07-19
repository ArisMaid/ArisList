use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};

use lofty::prelude::*;
use lofty::probe::Probe;
use regex::Regex;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use walkdir::WalkDir;
use zip::ZipArchive;

use crate::db::ScannerAssetInput;
use crate::error::{AppError, Result};
use crate::models::ScanResponse;
use crate::settings;
use crate::vfs;
use crate::AppState;

static NATURAL_NUMBER_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\d+").unwrap());
static RJ_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"RJ\d{6,9}").unwrap());
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
    Regex::new(r"(?i)\b(mp3|wav|flac|ogg|m4a|効果音なし|seなし|bonus|特典)\b").unwrap()
});
static SCANNER_BLOCKING_LIMIT: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(4)));
const MAX_COMIC_INFO_BYTES: u64 = 1024 * 1024;
const MAX_EPUB_XML_BYTES: u64 = 2 * 1024 * 1024;
const MAX_TEXT_SUMMARY_BYTES: u64 = 64 * 1024;
const MAX_EPUB_COVER_BYTES: u64 = 12 * 1024 * 1024;
const SCANNER_ASSET_BATCH_SIZE: usize = 512;

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
}

struct ScanWalk {
    files: Vec<PathBuf>,
    usable: bool,
    complete: bool,
}

async fn scanner_blocking<T, F>(task: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    let permit = SCANNER_BLOCKING_LIMIT
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| AppError::Other("scanner blocking executor is closed".to_string()))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        task()
    })
    .await
    .map_err(|err| AppError::Other(format!("scanner blocking task failed: {err}")))?
}

async fn fingerprint_paths(paths: Vec<PathBuf>) -> Result<ScanFingerprint> {
    scanner_blocking(move || Ok(ScanFingerprint::from_paths(paths))).await
}

async fn fingerprint_archive(
    archive_path: PathBuf,
    include_comic_info: bool,
    include_cover: bool,
) -> Result<ArchiveScanState> {
    scanner_blocking(move || {
        let dir = archive_path.parent().unwrap_or_else(|| Path::new(""));
        let comic_info_path = dir.join("ComicInfo.xml");
        let cover = include_cover.then(|| find_cover_file(dir)).flatten();
        let mut paths = vec![archive_path];
        if include_comic_info && comic_info_path.is_file() {
            paths.push(comic_info_path);
        }
        if let Some(path) = cover.as_ref() {
            paths.push(path.clone());
        }
        Ok(ArchiveScanState {
            fingerprint: ScanFingerprint::from_paths(paths),
            cover,
        })
    })
    .await
}

async fn fingerprint_audio_group(
    audio_dir: PathBuf,
    rj: String,
    files: Vec<PathBuf>,
) -> Result<AudioFingerprintState> {
    scanner_blocking(move || {
        let root = common_rj_root(&audio_dir, &rj, &files);
        let cover = audio_cover_candidate(&files, &root);
        let mut fingerprint_files = files;
        if let Some(path) = cover.as_ref() {
            if !fingerprint_files.contains(path) {
                fingerprint_files.push(path.clone());
            }
        }
        Ok(AudioFingerprintState {
            fingerprint: ScanFingerprint::from_paths(fingerprint_files),
            root,
            cover,
        })
    })
    .await
}

async fn read_audio_group_metadata(files: Vec<PathBuf>) -> Result<AudioMetadataState> {
    scanner_blocking(move || {
        let summary = read_first_text_summary(&files);
        let tracks = files
            .iter()
            .map(|path| {
                if !extension_is(path, &["mp3", "wav", "flac", "ogg", "m4a"]) {
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

async fn read_comic_info_blocking(dir: PathBuf) -> Result<Option<ComicInfo>> {
    scanner_blocking(move || read_comic_info(&dir)).await
}

async fn count_cbz_pages_blocking(path: PathBuf) -> Result<i64> {
    scanner_blocking(move || count_cbz_pages(&path)).await
}

async fn read_epub_metadata_blocking(path: PathBuf) -> Result<EpubMetadata> {
    scanner_blocking(move || read_epub_metadata(&path)).await
}

async fn extract_epub_cover_blocking(
    epub_path: PathBuf,
    generated_dir: PathBuf,
    work_id: i64,
) -> Result<Option<ExtractedCover>> {
    scanner_blocking(move || {
        let Some(path) = extract_epub_cover(&epub_path, &generated_dir, work_id)? else {
            return Ok(None);
        };
        let size = std::fs::metadata(&path)
            .ok()
            .and_then(|metadata| i64::try_from(metadata.len()).ok());
        Ok(Some(ExtractedCover { path, size }))
    })
    .await
}

async fn read_qms_strm_url_blocking(path: PathBuf) -> Result<String> {
    scanner_blocking(move || vfs::read_qms_strm_url(&path)).await
}

async fn walk_matching_files<F>(
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
    scanner_blocking(move || {
        if !root.is_dir() || std::fs::read_dir(&root).is_err() {
            tracing::warn!(path = %root.display(), "{label} root is not readable");
            return Ok(ScanWalk {
                files: Vec::new(),
                usable: false,
                complete: false,
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
        Ok(ScanWalk {
            files,
            usable: true,
            complete,
        })
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
) -> Result<ScanResponse> {
    let context = ScanContext {
        state,
        settings: settings::load_settings(&state.config).await?,
        token,
        lease_valid,
    };
    let mut stats = ScanStats {
        comics: scan_comics(&context).await?,
        ..Default::default()
    };
    let (novels, novel_jobs) = scan_novels(&context, enqueue_enrichment).await?;
    stats.novels = novels;
    stats.jobs_created += novel_jobs;
    let (audio, audio_jobs) = scan_audio(&context, enqueue_enrichment).await?;
    stats.audio = audio;
    stats.jobs_created += audio_jobs;
    stats.gallery = scan_gallery(&context).await?;
    stats.coser_picture = scan_coser_pictures(&context).await?;
    let (_, created) = state
        .db
        .create_job_if_absent(
            "rebuild-search-index",
            "queued",
            json!({ "source": "scan" }),
        )
        .await?;
    stats.jobs_created += usize::from(created);
    state.db.refresh_tag_counts().await?;

    Ok(ScanResponse {
        comics: stats.comics,
        novels: stats.novels,
        audio: stats.audio,
        gallery: stats.gallery,
        coser_picture: stats.coser_picture,
        jobs_created: stats.jobs_created,
    })
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
struct ComicInfo {
    series: Option<String>,
    alternate_series: Option<String>,
    writer: Option<String>,
    penciller: Option<String>,
    genre: Option<String>,
    page_count: Option<i64>,
    language_iso: Option<String>,
    community_rating: Option<f64>,
}

async fn scan_comics(context: &ScanContext<'_>) -> Result<usize> {
    let state = context.state;
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
    let mut count = 0;
    for root in roots {
        let scope = context.prepare_scope("comic", &root).await?;
        let walk = walk_matching_files(
            &root,
            1,
            Some(2),
            "comic",
            context.lease_valid.clone(),
            |path| extension_is(path, &["cbz"]),
        )
        .await?;
        if !walk.usable || !walk.complete {
            continue;
        }
        for cbz_path in walk.files {
            context.ensure_lease_valid()?;
            let dir = cbz_path.parent().unwrap_or(root.as_path()).to_path_buf();
            let ArchiveScanState { fingerprint, cover } =
                fingerprint_archive(cbz_path.clone(), true, true).await?;
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
            let comic_info = match read_comic_info_blocking(dir.clone()).await {
                Ok(comic_info) => comic_info.unwrap_or_default(),
                Err(err) => {
                    tracing::warn!(path = %dir.display(), error = %err, "preserving comic after ComicInfo.xml read failure");
                    if preserve_existing_scanner_work(context, "comic", &source_path, &scope)
                        .await?
                    {
                        count += 1;
                    }
                    continue;
                }
            };
            let title = clean_title(
                comic_info
                    .series
                    .as_deref()
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or_else(|| {
                        dir.file_name()
                            .and_then(|v| v.to_str())
                            .unwrap_or("Untitled comic")
                    }),
            );
            let archive_page_count = match count_cbz_pages_blocking(cbz_path.clone()).await {
                Ok(page_count) => page_count,
                Err(err) => {
                    tracing::warn!(path = %cbz_path.display(), error = %err, "skipping unreadable comic archive");
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

            let work_id = state
                .db
                .upsert_scanner_work(
                    "comic",
                    &title,
                    Some(&source_path),
                    Some("Doujinshi"),
                    comic_info.alternate_series.as_deref(),
                    rating,
                    json!({
                        "page_count": page_count,
                        "writer": comic_info.writer.clone(),
                        "penciller": comic_info.penciller.clone(),
                        "language_iso": comic_info.language_iso.clone(),
                    }),
                    &context.token,
                    &fingerprint.value,
                )
                .await?;

            let size = fingerprint.size(&cbz_path);
            state
                .db
                .upsert_scanner_asset(
                    work_id,
                    &path_string(&cbz_path),
                    "application/vnd.comicbook+zip",
                    "archive",
                    Some("cbz"),
                    None,
                    size,
                    fingerprint.asset_meta(&cbz_path, json!({ "page_count": page_count })),
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
                        fingerprint.asset_meta(&cover, json!({})),
                        &context.token,
                    )
                    .await?;
            }

            if let Some(genre) = comic_info.genre.as_deref() {
                for tag in parse_comic_genre_tags(genre) {
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
            }
            for (namespace, value) in [
                ("artist", comic_info.penciller.as_deref()),
                ("group", comic_info.writer.as_deref()),
            ] {
                if let Some(value) = value.filter(|v| !v.trim().is_empty()) {
                    link_tag(
                        context,
                        work_id,
                        namespace,
                        &normalize_key(value),
                        value,
                        "comic-info",
                    )
                    .await?;
                }
            }
            if let Some(lang) = comic_info.language_iso.as_deref() {
                let label = match lang {
                    "zh" | "cn" => "chinese",
                    "ja" => "japanese",
                    "en" => "english",
                    other => other,
                };
                link_tag(context, work_id, "language", label, label, "comic-info").await?;
            }
            context
                .finish_work(work_id, &scope, &fingerprint.value)
                .await?;
            count += 1;
        }
        if walk.complete {
            state
                .db
                .finish_scanner_scope(&scope, &context.token)
                .await?;
        }
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
    let roots = context.settings.novel_roots();
    let active_scopes = roots
        .iter()
        .map(|root| context.scope("novel", root))
        .collect::<Vec<_>>();
    let mut count = 0;
    let mut jobs_created = 0;
    for root in roots {
        let scope = context.prepare_scope("novel", &root).await?;
        let walk = walk_matching_files(
            &root,
            1,
            None,
            "novel",
            context.lease_valid.clone(),
            |path| extension_is(path, &["epub"]),
        )
        .await?;
        if !walk.usable || !walk.complete {
            continue;
        }
        for epub_path in walk.files {
            context.ensure_lease_valid()?;
            let fingerprint = fingerprint_paths(vec![epub_path.clone()]).await?;
            let source_path = path_string(&epub_path);
            if let Some((work_id, previous)) = state
                .db
                .scanner_work_fingerprint("novel", &source_path)
                .await?
            {
                if previous == fingerprint.value.as_str() {
                    state
                        .db
                        .touch_scanner_work(work_id, &scope, &context.token)
                        .await?;
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
                                }),
                            )
                            .await?;
                        jobs_created += usize::from(created);
                    }
                    count += 1;
                    continue;
                }
            }
            let meta = match read_epub_metadata_blocking(epub_path.clone()).await {
                Ok(meta) => meta,
                Err(err) => {
                    tracing::warn!(path = %epub_path.display(), error = %err, "skipping unreadable EPUB");
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

            let work_id = state
                .db
                .upsert_scanner_work(
                    "novel",
                    &clean_title(&title),
                    Some(&source_path),
                    Some("Light Novel"),
                    meta.description.as_deref(),
                    None,
                    json!({
                        "creator": meta.creator.clone(),
                        "language": meta.language.clone(),
                        "series": series.clone(),
                        "volume": meta.volume.clone(),
                        "source": meta.source.clone(),
                    }),
                    &context.token,
                    &fingerprint.value,
                )
                .await?;
            let previous_epub_cover = state
                .db
                .work_asset_path(work_id, "cover", Some("epub-extracted"))
                .await?
                .map(PathBuf::from);
            let mut current_epub_cover = None;

            let size = fingerprint.size(&epub_path);
            state
                .db
                .upsert_scanner_asset(
                    work_id,
                    &path_string(&epub_path),
                    "application/epub+zip",
                    "book",
                    Some("epub"),
                    None,
                    size,
                    fingerprint.asset_meta(&epub_path, json!({})),
                    &context.token,
                )
                .await?;

            match extract_epub_cover_blocking(
                epub_path.clone(),
                state.config.generated_dir.clone(),
                work_id,
            )
            .await
            {
                Ok(Some(ExtractedCover { path: cover, size })) => {
                    current_epub_cover = Some(cover.clone());
                    let mime = mime_guess::from_path(&cover)
                        .first_or_octet_stream()
                        .to_string();
                    state
                        .db
                        .upsert_scanner_asset(
                            work_id,
                            &path_string(&cover),
                            &mime,
                            "cover",
                            Some("epub-extracted"),
                            None,
                            size,
                            json!({ "source": "epub" }),
                            &context.token,
                        )
                        .await?;
                }
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!(path = %epub_path.display(), error = %err, "preserving previous EPUB assets after cover extraction failure");
                    if preserve_existing_scanner_work(context, "novel", &source_path, &scope)
                        .await?
                    {
                        count += 1;
                        continue;
                    }
                }
            }

            if let Some(series) = series.as_deref() {
                link_tag(
                    context,
                    work_id,
                    "series",
                    &normalize_key(series),
                    series,
                    "epub",
                )
                .await?;
            }
            if let Some(author) = meta.creator.as_deref() {
                link_tag(
                    context,
                    work_id,
                    "artist",
                    &normalize_key(author),
                    author,
                    "epub",
                )
                .await?;
            }
            for subject in &meta.subjects {
                link_tag(
                    context,
                    work_id,
                    "ln",
                    &normalize_key(subject),
                    subject,
                    "epub",
                )
                .await?;
            }
            if let Some(lang) = meta.language.as_deref() {
                link_tag(
                    context,
                    work_id,
                    "language",
                    &normalize_key(lang),
                    lang,
                    "epub",
                )
                .await?;
            }
            context
                .finish_work(work_id, &scope, &fingerprint.value)
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
        if walk.complete {
            state
                .db
                .finish_scanner_scope(&scope, &context.token)
                .await?;
        }
    }
    finish_kind_scopes(context, "novel", &active_scopes).await?;
    Ok((count, jobs_created))
}

async fn scan_audio(context: &ScanContext<'_>, enqueue_enrichment: bool) -> Result<(usize, usize)> {
    let state = context.state;
    let roots = context.settings.audio_roots();
    let active_scopes = roots
        .iter()
        .map(|root| context.scope("audio", root))
        .collect::<Vec<_>>();
    let mut count = 0;
    let mut jobs_created = 0;
    for audio_dir in roots {
        let scope = context.prepare_scope("audio", &audio_dir).await?;
        let walk = walk_matching_files(
            &audio_dir,
            1,
            None,
            "audio",
            context.lease_valid.clone(),
            |path| {
                extension_is(
                    path,
                    &[
                        "mp3", "wav", "flac", "ogg", "m4a", "jpg", "jpeg", "png", "webp", "txt",
                    ],
                )
            },
        )
        .await?;
        if !walk.usable || !walk.complete {
            continue;
        }
        let mut groups: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();
        for path in walk.files {
            let full = path_string(&path);
            if let Some(m) = RJ_RE.find(&full) {
                groups.entry(m.as_str().to_string()).or_default().push(path);
            }
        }

        for (rj, mut files) in groups {
            context.ensure_lease_valid()?;
            files.sort();
            let AudioFingerprintState {
                fingerprint,
                root,
                cover,
            } = fingerprint_audio_group(audio_dir.clone(), rj.clone(), files.clone()).await?;
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
                    if enqueue_enrichment {
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
            let audio_metadata = read_audio_group_metadata(files).await?;
            let files = audio_metadata.files;
            let title = infer_audio_title(&root, &rj);
            let track_count = files
                .iter()
                .filter(|p| extension_is(p, &["mp3", "wav", "flac", "ogg", "m4a"]))
                .count();

            let work_id = state
                .db
                .upsert_scanner_work(
                    "audio",
                    &title,
                    Some(&source_path),
                    Some("Audio"),
                    audio_metadata.summary.as_deref(),
                    None,
                    json!({ "rj": rj.clone(), "track_count": track_count }),
                    &context.token,
                    &fingerprint.value,
                )
                .await?;

            state
                .db
                .upsert_scanner_external_id(
                    work_id,
                    "asmr",
                    &rj,
                    None,
                    Some(&format!("https://asmr.one/work/{rj}")),
                    &context.token,
                )
                .await?;
            state
                .db
                .upsert_scanner_external_id(
                    work_id,
                    "dlsite",
                    &rj,
                    None,
                    Some(&format!(
                        "https://www.dlsite.com/maniax/work/=/product_id/{rj}.html"
                    )),
                    &context.token,
                )
                .await?;
            link_tag(context, work_id, "audio", "asmr", "ASMR", "audio-folder").await?;
            link_tag(
                context,
                work_id,
                "source",
                &rj.to_lowercase(),
                &rj,
                "audio-folder",
            )
            .await?;

            let mut next_position = 0_i64;
            let mut track_positions = BTreeMap::new();
            let mut seen_track_names = BTreeSet::new();
            let mut scanner_assets =
                Vec::with_capacity(track_count.min(SCANNER_ASSET_BATCH_SIZE).saturating_add(2));
            for (file, track_meta) in files
                .iter()
                .zip(audio_metadata.tracks.iter())
                .filter(|(path, _)| extension_is(path, &["mp3", "wav", "flac", "ogg", "m4a"]))
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
                link_tag(
                    context,
                    work_id,
                    "audio",
                    &normalize_key(&variant),
                    &variant,
                    "audio-folder",
                )
                .await?;
            }
            context
                .finish_work(work_id, &scope, &fingerprint.value)
                .await?;
            if enqueue_enrichment {
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
        if walk.complete {
            state
                .db
                .finish_scanner_scope(&scope, &context.token)
                .await?;
        }
    }
    finish_kind_scopes(context, "audio", &active_scopes).await?;
    Ok((count, jobs_created))
}

async fn scan_gallery(context: &ScanContext<'_>) -> Result<usize> {
    let state = context.state;
    let roots = context.settings.gallery_roots();
    let active_scopes = roots
        .iter()
        .map(|root| context.scope("gallery", root))
        .collect::<Vec<_>>();
    let mut count = 0;
    for root in roots {
        let scope = context.prepare_scope("gallery", &root).await?;
        let walk = walk_matching_files(
            &root,
            1,
            None,
            "gallery",
            context.lease_valid.clone(),
            gallery_image_name,
        )
        .await?;
        if !walk.usable || !walk.complete {
            continue;
        }
        let mut groups: BTreeMap<PathBuf, Vec<PathBuf>> = BTreeMap::new();
        for path in walk.files {
            let parent = path.parent().unwrap_or(root.as_path()).to_path_buf();
            groups.entry(parent).or_default().push(path);
        }

        for (folder, mut files) in groups {
            context.ensure_lease_valid()?;
            files.sort_by_cached_key(|path| naturalish_key(&path_string(path)));
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
            let fingerprint = fingerprint_paths(files.clone()).await?;
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
                        "image_count": files.len(),
                        "root": path_string(&root),
                        "folder": path_string(&folder),
                    }),
                    &context.token,
                    &fingerprint.value,
                )
                .await?;

            let cover_path = gallery_cover_candidate(&files).cloned();
            let mut scanner_assets = Vec::with_capacity(
                files
                    .len()
                    .min(SCANNER_ASSET_BATCH_SIZE)
                    .saturating_add(usize::from(cover_path.is_some())),
            );
            if let Some(cover) = cover_path.as_ref() {
                let mime = mime_guess::from_path(cover)
                    .first_or_octet_stream()
                    .to_string();
                let size = fingerprint.size(cover);
                scanner_assets.push(ScannerAssetInput {
                    path: path_string(cover),
                    mime,
                    role: "cover".to_string(),
                    variant: None,
                    position: None,
                    size,
                    meta: fingerprint.asset_meta(cover, json!({ "source": "gallery" })),
                });
            }

            for (index, file) in files.iter().enumerate() {
                let mime = mime_guess::from_path(file)
                    .first_or_octet_stream()
                    .to_string();
                let size = fingerprint.size(file);
                scanner_assets.push(ScannerAssetInput {
                    path: path_string(file),
                    mime,
                    role: "image".to_string(),
                    variant: None,
                    position: Some(index as i64),
                    size,
                    meta: fingerprint.asset_meta(file, json!({ "source": "gallery" })),
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

            link_tag(
                context,
                work_id,
                "gallery",
                "image-set",
                "图库",
                "gallery-folder",
            )
            .await?;
            link_tag(
                context,
                work_id,
                "folder",
                &normalize_key(&title),
                &title,
                "gallery-folder",
            )
            .await?;
            if let Some(top) = folder
                .strip_prefix(&root)
                .ok()
                .and_then(|path| path.components().next())
                .and_then(|part| part.as_os_str().to_str())
                .filter(|value| !value.trim().is_empty())
            {
                link_tag(
                    context,
                    work_id,
                    "artist",
                    &normalize_key(top),
                    top,
                    "gallery-folder",
                )
                .await?;
            }
            let mut filename_tags = BTreeSet::new();
            for file in &files {
                filename_tags.extend(gallery_filename_tags(file));
            }
            for tag in filename_tags {
                link_tag(
                    context,
                    work_id,
                    "gallery",
                    &normalize_key(&tag),
                    &tag,
                    "gallery-filename",
                )
                .await?;
            }
            context
                .finish_work(work_id, &scope, &fingerprint.value)
                .await?;
            count += 1;
        }
        if walk.complete {
            state
                .db
                .finish_scanner_scope(&scope, &context.token)
                .await?;
        }
    }
    finish_kind_scopes(context, "gallery", &active_scopes).await?;
    Ok(count)
}

async fn scan_coser_pictures(context: &ScanContext<'_>) -> Result<usize> {
    let state = context.state;
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
    let mut count = 0;
    for root in roots {
        let scope = context.prepare_scope("coser-picture", &root).await?;
        let walk = walk_matching_files(
            &root,
            1,
            None,
            "CoserPicture",
            context.lease_valid.clone(),
            |path| extension_is(path, &["zip"]),
        )
        .await?;
        if !walk.usable || !walk.complete {
            continue;
        }
        for zip_path in walk.files {
            context.ensure_lease_valid()?;
            let source_path = path_string(&zip_path);
            let fingerprint = fingerprint_paths(vec![zip_path.clone()]).await?;
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
            let page_count = match count_cbz_pages_blocking(zip_path.clone()).await {
                Ok(page_count) => page_count,
                Err(err) => {
                    tracing::warn!(path = %zip_path.display(), error = %err, "skipping unreadable CoserPicture archive");
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
                if preserve_existing_scanner_work(context, "coser-picture", &source_path, &scope)
                    .await?
                {
                    count += 1;
                }
                continue;
            }
            let title = zip_path
                .file_stem()
                .and_then(|value| value.to_str())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("Untitled CoserPicture")
                .to_string();
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

            let work_id = state
                .db
                .upsert_scanner_work(
                    "coser-picture",
                    &clean_title(&title),
                    Some(&source_path),
                    Some("CoserPicture"),
                    relative.as_deref(),
                    None,
                    json!({
                        "page_count": page_count,
                        "root": path_string(&root),
                        "archive": path_string(&zip_path),
                        "coser": coser.clone(),
                    }),
                    &context.token,
                    &fingerprint.value,
                )
                .await?;

            let size = fingerprint.size(&zip_path);
            state
                .db
                .upsert_scanner_asset(
                    work_id,
                    &source_path,
                    "application/zip",
                    "archive",
                    Some("zip"),
                    None,
                    size,
                    fingerprint.asset_meta(
                        &zip_path,
                        json!({ "source": "coser-picture", "page_count": page_count }),
                    ),
                    &context.token,
                )
                .await?;

            link_tag(
                context,
                work_id,
                "coser-picture",
                "image-set",
                "CoserPicture",
                "coser-picture-zip",
            )
            .await?;
            link_tag(
                context,
                work_id,
                "folder",
                &normalize_key(&coser),
                &coser,
                "coser-picture-zip",
            )
            .await?;
            link_tag(
                context,
                work_id,
                "artist",
                &normalize_key(&coser),
                &coser,
                "coser-picture-zip",
            )
            .await?;
            context
                .finish_work(work_id, &scope, &fingerprint.value)
                .await?;
            count += 1;
        }
        if walk.complete {
            state
                .db
                .finish_scanner_scope(&scope, &context.token)
                .await?;
        }
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
    let mut count = 0;
    for source in sources {
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
            &root,
            1,
            Some(source.scan_depth.clamp(1, 64)),
            "qmediasync comic",
            context.lease_valid.clone(),
            |path| is_strm_file(path) || extension_is(path, &["cbz"]),
        )
        .await?;
        if !walk.usable || !walk.complete {
            continue;
        }
        for archive_path in walk.files {
            context.ensure_lease_valid()?;
            let is_strm = is_strm_file(&archive_path);
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
            let ArchiveScanState { fingerprint, cover } =
                fingerprint_archive(archive_path.clone(), true, true).await?;
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
                match read_qms_strm_url_blocking(archive_path.clone()).await {
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
                        continue;
                    }
                }
            } else {
                None
            };
            let comic_info = match read_comic_info_blocking(dir.clone()).await {
                Ok(comic_info) => comic_info.unwrap_or_default(),
                Err(err) => {
                    tracing::warn!(path = %dir.display(), error = %err, "preserving qmediasync comic after ComicInfo.xml read failure");
                    if preserve_existing_scanner_work(context, "comic", &archive_uri, &scope)
                        .await?
                    {
                        count += 1;
                    }
                    continue;
                }
            };
            let title = clean_title(
                comic_info
                    .series
                    .as_deref()
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or_else(|| {
                        dir.file_name()
                            .and_then(|v| v.to_str())
                            .or_else(|| archive_path.file_stem().and_then(|v| v.to_str()))
                            .unwrap_or("qmediasync comic")
                    }),
            );
            let archive_page_count = if is_strm {
                None
            } else {
                match count_cbz_pages_blocking(archive_path.clone()).await {
                    Ok(page_count) => Some(page_count),
                    Err(err) => {
                        tracing::warn!(path = %archive_path.display(), error = %err, "skipping unreadable qmediasync comic archive");
                        if preserve_existing_scanner_work(context, "comic", &archive_uri, &scope)
                            .await?
                        {
                            count += 1;
                        }
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
                    comic_info.alternate_series.as_deref(),
                    comic_info.community_rating,
                    json!({
                        "source": "qmediasync",
                        "provider": "qmediasync",
                        "mount_name": source.mount_name.clone(),
                        "strm_root": path_string(&root),
                        "page_count": page_count,
                        "writer": comic_info.writer.clone(),
                        "penciller": comic_info.penciller.clone(),
                        "language_iso": comic_info.language_iso.clone(),
                    }),
                    &context.token,
                    &fingerprint.value,
                )
                .await?;

            let size = fingerprint.size(&archive_path);
            let meta = if let Some(target_url) = target_url.as_deref() {
                vfs::qms_strm_meta_json(
                    &source.mount_name,
                    &root,
                    &archive_path,
                    &relative,
                    target_url,
                )
                .await
            } else {
                json!({ "source": "qmediasync", "provider": "qmediasync", "page_count": page_count })
            };
            state
                .db
                .upsert_scanner_asset(
                    work_id,
                    &archive_uri,
                    "application/vnd.comicbook+zip",
                    "archive",
                    Some(if is_strm { "cbz-strm" } else { "cbz" }),
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

            if let Some(genre) = comic_info.genre.as_deref() {
                for tag in parse_comic_genre_tags(genre) {
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
            }
            for (namespace, value) in [
                ("artist", comic_info.penciller.as_deref()),
                ("group", comic_info.writer.as_deref()),
            ] {
                if let Some(value) = value.filter(|v| !v.trim().is_empty()) {
                    link_tag(
                        context,
                        work_id,
                        namespace,
                        &normalize_key(value),
                        value,
                        "comic-info",
                    )
                    .await?;
                }
            }
            if let Some(lang) = comic_info.language_iso.as_deref() {
                let label = match lang {
                    "zh" | "cn" => "chinese",
                    "ja" => "japanese",
                    "en" => "english",
                    other => other,
                };
                link_tag(context, work_id, "language", label, label, "comic-info").await?;
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
            context
                .finish_work(work_id, &scope, &fingerprint.value)
                .await?;
            count += 1;
        }
        if walk.complete {
            state
                .db
                .finish_scanner_scope(&scope, &context.token)
                .await?;
        }
    }
    Ok(count)
}

async fn scan_qmediasync_coser_pictures(
    context: &ScanContext<'_>,
    sources: &[settings::MediaSourceSettings],
) -> Result<usize> {
    let state = context.state;
    let mut count = 0;
    for source in sources {
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
            &root,
            1,
            Some(source.scan_depth.clamp(1, 64)),
            "qmediasync CoserPicture",
            context.lease_valid.clone(),
            |path| is_strm_file(path) || extension_is(path, &["zip"]),
        )
        .await?;
        if !walk.usable || !walk.complete {
            continue;
        }
        for archive_path in walk.files {
            context.ensure_lease_valid()?;
            let is_strm = is_strm_file(&archive_path);
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
            let ArchiveScanState { fingerprint, cover } =
                fingerprint_archive(archive_path.clone(), false, true).await?;
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
                match read_qms_strm_url_blocking(archive_path.clone()).await {
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
                        continue;
                    }
                }
            } else {
                None
            };
            let page_count = if is_strm {
                0
            } else {
                match count_cbz_pages_blocking(archive_path.clone()).await {
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
                        continue;
                    }
                }
            };
            let title = archive_path
                .file_stem()
                .and_then(|value| value.to_str())
                .filter(|value| !value.trim().is_empty())
                .or_else(|| dir.file_name().and_then(|value| value.to_str()))
                .unwrap_or("qmediasync CoserPicture")
                .to_string();
            let coser = dir
                .file_name()
                .and_then(|value| value.to_str())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("CoserPicture")
                .to_string();
            let work_id = state
                .db
                .upsert_scanner_work(
                    "coser-picture",
                    &clean_title(&title),
                    Some(&archive_uri),
                    Some("CoserPicture"),
                    Some(&relative),
                    None,
                    json!({
                        "source": "qmediasync",
                        "provider": "qmediasync",
                        "mount_name": source.mount_name.clone(),
                        "strm_root": path_string(&root),
                        "page_count": page_count,
                        "coser": coser.clone(),
                    }),
                    &context.token,
                    &fingerprint.value,
                )
                .await?;

            let size = fingerprint.size(&archive_path);
            let meta = if let Some(target_url) = target_url.as_deref() {
                vfs::qms_strm_meta_json(
                    &source.mount_name,
                    &root,
                    &archive_path,
                    &relative,
                    target_url,
                )
                .await
            } else {
                json!({ "source": "qmediasync", "provider": "qmediasync", "page_count": page_count })
            };
            state
                .db
                .upsert_scanner_asset(
                    work_id,
                    &archive_uri,
                    "application/zip",
                    "archive",
                    Some(if is_strm { "zip-strm" } else { "zip" }),
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
            context
                .finish_work(work_id, &scope, &fingerprint.value)
                .await?;
            count += 1;
        }
        if walk.complete {
            state
                .db
                .finish_scanner_scope(&scope, &context.token)
                .await?;
        }
    }
    Ok(count)
}

fn is_strm_file(path: &Path) -> bool {
    extension_is(path, &["strm"])
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

fn read_comic_info(dir: &Path) -> Result<Option<ComicInfo>> {
    let path = dir.join("ComicInfo.xml");
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
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
        .map(Some)
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

fn extract_epub_cover(
    epub_path: &Path,
    generated_dir: &Path,
    work_id: i64,
) -> Result<Option<PathBuf>> {
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
        let out = generated_dir.join(format!("epub-cover-{work_id}-{digest}.{ext}"));
        let already_published = std::fs::metadata(&out)
            .ok()
            .is_some_and(|metadata| metadata.is_file() && metadata.len() == bytes.len() as u64);
        if !already_published {
            crate::atomic_file::write_sync(&out, &bytes)?;
        }
        return Ok(Some(out));
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
    let safe_generated_file = previous.parent() == Some(state.config.generated_dir.as_path())
        && previous
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                name.starts_with(&expected_prefix) || name.starts_with(&legacy_prefix)
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
    let rj_root = audio_dir.join(rj);
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
    files
        .first()
        .and_then(|file| file.parent())
        .map(Path::to_path_buf)
        .unwrap_or(rj_root)
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

fn gallery_cover_candidate(files: &[PathBuf]) -> Option<&PathBuf> {
    files
        .iter()
        .find(|path| {
            path.file_name()
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
            comic_page_cache: Arc::new(assets::ComicPageCache::default()),
            auth_epoch: Arc::new(tokio::sync::RwLock::new("test".to_string())),
            admin_password_persisted: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
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
        let result = walk_matching_files(temp.path(), 1, None, "test", lease_valid, |path| {
            extension_is(path, &["epub"])
        })
        .await;
        assert!(result.err().unwrap().to_string().contains("lease was lost"));
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
            comic_page_cache: Arc::new(assets::ComicPageCache::default()),
            auth_epoch: Arc::new(tokio::sync::RwLock::new("test".to_string())),
            admin_password_persisted: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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
        };
        let mut app_settings = settings::AppSettings::defaults(&config);
        app_settings.qmediasync.enabled = true;
        app_settings
            .qmediasync
            .strm_roots
            .push(path_string(&qms_root));
        let state = AppState {
            config,
            db,
            http: reqwest::Client::new(),
            comic_page_cache: Arc::new(assets::ComicPageCache::default()),
            auth_epoch: Arc::new(tokio::sync::RwLock::new("test".to_string())),
            admin_password_persisted: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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
