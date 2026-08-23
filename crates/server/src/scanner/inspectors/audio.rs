//! Bounded, database-free audio work inspection.
//!
//! The producer owns one ScanIo/resource lease while it reads metadata and
//! emits 256-asset partial mutations through a depth-two channel. A final
//! empty mutation carries tags/external IDs and is the only chunk allowed to
//! clean stale scanner-owned facts.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use serde_json::{json, Value};
use tokio::sync::mpsc;
use url::Url;

use super::super::audio_grouping::{self, AudioGroupingMode};
use super::super::{
    audio_cover_candidate, audio_track_file, clean_title, common_rj_root, infer_audio_title,
    infer_audio_variant, normalize_key, normalize_track_key, path_string, read_audio_metadata,
    read_first_text_summary, ScanFingerprint, SCANNER_PROCESSING_BUDGET_BYTES,
};
use crate::catalog_writer::{
    AssetMutation, ExternalIdMutation, MutationFence, MutationOwner, MutationSource, TagMutation,
    WorkMutation, WorkMutationFields, WORK_MUTATION_ASSET_LIMIT, WORK_MUTATION_TAG_LIMIT,
};
use crate::error::{AppError, Result};
use crate::resource::{ResourceClass, ResourceGovernor};
use crate::vfs;

const AUDIO_INSPECTOR_VERSION: &str = "audio-v1";
const AUDIO_ASSET_CHUNK_SIZE: usize = 256;
const AUDIO_MUTATION_CHANNEL_DEPTH: usize = 2;
pub(crate) const MAX_AUDIO_FILES_PER_WORK: usize = 20_000;
pub(crate) const MAX_AUDIO_PATH_BYTES: usize = 16 * 1024 * 1024;
const MAX_AUDIO_TAG_TEXT_CHARS: usize = 2048;

#[derive(Debug, Clone)]
pub(crate) struct AudioInspectionRequest {
    pub root_id: i64,
    pub root_generation: i64,
    pub scan_token: String,
    pub provider: String,
    pub qmediasync_mount_name: Option<String>,
    pub root: PathBuf,
    pub work_key: String,
    pub previous_work_key: Option<String>,
    pub relative_paths: Vec<String>,
    pub grouping: AudioGroupingMode,
}

pub(crate) struct AudioInspectionStream {
    receiver: mpsc::Receiver<WorkMutation>,
    worker: tokio::task::JoinHandle<Result<()>>,
}

impl AudioInspectionStream {
    pub(crate) async fn next(&mut self) -> Option<WorkMutation> {
        self.receiver.recv().await
    }

    pub(crate) async fn finish(self) -> Result<()> {
        self.worker
            .await
            .map_err(|error| AppError::Other(format!("audio inspector task failed: {error}")))?
    }
}

pub(crate) async fn inspect(
    resources: &ResourceGovernor,
    request: AudioInspectionRequest,
) -> Result<AudioInspectionStream> {
    validate_request(&request)?;
    let lease = resources
        .reserve_background(ResourceClass::ScanIo, SCANNER_PROCESSING_BUDGET_BYTES, 0)
        .await?;
    let (sender, receiver) = mpsc::channel(AUDIO_MUTATION_CHANNEL_DEPTH);
    let worker = tokio::task::spawn_blocking(move || {
        let _lease = lease;
        inspect_blocking(request, sender)
    });
    Ok(AudioInspectionStream { receiver, worker })
}

#[derive(Clone)]
struct AudioMutationTemplate {
    source: MutationSource,
    fence: MutationFence,
    fingerprint: String,
    work: WorkMutationFields,
    root: PathBuf,
    qmediasync_mount_name: Option<String>,
}

#[derive(Debug, Clone)]
struct RemoteStrmInfo {
    target_url: String,
    extension: String,
    image: bool,
}

fn inspect_blocking(
    request: AudioInspectionRequest,
    sender: mpsc::Sender<WorkMutation>,
) -> Result<()> {
    let AudioInspectionRequest {
        root_id,
        root_generation,
        scan_token,
        provider,
        qmediasync_mount_name,
        root,
        work_key,
        previous_work_key,
        relative_paths,
        grouping,
    } = request;
    let is_qmediasync = vfs::is_qmediasync_provider(&provider);
    if is_qmediasync && qmediasync_mount_name.is_none() {
        return Err(AppError::BadRequest(
            "qmediasync audio inspection requires a mount name".to_string(),
        ));
    }
    let mut files = Vec::with_capacity(relative_paths.len());
    let mut relative_identities = BTreeSet::new();
    let mut remote_info = BTreeMap::new();
    for relative_path in relative_paths {
        let relative = Path::new(&relative_path);
        if !relative_identities.insert(relative_path.clone()) {
            return Err(AppError::BadRequest(format!(
                "audio inspection repeats relative path {relative_path}"
            )));
        }
        let path = root.join(relative);
        if !path.is_file() {
            return Err(AppError::NotFound(format!(
                "audio inventory path is not a readable file: {relative_path}"
            )));
        }
        if is_qmediasync && extension_is(&path, &["strm"]) {
            let target_url = vfs::read_qms_strm_url(&path)?;
            let extension = remote_audio_extension(&path, &target_url);
            let image = matches!(extension.as_str(), "jpg" | "jpeg" | "png" | "webp");
            remote_info.insert(
                path.clone(),
                RemoteStrmInfo {
                    target_url,
                    extension,
                    image,
                },
            );
        }
        if supported_audio_file(&path, is_qmediasync, remote_info.get(&path)) {
            files.push(path);
        }
    }
    files.sort();
    let track_count = files
        .iter()
        .filter(|path| audio_track(path, is_qmediasync, remote_info.get(*path)))
        .count();
    if track_count == 0 {
        return Err(AppError::Other(format!(
            "audio work {} contains no supported tracks",
            work_key
        )));
    }

    // Duplicate detection is complete once the inventory paths have been
    // converted to filesystem paths.  Releasing this set before metadata and
    // chunk generation avoids retaining a second copy of up to 20,000 paths.
    drop(relative_identities);

    let rj = audio_grouping::rj_key(&work_key)
        .filter(|_| audio_grouping::is_rj_work(grouping, &work_key));
    let work_root = if let Some(rj) = rj.as_deref() {
        common_rj_root(&root, rj, &files)
    } else {
        root.join(Path::new(&work_key))
    };
    let cover = audio_cover_candidate(&files, &work_root).or_else(|| {
        files
            .iter()
            .filter(|path| {
                remote_info
                    .get(*path)
                    .is_some_and(|info| info.image && is_cover_name(path))
            })
            .min()
            .cloned()
    });
    let mut fingerprint_files = files.clone();
    if let Some(cover) = cover.as_ref() {
        if !fingerprint_files.contains(cover) {
            fingerprint_files.push(cover.clone());
        }
    }
    let fingerprint = ScanFingerprint::from_paths(fingerprint_files);
    for path in &files {
        if fingerprint.source_version(path).is_none() {
            return Err(AppError::Other(format!(
                "audio metadata changed during inspection for {}",
                path.display()
            )));
        }
    }

    let mut variants = BTreeSet::new();
    for path in files
        .iter()
        .filter(|path| audio_track(path, is_qmediasync, remote_info.get(*path)))
    {
        variants.insert(infer_audio_variant(path));
    }
    let base_tag_count = if rj.is_some() { 2 } else { 1 };
    if variants.len().saturating_add(base_tag_count) > WORK_MUTATION_TAG_LIMIT {
        return Err(AppError::Other(format!(
            "audio work {} has too many playback variants ({})",
            work_key,
            variants.len()
        )));
    }

    let source_path = qmediasync_mount_name
        .as_deref()
        .map(|mount| vfs::qms_strm_uri(mount, &work_key))
        .unwrap_or_else(|| path_string(&work_root));
    let title = if let Some(rj) = rj.as_deref() {
        infer_audio_title(&work_root, rj)
    } else {
        work_root
            .file_name()
            .and_then(|value| value.to_str())
            .map(clean_title)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| work_key.clone())
    };
    let summary = read_first_text_summary(&files);
    let template = AudioMutationTemplate {
        source: MutationSource {
            kind: "audio".to_string(),
            root_id,
            work_key: work_key.clone(),
            provider,
        },
        fence: MutationFence {
            root_generation,
            scan_token,
            complete_snapshot: false,
        },
        fingerprint: format!("{AUDIO_INSPECTOR_VERSION}:{}", fingerprint.value),
        work: WorkMutationFields {
            title,
            subtitle: None,
            category: Some("Audio".to_string()),
            description: summary,
            rating: None,
            source_path,
            meta: json!({
                "rj": rj,
                "track_count": track_count,
                "grouping": grouping.as_str(),
            }),
        },
        root: root.clone(),
        qmediasync_mount_name: qmediasync_mount_name.clone(),
    };

    let mut previous_work_key = previous_work_key;
    let mut chunk = Vec::with_capacity(AUDIO_ASSET_CHUNK_SIZE);
    let mut next_position = 0_i64;
    let mut track_positions = BTreeMap::new();
    for file in files
        .iter()
        .filter(|path| audio_track(path, is_qmediasync, remote_info.get(*path)))
    {
        let remote = remote_info.get(file);
        let extension = remote
            .map(|info| info.extension.clone())
            .unwrap_or_else(|| {
                file.extension()
                    .and_then(|value| value.to_str())
                    .unwrap_or_default()
                    .to_ascii_lowercase()
            });
        let stem = file
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_string();
        let track_key = normalize_track_key(&stem);
        let position = *track_positions.entry(track_key.clone()).or_insert_with(|| {
            let position = next_position;
            next_position += 1;
            position
        });
        let mime = mime_guess::from_ext(&extension)
            .first_or_octet_stream()
            .to_string();
        let mut meta = if let Some(remote) = remote {
            json!({
                "title": stem,
                "quality": extension,
                "remote": true,
                "target_url_hash": vfs::short_hash(&remote.target_url),
            })
        } else {
            bounded_audio_meta(read_audio_metadata(file, &stem, &extension))
        };
        if let Some(object) = meta.as_object_mut() {
            object.insert("track_key".to_string(), json!(track_key));
            object.insert("format".to_string(), json!(extension));
            object.insert(
                "preferred_playback".to_string(),
                json!(mime == "audio/mpeg"),
            );
        }
        chunk.push(AssetMutation {
            path: asset_path_for_file(&template, file),
            mime,
            role: "track".to_string(),
            variant: Some(infer_audio_variant(file)),
            position: Some(position),
            size: if remote.is_some() {
                None
            } else {
                fingerprint.size(file)
            },
            source_version: fingerprint
                .source_version(file)
                .expect("fingerprint was validated before streaming")
                .to_string(),
            meta: fingerprint.asset_meta(file, meta),
        });
        if chunk.len() == AUDIO_ASSET_CHUNK_SIZE {
            send_partial(
                &sender,
                &template,
                std::mem::take(&mut chunk),
                previous_work_key.take(),
            )?;
            chunk = Vec::with_capacity(AUDIO_ASSET_CHUNK_SIZE);
        }
    }

    if let Some(cover) = cover {
        let remote = remote_info.get(&cover);
        let extension = remote
            .map(|info| info.extension.as_str())
            .or_else(|| cover.extension().and_then(|value| value.to_str()))
            .unwrap_or("jpg")
            .to_ascii_lowercase();
        chunk.push(AssetMutation {
            path: asset_path_for_file(&template, &cover),
            mime: mime_guess::from_ext(&extension)
                .first_or_octet_stream()
                .to_string(),
            role: "cover".to_string(),
            variant: None,
            position: None,
            size: if remote.is_some() {
                None
            } else {
                fingerprint.size(&cover)
            },
            source_version: fingerprint
                .source_version(&cover)
                .expect("cover fingerprint was validated before streaming")
                .to_string(),
            meta: fingerprint.asset_meta(&cover, json!({})),
        });
    }
    if !chunk.is_empty() {
        send_partial(&sender, &template, chunk, previous_work_key.take())?;
    }

    let tags = audio_tags(rj.as_deref(), &variants);
    let external_ids = audio_external_ids(rj.as_deref());
    let mut final_fence = template.fence.clone();
    final_fence.complete_snapshot = true;
    sender
        .blocking_send(WorkMutation {
            source: template.source,
            previous_work_key,
            fence: final_fence,
            fingerprint: template.fingerprint,
            work: template.work,
            assets: Vec::new(),
            tags,
            external_ids,
        })
        .map_err(|_| AppError::Other("audio mutation consumer stopped before finalize".to_string()))
}

fn send_partial(
    sender: &mpsc::Sender<WorkMutation>,
    template: &AudioMutationTemplate,
    assets: Vec<AssetMutation>,
    previous_work_key: Option<String>,
) -> Result<()> {
    debug_assert!(assets.len() <= AUDIO_ASSET_CHUNK_SIZE);
    debug_assert!(assets.len() <= WORK_MUTATION_ASSET_LIMIT);
    sender
        .blocking_send(WorkMutation {
            source: template.source.clone(),
            previous_work_key,
            fence: template.fence.clone(),
            fingerprint: template.fingerprint.clone(),
            work: template.work.clone(),
            assets,
            tags: Vec::new(),
            external_ids: Vec::new(),
        })
        .map_err(|_| {
            AppError::Other("audio mutation consumer stopped during streaming".to_string())
        })
}

fn audio_tags(rj: Option<&str>, variants: &BTreeSet<String>) -> Vec<TagMutation> {
    let mut tags = Vec::with_capacity(variants.len().saturating_add(2));
    tags.push(tag(
        "audio",
        if rj.is_some() { "asmr" } else { "local" },
        if rj.is_some() { "ASMR" } else { "Audio" },
    ));
    if let Some(rj) = rj {
        tags.push(tag("source", &rj.to_ascii_lowercase(), rj));
    }
    for variant in variants {
        tags.push(tag("audio", &normalize_key(variant), variant));
    }
    tags
}

fn tag(namespace: &str, key: &str, label: &str) -> TagMutation {
    TagMutation {
        namespace: namespace.to_string(),
        key: key.to_string(),
        label: label.to_string(),
        translated_label: None,
        translated_namespace: None,
        source: "audio-folder".to_string(),
        intro: None,
        links: None,
        owner: MutationOwner::Scanner,
    }
}

fn audio_external_ids(rj: Option<&str>) -> Vec<ExternalIdMutation> {
    let Some(rj) = rj else {
        return Vec::new();
    };
    vec![
        ExternalIdMutation {
            source: "asmr".to_string(),
            external_id: rj.to_string(),
            token: None,
            url: Some(format!("https://asmr.one/work/{rj}")),
            owner: MutationOwner::Scanner,
        },
        ExternalIdMutation {
            source: "dlsite".to_string(),
            external_id: rj.to_string(),
            token: None,
            url: Some(format!(
                "https://www.dlsite.com/maniax/work/=/product_id/{rj}.html"
            )),
            owner: MutationOwner::Scanner,
        },
    ]
}

fn bounded_audio_meta(mut meta: Value) -> Value {
    if let Some(object) = meta.as_object_mut() {
        for key in ["title", "artist", "album", "genre"] {
            let Some(Value::String(value)) = object.get_mut(key) else {
                continue;
            };
            if value.chars().count() > MAX_AUDIO_TAG_TEXT_CHARS {
                *value = value.chars().take(MAX_AUDIO_TAG_TEXT_CHARS).collect();
            }
        }
    }
    meta
}

fn supported_audio_file(path: &Path, remote: bool, remote_info: Option<&RemoteStrmInfo>) -> bool {
    audio_track(path, remote, remote_info)
        || extension_is(path, &["jpg", "jpeg", "png", "webp", "txt"])
        || (remote && extension_is(path, &["strm"]) && remote_info.is_some())
}

fn audio_track(path: &Path, remote: bool, remote_info: Option<&RemoteStrmInfo>) -> bool {
    audio_track_file(path)
        || (remote && extension_is(path, &["strm"]) && remote_info.is_some_and(|info| !info.image))
}

fn extension_is(path: &Path, extensions: &[&str]) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .map(|value| {
            extensions
                .iter()
                .any(|extension| value.eq_ignore_ascii_case(extension))
        })
        .unwrap_or(false)
}

fn remote_audio_extension(path: &Path, target_url: &str) -> String {
    let target_extension = Url::parse(target_url).ok().and_then(|url| {
        Path::new(url.path())
            .extension()
            .and_then(|value| value.to_str())
            .map(str::to_ascii_lowercase)
    });
    let stem_extension = path
        .file_stem()
        .and_then(|value| Path::new(value).extension())
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase);
    target_extension
        .or(stem_extension)
        .unwrap_or_else(|| "remote".to_string())
}

fn is_cover_name(path: &Path) -> bool {
    let value = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    value.contains("cover")
        || value.contains("jacket")
        || value.contains("thumb")
        || value.contains("folder")
        || value.contains("ジャケット")
}

fn asset_path_for_file(template: &AudioMutationTemplate, path: &Path) -> String {
    let Some(mount) = template.qmediasync_mount_name.as_deref() else {
        return path_string(path);
    };
    if !extension_is(path, &["strm"]) {
        return path_string(path);
    }
    let relative = path
        .strip_prefix(&template.root)
        .map(|value| value.to_string_lossy().replace('\\', "/"))
        .unwrap_or_else(|_| path_string(path));
    vfs::qms_strm_uri(mount, &relative)
}

fn validate_request(request: &AudioInspectionRequest) -> Result<()> {
    if request.root_id <= 0 || request.root_generation < 0 {
        return Err(AppError::BadRequest(
            "audio inspection root fence is invalid".to_string(),
        ));
    }
    for (label, value) in [
        ("scan token", request.scan_token.as_str()),
        ("provider", request.provider.as_str()),
        ("work key", request.work_key.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(AppError::BadRequest(format!(
                "audio inspection {label} must not be empty"
            )));
        }
    }
    if vfs::is_qmediasync_provider(&request.provider)
        && request
            .qmediasync_mount_name
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
    {
        return Err(AppError::BadRequest(
            "qmediasync audio inspection requires a mount name".to_string(),
        ));
    }
    let work_key = Path::new(&request.work_key);
    if work_key.is_absolute()
        || work_key.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(AppError::BadRequest(
            "audio inspection work key must remain below its root".to_string(),
        ));
    }
    if request.relative_paths.is_empty() {
        return Err(AppError::BadRequest(
            "audio inspection requires at least one inventory path".to_string(),
        ));
    }
    if request.relative_paths.len() > MAX_AUDIO_FILES_PER_WORK {
        return Err(AppError::Other(format!(
            "audio work {} exceeds the {} file safety limit",
            request.work_key, MAX_AUDIO_FILES_PER_WORK
        )));
    }
    let mut path_bytes = 0_usize;
    for relative_path in &request.relative_paths {
        path_bytes = path_bytes.saturating_add(relative_path.len());
        let relative = Path::new(relative_path);
        if relative_path.trim().is_empty()
            || relative.is_absolute()
            || relative.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(AppError::BadRequest(
                "audio inventory paths must remain below their root".to_string(),
            ));
        }
    }
    if path_bytes > MAX_AUDIO_PATH_BYTES {
        return Err(AppError::Other(format!(
            "audio work {} exceeds the {} byte path budget",
            request.work_key, MAX_AUDIO_PATH_BYTES
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::ResourceLimits;

    #[tokio::test]
    async fn inspector_streams_more_than_one_writer_limit_and_finishes_once() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("audio");
        let work = root.join("RJ123456").join("disc");
        std::fs::create_dir_all(&work).unwrap();
        let track_count = WORK_MUTATION_ASSET_LIMIT + 33;
        let mut relative_paths = Vec::with_capacity(track_count + 1);
        for index in 0..track_count {
            let path = work.join(format!("track-{index:04}.mp3"));
            std::fs::write(&path, b"not-real-mp3").unwrap();
            relative_paths.push(
                path.strip_prefix(&root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/"),
            );
        }
        let cover = work.join("cover.jpg");
        std::fs::write(&cover, b"cover").unwrap();
        relative_paths.push(
            cover
                .strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/"),
        );
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let mut stream = inspect(
            &resources,
            AudioInspectionRequest {
                root_id: 1,
                root_generation: 2,
                scan_token: "audio-test".to_string(),
                provider: "local".to_string(),
                qmediasync_mount_name: None,
                root,
                work_key: "RJ123456".to_string(),
                previous_work_key: None,
                relative_paths,
                grouping: AudioGroupingMode::Auto,
            },
        )
        .await
        .unwrap();
        let mut chunks = 0;
        let mut assets = 0;
        let mut finalizers = 0;
        while let Some(mutation) = stream.next().await {
            chunks += 1;
            assets += mutation.assets.len();
            assert!(mutation.assets.len() <= AUDIO_ASSET_CHUNK_SIZE);
            if mutation.fence.complete_snapshot {
                finalizers += 1;
                assert!(mutation.assets.is_empty());
                assert_eq!(mutation.external_ids.len(), 2);
            } else {
                assert!(mutation.tags.is_empty());
                assert!(mutation.external_ids.is_empty());
            }
        }
        stream.finish().await.unwrap();
        assert_eq!(assets, track_count + 1);
        assert_eq!(finalizers, 1);
        assert!(chunks >= 4);
        let snapshot = resources.snapshot();
        assert_eq!(snapshot.pools["scan_io"].used, 0);
        assert_eq!(snapshot.pools["processing_memory"].used, 0);
    }

    #[tokio::test]
    async fn qmediasync_strm_audio_uses_remote_identity_without_downloading() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("qms-audio");
        let work = root.join("RJ123456").join("Disc");
        std::fs::create_dir_all(&work).unwrap();
        let track = work.join("01.mp3.strm");
        let cover = work.join("cover.jpg.strm");
        std::fs::write(&track, "https://qmediasync.example/audio/01.mp3\n").unwrap();
        std::fs::write(&cover, "https://qmediasync.example/audio/cover.jpg\n").unwrap();
        let relative_paths = vec![
            "RJ123456/Disc/01.mp3.strm".to_string(),
            "RJ123456/Disc/cover.jpg.strm".to_string(),
        ];
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let mut stream = inspect(
            &resources,
            AudioInspectionRequest {
                root_id: 1,
                root_generation: 2,
                scan_token: "qms-audio-test".to_string(),
                provider: "qmediasync:audio".to_string(),
                qmediasync_mount_name: Some("audio".to_string()),
                root,
                work_key: "RJ123456".to_string(),
                previous_work_key: None,
                relative_paths,
                grouping: AudioGroupingMode::Auto,
            },
        )
        .await
        .unwrap();
        let mut mutations = Vec::new();
        while let Some(mutation) = stream.next().await {
            mutations.push(mutation);
        }
        stream.finish().await.unwrap();
        let assets = mutations
            .iter()
            .flat_map(|mutation| mutation.assets.iter())
            .collect::<Vec<_>>();
        assert_eq!(assets.len(), 2);
        assert!(assets
            .iter()
            .all(|asset| asset.path.starts_with("qms-strm://audio/")));
        assert!(assets.iter().any(|asset| {
            asset.role == "track"
                && asset.path.ends_with("01.mp3.strm")
                && asset.mime == "audio/mpeg"
                && asset.size.is_none()
                && !asset.meta.to_string().contains("qmediasync.example")
        }));
        assert!(assets
            .iter()
            .any(|asset| asset.role == "cover" && asset.mime == "image/jpeg"));
        assert_eq!(
            mutations
                .iter()
                .filter(|mutation| mutation.fence.complete_snapshot)
                .count(),
            1
        );
        let snapshot = resources.snapshot();
        assert_eq!(snapshot.pools["scan_io"].used, 0);
        assert_eq!(snapshot.pools["processing_memory"].used, 0);
    }

    #[tokio::test]
    async fn inspector_rejects_escape_paths_before_acquiring_resources() {
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let error = inspect(
            &resources,
            AudioInspectionRequest {
                root_id: 1,
                root_generation: 0,
                scan_token: "audio-test".to_string(),
                provider: "local".to_string(),
                qmediasync_mount_name: None,
                root: PathBuf::from("audio"),
                work_key: "../outside".to_string(),
                previous_work_key: None,
                relative_paths: vec!["../outside.mp3".to_string()],
                grouping: AudioGroupingMode::Auto,
            },
        )
        .await
        .err()
        .expect("escape path must be rejected")
        .to_string();
        assert!(error.contains("remain below its root"));
        assert_eq!(resources.snapshot().pools["scan_io"].used, 0);
    }
}
