//! Bounded, database-free gallery directory inspection.
//!
//! Inventory already paid the filesystem metadata cost. The inspector consumes
//! that fenced snapshot, sorts one directory naturally, and emits 256-asset
//! partial mutations through a depth-two channel. Only the final empty
//! mutation may remove stale scanner-owned facts.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use super::super::{
    clean_title, gallery_filename_tags, gallery_image_name, naturalish_key, normalize_key,
    path_string, SCANNER_PROCESSING_BUDGET_BYTES,
};
use crate::catalog_writer::{
    AssetMutation, MutationFence, MutationOwner, MutationSource, TagMutation, WorkMutation,
    WorkMutationFields, WORK_MUTATION_ASSET_LIMIT, WORK_MUTATION_TAG_LIMIT,
};
use crate::error::{AppError, Result};
use crate::resource::{ResourceClass, ResourceGovernor};

const GALLERY_INSPECTOR_VERSION: &str = "gallery-v1";
const GALLERY_ASSET_CHUNK_SIZE: usize = 256;
const GALLERY_MUTATION_CHANNEL_DEPTH: usize = 2;
pub(crate) const MAX_GALLERY_FILES_PER_WORK: usize = 20_000;
pub(crate) const MAX_GALLERY_PATH_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct GalleryInventoryAsset {
    pub relative_path: String,
    pub size: i64,
    pub source_version: String,
}

#[derive(Debug, Clone)]
pub(crate) struct GalleryInspectionRequest {
    pub root_id: i64,
    pub root_generation: i64,
    pub scan_token: String,
    pub provider: String,
    pub root: PathBuf,
    pub work_key: String,
    pub previous_work_key: Option<String>,
    pub assets: Vec<GalleryInventoryAsset>,
}

pub(crate) struct GalleryInspectionStream {
    receiver: mpsc::Receiver<WorkMutation>,
    worker: tokio::task::JoinHandle<Result<()>>,
}

impl GalleryInspectionStream {
    pub(crate) async fn next(&mut self) -> Option<WorkMutation> {
        self.receiver.recv().await
    }

    pub(crate) async fn finish(self) -> Result<()> {
        self.worker
            .await
            .map_err(|error| AppError::Other(format!("gallery inspector task failed: {error}")))?
    }
}

pub(crate) async fn inspect(
    resources: &ResourceGovernor,
    request: GalleryInspectionRequest,
) -> Result<GalleryInspectionStream> {
    validate_request(&request)?;
    let lease = resources
        .reserve_background(ResourceClass::ScanIo, SCANNER_PROCESSING_BUDGET_BYTES, 0)
        .await?;
    let (sender, receiver) = mpsc::channel(GALLERY_MUTATION_CHANNEL_DEPTH);
    let worker = tokio::task::spawn_blocking(move || {
        let _lease = lease;
        inspect_blocking(request, sender)
    });
    Ok(GalleryInspectionStream { receiver, worker })
}

#[derive(Clone)]
struct GalleryMutationTemplate {
    source: MutationSource,
    fence: MutationFence,
    fingerprint: String,
    work: WorkMutationFields,
}

#[derive(Debug)]
struct GalleryAssetSnapshot {
    relative_path: String,
    path: PathBuf,
    size: i64,
    source_version: String,
}

fn inspect_blocking(
    request: GalleryInspectionRequest,
    sender: mpsc::Sender<WorkMutation>,
) -> Result<()> {
    let mut seen_paths = BTreeSet::new();
    let mut assets = Vec::with_capacity(request.assets.len());
    for asset in request.assets {
        if !seen_paths.insert(asset.relative_path.clone()) {
            return Err(AppError::BadRequest(format!(
                "gallery inspection repeats relative path {}",
                asset.relative_path
            )));
        }
        let relative = Path::new(&asset.relative_path);
        if portable_parent(relative) != request.work_key {
            return Err(AppError::BadRequest(format!(
                "gallery asset {} does not belong to work key {}",
                asset.relative_path, request.work_key
            )));
        }
        if !gallery_image_name(relative) {
            return Err(AppError::BadRequest(format!(
                "gallery inventory path is not a supported image: {}",
                asset.relative_path
            )));
        }
        if asset.size < 0 || asset.source_version.trim().is_empty() {
            return Err(AppError::BadRequest(format!(
                "gallery inventory metadata is incomplete for {}",
                asset.relative_path
            )));
        }
        assets.push(GalleryAssetSnapshot {
            path: request.root.join(relative),
            relative_path: asset.relative_path,
            size: asset.size,
            source_version: asset.source_version,
        });
    }
    assets.sort_by_cached_key(|asset| naturalish_key(&asset.relative_path));

    let fingerprint = gallery_fingerprint(assets.iter().map(|asset| {
        (
            asset.relative_path.as_str(),
            asset.source_version.as_str(),
            asset.size,
        )
    }));
    let folder = if request.work_key == "." {
        request.root.clone()
    } else {
        request.root.join(Path::new(&request.work_key))
    };
    let title = folder
        .file_name()
        .and_then(|value| value.to_str())
        .map(clean_title)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "图库".to_string());
    let tags = gallery_tags(&request.work_key, &title, &assets)?;
    let template = GalleryMutationTemplate {
        source: MutationSource {
            kind: "gallery".to_string(),
            root_id: request.root_id,
            work_key: request.work_key.clone(),
            provider: request.provider,
        },
        fence: MutationFence {
            root_generation: request.root_generation,
            scan_token: request.scan_token,
            complete_snapshot: false,
            preserve_scanner_tags: false,
        },
        fingerprint: format!("{GALLERY_INSPECTOR_VERSION}:{fingerprint}"),
        work: WorkMutationFields {
            title,
            subtitle: None,
            category: Some("Gallery".to_string()),
            description: (request.work_key != ".").then_some(request.work_key.clone()),
            rating: None,
            source_path: path_string(&folder),
            meta: json!({
                "image_count": assets.len(),
                "root": path_string(&request.root),
                "folder": path_string(&folder),
            }),
        },
    };

    let cover_index = assets
        .iter()
        .position(|asset| gallery_cover_name(&asset.path))
        .or(Some(0));
    let mut previous_work_key = request.previous_work_key;
    let mut chunk = Vec::with_capacity(GALLERY_ASSET_CHUNK_SIZE);
    if let Some(index) = cover_index {
        let cover = &assets[index];
        chunk.push(asset_mutation(cover, "cover", None));
    }
    for (position, asset) in assets.iter().enumerate() {
        chunk.push(asset_mutation(
            asset,
            "image",
            Some(i64::try_from(position).unwrap_or(i64::MAX)),
        ));
        if chunk.len() == GALLERY_ASSET_CHUNK_SIZE {
            send_partial(
                &sender,
                &template,
                std::mem::take(&mut chunk),
                previous_work_key.take(),
            )?;
            chunk = Vec::with_capacity(GALLERY_ASSET_CHUNK_SIZE);
        }
    }
    if !chunk.is_empty() {
        send_partial(&sender, &template, chunk, previous_work_key.take())?;
    }

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
            external_ids: Vec::new(),
        })
        .map_err(|_| {
            AppError::Other("gallery mutation consumer stopped before finalize".to_string())
        })
}

fn asset_mutation(
    asset: &GalleryAssetSnapshot,
    role: &str,
    position: Option<i64>,
) -> AssetMutation {
    AssetMutation {
        path: path_string(&asset.path),
        mime: mime_guess::from_path(&asset.path)
            .first_or_octet_stream()
            .to_string(),
        role: role.to_string(),
        variant: None,
        position,
        size: Some(asset.size),
        source_version: asset.source_version.clone(),
        meta: json!({ "source": "gallery" }),
    }
}

fn send_partial(
    sender: &mpsc::Sender<WorkMutation>,
    template: &GalleryMutationTemplate,
    assets: Vec<AssetMutation>,
    previous_work_key: Option<String>,
) -> Result<()> {
    debug_assert!(assets.len() <= GALLERY_ASSET_CHUNK_SIZE);
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
            AppError::Other("gallery mutation consumer stopped during streaming".to_string())
        })
}

pub(crate) fn update_gallery_fingerprint(
    hasher: &mut Sha256,
    relative_path: &str,
    source_version: &str,
    size: i64,
) {
    for value in [relative_path.as_bytes(), source_version.as_bytes()] {
        hasher.update((value.len() as u64).to_le_bytes());
        hasher.update(value);
    }
    let size = size.to_le_bytes();
    hasher.update((size.len() as u64).to_le_bytes());
    hasher.update(size);
}

pub(crate) fn gallery_fingerprint<'a, I>(entries: I) -> String
where
    I: IntoIterator<Item = (&'a str, &'a str, i64)>,
{
    let mut hasher = Sha256::new();
    for (relative_path, source_version, size) in entries {
        update_gallery_fingerprint(&mut hasher, relative_path, source_version, size);
    }
    format!("{:x}", hasher.finalize())
}

fn gallery_tags(
    work_key: &str,
    title: &str,
    assets: &[GalleryAssetSnapshot],
) -> Result<Vec<TagMutation>> {
    let mut tags = BTreeMap::new();
    insert_tag(
        &mut tags,
        tag("gallery", "image-set", "图库", "gallery-folder"),
    );
    insert_tag(
        &mut tags,
        tag("folder", &normalize_key(title), title, "gallery-folder"),
    );
    if let Some(artist) = Path::new(work_key)
        .components()
        .next()
        .and_then(|component| match component {
            Component::Normal(value) => value.to_str(),
            _ => None,
        })
        .filter(|value| *value != "." && !value.trim().is_empty())
    {
        insert_tag(
            &mut tags,
            tag("artist", &normalize_key(artist), artist, "gallery-folder"),
        );
    }
    for asset in assets {
        for label in gallery_filename_tags(&asset.path) {
            insert_tag(
                &mut tags,
                tag(
                    "gallery",
                    &normalize_key(&label),
                    &label,
                    "gallery-filename",
                ),
            );
            if tags.len() > WORK_MUTATION_TAG_LIMIT {
                return Err(AppError::Other(format!(
                    "gallery work {work_key} exceeds the {WORK_MUTATION_TAG_LIMIT} tag limit"
                )));
            }
        }
    }
    Ok(tags.into_values().collect())
}

fn insert_tag(tags: &mut BTreeMap<(String, String), TagMutation>, tag: TagMutation) {
    tags.entry((tag.namespace.clone(), tag.key.clone()))
        .or_insert(tag);
}

fn tag(namespace: &str, key: &str, label: &str, source: &str) -> TagMutation {
    TagMutation {
        namespace: namespace.to_string(),
        key: key.to_string(),
        label: label.to_string(),
        translated_label: None,
        translated_namespace: None,
        source: source.to_string(),
        intro: None,
        links: None,
        owner: MutationOwner::Scanner,
    }
}

fn gallery_cover_name(path: &Path) -> bool {
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
}

fn portable_parent(relative: &Path) -> String {
    relative
        .parent()
        .map(|value| value.to_string_lossy().replace('\\', "/"))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| ".".to_string())
}

fn validate_request(request: &GalleryInspectionRequest) -> Result<()> {
    if request.root_id <= 0 || request.root_generation < 0 {
        return Err(AppError::BadRequest(
            "gallery inspection root fence is invalid".to_string(),
        ));
    }
    for (label, value) in [
        ("scan token", request.scan_token.as_str()),
        ("provider", request.provider.as_str()),
        ("work key", request.work_key.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(AppError::BadRequest(format!(
                "gallery inspection {label} must not be empty"
            )));
        }
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
            "gallery inspection work key must remain below its root".to_string(),
        ));
    }
    if request.assets.is_empty() {
        return Err(AppError::BadRequest(
            "gallery inspection requires at least one image".to_string(),
        ));
    }
    if request.assets.len() > MAX_GALLERY_FILES_PER_WORK {
        return Err(AppError::Other(format!(
            "gallery work {} exceeds the {} file safety limit",
            request.work_key, MAX_GALLERY_FILES_PER_WORK
        )));
    }
    let mut path_bytes = 0_usize;
    for asset in &request.assets {
        path_bytes = path_bytes.saturating_add(asset.relative_path.len());
        let relative = Path::new(&asset.relative_path);
        if asset.relative_path.trim().is_empty()
            || relative.is_absolute()
            || relative.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(AppError::BadRequest(
                "gallery inventory paths must remain below their root".to_string(),
            ));
        }
        if portable_parent(relative) != request.work_key {
            return Err(AppError::BadRequest(format!(
                "gallery asset {} does not belong to work key {}",
                asset.relative_path, request.work_key
            )));
        }
    }
    if path_bytes > MAX_GALLERY_PATH_BYTES {
        return Err(AppError::Other(format!(
            "gallery work {} exceeds the {} byte path budget",
            request.work_key, MAX_GALLERY_PATH_BYTES
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::ResourceLimits;

    #[tokio::test]
    async fn inspector_streams_twenty_thousand_images_with_one_finalize() {
        let assets = (0..MAX_GALLERY_FILES_PER_WORK)
            .map(|index| GalleryInventoryAsset {
                relative_path: format!("Artist/Set/{index:05}.jpg"),
                size: 1024 + i64::try_from(index).unwrap(),
                source_version: format!("version-{index}"),
            })
            .collect();
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let mut stream = inspect(
            &resources,
            GalleryInspectionRequest {
                root_id: 1,
                root_generation: 2,
                scan_token: "gallery-test".to_string(),
                provider: "local".to_string(),
                root: PathBuf::from("gallery"),
                work_key: "Artist/Set".to_string(),
                previous_work_key: None,
                assets,
            },
        )
        .await
        .unwrap();
        let mut image_assets = 0_usize;
        let mut cover_assets = 0_usize;
        let mut finalizers = 0_usize;
        let mut max_chunk = 0_usize;
        while let Some(mutation) = stream.next().await {
            max_chunk = max_chunk.max(mutation.assets.len());
            for asset in &mutation.assets {
                image_assets += usize::from(asset.role == "image");
                cover_assets += usize::from(asset.role == "cover");
            }
            if mutation.fence.complete_snapshot {
                finalizers += 1;
                assert!(mutation.assets.is_empty());
                assert!(!mutation.tags.is_empty());
            } else {
                assert!(mutation.tags.is_empty());
            }
        }
        stream.finish().await.unwrap();
        assert_eq!(image_assets, MAX_GALLERY_FILES_PER_WORK);
        assert_eq!(cover_assets, 1);
        assert_eq!(finalizers, 1);
        assert!(max_chunk <= GALLERY_ASSET_CHUNK_SIZE);
        let snapshot = resources.snapshot();
        assert_eq!(snapshot.pools["scan_io"].used, 0);
        assert_eq!(snapshot.pools["processing_memory"].used, 0);
    }

    #[tokio::test]
    async fn inspector_rejects_cross_directory_assets_before_acquiring_resources() {
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let error = inspect(
            &resources,
            GalleryInspectionRequest {
                root_id: 1,
                root_generation: 0,
                scan_token: "gallery-test".to_string(),
                provider: "local".to_string(),
                root: PathBuf::from("gallery"),
                work_key: "Artist/Set".to_string(),
                previous_work_key: None,
                assets: vec![GalleryInventoryAsset {
                    relative_path: "Artist/Other/1.jpg".to_string(),
                    size: 1,
                    source_version: "version".to_string(),
                }],
            },
        )
        .await
        .err()
        .expect("cross-directory asset must be rejected")
        .to_string();
        assert!(error.contains("does not belong to work key"));
        let snapshot = resources.snapshot();
        assert_eq!(snapshot.pools["scan_io"].used, 0);
        assert_eq!(snapshot.pools["processing_memory"].used, 0);
    }
}
