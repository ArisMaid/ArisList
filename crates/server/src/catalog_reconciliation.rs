//! Promotion evidence for the bounded Catalog v2 writers.
//!
//! Reconciliation runs an inspector without committing its mutation, then
//! compares the resulting scanner-owned facts with the current legacy
//! catalog.  The persisted result is tied to both the catalog revision and a
//! digest of the enabled Inventory roots, so promotion fails closed as soon
//! as either side changes.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use futures::TryStreamExt;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx_core::transaction::Transaction;
use sqlx_sqlite::SqliteConnection;

use crate::catalog_writer::{
    AssetMutation, ExternalIdMutation, MutationOwner, MutationSource, TagMutation, WorkMutation,
    WorkMutationFields,
};
use crate::db::Db;
use crate::error::{AppError, Result};
use crate::resource::ResourceGovernor;
use crate::scanner::audio_grouping::AudioGroupingMode;
use crate::scanner::inspectors::audio::{self, AudioInspectionRequest};
use crate::scanner::inspectors::comic::{self, ComicInspectionRequest};
use crate::scanner::inspectors::coser_picture::{self, CoserPictureInspectionRequest};
use crate::scanner::inspectors::gallery::{self, GalleryInspectionRequest, GalleryInventoryAsset};
use crate::scanner::inspectors::novel::{self, NovelInspectionRequest};
use crate::sqlite::SqliteRow;
use crate::{AppState, Row, Sqlite};

const NOVEL_KIND: &str = "novel";
const COMIC_KIND: &str = "comic";
const COSER_PICTURE_KIND: &str = "coser-picture";
const AUDIO_KIND: &str = "audio";
const GALLERY_KIND: &str = "gallery";
const NOVEL_JOB_TYPE: &str = "reconcile-catalog-novel";
const COMIC_JOB_TYPE: &str = "reconcile-catalog-comic";
const COSER_PICTURE_JOB_TYPE: &str = "reconcile-catalog-coser-picture";
const AUDIO_JOB_TYPE: &str = "reconcile-catalog-audio";
const GALLERY_JOB_TYPE: &str = "reconcile-catalog-gallery";
const NOVEL_FINGERPRINT_PREFIX: &str = "novel-v1:";
const COMIC_FINGERPRINT_PREFIX: &str = "comic-v1:";
const COSER_PICTURE_FINGERPRINT_PREFIX: &str = "coser-picture-v1:";
const AUDIO_FINGERPRINT_PREFIX: &str = "audio-v1:";
const GALLERY_FINGERPRINT_PREFIX: &str = "gallery-v1:";
const MAX_RECORDED_DIFFS: usize = 256;
const MAX_DIFF_FIELDS: usize = 24;
const MAX_WORK_KEY_CHARS: usize = 1_024;
const MAX_ERROR_CHARS: usize = 2_048;
const RECONCILIATION_WORK_KEY_PAGE_SIZE: i64 = 256;
// Gallery emits one additional cover relation in front of the image
// relations, so its 20,000-file safety limit can produce 20,001 streamed
// asset facts.  Keep this bound in the shared accumulator rather than making
// each caller re-aggregate a whole WorkMutation.
const MAX_RECONCILIATION_ASSETS_PER_WORK: usize = gallery::MAX_GALLERY_FILES_PER_WORK + 1;

pub(crate) const RECONCILE_NOVEL_JOB_TYPE: &str = NOVEL_JOB_TYPE;
pub(crate) const RECONCILE_COMIC_JOB_TYPE: &str = COMIC_JOB_TYPE;
pub(crate) const RECONCILE_COSER_PICTURE_JOB_TYPE: &str = COSER_PICTURE_JOB_TYPE;
pub(crate) const RECONCILE_AUDIO_JOB_TYPE: &str = AUDIO_JOB_TYPE;
pub(crate) const RECONCILE_GALLERY_JOB_TYPE: &str = GALLERY_JOB_TYPE;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CatalogReconciliationStatus {
    pub kind: String,
    pub status: String,
    pub current: bool,
    pub catalog_revision: i64,
    pub catalog_revision_after: i64,
    pub current_catalog_revision: i64,
    pub root_generation_sha256: Option<String>,
    pub root_generation_after_sha256: Option<String>,
    pub current_root_generation_sha256: String,
    pub root_count: i64,
    pub ready_root_count: i64,
    pub expected_works: i64,
    pub matched_works: i64,
    pub missing_works: i64,
    pub unexpected_works: i64,
    pub mismatch_works: i64,
    pub error_works: i64,
    pub diffs_recorded: i64,
    pub diffs_truncated: bool,
    pub consecutive_passes: i64,
    pub passed_since: Option<String>,
    pub started_at: Option<String>,
    pub last_checked_at: Option<String>,
    pub took_millis: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CatalogReconciliationDiff {
    pub kind: String,
    pub ordinal: i64,
    pub work_key: String,
    pub difference_kind: String,
    pub details: Value,
    pub checked_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CatalogReconciliationOverview {
    pub items: Vec<CatalogReconciliationStatus>,
    pub diffs: Vec<CatalogReconciliationDiff>,
    pub max_recorded_diffs_per_kind: usize,
}

#[derive(Debug, Clone, Serialize)]
struct RootStamp {
    id: i64,
    provider: String,
    root: String,
    generation: i64,
    completed_generation: i64,
    status: String,
    active_token: Option<String>,
    last_event_seq: Option<i64>,
    present_files: i64,
    missing_files: i64,
    audio_grouping: AudioGroupingMode,
}

#[derive(Debug, Clone)]
struct RootFact {
    stamp: RootStamp,
    path: PathBuf,
}

#[derive(Debug, Clone)]
struct RootSnapshot {
    roots: Vec<RootFact>,
    digest: String,
    ready_roots: i64,
}

impl RootSnapshot {
    fn root_count(&self) -> i64 {
        i64::try_from(self.roots.len()).unwrap_or(i64::MAX)
    }
}

#[derive(Debug, Default)]
struct ReconciliationCounts {
    expected: i64,
    matched: i64,
    missing: i64,
    unexpected: i64,
    mismatch: i64,
    errors: i64,
}

impl ReconciliationCounts {
    fn differences(&self) -> i64 {
        self.missing
            .saturating_add(self.unexpected)
            .saturating_add(self.mismatch)
            .saturating_add(self.errors)
    }
}

#[derive(Debug)]
struct PendingDiff {
    work_key: String,
    difference_kind: &'static str,
    details: Value,
}

#[derive(Debug)]
enum WorkComparison {
    Matched,
    Missing,
    Mismatch(Vec<String>),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct AssetMultisetSummary {
    count: u64,
    modular_sum: [u8; 32],
    xor: [u8; 32],
}

impl AssetMultisetSummary {
    fn insert(&mut self, canonical: &Value) -> Result<[u8; 32]> {
        let encoded = serde_json::to_vec(canonical).map_err(|error| {
            AppError::Other(format!(
                "failed to encode catalog reconciliation asset: {error}"
            ))
        })?;
        let digest: [u8; 32] = Sha256::digest(encoded).into();
        self.count = self.count.saturating_add(1);
        let mut carry = 0_u16;
        for index in (0..self.modular_sum.len()).rev() {
            let sum = u16::from(self.modular_sum[index])
                .saturating_add(u16::from(digest[index]))
                .saturating_add(carry);
            self.modular_sum[index] = sum as u8;
            carry = sum >> 8;
            self.xor[index] ^= digest[index];
        }
        Ok(digest)
    }

    fn fingerprint(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.count.to_le_bytes());
        hasher.update(self.modular_sum);
        hasher.update(self.xor);
        format!("{:x}", hasher.finalize())
    }
}

#[derive(Debug)]
struct ReconciliationCandidate {
    source: MutationSource,
    fingerprint: String,
    work: WorkMutationFields,
    assets: AssetMultisetSummary,
    cover_digest: Option<[u8; 32]>,
    tags: Vec<TagMutation>,
    external_ids: Vec<ExternalIdMutation>,
}

#[derive(Debug)]
struct ReconciliationCandidateHeader {
    source: MutationSource,
    root_generation: i64,
    scan_token: String,
    fingerprint: String,
    work: WorkMutationFields,
}

#[derive(Debug, Default)]
struct ReconciliationAccumulator {
    header: Option<ReconciliationCandidateHeader>,
    assets: AssetMultisetSummary,
    cover_digest: Option<[u8; 32]>,
    tags: Vec<TagMutation>,
    external_ids: Vec<ExternalIdMutation>,
    asset_count: usize,
    finalized: bool,
}

impl ReconciliationAccumulator {
    fn push(&mut self, mutation: WorkMutation) -> Result<()> {
        if self.finalized {
            return Err(AppError::Other(
                "catalog reconciliation inspector emitted data after final snapshot".to_string(),
            ));
        }
        let WorkMutation {
            source,
            previous_work_key: _,
            fence,
            fingerprint,
            work,
            assets,
            tags,
            external_ids,
        } = mutation;
        if let Some(header) = &self.header {
            if !same_mutation_source(&header.source, &source)
                || header.root_generation != fence.root_generation
                || header.scan_token != fence.scan_token
                || header.fingerprint != fingerprint
                || !same_work_fields(&header.work, &work)
            {
                return Err(AppError::Other(
                    "catalog reconciliation inspector changed work identity between chunks"
                        .to_string(),
                ));
            }
        } else {
            self.header = Some(ReconciliationCandidateHeader {
                source,
                root_generation: fence.root_generation,
                scan_token: fence.scan_token,
                fingerprint,
                work,
            });
        }
        let next_count = self.asset_count.checked_add(assets.len()).ok_or_else(|| {
            AppError::Other("catalog reconciliation streamed asset count overflowed".to_string())
        })?;
        if next_count > MAX_RECONCILIATION_ASSETS_PER_WORK {
            return Err(AppError::Other(format!(
                "catalog reconciliation work exceeds the {MAX_RECONCILIATION_ASSETS_PER_WORK} streamed asset safety limit"
            )));
        }
        self.asset_count = next_count;
        for asset in assets {
            let role = asset.role.clone();
            let canonical = canonical_expected_asset(&asset)?;
            let digest = self.assets.insert(&canonical)?;
            if role == "cover" {
                self.cover_digest = Some(digest);
            }
        }
        if fence.complete_snapshot {
            self.tags = tags;
            self.external_ids = external_ids;
            self.finalized = true;
        } else if !tags.is_empty() || !external_ids.is_empty() {
            return Err(AppError::Other(
                "catalog reconciliation partial mutation contains final relations".to_string(),
            ));
        }
        Ok(())
    }

    fn finish(self) -> Result<ReconciliationCandidate> {
        if !self.finalized {
            return Err(AppError::Other(
                "catalog reconciliation inspector ended without a complete snapshot marker"
                    .to_string(),
            ));
        }
        let header = self.header.ok_or_else(|| {
            AppError::Other("catalog reconciliation inspector emitted no mutation".to_string())
        })?;
        Ok(ReconciliationCandidate {
            source: header.source,
            fingerprint: header.fingerprint,
            work: header.work,
            assets: self.assets,
            cover_digest: self.cover_digest,
            tags: self.tags,
            external_ids: self.external_ids,
        })
    }
}

// Keep the old descriptive name available to local fixture helpers while the
// production type makes the bounded-streaming contract explicit.
type ReconciliationCandidateBuilder = ReconciliationAccumulator;

fn same_mutation_source(left: &MutationSource, right: &MutationSource) -> bool {
    left.kind == right.kind
        && left.root_id == right.root_id
        && left.work_key == right.work_key
        && left.provider == right.provider
}

fn same_work_fields(left: &WorkMutationFields, right: &WorkMutationFields) -> bool {
    left.title == right.title
        && left.subtitle == right.subtitle
        && left.category == right.category
        && left.description == right.description
        && left.rating == right.rating
        && left.source_path == right.source_path
        && left.meta == right.meta
}

#[derive(Debug)]
struct PreviousEvidence {
    current_pass: bool,
    consecutive_passes: i64,
    passed_since: Option<String>,
}

#[derive(Debug)]
struct ReconciliationBaseline {
    current_revision: i64,
    roots: RootSnapshot,
    previous: PreviousEvidence,
    prerequisite_error: Option<String>,
}

enum ReconciliationTarget<'a> {
    Novel { generated_dir: &'a Path },
    Comic,
    CoserPicture,
    Audio,
    Gallery,
}

impl ReconciliationTarget<'_> {
    fn kind(&self) -> &'static str {
        match self {
            Self::Novel { .. } => NOVEL_KIND,
            Self::Comic => COMIC_KIND,
            Self::CoserPicture => COSER_PICTURE_KIND,
            Self::Audio => AUDIO_KIND,
            Self::Gallery => GALLERY_KIND,
        }
    }

    fn display_name(&self) -> &'static str {
        match self {
            Self::Novel { .. } => "novel",
            Self::Comic => "comic",
            Self::CoserPicture => "CoserPicture",
            Self::Audio => "audio",
            Self::Gallery => "gallery",
        }
    }

    fn fingerprint_prefix(&self) -> &'static str {
        match self {
            Self::Novel { .. } => NOVEL_FINGERPRINT_PREFIX,
            Self::Comic => COMIC_FINGERPRINT_PREFIX,
            Self::CoserPicture => COSER_PICTURE_FINGERPRINT_PREFIX,
            Self::Audio => AUDIO_FINGERPRINT_PREFIX,
            Self::Gallery => GALLERY_FINGERPRINT_PREFIX,
        }
    }

    fn inventory_work_predicate(&self) -> &'static str {
        match self {
            Self::Novel { .. } => {
                "lower(relative_path) GLOB '*.epub' AND lower(work_key) GLOB '*.epub'"
            }
            Self::Comic => {
                "((lower(relative_path) GLOB '*.cbz' AND lower(work_key) GLOB '*.cbz') OR (lower(relative_path) GLOB '*.zip' AND lower(work_key) GLOB '*.zip'))"
            }
            Self::CoserPicture => {
                "lower(relative_path) GLOB '*.zip' AND lower(work_key) GLOB '*.zip'"
            }
            Self::Audio => {
                "work_key IS NOT NULL AND (lower(relative_path) GLOB '*.mp3' OR lower(relative_path) GLOB '*.wav' OR lower(relative_path) GLOB '*.flac' OR lower(relative_path) GLOB '*.ogg' OR lower(relative_path) GLOB '*.m4a' OR lower(relative_path) GLOB '*.aac' OR lower(relative_path) GLOB '*.opus' OR (lower(relative_path) GLOB '*.strm' AND EXISTS (SELECT 1 FROM library_roots AS qms_root WHERE qms_root.id = file_inventory.root_id AND qms_root.provider LIKE 'qmediasync%')))"
            }
            Self::Gallery => {
                "work_key IS NOT NULL AND (lower(relative_path) GLOB '*.jpg' OR lower(relative_path) GLOB '*.jpeg' OR lower(relative_path) GLOB '*.png' OR lower(relative_path) GLOB '*.webp' OR lower(relative_path) GLOB '*.gif' OR lower(relative_path) GLOB '*.avif' OR lower(relative_path) GLOB '*.bmp')"
            }
        }
    }

    fn inventory_presence_predicate(&self) -> &'static str {
        match self {
            Self::Novel { .. } => "lower(inventory.relative_path) GLOB '*.epub'",
            Self::Comic => {
                "(lower(inventory.relative_path) GLOB '*.cbz' OR lower(inventory.relative_path) GLOB '*.zip')"
            }
            Self::CoserPicture => "lower(inventory.relative_path) GLOB '*.zip'",
            Self::Audio => {
                "(lower(inventory.relative_path) GLOB '*.mp3' OR lower(inventory.relative_path) GLOB '*.wav' OR lower(inventory.relative_path) GLOB '*.flac' OR lower(inventory.relative_path) GLOB '*.ogg' OR lower(inventory.relative_path) GLOB '*.m4a' OR lower(inventory.relative_path) GLOB '*.aac' OR lower(inventory.relative_path) GLOB '*.opus' OR (lower(inventory.relative_path) GLOB '*.strm' AND EXISTS (SELECT 1 FROM library_roots AS qms_root WHERE qms_root.id = inventory.root_id AND qms_root.provider LIKE 'qmediasync%')))"
            }
            Self::Gallery => {
                "(lower(inventory.relative_path) GLOB '*.jpg' OR lower(inventory.relative_path) GLOB '*.jpeg' OR lower(inventory.relative_path) GLOB '*.png' OR lower(inventory.relative_path) GLOB '*.webp' OR lower(inventory.relative_path) GLOB '*.gif' OR lower(inventory.relative_path) GLOB '*.avif' OR lower(inventory.relative_path) GLOB '*.bmp')"
            }
        }
    }

    fn inventory_presence_source_expression(&self) -> &'static str {
        match self {
            Self::Novel { .. } | Self::Comic | Self::CoserPicture => {
                "CASE WHEN ?3 = '' THEN ('/' || inventory.relative_path) ELSE (?3 || '/' || inventory.relative_path) END"
            }
            Self::Audio | Self::Gallery => {
                "CASE WHEN inventory.work_key IS NULL OR inventory.work_key = '' OR inventory.work_key = '.' THEN ?3 WHEN EXISTS (SELECT 1 FROM library_roots AS qms_root WHERE qms_root.id = inventory.root_id AND qms_root.provider LIKE 'qmediasync%') AND ?4 = 'audio' THEN ('qms-strm://' || (SELECT CASE WHEN qms_root.provider = 'qmediasync' THEN 'qms' ELSE substr(qms_root.provider, length('qmediasync:') + 1) END FROM library_roots AS qms_root WHERE qms_root.id = inventory.root_id) || '/' || inventory.work_key) ELSE (?3 || '/' || inventory.work_key) END"
            }
        }
    }

    async fn inspect(
        &self,
        db: &Db,
        resources: &ResourceGovernor,
        root: &RootFact,
        run_token: &str,
        work_key: String,
    ) -> Result<ReconciliationCandidate> {
        let mutation = match self {
            Self::Novel { generated_dir } => {
                novel::inspect(
                    resources,
                    NovelInspectionRequest {
                        root_id: root.stamp.id,
                        root_generation: root.stamp.generation,
                        scan_token: run_token.to_string(),
                        provider: root.stamp.provider.clone(),
                        root: root.path.clone(),
                        work_key,
                        previous_work_key: None,
                        generated_dir: generated_dir.to_path_buf(),
                    },
                )
                .await
            }
            Self::Comic => {
                comic::inspect(
                    resources,
                    ComicInspectionRequest {
                        root_id: root.stamp.id,
                        root_generation: root.stamp.generation,
                        scan_token: run_token.to_string(),
                        provider: root.stamp.provider.clone(),
                        qmediasync_mount_name: crate::vfs::qmediasync_mount_name(
                            &root.stamp.provider,
                        )
                        .map(str::to_string),
                        root: root.path.clone(),
                        work_key,
                        previous_work_key: None,
                    },
                )
                .await
            }
            Self::CoserPicture => {
                coser_picture::inspect(
                    resources,
                    CoserPictureInspectionRequest {
                        root_id: root.stamp.id,
                        root_generation: root.stamp.generation,
                        scan_token: run_token.to_string(),
                        provider: root.stamp.provider.clone(),
                        qmediasync_mount_name: crate::vfs::qmediasync_mount_name(
                            &root.stamp.provider,
                        )
                        .map(str::to_string),
                        root: root.path.clone(),
                        work_key,
                        previous_work_key: None,
                    },
                )
                .await
            }
            Self::Audio => {
                let relative_paths =
                    load_audio_inventory_paths(db, root.stamp.id, root.stamp.generation, &work_key)
                        .await?;
                let stream = audio::inspect(
                    resources,
                    AudioInspectionRequest {
                        root_id: root.stamp.id,
                        root_generation: root.stamp.generation,
                        scan_token: run_token.to_string(),
                        provider: root.stamp.provider.clone(),
                        qmediasync_mount_name: crate::vfs::qmediasync_mount_name(
                            &root.stamp.provider,
                        )
                        .map(str::to_string),
                        root: root.path.clone(),
                        work_key,
                        previous_work_key: None,
                        relative_paths,
                        grouping: root.stamp.audio_grouping,
                    },
                )
                .await?;
                return collect_audio_candidate(stream).await;
            }
            Self::Gallery => {
                let assets = load_gallery_inventory_assets(
                    db,
                    root.stamp.id,
                    root.stamp.generation,
                    &work_key,
                )
                .await?;
                let stream = gallery::inspect(
                    resources,
                    GalleryInspectionRequest {
                        root_id: root.stamp.id,
                        root_generation: root.stamp.generation,
                        scan_token: run_token.to_string(),
                        provider: root.stamp.provider.clone(),
                        root: root.path.clone(),
                        work_key,
                        previous_work_key: None,
                        assets,
                    },
                )
                .await?;
                return collect_gallery_candidate(stream).await;
            }
        }?;
        let mut builder = ReconciliationCandidateBuilder::default();
        builder.push(mutation)?;
        builder.finish()
    }
}

async fn collect_audio_candidate(
    mut stream: audio::AudioInspectionStream,
) -> Result<ReconciliationCandidate> {
    let mut builder = ReconciliationAccumulator::default();
    let mut push_error = None;
    while let Some(mutation) = stream.next().await {
        if let Err(error) = builder.push(mutation) {
            push_error.get_or_insert(error);
        }
    }
    let finish_error = stream.finish().await.err();
    if let Some(error) = push_error {
        return Err(error);
    }
    if let Some(error) = finish_error {
        return Err(error);
    }
    builder.finish()
}

async fn collect_gallery_candidate(
    mut stream: gallery::GalleryInspectionStream,
) -> Result<ReconciliationCandidate> {
    let mut builder = ReconciliationAccumulator::default();
    let mut push_error = None;
    while let Some(mutation) = stream.next().await {
        if let Err(error) = builder.push(mutation) {
            push_error.get_or_insert(error);
        }
    }
    let finish_error = stream.finish().await.err();
    if let Some(error) = push_error {
        return Err(error);
    }
    if let Some(error) = finish_error {
        return Err(error);
    }
    builder.finish()
}

async fn load_audio_inventory_paths(
    db: &Db,
    root_id: i64,
    generation: i64,
    work_key: &str,
) -> Result<Vec<String>> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let result = async {
        let mut rows = sqlx::query_scalar::<_, String>(
            r#"
            SELECT relative_path
            FROM file_inventory
            WHERE root_id = ?1 AND seen_generation = ?2 AND work_key = ?3 AND status = 'present'
              AND (
                    lower(relative_path) GLOB '*.mp3'
                 OR lower(relative_path) GLOB '*.wav'
                 OR lower(relative_path) GLOB '*.flac'
                 OR lower(relative_path) GLOB '*.ogg'
                 OR lower(relative_path) GLOB '*.m4a'
                 OR lower(relative_path) GLOB '*.aac'
                 OR lower(relative_path) GLOB '*.opus'
                 OR (lower(relative_path) GLOB '*.strm' AND EXISTS (
                        SELECT 1 FROM library_roots AS qms_root
                        WHERE qms_root.id = file_inventory.root_id
                          AND qms_root.provider LIKE 'qmediasync%'
                    ))
                 OR lower(relative_path) GLOB '*.jpg'
                 OR lower(relative_path) GLOB '*.jpeg'
                 OR lower(relative_path) GLOB '*.png'
                 OR lower(relative_path) GLOB '*.webp'
                 OR lower(relative_path) GLOB '*.txt'
              )
            ORDER BY relative_path
            LIMIT ?4
            "#,
        )
        .bind(root_id)
        .bind(generation)
        .bind(work_key)
        .bind(i64::try_from(audio::MAX_AUDIO_FILES_PER_WORK + 1).unwrap_or(i64::MAX))
        .fetch(&mut *transaction);
        let mut paths = Vec::with_capacity(256);
        let mut path_bytes = 0_usize;
        while let Some(relative_path) = rows.try_next().await? {
            if paths.len() >= audio::MAX_AUDIO_FILES_PER_WORK {
                return Err(AppError::Other(format!(
                    "audio work {work_key} exceeds the {} file safety limit",
                    audio::MAX_AUDIO_FILES_PER_WORK
                )));
            }
            path_bytes = path_bytes.saturating_add(relative_path.len());
            if path_bytes > audio::MAX_AUDIO_PATH_BYTES {
                return Err(AppError::Other(format!(
                    "audio work {work_key} exceeds the {} byte path budget",
                    audio::MAX_AUDIO_PATH_BYTES
                )));
            }
            paths.push(relative_path);
        }
        Ok::<_, AppError>(paths)
    }
    .await;
    match result {
        Ok(paths) => {
            transaction.commit().await?;
            Ok(paths)
        }
        Err(error) => {
            transaction.rollback().await?;
            Err(error)
        }
    }
}

async fn load_gallery_inventory_assets(
    db: &Db,
    root_id: i64,
    generation: i64,
    work_key: &str,
) -> Result<Vec<GalleryInventoryAsset>> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let result = async {
        let mut rows = sqlx::query(
            r#"
            SELECT relative_path, size, fast_fingerprint
            FROM file_inventory
            WHERE root_id = ?1 AND seen_generation = ?2 AND work_key = ?3 AND status = 'present'
              AND (
                    lower(relative_path) GLOB '*.jpg'
                 OR lower(relative_path) GLOB '*.jpeg'
                 OR lower(relative_path) GLOB '*.png'
                 OR lower(relative_path) GLOB '*.webp'
                 OR lower(relative_path) GLOB '*.gif'
                 OR lower(relative_path) GLOB '*.avif'
                 OR lower(relative_path) GLOB '*.bmp'
              )
            ORDER BY relative_path
            LIMIT ?4
            "#,
        )
        .bind(root_id)
        .bind(generation)
        .bind(work_key)
        .bind(i64::try_from(gallery::MAX_GALLERY_FILES_PER_WORK + 1).unwrap_or(i64::MAX))
        .fetch(&mut *transaction);
        let mut assets = Vec::with_capacity(256);
        let mut path_bytes = 0_usize;
        while let Some(row) = rows.try_next().await? {
            if assets.len() >= gallery::MAX_GALLERY_FILES_PER_WORK {
                return Err(AppError::Other(format!(
                    "gallery work {work_key} exceeds the {} file safety limit",
                    gallery::MAX_GALLERY_FILES_PER_WORK
                )));
            }
            let relative_path = row.get::<String, _>("relative_path");
            path_bytes = path_bytes.saturating_add(relative_path.len());
            if path_bytes > gallery::MAX_GALLERY_PATH_BYTES {
                return Err(AppError::Other(format!(
                    "gallery work {work_key} exceeds the {} byte path budget",
                    gallery::MAX_GALLERY_PATH_BYTES
                )));
            }
            let size = row.get::<i64, _>("size");
            let source_version = row.get::<String, _>("fast_fingerprint");
            if size < 0 || source_version.trim().is_empty() {
                return Err(AppError::Other(format!(
                    "gallery inventory metadata is incomplete for {relative_path}"
                )));
            }
            assets.push(GalleryInventoryAsset {
                relative_path,
                size,
                source_version,
            });
        }
        Ok::<_, AppError>(assets)
    }
    .await;
    match result {
        Ok(assets) => {
            transaction.commit().await?;
            Ok(assets)
        }
        Err(error) => {
            transaction.rollback().await?;
            Err(error)
        }
    }
}

pub(crate) async fn overview(db: &Db) -> Result<CatalogReconciliationOverview> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let current_catalog_revision =
        catalog_revision_with_connection(transaction.connection()).await?;
    let root_snapshots = root_snapshots_with_connection(transaction.connection()).await?;
    let empty_root_digest = snapshot_from_rows(Vec::new())?.digest;
    let rows = sqlx::query(
        r#"
        SELECT kind, status, catalog_revision, catalog_revision_after,
               root_generation_sha256, root_generation_after_sha256,
               root_count, ready_root_count, expected_works, matched_works,
               missing_works, unexpected_works, mismatch_works, error_works,
               diffs_recorded, diffs_truncated, consecutive_passes,
               passed_since, started_at, last_checked_at, took_millis, last_error
        FROM catalog_reconciliation_state
        ORDER BY CASE kind
            WHEN 'novel' THEN 1
            WHEN 'comic' THEN 2
            WHEN 'coser-picture' THEN 3
            WHEN 'audio' THEN 4
            WHEN 'gallery' THEN 5
            ELSE 99 END
        "#,
    )
    .fetch_all(transaction.connection())
    .await?;
    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        let kind: String = row.get("kind");
        let current_root_digest = root_snapshots
            .get(&kind)
            .map(|roots| roots.digest.as_str())
            .unwrap_or(empty_root_digest.as_str());
        items.push(status_from_row(
            &row,
            current_catalog_revision,
            current_root_digest,
        ));
    }

    let diff_rows = sqlx::query(
        r#"
        SELECT kind, ordinal, work_key, difference_kind, details_json, checked_at
        FROM catalog_reconciliation_diffs
        ORDER BY CASE kind
            WHEN 'novel' THEN 1
            WHEN 'comic' THEN 2
            WHEN 'coser-picture' THEN 3
            WHEN 'audio' THEN 4
            WHEN 'gallery' THEN 5
            ELSE 99 END,
            ordinal
        "#,
    )
    .fetch_all(transaction.connection())
    .await?;
    let diffs = diff_rows
        .into_iter()
        .map(|row| {
            let details_json: String = row.get("details_json");
            CatalogReconciliationDiff {
                kind: row.get("kind"),
                ordinal: row.get("ordinal"),
                work_key: row.get("work_key"),
                difference_kind: row.get("difference_kind"),
                details: serde_json::from_str(&details_json)
                    .unwrap_or_else(|_| json!({ "invalid_details": true })),
                checked_at: row.get("checked_at"),
            }
        })
        .collect();
    let overview = CatalogReconciliationOverview {
        items,
        diffs,
        max_recorded_diffs_per_kind: MAX_RECORDED_DIFFS,
    };
    transaction.commit().await?;
    Ok(overview)
}

pub(crate) async fn reconcile_novel(state: Arc<AppState>) -> Result<()> {
    reconcile_novel_with(&state.db, &state.resources, &state.config.generated_dir)
        .await
        .map(|_| ())
}

pub(crate) async fn reconcile_comic(state: Arc<AppState>) -> Result<()> {
    reconcile_comic_with(&state.db, &state.resources)
        .await
        .map(|_| ())
}

pub(crate) async fn reconcile_coser_picture(state: Arc<AppState>) -> Result<()> {
    reconcile_coser_picture_with(&state.db, &state.resources)
        .await
        .map(|_| ())
}

pub(crate) async fn reconcile_audio(state: Arc<AppState>) -> Result<()> {
    reconcile_audio_with(&state.db, &state.resources)
        .await
        .map(|_| ())
}

pub(crate) async fn reconcile_gallery(state: Arc<AppState>) -> Result<()> {
    reconcile_gallery_with(&state.db, &state.resources)
        .await
        .map(|_| ())
}

pub(crate) async fn reconcile_novel_with(
    db: &Db,
    resources: &ResourceGovernor,
    generated_dir: &Path,
) -> Result<CatalogReconciliationStatus> {
    reconcile_target(db, resources, ReconciliationTarget::Novel { generated_dir }).await
}

pub(crate) async fn reconcile_comic_with(
    db: &Db,
    resources: &ResourceGovernor,
) -> Result<CatalogReconciliationStatus> {
    reconcile_target(db, resources, ReconciliationTarget::Comic).await
}

pub(crate) async fn reconcile_coser_picture_with(
    db: &Db,
    resources: &ResourceGovernor,
) -> Result<CatalogReconciliationStatus> {
    reconcile_target(db, resources, ReconciliationTarget::CoserPicture).await
}

pub(crate) async fn reconcile_audio_with(
    db: &Db,
    resources: &ResourceGovernor,
) -> Result<CatalogReconciliationStatus> {
    reconcile_target(db, resources, ReconciliationTarget::Audio).await
}

pub(crate) async fn reconcile_gallery_with(
    db: &Db,
    resources: &ResourceGovernor,
) -> Result<CatalogReconciliationStatus> {
    reconcile_target(db, resources, ReconciliationTarget::Gallery).await
}

async fn reconcile_target(
    db: &Db,
    resources: &ResourceGovernor,
    target: ReconciliationTarget<'_>,
) -> Result<CatalogReconciliationStatus> {
    let kind = target.kind();
    match reconcile_target_inner(db, resources, &target).await {
        Ok(status) => Ok(status),
        Err(error) => {
            if let Err(record_error) = record_runtime_error(db, kind, &error.to_string()).await {
                tracing::error!(
                    kind,
                    error = %record_error,
                    original_error = %error,
                    "failed to persist catalog reconciliation runtime error"
                );
            }
            Err(error)
        }
    }
}

async fn reconcile_target_inner(
    db: &Db,
    resources: &ResourceGovernor,
    target: &ReconciliationTarget<'_>,
) -> Result<CatalogReconciliationStatus> {
    let kind = target.kind();
    let started = Instant::now();
    let started_at = Utc::now();
    let baseline = reconciliation_baseline(db, kind).await?;
    let before_revision = baseline.current_revision;
    let before_roots = baseline.roots;
    let previous = baseline.previous;
    begin_run(db, kind, before_revision, &before_roots, started_at).await?;

    if let Some(error) = baseline.prerequisite_error {
        let counts = ReconciliationCounts::default();
        persist_result(
            db,
            kind,
            "error",
            before_revision,
            before_revision,
            &before_roots,
            &before_roots,
            &counts,
            &[],
            &previous,
            started_at,
            started.elapsed(),
            Some(&error),
        )
        .await?;
        return status_for_kind(db, kind).await;
    }

    let run_token = format!("catalog-reconciliation:{}", uuid::Uuid::new_v4());
    let mut counts = ReconciliationCounts::default();
    let mut diffs = Vec::new();
    for root in &before_roots.roots {
        let mut after_work_key: Option<String> = None;
        loop {
            let work_keys_sql = format!(
                r#"
                SELECT DISTINCT work_key
                FROM file_inventory
                WHERE root_id = ?1
                  AND seen_generation = ?2
                  AND status = 'present'
                  AND work_key IS NOT NULL
                  AND (?3 IS NULL OR work_key > ?3)
                  AND ({})
                ORDER BY work_key
                LIMIT ?4
                "#,
                target.inventory_work_predicate()
            );
            // Keep the inventory page snapshot short: the following inspector
            // can perform filesystem/archive I/O and must not hold SQLite.
            let mut page_transaction = db.begin_tracked_read_transaction().await?;
            let work_keys = sqlx::query_scalar::<_, String>(&work_keys_sql)
                .bind(root.stamp.id)
                .bind(root.stamp.generation)
                .bind(after_work_key.as_deref())
                .bind(RECONCILIATION_WORK_KEY_PAGE_SIZE)
                .fetch_all(&mut *page_transaction)
                .await?;
            page_transaction.commit().await?;
            if work_keys.is_empty() {
                break;
            }
            let page_len = work_keys.len();
            let mut candidates = Vec::with_capacity(page_len);
            for work_key in work_keys {
                after_work_key = Some(work_key.clone());
                counts.expected = counts.expected.saturating_add(1);
                let candidate = match target
                    .inspect(db, resources, root, &run_token, work_key.clone())
                    .await
                {
                    Ok(candidate) => candidate,
                    Err(error) => {
                        counts.errors = counts.errors.saturating_add(1);
                        push_diff(
                            &mut diffs,
                            &work_key,
                            "error",
                            json!({ "message": bounded_text(&error.to_string(), MAX_ERROR_CHARS) }),
                        );
                        continue;
                    }
                };
                candidates.push((work_key, candidate));
            }
            let candidate_refs = candidates
                .iter()
                .map(|(_, candidate)| candidate)
                .collect::<Vec<_>>();
            let comparisons =
                compare_catalog_works(db, &candidate_refs, target.fingerprint_prefix()).await?;
            for ((work_key, _candidate), comparison) in candidates.into_iter().zip(comparisons) {
                match comparison {
                    WorkComparison::Matched => {
                        counts.matched = counts.matched.saturating_add(1);
                    }
                    WorkComparison::Missing => {
                        counts.missing = counts.missing.saturating_add(1);
                        push_diff(
                            &mut diffs,
                            &work_key,
                            "missing",
                            json!({ "message": "legacy canonical work is missing" }),
                        );
                    }
                    WorkComparison::Mismatch(fields) => {
                        counts.mismatch = counts.mismatch.saturating_add(1);
                        push_diff(
                            &mut diffs,
                            &work_key,
                            "mismatch",
                            json!({ "fields": bounded_fields(fields) }),
                        );
                    }
                }
            }
            if page_len < usize::try_from(RECONCILIATION_WORK_KEY_PAGE_SIZE).unwrap_or(usize::MAX) {
                break;
            }
        }
    }
    record_unexpected_works(db, target, &before_roots, &mut counts, &mut diffs).await?;

    let after_revision = catalog_revision(db).await?;
    let after_roots = root_snapshot(db, kind).await?;
    let stale = before_revision != after_revision || before_roots.digest != after_roots.digest;
    let status = if counts.errors > 0 {
        "error"
    } else if stale {
        "stale"
    } else if counts.differences() > 0 {
        "failed"
    } else {
        "passed"
    };
    let last_error = match status {
        "error" => Some(format!(
            "{} {} work(s) could not be inspected",
            counts.errors,
            target.display_name()
        )),
        "stale" => Some(
            "catalog revision or inventory root generation changed during reconciliation"
                .to_string(),
        ),
        "failed" => Some(format!(
            "catalog reconciliation found {} differing {} work(s)",
            counts.differences(),
            target.display_name()
        )),
        _ => None,
    };
    persist_result(
        db,
        kind,
        status,
        before_revision,
        after_revision,
        &before_roots,
        &after_roots,
        &counts,
        &diffs,
        &previous,
        started_at,
        started.elapsed(),
        last_error.as_deref(),
    )
    .await?;
    status_for_kind(db, kind).await
}

pub(crate) async fn require_current_pass(
    transaction: &mut Transaction<'_, Sqlite>,
    kind: &str,
) -> Result<()> {
    if !matches!(
        kind,
        NOVEL_KIND | COMIC_KIND | COSER_PICTURE_KIND | AUDIO_KIND | GALLERY_KIND
    ) {
        return Ok(());
    }
    let evidence = sqlx::query(
        r#"
        SELECT status, catalog_revision, catalog_revision_after,
               root_generation_sha256, root_generation_after_sha256,
               expected_works, matched_works, missing_works, unexpected_works,
               mismatch_works, error_works
        FROM catalog_reconciliation_state
        WHERE kind = ?1
        "#,
    )
    .bind(kind)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| {
        AppError::Other(format!(
            "cannot promote {kind}: catalog reconciliation evidence is missing"
        ))
    })?;
    let current_revision =
        sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
            .fetch_one(&mut **transaction)
            .await?;
    let root_rows = root_rows_in_transaction(transaction, kind).await?;
    let roots = snapshot_from_rows(root_rows)?;
    let status: String = evidence.get("status");
    let before_revision: i64 = evidence.get("catalog_revision");
    let after_revision: i64 = evidence.get("catalog_revision_after");
    let before_roots: Option<String> = evidence.get("root_generation_sha256");
    let after_roots: Option<String> = evidence.get("root_generation_after_sha256");
    let expected: i64 = evidence.get("expected_works");
    let matched: i64 = evidence.get("matched_works");
    let missing: i64 = evidence.get("missing_works");
    let unexpected: i64 = evidence.get("unexpected_works");
    let mismatch: i64 = evidence.get("mismatch_works");
    let errors: i64 = evidence.get("error_works");
    let exact = status == "passed"
        && before_revision == current_revision
        && after_revision == current_revision
        && before_roots.as_deref() == Some(roots.digest.as_str())
        && after_roots.as_deref() == Some(roots.digest.as_str())
        && expected == matched
        && missing == 0
        && unexpected == 0
        && mismatch == 0
        && errors == 0;
    if !exact {
        return Err(AppError::Other(format!(
            "cannot promote {kind}: a current passing legacy/Catalog v2 reconciliation is required"
        )));
    }
    Ok(())
}

#[derive(Debug)]
struct CatalogWorkFact {
    id: i64,
    title: String,
    subtitle: Option<String>,
    category: Option<String>,
    description: Option<String>,
    rating: Option<f64>,
    source_path: Option<String>,
    cover_asset_id: Option<i64>,
    meta_json: String,
    deleted_at: Option<String>,
    scanner_scope: Option<String>,
    scanner_fingerprint: Option<String>,
}

#[derive(Debug, Default)]
struct CatalogStatsFact {
    asset_count: i64,
    tag_count: i64,
    image_count: i64,
    track_count: i64,
    page_count: i64,
    actual_assets: i64,
    actual_tags: i64,
    actual_images: i64,
    actual_tracks: i64,
    actual_pages: i64,
}

fn numbered_placeholders(start: usize, count: usize) -> String {
    (start..start.saturating_add(count))
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ")
}

async fn compare_catalog_works(
    db: &Db,
    candidates: &[&ReconciliationCandidate],
    fingerprint_prefix: &str,
) -> Result<Vec<WorkComparison>> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    // Keep all Legacy Catalog facts for one reconciliation page on one short
    // SQLite snapshot.  The snapshot ends before the in-memory comparison so
    // it never spans inspector/file I/O or the next work-key page.
    let mut transaction = db.begin_tracked_read_transaction().await?;

    let source_paths = candidates
        .iter()
        .map(|candidate| candidate.work.source_path.as_str())
        .collect::<Vec<_>>();
    let source_placeholders = numbered_placeholders(2, source_paths.len());
    let work_query = format!(
        r#"
        SELECT work.id, work.title, work.subtitle, work.category,
               work.description, work.rating, work.source_path,
               work.cover_asset_id, work.meta_json, work.deleted_at,
               scanner.scope AS scanner_scope,
               scanner.fingerprint AS scanner_fingerprint
        FROM works AS work
        LEFT JOIN scanner_works AS scanner ON scanner.work_id = work.id
        WHERE work.kind = ?1 AND work.source_path IN ({source_placeholders})
        "#,
    );
    let mut work_query = sqlx::query(&work_query).bind(&candidates[0].source.kind);
    for source_path in &source_paths {
        work_query = work_query.bind(*source_path);
    }
    let work_rows = work_query.fetch_all(&mut *transaction).await?;
    let mut works = BTreeMap::<String, CatalogWorkFact>::new();
    for row in work_rows {
        let source_path = row.get::<Option<String>, _>("source_path");
        if let Some(source_path_key) = source_path.clone() {
            works.insert(
                source_path_key,
                CatalogWorkFact {
                    id: row.get("id"),
                    title: row.get("title"),
                    subtitle: row.get("subtitle"),
                    category: row.get("category"),
                    description: row.get("description"),
                    rating: row.get("rating"),
                    source_path,
                    cover_asset_id: row.get("cover_asset_id"),
                    meta_json: row.get("meta_json"),
                    deleted_at: row.get("deleted_at"),
                    scanner_scope: row.get("scanner_scope"),
                    scanner_fingerprint: row.get("scanner_fingerprint"),
                },
            );
        }
    }

    let work_ids = works.values().map(|work| work.id).collect::<Vec<_>>();
    let cover_asset_ids = works
        .values()
        .map(|work| (work.id, work.cover_asset_id))
        .collect::<BTreeMap<_, _>>();
    let work_id_placeholders = numbered_placeholders(1, work_ids.len());
    let mut actual_assets = BTreeMap::<i64, AssetMultisetSummary>::new();
    let mut actual_cover_digests = BTreeMap::<i64, [u8; 32]>::new();
    if !work_ids.is_empty() {
        let asset_query_sql = format!(
            r#"
            SELECT scanner.work_id, asset.id, asset.path, asset.mime, asset.role,
                   asset.variant, asset.position, asset.size, asset.meta_json
            FROM scanner_assets AS scanner
            JOIN assets AS asset ON asset.id = scanner.asset_id
                               AND asset.work_id = scanner.work_id
            WHERE scanner.work_id IN ({work_id_placeholders})
            ORDER BY scanner.work_id, asset.role, asset.variant,
                     asset.position, asset.id
            "#
        );
        let mut asset_query = sqlx::query(&asset_query_sql).bind(work_ids[0]);
        for work_id in work_ids.iter().skip(1) {
            asset_query = asset_query.bind(*work_id);
        }
        let mut rows = asset_query.fetch(&mut *transaction);
        while let Some(row) = rows.try_next().await? {
            let work_id: i64 = row.get("work_id");
            let asset_id: i64 = row.get("id");
            let canonical = canonical_actual_asset(&row);
            let digest = actual_assets
                .entry(work_id)
                .or_default()
                .insert(&canonical)?;
            if cover_asset_ids.get(&work_id).copied().flatten() == Some(asset_id) {
                actual_cover_digests.insert(work_id, digest);
            }
        }
    }

    let mut actual_tags = BTreeMap::<i64, BTreeMap<(String, String), String>>::new();
    if !work_ids.is_empty() {
        let tag_query_sql = format!(
            r#"
            SELECT source.work_id, tag.namespace, tag.key, tag.label
            FROM work_tag_sources AS source
            JOIN tags AS tag ON tag.id = source.tag_id
            WHERE source.owner = 'scanner'
              AND source.work_id IN ({work_id_placeholders})
            ORDER BY source.work_id, tag.namespace, tag.key
            "#
        );
        let mut query = sqlx::query(&tag_query_sql).bind(work_ids[0]);
        for work_id in work_ids.iter().skip(1) {
            query = query.bind(*work_id);
        }
        for row in query.fetch_all(&mut *transaction).await? {
            actual_tags
                .entry(row.get("work_id"))
                .or_default()
                .insert((row.get("namespace"), row.get("key")), row.get("label"));
        }
    }

    let mut actual_external_ids =
        BTreeMap::<i64, BTreeSet<(String, String, Option<String>, Option<String>)>>::new();
    if !work_ids.is_empty() {
        let external_query_sql = format!(
            r#"
            SELECT source.work_id, external.source, external.external_id,
                   external.token, external.url
            FROM external_id_sources AS source
            JOIN external_ids AS external ON external.id = source.external_id_id
            WHERE source.owner = 'scanner'
              AND source.work_id IN ({work_id_placeholders})
            ORDER BY source.work_id, external.source, external.external_id
            "#
        );
        let mut query = sqlx::query(&external_query_sql).bind(work_ids[0]);
        for work_id in work_ids.iter().skip(1) {
            query = query.bind(*work_id);
        }
        for row in query.fetch_all(&mut *transaction).await? {
            actual_external_ids
                .entry(row.get("work_id"))
                .or_default()
                .insert((
                    row.get("source"),
                    row.get("external_id"),
                    row.get("token"),
                    row.get("url"),
                ));
        }
    }

    let mut stats_by_work = BTreeMap::<i64, CatalogStatsFact>::new();
    if !work_ids.is_empty() {
        let stats_query_sql = format!(
            r#"
            WITH asset_stats AS (
                SELECT work_id,
                       COUNT(*) AS actual_assets,
                       SUM(CASE WHEN mime LIKE 'image/%' THEN 1 ELSE 0 END) AS actual_images,
                       SUM(CASE WHEN role = 'track' OR mime LIKE 'audio/%' THEN 1 ELSE 0 END) AS actual_tracks,
                       COALESCE(SUM(CASE
                           WHEN role = 'page' THEN 1
                           WHEN role = 'archive' THEN CAST(COALESCE(json_extract(meta_json, '$.page_count'), 0) AS INTEGER)
                           ELSE 0 END), 0) AS actual_pages
                FROM assets
                WHERE work_id IN ({work_id_placeholders})
                GROUP BY work_id
            ), tag_stats AS (
                SELECT work_id, COUNT(*) AS actual_tags
                FROM work_tags
                WHERE work_id IN ({work_id_placeholders})
                GROUP BY work_id
            )
            SELECT stats.work_id, stats.asset_count, stats.tag_count,
                   stats.image_count, stats.track_count, stats.page_count,
                   COALESCE(asset_stats.actual_assets, 0) AS actual_assets,
                   COALESCE(tag_stats.actual_tags, 0) AS actual_tags,
                   COALESCE(asset_stats.actual_images, 0) AS actual_images,
                   COALESCE(asset_stats.actual_tracks, 0) AS actual_tracks,
                   COALESCE(asset_stats.actual_pages, 0) AS actual_pages
            FROM work_stats AS stats
            LEFT JOIN asset_stats ON asset_stats.work_id = stats.work_id
            LEFT JOIN tag_stats ON tag_stats.work_id = stats.work_id
            WHERE stats.work_id IN ({work_id_placeholders})
            "#
        );
        let mut query = sqlx::query(&stats_query_sql);
        for work_id in &work_ids {
            query = query.bind(*work_id);
        }
        for row in query.fetch_all(&mut *transaction).await? {
            stats_by_work.insert(
                row.get("work_id"),
                CatalogStatsFact {
                    asset_count: row.get("asset_count"),
                    tag_count: row.get("tag_count"),
                    image_count: row.get("image_count"),
                    track_count: row.get("track_count"),
                    page_count: row.get("page_count"),
                    actual_assets: row.get("actual_assets"),
                    actual_tags: row.get("actual_tags"),
                    actual_images: row.get("actual_images"),
                    actual_tracks: row.get("actual_tracks"),
                    actual_pages: row.get("actual_pages"),
                },
            );
        }
    }

    transaction.commit().await?;

    let mut comparisons = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let Some(actual) = works.get(&candidate.work.source_path) else {
            comparisons.push(WorkComparison::Missing);
            continue;
        };
        let mut fields = Vec::new();
        compare_value(
            &mut fields,
            "work.title",
            &actual.title,
            &candidate.work.title,
        );
        if let Some(expected) = candidate.work.subtitle.as_ref() {
            compare_option_value(
                &mut fields,
                "work.subtitle",
                actual.subtitle.as_ref(),
                Some(expected),
            );
        }
        compare_option_value(
            &mut fields,
            "work.category",
            actual.category.as_ref(),
            candidate.work.category.as_ref(),
        );
        if let Some(expected) = candidate.work.description.as_ref() {
            compare_option_value(
                &mut fields,
                "work.description",
                actual.description.as_ref(),
                Some(expected),
            );
        }
        if let Some(expected) = candidate.work.rating {
            if actual.rating != Some(expected) {
                fields.push("work.rating".to_string());
            }
        }
        compare_option_value(
            &mut fields,
            "work.source_path",
            actual.source_path.as_ref(),
            Some(&candidate.work.source_path),
        );
        if actual.deleted_at.is_some() {
            fields.push("work.deleted_at".to_string());
        }
        if actual.scanner_scope.is_none() {
            fields.push("scanner_ownership".to_string());
        }
        let expected_fingerprint = candidate
            .fingerprint
            .strip_prefix(fingerprint_prefix)
            .unwrap_or(&candidate.fingerprint);
        if actual.scanner_fingerprint.as_deref() != Some(expected_fingerprint) {
            fields.push("scanner_fingerprint".to_string());
        }

        let actual_meta = serde_json::from_str::<Value>(&actual.meta_json).unwrap_or(Value::Null);
        if let Some(actual_meta) = actual_meta.as_object() {
            let actual_meta_fingerprint = actual_meta
                .get("_scanner_fingerprint")
                .and_then(Value::as_str)
                .map(|value| value.strip_prefix(fingerprint_prefix).unwrap_or(value));
            if actual_meta_fingerprint != Some(expected_fingerprint) {
                fields.push("work.meta._scanner_fingerprint".to_string());
            }
            if let Some(expected_meta) = candidate.work.meta.as_object() {
                for (key, expected) in expected_meta {
                    let actual_value = actual_meta.get(key);
                    let matches = if expected.is_null() {
                        actual_value.is_none() || actual_value.is_some_and(Value::is_null)
                    } else {
                        actual_value == Some(expected)
                    };
                    if !matches {
                        fields.push(format!("work.meta.{key}"));
                    }
                }
            } else {
                fields.push("mutation.work.meta".to_string());
            }
        } else {
            fields.push("work.meta_json".to_string());
        }

        let actual_asset_summary = actual_assets.get(&actual.id).cloned().unwrap_or_default();
        if actual_asset_summary != candidate.assets {
            fields.push(format!(
                "scanner_assets(expected={},actual={},expected_sha256={},actual_sha256={})",
                candidate.assets.count,
                actual_asset_summary.count,
                candidate.assets.fingerprint(),
                actual_asset_summary.fingerprint()
            ));
        }
        if actual.cover_asset_id.is_some() != candidate.cover_digest.is_some()
            || actual_cover_digests.get(&actual.id).copied() != candidate.cover_digest
        {
            fields.push("work.cover_asset_id".to_string());
        }

        let expected_tags = candidate
            .tags
            .iter()
            .filter(|tag| matches!(tag.owner, MutationOwner::Scanner))
            .map(|tag| ((tag.namespace.clone(), tag.key.clone()), tag.label.clone()))
            .collect::<BTreeMap<_, _>>();
        let actual_tags_for_work = actual_tags.get(&actual.id).cloned().unwrap_or_default();
        if actual_tags_for_work != expected_tags {
            fields.push(format!(
                "scanner_tags(expected={},actual={})",
                expected_tags.len(),
                actual_tags_for_work.len()
            ));
        }

        let expected_external = candidate
            .external_ids
            .iter()
            .filter(|external| matches!(external.owner, MutationOwner::Scanner))
            .map(|external| {
                (
                    external.source.clone(),
                    external.external_id.clone(),
                    external.token.clone(),
                    external.url.clone(),
                )
            })
            .collect::<BTreeSet<_>>();
        let actual_external_for_work = actual_external_ids
            .get(&actual.id)
            .cloned()
            .unwrap_or_default();
        if actual_external_for_work != expected_external {
            fields.push(format!(
                "scanner_external_ids(expected={},actual={})",
                expected_external.len(),
                actual_external_for_work.len()
            ));
        }

        let Some(stats) = stats_by_work.get(&actual.id) else {
            fields.push("work_stats.missing".to_string());
            comparisons.push(if fields.is_empty() {
                WorkComparison::Matched
            } else {
                WorkComparison::Mismatch(fields)
            });
            continue;
        };
        for (stored, actual_count, label) in [
            (
                stats.asset_count,
                stats.actual_assets,
                "work_stats.asset_count",
            ),
            (stats.tag_count, stats.actual_tags, "work_stats.tag_count"),
            (
                stats.image_count,
                stats.actual_images,
                "work_stats.image_count",
            ),
            (
                stats.track_count,
                stats.actual_tracks,
                "work_stats.track_count",
            ),
            (
                stats.page_count,
                stats.actual_pages,
                "work_stats.page_count",
            ),
        ] {
            if stored != actual_count {
                fields.push(label.to_string());
            }
        }
        comparisons.push(if fields.is_empty() {
            WorkComparison::Matched
        } else {
            WorkComparison::Mismatch(fields)
        });
    }
    Ok(comparisons)
}

fn canonical_expected_asset(asset: &AssetMutation) -> Result<Value> {
    let mut meta = asset.meta.as_object().cloned().ok_or_else(|| {
        AppError::Other("catalog reconciliation asset metadata is not an object".to_string())
    })?;
    meta.insert(
        "_source_version".to_string(),
        Value::String(asset.source_version.clone()),
    );
    let variant = asset.variant.as_deref().unwrap_or("");
    Ok(json!({
        "path": semantic_asset_path(&asset.path, &asset.role, variant, &asset.source_version),
        "mime": asset.mime,
        "role": asset.role,
        "variant": variant,
        "position": asset.position.unwrap_or(-1),
        "size": asset.size,
        "meta": Value::Object(meta),
    }))
}

fn canonical_actual_asset(row: &SqliteRow) -> Value {
    let meta_json: String = row.get("meta_json");
    let meta = serde_json::from_str::<Value>(&meta_json)
        .unwrap_or_else(|_| json!({ "_invalid_json": true }));
    let source_version = meta
        .get("_source_version")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let role: String = row.get("role");
    let variant: String = row.get("variant");
    let path: String = row.get("path");
    json!({
        "path": semantic_asset_path(&path, &role, &variant, source_version),
        "mime": row.get::<String, _>("mime"),
        "role": role,
        "variant": variant,
        "position": row.get::<i64, _>("position"),
        "size": row.get::<Option<i64>, _>("size"),
        "meta": meta,
    })
}

fn semantic_asset_path(path: &str, role: &str, variant: &str, source_version: &str) -> String {
    if role == "cover" && variant == "epub-extracted" {
        format!("content-version:{source_version}")
    } else {
        path.to_string()
    }
}

async fn record_unexpected_works(
    db: &Db,
    target: &ReconciliationTarget<'_>,
    roots: &RootSnapshot,
    counts: &mut ReconciliationCounts,
    diffs: &mut Vec<PendingDiff>,
) -> Result<()> {
    let mut seen = BTreeSet::new();
    let kind = target.kind();
    let legacy_scope = format!("legacy|{kind}");
    let mut transaction = db.begin_tracked_read_transaction().await?;
    for root in &roots.roots {
        let root_path = normalized_root_path(&root.stamp.root);
        let scope = format!("{kind}|{root_path}");
        let source_expression = target.inventory_presence_source_expression();
        let query = format!(
            r#"
            SELECT work.source_path
            FROM works AS work
            JOIN scanner_works AS scanner ON scanner.work_id = work.id
            WHERE work.kind = ?4
              AND work.deleted_at IS NULL
              AND work.source_path IS NOT NULL
              AND (scanner.scope = ?2 OR scanner.scope = ?5)
              AND (
                    ?3 = ''
                 OR substr(work.source_path, 1, length(?3) + 1) = (?3 || '/')
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM file_inventory AS inventory
                  WHERE inventory.root_id = ?1
                    AND inventory.seen_generation = ?6
                    AND inventory.status = 'present'
                    AND ({})
                    AND (
                        work.source_path = {}
                        OR (
                            ?4 = 'audio'
                            AND inventory.work_key GLOB 'RJ[0-9]*'
                            AND substr(work.source_path, 1, length({}) + 1)
                                = ({} || '/')
                        )
                    )
              )
            ORDER BY work.source_path
            "#,
            target.inventory_presence_predicate(),
            source_expression,
            source_expression,
            source_expression
        );
        let mut rows = sqlx::query_scalar::<_, String>(&query)
            .bind(root.stamp.id)
            .bind(scope)
            .bind(&root_path)
            .bind(kind)
            .bind(&legacy_scope)
            .bind(root.stamp.generation)
            .fetch(&mut *transaction);
        while let Some(source_path) = rows.try_next().await? {
            if !seen.insert(source_path.clone()) {
                continue;
            }
            counts.unexpected = counts.unexpected.saturating_add(1);
            let work_key = source_path
                .strip_prefix(&root_path)
                .unwrap_or(&source_path)
                .trim_start_matches('/')
                .to_string();
            push_diff(
                diffs,
                &work_key,
                "unexpected",
                json!({ "message": "legacy scanner work is absent from the current inventory" }),
            );
        }
    }
    transaction.commit().await?;
    Ok(())
}

async fn reconciliation_baseline(db: &Db, kind: &str) -> Result<ReconciliationBaseline> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let current_revision = catalog_revision_with_connection(transaction.connection()).await?;
    let roots = root_snapshot_with_connection(transaction.connection(), kind).await?;
    let previous = previous_evidence_with_connection(
        transaction.connection(),
        kind,
        current_revision,
        &roots.digest,
    )
    .await?;
    let prerequisite_error =
        reconciliation_prerequisite_error_with_connection(transaction.connection(), kind, &roots)
            .await?;
    transaction.commit().await?;
    Ok(ReconciliationBaseline {
        current_revision,
        roots,
        previous,
        prerequisite_error,
    })
}

async fn reconciliation_prerequisite_error_with_connection(
    connection: &mut SqliteConnection,
    kind: &str,
    roots: &RootSnapshot,
) -> Result<Option<String>> {
    let ownership = sqlx::query_scalar::<_, String>(
        "SELECT authoritative_writer FROM catalog_kind_ownership WHERE kind = ?1",
    )
    .bind(kind)
    .fetch_optional(&mut *connection)
    .await?;
    if ownership.as_deref() != Some("legacy") {
        return Ok(Some(format!(
            "catalog reconciliation requires {kind} ownership to remain legacy"
        )));
    }
    if roots.roots.is_empty() {
        return Ok(Some(format!(
            "catalog reconciliation requires at least one enabled {kind} inventory root"
        )));
    }
    if roots.ready_roots != roots.root_count() {
        return Ok(Some(format!(
            "catalog reconciliation requires all enabled {kind} roots to have a complete idle inventory generation"
        )));
    }
    let active_scan =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM scanner_locks WHERE name = 'library'")
            .fetch_one(&mut *connection)
            .await?;
    if active_scan > 0 {
        return Ok(Some(
            "catalog reconciliation cannot run while a library scan is active".to_string(),
        ));
    }
    let pending_events = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM scan_events AS event
        JOIN library_roots AS root ON root.id = event.root_id
        WHERE root.kind = ?1 AND root.enabled = 1
          AND event.status IN ('pending', 'processing')
        "#,
    )
    .bind(kind)
    .fetch_one(&mut *connection)
    .await?;
    if pending_events > 0 {
        return Ok(Some(format!(
            "catalog reconciliation requires the inventory event queue to be empty ({pending_events} pending)"
        )));
    }
    Ok(None)
}

async fn begin_run(
    db: &Db,
    kind: &str,
    catalog_revision: i64,
    roots: &RootSnapshot,
    started_at: chrono::DateTime<Utc>,
) -> Result<()> {
    let _write_slot = db.acquire_write_slot(128 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    sqlx::query("DELETE FROM catalog_reconciliation_diffs WHERE kind = ?1")
        .bind(kind)
        .execute(&mut *transaction)
        .await?;
    sqlx::query(
        r#"
        UPDATE catalog_reconciliation_state
        SET status = 'running',
            catalog_revision = ?2,
            catalog_revision_after = ?2,
            root_generation_sha256 = ?3,
            root_generation_after_sha256 = ?3,
            root_count = ?4,
            ready_root_count = ?5,
            expected_works = 0,
            matched_works = 0,
            missing_works = 0,
            unexpected_works = 0,
            mismatch_works = 0,
            error_works = 0,
            diffs_recorded = 0,
            diffs_truncated = 0,
            started_at = ?6,
            last_checked_at = NULL,
            took_millis = NULL,
            last_error = NULL
        WHERE kind = ?1
        "#,
    )
    .bind(kind)
    .bind(catalog_revision)
    .bind(&roots.digest)
    .bind(roots.root_count())
    .bind(roots.ready_roots)
    .bind(started_at)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn persist_result(
    db: &Db,
    kind: &str,
    status: &str,
    before_revision: i64,
    after_revision: i64,
    before_roots: &RootSnapshot,
    after_roots: &RootSnapshot,
    counts: &ReconciliationCounts,
    diffs: &[PendingDiff],
    previous: &PreviousEvidence,
    started_at: chrono::DateTime<Utc>,
    elapsed: std::time::Duration,
    last_error: Option<&str>,
) -> Result<()> {
    let checked_at = Utc::now();
    let took_millis = i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX);
    let total_differences = counts.differences();
    let diffs_recorded = i64::try_from(diffs.len()).unwrap_or(i64::MAX);
    let diffs_truncated = total_differences > diffs_recorded;
    let (consecutive_passes, passed_since) = if status == "passed" {
        if previous.current_pass {
            (
                previous.consecutive_passes.saturating_add(1),
                previous
                    .passed_since
                    .clone()
                    .or_else(|| Some(checked_at.to_rfc3339())),
            )
        } else {
            (1, Some(checked_at.to_rfc3339()))
        }
    } else {
        (0, None)
    };
    let _write_slot = db.acquire_write_slot(256 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    sqlx::query("DELETE FROM catalog_reconciliation_diffs WHERE kind = ?1")
        .bind(kind)
        .execute(&mut *transaction)
        .await?;
    for (ordinal, diff) in diffs.iter().enumerate() {
        sqlx::query(
            r#"
            INSERT INTO catalog_reconciliation_diffs (
                kind, ordinal, work_key, difference_kind, details_json, checked_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
        )
        .bind(kind)
        .bind(i64::try_from(ordinal).unwrap_or(i64::MAX))
        .bind(&diff.work_key)
        .bind(diff.difference_kind)
        .bind(diff.details.to_string())
        .bind(checked_at)
        .execute(&mut *transaction)
        .await?;
    }
    sqlx::query(
        r#"
        UPDATE catalog_reconciliation_state
        SET status = ?2,
            catalog_revision = ?3,
            catalog_revision_after = ?4,
            root_generation_sha256 = ?5,
            root_generation_after_sha256 = ?6,
            root_count = ?7,
            ready_root_count = ?8,
            expected_works = ?9,
            matched_works = ?10,
            missing_works = ?11,
            unexpected_works = ?12,
            mismatch_works = ?13,
            error_works = ?14,
            diffs_recorded = ?15,
            diffs_truncated = ?16,
            consecutive_passes = ?17,
            passed_since = ?18,
            started_at = ?19,
            last_checked_at = ?20,
            took_millis = ?21,
            last_error = ?22
        WHERE kind = ?1
        "#,
    )
    .bind(kind)
    .bind(status)
    .bind(before_revision)
    .bind(after_revision)
    .bind(&before_roots.digest)
    .bind(&after_roots.digest)
    .bind(before_roots.root_count())
    .bind(before_roots.ready_roots)
    .bind(counts.expected)
    .bind(counts.matched)
    .bind(counts.missing)
    .bind(counts.unexpected)
    .bind(counts.mismatch)
    .bind(counts.errors)
    .bind(diffs_recorded)
    .bind(diffs_truncated)
    .bind(consecutive_passes)
    .bind(passed_since)
    .bind(started_at)
    .bind(checked_at)
    .bind(took_millis)
    .bind(last_error.map(|value| bounded_text(value, MAX_ERROR_CHARS)))
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

async fn previous_evidence_with_connection(
    connection: &mut SqliteConnection,
    kind: &str,
    current_revision: i64,
    current_roots: &str,
) -> Result<PreviousEvidence> {
    let row = sqlx::query(
        r#"
        SELECT status, catalog_revision, catalog_revision_after,
               root_generation_sha256, root_generation_after_sha256,
               consecutive_passes, passed_since
        FROM catalog_reconciliation_state
        WHERE kind = ?1
        "#,
    )
    .bind(kind)
    .fetch_one(&mut *connection)
    .await?;
    let current_pass = row.get::<String, _>("status") == "passed"
        && row.get::<i64, _>("catalog_revision") == current_revision
        && row.get::<i64, _>("catalog_revision_after") == current_revision
        && row
            .get::<Option<String>, _>("root_generation_sha256")
            .as_deref()
            == Some(current_roots)
        && row
            .get::<Option<String>, _>("root_generation_after_sha256")
            .as_deref()
            == Some(current_roots);
    Ok(PreviousEvidence {
        current_pass,
        consecutive_passes: row.get("consecutive_passes"),
        passed_since: row.get("passed_since"),
    })
}

async fn record_runtime_error(db: &Db, kind: &str, error: &str) -> Result<()> {
    let _write_slot = db.acquire_write_slot(64 * 1024).await?;
    let mut transaction = db.begin_tracked_transaction().await?;
    sqlx::query(
        r#"
        UPDATE catalog_reconciliation_state
        SET status = 'error',
            consecutive_passes = 0,
            passed_since = NULL,
            last_checked_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
            last_error = ?2
        WHERE kind = ?1
        "#,
    )
    .bind(kind)
    .bind(bounded_text(error, MAX_ERROR_CHARS))
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

async fn status_for_kind(db: &Db, kind: &str) -> Result<CatalogReconciliationStatus> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let current_revision = catalog_revision_with_connection(&mut transaction).await?;
    let roots = root_snapshot_with_connection(&mut transaction, kind).await?;
    let row = sqlx::query(
        r#"
        SELECT kind, status, catalog_revision, catalog_revision_after,
               root_generation_sha256, root_generation_after_sha256,
               root_count, ready_root_count, expected_works, matched_works,
               missing_works, unexpected_works, mismatch_works, error_works,
               diffs_recorded, diffs_truncated, consecutive_passes,
               passed_since, started_at, last_checked_at, took_millis, last_error
        FROM catalog_reconciliation_state
        WHERE kind = ?1
        "#,
    )
    .bind(kind)
    .fetch_one(&mut *transaction)
    .await?;
    let status = status_from_row(&row, current_revision, &roots.digest);
    transaction.commit().await?;
    Ok(status)
}

fn status_from_row(
    row: &SqliteRow,
    current_catalog_revision: i64,
    current_root_digest: &str,
) -> CatalogReconciliationStatus {
    let stored_status: String = row.get("status");
    let catalog_revision: i64 = row.get("catalog_revision");
    let catalog_revision_after: i64 = row.get("catalog_revision_after");
    let root_generation_sha256: Option<String> = row.get("root_generation_sha256");
    let root_generation_after_sha256: Option<String> = row.get("root_generation_after_sha256");
    let current = stored_status == "passed"
        && catalog_revision == current_catalog_revision
        && catalog_revision_after == current_catalog_revision
        && root_generation_sha256.as_deref() == Some(current_root_digest)
        && root_generation_after_sha256.as_deref() == Some(current_root_digest)
        && row.get::<i64, _>("expected_works") == row.get::<i64, _>("matched_works")
        && row.get::<i64, _>("missing_works") == 0
        && row.get::<i64, _>("unexpected_works") == 0
        && row.get::<i64, _>("mismatch_works") == 0
        && row.get::<i64, _>("error_works") == 0;
    CatalogReconciliationStatus {
        kind: row.get("kind"),
        status: if stored_status == "passed" && !current {
            "stale".to_string()
        } else {
            stored_status
        },
        current,
        catalog_revision,
        catalog_revision_after,
        current_catalog_revision,
        root_generation_sha256,
        root_generation_after_sha256,
        current_root_generation_sha256: current_root_digest.to_string(),
        root_count: row.get("root_count"),
        ready_root_count: row.get("ready_root_count"),
        expected_works: row.get("expected_works"),
        matched_works: row.get("matched_works"),
        missing_works: row.get("missing_works"),
        unexpected_works: row.get("unexpected_works"),
        mismatch_works: row.get("mismatch_works"),
        error_works: row.get("error_works"),
        diffs_recorded: row.get("diffs_recorded"),
        diffs_truncated: row.get::<i64, _>("diffs_truncated") != 0,
        consecutive_passes: row.get("consecutive_passes"),
        passed_since: row.get("passed_since"),
        started_at: row.get("started_at"),
        last_checked_at: row.get("last_checked_at"),
        took_millis: row.get("took_millis"),
        last_error: row.get("last_error"),
    }
}

async fn catalog_revision(db: &Db) -> Result<i64> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let revision =
        sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
            .fetch_one(&mut *transaction)
            .await?;
    transaction.commit().await?;
    Ok(revision)
}

async fn root_snapshot(db: &Db, kind: &str) -> Result<RootSnapshot> {
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let rows = sqlx::query(
        r#"
        SELECT id, provider, root, generation, completed_generation, status,
               active_token, last_event_seq, present_files, missing_files,
               audio_grouping
        FROM library_roots
        WHERE kind = ?1 AND enabled = 1
        ORDER BY id
        "#,
    )
    .bind(kind)
    .fetch_all(&mut *transaction)
    .await?;
    let snapshot = snapshot_from_rows(rows)?;
    transaction.commit().await?;
    Ok(snapshot)
}

async fn catalog_revision_with_connection(connection: &mut SqliteConnection) -> Result<i64> {
    Ok(
        sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
            .fetch_one(&mut *connection)
            .await?,
    )
}

async fn root_snapshot_with_connection(
    connection: &mut SqliteConnection,
    kind: &str,
) -> Result<RootSnapshot> {
    let rows = sqlx::query(
        r#"
        SELECT id, provider, root, generation, completed_generation, status,
               active_token, last_event_seq, present_files, missing_files,
               audio_grouping
        FROM library_roots
        WHERE kind = ?1 AND enabled = 1
        ORDER BY id
        "#,
    )
    .bind(kind)
    .fetch_all(&mut *connection)
    .await?;
    snapshot_from_rows(rows)
}

async fn root_snapshots_with_connection(
    connection: &mut SqliteConnection,
) -> Result<BTreeMap<String, RootSnapshot>> {
    let rows = sqlx::query(
        r#"
        SELECT kind, id, provider, root, generation, completed_generation, status,
               active_token, last_event_seq, present_files, missing_files,
               audio_grouping
        FROM library_roots
        WHERE enabled = 1
        ORDER BY kind, id
        "#,
    )
    .fetch_all(&mut *connection)
    .await?;
    let mut grouped = BTreeMap::<String, Vec<SqliteRow>>::new();
    for row in rows {
        let kind: String = row.get("kind");
        grouped.entry(kind).or_default().push(row);
    }
    let mut snapshots = BTreeMap::new();
    for (kind, rows) in grouped {
        snapshots.insert(kind, snapshot_from_rows(rows)?);
    }
    Ok(snapshots)
}

async fn root_rows_in_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    kind: &str,
) -> Result<Vec<SqliteRow>> {
    Ok(sqlx::query(
        r#"
        SELECT id, provider, root, generation, completed_generation, status,
               active_token, last_event_seq, present_files, missing_files,
               audio_grouping
        FROM library_roots
        WHERE kind = ?1 AND enabled = 1
        ORDER BY id
        "#,
    )
    .bind(kind)
    .fetch_all(&mut **transaction)
    .await?)
}

fn snapshot_from_rows(rows: Vec<SqliteRow>) -> Result<RootSnapshot> {
    let mut roots = Vec::with_capacity(rows.len());
    let mut ready_roots = 0_i64;
    for row in rows {
        let root: String = row.get("root");
        let stamp = RootStamp {
            id: row.get("id"),
            provider: row.get("provider"),
            root: normalized_root_path(&root),
            generation: row.get("generation"),
            completed_generation: row.get("completed_generation"),
            status: row.get("status"),
            active_token: row.get("active_token"),
            last_event_seq: row.get("last_event_seq"),
            present_files: row.get("present_files"),
            missing_files: row.get("missing_files"),
            audio_grouping: parse_audio_grouping(row.get("audio_grouping")),
        };
        if stamp.status == "idle"
            && stamp.generation == stamp.completed_generation
            && stamp.active_token.is_none()
        {
            ready_roots = ready_roots.saturating_add(1);
        }
        roots.push(RootFact {
            path: PathBuf::from(&root),
            stamp,
        });
    }
    let encoded = serde_json::to_vec(&roots.iter().map(|root| &root.stamp).collect::<Vec<_>>())
        .map_err(|error| AppError::Other(format!("failed to encode root snapshot: {error}")))?;
    let digest = format!("{:x}", Sha256::digest(encoded));
    Ok(RootSnapshot {
        roots,
        digest,
        ready_roots,
    })
}

fn parse_audio_grouping(value: String) -> AudioGroupingMode {
    match value.trim().to_ascii_lowercase().as_str() {
        "rj" => AudioGroupingMode::Rj,
        "folder" => AudioGroupingMode::Folder,
        _ => AudioGroupingMode::Auto,
    }
}

fn compare_value<T: PartialEq>(fields: &mut Vec<String>, label: &str, actual: &T, expected: &T) {
    if actual != expected {
        fields.push(label.to_string());
    }
}

fn compare_option_value<T: PartialEq>(
    fields: &mut Vec<String>,
    label: &str,
    actual: Option<&T>,
    expected: Option<&T>,
) {
    if actual != expected {
        fields.push(label.to_string());
    }
}

fn push_diff(
    diffs: &mut Vec<PendingDiff>,
    work_key: &str,
    difference_kind: &'static str,
    details: Value,
) {
    if diffs.len() >= MAX_RECORDED_DIFFS {
        return;
    }
    let work_key = bounded_text(work_key, MAX_WORK_KEY_CHARS);
    diffs.push(PendingDiff {
        work_key: if work_key.is_empty() {
            "<root>".to_string()
        } else {
            work_key
        },
        difference_kind,
        details,
    });
}

fn bounded_fields(fields: Vec<String>) -> Vec<String> {
    fields
        .into_iter()
        .take(MAX_DIFF_FIELDS)
        .map(|field| bounded_text(&field, 512))
        .collect()
}

fn bounded_text(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn normalized_root_path(value: &str) -> String {
    value.replace('\\', "/").trim_end_matches('/').to_string()
}

#[cfg(test)]
pub(crate) async fn record_current_pass_for_test(db: &Db, kind: &str) {
    let revision = catalog_revision(db).await.unwrap();
    let roots = root_snapshot(db, kind).await.unwrap();
    let now = Utc::now();
    sqlx::query(
        r#"
        UPDATE catalog_reconciliation_state
        SET status = 'passed', catalog_revision = ?2, catalog_revision_after = ?2,
            root_generation_sha256 = ?3, root_generation_after_sha256 = ?3,
            root_count = ?4, ready_root_count = ?5,
            expected_works = 0, matched_works = 0,
            missing_works = 0, unexpected_works = 0,
            mismatch_works = 0, error_works = 0,
            diffs_recorded = 0, diffs_truncated = 0,
            consecutive_passes = 1, passed_since = ?6,
            started_at = ?6, last_checked_at = ?6,
            took_millis = 0, last_error = NULL
        WHERE kind = ?1
        "#,
    )
    .bind(kind)
    .bind(revision)
    .bind(&roots.digest)
    .bind(roots.root_count())
    .bind(roots.ready_roots)
    .bind(now)
    .execute(db.pool())
    .await
    .unwrap();
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use super::*;
    use crate::resource::ResourceLimits;

    struct NovelFixture {
        _temp: tempfile::TempDir,
        db: Db,
        resources: ResourceGovernor,
        root: PathBuf,
        generated_dir: PathBuf,
        root_id: i64,
        work_id: i64,
    }

    struct ComicFixture {
        _temp: tempfile::TempDir,
        db: Db,
        resources: ResourceGovernor,
        root: PathBuf,
        archive: PathBuf,
        root_id: i64,
        work_id: i64,
    }

    struct CoserPictureFixture {
        _temp: tempfile::TempDir,
        db: Db,
        resources: ResourceGovernor,
        root: PathBuf,
        archive: PathBuf,
        root_id: i64,
        work_id: i64,
    }

    struct AudioFixture {
        _temp: tempfile::TempDir,
        db: Db,
        resources: ResourceGovernor,
        track: PathBuf,
        root_id: i64,
        work_id: i64,
    }

    struct GalleryFixture {
        _temp: tempfile::TempDir,
        db: Db,
        resources: ResourceGovernor,
        image: PathBuf,
        root_id: i64,
        work_id: i64,
    }

    fn database_url(temp: &tempfile::TempDir) -> String {
        format!(
            "sqlite://{}",
            temp.path()
                .join("catalog-reconciliation.sqlite")
                .to_string_lossy()
                .replace('\\', "/")
        )
    }

    fn path_string(path: &Path) -> String {
        path.to_string_lossy().replace('\\', "/")
    }

    fn write_epub(path: &Path, title: &str) {
        let file = File::create(path).unwrap();
        let mut archive = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();
        archive
            .start_file("META-INF/container.xml", options)
            .unwrap();
        archive
            .write_all(
                br#"<?xml version="1.0"?><container><rootfiles><rootfile full-path="OPS/content.opf" media-type="application/oebps-package+xml"/></rootfiles></container>"#,
            )
            .unwrap();
        archive.start_file("OPS/content.opf", options).unwrap();
        archive
            .write_all(
                format!(
                    r#"<?xml version="1.0"?><package><metadata><dc:title>{title}</dc:title><dc:creator>Fixture Author</dc:creator><dc:description>Fixture Description</dc:description><dc:language>zh-CN</dc:language><dc:subject>Fantasy</dc:subject><meta property="belongs-to-collection">Fixture Series</meta><meta property="group-position">2</meta></metadata><manifest><item id="cover" href="cover.jpg" media-type="image/jpeg" properties="cover-image"/></manifest><spine/></package>"#
                )
                .as_bytes(),
            )
            .unwrap();
        archive.start_file("OPS/cover.jpg", options).unwrap();
        archive
            .write_all(&[0xff, 0xd8, 0xff, 0xe0, 1, 2, 3, 4, 0xff, 0xd9])
            .unwrap();
        archive.finish().unwrap();
    }

    fn write_comic_archive(path: &Path) {
        let file = File::create(path).unwrap();
        let mut archive = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();
        archive.start_file("001.jpg", options).unwrap();
        archive
            .write_all(&[0xff, 0xd8, 0xff, 0xe0, 1, 2, 0xff, 0xd9])
            .unwrap();
        archive.start_file("002.png", options).unwrap();
        archive
            .write_all(&[0x89, b'P', b'N', b'G', 1, 2, 3])
            .unwrap();
        archive.finish().unwrap();
    }

    fn streamed_reconciliation_chunk(
        assets: Vec<AssetMutation>,
        complete_snapshot: bool,
    ) -> WorkMutation {
        WorkMutation {
            source: MutationSource {
                kind: "audio".to_string(),
                root_id: 1,
                work_key: "RJ000001".to_string(),
                provider: "local".to_string(),
            },
            previous_work_key: None,
            fence: crate::catalog_writer::MutationFence {
                root_generation: 1,
                scan_token: "streamed-reconciliation".to_string(),
                complete_snapshot,
            },
            fingerprint: "audio-v1:fixture".to_string(),
            work: WorkMutationFields {
                title: "Streamed fixture".to_string(),
                subtitle: None,
                category: Some("Audio".to_string()),
                description: None,
                rating: None,
                source_path: "/audio/RJ000001".to_string(),
                meta: json!({ "track_count": 10_000 }),
            },
            assets,
            tags: complete_snapshot
                .then(|| TagMutation {
                    namespace: "audio".to_string(),
                    key: "voice-work".to_string(),
                    label: "Audio".to_string(),
                    translated_label: None,
                    translated_namespace: None,
                    source: "audio-folder".to_string(),
                    intro: None,
                    links: None,
                    owner: MutationOwner::Scanner,
                })
                .into_iter()
                .collect(),
            external_ids: Vec::new(),
        }
    }

    #[tokio::test]
    async fn batch_reconciliation_compares_a_full_page_without_parameter_overflow() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();

        let _write_slot = db.acquire_write_slot(4 * 1024 * 1024).await.unwrap();
        let mut transaction = db.begin_tracked_transaction().await.unwrap();
        for index in 0..RECONCILIATION_WORK_KEY_PAGE_SIZE {
            let source_path = format!("/audio/RJ{index:06}");
            let work_id = sqlx::query_scalar::<_, i64>(
                r#"
                INSERT INTO works (kind, title, category, source_path, meta_json)
                VALUES ('audio', ?1, 'Audio', ?2, ?3)
                RETURNING id
                "#,
            )
            .bind(format!("Full page {index}"))
            .bind(&source_path)
            .bind(r#"{"_scanner_fingerprint":"fixture"}"#)
            .fetch_one(&mut *transaction)
            .await
            .unwrap();
            sqlx::query(
                r#"
                INSERT INTO scanner_works (work_id, scope, seen_token, fingerprint)
                VALUES (?1, 'audio|/audio', 'fixture-token', 'fixture')
                "#,
            )
            .bind(work_id)
            .execute(&mut *transaction)
            .await
            .unwrap();
        }
        transaction.commit().await.unwrap();

        let candidates = (0..RECONCILIATION_WORK_KEY_PAGE_SIZE)
            .map(|index| ReconciliationCandidate {
                source: MutationSource {
                    kind: AUDIO_KIND.to_string(),
                    root_id: 1,
                    work_key: format!("RJ{index:06}"),
                    provider: "local".to_string(),
                },
                fingerprint: "audio-v1:fixture".to_string(),
                work: WorkMutationFields {
                    title: format!("Full page {index}"),
                    subtitle: None,
                    category: Some("Audio".to_string()),
                    description: None,
                    rating: None,
                    source_path: format!("/audio/RJ{index:06}"),
                    meta: json!({}),
                },
                assets: AssetMultisetSummary::default(),
                cover_digest: None,
                tags: Vec::new(),
                external_ids: Vec::new(),
            })
            .collect::<Vec<_>>();
        let candidate_refs = candidates.iter().collect::<Vec<_>>();
        let comparisons = compare_catalog_works(&db, &candidate_refs, AUDIO_FINGERPRINT_PREFIX)
            .await
            .unwrap();

        assert_eq!(
            comparisons.len(),
            RECONCILIATION_WORK_KEY_PAGE_SIZE as usize
        );
        assert!(comparisons
            .iter()
            .all(|comparison| matches!(comparison, WorkComparison::Matched)));
    }

    #[test]
    fn streamed_candidate_summarizes_twenty_thousand_assets_without_retaining_chunks() {
        let mut builder = ReconciliationAccumulator::default();
        let track_count = MAX_RECONCILIATION_ASSETS_PER_WORK - 1;
        for chunk_start in (0..track_count).step_by(256) {
            let chunk_end = (chunk_start + 256).min(track_count);
            let assets = (chunk_start..chunk_end)
                .map(|position| AssetMutation {
                    path: format!("/audio/RJ000001/{position:05}.flac"),
                    mime: "audio/flac".to_string(),
                    role: "track".to_string(),
                    variant: Some("flac".to_string()),
                    position: Some(i64::try_from(position).unwrap()),
                    size: Some(1024),
                    source_version: format!("v{position}"),
                    meta: json!({ "track_key": format!("track-{position}") }),
                })
                .collect();
            builder
                .push(streamed_reconciliation_chunk(assets, false))
                .unwrap();
        }
        builder
            .push(streamed_reconciliation_chunk(
                vec![AssetMutation {
                    path: "/audio/RJ000001/cover.jpg".to_string(),
                    mime: "image/jpeg".to_string(),
                    role: "cover".to_string(),
                    variant: None,
                    position: None,
                    size: Some(2048),
                    source_version: "cover-v1".to_string(),
                    meta: json!({}),
                }],
                false,
            ))
            .unwrap();
        builder
            .push(streamed_reconciliation_chunk(Vec::new(), true))
            .unwrap();

        let candidate = builder.finish().unwrap();
        assert_eq!(
            candidate.assets.count,
            MAX_RECONCILIATION_ASSETS_PER_WORK as u64
        );
        assert!(candidate.cover_digest.is_some());
        assert_eq!(candidate.tags.len(), 1);
        assert!(std::mem::size_of::<ReconciliationCandidate>() < 1024);

        let mut forward = AssetMultisetSummary::default();
        let mut reverse = AssetMultisetSummary::default();
        for value in 0..512 {
            forward.insert(&json!({ "value": value })).unwrap();
        }
        for value in (0..512).rev() {
            reverse.insert(&json!({ "value": value })).unwrap();
        }
        assert_eq!(forward, reverse);
    }

    #[tokio::test]
    async fn runtime_error_update_uses_the_single_writer_gate() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let before = db.write_snapshot();

        record_runtime_error(&db, NOVEL_KIND, "fixture reconciliation failure")
            .await
            .unwrap();

        let after = db.write_snapshot();
        assert_eq!(after.completed, before.completed + 1);
        let state = sqlx::query_as::<_, (String, i64, Option<String>)>(
            "SELECT status, consecutive_passes, last_error FROM catalog_reconciliation_state WHERE kind = ?1",
        )
        .bind(NOVEL_KIND)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(state.0, "error");
        assert_eq!(state.1, 0);
        assert_eq!(state.2.as_deref(), Some("fixture reconciliation failure"));
    }

    #[test]
    fn streamed_accumulator_rejects_assets_beyond_the_work_bound() {
        let mut accumulator = ReconciliationAccumulator::default();
        let chunk = |start: usize, end: usize| {
            (start..end)
                .map(|position| AssetMutation {
                    path: format!("/audio/RJ000001/{position:05}.flac"),
                    mime: "audio/flac".to_string(),
                    role: "track".to_string(),
                    variant: Some("flac".to_string()),
                    position: Some(i64::try_from(position).unwrap()),
                    size: Some(1024),
                    source_version: format!("v{position}"),
                    meta: json!({ "track_key": format!("track-{position}") }),
                })
                .collect::<Vec<_>>()
        };
        for start in (0..MAX_RECONCILIATION_ASSETS_PER_WORK).step_by(256) {
            let end = (start + 256).min(MAX_RECONCILIATION_ASSETS_PER_WORK);
            accumulator
                .push(streamed_reconciliation_chunk(chunk(start, end), false))
                .unwrap();
        }
        let error = accumulator
            .push(streamed_reconciliation_chunk(
                chunk(
                    MAX_RECONCILIATION_ASSETS_PER_WORK,
                    MAX_RECONCILIATION_ASSETS_PER_WORK + 1,
                ),
                false,
            ))
            .unwrap_err()
            .to_string();
        assert!(error.contains("streamed asset safety limit"));
    }

    async fn insert_inventory_file(
        db: &Db,
        root_id: i64,
        generation: i64,
        relative_path: &str,
        size: i64,
    ) {
        sqlx::query(
            r#"
            INSERT INTO file_inventory (
                root_id, relative_path, parent_key, media_class, size, mtime_ns,
                fast_fingerprint, work_key, seen_generation, status
            )
            VALUES (?1, ?2, '', 'novel', ?3, 1, ?2, ?2, ?4, 'present')
            "#,
        )
        .bind(root_id)
        .bind(relative_path)
        .bind(size)
        .bind(generation)
        .execute(db.pool())
        .await
        .unwrap();
    }

    async fn insert_comic_inventory_file(
        db: &Db,
        root_id: i64,
        generation: i64,
        relative_path: &str,
        size: i64,
    ) {
        sqlx::query(
            r#"
            INSERT INTO file_inventory (
                root_id, relative_path, parent_key, media_class, size, mtime_ns,
                fast_fingerprint, work_key, seen_generation, status
            )
            VALUES (?1, ?2, 'Author', 'comic', ?3, 1, ?2, ?2, ?4, 'present')
            "#,
        )
        .bind(root_id)
        .bind(relative_path)
        .bind(size)
        .bind(generation)
        .execute(db.pool())
        .await
        .unwrap();
    }

    async fn insert_coser_picture_inventory_file(
        db: &Db,
        root_id: i64,
        generation: i64,
        relative_path: &str,
        size: i64,
    ) {
        sqlx::query(
            r#"
            INSERT INTO file_inventory (
                root_id, relative_path, parent_key, media_class, size, mtime_ns,
                fast_fingerprint, work_key, seen_generation, status
            )
            VALUES (?1, ?2, 'Alice', 'coser-picture', ?3, 1, ?2, ?2, ?4, 'present')
            "#,
        )
        .bind(root_id)
        .bind(relative_path)
        .bind(size)
        .bind(generation)
        .execute(db.pool())
        .await
        .unwrap();
    }

    async fn insert_audio_inventory_file(
        db: &Db,
        root_id: i64,
        generation: i64,
        relative_path: &str,
        work_key: &str,
        size: i64,
    ) {
        sqlx::query(
            r#"
            INSERT INTO file_inventory (
                root_id, relative_path, parent_key, media_class, size, mtime_ns,
                fast_fingerprint, work_key, seen_generation, status
            )
            VALUES (?1, ?2, ?3, 'audio', ?4, 1, ?2, ?3, ?5, 'present')
            "#,
        )
        .bind(root_id)
        .bind(relative_path)
        .bind(work_key)
        .bind(size)
        .bind(generation)
        .execute(db.pool())
        .await
        .unwrap();
    }

    async fn insert_gallery_inventory_file(
        db: &Db,
        root_id: i64,
        generation: i64,
        relative_path: &str,
        work_key: &str,
        size: i64,
    ) {
        sqlx::query(
            r#"
            INSERT INTO file_inventory (
                root_id, relative_path, parent_key, media_class, size, mtime_ns,
                fast_fingerprint, work_key, seen_generation, status
            )
            VALUES (?1, ?2, ?3, 'gallery', ?4, 1, ?2, ?3, ?5, 'present')
            "#,
        )
        .bind(root_id)
        .bind(relative_path)
        .bind(work_key)
        .bind(size)
        .bind(generation)
        .execute(db.pool())
        .await
        .unwrap();
    }

    async fn persist_legacy_mutation_stream(
        db: &Db,
        kind: &str,
        root: &Path,
        fingerprint_prefix: &str,
        scan_token: &str,
        mutations: &[WorkMutation],
    ) -> i64 {
        let first = mutations
            .first()
            .expect("legacy fixture requires at least one mutation");
        assert!(db
            .try_acquire_scanner_lock("library", scan_token, 60)
            .await
            .unwrap());
        let legacy_fingerprint = first
            .fingerprint
            .strip_prefix(fingerprint_prefix)
            .unwrap_or(&first.fingerprint);
        let work_id = db
            .upsert_scanner_work(
                kind,
                &first.work.title,
                Some(&first.work.source_path),
                first.work.category.as_deref(),
                first.work.description.as_deref(),
                first.work.rating,
                first.work.meta.clone(),
                scan_token,
                legacy_fingerprint,
            )
            .await
            .unwrap();
        for mutation in mutations {
            for asset in &mutation.assets {
                let mut meta = asset.meta.as_object().cloned().unwrap_or_default();
                meta.insert(
                    "_source_version".to_string(),
                    Value::String(asset.source_version.clone()),
                );
                db.upsert_scanner_asset(
                    work_id,
                    &asset.path,
                    &asset.mime,
                    &asset.role,
                    asset.variant.as_deref(),
                    asset.position,
                    asset.size,
                    Value::Object(meta),
                    scan_token,
                )
                .await
                .unwrap();
            }
            for tag in &mutation.tags {
                db.upsert_and_link_scanner_tag(
                    work_id,
                    &tag.namespace,
                    &tag.key,
                    &tag.label,
                    tag.translated_label.as_deref(),
                    tag.translated_namespace.as_deref(),
                    &tag.source,
                    tag.intro.as_deref(),
                    tag.links.as_deref(),
                    scan_token,
                )
                .await
                .unwrap();
            }
            for external in &mutation.external_ids {
                db.upsert_scanner_external_id(
                    work_id,
                    &external.source,
                    &external.external_id,
                    external.token.as_deref(),
                    external.url.as_deref(),
                    scan_token,
                )
                .await
                .unwrap();
            }
        }
        db.finish_scanner_work(
            work_id,
            &format!("{kind}|{}", path_string(root)),
            scan_token,
            legacy_fingerprint,
        )
        .await
        .unwrap();
        db.release_scanner_lock("library", scan_token)
            .await
            .unwrap();
        work_id
    }

    async fn setup_legacy_fixture() -> NovelFixture {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("novels");
        let generated_dir = temp.path().join("generated");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&generated_dir).unwrap();
        let epub = root.join("book.epub");
        write_epub(&epub, "Fixture Novel");

        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let generation = 4_i64;
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots (
                kind, provider, root, generation, completed_generation,
                status, present_files
            )
            VALUES ('novel', 'local', ?1, ?2, ?2, 'idle', 1)
            RETURNING id
            "#,
        )
        .bind(path_string(&root))
        .bind(generation)
        .fetch_one(db.pool())
        .await
        .unwrap();
        insert_inventory_file(
            &db,
            root_id,
            generation,
            "book.epub",
            i64::try_from(std::fs::metadata(&epub).unwrap().len()).unwrap(),
        )
        .await;

        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let mutation = novel::inspect(
            &resources,
            NovelInspectionRequest {
                root_id,
                root_generation: generation,
                scan_token: "legacy-scan".to_string(),
                provider: "local".to_string(),
                root: root.clone(),
                work_key: "book.epub".to_string(),
                previous_work_key: None,
                generated_dir: generated_dir.clone(),
            },
        )
        .await
        .unwrap();
        assert!(db
            .try_acquire_scanner_lock("library", "legacy-scan", 60)
            .await
            .unwrap());
        let legacy_fingerprint = mutation
            .fingerprint
            .strip_prefix(NOVEL_FINGERPRINT_PREFIX)
            .unwrap();
        let work_id = db
            .upsert_scanner_work(
                NOVEL_KIND,
                &mutation.work.title,
                Some(&mutation.work.source_path),
                mutation.work.category.as_deref(),
                mutation.work.description.as_deref(),
                mutation.work.rating,
                mutation.work.meta.clone(),
                "legacy-scan",
                legacy_fingerprint,
            )
            .await
            .unwrap();
        for asset in &mutation.assets {
            let path = if asset.role == "cover" {
                let extension = Path::new(&asset.path)
                    .extension()
                    .and_then(|value| value.to_str())
                    .unwrap_or("jpg");
                let legacy_path = generated_dir.join(format!(
                    "epub-cover-{work_id}-{}.{extension}",
                    asset.source_version
                ));
                std::fs::copy(&asset.path, &legacy_path).unwrap();
                path_string(&legacy_path)
            } else {
                asset.path.clone()
            };
            let mut meta = asset.meta.as_object().unwrap().clone();
            meta.insert(
                "_source_version".to_string(),
                Value::String(asset.source_version.clone()),
            );
            db.upsert_scanner_asset(
                work_id,
                &path,
                &asset.mime,
                &asset.role,
                asset.variant.as_deref(),
                asset.position,
                asset.size,
                Value::Object(meta),
                "legacy-scan",
            )
            .await
            .unwrap();
        }
        for tag in &mutation.tags {
            db.upsert_and_link_scanner_tag(
                work_id,
                &tag.namespace,
                &tag.key,
                &tag.label,
                tag.translated_label.as_deref(),
                tag.translated_namespace.as_deref(),
                &tag.source,
                tag.intro.as_deref(),
                tag.links.as_deref(),
                "legacy-scan",
            )
            .await
            .unwrap();
        }
        db.finish_scanner_work(
            work_id,
            &format!("novel|{}", path_string(&root)),
            "legacy-scan",
            legacy_fingerprint,
        )
        .await
        .unwrap();
        db.release_scanner_lock("library", "legacy-scan")
            .await
            .unwrap();
        NovelFixture {
            _temp: temp,
            db,
            resources,
            root,
            generated_dir,
            root_id,
            work_id,
        }
    }

    async fn setup_legacy_comic_fixture() -> ComicFixture {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("comics");
        let author = root.join("Author");
        std::fs::create_dir_all(&author).unwrap();
        let archive = author.join("book.cbz");
        write_comic_archive(&archive);
        std::fs::write(author.join("cover.jpg"), [1_u8, 2, 3, 4]).unwrap();
        std::fs::write(
            author.join("ComicInfo.xml"),
            r#"<ComicInfo><Series>Fixture Comic</Series><AlternateSeries>Alternate title</AlternateSeries><Writer>Fixture Circle</Writer><Penciller>Fixture Artist</Penciller><Genre>f:Romance, m:Action</Genre><PageCount>9</PageCount><LanguageIso>ja</LanguageIso><CommunityRating>4.5</CommunityRating></ComicInfo>"#,
        )
        .unwrap();

        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let generation = 7_i64;
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots (
                kind, provider, root, scan_depth, generation,
                completed_generation, status, present_files
            )
            VALUES ('comic', 'local', ?1, 2, ?2, ?2, 'idle', 1)
            RETURNING id
            "#,
        )
        .bind(path_string(&root))
        .bind(generation)
        .fetch_one(db.pool())
        .await
        .unwrap();
        insert_comic_inventory_file(
            &db,
            root_id,
            generation,
            "Author/book.cbz",
            i64::try_from(std::fs::metadata(&archive).unwrap().len()).unwrap(),
        )
        .await;

        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let mutation = comic::inspect(
            &resources,
            ComicInspectionRequest {
                root_id,
                root_generation: generation,
                scan_token: "legacy-comic-scan".to_string(),
                provider: "local".to_string(),
                qmediasync_mount_name: None,
                root: root.clone(),
                work_key: "Author/book.cbz".to_string(),
                previous_work_key: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            mutation.work.description.as_deref(),
            Some("Alternate title")
        );
        assert!(db
            .try_acquire_scanner_lock("library", "legacy-comic-scan", 60)
            .await
            .unwrap());
        let legacy_fingerprint = mutation
            .fingerprint
            .strip_prefix(COMIC_FINGERPRINT_PREFIX)
            .unwrap();
        let work_id = db
            .upsert_scanner_work(
                COMIC_KIND,
                &mutation.work.title,
                Some(&mutation.work.source_path),
                mutation.work.category.as_deref(),
                mutation.work.description.as_deref(),
                mutation.work.rating,
                mutation.work.meta.clone(),
                "legacy-comic-scan",
                legacy_fingerprint,
            )
            .await
            .unwrap();
        for asset in &mutation.assets {
            let mut meta = asset.meta.as_object().unwrap().clone();
            meta.insert(
                "_source_version".to_string(),
                Value::String(asset.source_version.clone()),
            );
            db.upsert_scanner_asset(
                work_id,
                &asset.path,
                &asset.mime,
                &asset.role,
                asset.variant.as_deref(),
                asset.position,
                asset.size,
                Value::Object(meta),
                "legacy-comic-scan",
            )
            .await
            .unwrap();
        }
        for tag in &mutation.tags {
            db.upsert_and_link_scanner_tag(
                work_id,
                &tag.namespace,
                &tag.key,
                &tag.label,
                tag.translated_label.as_deref(),
                tag.translated_namespace.as_deref(),
                &tag.source,
                tag.intro.as_deref(),
                tag.links.as_deref(),
                "legacy-comic-scan",
            )
            .await
            .unwrap();
        }
        db.finish_scanner_work(
            work_id,
            &format!("comic|{}", path_string(&root)),
            "legacy-comic-scan",
            legacy_fingerprint,
        )
        .await
        .unwrap();
        db.release_scanner_lock("library", "legacy-comic-scan")
            .await
            .unwrap();
        ComicFixture {
            _temp: temp,
            db,
            resources,
            root,
            archive,
            root_id,
            work_id,
        }
    }

    async fn setup_legacy_coser_picture_fixture() -> CoserPictureFixture {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("coser-picture");
        let author = root.join("Alice");
        std::fs::create_dir_all(&author).unwrap();
        let archive = author.join("set.zip");
        write_comic_archive(&archive);

        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let generation = 8_i64;
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots (
                kind, provider, root, scan_depth, generation,
                completed_generation, status, present_files
            )
            VALUES ('coser-picture', 'local', ?1, 2, ?2, ?2, 'idle', 1)
            RETURNING id
            "#,
        )
        .bind(path_string(&root))
        .bind(generation)
        .fetch_one(db.pool())
        .await
        .unwrap();
        insert_coser_picture_inventory_file(
            &db,
            root_id,
            generation,
            "Alice/set.zip",
            i64::try_from(std::fs::metadata(&archive).unwrap().len()).unwrap(),
        )
        .await;

        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let mutation = coser_picture::inspect(
            &resources,
            CoserPictureInspectionRequest {
                root_id,
                root_generation: generation,
                scan_token: "legacy-coser-picture-scan".to_string(),
                provider: "local".to_string(),
                qmediasync_mount_name: None,
                root: root.clone(),
                work_key: "Alice/set.zip".to_string(),
                previous_work_key: None,
            },
        )
        .await
        .unwrap();
        assert!(db
            .try_acquire_scanner_lock("library", "legacy-coser-picture-scan", 60)
            .await
            .unwrap());
        let legacy_fingerprint = mutation
            .fingerprint
            .strip_prefix(COSER_PICTURE_FINGERPRINT_PREFIX)
            .unwrap();
        let work_id = db
            .upsert_scanner_work(
                COSER_PICTURE_KIND,
                &mutation.work.title,
                Some(&mutation.work.source_path),
                mutation.work.category.as_deref(),
                mutation.work.description.as_deref(),
                mutation.work.rating,
                mutation.work.meta.clone(),
                "legacy-coser-picture-scan",
                legacy_fingerprint,
            )
            .await
            .unwrap();
        for asset in &mutation.assets {
            let mut meta = asset.meta.as_object().unwrap().clone();
            meta.insert(
                "_source_version".to_string(),
                Value::String(asset.source_version.clone()),
            );
            db.upsert_scanner_asset(
                work_id,
                &asset.path,
                &asset.mime,
                &asset.role,
                asset.variant.as_deref(),
                asset.position,
                asset.size,
                Value::Object(meta),
                "legacy-coser-picture-scan",
            )
            .await
            .unwrap();
        }
        for tag in &mutation.tags {
            db.upsert_and_link_scanner_tag(
                work_id,
                &tag.namespace,
                &tag.key,
                &tag.label,
                tag.translated_label.as_deref(),
                tag.translated_namespace.as_deref(),
                &tag.source,
                tag.intro.as_deref(),
                tag.links.as_deref(),
                "legacy-coser-picture-scan",
            )
            .await
            .unwrap();
        }
        db.finish_scanner_work(
            work_id,
            &format!("coser-picture|{}", path_string(&root)),
            "legacy-coser-picture-scan",
            legacy_fingerprint,
        )
        .await
        .unwrap();
        db.release_scanner_lock("library", "legacy-coser-picture-scan")
            .await
            .unwrap();
        CoserPictureFixture {
            _temp: temp,
            db,
            resources,
            root,
            archive,
            root_id,
            work_id,
        }
    }

    async fn setup_legacy_audio_fixture() -> AudioFixture {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("audio");
        let work_root = root.join("RJ123456");
        std::fs::create_dir_all(&work_root).unwrap();
        let track = work_root.join("01.mp3");
        let cover = work_root.join("cover.jpg");
        std::fs::write(&track, b"not-real-mp3").unwrap();
        std::fs::write(&cover, b"cover").unwrap();

        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let generation = 9_i64;
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots (
                kind, provider, root, generation, completed_generation,
                status, present_files
            )
            VALUES ('audio', 'local', ?1, ?2, ?2, 'idle', 2)
            RETURNING id
            "#,
        )
        .bind(path_string(&root))
        .bind(generation)
        .fetch_one(db.pool())
        .await
        .unwrap();
        insert_audio_inventory_file(
            &db,
            root_id,
            generation,
            "RJ123456/01.mp3",
            "RJ123456",
            i64::try_from(std::fs::metadata(&track).unwrap().len()).unwrap(),
        )
        .await;
        insert_audio_inventory_file(
            &db,
            root_id,
            generation,
            "RJ123456/cover.jpg",
            "RJ123456",
            i64::try_from(std::fs::metadata(&cover).unwrap().len()).unwrap(),
        )
        .await;

        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let mut stream = audio::inspect(
            &resources,
            AudioInspectionRequest {
                root_id,
                root_generation: generation,
                scan_token: "legacy-audio-scan".to_string(),
                provider: "local".to_string(),
                qmediasync_mount_name: None,
                root: root.clone(),
                work_key: "RJ123456".to_string(),
                previous_work_key: None,
                relative_paths: vec![
                    "RJ123456/01.mp3".to_string(),
                    "RJ123456/cover.jpg".to_string(),
                ],
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
        let work_id = persist_legacy_mutation_stream(
            &db,
            AUDIO_KIND,
            &root,
            AUDIO_FINGERPRINT_PREFIX,
            "legacy-audio-scan",
            &mutations,
        )
        .await;
        AudioFixture {
            _temp: temp,
            db,
            resources,
            track,
            root_id,
            work_id,
        }
    }

    async fn setup_legacy_gallery_fixture() -> GalleryFixture {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("gallery");
        let work_root = root.join("Artist").join("Set");
        std::fs::create_dir_all(&work_root).unwrap();
        let image = work_root.join("001.jpg");
        let cover = work_root.join("cover.jpg");
        std::fs::write(&image, [0xff_u8, 0xd8, 0xff, 0xd9]).unwrap();
        std::fs::write(&cover, [0xff_u8, 0xd8, 0xff, 0xd9]).unwrap();

        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let generation = 10_i64;
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots (
                kind, provider, root, generation, completed_generation,
                status, present_files
            )
            VALUES ('gallery', 'local', ?1, ?2, ?2, 'idle', 2)
            RETURNING id
            "#,
        )
        .bind(path_string(&root))
        .bind(generation)
        .fetch_one(db.pool())
        .await
        .unwrap();
        insert_gallery_inventory_file(
            &db,
            root_id,
            generation,
            "Artist/Set/001.jpg",
            "Artist/Set",
            i64::try_from(std::fs::metadata(&image).unwrap().len()).unwrap(),
        )
        .await;
        insert_gallery_inventory_file(
            &db,
            root_id,
            generation,
            "Artist/Set/cover.jpg",
            "Artist/Set",
            i64::try_from(std::fs::metadata(&cover).unwrap().len()).unwrap(),
        )
        .await;

        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let mut stream = gallery::inspect(
            &resources,
            GalleryInspectionRequest {
                root_id,
                root_generation: generation,
                scan_token: "legacy-gallery-scan".to_string(),
                provider: "local".to_string(),
                root: root.clone(),
                work_key: "Artist/Set".to_string(),
                previous_work_key: None,
                assets: vec![
                    GalleryInventoryAsset {
                        relative_path: "Artist/Set/001.jpg".to_string(),
                        size: i64::try_from(std::fs::metadata(&image).unwrap().len()).unwrap(),
                        source_version: "Artist/Set/001.jpg".to_string(),
                    },
                    GalleryInventoryAsset {
                        relative_path: "Artist/Set/cover.jpg".to_string(),
                        size: i64::try_from(std::fs::metadata(&cover).unwrap().len()).unwrap(),
                        source_version: "Artist/Set/cover.jpg".to_string(),
                    },
                ],
            },
        )
        .await
        .unwrap();
        let mut mutations = Vec::new();
        while let Some(mutation) = stream.next().await {
            mutations.push(mutation);
        }
        stream.finish().await.unwrap();
        let work_id = persist_legacy_mutation_stream(
            &db,
            GALLERY_KIND,
            &root,
            GALLERY_FINGERPRINT_PREFIX,
            "legacy-gallery-scan",
            &mutations,
        )
        .await;
        GalleryFixture {
            _temp: temp,
            db,
            resources,
            image,
            root_id,
            work_id,
        }
    }

    #[tokio::test]
    async fn novel_reconciliation_matches_legacy_facts_and_normalizes_cover_paths() {
        let fixture = setup_legacy_fixture().await;
        let first = reconcile_novel_with(&fixture.db, &fixture.resources, &fixture.generated_dir)
            .await
            .unwrap();
        assert_eq!(first.status, "passed");
        assert!(first.current);
        assert_eq!(first.expected_works, 1);
        assert_eq!(first.matched_works, 1);
        assert_eq!(first.consecutive_passes, 1);

        let second = reconcile_novel_with(&fixture.db, &fixture.resources, &fixture.generated_dir)
            .await
            .unwrap();
        assert_eq!(second.status, "passed");
        assert_eq!(second.consecutive_passes, 2);
        assert!(overview(&fixture.db).await.unwrap().diffs.is_empty());

        let promoted = fixture
            .db
            .change_catalog_kind_ownership(NOVEL_KIND, "catalog-v2", Some("reconciled fixture"))
            .await
            .unwrap();
        assert!(promoted.changed);
    }

    #[tokio::test]
    async fn reconciliation_overview_uses_one_tracked_read_snapshot() {
        let fixture = setup_legacy_fixture().await;
        let before = fixture.db.runtime_snapshot().await.read_snapshot;

        let overview = overview(&fixture.db).await.unwrap();

        let after = fixture.db.runtime_snapshot().await.read_snapshot;
        assert_eq!(after.samples, before.samples + 1);
        assert_eq!(after.completed, before.completed + 1);
        assert_eq!(after.implicit_rollbacks, before.implicit_rollbacks);
        assert_eq!(after.active, 0);
        assert!(overview.items.iter().any(|item| item.kind == NOVEL_KIND));
    }

    #[tokio::test]
    async fn reconciliation_baseline_uses_one_tracked_read_snapshot() {
        let fixture = setup_legacy_fixture().await;
        let before = fixture.db.runtime_snapshot().await.read_snapshot;

        let baseline = reconciliation_baseline(&fixture.db, NOVEL_KIND)
            .await
            .unwrap();

        let after = fixture.db.runtime_snapshot().await.read_snapshot;
        assert_eq!(after.samples, before.samples + 1);
        assert_eq!(after.completed, before.completed + 1);
        assert_eq!(after.implicit_rollbacks, before.implicit_rollbacks);
        assert_eq!(after.active, 0);
        assert_eq!(baseline.roots.root_count(), 1);
        assert_eq!(baseline.roots.ready_roots, 1);
        assert!(baseline.previous.consecutive_passes >= 0);
        assert!(baseline.prerequisite_error.is_none());
    }

    #[tokio::test]
    async fn reconciliation_ignores_inventory_rows_from_an_older_generation() {
        let fixture = setup_legacy_fixture().await;
        sqlx::query(
            "UPDATE library_roots SET generation = 2, completed_generation = 2 WHERE id = ?1",
        )
        .bind(fixture.root_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE file_inventory SET seen_generation = 1 WHERE root_id = ?1 AND relative_path = 'book.epub'",
        )
        .bind(fixture.root_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();

        let status = reconcile_novel_with(&fixture.db, &fixture.resources, &fixture.generated_dir)
            .await
            .unwrap();
        assert_eq!(status.status, "failed");
        assert_eq!(status.expected_works, 0);
        assert_eq!(status.unexpected_works, 1);
        assert_eq!(status.error_works, 0);
    }

    #[tokio::test]
    async fn catalog_fact_comparison_uses_one_tracked_read_snapshot_per_page() {
        let fixture = setup_legacy_fixture().await;
        let roots = root_snapshot(&fixture.db, NOVEL_KIND).await.unwrap();
        let target = ReconciliationTarget::Novel {
            generated_dir: &fixture.generated_dir,
        };
        let candidate = target
            .inspect(
                &fixture.db,
                &fixture.resources,
                &roots.roots[0],
                "catalog-fact-comparison-test",
                "book.epub".to_string(),
            )
            .await
            .unwrap();
        let candidates = [candidate];
        let candidate_refs = candidates.iter().collect::<Vec<_>>();
        let before = fixture.db.runtime_snapshot().await.read_snapshot;

        let comparisons =
            compare_catalog_works(&fixture.db, &candidate_refs, NOVEL_FINGERPRINT_PREFIX)
                .await
                .unwrap();

        let after = fixture.db.runtime_snapshot().await.read_snapshot;
        assert_eq!(comparisons.len(), 1);
        assert!(matches!(comparisons[0], WorkComparison::Matched));
        assert_eq!(after.samples, before.samples + 1);
        assert_eq!(after.completed, before.completed + 1);
        assert_eq!(after.active, 0);
        assert_eq!(after.implicit_rollbacks, before.implicit_rollbacks);
    }

    #[tokio::test]
    async fn audio_and_gallery_inventory_loads_commit_tracked_read_snapshots() {
        let audio = setup_legacy_audio_fixture().await;
        let audio_before = audio.db.runtime_snapshot().await.read_snapshot;
        let audio_paths = load_audio_inventory_paths(&audio.db, audio.root_id, 9, "RJ123456")
            .await
            .unwrap();
        let audio_after = audio.db.runtime_snapshot().await.read_snapshot;
        assert_eq!(audio_paths, vec!["RJ123456/01.mp3", "RJ123456/cover.jpg"]);
        assert_eq!(audio_after.samples, audio_before.samples + 1);
        assert_eq!(audio_after.completed, audio_before.completed + 1);
        assert_eq!(audio_after.active, 0);
        assert_eq!(
            audio_after.implicit_rollbacks,
            audio_before.implicit_rollbacks
        );

        let gallery = setup_legacy_gallery_fixture().await;
        let gallery_before = gallery.db.runtime_snapshot().await.read_snapshot;
        let gallery_assets =
            load_gallery_inventory_assets(&gallery.db, gallery.root_id, 10, "Artist/Set")
                .await
                .unwrap();
        let gallery_after = gallery.db.runtime_snapshot().await.read_snapshot;
        assert_eq!(gallery_assets.len(), 2);
        assert_eq!(gallery_assets[0].relative_path, "Artist/Set/001.jpg");
        assert_eq!(gallery_assets[1].relative_path, "Artist/Set/cover.jpg");
        assert_eq!(gallery_after.samples, gallery_before.samples + 1);
        assert_eq!(gallery_after.completed, gallery_before.completed + 1);
        assert_eq!(gallery_after.active, 0);
        assert_eq!(
            gallery_after.implicit_rollbacks,
            gallery_before.implicit_rollbacks
        );
    }

    #[tokio::test]
    async fn unexpected_work_check_uses_one_tracked_snapshot_for_all_roots() {
        let fixture = setup_legacy_fixture().await;
        let roots = root_snapshot(&fixture.db, NOVEL_KIND).await.unwrap();
        let target = ReconciliationTarget::Novel {
            generated_dir: &fixture.generated_dir,
        };
        let mut counts = ReconciliationCounts::default();
        let mut diffs = Vec::new();
        let before = fixture.db.runtime_snapshot().await.read_snapshot;

        record_unexpected_works(&fixture.db, &target, &roots, &mut counts, &mut diffs)
            .await
            .unwrap();

        let after = fixture.db.runtime_snapshot().await.read_snapshot;
        assert_eq!(counts.unexpected, 0);
        assert!(diffs.is_empty());
        assert_eq!(after.samples, before.samples + 1);
        assert_eq!(after.completed, before.completed + 1);
        assert_eq!(after.active, 0);
        assert_eq!(after.implicit_rollbacks, before.implicit_rollbacks);
    }

    #[tokio::test]
    async fn audio_unexpected_work_check_accepts_nested_rj_legacy_source_path() {
        let fixture = setup_legacy_audio_fixture().await;
        let root_path: String = sqlx::query_scalar("SELECT root FROM library_roots WHERE id = ?1")
            .bind(fixture.root_id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE works SET source_path = ?1 WHERE id = ?2")
            .bind(format!("{root_path}/RJ123456/01-product"))
            .bind(fixture.work_id)
            .execute(fixture.db.pool())
            .await
            .unwrap();

        let roots = root_snapshot(&fixture.db, AUDIO_KIND).await.unwrap();
        let target = ReconciliationTarget::Audio;
        let mut counts = ReconciliationCounts::default();
        let mut diffs = Vec::new();

        record_unexpected_works(&fixture.db, &target, &roots, &mut counts, &mut diffs)
            .await
            .unwrap();

        assert_eq!(counts.unexpected, 0);
        assert!(diffs.is_empty());
    }

    #[tokio::test]
    async fn reconciliation_keyset_paginates_past_one_work_key_page() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("novels");
        let generated_dir = temp.path().join("generated");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&generated_dir).unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let generation = 1_i64;
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots (
                kind, provider, root, generation, completed_generation,
                status, present_files
            )
            VALUES ('novel', 'local', ?1, ?2, ?2, 'idle', ?3)
            RETURNING id
            "#,
        )
        .bind(path_string(&root))
        .bind(generation)
        .bind(257_i64)
        .fetch_one(db.pool())
        .await
        .unwrap();

        for index in 0..257_i64 {
            let relative_path = format!("book-{index:03}.epub");
            let epub = root.join(&relative_path);
            write_epub(&epub, &format!("Fixture Novel {index}"));
            insert_inventory_file(
                &db,
                root_id,
                generation,
                &relative_path,
                i64::try_from(std::fs::metadata(&epub).unwrap().len()).unwrap(),
            )
            .await;
        }

        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let status = reconcile_novel_with(&db, &resources, &generated_dir)
            .await
            .unwrap();
        assert_eq!(status.status, "failed");
        assert_eq!(status.expected_works, 257);
        assert_eq!(status.missing_works, 257);
        assert_eq!(status.error_works, 0);
    }

    #[tokio::test]
    async fn novel_reconciliation_reports_work_tag_and_asset_drift() {
        let fixture = setup_legacy_fixture().await;
        sqlx::query("UPDATE works SET title = 'Drifted title' WHERE id = ?1")
            .bind(fixture.work_id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
        sqlx::query(
            "UPDATE tags SET label = 'Drifted label' WHERE id IN (SELECT tag_id FROM work_tags WHERE work_id = ?1)",
        )
        .bind(fixture.work_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE assets SET size = size + 1 WHERE work_id = ?1 AND role = 'book'")
            .bind(fixture.work_id)
            .execute(fixture.db.pool())
            .await
            .unwrap();

        let status = reconcile_novel_with(&fixture.db, &fixture.resources, &fixture.generated_dir)
            .await
            .unwrap();
        assert_eq!(status.status, "failed");
        assert_eq!(status.mismatch_works, 1);
        assert_eq!(status.matched_works, 0);
        let overview = overview(&fixture.db).await.unwrap();
        let fields = overview.diffs[0]
            .details
            .get("fields")
            .and_then(Value::as_array)
            .unwrap();
        assert!(fields.iter().any(|field| field == "work.title"));
        assert!(fields.iter().any(|field| field
            .as_str()
            .is_some_and(|value| value.starts_with("scanner_assets"))));
        assert!(fields.iter().any(|field| field
            .as_str()
            .is_some_and(|value| value.starts_with("scanner_tags"))));
    }

    #[tokio::test]
    async fn novel_reconciliation_reports_missing_and_unexpected_works() {
        let fixture = setup_legacy_fixture().await;
        let missing_epub = fixture.root.join("missing.epub");
        write_epub(&missing_epub, "Missing Novel");
        insert_inventory_file(
            &fixture.db,
            fixture.root_id,
            4,
            "missing.epub",
            i64::try_from(std::fs::metadata(&missing_epub).unwrap().len()).unwrap(),
        )
        .await;
        sqlx::query(
            "UPDATE file_inventory SET status = 'missing' WHERE root_id = ?1 AND relative_path = 'book.epub'",
        )
        .bind(fixture.root_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();

        let status = reconcile_novel_with(&fixture.db, &fixture.resources, &fixture.generated_dir)
            .await
            .unwrap();
        assert_eq!(status.status, "failed");
        assert_eq!(status.expected_works, 1);
        assert_eq!(status.missing_works, 1);
        assert_eq!(status.unexpected_works, 1);
        let kinds = overview(&fixture.db)
            .await
            .unwrap()
            .diffs
            .into_iter()
            .map(|diff| diff.difference_kind)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            kinds,
            BTreeSet::from(["missing".to_string(), "unexpected".to_string()])
        );
    }

    #[tokio::test]
    async fn unreadable_novel_records_error_without_mutating_legacy_history() {
        let fixture = setup_legacy_fixture().await;
        sqlx::query(
            "INSERT INTO reading_history(work_id, progress, position) VALUES (?1, 0.5, 'chapter-2')",
        )
        .bind(fixture.work_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
        std::fs::write(fixture.root.join("book.epub"), b"incomplete epub").unwrap();

        let status = reconcile_novel_with(&fixture.db, &fixture.resources, &fixture.generated_dir)
            .await
            .unwrap();
        assert_eq!(status.status, "error");
        assert_eq!(status.error_works, 1);
        assert_eq!(
            sqlx::query_as::<_, (String, f64, Option<String>)>(
                r#"
                SELECT work.title, history.progress, history.position
                FROM works AS work
                JOIN reading_history AS history ON history.work_id = work.id
                WHERE work.id = ?1
                "#,
            )
            .bind(fixture.work_id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap(),
            (
                "Fixture Novel".to_string(),
                0.5,
                Some("chapter-2".to_string())
            )
        );
    }

    #[tokio::test]
    async fn comic_reconciliation_matches_legacy_facts_and_allows_promotion() {
        let fixture = setup_legacy_comic_fixture().await;
        let error = fixture
            .db
            .change_catalog_kind_ownership(COMIC_KIND, "catalog-v2", Some("missing comic evidence"))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("current passing legacy/Catalog v2 reconciliation"));
        let first = reconcile_comic_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(first.status, "passed");
        assert!(first.current);
        assert_eq!(first.expected_works, 1);
        assert_eq!(first.matched_works, 1);
        assert_eq!(first.consecutive_passes, 1);

        let second = reconcile_comic_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(second.status, "passed");
        assert_eq!(second.consecutive_passes, 2);
        assert!(overview(&fixture.db).await.unwrap().diffs.is_empty());

        let promoted = fixture
            .db
            .change_catalog_kind_ownership(COMIC_KIND, "catalog-v2", Some("reconciled comic"))
            .await
            .unwrap();
        assert!(promoted.changed);
    }

    #[tokio::test]
    async fn comic_reconciliation_reports_work_tag_and_asset_drift() {
        let fixture = setup_legacy_comic_fixture().await;
        sqlx::query("UPDATE works SET description = 'Drifted description' WHERE id = ?1")
            .bind(fixture.work_id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
        sqlx::query(
            "UPDATE tags SET label = 'Drifted label' WHERE id IN (SELECT tag_id FROM work_tags WHERE work_id = ?1)",
        )
        .bind(fixture.work_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE assets SET size = size + 1 WHERE work_id = ?1 AND role = 'archive'")
            .bind(fixture.work_id)
            .execute(fixture.db.pool())
            .await
            .unwrap();

        let status = reconcile_comic_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(status.status, "failed");
        assert_eq!(status.mismatch_works, 1);
        let overview = overview(&fixture.db).await.unwrap();
        let fields = overview
            .diffs
            .iter()
            .find(|diff| diff.kind == COMIC_KIND)
            .unwrap()
            .details
            .get("fields")
            .and_then(Value::as_array)
            .unwrap();
        assert!(fields.iter().any(|field| field == "work.description"));
        assert!(fields.iter().any(|field| field
            .as_str()
            .is_some_and(|value| value.starts_with("scanner_assets"))));
        assert!(fields.iter().any(|field| field
            .as_str()
            .is_some_and(|value| value.starts_with("scanner_tags"))));
    }

    #[tokio::test]
    async fn comic_reconciliation_reports_missing_and_unexpected_archives() {
        let fixture = setup_legacy_comic_fixture().await;
        let missing_archive = fixture.root.join("Author").join("missing.zip");
        write_comic_archive(&missing_archive);
        insert_comic_inventory_file(
            &fixture.db,
            fixture.root_id,
            7,
            "Author/missing.zip",
            i64::try_from(std::fs::metadata(&missing_archive).unwrap().len()).unwrap(),
        )
        .await;
        sqlx::query(
            "UPDATE file_inventory SET status = 'missing' WHERE root_id = ?1 AND relative_path = 'Author/book.cbz'",
        )
        .bind(fixture.root_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();

        let status = reconcile_comic_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(status.status, "failed");
        assert_eq!(status.expected_works, 1);
        assert_eq!(status.missing_works, 1);
        assert_eq!(status.unexpected_works, 1);
        let kinds = overview(&fixture.db)
            .await
            .unwrap()
            .diffs
            .into_iter()
            .filter(|diff| diff.kind == COMIC_KIND)
            .map(|diff| diff.difference_kind)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            kinds,
            BTreeSet::from(["missing".to_string(), "unexpected".to_string()])
        );
    }

    #[tokio::test]
    async fn unreadable_comic_records_error_without_mutating_legacy_history() {
        let fixture = setup_legacy_comic_fixture().await;
        sqlx::query(
            "INSERT INTO reading_history(work_id, progress, position) VALUES (?1, 0.75, 'page-7')",
        )
        .bind(fixture.work_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
        std::fs::write(&fixture.archive, b"incomplete comic archive").unwrap();

        let status = reconcile_comic_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(status.status, "error");
        assert_eq!(status.error_works, 1);
        assert_eq!(
            sqlx::query_as::<_, (String, f64, Option<String>)>(
                r#"
                SELECT work.title, history.progress, history.position
                FROM works AS work
                JOIN reading_history AS history ON history.work_id = work.id
                WHERE work.id = ?1
                "#,
            )
            .bind(fixture.work_id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap(),
            (
                "Fixture Comic".to_string(),
                0.75,
                Some("page-7".to_string())
            )
        );
    }

    #[tokio::test]
    async fn comic_reconciliation_evidence_stales_after_a_root_event() {
        let fixture = setup_legacy_comic_fixture().await;
        let passed = reconcile_comic_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(passed.status, "passed");
        sqlx::query("UPDATE library_roots SET last_event_seq = 99 WHERE id = ?1")
            .bind(fixture.root_id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
        let current = overview(&fixture.db)
            .await
            .unwrap()
            .items
            .into_iter()
            .find(|item| item.kind == COMIC_KIND)
            .unwrap();
        assert_eq!(current.status, "stale");
        assert!(!current.current);
        let error = fixture
            .db
            .change_catalog_kind_ownership(COMIC_KIND, "catalog-v2", Some("stale comic"))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("current passing legacy/Catalog v2 reconciliation"));
    }

    #[tokio::test]
    async fn coser_picture_reconciliation_matches_legacy_facts_and_allows_promotion() {
        let fixture = setup_legacy_coser_picture_fixture().await;
        let error = fixture
            .db
            .change_catalog_kind_ownership(
                COSER_PICTURE_KIND,
                "catalog-v2",
                Some("missing CoserPicture evidence"),
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("current passing legacy/Catalog v2 reconciliation"));

        let first = reconcile_coser_picture_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(first.status, "passed");
        assert!(first.current);
        assert_eq!(first.expected_works, 1);
        assert_eq!(first.matched_works, 1);
        assert_eq!(first.consecutive_passes, 1);

        let second = reconcile_coser_picture_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(second.status, "passed");
        assert_eq!(second.consecutive_passes, 2);
        assert!(overview(&fixture.db)
            .await
            .unwrap()
            .diffs
            .into_iter()
            .all(|diff| diff.kind != COSER_PICTURE_KIND));

        let promoted = fixture
            .db
            .change_catalog_kind_ownership(
                COSER_PICTURE_KIND,
                "catalog-v2",
                Some("reconciled CoserPicture"),
            )
            .await
            .unwrap();
        assert!(promoted.changed);
    }

    #[tokio::test]
    async fn coser_picture_reconciliation_reports_work_tag_and_asset_drift() {
        let fixture = setup_legacy_coser_picture_fixture().await;
        sqlx::query("UPDATE works SET title = 'Drifted title' WHERE id = ?1")
            .bind(fixture.work_id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
        sqlx::query(
            "UPDATE tags SET label = 'Drifted label' WHERE id IN (SELECT tag_id FROM work_tags WHERE work_id = ?1)",
        )
        .bind(fixture.work_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE assets SET size = size + 1 WHERE work_id = ?1 AND role = 'archive'")
            .bind(fixture.work_id)
            .execute(fixture.db.pool())
            .await
            .unwrap();

        let status = reconcile_coser_picture_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(status.status, "failed");
        assert_eq!(status.mismatch_works, 1);
        let overview = overview(&fixture.db).await.unwrap();
        let fields = overview
            .diffs
            .iter()
            .find(|diff| diff.kind == COSER_PICTURE_KIND)
            .unwrap()
            .details
            .get("fields")
            .and_then(Value::as_array)
            .unwrap();
        assert!(fields.iter().any(|field| field == "work.title"));
        assert!(fields.iter().any(|field| field
            .as_str()
            .is_some_and(|value| value.starts_with("scanner_assets"))));
        assert!(fields.iter().any(|field| field
            .as_str()
            .is_some_and(|value| value.starts_with("scanner_tags"))));
    }

    #[tokio::test]
    async fn coser_picture_reconciliation_reports_missing_and_unexpected_archives() {
        let fixture = setup_legacy_coser_picture_fixture().await;
        let missing_archive = fixture.root.join("Alice").join("missing.zip");
        write_comic_archive(&missing_archive);
        insert_coser_picture_inventory_file(
            &fixture.db,
            fixture.root_id,
            8,
            "Alice/missing.zip",
            i64::try_from(std::fs::metadata(&missing_archive).unwrap().len()).unwrap(),
        )
        .await;
        sqlx::query(
            "UPDATE file_inventory SET status = 'missing' WHERE root_id = ?1 AND relative_path = 'Alice/set.zip'",
        )
        .bind(fixture.root_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();

        let status = reconcile_coser_picture_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(status.status, "failed");
        assert_eq!(status.expected_works, 1);
        assert_eq!(status.missing_works, 1);
        assert_eq!(status.unexpected_works, 1);
        let kinds = overview(&fixture.db)
            .await
            .unwrap()
            .diffs
            .into_iter()
            .filter(|diff| diff.kind == COSER_PICTURE_KIND)
            .map(|diff| diff.difference_kind)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            kinds,
            BTreeSet::from(["missing".to_string(), "unexpected".to_string()])
        );
    }

    #[tokio::test]
    async fn unreadable_coser_picture_records_error_without_mutating_legacy_history() {
        let fixture = setup_legacy_coser_picture_fixture().await;
        sqlx::query(
            "INSERT INTO reading_history(work_id, progress, position) VALUES (?1, 0.5, 'page-2')",
        )
        .bind(fixture.work_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
        std::fs::write(&fixture.archive, b"incomplete CoserPicture archive").unwrap();

        let status = reconcile_coser_picture_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(status.status, "error");
        assert_eq!(status.error_works, 1);
        assert_eq!(
            sqlx::query_as::<_, (String, f64, Option<String>)>(
                r#"
                SELECT work.title, history.progress, history.position
                FROM works AS work
                JOIN reading_history AS history ON history.work_id = work.id
                WHERE work.id = ?1
                "#,
            )
            .bind(fixture.work_id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap(),
            ("set".to_string(), 0.5, Some("page-2".to_string()))
        );
    }

    #[tokio::test]
    async fn coser_picture_reconciliation_evidence_stales_after_a_root_event() {
        let fixture = setup_legacy_coser_picture_fixture().await;
        let passed = reconcile_coser_picture_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(passed.status, "passed");
        sqlx::query("UPDATE library_roots SET last_event_seq = 99 WHERE id = ?1")
            .bind(fixture.root_id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
        let current = overview(&fixture.db)
            .await
            .unwrap()
            .items
            .into_iter()
            .find(|item| item.kind == COSER_PICTURE_KIND)
            .unwrap();
        assert_eq!(current.status, "stale");
        assert!(!current.current);
        let error = fixture
            .db
            .change_catalog_kind_ownership(
                COSER_PICTURE_KIND,
                "catalog-v2",
                Some("stale CoserPicture"),
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("current passing legacy/Catalog v2 reconciliation"));
    }

    #[tokio::test]
    async fn audio_reconciliation_matches_streamed_legacy_facts_and_allows_promotion() {
        let fixture = setup_legacy_audio_fixture().await;
        let error = fixture
            .db
            .change_catalog_kind_ownership(AUDIO_KIND, "catalog-v2", Some("missing audio evidence"))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("current passing legacy/Catalog v2 reconciliation"));

        let first = reconcile_audio_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(first.status, "passed");
        assert!(first.current);
        assert_eq!(first.expected_works, 1);
        assert_eq!(first.matched_works, 1);
        assert_eq!(first.consecutive_passes, 1);

        let second = reconcile_audio_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(second.status, "passed");
        assert_eq!(second.consecutive_passes, 2);
        assert!(overview(&fixture.db)
            .await
            .unwrap()
            .diffs
            .into_iter()
            .all(|diff| diff.kind != AUDIO_KIND));

        let promoted = fixture
            .db
            .change_catalog_kind_ownership(AUDIO_KIND, "catalog-v2", Some("reconciled audio"))
            .await
            .unwrap();
        assert!(promoted.changed);
    }

    #[tokio::test]
    async fn audio_reconciliation_stales_after_a_root_event_and_preserves_history_on_failure() {
        let fixture = setup_legacy_audio_fixture().await;
        let passed = reconcile_audio_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(passed.status, "passed");
        sqlx::query("UPDATE library_roots SET last_event_seq = 99 WHERE id = ?1")
            .bind(fixture.root_id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
        let current = overview(&fixture.db)
            .await
            .unwrap()
            .items
            .into_iter()
            .find(|item| item.kind == AUDIO_KIND)
            .unwrap();
        assert_eq!(current.status, "stale");
        assert!(!current.current);

        sqlx::query(
            "INSERT INTO reading_history(work_id, progress, position) VALUES (?1, 0.5, 'track:0')",
        )
        .bind(fixture.work_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
        std::fs::remove_file(&fixture.track).unwrap();
        let error_status = reconcile_audio_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(error_status.status, "error");
        assert_eq!(error_status.error_works, 1);
        assert_eq!(
            sqlx::query_as::<_, (String, f64, Option<String>)>(
                r#"
                SELECT work.title, history.progress, history.position
                FROM works AS work
                JOIN reading_history AS history ON history.work_id = work.id
                WHERE work.id = ?1
                "#,
            )
            .bind(fixture.work_id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap()
            .1,
            0.5
        );
    }

    #[tokio::test]
    async fn gallery_reconciliation_matches_directory_snapshot_and_allows_promotion() {
        let fixture = setup_legacy_gallery_fixture().await;
        let error = fixture
            .db
            .change_catalog_kind_ownership(
                GALLERY_KIND,
                "catalog-v2",
                Some("missing gallery evidence"),
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("current passing legacy/Catalog v2 reconciliation"));

        let first = reconcile_gallery_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(first.status, "passed");
        assert!(first.current);
        assert_eq!(first.expected_works, 1);
        assert_eq!(first.matched_works, 1);
        assert_eq!(first.consecutive_passes, 1);

        // Catalog v2 writes the same scanner fingerprint with its writer
        // prefix into work metadata. Reconciliation must treat that as the
        // same fact as the unprefixed legacy scanner row.
        sqlx::query(
            "UPDATE works SET meta_json = json_set(meta_json, '$._scanner_fingerprint', 'gallery-v1:' || (SELECT fingerprint FROM scanner_works WHERE work_id = ?1)) WHERE id = ?1",
        )
        .bind(fixture.work_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
        let second = reconcile_gallery_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(second.status, "passed");
        assert_eq!(second.matched_works, 1);

        let promoted = fixture
            .db
            .change_catalog_kind_ownership(GALLERY_KIND, "catalog-v2", Some("reconciled gallery"))
            .await
            .unwrap();
        assert!(promoted.changed);
    }

    #[tokio::test]
    async fn gallery_reconciliation_reports_snapshot_drift_and_preserves_history_on_failure() {
        let fixture = setup_legacy_gallery_fixture().await;
        sqlx::query(
            "INSERT INTO reading_history(work_id, progress, position) VALUES (?1, 0.25, 'image:0')",
        )
        .bind(fixture.work_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
        std::fs::write(&fixture.image, b"changed image").unwrap();
        let status = reconcile_gallery_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(status.status, "passed");
        assert_eq!(status.matched_works, 1);
        sqlx::query("UPDATE file_inventory SET fast_fingerprint = 'changed' WHERE root_id = ?1 AND relative_path = 'Artist/Set/001.jpg'")
            .bind(fixture.root_id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
        let drifted = reconcile_gallery_with(&fixture.db, &fixture.resources)
            .await
            .unwrap();
        assert_eq!(drifted.status, "failed");
        assert_eq!(drifted.mismatch_works, 1);
        assert_eq!(
            sqlx::query_scalar::<_, f64>(
                "SELECT progress FROM reading_history WHERE work_id = ?1",
            )
            .bind(fixture.work_id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap(),
            0.25
        );
    }

    #[tokio::test]
    async fn catalog_reconciliation_jobs_are_unique_per_kind() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        for job_type in [
            RECONCILE_NOVEL_JOB_TYPE,
            RECONCILE_COMIC_JOB_TYPE,
            RECONCILE_COSER_PICTURE_JOB_TYPE,
            RECONCILE_AUDIO_JOB_TYPE,
            RECONCILE_GALLERY_JOB_TYPE,
        ] {
            let (first_id, first_created) = db
                .create_job_if_absent(job_type, "queued", json!({ "attempt": 1 }))
                .await
                .unwrap();
            let (second_id, second_created) = db
                .create_job_if_absent(job_type, "queued", json!({ "attempt": 2 }))
                .await
                .unwrap();
            assert!(first_created);
            assert!(!second_created);
            assert_eq!(first_id, second_id);
        }
    }
}
