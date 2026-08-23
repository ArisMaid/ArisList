use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::TryStreamExt;
use serde::Serialize;
use sqlx::Row;
use tokio::sync::Mutex;

use crate::db::Db;
use crate::error::{AppError, Result};
use crate::resource::{ResourceClass, ResourceGovernor};

pub(super) const FACET_BITMAP_MAX_WORKS: usize = 50_000;
pub(super) const FACET_BITMAP_MAX_TAGS: usize = 65_536;
pub(super) const FACET_BITMAP_MAX_KINDS: usize = 32;
pub(super) const FACET_BITMAP_MAX_ASSOCIATIONS: usize = 1_000_000;
pub(super) const FACET_BITMAP_MAX_BYTES: usize = 64 * 1024 * 1024;
const FACET_BITMAP_MAX_REFRESH_DELTAS: usize = 512;
const FACET_BITMAP_DENSE_THRESHOLD: usize = 1_024;
const FACET_BITMAP_BUILD_RESERVATION_BYTES: u64 = FACET_BITMAP_MAX_BYTES as u64;
const FACET_BITMAP_BUILD_RETRY: Duration = Duration::from_secs(60);
const FACET_BITMAP_WORDS: usize = FACET_BITMAP_MAX_WORKS.div_ceil(64);

#[derive(Debug)]
enum AdaptiveBitmap {
    Sparse(Vec<u32>),
    Dense(Vec<u64>),
}

impl Default for AdaptiveBitmap {
    fn default() -> Self {
        Self::Sparse(Vec::new())
    }
}

impl AdaptiveBitmap {
    fn insert(&mut self, slot: usize) {
        match self {
            Self::Sparse(slots) => {
                let slot = slot as u32;
                if let Err(index) = slots.binary_search(&slot) {
                    slots.insert(index, slot);
                }
                if slots.len() >= FACET_BITMAP_DENSE_THRESHOLD {
                    let mut words = vec![0_u64; FACET_BITMAP_WORDS];
                    for slot in slots.iter().copied() {
                        set_bit(&mut words, slot as usize);
                    }
                    *self = Self::Dense(words);
                }
            }
            Self::Dense(words) => set_bit(words, slot),
        }
    }

    fn remove(&mut self, slot: usize) {
        match self {
            Self::Sparse(slots) => {
                if let Ok(index) = slots.binary_search(&(slot as u32)) {
                    slots.remove(index);
                }
            }
            Self::Dense(words) => clear_bit(words, slot),
        }
    }

    fn intersect_into(&self, candidate: &mut Vec<u64>) {
        match self {
            Self::Sparse(slots) => {
                let mut intersection = vec![0_u64; FACET_BITMAP_WORDS];
                for slot in slots.iter().copied().map(|slot| slot as usize) {
                    if bit_is_set(candidate, slot) {
                        set_bit(&mut intersection, slot);
                    }
                }
                *candidate = intersection;
            }
            Self::Dense(words) => {
                for (candidate, tag) in candidate.iter_mut().zip(words) {
                    *candidate &= *tag;
                }
            }
        }
    }

    fn estimated_bytes(&self) -> usize {
        match self {
            Self::Sparse(slots) => slots.capacity() * size_of::<u32>(),
            Self::Dense(words) => words.capacity() * size_of::<u64>(),
        }
    }
}

#[derive(Debug)]
struct BitmapWork {
    kind_ordinal: u16,
    tag_ordinals: Vec<u32>,
}

#[derive(Debug)]
struct BitmapKind {
    name: String,
    members: Vec<u64>,
}

#[derive(Debug)]
struct BitmapTag {
    tag_id: i64,
    members: AdaptiveBitmap,
}

#[derive(Debug)]
struct FacetBitmapIndex {
    catalog_revision: i64,
    active_work_count: usize,
    association_count: usize,
    work_slots: HashMap<i64, usize>,
    works: Vec<Option<BitmapWork>>,
    free_slots: Vec<usize>,
    active_members: Vec<u64>,
    kind_ordinals: HashMap<String, u16>,
    kinds: Vec<BitmapKind>,
    tag_ordinals: HashMap<i64, u32>,
    tags: Vec<BitmapTag>,
}

impl FacetBitmapIndex {
    fn empty(catalog_revision: i64) -> Self {
        Self {
            catalog_revision,
            active_work_count: 0,
            association_count: 0,
            work_slots: HashMap::new(),
            works: Vec::new(),
            free_slots: Vec::new(),
            active_members: vec![0_u64; FACET_BITMAP_WORDS],
            kind_ordinals: HashMap::new(),
            kinds: Vec::new(),
            tag_ordinals: HashMap::new(),
            tags: Vec::new(),
        }
    }

    async fn build(db: &Db) -> Result<Self> {
        let mut transaction = db.begin_tracked_read_transaction().await?;
        let revision =
            sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
                .fetch_one(&mut *transaction)
                .await?;
        let mut index = Self::empty(revision);
        {
            let mut rows =
                sqlx::query("SELECT id, kind FROM works WHERE deleted_at IS NULL ORDER BY id")
                    .fetch(&mut *transaction);
            while let Some(row) = rows.try_next().await? {
                index.insert_initial_work(row.get("id"), row.get("kind"))?;
            }
        }
        {
            let mut rows =
                sqlx::query("SELECT work_id, tag_id FROM work_tags ORDER BY work_id, tag_id")
                    .fetch(&mut *transaction);
            while let Some(row) = rows.try_next().await? {
                index.insert_initial_association(row.get("work_id"), row.get("tag_id"))?;
            }
        }
        transaction.commit().await?;
        index.validate_capacity()?;
        Ok(index)
    }

    fn insert_initial_work(&mut self, work_id: i64, kind: String) -> Result<()> {
        if self.active_work_count >= FACET_BITMAP_MAX_WORKS {
            return Err(capacity_error("works", self.active_work_count + 1));
        }
        let kind_ordinal = self.kind_ordinal(&kind)?;
        let slot = self.works.len();
        self.works.push(Some(BitmapWork {
            kind_ordinal,
            tag_ordinals: Vec::new(),
        }));
        self.work_slots.insert(work_id, slot);
        set_bit(&mut self.active_members, slot);
        set_bit(&mut self.kinds[kind_ordinal as usize].members, slot);
        self.active_work_count += 1;
        Ok(())
    }

    fn insert_initial_association(&mut self, work_id: i64, tag_id: i64) -> Result<()> {
        if self.association_count >= FACET_BITMAP_MAX_ASSOCIATIONS {
            return Err(capacity_error("associations", self.association_count + 1));
        }
        let slot = *self.work_slots.get(&work_id).ok_or_else(|| {
            AppError::Other(format!(
                "Facet bitmap association references missing work {work_id}"
            ))
        })?;
        let tag_ordinal = self.tag_ordinal(tag_id)?;
        self.tags[tag_ordinal as usize].members.insert(slot);
        if let Some(work) = self.works[slot].as_mut() {
            work.tag_ordinals.push(tag_ordinal);
        }
        self.association_count += 1;
        Ok(())
    }

    fn kind_ordinal(&mut self, kind: &str) -> Result<u16> {
        if let Some(ordinal) = self.kind_ordinals.get(kind) {
            return Ok(*ordinal);
        }
        if self.kinds.len() >= FACET_BITMAP_MAX_KINDS {
            return Err(capacity_error("kinds", self.kinds.len() + 1));
        }
        let ordinal = self.kinds.len() as u16;
        self.kinds.push(BitmapKind {
            name: kind.to_string(),
            members: vec![0_u64; FACET_BITMAP_WORDS],
        });
        self.kind_ordinals.insert(kind.to_string(), ordinal);
        Ok(ordinal)
    }

    fn tag_ordinal(&mut self, tag_id: i64) -> Result<u32> {
        if let Some(ordinal) = self.tag_ordinals.get(&tag_id) {
            return Ok(*ordinal);
        }
        if self.tags.len() >= FACET_BITMAP_MAX_TAGS {
            return Err(capacity_error("tags", self.tags.len() + 1));
        }
        let ordinal = self.tags.len() as u32;
        self.tags.push(BitmapTag {
            tag_id,
            members: AdaptiveBitmap::default(),
        });
        self.tag_ordinals.insert(tag_id, ordinal);
        Ok(ordinal)
    }

    fn apply_work_snapshot(&mut self, snapshot: BitmapWorkSnapshot) -> Result<()> {
        let existing_slot = self.work_slots.get(&snapshot.work_id).copied();
        if let Some(slot) = existing_slot {
            if let Some(previous) = self.works[slot].take() {
                clear_bit(&mut self.active_members, slot);
                clear_bit(
                    &mut self.kinds[previous.kind_ordinal as usize].members,
                    slot,
                );
                for tag_ordinal in previous.tag_ordinals {
                    self.tags[tag_ordinal as usize].members.remove(slot);
                    self.association_count = self.association_count.saturating_sub(1);
                }
                self.active_work_count = self.active_work_count.saturating_sub(1);
            }
        }

        let Some(kind) = snapshot.kind else {
            if let Some(slot) = existing_slot {
                self.work_slots.remove(&snapshot.work_id);
                self.free_slots.push(slot);
            }
            self.validate_capacity()?;
            return Ok(());
        };

        if existing_slot.is_none() && self.active_work_count >= FACET_BITMAP_MAX_WORKS {
            return Err(capacity_error("works", self.active_work_count + 1));
        }
        let slot = existing_slot
            .or_else(|| self.free_slots.pop())
            .unwrap_or_else(|| {
                let slot = self.works.len();
                self.works.push(None);
                slot
            });
        let kind_ordinal = self.kind_ordinal(&kind)?;
        let mut tag_ids = snapshot.tag_ids;
        tag_ids.sort_unstable();
        tag_ids.dedup();
        if self.association_count.saturating_add(tag_ids.len()) > FACET_BITMAP_MAX_ASSOCIATIONS {
            return Err(capacity_error(
                "associations",
                self.association_count.saturating_add(tag_ids.len()),
            ));
        }
        let mut tag_ordinals = Vec::with_capacity(tag_ids.len());
        for tag_id in tag_ids {
            let tag_ordinal = self.tag_ordinal(tag_id)?;
            self.tags[tag_ordinal as usize].members.insert(slot);
            tag_ordinals.push(tag_ordinal);
        }
        self.association_count += tag_ordinals.len();
        self.active_work_count += 1;
        self.work_slots.insert(snapshot.work_id, slot);
        set_bit(&mut self.active_members, slot);
        set_bit(&mut self.kinds[kind_ordinal as usize].members, slot);
        self.works[slot] = Some(BitmapWork {
            kind_ordinal,
            tag_ordinals,
        });
        self.validate_capacity()?;
        Ok(())
    }

    fn facet_counts(&self, kind: Option<&str>, selected_tag_ids: &[i64]) -> Vec<(i64, i64)> {
        let mut candidate = match kind {
            Some(kind) => self
                .kind_ordinals
                .get(kind)
                .map(|ordinal| self.kinds[*ordinal as usize].members.clone())
                .unwrap_or_else(|| vec![0_u64; FACET_BITMAP_WORDS]),
            None => self.active_members.clone(),
        };
        for tag_id in selected_tag_ids {
            let Some(tag_ordinal) = self.tag_ordinals.get(tag_id) else {
                candidate.fill(0);
                break;
            };
            self.tags[*tag_ordinal as usize]
                .members
                .intersect_into(&mut candidate);
        }

        let mut counts = vec![0_u32; self.tags.len()];
        for (word_index, word) in candidate.into_iter().enumerate() {
            let mut remaining = word;
            while remaining != 0 {
                let bit = remaining.trailing_zeros() as usize;
                let slot = word_index * 64 + bit;
                if let Some(Some(work)) = self.works.get(slot) {
                    for tag_ordinal in &work.tag_ordinals {
                        counts[*tag_ordinal as usize] += 1;
                    }
                }
                remaining &= remaining - 1;
            }
        }
        counts
            .into_iter()
            .enumerate()
            .filter_map(|(ordinal, count)| {
                (count > 0).then_some((self.tags[ordinal].tag_id, i64::from(count)))
            })
            .collect()
    }

    fn estimated_bytes(&self) -> usize {
        let work_bytes = self.works.capacity() * size_of::<Option<BitmapWork>>()
            + self
                .works
                .iter()
                .flatten()
                .map(|work| work.tag_ordinals.capacity() * size_of::<u32>())
                .sum::<usize>();
        let kind_bytes = self.kinds.capacity() * size_of::<BitmapKind>()
            + self
                .kinds
                .iter()
                .map(|kind| kind.name.capacity() + kind.members.capacity() * size_of::<u64>())
                .sum::<usize>();
        let tag_bytes = self.tags.capacity() * size_of::<BitmapTag>()
            + self
                .tags
                .iter()
                .map(|tag| tag.members.estimated_bytes())
                .sum::<usize>();
        let map_bytes = self.work_slots.capacity() * (size_of::<i64>() + size_of::<usize>() + 16)
            + self.kind_ordinals.capacity() * (size_of::<String>() + size_of::<u16>() + 16)
            + self.tag_ordinals.capacity() * (size_of::<i64>() + size_of::<u32>() + 16);
        work_bytes
            .saturating_add(kind_bytes)
            .saturating_add(tag_bytes)
            .saturating_add(map_bytes)
            .saturating_add(self.active_members.capacity() * size_of::<u64>())
            .saturating_add(self.free_slots.capacity() * size_of::<usize>())
    }

    fn validate_capacity(&self) -> Result<()> {
        if self.active_work_count > FACET_BITMAP_MAX_WORKS {
            return Err(capacity_error("works", self.active_work_count));
        }
        if self.tags.len() > FACET_BITMAP_MAX_TAGS {
            return Err(capacity_error("tags", self.tags.len()));
        }
        if self.kinds.len() > FACET_BITMAP_MAX_KINDS {
            return Err(capacity_error("kinds", self.kinds.len()));
        }
        if self.association_count > FACET_BITMAP_MAX_ASSOCIATIONS {
            return Err(capacity_error("associations", self.association_count));
        }
        let estimated = self.estimated_bytes();
        if estimated > FACET_BITMAP_MAX_BYTES {
            return Err(AppError::Other(format!(
                "Facet bitmap estimated {estimated} bytes exceeds {FACET_BITMAP_MAX_BYTES} byte limit"
            )));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct BitmapWorkSnapshot {
    work_id: i64,
    kind: Option<String>,
    tag_ids: Vec<i64>,
}

#[derive(Debug, Default)]
struct FacetBitmapState {
    index: Option<FacetBitmapIndex>,
    building: bool,
    next_retry_at: Option<Instant>,
    last_error: Option<String>,
    last_build_millis: Option<u64>,
}

struct FacetBitmapRuntimeInner {
    state: Mutex<FacetBitmapState>,
    refresh: Mutex<()>,
    builds: AtomicU64,
    build_failures: AtomicU64,
    queries: AtomicU64,
    fallbacks: AtomicU64,
    scope_rejections: AtomicU64,
    delta_refreshes: AtomicU64,
    delta_work_ids: AtomicU64,
    query_total_micros: AtomicU64,
    query_max_micros: AtomicU64,
}

#[derive(Clone)]
pub(super) struct FacetBitmapRuntime {
    inner: Arc<FacetBitmapRuntimeInner>,
}

impl Default for FacetBitmapRuntime {
    fn default() -> Self {
        Self {
            inner: Arc::new(FacetBitmapRuntimeInner {
                state: Mutex::new(FacetBitmapState::default()),
                refresh: Mutex::new(()),
                builds: AtomicU64::new(0),
                build_failures: AtomicU64::new(0),
                queries: AtomicU64::new(0),
                fallbacks: AtomicU64::new(0),
                scope_rejections: AtomicU64::new(0),
                delta_refreshes: AtomicU64::new(0),
                delta_work_ids: AtomicU64::new(0),
                query_total_micros: AtomicU64::new(0),
                query_max_micros: AtomicU64::new(0),
            }),
        }
    }
}

#[derive(Debug)]
pub(super) struct FacetBitmapCounts {
    pub catalog_revision: i64,
    pub counts: Vec<(i64, i64)>,
}

#[derive(Debug, Serialize)]
pub struct FacetBitmapRuntimeSnapshot {
    pub state: String,
    pub catalog_revision: Option<i64>,
    pub works: usize,
    pub tags: usize,
    pub associations: usize,
    pub estimated_bytes: usize,
    pub builds: u64,
    pub build_failures: u64,
    pub last_build_millis: Option<u64>,
    pub queries: u64,
    pub fallbacks: u64,
    pub scope_rejections: u64,
    pub delta_refreshes: u64,
    pub delta_work_ids: u64,
    pub query_average_millis: f64,
    pub query_max_millis: f64,
    pub last_error: Option<String>,
    pub max_works: usize,
    pub max_tags: usize,
    pub max_associations: usize,
    pub max_estimated_bytes: usize,
}

impl FacetBitmapRuntime {
    pub(super) async fn try_counts(
        &self,
        db: &Db,
        resources: &ResourceGovernor,
        target_revision: i64,
        kind: Option<&str>,
        selected_tag_ids: &[i64],
    ) -> Result<Option<FacetBitmapCounts>> {
        if selected_tag_ids.is_empty() {
            return Ok(None);
        }
        if !scope_uses_typed_writer(db, kind).await? {
            self.inner.scope_rejections.fetch_add(1, Ordering::Relaxed);
            self.inner.fallbacks.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }

        let _refresh = self.inner.refresh.lock().await;
        let base_revision = {
            let state = self.inner.state.lock().await;
            state.index.as_ref().map(|index| index.catalog_revision)
        };
        let Some(base_revision) = base_revision else {
            self.inner.fallbacks.fetch_add(1, Ordering::Relaxed);
            self.schedule_build(db.clone(), resources.clone()).await;
            return Ok(None);
        };

        if base_revision != target_revision {
            let Some(work_ids) = db.catalog_work_ids_for_revision_range(
                base_revision,
                target_revision,
                FACET_BITMAP_MAX_REFRESH_DELTAS,
            ) else {
                self.invalidate(format!(
                    "Facet bitmap revision chain {base_revision}->{target_revision} is unavailable"
                ))
                .await;
                self.inner.fallbacks.fetch_add(1, Ordering::Relaxed);
                self.schedule_build(db.clone(), resources.clone()).await;
                return Ok(None);
            };
            let unique_work_ids = work_ids.into_iter().collect::<HashSet<_>>();
            let snapshots = match load_work_snapshots(
                db,
                target_revision,
                unique_work_ids.iter().copied().collect(),
            )
            .await
            {
                Ok(Some(snapshots)) => snapshots,
                Ok(None) => {
                    self.inner.fallbacks.fetch_add(1, Ordering::Relaxed);
                    return Ok(None);
                }
                Err(error) => {
                    self.invalidate(format!("Facet bitmap delta refresh failed: {error}"))
                        .await;
                    self.inner.fallbacks.fetch_add(1, Ordering::Relaxed);
                    self.schedule_build(db.clone(), resources.clone()).await;
                    return Ok(None);
                }
            };
            let mut state = self.inner.state.lock().await;
            let Some(index) = state.index.as_mut() else {
                self.inner.fallbacks.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            };
            if index.catalog_revision != base_revision {
                self.inner.fallbacks.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
            for snapshot in snapshots {
                if let Err(error) = index.apply_work_snapshot(snapshot) {
                    state.index = None;
                    state.last_error = Some(error.to_string());
                    state.next_retry_at = Some(Instant::now() + FACET_BITMAP_BUILD_RETRY);
                    self.inner.fallbacks.fetch_add(1, Ordering::Relaxed);
                    return Ok(None);
                }
            }
            index.catalog_revision = target_revision;
            self.inner.delta_refreshes.fetch_add(1, Ordering::Relaxed);
            self.inner
                .delta_work_ids
                .fetch_add(unique_work_ids.len() as u64, Ordering::Relaxed);
        }

        let started = Instant::now();
        let counts = {
            let state = self.inner.state.lock().await;
            let Some(index) = state.index.as_ref() else {
                self.inner.fallbacks.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            };
            if index.catalog_revision != target_revision {
                self.inner.fallbacks.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
            index.facet_counts(kind, selected_tag_ids)
        };
        let elapsed = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.inner.queries.fetch_add(1, Ordering::Relaxed);
        self.inner
            .query_total_micros
            .fetch_add(elapsed, Ordering::Relaxed);
        self.inner
            .query_max_micros
            .fetch_max(elapsed, Ordering::Relaxed);
        Ok(Some(FacetBitmapCounts {
            catalog_revision: target_revision,
            counts,
        }))
    }

    async fn schedule_build(&self, db: Db, resources: ResourceGovernor) {
        {
            let mut state = self.inner.state.lock().await;
            if state.index.is_some() || state.building {
                return;
            }
            if state
                .next_retry_at
                .is_some_and(|retry_at| retry_at > Instant::now())
            {
                return;
            }
            state.building = true;
            state.last_error = None;
        }
        self.inner.builds.fetch_add(1, Ordering::Relaxed);
        let runtime = self.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let result = async {
                let _lease = resources
                    .reserve_background(
                        ResourceClass::CatalogWriter,
                        FACET_BITMAP_BUILD_RESERVATION_BYTES,
                        0,
                    )
                    .await?;
                FacetBitmapIndex::build(&db).await
            }
            .await;
            let elapsed = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
            let mut state = runtime.inner.state.lock().await;
            state.building = false;
            state.last_build_millis = Some(elapsed);
            match result {
                Ok(index) => {
                    state.index = Some(index);
                    state.next_retry_at = None;
                    state.last_error = None;
                }
                Err(error) => {
                    state.index = None;
                    state.next_retry_at = Some(Instant::now() + FACET_BITMAP_BUILD_RETRY);
                    state.last_error = Some(error.to_string());
                    runtime.inner.build_failures.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }

    async fn invalidate(&self, reason: String) {
        let mut state = self.inner.state.lock().await;
        state.index = None;
        state.next_retry_at = None;
        state.last_error = Some(reason);
    }

    pub(super) async fn snapshot(&self) -> FacetBitmapRuntimeSnapshot {
        let state = self.inner.state.lock().await;
        let (status, revision, works, tags, associations, estimated_bytes) =
            if let Some(index) = &state.index {
                (
                    "ready",
                    Some(index.catalog_revision),
                    index.active_work_count,
                    index.tags.len(),
                    index.association_count,
                    index.estimated_bytes(),
                )
            } else if state.building {
                ("building", None, 0, 0, 0, 0)
            } else if state.last_error.is_some() {
                ("failed", None, 0, 0, 0, 0)
            } else {
                ("idle", None, 0, 0, 0, 0)
            };
        let queries = self.inner.queries.load(Ordering::Relaxed);
        let total_micros = self.inner.query_total_micros.load(Ordering::Relaxed);
        FacetBitmapRuntimeSnapshot {
            state: status.to_string(),
            catalog_revision: revision,
            works,
            tags,
            associations,
            estimated_bytes,
            builds: self.inner.builds.load(Ordering::Relaxed),
            build_failures: self.inner.build_failures.load(Ordering::Relaxed),
            last_build_millis: state.last_build_millis,
            queries,
            fallbacks: self.inner.fallbacks.load(Ordering::Relaxed),
            scope_rejections: self.inner.scope_rejections.load(Ordering::Relaxed),
            delta_refreshes: self.inner.delta_refreshes.load(Ordering::Relaxed),
            delta_work_ids: self.inner.delta_work_ids.load(Ordering::Relaxed),
            query_average_millis: if queries == 0 {
                0.0
            } else {
                total_micros as f64 / queries as f64 / 1_000.0
            },
            query_max_millis: self.inner.query_max_micros.load(Ordering::Relaxed) as f64 / 1_000.0,
            last_error: state.last_error.clone(),
            max_works: FACET_BITMAP_MAX_WORKS,
            max_tags: FACET_BITMAP_MAX_TAGS,
            max_associations: FACET_BITMAP_MAX_ASSOCIATIONS,
            max_estimated_bytes: FACET_BITMAP_MAX_BYTES,
        }
    }
}

async fn scope_uses_typed_writer(db: &Db, kind: Option<&str>) -> Result<bool> {
    if let Some(kind) = kind {
        return Ok(sqlx::query_scalar::<_, i64>(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM catalog_kind_ownership
                WHERE kind = ?1 AND authoritative_writer = 'catalog-v2'
            )
            "#,
        )
        .bind(kind)
        .fetch_one(db.pool())
        .await?
            != 0);
    }
    Ok(sqlx::query_scalar::<_, i64>(
        r#"
        SELECT NOT EXISTS (
            SELECT 1
            FROM (SELECT DISTINCT kind FROM works WHERE deleted_at IS NULL) AS active_kind
            LEFT JOIN catalog_kind_ownership AS ownership
              ON ownership.kind = active_kind.kind
            WHERE COALESCE(ownership.authoritative_writer, 'legacy') != 'catalog-v2'
        )
        "#,
    )
    .fetch_one(db.pool())
    .await?
        != 0)
}

async fn load_work_snapshots(
    db: &Db,
    target_revision: i64,
    mut work_ids: Vec<i64>,
) -> Result<Option<Vec<BitmapWorkSnapshot>>> {
    work_ids.sort_unstable();
    work_ids.dedup();
    if work_ids.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let work_ids_json =
        serde_json::to_string(&work_ids).map_err(|error| AppError::Other(error.to_string()))?;
    let mut transaction = db.begin_tracked_read_transaction().await?;
    let revision =
        sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
            .fetch_one(&mut *transaction)
            .await?;
    if revision != target_revision {
        transaction.rollback().await?;
        return Ok(None);
    }
    let positions = work_ids
        .iter()
        .enumerate()
        .map(|(index, work_id)| (*work_id, index))
        .collect::<HashMap<_, _>>();
    let mut snapshots = work_ids
        .iter()
        .map(|work_id| BitmapWorkSnapshot {
            work_id: *work_id,
            kind: None,
            tag_ids: Vec::new(),
        })
        .collect::<Vec<_>>();
    {
        let mut rows = sqlx::query(
            r#"
            WITH requested(work_id) AS (
                SELECT CAST(value AS INTEGER) FROM json_each(?1)
            )
            SELECT requested.work_id, work.kind, work_tag.tag_id
            FROM requested
            LEFT JOIN works AS work
              ON work.id = requested.work_id AND work.deleted_at IS NULL
            LEFT JOIN work_tags AS work_tag ON work_tag.work_id = work.id
            ORDER BY requested.work_id, work_tag.tag_id
            "#,
        )
        .bind(&work_ids_json)
        .fetch(&mut *transaction);
        while let Some(row) = rows.try_next().await? {
            let work_id: i64 = row.get("work_id");
            let Some(position) = positions.get(&work_id).copied() else {
                continue;
            };
            if snapshots[position].kind.is_none() {
                snapshots[position].kind = row.get::<Option<String>, _>("kind");
            }
            if let Some(tag_id) = row.get::<Option<i64>, _>("tag_id") {
                snapshots[position].tag_ids.push(tag_id);
            }
        }
    }
    transaction.commit().await?;
    Ok(Some(snapshots))
}

fn set_bit(words: &mut [u64], slot: usize) {
    words[slot / 64] |= 1_u64 << (slot % 64);
}

fn clear_bit(words: &mut [u64], slot: usize) {
    words[slot / 64] &= !(1_u64 << (slot % 64));
}

fn bit_is_set(words: &[u64], slot: usize) -> bool {
    words[slot / 64] & (1_u64 << (slot % 64)) != 0
}

fn capacity_error(kind: &str, value: usize) -> AppError {
    AppError::Other(format!("Facet bitmap {kind} capacity exceeded at {value}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::ResourceLimits;

    fn snapshot(work_id: i64, kind: Option<&str>, tag_ids: &[i64]) -> BitmapWorkSnapshot {
        BitmapWorkSnapshot {
            work_id,
            kind: kind.map(str::to_string),
            tag_ids: tag_ids.to_vec(),
        }
    }

    #[test]
    fn adaptive_bitmap_intersects_sparse_and_dense_memberships() {
        let mut sparse = AdaptiveBitmap::default();
        sparse.insert(1);
        sparse.insert(65);
        let mut candidate = vec![u64::MAX; FACET_BITMAP_WORDS];
        sparse.intersect_into(&mut candidate);
        assert!(bit_is_set(&candidate, 1));
        assert!(bit_is_set(&candidate, 65));
        assert!(!bit_is_set(&candidate, 2));

        let mut dense = AdaptiveBitmap::default();
        for slot in 0..FACET_BITMAP_DENSE_THRESHOLD {
            dense.insert(slot);
        }
        assert!(matches!(dense, AdaptiveBitmap::Dense(_)));
        dense.remove(7);
        let mut candidate = vec![u64::MAX; FACET_BITMAP_WORDS];
        dense.intersect_into(&mut candidate);
        assert!(!bit_is_set(&candidate, 7));
        assert!(bit_is_set(&candidate, 8));
    }

    #[test]
    fn selected_tag_counts_follow_kind_and_incremental_work_snapshots() {
        let mut index = FacetBitmapIndex::empty(10);
        index
            .apply_work_snapshot(snapshot(1, Some("gallery"), &[10, 20]))
            .unwrap();
        index
            .apply_work_snapshot(snapshot(2, Some("gallery"), &[10, 30]))
            .unwrap();
        index
            .apply_work_snapshot(snapshot(3, Some("audio"), &[20]))
            .unwrap();

        let counts = index
            .facet_counts(None, &[10])
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(counts.get(&10), Some(&2));
        assert_eq!(counts.get(&20), Some(&1));
        assert_eq!(counts.get(&30), Some(&1));
        let gallery = index
            .facet_counts(Some("gallery"), &[20])
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(gallery.get(&10), Some(&1));
        assert_eq!(gallery.get(&20), Some(&1));

        index
            .apply_work_snapshot(snapshot(2, Some("gallery"), &[20, 30]))
            .unwrap();
        let counts = index
            .facet_counts(None, &[10])
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(counts.get(&10), Some(&1));
        assert_eq!(counts.get(&20), Some(&1));
        assert!(!counts.contains_key(&30));

        index.apply_work_snapshot(snapshot(1, None, &[])).unwrap();
        assert!(index.facet_counts(None, &[10]).is_empty());
        assert!(index.estimated_bytes() <= FACET_BITMAP_MAX_BYTES);
    }

    #[tokio::test]
    async fn runtime_builds_in_background_and_applies_a_contiguous_revision_delta() {
        let temp = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}",
            temp.path().join("facet-bitmap.sqlite").display()
        );
        let db = Db::connect(&url).await.unwrap();
        db.migrate().await.unwrap();
        let first = db
            .upsert_work(
                "gallery",
                "First",
                Some("/gallery/first"),
                None,
                None,
                None,
                serde_json::json!({}),
            )
            .await
            .unwrap();
        let second = db
            .upsert_work(
                "gallery",
                "Second",
                Some("/gallery/second"),
                None,
                None,
                None,
                serde_json::json!({}),
            )
            .await
            .unwrap();
        let selected = db
            .upsert_tag(
                "scope", "selected", "selected", None, None, "test", None, None,
            )
            .await
            .unwrap();
        let shared = db
            .upsert_tag("scope", "shared", "shared", None, None, "test", None, None)
            .await
            .unwrap();
        let second_only = db
            .upsert_tag("scope", "second", "second", None, None, "test", None, None)
            .await
            .unwrap();
        db.link_tag(first, selected).await.unwrap();
        db.link_tag(first, shared).await.unwrap();
        db.link_tag(second, selected).await.unwrap();
        db.link_tag(second, second_only).await.unwrap();
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'gallery'",
        )
        .execute(db.pool())
        .await
        .unwrap();

        let revision =
            sqlx::query_scalar::<_, i64>("SELECT revision FROM catalog_state WHERE singleton = 1")
                .fetch_one(db.pool())
                .await
                .unwrap();
        let runtime = FacetBitmapRuntime::default();
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        assert!(runtime
            .try_counts(&db, &resources, revision, Some("gallery"), &[selected])
            .await
            .unwrap()
            .is_none());
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if runtime.snapshot().await.state == "ready" {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let initial = runtime
            .try_counts(&db, &resources, revision, Some("gallery"), &[selected])
            .await
            .unwrap()
            .unwrap()
            .counts
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(initial.get(&selected), Some(&2));
        assert_eq!(initial.get(&shared), Some(&1));
        assert_eq!(initial.get(&second_only), Some(&1));

        let next_revision = revision + 1;
        let mut transaction = db.pool().begin().await.unwrap();
        sqlx::query("DELETE FROM work_tags WHERE work_id = ?1 AND tag_id = ?2")
            .bind(second)
            .bind(selected)
            .execute(&mut *transaction)
            .await
            .unwrap();
        sqlx::query("INSERT OR IGNORE INTO work_tags(work_id, tag_id) VALUES (?1, ?2)")
            .bind(second)
            .bind(shared)
            .execute(&mut *transaction)
            .await
            .unwrap();
        sqlx::query("UPDATE catalog_state SET revision = ?1 WHERE singleton = 1")
            .bind(next_revision)
            .execute(&mut *transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        db.record_catalog_work_revision_delta(revision, next_revision, second);

        let refreshed = runtime
            .try_counts(&db, &resources, next_revision, Some("gallery"), &[selected])
            .await
            .unwrap()
            .unwrap()
            .counts
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(refreshed.get(&selected), Some(&1));
        assert_eq!(refreshed.get(&shared), Some(&1));
        assert!(!refreshed.contains_key(&second_only));
        let snapshot = runtime.snapshot().await;
        assert_eq!(snapshot.delta_refreshes, 1);
        assert_eq!(snapshot.delta_work_ids, 1);
        assert_eq!(snapshot.catalog_revision, Some(next_revision));
    }
}
