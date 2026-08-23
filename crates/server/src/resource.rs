use std::collections::BTreeMap;
use std::env;
#[cfg(target_os = "linux")]
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use serde::Serialize;
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::time::Instant;

use crate::error::{AppError, Result};

const MIB: u64 = 1024 * 1024;
const PROCESSING_UNIT_BYTES: u64 = 16 * MIB;
const INFLIGHT_UNIT_BYTES: u64 = 8 * MIB;
const MIN_ARCHIVE_MANIFEST_CACHE_BYTES: u64 = 16 * MIB;
const MIN_RESOURCE_WAIT_TIMEOUT_MILLIS: u64 = 100;
const MAX_RESOURCE_WAIT_TIMEOUT_MILLIS: u64 = 5 * 60 * 1000;

#[derive(Debug, Clone)]
pub struct ResourceLimits {
    pub profile: String,
    pub processing_memory_bytes: u64,
    pub inflight_media_bytes: u64,
    pub memory_soft_limit_bytes: u64,
    pub memory_resume_limit_bytes: u64,
    pub archive_manifest_cache_bytes: u64,
    pub interactive_wait_timeout_millis: u64,
    pub thumbnail_workers: usize,
    pub archive_workers: usize,
    pub local_media_stream_workers: usize,
    pub remote_workers: usize,
    pub scan_io_workers: usize,
    pub catalog_writers: usize,
    pub search_writers: usize,
    pub search_writer_heap_bytes: usize,
}

impl ResourceLimits {
    pub fn standard() -> Self {
        Self {
            profile: "standard".to_string(),
            processing_memory_bytes: 2 * 1024 * MIB,
            inflight_media_bytes: 1024 * MIB,
            memory_soft_limit_bytes: 6 * 1024 * MIB,
            memory_resume_limit_bytes: 5 * 1024 * MIB,
            archive_manifest_cache_bytes: 512 * MIB,
            interactive_wait_timeout_millis: 30_000,
            thumbnail_workers: 2,
            archive_workers: 4,
            local_media_stream_workers: 4,
            remote_workers: 2,
            scan_io_workers: 4,
            catalog_writers: 1,
            search_writers: 1,
            search_writer_heap_bytes: 50_000_000,
        }
    }

    pub fn nas_n100_4g() -> Self {
        Self {
            profile: "nas-n100-4g".to_string(),
            // Start conservatively on a 4 GiB cgroup. These are admission
            // budgets, not estimates of the process's total resident set.
            // Operators can raise them after observing the target NAS.
            processing_memory_bytes: 1024 * MIB,
            inflight_media_bytes: 256 * MIB,
            memory_soft_limit_bytes: 3072 * MIB,
            memory_resume_limit_bytes: 2688 * MIB,
            archive_manifest_cache_bytes: 256 * MIB,
            interactive_wait_timeout_millis: 10_000,
            thumbnail_workers: 1,
            archive_workers: 2,
            local_media_stream_workers: 2,
            remote_workers: 1,
            scan_io_workers: 1,
            catalog_writers: 1,
            search_writers: 1,
            search_writer_heap_bytes: 32 * MIB as usize,
        }
    }

    pub fn from_env() -> anyhow::Result<Self> {
        let profile = env::var("RESOURCE_PROFILE")
            .unwrap_or_else(|_| "standard".to_string())
            .trim()
            .to_ascii_lowercase();
        let mut limits = match profile.as_str() {
            "standard" | "desktop" => Self::standard(),
            "nas-n100-4g" | "n100" => Self::nas_n100_4g(),
            other => bail!("RESOURCE_PROFILE must be standard or nas-n100-4g, got {other:?}"),
        };
        limits.profile = profile;
        limits.processing_memory_bytes = env_u64(
            "PROCESSING_MEMORY_BUDGET_BYTES",
            limits.processing_memory_bytes,
        )?;
        limits.inflight_media_bytes =
            env_u64("INFLIGHT_MEDIA_BUDGET_BYTES", limits.inflight_media_bytes)?;
        limits.memory_soft_limit_bytes =
            env_u64("MEMORY_SOFT_LIMIT_BYTES", limits.memory_soft_limit_bytes)?;
        limits.memory_resume_limit_bytes = env_u64(
            "MEMORY_RESUME_LIMIT_BYTES",
            limits.memory_soft_limit_bytes.saturating_mul(7) / 8,
        )?;
        limits.archive_manifest_cache_bytes = env_u64(
            "ARCHIVE_MANIFEST_CACHE_BYTES",
            limits.archive_manifest_cache_bytes,
        )?;
        limits.interactive_wait_timeout_millis = env_u64(
            "RESOURCE_WAIT_TIMEOUT_MILLIS",
            limits.interactive_wait_timeout_millis,
        )?;
        limits.thumbnail_workers = env_usize("THUMBNAIL_WORKERS", limits.thumbnail_workers)?;
        limits.archive_workers = env_usize("ARCHIVE_STREAM_WORKERS", limits.archive_workers)?;
        limits.local_media_stream_workers = env_usize(
            "LOCAL_MEDIA_STREAM_WORKERS",
            limits.local_media_stream_workers,
        )?;
        limits.remote_workers = env_usize("REMOTE_SOURCE_WORKERS", limits.remote_workers)?;
        limits.scan_io_workers = env_usize("SCAN_IO_CONCURRENCY", limits.scan_io_workers)?;
        limits.catalog_writers = env_usize("CATALOG_WRITERS", limits.catalog_writers)?;
        limits.search_writers = env_usize("SEARCH_WRITERS", limits.search_writers)?;
        limits.search_writer_heap_bytes =
            env_usize("SEARCH_WRITER_HEAP_BYTES", limits.search_writer_heap_bytes)?;
        limits.validate()?;
        Ok(limits)
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.processing_memory_bytes < PROCESSING_UNIT_BYTES {
            bail!("PROCESSING_MEMORY_BUDGET_BYTES must be at least {PROCESSING_UNIT_BYTES}");
        }
        if self.inflight_media_bytes < INFLIGHT_UNIT_BYTES {
            bail!("INFLIGHT_MEDIA_BUDGET_BYTES must be at least {INFLIGHT_UNIT_BYTES}");
        }
        if self.memory_resume_limit_bytes >= self.memory_soft_limit_bytes {
            bail!("MEMORY_RESUME_LIMIT_BYTES must be lower than MEMORY_SOFT_LIMIT_BYTES");
        }
        if self.archive_manifest_cache_bytes < MIN_ARCHIVE_MANIFEST_CACHE_BYTES {
            bail!(
                "ARCHIVE_MANIFEST_CACHE_BYTES must be at least {MIN_ARCHIVE_MANIFEST_CACHE_BYTES}"
            );
        }
        if !(MIN_RESOURCE_WAIT_TIMEOUT_MILLIS..=MAX_RESOURCE_WAIT_TIMEOUT_MILLIS)
            .contains(&self.interactive_wait_timeout_millis)
        {
            bail!(
                "RESOURCE_WAIT_TIMEOUT_MILLIS must be between {MIN_RESOURCE_WAIT_TIMEOUT_MILLIS} and {MAX_RESOURCE_WAIT_TIMEOUT_MILLIS}"
            );
        }
        for (name, value) in [
            ("THUMBNAIL_WORKERS", self.thumbnail_workers),
            ("ARCHIVE_STREAM_WORKERS", self.archive_workers),
            (
                "LOCAL_MEDIA_STREAM_WORKERS",
                self.local_media_stream_workers,
            ),
            ("REMOTE_SOURCE_WORKERS", self.remote_workers),
            ("SCAN_IO_CONCURRENCY", self.scan_io_workers),
            ("CATALOG_WRITERS", self.catalog_writers),
            ("SEARCH_WRITERS", self.search_writers),
        ] {
            if value == 0 || value > u32::MAX as usize {
                bail!("{name} must be between 1 and {}", u32::MAX);
            }
        }
        if self.search_writer_heap_bytes < MIB as usize {
            bail!("SEARCH_WRITER_HEAP_BYTES must be at least {MIB}");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceClass {
    ThumbnailDecode,
    ArchiveStream,
    LocalMediaStream,
    RemoteSource,
    ScanIo,
    CatalogWriter,
    SearchWriter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourcePriority {
    Interactive,
    Background,
}

#[derive(Debug, Clone, Copy)]
pub struct ResourceRequest {
    pub class: ResourceClass,
    pub processing_bytes: u64,
    pub inflight_bytes: u64,
    pub priority: ResourcePriority,
    pub deadline: Option<Instant>,
}

#[derive(Clone)]
pub struct ResourceGovernor {
    inner: Arc<ResourceGovernorInner>,
}

struct ResourceGovernorInner {
    limits: ResourceLimits,
    thumbnail_decode: ResourcePool,
    archive_stream: ResourcePool,
    local_media_stream: ResourcePool,
    remote_source: ResourcePool,
    scan_io: ResourcePool,
    catalog_writer: ResourcePool,
    search_writer: ResourcePool,
    processing_memory: ResourcePool,
    inflight_media: ResourcePool,
    admission: AsyncMutex<()>,
    changed: Arc<Notify>,
    interactive_waiters: AtomicUsize,
    background_waiters: AtomicUsize,
    resource_wait_samples: AtomicU64,
    resource_wait_total_micros: AtomicU64,
    resource_wait_max_micros: AtomicU64,
    resource_wait_timeouts: AtomicU64,
    observed_memory_bytes: AtomicU64,
    background_paused: AtomicBool,
}

struct ResourcePool {
    name: &'static str,
    semaphore: Arc<Semaphore>,
    limit_units: u32,
    unit_bytes: u64,
    waiters: AtomicUsize,
    wait_samples: AtomicU64,
    wait_total_micros: AtomicU64,
    wait_max_micros: AtomicU64,
    wait_timeouts: AtomicU64,
    changed: Arc<Notify>,
}

pub struct ResourceLease {
    class: PoolLease,
    processing: Option<PoolLease>,
    inflight: Option<PoolLease>,
}

pub struct ResourceWorkLease {
    _class: PoolLease,
    _processing: Option<PoolLease>,
}

pub struct ResourceBufferLease {
    _inflight: Option<PoolLease>,
}

struct PoolLease {
    _permit: OwnedSemaphorePermit,
    changed: Arc<Notify>,
    notify_on_drop: bool,
}

impl Drop for PoolLease {
    fn drop(&mut self) {
        if self.notify_on_drop {
            self.changed.notify_waiters();
        }
    }
}

impl PoolLease {
    fn arm(mut self) -> Self {
        self.notify_on_drop = true;
        self
    }
}

impl ResourceLease {
    pub fn split(self) -> (ResourceWorkLease, ResourceBufferLease) {
        (
            ResourceWorkLease {
                _class: self.class,
                _processing: self.processing,
            },
            ResourceBufferLease {
                _inflight: self.inflight,
            },
        )
    }
}

struct ReservationWaiter {
    inner: Arc<ResourceGovernorInner>,
    class: ResourceClass,
    processing: bool,
    inflight: bool,
    priority: ResourcePriority,
}

impl ReservationWaiter {
    fn new(inner: Arc<ResourceGovernorInner>, request: &ResourceRequest) -> Self {
        inner
            .class_pool(request.class)
            .waiters
            .fetch_add(1, Ordering::Relaxed);
        if request.processing_bytes > 0 {
            inner
                .processing_memory
                .waiters
                .fetch_add(1, Ordering::Relaxed);
        }
        if request.inflight_bytes > 0 {
            inner.inflight_media.waiters.fetch_add(1, Ordering::Relaxed);
        }
        match request.priority {
            ResourcePriority::Interactive => {
                inner.interactive_waiters.fetch_add(1, Ordering::AcqRel);
            }
            ResourcePriority::Background => {
                inner.background_waiters.fetch_add(1, Ordering::AcqRel);
            }
        }
        Self {
            inner,
            class: request.class,
            processing: request.processing_bytes > 0,
            inflight: request.inflight_bytes > 0,
            priority: request.priority,
        }
    }
}

impl Drop for ReservationWaiter {
    fn drop(&mut self) {
        self.inner
            .class_pool(self.class)
            .waiters
            .fetch_sub(1, Ordering::Relaxed);
        if self.processing {
            self.inner
                .processing_memory
                .waiters
                .fetch_sub(1, Ordering::Relaxed);
        }
        if self.inflight {
            self.inner
                .inflight_media
                .waiters
                .fetch_sub(1, Ordering::Relaxed);
        }
        match self.priority {
            ResourcePriority::Interactive => {
                self.inner
                    .interactive_waiters
                    .fetch_sub(1, Ordering::AcqRel);
            }
            ResourcePriority::Background => {
                self.inner.background_waiters.fetch_sub(1, Ordering::AcqRel);
            }
        }
        self.inner.changed.notify_waiters();
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ResourceSnapshot {
    pub profile: String,
    pub memory_soft_limit_bytes: u64,
    pub memory_resume_limit_bytes: u64,
    pub observed_memory_bytes: Option<u64>,
    pub background_paused: bool,
    pub interactive_waiters: usize,
    pub background_waiters: usize,
    pub resource_wait_samples: u64,
    pub resource_wait_total_micros: u64,
    pub resource_wait_max_micros: u64,
    pub resource_wait_timeouts: u64,
    pub archive_manifest_cache_bytes: u64,
    pub interactive_wait_timeout_millis: u64,
    pub search_writer_heap_bytes: usize,
    pub pools: BTreeMap<String, ResourcePoolSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResourcePoolSnapshot {
    pub used: u64,
    pub limit: u64,
    pub waiters: usize,
    /// Reservation wait counters attributed to this pool.  A combined
    /// reservation may contribute one sample to each requested pool; these
    /// are contention evidence, not a mutually exclusive breakdown.
    pub wait_samples: u64,
    pub wait_total_micros: u64,
    pub wait_max_micros: u64,
    pub wait_timeouts: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub used_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CgroupMemorySnapshot {
    pub current_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub high_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
}

/// CPU quota observed from the cgroup that contains the server process.
///
/// `quota_micros = None` means that the cgroup is explicitly unlimited (the
/// v2 `max` spelling or the v1 negative quota).  A missing cgroup filesystem
/// is represented by the whole snapshot being absent, so a benchmark can
/// distinguish "unlimited" from "not observable".
#[derive(Debug, Clone, Serialize)]
pub struct CgroupCpuSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota_micros: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub period_micros: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_cores: Option<f64>,
}

impl ResourceGovernor {
    pub fn new(limits: ResourceLimits) -> Self {
        let processing_units =
            byte_limit_units(limits.processing_memory_bytes, PROCESSING_UNIT_BYTES);
        let inflight_units = byte_limit_units(limits.inflight_media_bytes, INFLIGHT_UNIT_BYTES);
        let changed = Arc::new(Notify::new());
        Self {
            inner: Arc::new(ResourceGovernorInner {
                thumbnail_decode: ResourcePool::new(
                    "thumbnail_decode",
                    limits.thumbnail_workers as u32,
                    0,
                    changed.clone(),
                ),
                archive_stream: ResourcePool::new(
                    "archive_stream",
                    limits.archive_workers as u32,
                    0,
                    changed.clone(),
                ),
                local_media_stream: ResourcePool::new(
                    "local_media_stream",
                    limits.local_media_stream_workers as u32,
                    0,
                    changed.clone(),
                ),
                remote_source: ResourcePool::new(
                    "remote_source",
                    limits.remote_workers as u32,
                    0,
                    changed.clone(),
                ),
                scan_io: ResourcePool::new(
                    "scan_io",
                    limits.scan_io_workers as u32,
                    0,
                    changed.clone(),
                ),
                catalog_writer: ResourcePool::new(
                    "catalog_writer",
                    limits.catalog_writers as u32,
                    0,
                    changed.clone(),
                ),
                search_writer: ResourcePool::new(
                    "search_writer",
                    limits.search_writers as u32,
                    0,
                    changed.clone(),
                ),
                processing_memory: ResourcePool::new(
                    "processing_memory",
                    processing_units,
                    PROCESSING_UNIT_BYTES,
                    changed.clone(),
                ),
                inflight_media: ResourcePool::new(
                    "inflight_media",
                    inflight_units,
                    INFLIGHT_UNIT_BYTES,
                    changed.clone(),
                ),
                admission: AsyncMutex::new(()),
                changed,
                interactive_waiters: AtomicUsize::new(0),
                background_waiters: AtomicUsize::new(0),
                resource_wait_samples: AtomicU64::new(0),
                resource_wait_total_micros: AtomicU64::new(0),
                resource_wait_max_micros: AtomicU64::new(0),
                resource_wait_timeouts: AtomicU64::new(0),
                observed_memory_bytes: AtomicU64::new(0),
                background_paused: AtomicBool::new(false),
                limits,
            }),
        }
    }

    pub fn standard() -> Self {
        Self::new(ResourceLimits::standard())
    }

    pub fn limits(&self) -> &ResourceLimits {
        &self.inner.limits
    }

    pub async fn reserve(
        &self,
        class: ResourceClass,
        processing_bytes: u64,
        inflight_bytes: u64,
    ) -> Result<ResourceLease> {
        let deadline = Instant::now()
            + Duration::from_millis(self.inner.limits.interactive_wait_timeout_millis);
        self.reserve_request(ResourceRequest {
            class,
            processing_bytes,
            inflight_bytes,
            priority: ResourcePriority::Interactive,
            deadline: Some(deadline),
        })
        .await
    }

    pub async fn reserve_background(
        &self,
        class: ResourceClass,
        processing_bytes: u64,
        inflight_bytes: u64,
    ) -> Result<ResourceLease> {
        self.reserve_request(ResourceRequest {
            class,
            processing_bytes,
            inflight_bytes,
            priority: ResourcePriority::Background,
            deadline: None,
        })
        .await
    }

    pub async fn reserve_request(&self, request: ResourceRequest) -> Result<ResourceLease> {
        self.inner
            .processing_memory
            .validate_bytes(request.processing_bytes)?;
        self.inner
            .inflight_media
            .validate_bytes(request.inflight_bytes)?;
        let _waiter = ReservationWaiter::new(self.inner.clone(), &request);
        let wait_started = Instant::now();

        loop {
            // Register for the next state change before checking availability
            // so a permit release between the check and await cannot be lost.
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(lease) = self.try_reserve_once(&request).await? {
                self.record_resource_wait(&request, wait_started, false);
                return Ok(lease);
            }
            if let Some(deadline) = request.deadline {
                if tokio::time::timeout_at(deadline, changed).await.is_err() {
                    self.record_resource_wait(&request, wait_started, true);
                    return Err(AppError::Overloaded {
                        message: format!(
                            "resource request for {:?} exceeded the interactive wait deadline",
                            request.class
                        ),
                        retry_after_seconds: 1,
                    });
                }
            } else {
                changed.await;
            }
        }
    }

    fn record_resource_wait(&self, request: &ResourceRequest, started: Instant, timed_out: bool) {
        let micros = started.elapsed().as_micros().min(u64::MAX as u128) as u64;
        self.inner
            .resource_wait_samples
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .resource_wait_total_micros
            .fetch_add(micros, Ordering::Relaxed);
        self.inner
            .resource_wait_max_micros
            .fetch_max(micros, Ordering::Relaxed);
        if timed_out {
            self.inner
                .resource_wait_timeouts
                .fetch_add(1, Ordering::Relaxed);
        }
        self.inner
            .class_pool(request.class)
            .record_wait(micros, timed_out);
        if request.processing_bytes > 0 {
            self.inner.processing_memory.record_wait(micros, timed_out);
        }
        if request.inflight_bytes > 0 {
            self.inner.inflight_media.record_wait(micros, timed_out);
        }
    }

    async fn try_reserve_once(&self, request: &ResourceRequest) -> Result<Option<ResourceLease>> {
        let _admission = self.inner.admission.lock().await;
        if request.priority == ResourcePriority::Background
            && (self.inner.background_paused.load(Ordering::Acquire)
                || self.inner.interactive_waiters.load(Ordering::Acquire) > 0)
        {
            return Ok(None);
        }

        let processing = match self
            .inner
            .processing_memory
            .try_acquire_bytes(request.processing_bytes)?
        {
            PoolAcquire::Acquired(lease) => lease,
            PoolAcquire::Unavailable => return Ok(None),
        };
        let inflight = match self
            .inner
            .inflight_media
            .try_acquire_bytes(request.inflight_bytes)?
        {
            PoolAcquire::Acquired(lease) => lease,
            PoolAcquire::Unavailable => {
                drop(processing);
                return Ok(None);
            }
        };
        let Some(class) = self.inner.class_pool(request.class).try_acquire_units(1)? else {
            drop(inflight);
            drop(processing);
            return Ok(None);
        };
        Ok(Some(ResourceLease {
            class: class.arm(),
            processing: processing.map(PoolLease::arm),
            inflight: inflight.map(PoolLease::arm),
        }))
    }

    pub fn spawn_memory_monitor(&self) -> tokio::task::JoinHandle<()> {
        let governor = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                if let Some(snapshot) = cgroup_memory_snapshot().await {
                    governor.observe_memory(snapshot.current_bytes);
                }
            }
        })
    }

    fn observe_memory(&self, current_bytes: u64) {
        self.inner
            .observed_memory_bytes
            .store(current_bytes, Ordering::Release);
        let was_paused = self.inner.background_paused.load(Ordering::Acquire);
        let should_pause = if was_paused {
            current_bytes > self.inner.limits.memory_resume_limit_bytes
        } else {
            current_bytes >= self.inner.limits.memory_soft_limit_bytes
        };
        if was_paused != should_pause {
            self.inner
                .background_paused
                .store(should_pause, Ordering::Release);
            self.inner.changed.notify_waiters();
        }
    }

    pub fn snapshot(&self) -> ResourceSnapshot {
        let pools = [
            &self.inner.thumbnail_decode,
            &self.inner.archive_stream,
            &self.inner.local_media_stream,
            &self.inner.remote_source,
            &self.inner.scan_io,
            &self.inner.catalog_writer,
            &self.inner.search_writer,
            &self.inner.processing_memory,
            &self.inner.inflight_media,
        ]
        .into_iter()
        .map(|pool| (pool.name.to_string(), pool.snapshot()))
        .collect();
        ResourceSnapshot {
            profile: self.inner.limits.profile.clone(),
            memory_soft_limit_bytes: self.inner.limits.memory_soft_limit_bytes,
            memory_resume_limit_bytes: self.inner.limits.memory_resume_limit_bytes,
            observed_memory_bytes: match self.inner.observed_memory_bytes.load(Ordering::Acquire) {
                0 => None,
                value => Some(value),
            },
            background_paused: self.inner.background_paused.load(Ordering::Acquire),
            interactive_waiters: self.inner.interactive_waiters.load(Ordering::Acquire),
            background_waiters: self.inner.background_waiters.load(Ordering::Acquire),
            resource_wait_samples: self.inner.resource_wait_samples.load(Ordering::Relaxed),
            resource_wait_total_micros: self
                .inner
                .resource_wait_total_micros
                .load(Ordering::Relaxed),
            resource_wait_max_micros: self.inner.resource_wait_max_micros.load(Ordering::Relaxed),
            resource_wait_timeouts: self.inner.resource_wait_timeouts.load(Ordering::Relaxed),
            archive_manifest_cache_bytes: self.inner.limits.archive_manifest_cache_bytes,
            interactive_wait_timeout_millis: self.inner.limits.interactive_wait_timeout_millis,
            search_writer_heap_bytes: self.inner.limits.search_writer_heap_bytes,
            pools,
        }
    }
}

impl ResourceGovernorInner {
    fn class_pool(&self, class: ResourceClass) -> &ResourcePool {
        match class {
            ResourceClass::ThumbnailDecode => &self.thumbnail_decode,
            ResourceClass::ArchiveStream => &self.archive_stream,
            ResourceClass::LocalMediaStream => &self.local_media_stream,
            ResourceClass::RemoteSource => &self.remote_source,
            ResourceClass::ScanIo => &self.scan_io,
            ResourceClass::CatalogWriter => &self.catalog_writer,
            ResourceClass::SearchWriter => &self.search_writer,
        }
    }
}

pub async fn cgroup_memory_snapshot() -> Option<CgroupMemorySnapshot> {
    cgroup_v2_memory_snapshot()
        .await
        .or_else(cgroup_v1_memory_snapshot)
}

/// Read the CPU quota for the current cgroup without affecting scheduling.
/// This is intentionally a health/benchmark observation only; ResourceGovernor
/// continues to use its explicit worker limits as the admission policy.
pub async fn cgroup_cpu_snapshot() -> Option<CgroupCpuSnapshot> {
    cgroup_v2_cpu_snapshot()
        .await
        .or_else(cgroup_v1_cpu_snapshot)
}

#[cfg(target_os = "linux")]
async fn cgroup_v2_memory_snapshot() -> Option<CgroupMemorySnapshot> {
    let root = Path::new("/sys/fs/cgroup");
    let current_bytes = read_cgroup_u64(&root.join("memory.current")).await??;
    let high_bytes = read_cgroup_u64(&root.join("memory.high")).await?;
    let max_bytes = read_cgroup_u64(&root.join("memory.max")).await?;
    Some(CgroupMemorySnapshot {
        current_bytes,
        high_bytes,
        max_bytes,
    })
}

#[cfg(not(target_os = "linux"))]
async fn cgroup_v2_memory_snapshot() -> Option<CgroupMemorySnapshot> {
    None
}

#[cfg(target_os = "linux")]
fn cgroup_v1_memory_snapshot() -> Option<CgroupMemorySnapshot> {
    let root = Path::new("/sys/fs/cgroup/memory");
    let current_bytes = read_cgroup_u64_blocking(&root.join("memory.usage_in_bytes"))??;
    let max_bytes = read_cgroup_u64_blocking(&root.join("memory.limit_in_bytes"))?;
    Some(CgroupMemorySnapshot {
        current_bytes,
        high_bytes: None,
        max_bytes,
    })
}

#[cfg(not(target_os = "linux"))]
fn cgroup_v1_memory_snapshot() -> Option<CgroupMemorySnapshot> {
    None
}

#[cfg(target_os = "linux")]
async fn cgroup_v2_cpu_snapshot() -> Option<CgroupCpuSnapshot> {
    let root = Path::new("/sys/fs/cgroup");
    let value = tokio::fs::read_to_string(root.join("cpu.max")).await.ok()?;
    parse_cgroup_cpu_max(&value)
}

#[cfg(not(target_os = "linux"))]
async fn cgroup_v2_cpu_snapshot() -> Option<CgroupCpuSnapshot> {
    None
}

#[cfg(target_os = "linux")]
fn cgroup_v1_cpu_snapshot() -> Option<CgroupCpuSnapshot> {
    let root = Path::new("/sys/fs/cgroup/cpu");
    let quota = std::fs::read_to_string(root.join("cpu.cfs_quota_us"))
        .ok()
        .and_then(|value| parse_cgroup_cpu_quota(&value));
    let period = std::fs::read_to_string(root.join("cpu.cfs_period_us"))
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())?;
    Some(cpu_snapshot(quota, Some(period)))
}

#[cfg(not(target_os = "linux"))]
fn cgroup_v1_cpu_snapshot() -> Option<CgroupCpuSnapshot> {
    None
}

#[cfg(target_os = "linux")]
async fn read_cgroup_u64(path: &Path) -> Option<Option<u64>> {
    let value = tokio::fs::read_to_string(path).await.ok()?;
    Some(parse_cgroup_limit(&value))
}

#[cfg(target_os = "linux")]
fn read_cgroup_u64_blocking(path: &Path) -> Option<Option<u64>> {
    let value = std::fs::read_to_string(path).ok()?;
    Some(parse_cgroup_limit(&value))
}

#[cfg(any(target_os = "linux", test))]
fn parse_cgroup_limit(value: &str) -> Option<u64> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("max") {
        None
    } else {
        let parsed = value.parse().ok()?;
        // cgroup v1 uses a near-u64 sentinel for an unlimited memory limit.
        // Treat it like v2's `max` instead of exposing a misleading exabyte
        // quota in health evidence.
        (parsed <= (1_u64 << 60)).then_some(parsed)
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_cgroup_cpu_max(value: &str) -> Option<CgroupCpuSnapshot> {
    let mut fields = value.split_whitespace();
    let quota = fields.next()?;
    let period = fields.next()?.parse::<u64>().ok()?;
    if period == 0 {
        return None;
    }
    let quota = if quota.eq_ignore_ascii_case("max") {
        None
    } else {
        Some(quota.parse::<u64>().ok()?)
    };
    Some(cpu_snapshot(quota, Some(period)))
}

#[cfg(any(target_os = "linux", test))]
fn parse_cgroup_cpu_quota(value: &str) -> Option<u64> {
    let value = value.trim().parse::<i64>().ok()?;
    (value >= 0).then_some(value as u64)
}

#[cfg(any(target_os = "linux", test))]
fn cpu_snapshot(quota_micros: Option<u64>, period_micros: Option<u64>) -> CgroupCpuSnapshot {
    let limit_cores = quota_micros
        .zip(period_micros)
        .filter(|(_, period)| *period > 0)
        .map(|(quota, period)| quota as f64 / period as f64);
    CgroupCpuSnapshot {
        quota_micros,
        period_micros,
        limit_cores,
    }
}

enum PoolAcquire {
    Acquired(Option<PoolLease>),
    Unavailable,
}

impl ResourcePool {
    fn new(name: &'static str, limit_units: u32, unit_bytes: u64, changed: Arc<Notify>) -> Self {
        Self {
            name,
            semaphore: Arc::new(Semaphore::new(limit_units as usize)),
            limit_units,
            unit_bytes,
            waiters: AtomicUsize::new(0),
            wait_samples: AtomicU64::new(0),
            wait_total_micros: AtomicU64::new(0),
            wait_max_micros: AtomicU64::new(0),
            wait_timeouts: AtomicU64::new(0),
            changed,
        }
    }

    fn record_wait(&self, micros: u64, timed_out: bool) {
        self.wait_samples.fetch_add(1, Ordering::Relaxed);
        self.wait_total_micros.fetch_add(micros, Ordering::Relaxed);
        self.wait_max_micros.fetch_max(micros, Ordering::Relaxed);
        if timed_out {
            self.wait_timeouts.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn try_acquire_units(&self, units: u32) -> Result<Option<PoolLease>> {
        match self.semaphore.clone().try_acquire_many_owned(units) {
            Ok(permit) => Ok(Some(PoolLease {
                _permit: permit,
                changed: self.changed.clone(),
                notify_on_drop: false,
            })),
            Err(TryAcquireError::NoPermits) => Ok(None),
            Err(TryAcquireError::Closed) => Err(AppError::Other(format!(
                "{} resource pool is closed",
                self.name
            ))),
        }
    }

    fn validate_bytes(&self, bytes: u64) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let units = bytes_to_units(bytes, self.unit_bytes);
        if units > self.limit_units {
            return Err(AppError::Other(format!(
                "{} request requires {bytes} bytes ({} units), exceeding the {} byte pool limit",
                self.name,
                units,
                self.limit_units as u64 * self.unit_bytes
            )));
        }
        Ok(())
    }

    fn try_acquire_bytes(&self, bytes: u64) -> Result<PoolAcquire> {
        if bytes == 0 {
            return Ok(PoolAcquire::Acquired(None));
        }
        self.validate_bytes(bytes)?;
        match self.try_acquire_units(bytes_to_units(bytes, self.unit_bytes))? {
            Some(lease) => Ok(PoolAcquire::Acquired(Some(lease))),
            None => Ok(PoolAcquire::Unavailable),
        }
    }

    fn snapshot(&self) -> ResourcePoolSnapshot {
        let available = self.semaphore.available_permits() as u64;
        let limit = self.limit_units as u64;
        let used = limit.saturating_sub(available.min(limit));
        let bytes = (self.unit_bytes > 0).then_some(self.unit_bytes);
        ResourcePoolSnapshot {
            used,
            limit,
            waiters: self.waiters.load(Ordering::Relaxed),
            wait_samples: self.wait_samples.load(Ordering::Relaxed),
            wait_total_micros: self.wait_total_micros.load(Ordering::Relaxed),
            wait_max_micros: self.wait_max_micros.load(Ordering::Relaxed),
            wait_timeouts: self.wait_timeouts.load(Ordering::Relaxed),
            unit_bytes: bytes,
            used_bytes: bytes.map(|unit| used.saturating_mul(unit)),
            limit_bytes: bytes.map(|unit| limit.saturating_mul(unit)),
        }
    }
}

fn byte_limit_units(bytes: u64, unit: u64) -> u32 {
    ((bytes / unit).max(1).min(u32::MAX as u64)) as u32
}

fn bytes_to_units(bytes: u64, unit: u64) -> u32 {
    bytes
        .saturating_add(unit.saturating_sub(1))
        .checked_div(unit)
        .unwrap_or(0)
        .max(1)
        .min(u32::MAX as u64) as u32
}

fn env_u64(name: &str, fallback: u64) -> anyhow::Result<u64> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => value
            .trim()
            .parse::<u64>()
            .with_context(|| format!("{name} must be a positive integer"))
            .and_then(|value| {
                if value == 0 {
                    bail!("{name} must be greater than zero")
                }
                Ok(value)
            }),
        _ => Ok(fallback),
    }
}

fn env_usize(name: &str, fallback: usize) -> anyhow::Result<usize> {
    let value = env_u64(name, fallback as u64)?;
    usize::try_from(value).with_context(|| format!("{name} is too large for this platform"))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn n100_profile_stays_within_the_documented_budgets() {
        let limits = ResourceLimits::nas_n100_4g();
        assert_eq!(limits.thumbnail_workers, 1);
        assert_eq!(limits.archive_workers, 2);
        assert_eq!(limits.local_media_stream_workers, 2);
        assert_eq!(limits.processing_memory_bytes, 1024 * MIB);
        assert_eq!(limits.inflight_media_bytes, 256 * MIB);
        assert_eq!(limits.memory_soft_limit_bytes, 3072 * MIB);
        assert_eq!(limits.memory_resume_limit_bytes, 2688 * MIB);
        assert_eq!(limits.archive_manifest_cache_bytes, 256 * MIB);
        assert_eq!(limits.interactive_wait_timeout_millis, 10_000);
        limits.validate().unwrap();
    }

    #[test]
    fn parses_cgroup_v2_cpu_quota_and_effective_cores() {
        let snapshot = parse_cgroup_cpu_max("200000 100000\n").unwrap();
        assert_eq!(snapshot.quota_micros, Some(200_000));
        assert_eq!(snapshot.period_micros, Some(100_000));
        assert_eq!(snapshot.limit_cores, Some(2.0));
    }

    #[test]
    fn preserves_unlimited_cgroup_cpu_as_explicit_null_quota() {
        let snapshot = parse_cgroup_cpu_max("max 100000").unwrap();
        assert_eq!(snapshot.quota_micros, None);
        assert_eq!(snapshot.period_micros, Some(100_000));
        assert_eq!(snapshot.limit_cores, None);
        assert_eq!(parse_cgroup_cpu_quota("-1"), None);
    }

    #[test]
    fn rejects_malformed_or_zero_period_cpu_limits() {
        assert!(parse_cgroup_cpu_max("200000").is_none());
        assert!(parse_cgroup_cpu_max("200000 0").is_none());
        assert!(parse_cgroup_cpu_max("not-a-number 100000").is_none());
    }

    #[test]
    fn treats_cgroup_v1_near_u64_memory_sentinel_as_unlimited() {
        assert_eq!(parse_cgroup_limit("9223372036854771712"), None);
        assert_eq!(parse_cgroup_limit("4294967296"), Some(4_294_967_296));
    }

    #[tokio::test]
    async fn local_media_stream_pool_isolated_from_archive_pool() {
        let governor = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let local = governor
            .reserve(ResourceClass::LocalMediaStream, 0, INFLIGHT_UNIT_BYTES)
            .await
            .unwrap();
        let snapshot = governor.snapshot();
        assert_eq!(snapshot.pools["local_media_stream"].used, 1);
        assert_eq!(snapshot.pools["archive_stream"].used, 0);
        drop(local);
        assert_eq!(governor.snapshot().pools["local_media_stream"].used, 0);
    }

    #[tokio::test]
    async fn reservations_wait_and_drop_releases_every_pool() {
        let mut limits = ResourceLimits::nas_n100_4g();
        limits.processing_memory_bytes = PROCESSING_UNIT_BYTES;
        limits.inflight_media_bytes = INFLIGHT_UNIT_BYTES;
        let governor = ResourceGovernor::new(limits);
        let first = governor
            .reserve(
                ResourceClass::ThumbnailDecode,
                PROCESSING_UNIT_BYTES,
                INFLIGHT_UNIT_BYTES,
            )
            .await
            .unwrap();
        assert_eq!(
            governor.snapshot().pools["processing_memory"].used_bytes,
            Some(PROCESSING_UNIT_BYTES)
        );

        let waiting = governor.clone();
        let task = tokio::spawn(async move {
            waiting
                .reserve(
                    ResourceClass::ThumbnailDecode,
                    PROCESSING_UNIT_BYTES,
                    INFLIGHT_UNIT_BYTES,
                )
                .await
                .unwrap()
        });
        assert!(tokio::time::timeout(Duration::from_millis(20), async {
            while governor.snapshot().pools["processing_memory"].waiters == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok());
        assert!(!task.is_finished());
        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        drop(second);
        assert_eq!(governor.snapshot().pools["processing_memory"].used, 0);
        assert_eq!(governor.snapshot().pools["inflight_media"].used, 0);
        let snapshot = governor.snapshot();
        assert!(snapshot.resource_wait_samples >= 2);
        assert!(snapshot.resource_wait_total_micros > 0);
        assert!(snapshot.resource_wait_max_micros > 0);
        assert_eq!(snapshot.resource_wait_timeouts, 0);
        for pool_name in ["thumbnail_decode", "processing_memory", "inflight_media"] {
            let pool = &snapshot.pools[pool_name];
            assert!(
                pool.wait_samples >= 2,
                "missing wait evidence for {pool_name}"
            );
            assert!(pool.wait_total_micros > 0);
            assert!(pool.wait_max_micros > 0);
            assert_eq!(pool.wait_timeouts, 0);
        }
    }

    #[tokio::test]
    async fn combined_reservation_never_holds_a_partial_pool() {
        let mut limits = ResourceLimits::nas_n100_4g();
        limits.processing_memory_bytes = PROCESSING_UNIT_BYTES;
        limits.inflight_media_bytes = INFLIGHT_UNIT_BYTES;
        let governor = ResourceGovernor::new(limits);
        let inflight_blocker = governor
            .reserve(ResourceClass::ArchiveStream, 0, INFLIGHT_UNIT_BYTES)
            .await
            .unwrap();

        let waiting = governor.clone();
        let task = tokio::spawn(async move {
            waiting
                .reserve(
                    ResourceClass::ThumbnailDecode,
                    PROCESSING_UNIT_BYTES,
                    INFLIGHT_UNIT_BYTES,
                )
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while governor.snapshot().pools["inflight_media"].waiters == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(governor.snapshot().pools["processing_memory"].used, 0);
        assert!(!task.is_finished());

        drop(inflight_blocker);
        let lease = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        drop(lease);
    }

    #[tokio::test]
    async fn memory_pressure_pauses_background_but_not_interactive_work() {
        let governor = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        governor.observe_memory(governor.limits().memory_soft_limit_bytes);
        assert!(governor.snapshot().background_paused);

        let background = governor.clone();
        let task = tokio::spawn(async move {
            background
                .reserve_background(ResourceClass::ScanIo, 0, 0)
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while governor.snapshot().background_waiters == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!task.is_finished());

        let interactive = governor.reserve(ResourceClass::ScanIo, 0, 0).await.unwrap();
        drop(interactive);
        assert!(!task.is_finished());

        governor.observe_memory(governor.limits().memory_resume_limit_bytes);
        let background = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        drop(background);
        assert!(!governor.snapshot().background_paused);
    }

    #[tokio::test]
    async fn queued_interactive_work_has_priority_over_background_work() {
        let mut limits = ResourceLimits::nas_n100_4g();
        limits.archive_workers = 1;
        let governor = ResourceGovernor::new(limits);
        let blocker = governor
            .reserve(ResourceClass::ArchiveStream, 0, 0)
            .await
            .unwrap();

        let interactive_governor = governor.clone();
        let interactive = tokio::spawn(async move {
            interactive_governor
                .reserve(ResourceClass::ArchiveStream, 0, 0)
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while governor.snapshot().interactive_waiters == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let background_governor = governor.clone();
        let background = tokio::spawn(async move {
            background_governor
                .reserve_background(ResourceClass::ArchiveStream, 0, 0)
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while governor.snapshot().background_waiters == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        drop(blocker);
        let interactive_lease = tokio::time::timeout(Duration::from_secs(1), interactive)
            .await
            .unwrap()
            .unwrap();
        assert!(!background.is_finished());
        drop(interactive_lease);
        let background_lease = tokio::time::timeout(Duration::from_secs(1), background)
            .await
            .unwrap()
            .unwrap();
        drop(background_lease);
    }

    #[tokio::test]
    async fn interactive_deadline_returns_controlled_overload() {
        let governor = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let first = governor
            .reserve(ResourceClass::ThumbnailDecode, 0, 0)
            .await
            .unwrap();
        let result = governor
            .reserve_request(ResourceRequest {
                class: ResourceClass::ThumbnailDecode,
                processing_bytes: 0,
                inflight_bytes: 0,
                priority: ResourcePriority::Interactive,
                deadline: Some(Instant::now() + Duration::from_millis(20)),
            })
            .await;
        assert!(matches!(result, Err(AppError::Overloaded { .. })));
        drop(first);
    }

    #[tokio::test]
    async fn cancelling_a_waiter_clears_all_waiter_metrics() {
        let governor = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let first = governor
            .reserve(ResourceClass::ThumbnailDecode, 0, 0)
            .await
            .unwrap();
        let waiting = governor.clone();
        let task = tokio::spawn(async move {
            waiting
                .reserve(ResourceClass::ThumbnailDecode, PROCESSING_UNIT_BYTES, 0)
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while governor.snapshot().interactive_waiters == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        let _ = task.await;
        tokio::task::yield_now().await;
        assert_eq!(governor.snapshot().interactive_waiters, 0);
        assert_eq!(governor.snapshot().pools["thumbnail_decode"].waiters, 0);
        assert_eq!(governor.snapshot().pools["processing_memory"].waiters, 0);
        drop(first);
    }

    #[tokio::test]
    async fn split_lease_releases_work_before_inflight_bytes() {
        let governor = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let lease = governor
            .reserve(
                ResourceClass::ArchiveStream,
                PROCESSING_UNIT_BYTES,
                INFLIGHT_UNIT_BYTES,
            )
            .await
            .unwrap();
        let (work, buffer) = lease.split();
        drop(work);
        let snapshot = governor.snapshot();
        assert_eq!(snapshot.pools["archive_stream"].used, 0);
        assert_eq!(snapshot.pools["processing_memory"].used, 0);
        assert_eq!(snapshot.pools["inflight_media"].used, 1);
        drop(buffer);
        assert_eq!(governor.snapshot().pools["inflight_media"].used, 0);
    }
}
