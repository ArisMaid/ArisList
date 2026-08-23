use serde::Serialize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use tokio::sync::{Semaphore, SemaphorePermit};

use crate::enrich;
use crate::settings;
use crate::AppState;

const MAX_WORKERS: usize = 8;
static MAINTENANCE_JOB_LIMIT: Semaphore = Semaphore::const_new(1);
static ACTIVE_WORKER_LIMIT: AtomicUsize = AtomicUsize::new(1);
static JOB_RUNTIME: OnceLock<JobRuntime> = OnceLock::new();

#[derive(Clone, Default)]
struct JobRuntime {
    inner: Arc<JobRuntimeInner>,
}

#[derive(Default)]
struct JobRuntimeInner {
    worker_limit: AtomicUsize,
    active_jobs: AtomicUsize,
    maintenance_active: AtomicUsize,
    maintenance_wait_samples: AtomicU64,
    maintenance_wait_total_micros: AtomicU64,
    maintenance_wait_max_micros: AtomicU64,
    completed_jobs: AtomicU64,
    failed_jobs: AtomicU64,
    claim_errors: AtomicU64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct JobRuntimeSnapshot {
    pub worker_limit: usize,
    pub active_jobs: usize,
    pub maintenance_active: usize,
    pub maintenance_wait_samples: u64,
    pub maintenance_wait_total_micros: u64,
    pub maintenance_wait_max_micros: u64,
    pub completed_jobs: u64,
    pub failed_jobs: u64,
    pub claim_errors: u64,
}

fn runtime() -> &'static JobRuntime {
    JOB_RUNTIME.get_or_init(JobRuntime::default)
}

pub fn runtime_snapshot() -> JobRuntimeSnapshot {
    let runtime = runtime();
    JobRuntimeSnapshot {
        worker_limit: runtime.inner.worker_limit.load(Ordering::Acquire),
        active_jobs: runtime.inner.active_jobs.load(Ordering::Acquire),
        maintenance_active: runtime.inner.maintenance_active.load(Ordering::Acquire),
        maintenance_wait_samples: runtime
            .inner
            .maintenance_wait_samples
            .load(Ordering::Relaxed),
        maintenance_wait_total_micros: runtime
            .inner
            .maintenance_wait_total_micros
            .load(Ordering::Relaxed),
        maintenance_wait_max_micros: runtime
            .inner
            .maintenance_wait_max_micros
            .load(Ordering::Relaxed),
        completed_jobs: runtime.inner.completed_jobs.load(Ordering::Relaxed),
        failed_jobs: runtime.inner.failed_jobs.load(Ordering::Relaxed),
        claim_errors: runtime.inner.claim_errors.load(Ordering::Relaxed),
    }
}

struct ActiveJobGuard {
    runtime: &'static JobRuntime,
}

impl Drop for ActiveJobGuard {
    fn drop(&mut self) {
        self.runtime
            .inner
            .active_jobs
            .fetch_sub(1, Ordering::AcqRel);
    }
}

fn job_started() -> ActiveJobGuard {
    let runtime = runtime();
    runtime.inner.active_jobs.fetch_add(1, Ordering::AcqRel);
    ActiveJobGuard { runtime }
}

fn record_maintenance_wait(started: Instant) {
    let waited = started.elapsed().as_micros().min(u64::MAX as u128) as u64;
    let runtime = runtime();
    runtime
        .inner
        .maintenance_wait_samples
        .fetch_add(1, Ordering::Relaxed);
    runtime
        .inner
        .maintenance_wait_total_micros
        .fetch_add(waited, Ordering::Relaxed);
    runtime
        .inner
        .maintenance_wait_max_micros
        .fetch_max(waited, Ordering::Relaxed);
    runtime
        .inner
        .maintenance_active
        .fetch_add(1, Ordering::AcqRel);
}

struct MaintenancePermitGuard {
    _permit: SemaphorePermit<'static>,
}

impl Drop for MaintenancePermitGuard {
    fn drop(&mut self) {
        runtime()
            .inner
            .maintenance_active
            .fetch_sub(1, Ordering::AcqRel);
    }
}

pub fn spawn_recovery_worker(state: Arc<AppState>) {
    let initial_limit = state.config.enrichment_concurrency.clamp(1, MAX_WORKERS);
    ACTIVE_WORKER_LIMIT.store(initial_limit, Ordering::Relaxed);
    runtime()
        .inner
        .worker_limit
        .store(initial_limit, Ordering::Release);
    let settings_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            match settings::load_settings(&settings_state.config).await {
                Ok(settings) => {
                    let limit = settings.scan.enrichment_concurrency.clamp(1, MAX_WORKERS);
                    ACTIVE_WORKER_LIMIT.store(limit, Ordering::Relaxed);
                    runtime().inner.worker_limit.store(limit, Ordering::Release);
                }
                Err(err) => {
                    tracing::warn!(error = %err, "failed to load worker concurrency setting");
                }
            }
        }
    });
    for worker_id in 0..MAX_WORKERS {
        let state = state.clone();
        tokio::spawn(async move {
            run_worker(state, worker_id).await;
        });
    }
}

async fn run_worker(state: Arc<AppState>, worker_id: usize) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        interval.tick().await;
        let worker_limit = ACTIVE_WORKER_LIMIT.load(Ordering::Relaxed);
        if worker_id >= worker_limit {
            continue;
        }
        let job = match state.db.claim_next_queued_job().await {
            Ok(Some(job)) => job,
            Ok(None) => continue,
            Err(err) => {
                runtime().inner.claim_errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(worker_id, error = %err, "failed to claim queued job");
                continue;
            }
        };
        let _job_guard = job_started();

        let payload =
            serde_json::from_str(&job.payload_json).unwrap_or_else(|_| serde_json::json!({}));
        let maintenance_permit = if matches!(
            job.job_type.as_str(),
            "scan-library"
                | "rebuild-search-index"
                | crate::search::outbox::REBUILD_SHADOW_SEARCH_JOB_TYPE
                | crate::catalog_reconciliation::RECONCILE_NOVEL_JOB_TYPE
                | crate::catalog_reconciliation::RECONCILE_COMIC_JOB_TYPE
                | crate::catalog_reconciliation::RECONCILE_COSER_PICTURE_JOB_TYPE
                | crate::catalog_reconciliation::RECONCILE_AUDIO_JOB_TYPE
                | crate::catalog_reconciliation::RECONCILE_GALLERY_JOB_TYPE
        ) {
            let wait_started = Instant::now();
            let permit = MAINTENANCE_JOB_LIMIT
                .acquire()
                .await
                .expect("semaphore open");
            record_maintenance_wait(wait_started);
            Some(MaintenancePermitGuard { _permit: permit })
        } else {
            None
        };
        let outcome = enrich::run_job(state.clone(), job.id, &job.job_type, payload).await;
        drop(maintenance_permit);
        match outcome {
            Ok(()) => {
                runtime()
                    .inner
                    .completed_jobs
                    .fetch_add(1, Ordering::Relaxed);
                if let Err(err) = state.db.update_job(job.id, "done", None).await {
                    tracing::error!(
                        worker_id,
                        job_id = job.id,
                        job_type = %job.job_type,
                        error = %err,
                        "job completed but its final status could not be persisted"
                    );
                }
            }
            Err(err) => {
                runtime().inner.failed_jobs.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(worker_id, job_id = job.id, job_type = %job.job_type, error = %err, "job failed");
                let attempt = job.attempts.max(1);
                if attempt < 3 {
                    let delay = 30 * attempt;
                    if let Err(status_err) = state
                        .db
                        .reschedule_job(job.id, &err.to_string(), delay)
                        .await
                    {
                        tracing::error!(
                            worker_id,
                            job_id = job.id,
                            error = %status_err,
                            "failed to persist job retry status"
                        );
                    }
                } else {
                    if let Err(status_err) = state
                        .db
                        .update_job(job.id, "failed", Some(&err.to_string()))
                        .await
                    {
                        tracing::error!(
                            worker_id,
                            job_id = job.id,
                            error = %status_err,
                            "failed to persist terminal job failure"
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maintenance_permit_wait_records_bounded_timing() {
        let before = runtime_snapshot();
        record_maintenance_wait(Instant::now() - Duration::from_millis(1));
        let during = runtime_snapshot();
        assert_eq!(
            during.maintenance_wait_samples,
            before.maintenance_wait_samples + 1
        );
        assert!(during.maintenance_wait_total_micros >= before.maintenance_wait_total_micros);
        assert!(during.maintenance_wait_max_micros > 0);
        assert_eq!(during.maintenance_active, before.maintenance_active + 1);
        runtime()
            .inner
            .maintenance_active
            .fetch_sub(1, Ordering::AcqRel);
        assert_eq!(
            runtime_snapshot().maintenance_active,
            before.maintenance_active
        );
    }
}
