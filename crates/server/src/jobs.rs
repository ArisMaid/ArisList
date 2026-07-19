use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::enrich;
use crate::settings;
use crate::AppState;

const MAX_WORKERS: usize = 8;
static MAINTENANCE_JOB_LIMIT: Semaphore = Semaphore::const_new(1);
static ACTIVE_WORKER_LIMIT: AtomicUsize = AtomicUsize::new(1);

pub fn spawn_recovery_worker(state: Arc<AppState>) {
    ACTIVE_WORKER_LIMIT.store(
        state.config.enrichment_concurrency.clamp(1, MAX_WORKERS),
        Ordering::Relaxed,
    );
    let settings_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            match settings::load_settings(&settings_state.config).await {
                Ok(settings) => ACTIVE_WORKER_LIMIT.store(
                    settings.scan.enrichment_concurrency.clamp(1, MAX_WORKERS),
                    Ordering::Relaxed,
                ),
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
                tracing::warn!(worker_id, error = %err, "failed to claim queued job");
                continue;
            }
        };

        let payload =
            serde_json::from_str(&job.payload_json).unwrap_or_else(|_| serde_json::json!({}));
        let maintenance_permit = if matches!(
            job.job_type.as_str(),
            "scan-library" | "rebuild-search-index"
        ) {
            Some(
                MAINTENANCE_JOB_LIMIT
                    .acquire()
                    .await
                    .expect("semaphore open"),
            )
        } else {
            None
        };
        let outcome = enrich::run_job(state.clone(), job.id, &job.job_type, payload).await;
        drop(maintenance_permit);
        match outcome {
            Ok(()) => {
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
