use std::sync::Arc;
use std::time::Duration;

use crate::enrich;
use crate::AppState;

pub fn spawn_recovery_worker(state: Arc<AppState>) {
    let workers = state.config.enrichment_concurrency.clamp(1, 8);
    for worker_id in 0..workers {
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
        match enrich::run_job(state.clone(), job.id, &job.job_type, payload).await {
            Ok(()) => {
                let _ = state.db.update_job(job.id, "done", None).await;
            }
            Err(err) => {
                tracing::warn!(worker_id, job_id = job.id, job_type = %job.job_type, error = %err, "job failed");
                let attempt = job.attempts.max(1);
                if attempt < 3 {
                    let delay = 30 * attempt;
                    let _ = state
                        .db
                        .reschedule_job(job.id, &err.to_string(), delay)
                        .await;
                } else {
                    let _ = state
                        .db
                        .update_job(job.id, "failed", Some(&err.to_string()))
                        .await;
                }
            }
        }
    }
}
