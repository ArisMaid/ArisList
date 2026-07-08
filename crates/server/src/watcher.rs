use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use notify::{Event, EventKind, RecursiveMode, Watcher};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time::{interval, Instant};

use crate::error::{AppError, Result};
use crate::AppState;

pub fn spawn_library_watcher(state: Arc<AppState>) {
    tokio::spawn(async move {
        if let Err(err) = run_library_watcher(state).await {
            tracing::warn!(error = %err, "library file watcher stopped");
        }
    });
}

async fn run_library_watcher(state: Arc<AppState>) -> Result<()> {
    let (tx, mut rx) = mpsc::channel(128);
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<Event>| {
        let _ = tx.blocking_send(event);
    })
    .map_err(watch_error)?;

    for root in [
        state.config.comics_dir.as_path(),
        state.config.novels_dir.as_path(),
        state.config.audio_dir.as_path(),
        state.config.gallery_dir.as_path(),
        state.config.coser_picture_dir.as_path(),
    ] {
        if root.exists() {
            watcher
                .watch(root, RecursiveMode::Recursive)
                .map_err(watch_error)?;
            tracing::info!(path = %root.display(), "watching library path");
        }
    }

    let debounce = Duration::from_secs(state.config.watch_debounce_seconds.max(3));
    let mut last_event: Option<Instant> = None;
    let mut ticker = interval(Duration::from_secs(2));

    loop {
        let _ = &watcher;
        tokio::select! {
            event = rx.recv() => {
                let Some(event) = event else {
                    return Ok(());
                };
                match event {
                    Ok(event) if is_media_event(&event) => {
                        last_event = Some(Instant::now());
                    }
                    Ok(_) => {}
                    Err(err) => tracing::warn!(error = %err, "library watcher event failed"),
                }
            }
            _ = ticker.tick() => {
                if last_event.map(|instant| instant.elapsed() >= debounce).unwrap_or(false) {
                    queue_scan(&state).await?;
                    last_event = None;
                }
            }
        }
    }
}

async fn queue_scan(state: &AppState) -> Result<()> {
    let id = state
        .db
        .create_job(
            "scan-library",
            "queued",
            json!({ "source": "watcher", "enqueue_enrichment": false }),
        )
        .await?;
    state
        .db
        .audit("watch.scan", "queued", json!({ "job_id": id }))
        .await?;
    Ok(())
}

fn is_media_event(event: &Event) -> bool {
    if !matches!(
        event.kind,
        EventKind::Any | EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
    ) {
        return false;
    }
    event.paths.iter().any(|path| is_media_path(path))
}

fn is_media_path(path: &Path) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .map(|ext| {
            matches!(
                ext.to_ascii_lowercase().as_str(),
                "cbz"
                    | "xml"
                    | "epub"
                    | "mp3"
                    | "wav"
                    | "flac"
                    | "ogg"
                    | "m4a"
                    | "jpg"
                    | "jpeg"
                    | "png"
                    | "webp"
                    | "zip"
                    | "txt"
            )
        })
        .unwrap_or(false)
}

fn watch_error(error: notify::Error) -> AppError {
    AppError::Other(format!("library watcher error: {error}"))
}
