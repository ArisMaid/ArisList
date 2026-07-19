use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use notify::{Event, EventKind, RecursiveMode, Watcher};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time::{interval, Instant};

use crate::error::{AppError, Result};
use crate::settings;
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
        let _ = tx.try_send(event);
    })
    .map_err(watch_error)?;

    let debounce = Duration::from_secs(state.config.watch_debounce_seconds.max(3));
    let mut last_event: Option<Instant> = None;
    let mut ticker = interval(Duration::from_secs(2));
    let mut watched_roots = HashSet::new();
    let mut watcher_enabled = false;

    loop {
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
                match settings::load_settings(&state.config).await {
                    Ok(app_settings) => {
                        watcher_enabled = app_settings.scan.file_watcher;
                        let roots = if watcher_enabled {
                            app_settings.all_media_roots()
                        } else {
                            Vec::new()
                        };
                        reconcile_watched_roots(
                            &mut watcher,
                            &mut watched_roots,
                            roots,
                        );
                    }
                    Err(err) => tracing::warn!(error = %err, "failed to refresh watcher settings"),
                }
                if watcher_enabled && last_event.map(|instant| instant.elapsed() >= debounce).unwrap_or(false) {
                    if let Err(err) = queue_scan(&state).await {
                        tracing::warn!(error = %err, "failed to queue watcher scan");
                    }
                    last_event = None;
                }
            }
        }
    }
}

async fn queue_scan(state: &AppState) -> Result<()> {
    let (id, created) = state
        .db
        .create_job_if_absent(
            "scan-library",
            "queued",
            json!({ "source": "watcher", "enqueue_enrichment": false }),
        )
        .await?;
    if created {
        state
            .db
            .audit("watch.scan", "queued", json!({ "job_id": id }))
            .await?;
    }
    Ok(())
}

fn reconcile_watched_roots(
    watcher: &mut impl Watcher,
    watched: &mut HashSet<PathBuf>,
    desired: Vec<PathBuf>,
) {
    let desired = desired
        .into_iter()
        .filter(|root| root.exists() && root.is_dir())
        .collect::<HashSet<_>>();

    for root in watched.difference(&desired).cloned().collect::<Vec<_>>() {
        if let Err(err) = watcher.unwatch(&root) {
            tracing::warn!(path = %root.display(), error = %err, "failed to stop watching library path");
        }
        watched.remove(&root);
    }
    for root in desired.difference(watched).cloned().collect::<Vec<_>>() {
        match watcher.watch(&root, RecursiveMode::Recursive) {
            Ok(()) => {
                tracing::info!(path = %root.display(), "watching library path");
                watched.insert(root);
            }
            Err(err) => {
                tracing::warn!(path = %root.display(), error = %err, "failed to watch library path")
            }
        }
    }
}

fn is_media_event(event: &Event) -> bool {
    if !matches!(
        event.kind,
        EventKind::Any | EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
    ) {
        return false;
    }
    matches!(event.kind, EventKind::Remove(_)) || event.paths.iter().any(|path| is_media_path(path))
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
                    | "gif"
                    | "avif"
                    | "bmp"
                    | "zip"
                    | "strm"
                    | "txt"
            )
        })
        .unwrap_or(false)
}

fn watch_error(error: notify::Error) -> AppError {
    AppError::Other(format!("library watcher error: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_all_scanner_media_extensions() {
        for name in [
            "book.cbz",
            "book.epub",
            "remote.strm",
            "image.gif",
            "image.avif",
            "image.bmp",
            "audio.flac",
        ] {
            assert!(is_media_path(Path::new(name)), "missed {name}");
        }
        assert!(!is_media_path(Path::new("notes.md")));
    }

    #[test]
    fn remove_events_trigger_reconciliation_even_without_an_extension() {
        let event = Event {
            kind: EventKind::Remove(notify::event::RemoveKind::Folder),
            paths: vec![PathBuf::from("gallery/deleted-folder")],
            attrs: Default::default(),
        };

        assert!(is_media_event(&event));
    }
}
