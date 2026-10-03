//! Small helpers shared by STRM cache and preparation jobs.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// Build a stable cache component without putting credentials or a plaintext
/// password into a filename.
pub fn stable_key(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn entry_cache_path(root: &Path, parent: &str, source_version: &str, entry: &str) -> PathBuf {
    let key = stable_key(&[parent, source_version, entry]);
    let extension = Path::new(entry)
        .extension()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("bin");
    root.join("cloud-cache")
        .join(format!("strm-entry-{key}.{extension}"))
}

// Serialize cold page preparation and eviction. The caller opens its stream
// before releasing this gate; eviction never needs a directory-wide delete.
pub static PAGE_CACHE_GATE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub async fn reserve_page_space(
    directory: &Path,
    incoming: u64,
    keep: &Path,
) -> crate::error::Result<()> {
    let directory = directory.to_path_buf();
    let keep = keep.to_path_buf();
    tokio::task::spawn_blocking(move || -> crate::error::Result<()> {
        use crate::error::AppError;
        use std::time::{Duration, SystemTime};
        let limit = std::env::var("STRM_PAGE_CACHE_MAX_BYTES")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(512 * 1024 * 1024);
        if incoming > limit.min(128 * 1024 * 1024) {
            return Err(AppError::BadRequest(
                "comic page exceeds the original-page cache budget".to_string(),
            ));
        }
        std::fs::create_dir_all(&directory)?;
        let now = SystemTime::now();
        let mut files = Vec::new();
        let mut used = 0_u64;
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            // Never follow links or touch files outside our owned namespace.
            if !entry.file_type()?.is_file() {
                continue;
            }
            let path = entry.path();
            if !entry
                .file_name()
                .to_string_lossy()
                .starts_with("strm-entry-")
            {
                continue;
            }
            let metadata = entry.metadata()?;
            let modified = metadata.modified().unwrap_or(now);
            let age = now.duration_since(modified).unwrap_or_default();
            if path != keep
                && age >= Duration::from_secs(24 * 60 * 60)
                && std::fs::remove_file(&path).is_ok()
            {
                continue;
            }
            used = used.saturating_add(metadata.len());
            files.push((modified, path, metadata.len()));
        }
        if used.saturating_add(incoming) > limit {
            files.sort_by_key(|file| file.0);
            let target = (limit.saturating_mul(3) / 4).max(incoming);
            for (modified, path, size) in files {
                if used.saturating_add(incoming) <= target {
                    break;
                }
                // Protect recently served pages; retry later instead of deleting
                // the page a reader is about to display.
                if path == keep
                    || now.duration_since(modified).unwrap_or_default() < Duration::from_secs(60)
                {
                    continue;
                }
                if std::fs::remove_file(&path).is_ok() {
                    used = used.saturating_sub(size);
                }
            }
        }
        if used.saturating_add(incoming) > limit {
            return Err(AppError::Overloaded {
                message: "comic page cache is full with recently used pages; retry later"
                    .to_string(),
                retry_after_seconds: 60,
            });
        }
        Ok(())
    })
    .await
    .map_err(|error| crate::error::AppError::Other(error.to_string()))?
}

pub async fn touch_page(path: &Path) -> crate::error::Result<()> {
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .await?
        .into_std()
        .await;
    file.set_modified(std::time::SystemTime::now())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_key_is_stable_and_does_not_expose_entry_text() {
        let path = entry_cache_path(Path::new("data"), "asset-1", "etag", "folder/secret.mp3");
        assert!(path.to_string_lossy().contains(".mp3"));
        assert!(!path.to_string_lossy().contains("secret"));
    }
}
