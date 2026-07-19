use std::path::{Path, PathBuf};
use std::{fs, io::Write};

use tokio::io::AsyncWriteExt;
use uuid::Uuid;

struct TempFileGuard {
    path: Option<PathBuf>,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn disarm(&mut self) {
        self.path = None;
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        let Some(path) = self.path.take() else {
            return;
        };
        if let Err(err) = std::fs::remove_file(&path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(path = %path.display(), error = %err, "failed to clean atomic-write temp file");
            }
        }
    }
}

pub async fn write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    tokio::fs::create_dir_all(parent).await?;
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| std::io::Error::other("atomic write path has no valid file name"))?;
    let temp_path = parent.join(format!(".{file_name}.{}.part", Uuid::new_v4()));
    let mut temp_guard = TempFileGuard::new(temp_path.clone());
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .await?;
    file.write_all(bytes).await?;
    file.flush().await?;
    file.sync_all().await?;
    drop(file);

    match tokio::fs::rename(&temp_path, path).await {
        Ok(()) => {
            temp_guard.disarm();
            Ok(())
        }
        Err(_err) if tokio::fs::try_exists(path).await? => {
            let backup_path = parent.join(format!(".{file_name}.{}.bak", Uuid::new_v4()));
            tokio::fs::rename(path, &backup_path).await?;
            match tokio::fs::rename(&temp_path, path).await {
                Ok(()) => {
                    temp_guard.disarm();
                    if let Err(remove_err) = tokio::fs::remove_file(&backup_path).await {
                        if remove_err.kind() != std::io::ErrorKind::NotFound {
                            tracing::warn!(path = %backup_path.display(), error = %remove_err, "failed to remove atomic-write backup file");
                        }
                    }
                    Ok(())
                }
                Err(publish_err) => {
                    if let Err(restore_err) = tokio::fs::rename(&backup_path, path).await {
                        tracing::error!(path = %path.display(), backup = %backup_path.display(), error = %restore_err, "failed to restore atomic-write backup");
                    }
                    Err(publish_err)
                }
            }
        }
        Err(err) => Err(err),
    }
}

pub fn write_sync(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| std::io::Error::other("atomic write path has no valid file name"))?;
    let temp_path = parent.join(format!(".{file_name}.{}.part", Uuid::new_v4()));
    let mut temp_guard = TempFileGuard::new(temp_path.clone());
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)?;
    file.write_all(bytes)?;
    file.flush()?;
    file.sync_all()?;
    drop(file);

    match fs::rename(&temp_path, path) {
        Ok(()) => {
            temp_guard.disarm();
            Ok(())
        }
        Err(_err) if path.exists() => {
            let backup_path = parent.join(format!(".{file_name}.{}.bak", Uuid::new_v4()));
            fs::rename(path, &backup_path)?;
            match fs::rename(&temp_path, path) {
                Ok(()) => {
                    temp_guard.disarm();
                    if let Err(remove_err) = fs::remove_file(&backup_path) {
                        if remove_err.kind() != std::io::ErrorKind::NotFound {
                            tracing::warn!(path = %backup_path.display(), error = %remove_err, "failed to remove atomic-write backup file");
                        }
                    }
                    Ok(())
                }
                Err(publish_err) => {
                    if let Err(restore_err) = fs::rename(&backup_path, path) {
                        tracing::error!(path = %path.display(), backup = %backup_path.display(), error = %restore_err, "failed to restore atomic-write backup");
                    }
                    Err(publish_err)
                }
            }
        }
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn atomically_creates_and_replaces_one_file() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("settings.json");

        write(&path, b"first").await.unwrap();
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"first");
        write(&path, b"second").await.unwrap();
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"second");
        let siblings = std::fs::read_dir(temp.path()).unwrap().count();
        assert_eq!(siblings, 1);
    }

    #[test]
    fn synchronously_creates_and_replaces_one_file() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("cover.jpg");

        write_sync(&path, b"first").unwrap();
        write_sync(&path, b"second").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"second");
        assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
    }
}
