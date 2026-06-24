use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

use crate::config::Config;
use crate::error::{AppError, Result};

pub fn encrypt_secret(secret: &str, plaintext: &str) -> Result<String> {
    let key_bytes = Sha256::digest(secret.as_bytes());
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key_bytes));
    let mut nonce_bytes = [0_u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|e| AppError::Other(format!("credential encryption failed: {e}")))?;
    Ok(format!(
        "{}.{}",
        STANDARD.encode(nonce_bytes),
        STANDARD.encode(ciphertext)
    ))
}

#[allow(dead_code)]
pub fn decrypt_secret(secret: &str, packed: &str) -> Result<String> {
    let (nonce_b64, ciphertext_b64) = packed
        .split_once('.')
        .ok_or_else(|| AppError::BadRequest("invalid encrypted value".to_string()))?;
    let nonce = STANDARD
        .decode(nonce_b64)
        .map_err(|e| AppError::BadRequest(format!("invalid nonce: {e}")))?;
    let ciphertext = STANDARD
        .decode(ciphertext_b64)
        .map_err(|e| AppError::BadRequest(format!("invalid ciphertext: {e}")))?;
    let key_bytes = Sha256::digest(secret.as_bytes());
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key_bytes));
    let plaintext = cipher
        .decrypt(Nonce::from_slice(&nonce), ciphertext.as_ref())
        .map_err(|e| AppError::Unauthorized(format!("credential decrypt failed: {e}")))?;
    String::from_utf8(plaintext)
        .map_err(|e| AppError::BadRequest(format!("credential is not utf8: {e}")))
}

pub fn ensure_asset_path_allowed(config: &Config, raw: &str) -> Result<PathBuf> {
    ensure_asset_path_allowed_with_roots(config, raw, &[])
}

pub fn ensure_asset_path_allowed_with_roots(
    config: &Config,
    raw: &str,
    additional_roots: &[PathBuf],
) -> Result<PathBuf> {
    let mut roots = vec![
        config.comics_dir.as_path(),
        config.novels_dir.as_path(),
        config.audio_dir.as_path(),
        config.gallery_dir.as_path(),
        config.generated_dir.as_path(),
    ];
    roots.extend(additional_roots.iter().map(|path| path.as_path()));
    let canonical_roots = roots
        .into_iter()
        .filter_map(|p| p.canonicalize().ok())
        .collect::<Vec<_>>();

    let mut candidates = vec![PathBuf::from(raw)];
    if let Some(mapped) = legacy_container_asset_path(config, raw) {
        if !candidates.iter().any(|path| path == &mapped) {
            candidates.push(mapped);
        }
    }

    let mut last_io_error = None;
    let mut found_outside_root = false;
    for candidate in candidates {
        match candidate.canonicalize() {
            Ok(canonical) => {
                if canonical_roots
                    .iter()
                    .any(|root| canonical.starts_with(root))
                {
                    return Ok(canonical);
                }
                found_outside_root = true;
            }
            Err(err) => last_io_error = Some(err),
        }
    }

    if found_outside_root {
        return Err(AppError::Unauthorized(format!(
            "asset path is outside configured libraries: {raw}"
        )));
    }
    Err(last_io_error
        .map(AppError::from)
        .unwrap_or_else(|| AppError::Unauthorized(format!("asset path is not readable: {raw}"))))
}

fn legacy_container_asset_path(config: &Config, raw: &str) -> Option<PathBuf> {
    let normalized = raw.trim().replace('\\', "/");
    let mappings = [
        ("/library/comics", &config.comics_dir),
        ("/library/novels", &config.novels_dir),
        ("/library/audio", &config.audio_dir),
        ("/library/gallery", &config.gallery_dir),
        ("/app/generated", &config.generated_dir),
    ];
    for (prefix, root) in mappings {
        if normalized == prefix || normalized.starts_with(&format!("{prefix}/")) {
            let tail = normalized[prefix.len()..].trim_start_matches('/');
            let mut mapped = root.clone();
            for part in tail.split('/').filter(|part| !part.is_empty()) {
                mapped.push(part);
            }
            return Some(mapped);
        }
    }
    None
}

pub fn path_mime(path: &Path) -> String {
    mime_guess::from_path(path)
        .first_or_octet_stream()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(root: &Path) -> Config {
        Config {
            bind: "127.0.0.1:0".to_string(),
            database_url: "sqlite::memory:".to_string(),
            data_dir: root.join("data"),
            comics_dir: root.join("comics"),
            novels_dir: root.join("novels"),
            audio_dir: root.join("audio"),
            gallery_dir: root.join("gallery"),
            generated_dir: root.join("generated"),
            app_admin_password: "admin".to_string(),
            lightnovel_api_bases: Vec::new(),
            lightnovel_access_token: None,
            enrichment_concurrency: 1,
            ehtt_url: String::new(),
            openai_api_key: None,
            openai_image_model: "gpt-image-2".to_string(),
            qmediasync_base_url: String::new(),
            session_secret: "test".to_string(),
            enable_file_watcher: false,
            watch_debounce_seconds: 20,
        }
    }

    #[test]
    fn maps_legacy_library_paths_to_configured_roots() {
        let temp = tempfile::tempdir().unwrap();
        let config = test_config(temp.path());
        let asset = config.comics_dir.join("book").join("cover.jpg");
        std::fs::create_dir_all(asset.parent().unwrap()).unwrap();
        std::fs::write(&asset, b"jpg").unwrap();

        let resolved =
            ensure_asset_path_allowed_with_roots(&config, "/library/comics/book/cover.jpg", &[])
                .unwrap();

        assert_eq!(resolved, asset.canonicalize().unwrap());
    }

    #[test]
    fn maps_legacy_generated_paths_to_configured_root() {
        let temp = tempfile::tempdir().unwrap();
        let config = test_config(temp.path());
        let asset = config.generated_dir.join("covers").join("1.jpg");
        std::fs::create_dir_all(asset.parent().unwrap()).unwrap();
        std::fs::write(&asset, b"jpg").unwrap();

        let resolved =
            ensure_asset_path_allowed_with_roots(&config, "/app/generated/covers/1.jpg", &[])
                .unwrap();

        assert_eq!(resolved, asset.canonicalize().unwrap());
    }
}
