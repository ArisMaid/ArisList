use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, RwLock};

use tokio::sync::Semaphore;

use crate::config::Config;
use crate::error::{AppError, Result};

const CANONICAL_ROOT_CACHE_LIMIT: usize = 512;
static CANONICAL_ROOT_CACHE: LazyLock<RwLock<HashMap<PathBuf, PathBuf>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
static PATH_CANONICALIZE_WORKERS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(4)));

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
    const MAX_PACKED_SECRET_LEN: usize = 16 * 1024;
    const AES_GCM_NONCE_LEN: usize = 12;
    const AES_GCM_TAG_LEN: usize = 16;

    if packed.len() > MAX_PACKED_SECRET_LEN {
        return Err(AppError::BadRequest(
            "encrypted value is too large".to_string(),
        ));
    }
    let (nonce_b64, ciphertext_b64) = packed
        .split_once('.')
        .ok_or_else(|| AppError::BadRequest("invalid encrypted value".to_string()))?;
    let nonce = STANDARD
        .decode(nonce_b64)
        .map_err(|e| AppError::BadRequest(format!("invalid nonce: {e}")))?;
    let ciphertext = STANDARD
        .decode(ciphertext_b64)
        .map_err(|e| AppError::BadRequest(format!("invalid ciphertext: {e}")))?;
    if nonce.len() != AES_GCM_NONCE_LEN {
        return Err(AppError::BadRequest(format!(
            "invalid nonce length: expected {AES_GCM_NONCE_LEN} bytes"
        )));
    }
    if ciphertext.len() < AES_GCM_TAG_LEN {
        return Err(AppError::BadRequest(
            "invalid ciphertext length".to_string(),
        ));
    }
    let key_bytes = Sha256::digest(secret.as_bytes());
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key_bytes));
    let plaintext = cipher
        .decrypt(Nonce::from_slice(&nonce), ciphertext.as_ref())
        .map_err(|e| AppError::Unauthorized(format!("credential decrypt failed: {e}")))?;
    String::from_utf8(plaintext)
        .map_err(|e| AppError::BadRequest(format!("credential is not utf8: {e}")))
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
        config.coser_picture_dir.as_path(),
        config.generated_dir.as_path(),
    ];
    roots.extend(additional_roots.iter().map(|path| path.as_path()));
    let canonical_roots = roots
        .into_iter()
        .filter_map(cached_canonical_root)
        .collect::<Vec<_>>();

    let mut candidates = vec![PathBuf::from(raw)];
    if let Some(mapped) = legacy_container_asset_path(config, raw) {
        if !candidates.iter().any(|path| path == &mapped) {
            candidates.push(mapped);
        }
    }
    if let Some(mapped) = legacy_root_alias_asset_path(config, raw) {
        if !candidates.iter().any(|path| path == &mapped) {
            candidates.push(mapped);
        }
    }
    if let Some(repaired) = repair_utf8_mojibake_path(raw) {
        let repaired_path = PathBuf::from(&repaired);
        if !candidates.iter().any(|path| path == &repaired_path) {
            candidates.push(repaired_path);
        }
        if let Some(mapped) = legacy_container_asset_path(config, &repaired) {
            if !candidates.iter().any(|path| path == &mapped) {
                candidates.push(mapped);
            }
        }
        if let Some(mapped) = configured_root_name_asset_path(config, &repaired) {
            if !candidates.iter().any(|path| path == &mapped) {
                candidates.push(mapped);
            }
        }
        if let Some(mapped) = legacy_root_alias_asset_path(config, &repaired) {
            if !candidates.iter().any(|path| path == &mapped) {
                candidates.push(mapped);
            }
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

pub async fn ensure_asset_path_allowed_with_roots_async(
    config: &Config,
    raw: &str,
    additional_roots: &[PathBuf],
) -> Result<PathBuf> {
    let permit = PATH_CANONICALIZE_WORKERS
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| AppError::Other("path canonicalizer is closed".to_string()))?;
    let config = config.clone();
    let raw = raw.to_string();
    let additional_roots = additional_roots.to_vec();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        ensure_asset_path_allowed_with_roots(&config, &raw, &additional_roots)
    })
    .await
    .map_err(|err| AppError::Other(format!("path canonicalizer task failed: {err}")))?
}

fn cached_canonical_root(path: &Path) -> Option<PathBuf> {
    if let Ok(cache) = CANONICAL_ROOT_CACHE.read() {
        if let Some(canonical) = cache.get(path) {
            return Some(canonical.clone());
        }
    }

    let canonical = path.canonicalize().ok()?;
    if let Ok(mut cache) = CANONICAL_ROOT_CACHE.write() {
        if cache.len() >= CANONICAL_ROOT_CACHE_LIMIT {
            cache.clear();
        }
        cache.insert(path.to_path_buf(), canonical.clone());
    }
    Some(canonical)
}

fn repair_utf8_mojibake_path(raw: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(raw.len());
    for ch in raw.chars() {
        let value = ch as u32;
        if value > u8::MAX as u32 {
            return None;
        }
        bytes.push(value as u8);
    }
    String::from_utf8(bytes).ok().filter(|value| value != raw)
}

fn legacy_container_asset_path(config: &Config, raw: &str) -> Option<PathBuf> {
    let normalized = raw.trim().replace('\\', "/");
    let mappings = [
        ("/library/comics", &config.comics_dir),
        ("/library/novels", &config.novels_dir),
        ("/library/audio", &config.audio_dir),
        ("/library/gallery", &config.gallery_dir),
        ("/library/coser-picture", &config.coser_picture_dir),
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

fn configured_root_name_asset_path(config: &Config, raw: &str) -> Option<PathBuf> {
    let normalized = raw.trim().replace('\\', "/");
    let (head, tail) = normalized.split_once('/')?;
    let roots = [
        &config.comics_dir,
        &config.novels_dir,
        &config.audio_dir,
        &config.gallery_dir,
        &config.coser_picture_dir,
        &config.generated_dir,
    ];
    let root = roots.iter().find(|root| {
        root.file_name()
            .and_then(|value| value.to_str())
            .map(|name| name == head)
            .unwrap_or(false)
    })?;
    let mut mapped = (*root).clone();
    for part in tail.split('/').filter(|part| !part.is_empty()) {
        mapped.push(part);
    }
    Some(mapped)
}

fn legacy_root_alias_asset_path(config: &Config, raw: &str) -> Option<PathBuf> {
    let normalized = raw.trim().replace('\\', "/");
    let (head, tail) = normalized.split_once('/')?;
    let root = match head {
        "漫画" => &config.comics_dir,
        "轻小说" => &config.novels_dir,
        "音声" => &config.audio_dir,
        "图库" => &config.gallery_dir,
        "COS图" | "CoserPicture" | "coser-picture" => &config.coser_picture_dir,
        _ => return None,
    };
    let mut mapped = root.clone();
    for part in tail.split('/').filter(|part| !part.is_empty()) {
        mapped.push(part);
    }
    Some(mapped)
}

pub fn path_mime(path: &Path) -> String {
    mime_guess::from_path(path)
        .first_or_octet_stream()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    fn test_config(root: &Path) -> Config {
        Config {
            bind: "127.0.0.1:0".to_string(),
            database_url: "sqlite::memory:".to_string(),
            data_dir: root.join("data"),
            cover_cache_dir: root.join("cover-cache"),
            comic_cover_cache_dir: root.join("cover-cache").join("comic"),
            novel_cover_cache_dir: root.join("cover-cache").join("novel"),
            audio_cover_cache_dir: root.join("cover-cache").join("audio"),
            gallery_cover_cache_dir: root.join("cover-cache").join("gallery"),
            coser_picture_cover_cache_dir: root.join("cover-cache").join("coser-picture"),
            comics_dir: root.join("comics"),
            novels_dir: root.join("novels"),
            audio_dir: root.join("audio"),
            gallery_dir: root.join("gallery"),
            coser_picture_dir: root.join("coser-picture"),
            generated_dir: root.join("generated"),
            app_admin_password: "admin".to_string(),
            admin_password_persisted: false,
            admin_password_ephemeral: false,
            lightnovel_api_bases: Vec::new(),
            lightnovel_access_token: None,
            enrichment_concurrency: 1,
            ehtt_url: String::new(),
            openai_api_key: None,
            openai_image_model: "gpt-image-2".to_string(),
            qmediasync_base_url: String::new(),
            cloud_cache_max_bytes: 64 * 1024 * 1024 * 1024,
            thumbnail_cache_max_bytes_per_dir: 8 * 1024 * 1024 * 1024,
            session_secret: "test".to_string(),
            enable_file_watcher: false,
            watch_debounce_seconds: 20,
        }
    }

    #[test]
    fn encrypted_secret_round_trips() {
        let packed = encrypt_secret("test-secret", "session claims").unwrap();

        assert_eq!(
            decrypt_secret("test-secret", &packed).unwrap(),
            "session claims"
        );
    }

    #[test]
    fn malformed_nonce_is_rejected_without_panicking() {
        for nonce_len in [0, 1, 11, 13, 64] {
            let packed = format!(
                "{}.{}",
                STANDARD.encode(vec![0_u8; nonce_len]),
                STANDARD.encode(vec![0_u8; 16])
            );
            let result = catch_unwind(AssertUnwindSafe(|| decrypt_secret("test-secret", &packed)));

            assert!(result.is_ok(), "nonce length {nonce_len} panicked");
            assert!(result.unwrap().is_err());
        }
    }

    #[test]
    fn malformed_cookie_ciphertext_is_bounded_and_rejected() {
        let short_ciphertext = format!(
            "{}.{}",
            STANDARD.encode([0_u8; 12]),
            STANDARD.encode([0_u8; 15])
        );
        assert!(decrypt_secret("test-secret", &short_ciphertext).is_err());

        let oversized = "a".repeat(16 * 1024 + 1);
        assert!(decrypt_secret("test-secret", &oversized).is_err());
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

    #[test]
    fn repairs_utf8_mojibake_paths_when_resolving_assets() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = test_config(temp.path());
        config.coser_picture_dir = temp.path().join("COS图");
        let asset = config.coser_picture_dir.join("Aram").join("set.zip");
        std::fs::create_dir_all(asset.parent().unwrap()).unwrap();
        std::fs::write(&asset, b"zip").unwrap();

        let mojibake = String::from_utf8(vec![
            b'C', b'O', b'S', 0xc3, 0xa5, 0xc2, 0x9b, 0xc2, 0xbe, b'/', b'A', b'r', b'a', b'm',
            b'/', b's', b'e', b't', b'.', b'z', b'i', b'p',
        ])
        .unwrap();
        let resolved = ensure_asset_path_allowed_with_roots(&config, &mojibake, &[]).unwrap();

        assert_eq!(resolved, asset.canonicalize().unwrap());
    }

    #[test]
    fn maps_legacy_coser_root_alias_to_configured_root() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = test_config(temp.path());
        config.coser_picture_dir = temp.path().join("library").join("coser-picture");
        let asset = config.coser_picture_dir.join("Aram").join("set.zip");
        std::fs::create_dir_all(asset.parent().unwrap()).unwrap();
        std::fs::write(&asset, b"zip").unwrap();

        let resolved =
            ensure_asset_path_allowed_with_roots(&config, "COS图/Aram/set.zip", &[]).unwrap();

        assert_eq!(resolved, asset.canonicalize().unwrap());
    }
}
