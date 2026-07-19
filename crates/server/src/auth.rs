use std::sync::Arc;

use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{Duration, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::Ordering;
use tokio::io::AsyncReadExt;

use crate::error::{AppError, Result};
use crate::security::{decrypt_secret, encrypt_secret};
use crate::AppState;

const SESSION_COOKIE: &str = "media_shelf_session";
const CSRF_HEADER: &str = "x-csrf-token";
const MAX_ADMIN_PASSWORD_BYTES: usize = 256;
const MAX_ADMIN_PASSWORD_FILE_BYTES: u64 = 4 * 1024;

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct ChangePasswordRequest {
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionClaims {
    pub user: String,
    pub csrf: String,
    pub exp: i64,
    pub auth_epoch: String,
}

#[derive(Debug, Serialize)]
pub struct SessionResponse {
    pub authenticated: bool,
    pub csrf: Option<String>,
    pub user: Option<String>,
}

pub async fn session(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<SessionResponse>> {
    match read_session(&state, &headers).await {
        Ok(claims) => Ok(Json(SessionResponse {
            authenticated: true,
            csrf: Some(claims.csrf),
            user: Some(claims.user),
        })),
        Err(_) => Ok(Json(SessionResponse {
            authenticated: false,
            csrf: None,
            user: None,
        })),
    }
}

pub async fn login(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    Json(input): Json<LoginRequest>,
) -> Result<Response> {
    // Keep the credential read and epoch snapshot under one lock. Otherwise a
    // password change between those two operations could issue a session with
    // the new epoch after authenticating the old password.
    let (password_matches, auth_epoch) = {
        let auth_epoch = state.auth_epoch.read().await;
        (
            input.password == admin_password(&state).await?,
            auth_epoch.clone(),
        )
    };
    if !password_matches {
        state
            .db
            .audit("auth.login", "denied", json!({ "reason": "bad-password" }))
            .await?;
        return Err(AppError::Unauthorized("invalid admin password".to_string()));
    }

    let claims = SessionClaims {
        user: "admin".to_string(),
        csrf: random_token(),
        exp: (Utc::now() + Duration::hours(12)).timestamp(),
        auth_epoch,
    };
    let encrypted = encrypt_secret(
        &state.config.session_secret,
        &serde_json::to_string(&claims).map_err(|e| AppError::Other(e.to_string()))?,
    )?;
    state
        .db
        .audit("auth.login", "ok", json!({ "user": claims.user }))
        .await?;

    let mut response = Json(SessionResponse {
        authenticated: true,
        csrf: Some(claims.csrf),
        user: Some(claims.user),
    })
    .into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&format!(
            "{SESSION_COOKIE}={}; Path=/; HttpOnly; SameSite=Lax; Max-Age=43200",
            cookie_value(&encrypted)
        ))
        .map_err(|e| AppError::Other(e.to_string()))?,
    );
    Ok(response)
}

pub async fn change_password(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    Json(input): Json<ChangePasswordRequest>,
) -> Result<Json<serde_json::Value>> {
    require_csrf(&state, &headers, "auth.password").await?;
    let password = normalized_password(&input.password)?;
    // A login holds the read side of this lock while reading the password and
    // epoch. Holding the write side across publication makes the two values a
    // single credential generation from the point of view of every login.
    let mut auth_epoch = state.auth_epoch.write().await;
    save_admin_password(&state, &password).await?;
    state
        .admin_password_persisted
        .store(true, Ordering::Release);
    *auth_epoch = random_token();
    drop(auth_epoch);
    // The credential has already been durably published and all old sessions
    // invalidated. An audit sink failure must not report the password change as
    // failed after that irreversible point.
    if let Err(err) = state
        .db
        .audit("auth.password", "changed", json!({ "user": "admin" }))
        .await
    {
        tracing::warn!(error = %err, "password changed but audit logging failed");
    }
    Ok(Json(json!({ "status": "saved" })))
}

pub async fn logout(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
) -> Result<Response> {
    state.db.audit("auth.logout", "ok", json!({})).await?;
    let mut response = (StatusCode::NO_CONTENT, "").into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_static("media_shelf_session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"),
    );
    Ok(response)
}

pub async fn require_csrf(
    state: &AppState,
    headers: &HeaderMap,
    action: &str,
) -> Result<SessionClaims> {
    let claims = read_session(state, headers).await?;
    let header = headers
        .get(CSRF_HEADER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if header != claims.csrf {
        state
            .db
            .audit(action, "denied", json!({ "reason": "csrf" }))
            .await?;
        return Err(AppError::Unauthorized(
            "missing or invalid CSRF token".to_string(),
        ));
    }
    Ok(claims)
}

async fn read_session(state: &AppState, headers: &HeaderMap) -> Result<SessionClaims> {
    let cookie = headers
        .get(header::COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| cookie_lookup(value, SESSION_COOKIE))
        .ok_or_else(|| AppError::Unauthorized("admin session is required".to_string()))?;
    let decrypted = decrypt_secret(&state.config.session_secret, &cookie)?;
    let claims = serde_json::from_str::<SessionClaims>(&decrypted)
        .map_err(|e| AppError::Unauthorized(format!("invalid session: {e}")))?;
    if claims.exp < Utc::now().timestamp() {
        return Err(AppError::Unauthorized("session expired".to_string()));
    }
    if claims.auth_epoch != *state.auth_epoch.read().await {
        return Err(AppError::Unauthorized(
            "session was invalidated by an administrator credential change".to_string(),
        ));
    }
    Ok(claims)
}

fn cookie_lookup(header: &str, name: &str) -> Option<String> {
    header.split(';').find_map(|part| {
        let (key, value) = part.trim().split_once('=')?;
        (key == name).then(|| value.to_string())
    })
}

fn cookie_value(value: &str) -> String {
    value.replace([';', ','], "")
}

fn random_token() -> String {
    let mut bytes = [0_u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

async fn admin_password(state: &AppState) -> Result<String> {
    let path = admin_password_path(state);
    let file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if !state.admin_password_persisted.load(Ordering::Acquire) {
                return Ok(state.config.app_admin_password.clone());
            }
            return Err(AppError::Other(
                "persisted admin password file is temporarily unavailable".to_string(),
            ));
        }
        Err(err) if allow_loopback_password_fallback(state) => {
            tracing::warn!(error = %err, "using the validated in-memory admin password on loopback");
            return Ok(state.config.app_admin_password.clone());
        }
        Err(err) => return Err(err.into()),
    };
    let mut bytes = Vec::new();
    if let Err(err) = file
        .take(MAX_ADMIN_PASSWORD_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .await
    {
        if allow_loopback_password_fallback(state) {
            tracing::warn!(error = %err, "using the validated in-memory admin password on loopback");
            return Ok(state.config.app_admin_password.clone());
        }
        return Err(err.into());
    }
    if bytes.len() as u64 > MAX_ADMIN_PASSWORD_FILE_BYTES {
        return invalid_password_file(state, "admin password file is too large");
    }
    let password = match std::str::from_utf8(&bytes) {
        Ok(password) => password.trim().to_string(),
        Err(_) => return invalid_password_file(state, "admin password file is not valid UTF-8"),
    };
    if !password.is_empty() {
        return Ok(password);
    }
    invalid_password_file(state, "admin password file is empty")
}

fn invalid_password_file(state: &AppState, reason: &str) -> Result<String> {
    if allow_loopback_password_fallback(state) {
        tracing::warn!(
            reason,
            "using the validated in-memory admin password on loopback"
        );
        return Ok(state.config.app_admin_password.clone());
    }
    Err(AppError::Other(reason.to_string()))
}

fn allow_loopback_password_fallback(state: &AppState) -> bool {
    allow_password_fallback(
        state.config.is_loopback_bind(),
        state.admin_password_persisted.load(Ordering::Acquire),
    )
}

fn allow_password_fallback(loopback: bool, persisted: bool) -> bool {
    loopback && !persisted
}

async fn save_admin_password(state: &AppState, password: &str) -> Result<()> {
    crate::atomic_file::write(&admin_password_path(state), password.as_bytes()).await?;
    Ok(())
}

fn admin_password_path(state: &AppState) -> std::path::PathBuf {
    state.config.data_dir.join("admin-password.txt")
}

fn normalized_password(value: &str) -> Result<String> {
    let value = value.trim();
    if value.len() < 8 {
        return Err(AppError::BadRequest(
            "admin password must be at least 8 characters".to_string(),
        ));
    }
    if value.len() > MAX_ADMIN_PASSWORD_BYTES {
        return Err(AppError::BadRequest(format!(
            "password must not exceed {MAX_ADMIN_PASSWORD_BYTES} bytes"
        )));
    }
    if crate::config::is_weak_admin_password(value) {
        return Err(AppError::BadRequest(
            "admin password must not be a common, repetitive, or all-numeric password".to_string(),
        ));
    }
    Ok(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_change_requires_at_least_eight_characters() {
        assert!(normalized_password("1234567").is_err());
        assert!(normalized_password(" 12345678 ").is_err());
        assert_eq!(
            normalized_password(" correct horse battery staple ").unwrap(),
            "correct horse battery staple"
        );
    }

    #[test]
    fn persisted_password_is_authoritative_even_on_loopback() {
        assert!(allow_password_fallback(true, false));
        assert!(!allow_password_fallback(true, true));
        assert!(!allow_password_fallback(false, false));
        assert!(!allow_password_fallback(false, true));
    }
}
