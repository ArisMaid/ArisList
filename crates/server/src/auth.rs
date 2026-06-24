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

use crate::error::{AppError, Result};
use crate::security::{decrypt_secret, encrypt_secret};
use crate::AppState;

const SESSION_COOKIE: &str = "media_shelf_session";
const CSRF_HEADER: &str = "x-csrf-token";

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
    match read_session(&state, &headers) {
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
    if input.password != admin_password(&state).await? {
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
    save_admin_password(&state, &password).await?;
    state
        .db
        .audit("auth.password", "changed", json!({ "user": "admin" }))
        .await?;
    Ok(Json(json!({ "status": "saved" })))
}

pub async fn reset_password(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>> {
    if !is_private_host(&headers) {
        state
            .db
            .audit(
                "auth.password-reset",
                "denied",
                json!({ "reason": "not-private-host" }),
            )
            .await?;
        return Err(AppError::Unauthorized(
            "password reset is only available from local or intranet access".to_string(),
        ));
    }
    save_admin_password(&state, "admin").await?;
    state
        .db
        .audit(
            "auth.password-reset",
            "reset",
            json!({ "value": "default" }),
        )
        .await?;
    Ok(Json(json!({ "status": "reset", "password": "admin" })))
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
    let claims = read_session(state, headers)?;
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

fn read_session(state: &AppState, headers: &HeaderMap) -> Result<SessionClaims> {
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
    Ok(claims)
}

fn cookie_lookup(header: &str, name: &str) -> Option<String> {
    header.split(';').find_map(|part| {
        let (key, value) = part.trim().split_once('=')?;
        (key == name).then(|| value.to_string())
    })
}

fn cookie_value(value: &str) -> String {
    value.replace(';', "").replace(',', "")
}

fn random_token() -> String {
    let mut bytes = [0_u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

async fn admin_password(state: &AppState) -> Result<String> {
    let path = admin_password_path(state);
    if path.exists() {
        let password = tokio::fs::read_to_string(path).await?;
        let password = password.trim().to_string();
        if !password.is_empty() {
            return Ok(password);
        }
    }
    Ok(state.config.app_admin_password.clone())
}

async fn save_admin_password(state: &AppState, password: &str) -> Result<()> {
    tokio::fs::create_dir_all(&state.config.data_dir).await?;
    tokio::fs::write(admin_password_path(state), password).await?;
    Ok(())
}

fn admin_password_path(state: &AppState) -> std::path::PathBuf {
    state.config.data_dir.join("admin-password.txt")
}

fn normalized_password(value: &str) -> Result<String> {
    let value = value.trim();
    if value.len() < 4 {
        return Err(AppError::BadRequest(
            "admin password must be at least 4 characters".to_string(),
        ));
    }
    Ok(value.to_string())
}

fn is_private_host(headers: &HeaderMap) -> bool {
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or_else(|| {
            headers
                .get(header::HOST)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
        })
        .trim_matches(['[', ']'])
        .to_ascii_lowercase();
    host == "localhost"
        || host == "host.docker.internal"
        || host.starts_with("127.")
        || host == "::1"
        || host.starts_with("10.")
        || host.starts_with("192.168.")
        || private_172_host(&host)
}

fn private_172_host(host: &str) -> bool {
    let mut parts = host.split('.');
    if parts.next() != Some("172") {
        return false;
    }
    parts
        .next()
        .and_then(|value| value.parse::<u8>().ok())
        .is_some_and(|value| (16..=31).contains(&value))
}
