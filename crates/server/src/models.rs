use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqliteRow, FromRow, Row};

macro_rules! impl_sqlite_from_row {
    ($model:ty { $($field:ident),+ $(,)? }) => {
        impl<'r> FromRow<'r, SqliteRow> for $model {
            fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
                Ok(Self {
                    $($field: row.try_get(stringify!($field))?,)+
                })
            }
        }
    };
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum WorkKind {
    Comic,
    Novel,
    Audio,
    Generated,
    Gallery,
    CoserPicture,
}

impl WorkKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkKind::Comic => "comic",
            WorkKind::Novel => "novel",
            WorkKind::Audio => "audio",
            WorkKind::Generated => "generated",
            WorkKind::Gallery => "gallery",
            WorkKind::CoserPicture => "coser-picture",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Work {
    pub id: i64,
    pub kind: String,
    pub title: String,
    pub subtitle: Option<String>,
    pub category: Option<String>,
    pub description: Option<String>,
    pub rating: Option<f64>,
    pub progress: f64,
    pub source_path: Option<String>,
    pub cover_asset_id: Option<i64>,
    pub meta_json: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl_sqlite_from_row!(Work {
    id,
    kind,
    title,
    subtitle,
    category,
    description,
    rating,
    progress,
    source_path,
    cover_asset_id,
    meta_json,
    created_at,
    updated_at,
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    pub id: i64,
    pub work_id: i64,
    pub path: String,
    pub mime: String,
    pub role: String,
    pub variant: Option<String>,
    pub position: Option<i64>,
    pub size: Option<i64>,
    pub meta_json: String,
    pub created_at: DateTime<Utc>,
}

impl_sqlite_from_row!(Asset {
    id,
    work_id,
    path,
    mime,
    role,
    variant,
    position,
    size,
    meta_json,
    created_at,
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    pub id: i64,
    pub namespace: String,
    pub key: String,
    pub label: String,
    pub translated_label: Option<String>,
    pub translated_namespace: Option<String>,
    pub source: String,
    pub intro: Option<String>,
    pub links: Option<String>,
    pub count: i64,
}

impl_sqlite_from_row!(Tag {
    id,
    namespace,
    key,
    label,
    translated_label,
    translated_namespace,
    source,
    intro,
    links,
    count,
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: i64,
    pub job_type: String,
    pub status: String,
    pub payload_json: String,
    pub attempts: i64,
    pub retry_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl_sqlite_from_row!(Job {
    id,
    job_type,
    status,
    payload_json,
    attempts,
    retry_at,
    last_error,
    created_at,
    updated_at,
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryRecord {
    pub work_id: i64,
    pub kind: String,
    pub title: String,
    pub subtitle: Option<String>,
    pub cover_asset_id: Option<i64>,
    pub progress: f64,
    pub position: Option<String>,
    pub last_opened_at: DateTime<Utc>,
}

impl_sqlite_from_row!(HistoryRecord {
    work_id,
    kind,
    title,
    subtitle,
    cover_asset_id,
    progress,
    position,
    last_opened_at,
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalId {
    pub id: i64,
    pub work_id: i64,
    pub source: String,
    pub external_id: String,
    pub token: Option<String>,
    pub url: Option<String>,
}

impl_sqlite_from_row!(ExternalId {
    id,
    work_id,
    source,
    external_id,
    token,
    url,
});

#[derive(Debug, Serialize)]
pub struct LibraryResponse {
    pub works: Vec<WorkSummary>,
    pub tags: Vec<Tag>,
    pub jobs: Vec<Job>,
    pub history: Vec<HistoryRecord>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct WorkSummary {
    pub id: i64,
    pub kind: String,
    pub title: String,
    pub subtitle: Option<String>,
    pub category: Option<String>,
    pub rating: Option<f64>,
    pub progress: f64,
    pub source_path: Option<String>,
    pub cover_asset_id: Option<i64>,
    pub meta_json: String,
    pub tag_keys: Option<String>,
    pub tag_count: i64,
    pub asset_count: i64,
    pub updated_at: DateTime<Utc>,
}

impl_sqlite_from_row!(WorkSummary {
    id,
    kind,
    title,
    subtitle,
    category,
    rating,
    progress,
    source_path,
    cover_asset_id,
    meta_json,
    tag_keys,
    tag_count,
    asset_count,
    updated_at,
});

#[derive(Debug, Serialize)]
pub struct WorkDetail {
    pub work: Work,
    pub assets: Vec<Asset>,
    pub tags: Vec<Tag>,
    pub external_ids: Vec<ExternalId>,
}

#[derive(Debug, Deserialize)]
pub struct ScanRequest {
    pub enqueue_enrichment: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct ScanResponse {
    pub comics: usize,
    pub novels: usize,
    pub audio: usize,
    pub gallery: usize,
    pub coser_picture: usize,
    pub jobs_created: usize,
}
