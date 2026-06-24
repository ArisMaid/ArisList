use chrono::Utc;
use serde_json::{json, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Pool, Row, Sqlite};
use std::str::FromStr;

use crate::error::{AppError, Result};
use crate::models::{
    Asset, ExternalId, HistoryRecord, Job, LibraryResponse, Tag, Work, WorkDetail, WorkKind,
    WorkSummary,
};

#[derive(Clone)]
pub struct Db {
    pool: Pool<Sqlite>,
}

impl Db {
    pub async fn connect(url: &str) -> Result<Self> {
        let options = SqliteConnectOptions::from_str(url)
            .map_err(|e| AppError::Other(e.to_string()))?
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &Pool<Sqlite> {
        &self.pool
    }

    pub async fn migrate(&self) -> Result<()> {
        let schema = [
            "PRAGMA journal_mode = WAL",
            "PRAGMA foreign_keys = ON",
            r#"
            CREATE TABLE IF NOT EXISTS works (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                title TEXT NOT NULL,
                subtitle TEXT,
                category TEXT,
                description TEXT,
                rating REAL,
                progress REAL NOT NULL DEFAULT 0,
                source_path TEXT,
                cover_asset_id INTEGER,
                meta_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                UNIQUE(kind, source_path)
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
                path TEXT NOT NULL,
                mime TEXT NOT NULL,
                role TEXT NOT NULL,
                variant TEXT NOT NULL DEFAULT '',
                position INTEGER NOT NULL DEFAULT -1,
                size INTEGER,
                meta_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                UNIQUE(work_id, path, role, variant, position)
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                namespace TEXT NOT NULL,
                key TEXT NOT NULL,
                label TEXT NOT NULL,
                translated_label TEXT,
                translated_namespace TEXT,
                source TEXT NOT NULL DEFAULT 'local',
                intro TEXT,
                links TEXT,
                count INTEGER NOT NULL DEFAULT 0,
                UNIQUE(namespace, key)
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS work_tags (
                work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
                tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
                PRIMARY KEY(work_id, tag_id)
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS external_ids (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
                source TEXT NOT NULL,
                external_id TEXT NOT NULL,
                token TEXT,
                url TEXT,
                UNIQUE(work_id, source, external_id)
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                attempts INTEGER NOT NULL DEFAULT 0,
                retry_at TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS reading_history (
                work_id INTEGER PRIMARY KEY REFERENCES works(id) ON DELETE CASCADE,
                progress REAL NOT NULL DEFAULT 0,
                position TEXT,
                last_opened_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )"#,
            "CREATE INDEX IF NOT EXISTS idx_assets_work ON assets(work_id)",
            "CREATE INDEX IF NOT EXISTS idx_assets_role ON assets(role, work_id, position)",
            "CREATE INDEX IF NOT EXISTS idx_assets_gallery_position ON assets(work_id, role, position, id)",
            "CREATE INDEX IF NOT EXISTS idx_works_kind_updated ON works(kind, updated_at DESC, id DESC)",
            "CREATE INDEX IF NOT EXISTS idx_works_source_path ON works(source_path)",
            "CREATE INDEX IF NOT EXISTS idx_work_tags_tag ON work_tags(tag_id)",
            "CREATE INDEX IF NOT EXISTS idx_work_tags_work ON work_tags(work_id)",
            "CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, retry_at)",
            "CREATE INDEX IF NOT EXISTS idx_audit_logs_created ON audit_logs(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_history_opened ON reading_history(last_opened_at DESC)",
        ];

        for statement in schema {
            sqlx::query(statement).execute(&self.pool).await?;
        }
        Ok(())
    }

    pub async fn requeue_interrupted_running_jobs(&self) -> Result<u64> {
        let now = Utc::now();
        let result = sqlx::query(
            r#"
            UPDATE jobs SET
                status = 'queued',
                retry_at = NULL,
                last_error = COALESCE(last_error, 'interrupted by server restart'),
                updated_at = ?1
            WHERE status = 'running'
            "#,
        )
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn upsert_work(
        &self,
        kind: &str,
        title: &str,
        source_path: Option<&str>,
        category: Option<&str>,
        description: Option<&str>,
        rating: Option<f64>,
        meta: Value,
    ) -> Result<i64> {
        let row = sqlx::query(
            r#"
            INSERT INTO works (kind, title, category, description, rating, source_path, meta_json, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(kind, source_path) DO UPDATE SET
                title = excluded.title,
                category = excluded.category,
                description = COALESCE(excluded.description, works.description),
                rating = COALESCE(excluded.rating, works.rating),
                meta_json = excluded.meta_json,
                updated_at = excluded.updated_at
            RETURNING id
            "#,
        )
        .bind(kind)
        .bind(title)
        .bind(category)
        .bind(description)
        .bind(rating)
        .bind(source_path)
        .bind(meta.to_string())
        .bind(Utc::now())
        .fetch_one(&self.pool)
        .await?;
        Ok(row.get(0))
    }

    pub async fn upsert_asset(
        &self,
        work_id: i64,
        path: &str,
        mime: &str,
        role: &str,
        variant: Option<&str>,
        position: Option<i64>,
        size: Option<i64>,
        meta: Value,
    ) -> Result<i64> {
        let row = sqlx::query(
            r#"
            INSERT INTO assets (work_id, path, mime, role, variant, position, size, meta_json)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(work_id, path, role, variant, position) DO UPDATE SET
                mime = excluded.mime,
                size = excluded.size,
                meta_json = excluded.meta_json
            RETURNING id
            "#,
        )
        .bind(work_id)
        .bind(path)
        .bind(mime)
        .bind(role)
        .bind(variant.unwrap_or(""))
        .bind(position.unwrap_or(-1))
        .bind(size)
        .bind(meta.to_string())
        .fetch_one(&self.pool)
        .await?;

        let asset_id: i64 = row.get(0);
        if role == "cover" {
            sqlx::query("UPDATE works SET cover_asset_id = ?1, updated_at = ?2 WHERE id = ?3")
                .bind(asset_id)
                .bind(Utc::now())
                .bind(work_id)
                .execute(&self.pool)
                .await?;
        }
        Ok(asset_id)
    }

    pub async fn set_work_cover(&self, work_id: i64, asset_id: i64) -> Result<()> {
        let affected =
            sqlx::query("UPDATE works SET cover_asset_id = ?1, updated_at = ?2 WHERE id = ?3")
                .bind(asset_id)
                .bind(Utc::now())
                .bind(work_id)
                .execute(&self.pool)
                .await?
                .rows_affected();
        if affected == 0 {
            return Err(AppError::NotFound(format!("work {work_id} not found")));
        }
        Ok(())
    }

    pub async fn generated_assets_work(&self) -> Result<i64> {
        self.upsert_work(
            "generated",
            "Generated UI Assets",
            Some("__generated_ui_assets__"),
            Some("UI Assets"),
            Some("Safe local UI backgrounds, empty states, and placeholder covers generated through the image asset queue."),
            None,
            json!({
                "system": true,
                "source": "openai-image-generation",
                "collection": "ui-assets"
            }),
        )
        .await
    }

    pub async fn upsert_tag(
        &self,
        namespace: &str,
        key: &str,
        label: &str,
        translated_label: Option<&str>,
        translated_namespace: Option<&str>,
        source: &str,
        intro: Option<&str>,
        links: Option<&str>,
    ) -> Result<i64> {
        let row = sqlx::query(
            r#"
            INSERT INTO tags (namespace, key, label, translated_label, translated_namespace, source, intro, links)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(namespace, key) DO UPDATE SET
                label = excluded.label,
                translated_label = COALESCE(excluded.translated_label, tags.translated_label),
                translated_namespace = COALESCE(excluded.translated_namespace, tags.translated_namespace),
                source = excluded.source,
                intro = COALESCE(excluded.intro, tags.intro),
                links = COALESCE(excluded.links, tags.links)
            RETURNING id
            "#,
        )
        .bind(namespace)
        .bind(key)
        .bind(label)
        .bind(translated_label)
        .bind(translated_namespace)
        .bind(source)
        .bind(intro)
        .bind(links)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.get(0))
    }

    pub async fn link_tag(&self, work_id: i64, tag_id: i64) -> Result<()> {
        sqlx::query("INSERT OR IGNORE INTO work_tags (work_id, tag_id) VALUES (?1, ?2)")
            .bind(work_id)
            .bind(tag_id)
            .execute(&self.pool)
            .await?;
        sqlx::query("UPDATE tags SET count = (SELECT COUNT(*) FROM work_tags WHERE tag_id = ?1) WHERE id = ?1")
            .bind(tag_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn upsert_external_id(
        &self,
        work_id: i64,
        source: &str,
        external_id: &str,
        token: Option<&str>,
        url: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO external_ids (work_id, source, external_id, token, url)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(work_id, source, external_id) DO UPDATE SET
                token = COALESCE(excluded.token, external_ids.token),
                url = COALESCE(excluded.url, external_ids.url)
            "#,
        )
        .bind(work_id)
        .bind(source)
        .bind(external_id)
        .bind(token)
        .bind(url)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn create_job(&self, job_type: &str, status: &str, payload: Value) -> Result<i64> {
        let row = sqlx::query(
            r#"
            INSERT INTO jobs (job_type, status, payload_json, created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?4)
            RETURNING id
            "#,
        )
        .bind(job_type)
        .bind(status)
        .bind(payload.to_string())
        .bind(Utc::now())
        .fetch_one(&self.pool)
        .await?;
        Ok(row.get(0))
    }

    pub async fn update_job(&self, id: i64, status: &str, last_error: Option<&str>) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE jobs SET
                status = ?1,
                last_error = ?2,
                attempts = CASE WHEN ?1 = 'running' THEN attempts + 1 ELSE attempts END,
                updated_at = ?3
            WHERE id = ?4
            "#,
        )
        .bind(status)
        .bind(last_error)
        .bind(Utc::now())
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn reschedule_job(
        &self,
        id: i64,
        last_error: &str,
        retry_delay_seconds: i64,
    ) -> Result<()> {
        let retry_at = Utc::now() + chrono::Duration::seconds(retry_delay_seconds);
        sqlx::query(
            r#"
            UPDATE jobs SET
                status = 'queued',
                last_error = ?1,
                retry_at = ?2,
                updated_at = ?3
            WHERE id = ?4
            "#,
        )
        .bind(last_error)
        .bind(retry_at)
        .bind(Utc::now())
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_work_enrichment(
        &self,
        id: i64,
        title: Option<&str>,
        category: Option<&str>,
        description: Option<&str>,
        rating: Option<f64>,
        meta: Value,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE works SET
                title = COALESCE(?1, title),
                category = COALESCE(?2, category),
                description = COALESCE(?3, description),
                rating = COALESCE(?4, rating),
                meta_json = ?5,
                updated_at = ?6
            WHERE id = ?7
            "#,
        )
        .bind(title)
        .bind(category)
        .bind(description)
        .bind(rating)
        .bind(meta.to_string())
        .bind(Utc::now())
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_work_meta(&self, id: i64, meta: Value) -> Result<()> {
        let affected = sqlx::query(
            r#"
            UPDATE works SET meta_json = ?1, updated_at = ?2
            WHERE id = ?3
            "#,
        )
        .bind(meta.to_string())
        .bind(Utc::now())
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if affected == 0 {
            return Err(AppError::NotFound(format!("work {id} not found")));
        }
        Ok(())
    }

    pub async fn update_work_progress(
        &self,
        id: i64,
        progress: f64,
        position: Option<&str>,
    ) -> Result<()> {
        let progress = progress.clamp(0.0, 1.0);
        let affected = sqlx::query(
            r#"
            UPDATE works SET progress = ?1, updated_at = ?2
            WHERE id = ?3
            "#,
        )
        .bind(progress)
        .bind(Utc::now())
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if affected == 0 {
            return Err(AppError::NotFound(format!("work {id} not found")));
        }
        self.upsert_history(id, progress, position).await?;
        Ok(())
    }

    pub async fn upsert_history(
        &self,
        work_id: i64,
        progress: f64,
        position: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO reading_history (work_id, progress, position, last_opened_at)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(work_id) DO UPDATE SET
                progress = excluded.progress,
                position = excluded.position,
                last_opened_at = excluded.last_opened_at
            "#,
        )
        .bind(work_id)
        .bind(progress.clamp(0.0, 1.0))
        .bind(position)
        .bind(Utc::now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn library(&self) -> Result<LibraryResponse> {
        let works = sqlx::query_as::<_, WorkSummary>(
            r#"
            SELECT
                w.id, w.kind, w.title, w.subtitle, w.category, w.rating, w.progress, w.source_path,
                w.cover_asset_id, w.meta_json,
                (
                    SELECT GROUP_CONCAT(tag_key) FROM (
                        SELECT DISTINCT t.namespace || ':' || t.key AS tag_key
                        FROM work_tags wt
                        JOIN tags t ON t.id = wt.tag_id
                        WHERE wt.work_id = w.id
                        ORDER BY t.namespace, t.key
                    )
                ) AS tag_keys,
                (SELECT COUNT(*) FROM work_tags wt WHERE wt.work_id = w.id) AS tag_count,
                (SELECT COUNT(*) FROM assets a WHERE a.work_id = w.id) AS asset_count,
                w.updated_at
            FROM works w
            ORDER BY w.updated_at DESC, w.id DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        let tags = self.tags().await?;
        let jobs = self.jobs(20).await?;
        let history = self.history(20).await?;
        Ok(LibraryResponse {
            works,
            tags,
            jobs,
            history,
            next_cursor: None,
        })
    }

    pub async fn history(&self, limit: i64) -> Result<Vec<HistoryRecord>> {
        Ok(sqlx::query_as::<_, HistoryRecord>(
            r#"
            SELECT
                w.id AS work_id,
                w.kind,
                w.title,
                w.subtitle,
                w.cover_asset_id,
                h.progress,
                h.position,
                h.last_opened_at
            FROM reading_history h
            JOIN works w ON w.id = h.work_id
            ORDER BY h.last_opened_at DESC
            LIMIT ?1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn work_detail(&self, id: i64) -> Result<WorkDetail> {
        let work = sqlx::query_as::<_, Work>("SELECT * FROM works WHERE id = ?1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("work {id} not found")))?;
        let asset_query = if work.kind == WorkKind::Gallery.as_str() {
            "SELECT * FROM assets WHERE work_id = ?1 AND role != 'image' ORDER BY role, position, id"
        } else {
            "SELECT * FROM assets WHERE work_id = ?1 ORDER BY role, position, id"
        };
        let assets = sqlx::query_as::<_, Asset>(asset_query)
            .bind(id)
            .fetch_all(&self.pool)
            .await?;
        let tags = sqlx::query_as::<_, Tag>(
            r#"
            SELECT t.* FROM tags t
            JOIN work_tags wt ON wt.tag_id = t.id
            WHERE wt.work_id = ?1
            ORDER BY t.namespace, t.key
            "#,
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await?;
        let external_ids =
            sqlx::query_as::<_, ExternalId>("SELECT * FROM external_ids WHERE work_id = ?1")
                .bind(id)
                .fetch_all(&self.pool)
                .await?;
        Ok(WorkDetail {
            work,
            assets,
            tags,
            external_ids,
        })
    }

    pub async fn work_kind_and_meta(&self, id: i64) -> Result<(String, String)> {
        sqlx::query_as::<_, (String, String)>("SELECT kind, meta_json FROM works WHERE id = ?1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("work {id} not found")))
    }

    pub async fn asset(&self, id: i64) -> Result<Asset> {
        sqlx::query_as::<_, Asset>("SELECT * FROM assets WHERE id = ?1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("asset {id} not found")))
    }

    pub async fn gallery_assets(
        &self,
        work_id: i64,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<Asset>> {
        Ok(sqlx::query_as::<_, Asset>(
            r#"
            SELECT * FROM assets
            WHERE work_id = ?1 AND role = 'image' AND position >= ?3
            ORDER BY position, id
            LIMIT ?2
            "#,
        )
        .bind(work_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn gallery_asset_count(&self, work_id: i64) -> Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM assets WHERE work_id = ?1 AND role = 'image'",
        )
        .bind(work_id)
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn tags(&self) -> Result<Vec<Tag>> {
        Ok(sqlx::query_as::<_, Tag>(
            "SELECT * FROM tags ORDER BY count DESC, namespace, key LIMIT 500",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn jobs(&self, limit: i64) -> Result<Vec<Job>> {
        Ok(sqlx::query_as::<_, Job>(
            r#"
            SELECT * FROM jobs
            WHERE job_type NOT IN ('enrich-asmr-work', 'enrich-lightnovel-work', 'generate-image-asset')
            ORDER BY updated_at DESC, id DESC
            LIMIT ?1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn claim_next_queued_job(&self) -> Result<Option<Job>> {
        Ok(sqlx::query_as::<_, Job>(
            r#"
            UPDATE jobs SET
                status = 'running',
                last_error = NULL,
                attempts = attempts + 1,
                updated_at = ?1
            WHERE id = (
                SELECT id FROM jobs
                WHERE status = 'queued' AND (retry_at IS NULL OR retry_at <= ?1)
                ORDER BY
                    CASE
                        WHEN job_type IN ('scan-library', 'rebuild-search-index') THEN 0
                        WHEN job_type = 'generate-image-asset' THEN 1
                        WHEN job_type = 'import-tag-translations' THEN 2
                        WHEN job_type = 'enrich-asmr-work' THEN 4
                        ELSE 9
                    END,
                    created_at ASC
                LIMIT 1
            )
            RETURNING *
            "#,
        )
        .bind(Utc::now())
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn audit(&self, action: &str, status: &str, payload: Value) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO audit_logs (action, status, payload_json, created_at)
            VALUES (?1, ?2, ?3, ?4)
            "#,
        )
        .bind(action)
        .bind(status)
        .bind(payload.to_string())
        .bind(Utc::now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
