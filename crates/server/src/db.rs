use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
#[cfg(test)]
use chrono::DateTime;
use chrono::{Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Pool, Row, Sqlite};
use sqlx_core::transaction::Transaction;
use std::str::FromStr;
use std::time::Duration;

use crate::error::{AppError, Result};
use crate::models::{
    Asset, ExternalId, HistoryRecord, Job, LibraryResponse, Tag, Work, WorkDetail, WorkKind,
    WorkSummary,
};

const AUDIT_RETENTION_DAYS: i64 = 90;
const AUDIT_MAX_RECORDS: i64 = 20_000;
const AUDIT_PRUNE_INTERVAL: i64 = 256;

#[derive(Debug, Serialize, Deserialize)]
struct LibraryCursor {
    next_id: i64,
}

#[derive(Debug)]
pub struct ScannerAssetInput {
    pub path: String,
    pub mime: String,
    pub role: String,
    pub variant: Option<String>,
    pub position: Option<i64>,
    pub size: Option<i64>,
    pub meta: Value,
}

#[derive(Debug)]
pub struct EnrichmentTagInput {
    pub namespace: String,
    pub key: String,
    pub label: String,
    pub source: String,
}

#[derive(Debug)]
pub struct EnrichmentExternalIdInput {
    pub source: String,
    pub external_id: String,
    pub token: Option<String>,
    pub url: Option<String>,
}

#[derive(Debug)]
pub struct ScannerEnrichmentInput {
    pub title: Option<String>,
    pub category: Option<String>,
    pub description: Option<String>,
    pub rating: Option<f64>,
    pub meta: Value,
    pub tags: Vec<EnrichmentTagInput>,
    pub external_ids: Vec<EnrichmentExternalIdInput>,
}

#[derive(Debug)]
pub struct ProgressWrite {
    pub accepted: bool,
    pub progress: f64,
    pub position: Option<String>,
}

#[derive(Clone)]
pub struct Db {
    pool: Pool<Sqlite>,
}

impl Db {
    pub async fn connect(url: &str) -> Result<Self> {
        let options = SqliteConnectOptions::from_str(url)
            .map_err(|e| AppError::Other(e.to_string()))?
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(30));
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
        let tables = [
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
                UNIQUE(work_id, path, role, variant)
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
                last_opened_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                update_token INTEGER NOT NULL DEFAULT 0
            )"#,
        ];

        for statement in tables {
            sqlx::query(statement).execute(&self.pool).await?;
        }
        self.ensure_reading_history_update_token().await?;

        self.migrate_asset_identity().await?;

        let scan_tables = [
            r#"
            CREATE TABLE IF NOT EXISTS scanner_works (
                work_id INTEGER PRIMARY KEY REFERENCES works(id) ON DELETE CASCADE,
                scope TEXT NOT NULL,
                seen_token TEXT NOT NULL,
                fingerprint TEXT
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS scanner_assets (
                asset_id INTEGER PRIMARY KEY REFERENCES assets(id) ON DELETE CASCADE,
                work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
                seen_token TEXT NOT NULL
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS work_tag_sources (
                work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
                tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
                owner TEXT NOT NULL,
                seen_token TEXT,
                PRIMARY KEY(work_id, tag_id, owner)
            )"#,
            r#"
            CREATE TABLE IF NOT EXISTS scanner_locks (
                name TEXT PRIMARY KEY,
                token TEXT NOT NULL,
                acquired_at TEXT NOT NULL
            )"#,
        ];
        for statement in scan_tables {
            sqlx::query(statement).execute(&self.pool).await?;
        }

        self.migrate_scanner_ownership().await?;
        sqlx::query("DROP INDEX IF EXISTS idx_jobs_one_active_maintenance")
            .execute(&self.pool)
            .await?;
        self.coalesce_existing_jobs().await?;

        let indexes = [
            "CREATE INDEX IF NOT EXISTS idx_assets_work ON assets(work_id)",
            "CREATE INDEX IF NOT EXISTS idx_assets_role ON assets(role, work_id, position)",
            "CREATE INDEX IF NOT EXISTS idx_assets_gallery_position ON assets(work_id, role, position, id)",
            "CREATE INDEX IF NOT EXISTS idx_works_kind_updated ON works(kind, updated_at DESC, id DESC)",
            "CREATE INDEX IF NOT EXISTS idx_works_updated ON works(updated_at DESC, id DESC)",
            "CREATE INDEX IF NOT EXISTS idx_works_source_path ON works(source_path)",
            "CREATE INDEX IF NOT EXISTS idx_work_tags_tag ON work_tags(tag_id)",
            "CREATE INDEX IF NOT EXISTS idx_work_tags_work ON work_tags(work_id)",
            "CREATE INDEX IF NOT EXISTS idx_tags_count ON tags(count DESC, namespace, key)",
            "CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, retry_at)",
            "CREATE INDEX IF NOT EXISTS idx_audit_logs_created ON audit_logs(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_history_opened ON reading_history(last_opened_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_scanner_works_scope ON scanner_works(scope, seen_token)",
            "CREATE INDEX IF NOT EXISTS idx_scanner_assets_work ON scanner_assets(work_id, seen_token)",
            "CREATE INDEX IF NOT EXISTS idx_work_tag_sources_work ON work_tag_sources(work_id, owner, seen_token)",
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_one_queued_maintenance
            ON jobs(job_type)
            WHERE status = 'queued'
              AND job_type IN ('scan-library', 'rebuild-search-index')
            "#,
        ];

        for statement in indexes {
            sqlx::query(statement).execute(&self.pool).await?;
        }
        self.prune_audit_logs().await?;
        Ok(())
    }

    async fn ensure_reading_history_update_token(&self) -> Result<()> {
        let columns = sqlx::query("PRAGMA table_info(reading_history)")
            .fetch_all(&self.pool)
            .await?;
        if columns
            .iter()
            .any(|row| row.get::<String, _>("name") == "update_token")
        {
            return Ok(());
        }
        sqlx::query(
            "ALTER TABLE reading_history ADD COLUMN update_token INTEGER NOT NULL DEFAULT 0",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn migrate_asset_identity(&self) -> Result<()> {
        let schema = sqlx::query_scalar::<_, String>(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'assets'",
        )
        .fetch_optional(&self.pool)
        .await?
        .unwrap_or_default();
        let compact = schema
            .chars()
            .filter(|ch| !ch.is_whitespace())
            .collect::<String>()
            .to_ascii_lowercase();
        if !compact.contains("unique(work_id,path,role,variant,position)") {
            return Ok(());
        }

        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            r#"
            CREATE TABLE assets_v2 (
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
                UNIQUE(work_id, path, role, variant)
            )
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            CREATE TEMP TABLE asset_identity_map AS
            SELECT
                candidate.id AS old_id,
                COALESCE(
                    MAX(CASE WHEN keeper.id = work.cover_asset_id THEN keeper.id END),
                    MAX(keeper.id)
                ) AS keep_id
            FROM assets candidate
            JOIN works work ON work.id = candidate.work_id
            JOIN assets keeper
              ON keeper.work_id = candidate.work_id
             AND keeper.path = candidate.path
             AND keeper.role = candidate.role
             AND keeper.variant = candidate.variant
            GROUP BY candidate.id
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO assets_v2
                (id, work_id, path, mime, role, variant, position, size, meta_json, created_at)
            SELECT
                asset.id, asset.work_id, asset.path, asset.mime, asset.role, asset.variant,
                asset.position, asset.size, asset.meta_json, asset.created_at
            FROM assets asset
            JOIN asset_identity_map mapping ON mapping.old_id = asset.id
            WHERE mapping.old_id = mapping.keep_id
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            UPDATE works
            SET cover_asset_id = (
                SELECT keep_id FROM asset_identity_map WHERE old_id = works.cover_asset_id
            )
            WHERE cover_asset_id IS NOT NULL
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query("DROP TABLE assets")
            .execute(&mut *transaction)
            .await?;
        sqlx::query("ALTER TABLE assets_v2 RENAME TO assets")
            .execute(&mut *transaction)
            .await?;
        sqlx::query("DROP TABLE asset_identity_map")
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn migrate_scanner_ownership(&self) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO scanner_works (work_id, scope, seen_token, fingerprint)
            SELECT id, ('legacy|' || kind), 'legacy', NULL
            FROM works
            WHERE kind IN ('comic', 'novel', 'audio', 'gallery', 'coser-picture')
              AND source_path IS NOT NULL
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO scanner_assets (asset_id, work_id, seen_token)
            SELECT asset.id, asset.work_id, 'legacy'
            FROM assets asset
            JOIN scanner_works scanner ON scanner.work_id = asset.work_id
            WHERE asset.role != 'generated'
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO work_tag_sources (work_id, tag_id, owner, seen_token)
            SELECT work_tag.work_id, work_tag.tag_id, 'scanner', 'legacy'
            FROM work_tags work_tag
            JOIN tags tag ON tag.id = work_tag.tag_id
            JOIN scanner_works scanner ON scanner.work_id = work_tag.work_id
            WHERE tag.source IN (
                'comic-info', 'epub', 'audio-folder', 'gallery-folder',
                'gallery-filename', 'coser-picture-zip', 'qmediasync'
            )
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO work_tag_sources (work_id, tag_id, owner, seen_token)
            SELECT work_tag.work_id, work_tag.tag_id, 'external', NULL
            FROM work_tags work_tag
            JOIN tags tag ON tag.id = work_tag.tag_id
            LEFT JOIN scanner_works scanner ON scanner.work_id = work_tag.work_id
            WHERE scanner.work_id IS NULL
               OR tag.source NOT IN (
                    'comic-info', 'epub', 'audio-folder', 'gallery-folder',
                    'gallery-filename', 'coser-picture-zip', 'qmediasync'
               )
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            DELETE FROM work_tag_sources
            WHERE owner = 'scanner'
              AND seen_token = 'legacy'
              AND tag_id IN (
                  SELECT id FROM tags
                  WHERE source NOT IN (
                      'comic-info', 'epub', 'audio-folder', 'gallery-folder',
                      'gallery-filename', 'coser-picture-zip', 'qmediasync'
                  )
              )
            "#,
        )
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn coalesce_existing_jobs(&self) -> Result<()> {
        sqlx::query(
            r#"
            WITH active AS (
                SELECT
                    job_type,
                    status,
                    MIN(id) AS keep_id
                FROM jobs
                WHERE status IN ('queued', 'running')
                  AND job_type IN ('scan-library', 'rebuild-search-index')
                GROUP BY job_type, status
            )
            UPDATE jobs
            SET
                status = 'superseded',
                last_error = COALESCE(last_error, 'coalesced during migration'),
                updated_at = ?1
            WHERE status IN ('queued', 'running')
              AND job_type IN ('scan-library', 'rebuild-search-index')
              AND id != (
                  SELECT keep_id FROM active
                  WHERE active.job_type = jobs.job_type AND active.status = jobs.status
              )
            "#,
        )
        .bind(Utc::now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn requeue_interrupted_running_jobs(&self) -> Result<u64> {
        let mut transaction = self.pool.begin().await?;
        // This method is called once, before workers are started. Any persisted
        // scanner lease therefore belongs to the previous process and must not
        // make its requeued scan job fail until the normal lease timeout.
        sqlx::query("DELETE FROM scanner_locks")
            .execute(&mut *transaction)
            .await?;
        let now = Utc::now();
        // A queued singleton is the successor of the interrupted running job.
        // Fold its request into the job that will be requeued before marking the
        // successor superseded, otherwise a restart can silently drop options
        // such as enqueue_enrichment=true.
        sqlx::query(
            r#"
            UPDATE jobs AS running
            SET
                payload_json = json_set(
                    json_patch(
                        running.payload_json,
                        COALESCE((
                            SELECT queued.payload_json
                            FROM jobs AS queued
                            WHERE queued.job_type = running.job_type
                              AND queued.status = 'queued'
                            ORDER BY queued.id
                            LIMIT 1
                        ), '{}')
                    ),
                    '$.enqueue_enrichment',
                    json(CASE
                        WHEN COALESCE(json_extract(running.payload_json, '$.enqueue_enrichment'), 0) != 0
                          OR COALESCE((
                              SELECT json_extract(queued.payload_json, '$.enqueue_enrichment')
                              FROM jobs AS queued
                              WHERE queued.job_type = running.job_type
                                AND queued.status = 'queued'
                              ORDER BY queued.id
                              LIMIT 1
                          ), 0) != 0
                        THEN 'true'
                        ELSE 'false'
                    END)
                ),
                updated_at = ?1
            WHERE running.status = 'running'
              AND running.job_type = 'scan-library'
              AND EXISTS (
                  SELECT 1 FROM jobs AS queued
                  WHERE queued.job_type = running.job_type
                    AND queued.status = 'queued'
              )
            "#,
        )
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            UPDATE jobs AS running
            SET
                payload_json = json_patch(
                    running.payload_json,
                    COALESCE((
                        SELECT queued.payload_json
                        FROM jobs AS queued
                        WHERE queued.job_type = running.job_type
                          AND queued.status = 'queued'
                        ORDER BY queued.id
                        LIMIT 1
                    ), '{}')
                ),
                updated_at = ?1
            WHERE running.status = 'running'
              AND running.job_type = 'rebuild-search-index'
              AND EXISTS (
                  SELECT 1 FROM jobs AS queued
                  WHERE queued.job_type = running.job_type
                    AND queued.status = 'queued'
              )
            "#,
        )
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            UPDATE jobs AS queued
            SET
                status = 'superseded',
                last_error = COALESCE(last_error, 'covered by interrupted running job'),
                updated_at = ?1
            WHERE queued.status = 'queued'
              AND queued.job_type IN ('scan-library', 'rebuild-search-index')
              AND EXISTS (
                  SELECT 1 FROM jobs AS running
                  WHERE running.job_type = queued.job_type AND running.status = 'running'
              )
            "#,
        )
        .bind(now)
        .execute(&mut *transaction)
        .await?;
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
        .execute(&mut *transaction)
        .await?;
        let recovered = result.rows_affected();
        transaction.commit().await?;
        Ok(recovered)
    }

    #[allow(clippy::too_many_arguments)]
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
                meta_json = json_patch(
                    CASE WHEN json_valid(works.meta_json) THEN works.meta_json ELSE '{}' END,
                    excluded.meta_json
                ),
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

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_scanner_work(
        &self,
        kind: &str,
        title: &str,
        source_path: Option<&str>,
        category: Option<&str>,
        description: Option<&str>,
        rating: Option<f64>,
        mut meta: Value,
        seen_token: &str,
        fingerprint: &str,
    ) -> Result<i64> {
        let mut transaction = self.pool.begin().await?;
        require_scanner_lease(&mut transaction, "library", seen_token).await?;
        if let Some(object) = meta.as_object_mut() {
            object.insert("_scanner_fingerprint".to_string(), json!(fingerprint));
        }
        let row = sqlx::query(
            r#"
            INSERT INTO works (kind, title, category, description, rating, source_path, meta_json, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(kind, source_path) DO UPDATE SET
                title = excluded.title,
                category = excluded.category,
                description = COALESCE(excluded.description, works.description),
                rating = COALESCE(excluded.rating, works.rating),
                meta_json = json_patch(
                    CASE WHEN json_valid(works.meta_json) THEN works.meta_json ELSE '{}' END,
                    excluded.meta_json
                ),
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
        .fetch_one(&mut *transaction)
        .await?;
        let work_id = row.get(0);
        transaction.commit().await?;
        Ok(work_id)
    }

    #[allow(clippy::too_many_arguments)]
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
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            r#"
            INSERT INTO assets (work_id, path, mime, role, variant, position, size, meta_json)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(work_id, path, role, variant) DO UPDATE SET
                mime = excluded.mime,
                position = excluded.position,
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
        .fetch_one(&mut *transaction)
        .await?;

        let asset_id: i64 = row.get(0);
        if role == "cover" {
            sqlx::query("UPDATE works SET cover_asset_id = ?1, updated_at = ?2 WHERE id = ?3")
                .bind(asset_id)
                .bind(Utc::now())
                .bind(work_id)
                .execute(&mut *transaction)
                .await?;
        }
        transaction.commit().await?;
        Ok(asset_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_scanner_asset(
        &self,
        work_id: i64,
        path: &str,
        mime: &str,
        role: &str,
        variant: Option<&str>,
        position: Option<i64>,
        size: Option<i64>,
        meta: Value,
        seen_token: &str,
    ) -> Result<i64> {
        let inputs = vec![ScannerAssetInput {
            path: path.to_string(),
            mime: mime.to_string(),
            role: role.to_string(),
            variant: variant.map(str::to_string),
            position,
            size,
            meta,
        }];
        self.upsert_scanner_assets(work_id, inputs, seen_token)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| {
                AppError::Other("scanner asset batch was unexpectedly empty".to_string())
            })
    }

    pub async fn upsert_scanner_assets(
        &self,
        work_id: i64,
        assets: Vec<ScannerAssetInput>,
        seen_token: &str,
    ) -> Result<Vec<i64>> {
        if assets.is_empty() {
            return Ok(Vec::new());
        }
        let mut asset_ids = Vec::with_capacity(assets.len());
        let mut pending = assets.into_iter();
        loop {
            // Bound the SQLite writer hold time for very large galleries/audio
            // works. The final scanner-work transaction remains the commit
            // marker, so an interrupted partial batch never triggers deletion.
            let chunk = pending.by_ref().take(512).collect::<Vec<_>>();
            if chunk.is_empty() {
                break;
            }
            let version = Utc::now();
            let mut transaction = self.pool.begin().await?;
            require_scanner_lease(&mut transaction, "library", seen_token).await?;
            for asset in chunk {
                let row = sqlx::query(
                    r#"
                INSERT INTO assets (work_id, path, mime, role, variant, position, size, meta_json)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                ON CONFLICT(work_id, path, role, variant) DO UPDATE SET
                    mime = excluded.mime,
                    position = excluded.position,
                    size = excluded.size,
                    meta_json = excluded.meta_json,
                    created_at = CASE
                        WHEN assets.mime IS NOT excluded.mime
                          OR json_extract(assets.meta_json, '$._source_version')
                             IS NOT json_extract(excluded.meta_json, '$._source_version')
                        THEN ?9
                        ELSE assets.created_at
                    END
                RETURNING id
                "#,
                )
                .bind(work_id)
                .bind(&asset.path)
                .bind(&asset.mime)
                .bind(&asset.role)
                .bind(asset.variant.as_deref().unwrap_or(""))
                .bind(asset.position.unwrap_or(-1))
                .bind(asset.size)
                .bind(asset.meta.to_string())
                .bind(version)
                .fetch_one(&mut *transaction)
                .await?;
                let asset_id: i64 = row.get(0);

                sqlx::query(
                    r#"
                INSERT INTO scanner_assets (asset_id, work_id, seen_token)
                VALUES (?1, ?2, ?3)
                ON CONFLICT(asset_id) DO UPDATE SET
                    work_id = excluded.work_id,
                    seen_token = excluded.seen_token
                "#,
                )
                .bind(asset_id)
                .bind(work_id)
                .bind(seen_token)
                .execute(&mut *transaction)
                .await?;

                if asset.role == "cover" {
                    sqlx::query(
                        "UPDATE works SET cover_asset_id = ?1, updated_at = ?2 WHERE id = ?3",
                    )
                    .bind(asset_id)
                    .bind(version)
                    .bind(work_id)
                    .execute(&mut *transaction)
                    .await?;
                }
                asset_ids.push(asset_id);
            }
            transaction.commit().await?;
        }
        Ok(asset_ids)
    }

    pub async fn mark_scanner_work(
        &self,
        work_id: i64,
        scope: &str,
        seen_token: &str,
        fingerprint: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO scanner_works (work_id, scope, seen_token, fingerprint)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(work_id) DO UPDATE SET
                scope = excluded.scope,
                seen_token = excluded.seen_token,
                fingerprint = excluded.fingerprint
            "#,
        )
        .bind(work_id)
        .bind(scope)
        .bind(seen_token)
        .bind(fingerprint)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn adopt_scanner_scope(
        &self,
        kind: &str,
        source_prefix: &str,
        scope: &str,
        seen_token: &str,
    ) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        require_scanner_lease(&mut transaction, "library", seen_token).await?;
        sqlx::query(
            r#"
            UPDATE scanner_works
            SET scope = ?1
            WHERE scope = ('legacy|' || ?2)
              AND work_id IN (
                  SELECT id FROM works
                  WHERE kind = ?2
                    AND (
                        source_path = ?3
                        OR substr(source_path, 1, length(?3) + 1) = (?3 || '/')
                    )
              )
            "#,
        )
        .bind(scope)
        .bind(kind)
        .bind(source_prefix.trim_end_matches('/'))
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn scanner_work_fingerprint(
        &self,
        kind: &str,
        source_path: &str,
    ) -> Result<Option<(i64, String)>> {
        Ok(sqlx::query_as::<_, (i64, String)>(
            r#"
            SELECT work.id, scanner.fingerprint
            FROM works work
            JOIN scanner_works scanner ON scanner.work_id = work.id
            WHERE work.kind = ?1 AND work.source_path = ?2 AND scanner.fingerprint IS NOT NULL
            "#,
        )
        .bind(kind)
        .bind(source_path)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn scanner_work_id(&self, kind: &str, source_path: &str) -> Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            r#"
            SELECT work.id
            FROM works work
            JOIN scanner_works scanner ON scanner.work_id = work.id
            WHERE work.kind = ?1 AND work.source_path = ?2
            "#,
        )
        .bind(kind)
        .bind(source_path)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn touch_scanner_work(
        &self,
        work_id: i64,
        scope: &str,
        seen_token: &str,
    ) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        require_scanner_lease(&mut transaction, "library", seen_token).await?;
        sqlx::query("UPDATE scanner_works SET scope = ?1, seen_token = ?2 WHERE work_id = ?3")
            .bind(scope)
            .bind(seen_token)
            .bind(work_id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn finish_scanner_work(
        &self,
        work_id: i64,
        scope: &str,
        seen_token: &str,
        fingerprint: &str,
    ) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        require_scanner_lease(&mut transaction, "library", seen_token).await?;
        sqlx::query(
            r#"
            INSERT INTO scanner_works (work_id, scope, seen_token, fingerprint)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(work_id) DO UPDATE SET
                scope = excluded.scope,
                seen_token = excluded.seen_token,
                fingerprint = excluded.fingerprint
            "#,
        )
        .bind(work_id)
        .bind(scope)
        .bind(seen_token)
        .bind(fingerprint)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            UPDATE works
            SET cover_asset_id = NULL
            WHERE id = ?1
              AND cover_asset_id IN (
                  SELECT asset_id FROM scanner_assets
                  WHERE work_id = ?1 AND seen_token != ?2
              )
            "#,
        )
        .bind(work_id)
        .bind(seen_token)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            DELETE FROM assets
            WHERE id IN (
                SELECT asset_id FROM scanner_assets
                WHERE work_id = ?1 AND seen_token != ?2
            )
            "#,
        )
        .bind(work_id)
        .bind(seen_token)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "DELETE FROM work_tag_sources WHERE work_id = ?1 AND owner = 'scanner' AND seen_token != ?2",
        )
        .bind(work_id)
        .bind(seen_token)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            DELETE FROM work_tags
            WHERE work_id = ?1
              AND NOT EXISTS (
                  SELECT 1 FROM work_tag_sources source
                  WHERE source.work_id = work_tags.work_id AND source.tag_id = work_tags.tag_id
              )
            "#,
        )
        .bind(work_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn finish_scanner_scope(&self, scope: &str, seen_token: &str) -> Result<u64> {
        let mut transaction = self.pool.begin().await?;
        require_scanner_lease(&mut transaction, "library", seen_token).await?;
        let result = sqlx::query(
            r#"
            DELETE FROM works
            WHERE id IN (
                SELECT work_id FROM scanner_works
                WHERE scope = ?1 AND seen_token != ?2
            )
            "#,
        )
        .bind(scope)
        .bind(seen_token)
        .execute(&mut *transaction)
        .await?;
        let deleted = result.rows_affected();
        transaction.commit().await?;
        Ok(deleted)
    }

    pub async fn finish_removed_scanner_scopes(
        &self,
        kind: &str,
        active_scopes: &[String],
        seen_token: &str,
    ) -> Result<u64> {
        let mut transaction = self.pool.begin().await?;
        require_scanner_lease(&mut transaction, "library", seen_token).await?;
        let prefix = format!("{kind}|");
        let scopes = sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT scope FROM scanner_works WHERE scope LIKE ?1 OR scope = ?2",
        )
        .bind(format!("{prefix}%"))
        .bind(format!("legacy|{kind}"))
        .fetch_all(&mut *transaction)
        .await?;
        let mut deleted = 0;
        for scope in scopes {
            if active_scopes.iter().any(|active| active == &scope) {
                continue;
            }
            deleted += sqlx::query(
                "DELETE FROM works WHERE id IN (SELECT work_id FROM scanner_works WHERE scope = ?1)",
            )
            .bind(scope)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
        }
        transaction.commit().await?;
        Ok(deleted)
    }

    pub async fn try_acquire_scanner_lock(
        &self,
        name: &str,
        token: &str,
        stale_after_seconds: i64,
    ) -> Result<bool> {
        let stale_before = Utc::now() - chrono::Duration::seconds(stale_after_seconds.max(60));
        let result = sqlx::query(
            r#"
            INSERT INTO scanner_locks (name, token, acquired_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(name) DO UPDATE SET
                token = excluded.token,
                acquired_at = excluded.acquired_at
            WHERE scanner_locks.acquired_at < ?4
            "#,
        )
        .bind(name)
        .bind(token)
        .bind(Utc::now())
        .bind(stale_before)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn heartbeat_scanner_lock(&self, name: &str, token: &str) -> Result<bool> {
        let result =
            sqlx::query("UPDATE scanner_locks SET acquired_at = ?1 WHERE name = ?2 AND token = ?3")
                .bind(Utc::now())
                .bind(name)
                .bind(token)
                .execute(&self.pool)
                .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn release_scanner_lock(&self, name: &str, token: &str) -> Result<()> {
        sqlx::query("DELETE FROM scanner_locks WHERE name = ?1 AND token = ?2")
            .bind(name)
            .bind(token)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn refresh_tag_counts(&self) -> Result<()> {
        sqlx::query(
            "UPDATE tags SET count = (SELECT COUNT(*) FROM work_tags WHERE tag_id = tags.id)",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
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

    #[allow(clippy::too_many_arguments)]
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

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_and_link_scanner_tag(
        &self,
        work_id: i64,
        namespace: &str,
        key: &str,
        label: &str,
        translated_label: Option<&str>,
        translated_namespace: Option<&str>,
        source: &str,
        intro: Option<&str>,
        links: Option<&str>,
        seen_token: &str,
    ) -> Result<i64> {
        let mut transaction = self.pool.begin().await?;
        require_scanner_lease(&mut transaction, "library", seen_token).await?;
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
        .fetch_one(&mut *transaction)
        .await?;
        let tag_id: i64 = row.get(0);
        sqlx::query("INSERT OR IGNORE INTO work_tags (work_id, tag_id) VALUES (?1, ?2)")
            .bind(work_id)
            .bind(tag_id)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            r#"
            INSERT INTO work_tag_sources (work_id, tag_id, owner, seen_token)
            VALUES (?1, ?2, 'scanner', ?3)
            ON CONFLICT(work_id, tag_id, owner) DO UPDATE SET
                seen_token = excluded.seen_token
            "#,
        )
        .bind(work_id)
        .bind(tag_id)
        .bind(seen_token)
        .execute(&mut *transaction)
        .await?;
        // Scanner jobs refresh all counts once after reconciliation. Recounting
        // a popular tag for every linked work makes an initial large-library
        // scan quadratic in the number of works sharing that tag.
        transaction.commit().await?;
        Ok(tag_id)
    }

    pub async fn link_tag(&self, work_id: i64, tag_id: i64) -> Result<()> {
        self.link_tag_owned(work_id, tag_id, "external", None).await
    }

    pub async fn link_scanner_tag(
        &self,
        work_id: i64,
        tag_id: i64,
        seen_token: &str,
    ) -> Result<()> {
        self.link_tag_owned(work_id, tag_id, "scanner", Some(seen_token))
            .await
    }

    pub async fn link_current_scanner_tag(&self, work_id: i64, tag_id: i64) -> Result<()> {
        let seen_token = sqlx::query_scalar::<_, String>(
            "SELECT token FROM scanner_locks WHERE name = 'library'",
        )
        .fetch_optional(&self.pool)
        .await?
        .unwrap_or_else(|| "direct".to_string());
        self.link_scanner_tag(work_id, tag_id, &seen_token).await
    }

    async fn link_tag_owned(
        &self,
        work_id: i64,
        tag_id: i64,
        owner: &str,
        seen_token: Option<&str>,
    ) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        if owner == "scanner" {
            let seen_token = seen_token.ok_or_else(|| {
                AppError::Other("scanner tag link is missing a lease token".to_string())
            })?;
            require_scanner_lease(&mut transaction, "library", seen_token).await?;
        }
        sqlx::query("INSERT OR IGNORE INTO work_tags (work_id, tag_id) VALUES (?1, ?2)")
            .bind(work_id)
            .bind(tag_id)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            r#"
            INSERT INTO work_tag_sources (work_id, tag_id, owner, seen_token)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(work_id, tag_id, owner) DO UPDATE SET
                seen_token = excluded.seen_token
            "#,
        )
        .bind(work_id)
        .bind(tag_id)
        .bind(owner)
        .bind(seen_token)
        .execute(&mut *transaction)
        .await?;
        sqlx::query("UPDATE tags SET count = (SELECT COUNT(*) FROM work_tags WHERE tag_id = ?1) WHERE id = ?1")
            .bind(tag_id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
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

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_scanner_external_id(
        &self,
        work_id: i64,
        source: &str,
        external_id: &str,
        token: Option<&str>,
        url: Option<&str>,
        seen_token: &str,
    ) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        require_scanner_lease(&mut transaction, "library", seen_token).await?;
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
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
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

    pub async fn create_job_if_absent(
        &self,
        job_type: &str,
        status: &str,
        payload: Value,
    ) -> Result<(i64, bool)> {
        let payload = payload.to_string();
        for _ in 0..3 {
            let now = Utc::now();
            let inserted = sqlx::query_scalar::<_, i64>(
                r#"
                INSERT OR IGNORE INTO jobs (job_type, status, payload_json, created_at, updated_at)
                VALUES (?1, ?2, ?3, ?4, ?4)
                RETURNING id
                "#,
            )
            .bind(job_type)
            .bind(status)
            .bind(&payload)
            .bind(now)
            .fetch_optional(&self.pool)
            .await?;
            if let Some(id) = inserted {
                return Ok((id, true));
            }
            let existing = sqlx::query_as::<_, (i64, String)>(
                r#"
                SELECT id, status FROM jobs
                WHERE job_type = ?1 AND status IN ('queued', 'running')
                ORDER BY CASE WHEN status = 'queued' THEN 0 ELSE 1 END, id
                LIMIT 1
                "#,
            )
            .bind(job_type)
            .fetch_optional(&self.pool)
            .await?;
            if let Some((id, existing_status)) = existing {
                if existing_status == "queued" {
                    let merge_sql = if job_type == "scan-library" {
                        r#"
                        UPDATE jobs
                        SET
                            payload_json = json_set(
                                json_patch(payload_json, ?1),
                                '$.enqueue_enrichment',
                                json(CASE
                                    WHEN COALESCE(json_extract(payload_json, '$.enqueue_enrichment'), 0) != 0
                                      OR COALESCE(json_extract(?1, '$.enqueue_enrichment'), 0) != 0
                                    THEN 'true'
                                    ELSE 'false'
                                END)
                            ),
                            updated_at = ?2
                        WHERE id = ?3 AND status = 'queued'
                        "#
                    } else {
                        r#"
                        UPDATE jobs
                        SET payload_json = json_patch(payload_json, ?1), updated_at = ?2
                        WHERE id = ?3 AND status = 'queued'
                        "#
                    };
                    let updated = sqlx::query(merge_sql)
                        .bind(&payload)
                        .bind(Utc::now())
                        .bind(id)
                        .execute(&self.pool)
                        .await?
                        .rows_affected();
                    if updated > 0 {
                        return Ok((id, false));
                    }
                }
                continue;
            }
        }
        Err(AppError::Other(format!(
            "job {job_type} changed state repeatedly while being queued"
        )))
    }

    pub async fn create_work_job_once(
        &self,
        job_type: &str,
        work_id: i64,
        fingerprint: &str,
        payload: Value,
    ) -> Result<(i64, bool)> {
        let payload = payload.to_string();
        for _ in 0..3 {
            let inserted = sqlx::query_scalar::<_, i64>(
                r#"
                INSERT INTO jobs (job_type, status, payload_json, created_at, updated_at)
                SELECT ?1, 'queued', ?2, ?3, ?3
                WHERE NOT EXISTS (
                    SELECT 1 FROM jobs
                    WHERE job_type = ?1
                      AND status IN ('queued', 'running', 'done')
                      AND CAST(json_extract(payload_json, '$.work_id') AS INTEGER) = ?4
                      AND json_extract(payload_json, '$.fingerprint') = ?5
                )
                RETURNING id
                "#,
            )
            .bind(job_type)
            .bind(&payload)
            .bind(Utc::now())
            .bind(work_id)
            .bind(fingerprint)
            .fetch_optional(&self.pool)
            .await?;
            if let Some(id) = inserted {
                return Ok((id, true));
            }
            if let Some(id) = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT id FROM jobs
                WHERE job_type = ?1
                  AND status IN ('queued', 'running', 'done')
                  AND CAST(json_extract(payload_json, '$.work_id') AS INTEGER) = ?2
                  AND json_extract(payload_json, '$.fingerprint') = ?3
                ORDER BY id DESC
                LIMIT 1
                "#,
            )
            .bind(job_type)
            .bind(work_id)
            .bind(fingerprint)
            .fetch_optional(&self.pool)
            .await?
            {
                return Ok((id, false));
            }
        }
        Err(AppError::Other(format!(
            "work job {job_type}/{work_id} changed state repeatedly while being queued"
        )))
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
        let mut transaction = self.pool.begin().await?;
        let job_type = sqlx::query_scalar::<_, String>("SELECT job_type FROM jobs WHERE id = ?1")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("job {id} not found")))?;
        if matches!(job_type.as_str(), "scan-library" | "rebuild-search-index") {
            let successor = sqlx::query_scalar::<_, i64>(
                "SELECT id FROM jobs WHERE job_type = ?1 AND status = 'queued' AND id != ?2 ORDER BY id LIMIT 1",
            )
            .bind(&job_type)
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some(successor_id) = successor {
                let merge_sql = if job_type == "scan-library" {
                    r#"
                    UPDATE jobs
                    SET
                        payload_json = json_set(
                            json_patch(
                                COALESCE((SELECT payload_json FROM jobs WHERE id = ?1), '{}'),
                                payload_json
                            ),
                            '$.enqueue_enrichment',
                            json(CASE
                                WHEN COALESCE(json_extract(
                                    COALESCE((SELECT payload_json FROM jobs WHERE id = ?1), '{}'),
                                    '$.enqueue_enrichment'
                                ), 0) != 0
                                  OR COALESCE(json_extract(payload_json, '$.enqueue_enrichment'), 0) != 0
                                THEN 'true'
                                ELSE 'false'
                            END)
                        ),
                        updated_at = ?2
                    WHERE id = ?3 AND status = 'queued'
                    "#
                } else {
                    r#"
                    UPDATE jobs
                    SET
                        payload_json = json_patch(
                            COALESCE((SELECT payload_json FROM jobs WHERE id = ?1), '{}'),
                            payload_json
                        ),
                        updated_at = ?2
                    WHERE id = ?3 AND status = 'queued'
                    "#
                };
                sqlx::query(merge_sql)
                    .bind(id)
                    .bind(Utc::now())
                    .bind(successor_id)
                    .execute(&mut *transaction)
                    .await?;
                sqlx::query(
                    "UPDATE jobs SET status = 'superseded', last_error = ?1, updated_at = ?2 WHERE id = ?3",
                )
                .bind(format!("{last_error}; retry covered by queued successor {successor_id}"))
                .bind(Utc::now())
                .bind(id)
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
                return Ok(());
            }
        }
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
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
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

    pub async fn scanner_fingerprint_matches(
        &self,
        work_id: i64,
        fingerprint: &str,
    ) -> Result<bool> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM scanner_works WHERE work_id = ?1 AND fingerprint = ?2",
        )
        .bind(work_id)
        .bind(fingerprint)
        .fetch_optional(&self.pool)
        .await?
        .is_some())
    }

    pub async fn apply_scanner_enrichment(
        &self,
        work_id: i64,
        fingerprint: &str,
        enrichment: ScannerEnrichmentInput,
    ) -> Result<bool> {
        let mut transaction = self.pool.begin().await?;
        // This no-op UPDATE both checks the generation and acquires SQLite's
        // writer reservation. The scanner cannot publish a different
        // fingerprint until all enrichment fields, ids, and tags commit.
        let guarded = sqlx::query(
            r#"
            UPDATE scanner_works
            SET fingerprint = fingerprint
            WHERE work_id = ?1
              AND fingerprint = ?2
              AND (
                  json_extract((SELECT meta_json FROM works WHERE id = ?1), '$._scanner_fingerprint') IS NULL
                  OR json_extract((SELECT meta_json FROM works WHERE id = ?1), '$._scanner_fingerprint') = ?2
              )
            "#,
        )
        .bind(work_id)
        .bind(fingerprint)
        .execute(&mut *transaction)
        .await?
        .rows_affected()
            > 0;
        if !guarded {
            transaction.rollback().await?;
            return Ok(false);
        }

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
        .bind(enrichment.title.as_deref())
        .bind(enrichment.category.as_deref())
        .bind(enrichment.description.as_deref())
        .bind(enrichment.rating)
        .bind(enrichment.meta.to_string())
        .bind(Utc::now())
        .bind(work_id)
        .execute(&mut *transaction)
        .await?;

        for external in enrichment.external_ids {
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
            .bind(external.source)
            .bind(external.external_id)
            .bind(external.token)
            .bind(external.url)
            .execute(&mut *transaction)
            .await?;
        }

        for tag in enrichment.tags {
            let tag_id = sqlx::query_scalar::<_, i64>(
                r#"
                INSERT INTO tags (namespace, key, label, source)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(namespace, key) DO UPDATE SET
                    label = excluded.label,
                    source = excluded.source
                RETURNING id
                "#,
            )
            .bind(tag.namespace)
            .bind(tag.key)
            .bind(tag.label)
            .bind(tag.source)
            .fetch_one(&mut *transaction)
            .await?;
            sqlx::query("INSERT OR IGNORE INTO work_tags (work_id, tag_id) VALUES (?1, ?2)")
                .bind(work_id)
                .bind(tag_id)
                .execute(&mut *transaction)
                .await?;
            sqlx::query(
                r#"
                INSERT INTO work_tag_sources (work_id, tag_id, owner, seen_token)
                VALUES (?1, ?2, 'external', NULL)
                ON CONFLICT(work_id, tag_id, owner) DO NOTHING
                "#,
            )
            .bind(work_id)
            .bind(tag_id)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "UPDATE tags SET count = (SELECT COUNT(*) FROM work_tags WHERE tag_id = ?1) WHERE id = ?1",
            )
            .bind(tag_id)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(true)
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
        update_token: i64,
    ) -> Result<ProgressWrite> {
        let progress = progress.clamp(0.0, 1.0);
        let now = Utc::now();
        let mut transaction = self.pool.begin().await?;
        let exists = sqlx::query_scalar::<_, i64>("SELECT 1 FROM works WHERE id = ?1")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
        if !exists {
            return Err(AppError::NotFound(format!("work {id} not found")));
        }

        let history_affected = sqlx::query(
            r#"
            INSERT INTO reading_history (work_id, progress, position, last_opened_at, update_token)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(work_id) DO UPDATE SET
                progress = excluded.progress,
                position = excluded.position,
                last_opened_at = excluded.last_opened_at,
                update_token = excluded.update_token
            WHERE excluded.update_token > reading_history.update_token
            "#,
        )
        .bind(id)
        .bind(progress)
        .bind(position)
        .bind(now)
        .bind(update_token)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if history_affected == 0 {
            let current = sqlx::query_as::<_, (f64, Option<String>)>(
                "SELECT progress, position FROM reading_history WHERE work_id = ?1",
            )
            .bind(id)
            .fetch_one(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return Ok(ProgressWrite {
                accepted: false,
                progress: current.0,
                position: current.1,
            });
        }

        sqlx::query(
            r#"
            UPDATE works SET progress = ?1
            WHERE id = ?2
            "#,
        )
        .bind(progress)
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(ProgressWrite {
            accepted: true,
            progress,
            position: position.map(str::to_string),
        })
    }

    pub async fn library(&self) -> Result<LibraryResponse> {
        self.library_page(None, i64::MAX, true).await
    }

    pub async fn library_page(
        &self,
        cursor: Option<&str>,
        limit: i64,
        include_context: bool,
    ) -> Result<LibraryResponse> {
        let cursor = cursor.map(decode_library_cursor).transpose()?;
        let limit = if limit == i64::MAX {
            i64::MAX - 1
        } else {
            limit.clamp(1, 500)
        };
        let fetch_limit = limit.saturating_add(1);
        const LIBRARY_SELECT: &str = r#"
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
        "#;
        let mut works = if let Some(cursor) = cursor.as_ref() {
            let sql = format!("{LIBRARY_SELECT}\nWHERE w.id < ?1\nORDER BY w.id DESC\nLIMIT ?2");
            sqlx::query_as::<_, WorkSummary>(&sql)
                .bind(cursor.next_id)
                .bind(fetch_limit)
                .fetch_all(&self.pool)
                .await?
        } else {
            let sql = format!("{LIBRARY_SELECT}\nORDER BY w.updated_at DESC, w.id DESC\nLIMIT ?1");
            sqlx::query_as::<_, WorkSummary>(&sql)
                .bind(fetch_limit)
                .fetch_all(&self.pool)
                .await?
        };
        let has_more = works.len() as i64 > limit;
        works.truncate(limit as usize);
        let next_cursor = if !has_more {
            None
        } else if cursor.is_some() {
            works
                .last()
                .map(|work| encode_library_cursor(work.id))
                .transpose()?
        } else {
            let snapshot_max_id =
                sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(id), 0) FROM works")
                    .fetch_one(&self.pool)
                    .await?;
            Some(encode_library_cursor(snapshot_max_id.saturating_add(1))?)
        };
        let (tags, jobs, history) = if include_context {
            tokio::try_join!(self.tags(), self.jobs(20), self.history(20))?
        } else {
            (Vec::new(), Vec::new(), Vec::new())
        };
        Ok(LibraryResponse {
            works,
            tags,
            jobs,
            history,
            next_cursor,
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

    pub async fn work_history(&self, work_id: i64) -> Result<Option<HistoryRecord>> {
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
            WHERE h.work_id = ?1
            "#,
        )
        .bind(work_id)
        .fetch_optional(&self.pool)
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
        let assets_query = sqlx::query_as::<_, Asset>(asset_query)
            .bind(id)
            .fetch_all(&self.pool);
        let tags_query = sqlx::query_as::<_, Tag>(
            r#"
            SELECT t.* FROM tags t
            JOIN work_tags wt ON wt.tag_id = t.id
            WHERE wt.work_id = ?1
            ORDER BY t.namespace, t.key
            "#,
        )
        .bind(id)
        .fetch_all(&self.pool);
        let external_ids_query =
            sqlx::query_as::<_, ExternalId>("SELECT * FROM external_ids WHERE work_id = ?1")
                .bind(id)
                .fetch_all(&self.pool);
        let (assets, tags, external_ids) =
            tokio::try_join!(assets_query, tags_query, external_ids_query)?;
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

    pub async fn work_asset_path(
        &self,
        work_id: i64,
        role: &str,
        variant: Option<&str>,
    ) -> Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            r#"
            SELECT path FROM assets
            WHERE work_id = ?1 AND role = ?2 AND variant = ?3
            ORDER BY id DESC
            LIMIT 1
            "#,
        )
        .bind(work_id)
        .bind(role)
        .bind(variant.unwrap_or(""))
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn asset_path_reference_count(&self, path: &str) -> Result<i64> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM assets WHERE path = ?1")
                .bind(path)
                .fetch_one(&self.pool)
                .await?,
        )
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
            "SELECT * FROM tags WHERE count > 0 ORDER BY count DESC, namespace, key LIMIT 500",
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
                  AND (
                    job_type NOT IN ('scan-library', 'rebuild-search-index')
                    OR NOT EXISTS (
                        SELECT 1 FROM jobs running
                        WHERE running.job_type = jobs.job_type AND running.status = 'running'
                    )
                  )
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
        let id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO audit_logs (action, status, payload_json, created_at)
            VALUES (?1, ?2, ?3, ?4)
            RETURNING id
            "#,
        )
        .bind(action)
        .bind(status)
        .bind(payload.to_string())
        .bind(Utc::now())
        .fetch_one(&self.pool)
        .await?;
        if id % AUDIT_PRUNE_INTERVAL == 0 {
            if let Err(err) = self.prune_audit_logs().await {
                tracing::warn!(error = %err, "failed to prune audit logs");
            }
        }
        Ok(())
    }

    async fn prune_audit_logs(&self) -> Result<()> {
        let cutoff = Utc::now() - ChronoDuration::days(AUDIT_RETENTION_DAYS);
        sqlx::query(
            r#"
            DELETE FROM audit_logs
            WHERE created_at < ?1
               OR id < COALESCE((
                    SELECT id FROM audit_logs
                    ORDER BY id DESC
                    LIMIT 1 OFFSET ?2
               ), 0)
            "#,
        )
        .bind(cutoff)
        .bind(AUDIT_MAX_RECORDS - 1)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

async fn require_scanner_lease(
    transaction: &mut Transaction<'_, Sqlite>,
    name: &str,
    token: &str,
) -> Result<()> {
    // Make the lease check the transaction's first write. A read followed by a
    // write can fail with SQLITE_BUSY_SNAPSHOT if the heartbeat commits between
    // them; this conditional renewal both fences the token and acquires the
    // SQLite writer reservation up front.
    let held =
        sqlx::query("UPDATE scanner_locks SET acquired_at = ?1 WHERE name = ?2 AND token = ?3")
            .bind(Utc::now())
            .bind(name)
            .bind(token)
            .execute(&mut **transaction)
            .await?
            .rows_affected()
            > 0;
    if !held {
        return Err(AppError::Other(format!(
            "scanner lease {name} is no longer held by this scan"
        )));
    }
    Ok(())
}

fn encode_library_cursor(next_id: i64) -> Result<String> {
    let encoded = serde_json::to_vec(&LibraryCursor { next_id })
        .map_err(|err| AppError::Other(format!("failed to encode library cursor: {err}")))?;
    Ok(URL_SAFE_NO_PAD.encode(encoded))
}

fn decode_library_cursor(encoded: &str) -> Result<LibraryCursor> {
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| AppError::BadRequest("invalid library cursor".to_string()))?;
    serde_json::from_slice(&bytes)
        .map_err(|_| AppError::BadRequest("invalid library cursor".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database_url(temp: &tempfile::TempDir) -> String {
        format!(
            "sqlite://{}",
            temp.path()
                .join("library.sqlite")
                .to_string_lossy()
                .replace('\\', "/")
        )
    }

    #[tokio::test]
    async fn migrates_reading_history_update_tokens() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        sqlx::query(
            r#"
            CREATE TABLE reading_history (
                work_id INTEGER PRIMARY KEY,
                progress REAL NOT NULL DEFAULT 0,
                position TEXT,
                last_opened_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )
            "#,
        )
        .execute(db.pool())
        .await
        .unwrap();

        db.migrate().await.unwrap();
        let columns = sqlx::query("PRAGMA table_info(reading_history)")
            .fetch_all(db.pool())
            .await
            .unwrap();
        assert!(columns
            .iter()
            .any(|row| row.get::<String, _>("name") == "update_token"));
    }

    #[tokio::test]
    async fn migrates_duplicate_asset_identity_and_preserves_cover() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        sqlx::query(
            r#"
            CREATE TABLE works (
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
            )
            "#,
        )
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE assets (
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
            )
            "#,
        )
        .execute(db.pool())
        .await
        .unwrap();
        let work_id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO works (kind, title, source_path) VALUES ('gallery', 'set', '/set') RETURNING id",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        let cover_id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO assets (work_id, path, mime, role, variant, position) VALUES (?1, '/set/a.jpg', 'image/jpeg', 'cover', '', 0) RETURNING id",
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO assets (work_id, path, mime, role, variant, position) VALUES (?1, '/set/a.jpg', 'image/jpeg', 'cover', '', 1)",
        )
        .bind(work_id)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE works SET cover_asset_id = ?1 WHERE id = ?2")
            .bind(cover_id)
            .bind(work_id)
            .execute(db.pool())
            .await
            .unwrap();

        db.migrate().await.unwrap();

        let assets =
            sqlx::query_as::<_, (i64, i64)>("SELECT id, position FROM assets WHERE work_id = ?1")
                .bind(work_id)
                .fetch_all(db.pool())
                .await
                .unwrap();
        assert_eq!(assets, vec![(cover_id, 0)]);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT cover_asset_id FROM works WHERE id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            cover_id
        );

        let same_id = db
            .upsert_asset(
                work_id,
                "/set/a.jpg",
                "image/jpeg",
                "cover",
                None,
                Some(7),
                None,
                json!({}),
            )
            .await
            .unwrap();
        assert_eq!(same_id, cover_id);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT position FROM assets WHERE id = ?1")
                .bind(cover_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            7
        );
    }

    #[tokio::test]
    async fn scanner_updates_preserve_metadata_and_external_tags() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "audio",
                "work",
                Some("/audio/work"),
                Some("Audio"),
                None,
                None,
                json!({
                    "page_count": 1,
                    "runtime": { "bookmark": 42 },
                    "enrichment": { "provider": "asmr.one" }
                }),
            )
            .await
            .unwrap();
        db.upsert_work(
            "audio",
            "work rescanned",
            Some("/audio/work"),
            Some("Audio"),
            None,
            None,
            json!({ "page_count": 2, "scanner": { "version": 2 } }),
        )
        .await
        .unwrap();
        let metadata = sqlx::query_scalar::<_, String>("SELECT meta_json FROM works WHERE id = ?1")
            .bind(work_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
        let metadata: Value = serde_json::from_str(&metadata).unwrap();
        assert_eq!(metadata["page_count"], 2);
        assert_eq!(metadata["runtime"]["bookmark"], 42);
        assert_eq!(metadata["enrichment"]["provider"], "asmr.one");

        let scanner_tag = db
            .upsert_tag(
                "audio",
                "asmr",
                "ASMR",
                None,
                None,
                "audio-folder",
                None,
                None,
            )
            .await
            .unwrap();
        let external_tag = db
            .upsert_tag(
                "provider", "asmr-one", "asmr.one", None, None, "asmr.one", None, None,
            )
            .await
            .unwrap();
        db.link_tag(work_id, scanner_tag).await.unwrap();
        db.link_tag(work_id, external_tag).await.unwrap();
        db.mark_scanner_work(work_id, "legacy|audio", "legacy", None)
            .await
            .unwrap();
        sqlx::query("DELETE FROM work_tag_sources WHERE work_id = ?1")
            .bind(work_id)
            .execute(db.pool())
            .await
            .unwrap();

        db.migrate_scanner_ownership().await.unwrap();

        let owners = sqlx::query_as::<_, (i64, String)>(
            "SELECT tag_id, owner FROM work_tag_sources WHERE work_id = ?1 ORDER BY tag_id, owner",
        )
        .bind(work_id)
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(
            owners,
            vec![
                (scanner_tag, "scanner".to_string()),
                (external_tag, "external".to_string())
            ]
        );

        assert!(db
            .try_acquire_scanner_lock("library", "new-scan", 3600)
            .await
            .unwrap());
        db.finish_scanner_work(work_id, "legacy|audio", "new-scan", "fingerprint")
            .await
            .unwrap();
        let remaining_tags = sqlx::query_scalar::<_, i64>(
            "SELECT tag_id FROM work_tags WHERE work_id = ?1 ORDER BY tag_id",
        )
        .bind(work_id)
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(remaining_tags, vec![external_tag]);
    }

    #[tokio::test]
    async fn stale_scanner_lease_cannot_mutate_work_assets_or_tags() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "gallery",
                "current title",
                Some("/gallery/set"),
                Some("Gallery"),
                None,
                None,
                json!({ "generation": "current" }),
            )
            .await
            .unwrap();

        assert!(db
            .try_acquire_scanner_lock("library", "stale", 60)
            .await
            .unwrap());
        db.release_scanner_lock("library", "stale").await.unwrap();
        assert!(db
            .try_acquire_scanner_lock("library", "current", 60)
            .await
            .unwrap());

        assert!(db
            .upsert_scanner_work(
                "gallery",
                "stale title",
                Some("/gallery/set"),
                Some("Gallery"),
                None,
                None,
                json!({ "generation": "stale" }),
                "stale",
                "stale-fingerprint",
            )
            .await
            .is_err());
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT title FROM works WHERE id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "current title"
        );

        assert!(db
            .upsert_scanner_asset(
                work_id,
                "/gallery/set/stale.jpg",
                "image/jpeg",
                "image",
                None,
                Some(0),
                Some(10),
                json!({}),
                "stale",
            )
            .await
            .is_err());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM assets WHERE work_id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            0
        );

        assert!(db
            .upsert_and_link_scanner_tag(
                work_id,
                "folder",
                "stale",
                "stale",
                None,
                None,
                "scanner-test",
                None,
                None,
                "stale",
            )
            .await
            .is_err());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM work_tags WHERE work_id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn enrichment_commit_is_fenced_by_scanner_fingerprint() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "novel",
                "local title",
                Some("/novels/book.epub"),
                Some("Light Novel"),
                None,
                None,
                json!({ "local": true }),
            )
            .await
            .unwrap();
        db.mark_scanner_work(work_id, "novel|/novels", "scan", Some("current"))
            .await
            .unwrap();

        let stale = ScannerEnrichmentInput {
            title: Some("stale remote title".to_string()),
            category: None,
            description: None,
            rating: None,
            meta: json!({ "remote": "stale" }),
            tags: vec![EnrichmentTagInput {
                namespace: "ln".to_string(),
                key: "stale".to_string(),
                label: "stale".to_string(),
                source: "test".to_string(),
            }],
            external_ids: vec![EnrichmentExternalIdInput {
                source: "test".to_string(),
                external_id: "stale".to_string(),
                token: None,
                url: None,
            }],
        };
        assert!(!db
            .apply_scanner_enrichment(work_id, "old", stale)
            .await
            .unwrap());
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT title FROM works WHERE id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "local title"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM work_tags WHERE work_id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            0
        );

        let current = ScannerEnrichmentInput {
            title: Some("current remote title".to_string()),
            category: None,
            description: None,
            rating: Some(4.5),
            meta: json!({ "remote": "current" }),
            tags: vec![EnrichmentTagInput {
                namespace: "ln".to_string(),
                key: "current".to_string(),
                label: "current".to_string(),
                source: "test".to_string(),
            }],
            external_ids: vec![EnrichmentExternalIdInput {
                source: "test".to_string(),
                external_id: "current".to_string(),
                token: None,
                url: Some("https://example.test/current".to_string()),
            }],
        };
        assert!(db
            .apply_scanner_enrichment(work_id, "current", current)
            .await
            .unwrap());
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT title FROM works WHERE id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "current remote title"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM work_tags WHERE work_id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM external_ids WHERE work_id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            1
        );

        // A new scan publishes its generation marker before its final
        // scanner_works fingerprint. The old response must be rejected in that
        // in-between window as well.
        sqlx::query(
            "UPDATE works SET meta_json = json_set(meta_json, '$._scanner_fingerprint', 'next') WHERE id = ?1",
        )
        .bind(work_id)
        .execute(db.pool())
        .await
        .unwrap();
        let late_current = ScannerEnrichmentInput {
            title: Some("late old response".to_string()),
            category: None,
            description: None,
            rating: None,
            meta: json!({ "_scanner_fingerprint": "current" }),
            tags: Vec::new(),
            external_ids: Vec::new(),
        };
        assert!(!db
            .apply_scanner_enrichment(work_id, "current", late_current)
            .await
            .unwrap());
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT title FROM works WHERE id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "current remote title"
        );
    }

    #[tokio::test]
    async fn scanner_refreshes_only_changed_asset_versions() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "gallery",
                "set",
                Some("/gallery/set"),
                Some("Gallery"),
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();
        assert!(db
            .try_acquire_scanner_lock("library", "asset-version", 60)
            .await
            .unwrap());

        let assets = |second_version: &str| {
            vec![
                ScannerAssetInput {
                    path: "/gallery/set/one.jpg".to_string(),
                    mime: "image/jpeg".to_string(),
                    role: "image".to_string(),
                    variant: None,
                    position: Some(0),
                    size: Some(3),
                    meta: json!({ "_source_version": "one-v1" }),
                },
                ScannerAssetInput {
                    path: "/gallery/set/two.jpg".to_string(),
                    mime: "image/jpeg".to_string(),
                    role: "image".to_string(),
                    variant: None,
                    position: Some(1),
                    size: Some(3),
                    meta: json!({ "_source_version": second_version }),
                },
            ]
        };
        db.upsert_scanner_assets(work_id, assets("two-v1"), "asset-version")
            .await
            .unwrap();
        sqlx::query("UPDATE assets SET created_at = '2000-01-01T00:00:00Z' WHERE work_id = ?1")
            .bind(work_id)
            .execute(db.pool())
            .await
            .unwrap();
        db.upsert_scanner_assets(work_id, assets("two-v2"), "asset-version")
            .await
            .unwrap();

        let versions = sqlx::query_as::<_, (String, String)>(
            "SELECT path, created_at FROM assets WHERE work_id = ?1 ORDER BY path",
        )
        .bind(work_id)
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(versions[0].1, "2000-01-01T00:00:00Z");
        assert_ne!(versions[1].1, "2000-01-01T00:00:00Z");
    }

    #[tokio::test]
    async fn tag_context_excludes_unreferenced_translation_rows() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "comic",
                "tag context",
                Some("/comic/tag-context"),
                None,
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();
        let used = db
            .upsert_tag("test", "used", "used", None, None, "test", None, None)
            .await
            .unwrap();
        db.upsert_tag(
            "test",
            "unused",
            "unused",
            None,
            None,
            "translation",
            None,
            None,
        )
        .await
        .unwrap();
        db.link_tag(work_id, used).await.unwrap();

        let tags = db.tags().await.unwrap();
        assert_eq!(
            tags.iter().map(|tag| tag.key.as_str()).collect::<Vec<_>>(),
            vec!["used"]
        );
    }

    #[tokio::test]
    async fn library_keyset_pages_are_complete_and_context_is_optional() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let base_time = Utc::now() - ChronoDuration::minutes(10);
        let mut expected = Vec::new();
        for index in 0..5 {
            let id = db
                .upsert_work(
                    "comic",
                    &format!("work {index}"),
                    Some(&format!("/comic/{index}")),
                    None,
                    None,
                    None,
                    json!({}),
                )
                .await
                .unwrap();
            sqlx::query("UPDATE works SET updated_at = ?1 WHERE id = ?2")
                .bind(base_time + ChronoDuration::seconds(index))
                .bind(id)
                .execute(db.pool())
                .await
                .unwrap();
            expected.push(id);
        }
        expected.reverse();
        let expected_ids = expected
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        db.create_job("test-job", "queued", json!({}))
            .await
            .unwrap();

        let mut cursor = None;
        let mut actual = std::collections::BTreeSet::new();
        let mut inserted_after_snapshot = None;
        loop {
            let include_context = cursor.is_none();
            let page = db
                .library_page(cursor.as_deref(), 2, include_context)
                .await
                .unwrap();
            if include_context {
                assert_eq!(page.jobs.len(), 1);
            } else {
                assert!(page.jobs.is_empty());
                assert!(page.tags.is_empty());
                assert!(page.history.is_empty());
            }
            actual.extend(page.works.into_iter().map(|work| work.id));
            cursor = page.next_cursor;
            if inserted_after_snapshot.is_none() && cursor.is_some() {
                sqlx::query("UPDATE works SET updated_at = ?1 WHERE id = ?2")
                    .bind(Utc::now())
                    .bind(expected[0])
                    .execute(db.pool())
                    .await
                    .unwrap();
                inserted_after_snapshot = Some(
                    db.upsert_work(
                        "comic",
                        "inserted during hydration",
                        Some("/comic/new"),
                        None,
                        None,
                        None,
                        json!({}),
                    )
                    .await
                    .unwrap(),
                );
            }
            if cursor.is_none() {
                break;
            }
        }

        assert_eq!(actual, expected_ids);
        assert!(!actual.contains(&inserted_after_snapshot.unwrap()));
        assert!(matches!(
            db.library_page(Some("not-a-cursor"), 2, false).await,
            Err(AppError::BadRequest(_))
        ));
    }

    #[tokio::test]
    async fn running_scan_keeps_one_queued_successor() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();

        let (first_id, first_created) = db
            .create_job_if_absent("scan-library", "queued", json!({ "source": "first" }))
            .await
            .unwrap();
        assert!(first_created);
        let running = db.claim_next_queued_job().await.unwrap().unwrap();
        assert_eq!(running.id, first_id);

        let (successor_id, successor_created) = db
            .create_job_if_absent("scan-library", "queued", json!({ "source": "watcher" }))
            .await
            .unwrap();
        assert!(successor_created);
        assert_ne!(successor_id, first_id);
        let (same_id, duplicate_created) = db
            .create_job_if_absent(
                "scan-library",
                "queued",
                json!({ "enqueue_enrichment": true }),
            )
            .await
            .unwrap();
        assert!(!duplicate_created);
        assert_eq!(same_id, successor_id);
        let (_, watcher_duplicate_created) = db
            .create_job_if_absent(
                "scan-library",
                "queued",
                json!({ "source": "watcher", "enqueue_enrichment": false }),
            )
            .await
            .unwrap();
        assert!(!watcher_duplicate_created);
        let queued_payload =
            sqlx::query_scalar::<_, String>("SELECT payload_json FROM jobs WHERE id = ?1")
                .bind(successor_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&queued_payload)
                .unwrap()
                .get("enqueue_enrichment")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert!(db.claim_next_queued_job().await.unwrap().is_none());

        db.reschedule_job(first_id, "transient scan failure", 30)
            .await
            .unwrap();
        let first_status = sqlx::query_scalar::<_, String>("SELECT status FROM jobs WHERE id = ?1")
            .bind(first_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(first_status, "superseded");
        let successor_payload =
            sqlx::query_scalar::<_, String>("SELECT payload_json FROM jobs WHERE id = ?1")
                .bind(successor_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&successor_payload)
                .unwrap()
                .get("enqueue_enrichment")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            db.claim_next_queued_job().await.unwrap().unwrap().id,
            successor_id
        );
    }

    #[tokio::test]
    async fn restart_merges_scan_successor_before_requeueing_running_job() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();

        let (running_id, _) = db
            .create_job_if_absent(
                "scan-library",
                "queued",
                json!({ "source": "manual", "enqueue_enrichment": false }),
            )
            .await
            .unwrap();
        assert_eq!(
            db.claim_next_queued_job().await.unwrap().unwrap().id,
            running_id
        );
        let (successor_id, created) = db
            .create_job_if_absent(
                "scan-library",
                "queued",
                json!({ "source": "watcher", "enqueue_enrichment": true }),
            )
            .await
            .unwrap();
        assert!(created);

        assert_eq!(db.requeue_interrupted_running_jobs().await.unwrap(), 1);
        let (running_status, running_payload) = sqlx::query_as::<_, (String, String)>(
            "SELECT status, payload_json FROM jobs WHERE id = ?1",
        )
        .bind(running_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(running_status, "queued");
        let running_payload: Value = serde_json::from_str(&running_payload).unwrap();
        assert_eq!(
            running_payload
                .get("enqueue_enrichment")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            running_payload.get("source").and_then(Value::as_str),
            Some("watcher")
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM jobs WHERE id = ?1")
                .bind(successor_id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "superseded"
        );
        assert_eq!(
            db.claim_next_queued_job().await.unwrap().unwrap().id,
            running_id
        );
    }

    #[tokio::test]
    async fn progress_and_history_are_written_together() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let work_id = db
            .upsert_work(
                "novel",
                "book",
                Some("/novel/book.epub"),
                None,
                None,
                None,
                json!({}),
            )
            .await
            .unwrap();

        let before_updated_at =
            sqlx::query_scalar::<_, DateTime<Utc>>("SELECT updated_at FROM works WHERE id = ?1")
                .bind(work_id)
                .fetch_one(db.pool())
                .await
                .unwrap();

        let saved = db
            .update_work_progress(work_id, 0.42, Some("chapter-3"), 20)
            .await
            .unwrap();
        assert!(saved.accepted);
        let stale = db
            .update_work_progress(work_id, 0.1, Some("chapter-1"), 10)
            .await
            .unwrap();
        assert!(!stale.accepted);
        assert_eq!(stale.progress, 0.42);
        assert_eq!(stale.position.as_deref(), Some("chapter-3"));
        let work = sqlx::query_as::<_, (f64, DateTime<Utc>)>(
            "SELECT progress, updated_at FROM works WHERE id = ?1",
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        let history = sqlx::query_as::<_, (f64, Option<String>, DateTime<Utc>, i64)>(
            "SELECT progress, position, last_opened_at, update_token FROM reading_history WHERE work_id = ?1",
        )
        .bind(work_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(work.0, history.0);
        assert_eq!(history.1.as_deref(), Some("chapter-3"));
        assert_eq!(work.1, before_updated_at);
        assert!(history.2 >= before_updated_at);
        assert_eq!(history.3, 20);
    }

    #[tokio::test]
    async fn audit_retention_removes_expired_records() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        sqlx::query(
            "INSERT INTO audit_logs (action, status, payload_json, created_at) VALUES ('old', 'ok', '{}', ?1), ('new', 'ok', '{}', ?2)",
        )
        .bind(Utc::now() - ChronoDuration::days(AUDIT_RETENTION_DAYS + 1))
        .bind(Utc::now())
        .execute(db.pool())
        .await
        .unwrap();

        db.prune_audit_logs().await.unwrap();
        let actions = sqlx::query_scalar::<_, String>("SELECT action FROM audit_logs ORDER BY id")
            .fetch_all(db.pool())
            .await
            .unwrap();
        assert_eq!(actions, vec!["new"]);
    }
}
