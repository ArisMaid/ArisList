use sha2::{Digest, Sha256};

use crate::error::{AppError, Result};
use crate::{Pool, Row, Sqlite};
use sqlx_sqlite::SqliteConnection;

struct Migration {
    version: i64,
    name: &'static str,
    statements: &'static [&'static str],
}

// Applied migrations are append-only. Changing a name or statement after a
// release intentionally fails checksum validation instead of silently
// accepting schema drift.
const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "legacy-schema-baseline",
        statements: &[],
    },
    Migration {
        version: 2,
        name: "derivative-cache-ledger",
        statements: &[
            r#"
            CREATE TABLE derivatives (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_kind TEXT NOT NULL,
                source_id INTEGER NOT NULL,
                variant TEXT NOT NULL,
                source_version TEXT NOT NULL,
                relative_path TEXT NOT NULL UNIQUE,
                mime TEXT NOT NULL,
                width INTEGER NOT NULL DEFAULT 0 CHECK(width >= 0),
                height INTEGER NOT NULL DEFAULT 0 CHECK(height >= 0),
                bytes INTEGER NOT NULL DEFAULT 0 CHECK(bytes >= 0),
                status TEXT NOT NULL CHECK(status IN (
                    'queued', 'generating', 'ready', 'failed', 'stale', 'orphan', 'evicting'
                )),
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                last_access_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                retry_at TEXT,
                error_count INTEGER NOT NULL DEFAULT 0 CHECK(error_count >= 0),
                last_error TEXT,
                UNIQUE(source_kind, source_id, variant, source_version)
            )
            "#,
            r#"
            CREATE INDEX idx_derivatives_lru
            ON derivatives(status, last_access_at, id)
            "#,
            r#"
            CREATE INDEX idx_derivatives_source
            ON derivatives(source_kind, source_id, variant, status)
            "#,
            r#"
            CREATE INDEX idx_derivatives_recovery
            ON derivatives(status, updated_at, id)
            "#,
            r#"
            CREATE TABLE derivative_cache_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                resident_bytes INTEGER NOT NULL DEFAULT 0 CHECK(resident_bytes >= 0),
                resident_files INTEGER NOT NULL DEFAULT 0 CHECK(resident_files >= 0),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )
            "#,
            r#"
            INSERT INTO derivative_cache_state (singleton, resident_bytes, resident_files)
            VALUES (1, 0, 0)
            "#,
            r#"
            CREATE TABLE derivative_status_counts (
                status TEXT PRIMARY KEY,
                row_count INTEGER NOT NULL DEFAULT 0 CHECK(row_count >= 0)
            )
            "#,
            r#"
            INSERT INTO derivative_status_counts (status, row_count)
            VALUES
                ('queued', 0), ('generating', 0), ('ready', 0), ('failed', 0),
                ('stale', 0), ('orphan', 0), ('evicting', 0)
            "#,
            r#"
            CREATE TRIGGER derivatives_status_after_insert
            AFTER INSERT ON derivatives
            BEGIN
                UPDATE derivative_status_counts
                SET row_count = row_count + 1
                WHERE status = NEW.status;
            END
            "#,
            r#"
            CREATE TRIGGER derivatives_status_after_update
            AFTER UPDATE OF status ON derivatives
            WHEN OLD.status != NEW.status
            BEGIN
                UPDATE derivative_status_counts
                SET row_count = MAX(0, row_count - 1)
                WHERE status = OLD.status;
                UPDATE derivative_status_counts
                SET row_count = row_count + 1
                WHERE status = NEW.status;
            END
            "#,
            r#"
            CREATE TRIGGER derivatives_status_after_delete
            AFTER DELETE ON derivatives
            BEGIN
                UPDATE derivative_status_counts
                SET row_count = MAX(0, row_count - 1)
                WHERE status = OLD.status;
            END
            "#,
            r#"
            CREATE TRIGGER derivatives_capacity_after_insert
            AFTER INSERT ON derivatives
            WHEN NEW.bytes > 0
             AND NEW.status IN ('ready', 'stale', 'orphan', 'evicting')
            BEGIN
                UPDATE derivative_cache_state
                SET resident_bytes = resident_bytes + NEW.bytes,
                    resident_files = resident_files + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
            r#"
            CREATE TRIGGER derivatives_capacity_after_update
            AFTER UPDATE OF status, bytes ON derivatives
            WHEN (OLD.bytes > 0 AND OLD.status IN ('ready', 'stale', 'orphan', 'evicting'))
              OR (NEW.bytes > 0 AND NEW.status IN ('ready', 'stale', 'orphan', 'evicting'))
            BEGIN
                UPDATE derivative_cache_state
                SET resident_bytes = MAX(
                        0,
                        resident_bytes
                        - CASE
                            WHEN OLD.bytes > 0 AND OLD.status IN ('ready', 'stale', 'orphan', 'evicting')
                            THEN OLD.bytes ELSE 0
                          END
                        + CASE
                            WHEN NEW.bytes > 0 AND NEW.status IN ('ready', 'stale', 'orphan', 'evicting')
                            THEN NEW.bytes ELSE 0
                          END
                    ),
                    resident_files = MAX(
                        0,
                        resident_files
                        - CASE
                            WHEN OLD.bytes > 0 AND OLD.status IN ('ready', 'stale', 'orphan', 'evicting')
                            THEN 1 ELSE 0
                          END
                        + CASE
                            WHEN NEW.bytes > 0 AND NEW.status IN ('ready', 'stale', 'orphan', 'evicting')
                            THEN 1 ELSE 0
                          END
                    ),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
            r#"
            CREATE TRIGGER derivatives_capacity_after_delete
            AFTER DELETE ON derivatives
            WHEN OLD.bytes > 0
             AND OLD.status IN ('ready', 'stale', 'orphan', 'evicting')
            BEGIN
                UPDATE derivative_cache_state
                SET resident_bytes = MAX(0, resident_bytes - OLD.bytes),
                    resident_files = MAX(0, resident_files - 1),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
        ],
    },
    Migration {
        version: 3,
        name: "catalog-v2-foundation",
        statements: &[
            r#"
            CREATE TABLE work_stats (
                work_id INTEGER PRIMARY KEY REFERENCES works(id) ON DELETE CASCADE,
                asset_count INTEGER NOT NULL DEFAULT 0 CHECK(asset_count >= 0),
                tag_count INTEGER NOT NULL DEFAULT 0 CHECK(tag_count >= 0),
                image_count INTEGER NOT NULL DEFAULT 0 CHECK(image_count >= 0),
                track_count INTEGER NOT NULL DEFAULT 0 CHECK(track_count >= 0),
                page_count INTEGER NOT NULL DEFAULT 0 CHECK(page_count >= 0),
                catalog_revision INTEGER NOT NULL DEFAULT 0,
                computed_at TEXT
            )
            "#,
            r#"
            INSERT INTO work_stats (work_id)
            SELECT id FROM works
            "#,
            r#"
            CREATE TABLE catalog_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                revision INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )
            "#,
            r#"
            INSERT INTO catalog_state (singleton, revision) VALUES (1, 1)
            "#,
            r#"
            CREATE INDEX idx_work_tags_tag_work
            ON work_tags(tag_id, work_id)
            "#,
            r#"
            CREATE INDEX idx_work_stats_pending
            ON work_stats(computed_at, work_id)
            "#,
            r#"
            CREATE TRIGGER catalog_work_after_insert
            AFTER INSERT ON works
            BEGIN
                INSERT OR IGNORE INTO work_stats (work_id, computed_at)
                VALUES (NEW.id, strftime('%Y-%m-%dT%H:%M:%fZ','now'));
                UPDATE catalog_state
                SET revision = revision + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
            r#"
            CREATE TRIGGER catalog_work_after_update
            AFTER UPDATE OF kind, title, subtitle, category, rating, progress,
                            cover_asset_id, meta_json, updated_at ON works
            BEGIN
                UPDATE catalog_state
                SET revision = revision + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
            r#"
            CREATE TRIGGER catalog_work_after_delete
            AFTER DELETE ON works
            BEGIN
                UPDATE catalog_state
                SET revision = revision + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
            r#"
            CREATE TRIGGER work_stats_asset_after_insert
            AFTER INSERT ON assets
            BEGIN
                INSERT OR IGNORE INTO work_stats (work_id) VALUES (NEW.work_id);
                UPDATE work_stats
                SET asset_count = asset_count + 1,
                    image_count = image_count + CASE WHEN NEW.mime LIKE 'image/%' THEN 1 ELSE 0 END,
                    track_count = track_count + CASE
                        WHEN NEW.role = 'track' OR NEW.mime LIKE 'audio/%' THEN 1 ELSE 0 END,
                    page_count = page_count + CASE
                        WHEN NEW.role = 'page' THEN 1
                        WHEN NEW.role = 'archive' THEN CAST(
                            COALESCE(json_extract(NEW.meta_json, '$.page_count'), 0) AS INTEGER
                        )
                        ELSE 0
                    END
                WHERE work_id = NEW.work_id;
            END
            "#,
            r#"
            CREATE TRIGGER work_stats_asset_after_delete
            AFTER DELETE ON assets
            BEGIN
                UPDATE work_stats
                SET asset_count = MAX(0, asset_count - 1),
                    image_count = MAX(0, image_count - CASE
                        WHEN OLD.mime LIKE 'image/%' THEN 1 ELSE 0 END),
                    track_count = MAX(0, track_count - CASE
                        WHEN OLD.role = 'track' OR OLD.mime LIKE 'audio/%' THEN 1 ELSE 0 END),
                    page_count = MAX(0, page_count - CASE
                        WHEN OLD.role = 'page' THEN 1
                        WHEN OLD.role = 'archive' THEN CAST(
                            COALESCE(json_extract(OLD.meta_json, '$.page_count'), 0) AS INTEGER
                        )
                        ELSE 0
                    END)
                WHERE work_id = OLD.work_id;
            END
            "#,
            r#"
            CREATE TRIGGER work_stats_asset_after_update
            AFTER UPDATE OF work_id, mime, role, meta_json ON assets
            BEGIN
                UPDATE work_stats
                SET asset_count = MAX(0, asset_count - 1),
                    image_count = MAX(0, image_count - CASE
                        WHEN OLD.mime LIKE 'image/%' THEN 1 ELSE 0 END),
                    track_count = MAX(0, track_count - CASE
                        WHEN OLD.role = 'track' OR OLD.mime LIKE 'audio/%' THEN 1 ELSE 0 END),
                    page_count = MAX(0, page_count - CASE
                        WHEN OLD.role = 'page' THEN 1
                        WHEN OLD.role = 'archive' THEN CAST(
                            COALESCE(json_extract(OLD.meta_json, '$.page_count'), 0) AS INTEGER
                        )
                        ELSE 0
                    END)
                WHERE work_id = OLD.work_id;
                INSERT OR IGNORE INTO work_stats (work_id) VALUES (NEW.work_id);
                UPDATE work_stats
                SET asset_count = asset_count + 1,
                    image_count = image_count + CASE WHEN NEW.mime LIKE 'image/%' THEN 1 ELSE 0 END,
                    track_count = track_count + CASE
                        WHEN NEW.role = 'track' OR NEW.mime LIKE 'audio/%' THEN 1 ELSE 0 END,
                    page_count = page_count + CASE
                        WHEN NEW.role = 'page' THEN 1
                        WHEN NEW.role = 'archive' THEN CAST(
                            COALESCE(json_extract(NEW.meta_json, '$.page_count'), 0) AS INTEGER
                        )
                        ELSE 0
                    END
                WHERE work_id = NEW.work_id;
            END
            "#,
            r#"
            CREATE TRIGGER work_stats_tag_after_insert
            AFTER INSERT ON work_tags
            BEGIN
                INSERT OR IGNORE INTO work_stats (work_id) VALUES (NEW.work_id);
                UPDATE work_stats
                SET tag_count = tag_count + 1
                WHERE work_id = NEW.work_id;
            END
            "#,
            r#"
            CREATE TRIGGER work_stats_tag_after_delete
            AFTER DELETE ON work_tags
            BEGIN
                UPDATE work_stats
                SET tag_count = MAX(0, tag_count - 1)
                WHERE work_id = OLD.work_id;
            END
            "#,
        ],
    },
    Migration {
        version: 4,
        name: "catalog-v2-trigger-conflict-repair",
        statements: &[
            "DROP TRIGGER IF EXISTS catalog_work_after_insert",
            r#"
            CREATE TRIGGER catalog_work_after_insert
            AFTER INSERT ON works
            BEGIN
                INSERT INTO work_stats (work_id, computed_at)
                SELECT NEW.id, strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE NOT EXISTS (
                    SELECT 1 FROM work_stats WHERE work_id = NEW.id
                );
                UPDATE catalog_state
                SET revision = revision + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
            "DROP TRIGGER IF EXISTS work_stats_asset_after_insert",
            r#"
            CREATE TRIGGER work_stats_asset_after_insert
            AFTER INSERT ON assets
            BEGIN
                INSERT INTO work_stats (work_id)
                SELECT NEW.work_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM work_stats WHERE work_id = NEW.work_id
                );
                UPDATE work_stats
                SET asset_count = asset_count + 1,
                    image_count = image_count + CASE WHEN NEW.mime LIKE 'image/%' THEN 1 ELSE 0 END,
                    track_count = track_count + CASE
                        WHEN NEW.role = 'track' OR NEW.mime LIKE 'audio/%' THEN 1 ELSE 0 END,
                    page_count = page_count + CASE
                        WHEN NEW.role = 'page' THEN 1
                        WHEN NEW.role = 'archive' THEN CAST(
                            COALESCE(json_extract(NEW.meta_json, '$.page_count'), 0) AS INTEGER
                        )
                        ELSE 0
                    END
                WHERE work_id = NEW.work_id;
            END
            "#,
            "DROP TRIGGER IF EXISTS work_stats_asset_after_update",
            r#"
            CREATE TRIGGER work_stats_asset_after_update
            AFTER UPDATE OF work_id, mime, role, meta_json ON assets
            BEGIN
                UPDATE work_stats
                SET asset_count = MAX(0, asset_count - 1),
                    image_count = MAX(0, image_count - CASE
                        WHEN OLD.mime LIKE 'image/%' THEN 1 ELSE 0 END),
                    track_count = MAX(0, track_count - CASE
                        WHEN OLD.role = 'track' OR OLD.mime LIKE 'audio/%' THEN 1 ELSE 0 END),
                    page_count = MAX(0, page_count - CASE
                        WHEN OLD.role = 'page' THEN 1
                        WHEN OLD.role = 'archive' THEN CAST(
                            COALESCE(json_extract(OLD.meta_json, '$.page_count'), 0) AS INTEGER
                        )
                        ELSE 0
                    END)
                WHERE work_id = OLD.work_id;
                INSERT INTO work_stats (work_id)
                SELECT NEW.work_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM work_stats WHERE work_id = NEW.work_id
                );
                UPDATE work_stats
                SET asset_count = asset_count + 1,
                    image_count = image_count + CASE WHEN NEW.mime LIKE 'image/%' THEN 1 ELSE 0 END,
                    track_count = track_count + CASE
                        WHEN NEW.role = 'track' OR NEW.mime LIKE 'audio/%' THEN 1 ELSE 0 END,
                    page_count = page_count + CASE
                        WHEN NEW.role = 'page' THEN 1
                        WHEN NEW.role = 'archive' THEN CAST(
                            COALESCE(json_extract(NEW.meta_json, '$.page_count'), 0) AS INTEGER
                        )
                        ELSE 0
                    END
                WHERE work_id = NEW.work_id;
            END
            "#,
            "DROP TRIGGER IF EXISTS work_stats_tag_after_insert",
            r#"
            CREATE TRIGGER work_stats_tag_after_insert
            AFTER INSERT ON work_tags
            BEGIN
                INSERT INTO work_stats (work_id)
                SELECT NEW.work_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM work_stats WHERE work_id = NEW.work_id
                );
                UPDATE work_stats
                SET tag_count = tag_count + 1
                WHERE work_id = NEW.work_id;
            END
            "#,
        ],
    },
    Migration {
        version: 5,
        name: "catalog-v2-collections-and-keyset-assets",
        statements: &[
            "ALTER TABLE work_stats ADD COLUMN collection_key TEXT",
            "ALTER TABLE work_stats ADD COLUMN collection_title TEXT",
            r#"
            CREATE INDEX idx_work_stats_collection
            ON work_stats(collection_key, work_id)
            "#,
            r#"
            CREATE INDEX idx_history_opened_work
            ON reading_history(last_opened_at DESC, work_id DESC)
            "#,
            "UPDATE work_stats SET computed_at = NULL, collection_key = NULL, collection_title = NULL",
            "DROP TRIGGER IF EXISTS catalog_work_after_insert",
            r#"
            CREATE TRIGGER catalog_work_after_insert
            AFTER INSERT ON works
            BEGIN
                INSERT INTO work_stats (work_id, computed_at)
                SELECT NEW.id, NULL
                WHERE NOT EXISTS (
                    SELECT 1 FROM work_stats WHERE work_id = NEW.id
                );
                UPDATE work_stats
                SET computed_at = NULL
                WHERE work_id = NEW.id;
                UPDATE catalog_state
                SET revision = revision + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
            "DROP TRIGGER IF EXISTS catalog_work_after_update",
            r#"
            CREATE TRIGGER catalog_work_after_update
            AFTER UPDATE OF kind, title, subtitle, category, rating, progress,
                            source_path, cover_asset_id, meta_json, updated_at ON works
            BEGIN
                UPDATE work_stats
                SET computed_at = CASE
                    WHEN OLD.kind IS NOT NEW.kind
                      OR OLD.title IS NOT NEW.title
                      OR OLD.source_path IS NOT NEW.source_path
                      OR OLD.meta_json IS NOT NEW.meta_json
                    THEN NULL ELSE computed_at END
                WHERE work_id = NEW.id;
                UPDATE catalog_state
                SET revision = revision + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
            "DROP TRIGGER IF EXISTS work_stats_tag_after_insert",
            r#"
            CREATE TRIGGER work_stats_tag_after_insert
            AFTER INSERT ON work_tags
            BEGIN
                INSERT INTO work_stats (work_id)
                SELECT NEW.work_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM work_stats WHERE work_id = NEW.work_id
                );
                UPDATE work_stats
                SET tag_count = tag_count + 1,
                    computed_at = CASE
                        WHEN EXISTS (
                            SELECT 1 FROM tags
                            WHERE id = NEW.tag_id AND namespace = 'artist'
                        ) THEN NULL ELSE computed_at END
                WHERE work_id = NEW.work_id;
            END
            "#,
            "DROP TRIGGER IF EXISTS work_stats_tag_after_delete",
            r#"
            CREATE TRIGGER work_stats_tag_after_delete
            AFTER DELETE ON work_tags
            BEGIN
                UPDATE work_stats
                SET tag_count = MAX(0, tag_count - 1),
                    computed_at = CASE
                        WHEN EXISTS (
                            SELECT 1 FROM tags
                            WHERE id = OLD.tag_id AND namespace = 'artist'
                        ) THEN NULL ELSE computed_at END
                WHERE work_id = OLD.work_id;
            END
            "#,
            r#"
            CREATE TRIGGER catalog_collection_tag_after_update
            AFTER UPDATE OF namespace, key ON tags
            WHEN OLD.namespace = 'artist' OR NEW.namespace = 'artist'
            BEGIN
                UPDATE work_stats
                SET computed_at = NULL
                WHERE work_id IN (
                    SELECT work_id FROM work_tags WHERE tag_id = NEW.id
                );
            END
            "#,
        ],
    },
    Migration {
        version: 6,
        name: "shadow-file-inventory",
        statements: &[
            r#"
            CREATE TABLE library_roots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                provider TEXT NOT NULL DEFAULT 'local',
                root TEXT NOT NULL,
                scan_depth INTEGER,
                device_class TEXT NOT NULL DEFAULT 'hdd',
                enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0, 1)),
                generation INTEGER NOT NULL DEFAULT 0 CHECK(generation >= 0),
                completed_generation INTEGER NOT NULL DEFAULT 0 CHECK(completed_generation >= 0),
                status TEXT NOT NULL DEFAULT 'idle' CHECK(status IN (
                    'idle', 'scanning', 'needs_reconcile', 'disabled'
                )),
                active_token TEXT,
                scan_started_at TEXT,
                last_reconcile_at TEXT,
                last_event_seq INTEGER,
                present_files INTEGER NOT NULL DEFAULT 0 CHECK(present_files >= 0),
                missing_files INTEGER NOT NULL DEFAULT 0 CHECK(missing_files >= 0),
                last_discovered INTEGER NOT NULL DEFAULT 0 CHECK(last_discovered >= 0),
                last_inserted INTEGER NOT NULL DEFAULT 0 CHECK(last_inserted >= 0),
                last_changed INTEGER NOT NULL DEFAULT 0 CHECK(last_changed >= 0),
                last_missing INTEGER NOT NULL DEFAULT 0 CHECK(last_missing >= 0),
                last_error TEXT,
                UNIQUE(kind, provider, root)
            )
            "#,
            r#"
            CREATE TABLE file_inventory (
                root_id INTEGER NOT NULL REFERENCES library_roots(id) ON DELETE CASCADE,
                relative_path TEXT NOT NULL,
                parent_key TEXT NOT NULL,
                media_class TEXT,
                size INTEGER NOT NULL CHECK(size >= 0),
                mtime_ns INTEGER NOT NULL CHECK(mtime_ns >= 0),
                file_id TEXT,
                fast_fingerprint TEXT NOT NULL,
                work_key TEXT,
                seen_generation INTEGER NOT NULL CHECK(seen_generation >= 0),
                seen_event_seq INTEGER,
                status TEXT NOT NULL DEFAULT 'present' CHECK(status IN (
                    'present', 'missing', 'error'
                )),
                last_error TEXT,
                PRIMARY KEY(root_id, relative_path)
            ) WITHOUT ROWID
            "#,
            r#"
            CREATE INDEX idx_inventory_root_parent
            ON file_inventory(root_id, parent_key)
            "#,
            r#"
            CREATE INDEX idx_inventory_work_key
            ON file_inventory(root_id, work_key)
            "#,
            r#"
            CREATE INDEX idx_inventory_root_generation
            ON file_inventory(root_id, seen_generation, status)
            "#,
            r#"
            CREATE TABLE scan_events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                root_id INTEGER NOT NULL REFERENCES library_roots(id) ON DELETE CASCADE,
                relative_path TEXT NOT NULL,
                event_kind TEXT NOT NULL,
                work_key TEXT,
                observed_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN (
                    'pending', 'processing', 'done', 'failed'
                )),
                attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
                last_error TEXT
            )
            "#,
            r#"
            CREATE INDEX idx_scan_events_pending
            ON scan_events(status, root_id, seq)
            "#,
            r#"
            CREATE UNIQUE INDEX idx_scan_events_pending_work
            ON scan_events(root_id, work_key)
            WHERE status = 'pending' AND work_key IS NOT NULL
            "#,
        ],
    },
    Migration {
        version: 7,
        name: "catalog-tag-kind-counts",
        statements: &[
            r#"
            CREATE TABLE tag_kind_counts (
                kind TEXT NOT NULL,
                tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
                work_count INTEGER NOT NULL CHECK(work_count >= 0),
                catalog_revision INTEGER NOT NULL,
                PRIMARY KEY(kind, tag_id)
            ) WITHOUT ROWID
            "#,
            r#"
            CREATE INDEX idx_tag_kind_counts_order
            ON tag_kind_counts(kind, work_count DESC, tag_id)
            "#,
            r#"
            CREATE TABLE tag_kind_count_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                ready INTEGER NOT NULL DEFAULT 0 CHECK(ready IN (0, 1)),
                catalog_revision INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                last_error TEXT
            )
            "#,
            r#"
            INSERT INTO tag_kind_count_state (singleton, ready, catalog_revision)
            VALUES (1, 0, 0)
            "#,
            r#"
            CREATE TRIGGER catalog_tag_kind_after_insert
            AFTER INSERT ON work_tags
            WHEN (SELECT ready FROM tag_kind_count_state WHERE singleton = 1) = 1
            BEGIN
                UPDATE catalog_state
                SET revision = revision + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
                UPDATE tag_kind_count_state
                SET ready = 0,
                    catalog_revision = (
                        SELECT revision FROM catalog_state WHERE singleton = 1
                    ),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
            r#"
            CREATE TRIGGER catalog_tag_kind_after_delete
            AFTER DELETE ON work_tags
            WHEN (SELECT ready FROM tag_kind_count_state WHERE singleton = 1) = 1
            BEGIN
                UPDATE catalog_state
                SET revision = revision + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
                UPDATE tag_kind_count_state
                SET ready = 0,
                    catalog_revision = (
                        SELECT revision FROM catalog_state WHERE singleton = 1
                    ),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
            r#"
            CREATE TRIGGER catalog_tag_kind_after_update
            AFTER UPDATE OF work_id, tag_id ON work_tags
            WHEN (OLD.work_id IS NOT NEW.work_id OR OLD.tag_id IS NOT NEW.tag_id)
             AND (SELECT ready FROM tag_kind_count_state WHERE singleton = 1) = 1
            BEGIN
                UPDATE catalog_state
                SET revision = revision + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
                UPDATE tag_kind_count_state
                SET ready = 0,
                    catalog_revision = (
                        SELECT revision FROM catalog_state WHERE singleton = 1
                    ),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
            r#"
            CREATE TRIGGER catalog_tag_kind_after_work_kind_update
            AFTER UPDATE OF kind ON works
            WHEN OLD.kind IS NOT NEW.kind
             AND (SELECT ready FROM tag_kind_count_state WHERE singleton = 1) = 1
            BEGIN
                UPDATE tag_kind_count_state
                SET ready = 0,
                    catalog_revision = (
                        SELECT revision FROM catalog_state WHERE singleton = 1
                    ),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
        ],
    },
    Migration {
        version: 8,
        name: "typed-catalog-writer-and-search-outbox",
        statements: &[
            r#"
            CREATE TABLE catalog_kind_ownership (
                kind TEXT PRIMARY KEY,
                authoritative_writer TEXT NOT NULL DEFAULT 'legacy' CHECK(
                    authoritative_writer IN ('legacy', 'catalog-v2')
                ),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )
            "#,
            r#"
            INSERT INTO catalog_kind_ownership (kind, authoritative_writer)
            VALUES
                ('novel', 'legacy'),
                ('comic', 'legacy'),
                ('coser-picture', 'legacy'),
                ('audio', 'legacy'),
                ('gallery', 'legacy')
            "#,
            r#"
            CREATE TABLE catalog_work_sources (
                kind TEXT NOT NULL,
                root_id INTEGER NOT NULL REFERENCES library_roots(id) ON DELETE CASCADE,
                work_key TEXT NOT NULL,
                provider TEXT NOT NULL,
                work_id INTEGER NOT NULL UNIQUE REFERENCES works(id) ON DELETE CASCADE,
                seen_generation INTEGER NOT NULL CHECK(seen_generation >= 0),
                seen_token TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                PRIMARY KEY(kind, root_id, work_key)
            ) WITHOUT ROWID
            "#,
            r#"
            CREATE INDEX idx_catalog_work_sources_root_generation
            ON catalog_work_sources(root_id, seen_generation, work_key)
            "#,
            r#"
            CREATE TABLE external_id_sources (
                external_id_id INTEGER NOT NULL REFERENCES external_ids(id) ON DELETE CASCADE,
                work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
                owner TEXT NOT NULL,
                seen_token TEXT,
                PRIMARY KEY(external_id_id, owner)
            ) WITHOUT ROWID
            "#,
            r#"
            CREATE INDEX idx_external_id_sources_work
            ON external_id_sources(work_id, owner, seen_token)
            "#,
            r#"
            INSERT INTO external_id_sources (external_id_id, work_id, owner, seen_token)
            SELECT
                external.id,
                external.work_id,
                CASE
                    WHEN work.kind = 'audio'
                     AND scanner.work_id IS NOT NULL
                     AND external.source IN ('asmr', 'dlsite')
                    THEN 'scanner'
                    ELSE 'external'
                END,
                CASE
                    WHEN work.kind = 'audio'
                     AND scanner.work_id IS NOT NULL
                     AND external.source IN ('asmr', 'dlsite')
                    THEN scanner.seen_token
                    ELSE NULL
                END
            FROM external_ids AS external
            JOIN works AS work ON work.id = external.work_id
            LEFT JOIN scanner_works AS scanner ON scanner.work_id = external.work_id
            "#,
            r#"
            CREATE TABLE search_outbox (
                work_id INTEGER PRIMARY KEY,
                operation TEXT NOT NULL CHECK(operation IN ('upsert', 'delete')),
                catalog_revision INTEGER NOT NULL CHECK(catalog_revision >= 0),
                payload_version INTEGER NOT NULL DEFAULT 1 CHECK(payload_version > 0),
                attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
                available_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                claimed_by TEXT,
                claimed_at TEXT,
                committed_at TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )
            "#,
            r#"
            CREATE INDEX idx_search_outbox_available
            ON search_outbox(committed_at, available_at, catalog_revision, work_id)
            "#,
            r#"
            CREATE INDEX idx_search_outbox_claimed
            ON search_outbox(claimed_at, claimed_by)
            WHERE claimed_at IS NOT NULL
            "#,
        ],
    },
    Migration {
        version: 9,
        name: "shadow-search-index-state",
        statements: &[
            r#"
            CREATE TABLE search_index_state (
                index_name TEXT PRIMARY KEY,
                schema_version INTEGER NOT NULL CHECK(schema_version > 0),
                baseline_revision INTEGER NOT NULL DEFAULT 0 CHECK(baseline_revision >= 0),
                applied_revision INTEGER NOT NULL DEFAULT 0 CHECK(applied_revision >= 0),
                indexed_documents INTEGER NOT NULL DEFAULT 0 CHECK(indexed_documents >= 0),
                ready INTEGER NOT NULL DEFAULT 0 CHECK(ready IN (0, 1)),
                status TEXT NOT NULL DEFAULT 'empty' CHECK(status IN (
                    'empty', 'building', 'shadow', 'catching-up', 'ready', 'degraded'
                )),
                last_error TEXT,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            ) WITHOUT ROWID
            "#,
            r#"
            INSERT INTO search_index_state (
                index_name, schema_version, baseline_revision, applied_revision,
                indexed_documents, ready, status
            )
            VALUES ('shadow-v3', 1, 0, 0, 0, 0, 'empty')
            "#,
        ],
    },
    Migration {
        version: 10,
        name: "derivative-eviction-lru",
        statements: &[
            r#"
            CREATE INDEX idx_derivatives_eviction_lru
            ON derivatives(status, last_access_at, id)
            WHERE bytes > 0
            "#,
            r#"
            DROP INDEX idx_derivatives_lru
            "#,
        ],
    },
    Migration {
        version: 11,
        name: "search-reconciliation-state",
        statements: &[
            r#"
            CREATE TABLE search_reconciliation_state (
                index_name TEXT PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'unknown' CHECK(status IN (
                    'unknown', 'stale', 'passed', 'failed'
                )),
                catalog_revision INTEGER NOT NULL DEFAULT 0 CHECK(catalog_revision >= 0),
                catalog_revision_after INTEGER NOT NULL DEFAULT 0 CHECK(catalog_revision_after >= 0),
                applied_revision INTEGER NOT NULL DEFAULT 0 CHECK(applied_revision >= 0),
                applied_revision_after INTEGER NOT NULL DEFAULT 0 CHECK(applied_revision_after >= 0),
                sqlite_work_count INTEGER NOT NULL DEFAULT 0 CHECK(sqlite_work_count >= 0),
                index_document_count INTEGER NOT NULL DEFAULT 0 CHECK(index_document_count >= 0),
                index_unique_work_count INTEGER NOT NULL DEFAULT 0 CHECK(index_unique_work_count >= 0),
                missing_work_ids INTEGER NOT NULL DEFAULT 0 CHECK(missing_work_ids >= 0),
                unexpected_work_ids INTEGER NOT NULL DEFAULT 0 CHECK(unexpected_work_ids >= 0),
                duplicate_documents INTEGER NOT NULL DEFAULT 0 CHECK(duplicate_documents >= 0),
                invalid_documents INTEGER NOT NULL DEFAULT 0 CHECK(invalid_documents >= 0),
                sqlite_ids_sha256 TEXT,
                index_ids_sha256 TEXT,
                consecutive_passes INTEGER NOT NULL DEFAULT 0 CHECK(consecutive_passes >= 0),
                passed_since TEXT,
                last_checked_at TEXT,
                took_millis INTEGER CHECK(took_millis IS NULL OR took_millis >= 0),
                FOREIGN KEY(index_name) REFERENCES search_index_state(index_name) ON DELETE CASCADE
            ) WITHOUT ROWID
            "#,
            r#"
            INSERT INTO search_reconciliation_state (index_name)
            VALUES ('shadow-v3')
            "#,
        ],
    },
    Migration {
        version: 12,
        name: "search-reader-cutover-arm",
        statements: &[r#"
            ALTER TABLE search_reconciliation_state
            ADD COLUMN cutover_armed INTEGER NOT NULL DEFAULT 0
                CHECK(cutover_armed IN (0, 1))
            "#],
    },
    Migration {
        version: 13,
        name: "work-tombstones-and-history-retention",
        statements: &[
            r#"
            ALTER TABLE works ADD COLUMN deleted_at TEXT
            "#,
            r#"
            ALTER TABLE works ADD COLUMN deleted_reason TEXT
            "#,
            r#"
            CREATE INDEX idx_works_active_kind_updated
            ON works(kind, updated_at DESC, id DESC)
            WHERE deleted_at IS NULL
            "#,
            r#"
            CREATE INDEX idx_works_deleted_at
            ON works(deleted_at, id)
            WHERE deleted_at IS NOT NULL
            "#,
            "DROP TRIGGER IF EXISTS catalog_work_after_update",
            r#"
            CREATE TRIGGER catalog_work_after_update
            AFTER UPDATE OF kind, title, subtitle, category, rating, progress,
                            source_path, cover_asset_id, meta_json, updated_at,
                            deleted_at, deleted_reason ON works
            BEGIN
                UPDATE work_stats
                SET computed_at = CASE
                    WHEN OLD.kind IS NOT NEW.kind
                      OR OLD.title IS NOT NEW.title
                      OR OLD.source_path IS NOT NEW.source_path
                      OR OLD.meta_json IS NOT NEW.meta_json
                      OR OLD.deleted_at IS NOT NEW.deleted_at
                    THEN NULL ELSE computed_at END
                WHERE work_id = NEW.id;
                UPDATE catalog_state
                SET revision = revision + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
        ],
    },
    Migration {
        version: 14,
        name: "novel-coordinator-checkpoints",
        statements: &[
            r#"
            CREATE TABLE novel_coordinator_state (
                root_id INTEGER PRIMARY KEY REFERENCES library_roots(id) ON DELETE CASCADE,
                generation INTEGER NOT NULL DEFAULT 0 CHECK(generation >= 0),
                phase TEXT NOT NULL DEFAULT 'idle' CHECK(phase IN (
                    'idle', 'processing', 'degraded', 'needs_reconcile'
                )),
                checkpoint_seq INTEGER NOT NULL DEFAULT 0 CHECK(checkpoint_seq >= 0),
                pending_keys INTEGER NOT NULL DEFAULT 0 CHECK(pending_keys >= 0),
                failed_keys INTEGER NOT NULL DEFAULT 0 CHECK(failed_keys >= 0),
                last_error TEXT,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            ) WITHOUT ROWID
            "#,
            r#"
            CREATE INDEX idx_novel_coordinator_state_phase
            ON novel_coordinator_state(phase, updated_at, root_id)
            "#,
        ],
    },
    Migration {
        version: 15,
        name: "catalog-reconciliation-evidence",
        statements: &[
            r#"
            CREATE TABLE catalog_reconciliation_state (
                kind TEXT PRIMARY KEY REFERENCES catalog_kind_ownership(kind) ON DELETE CASCADE,
                status TEXT NOT NULL DEFAULT 'unknown' CHECK(status IN (
                    'unknown', 'running', 'stale', 'passed', 'failed', 'error'
                )),
                catalog_revision INTEGER NOT NULL DEFAULT 0 CHECK(catalog_revision >= 0),
                catalog_revision_after INTEGER NOT NULL DEFAULT 0 CHECK(catalog_revision_after >= 0),
                root_generation_sha256 TEXT,
                root_generation_after_sha256 TEXT,
                root_count INTEGER NOT NULL DEFAULT 0 CHECK(root_count >= 0),
                ready_root_count INTEGER NOT NULL DEFAULT 0 CHECK(ready_root_count >= 0),
                expected_works INTEGER NOT NULL DEFAULT 0 CHECK(expected_works >= 0),
                matched_works INTEGER NOT NULL DEFAULT 0 CHECK(matched_works >= 0),
                missing_works INTEGER NOT NULL DEFAULT 0 CHECK(missing_works >= 0),
                unexpected_works INTEGER NOT NULL DEFAULT 0 CHECK(unexpected_works >= 0),
                mismatch_works INTEGER NOT NULL DEFAULT 0 CHECK(mismatch_works >= 0),
                error_works INTEGER NOT NULL DEFAULT 0 CHECK(error_works >= 0),
                diffs_recorded INTEGER NOT NULL DEFAULT 0 CHECK(diffs_recorded >= 0),
                diffs_truncated INTEGER NOT NULL DEFAULT 0 CHECK(diffs_truncated IN (0, 1)),
                consecutive_passes INTEGER NOT NULL DEFAULT 0 CHECK(consecutive_passes >= 0),
                passed_since TEXT,
                started_at TEXT,
                last_checked_at TEXT,
                took_millis INTEGER CHECK(took_millis IS NULL OR took_millis >= 0),
                last_error TEXT
            ) WITHOUT ROWID
            "#,
            r#"
            INSERT INTO catalog_reconciliation_state (kind)
            SELECT kind FROM catalog_kind_ownership
            "#,
            r#"
            CREATE TABLE catalog_reconciliation_diffs (
                kind TEXT NOT NULL REFERENCES catalog_reconciliation_state(kind) ON DELETE CASCADE,
                ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
                work_key TEXT NOT NULL,
                difference_kind TEXT NOT NULL CHECK(difference_kind IN (
                    'missing', 'unexpected', 'mismatch', 'error'
                )),
                details_json TEXT NOT NULL DEFAULT '{}',
                checked_at TEXT NOT NULL,
                PRIMARY KEY(kind, ordinal)
            ) WITHOUT ROWID
            "#,
            r#"
            CREATE INDEX idx_catalog_reconciliation_diffs_kind
            ON catalog_reconciliation_diffs(kind, difference_kind, ordinal)
            "#,
            r#"
            CREATE UNIQUE INDEX idx_jobs_one_active_catalog_reconciliation
            ON jobs(job_type)
            WHERE status IN ('queued', 'running')
              AND job_type = 'reconcile-catalog-novel'
            "#,
        ],
    },
    Migration {
        version: 16,
        name: "comic-catalog-reconciliation-job",
        statements: &[r#"
            CREATE UNIQUE INDEX idx_jobs_one_active_catalog_reconciliation_comic
            ON jobs(job_type)
            WHERE status IN ('queued', 'running')
              AND job_type = 'reconcile-catalog-comic'
            "#],
    },
    Migration {
        version: 17,
        name: "activity-revision-separation",
        statements: &[
            r#"
            CREATE TABLE activity_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                revision INTEGER NOT NULL DEFAULT 0 CHECK(revision >= 0),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )
            "#,
            r#"
            INSERT INTO activity_state (singleton, revision) VALUES (1, 0)
            "#,
            r#"
            CREATE TRIGGER reading_history_activity_after_insert
            AFTER INSERT ON reading_history
            BEGIN
                UPDATE activity_state
                SET revision = revision + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
            r#"
            CREATE TRIGGER reading_history_activity_after_update
            AFTER UPDATE ON reading_history
            BEGIN
                UPDATE activity_state
                SET revision = revision + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
            r#"
            CREATE TRIGGER reading_history_activity_after_delete
            AFTER DELETE ON reading_history
            BEGIN
                UPDATE activity_state
                SET revision = revision + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
            "DROP TRIGGER IF EXISTS catalog_work_after_update",
            r#"
            CREATE TRIGGER catalog_work_after_update
            AFTER UPDATE OF kind, title, subtitle, category, rating,
                            source_path, cover_asset_id, meta_json, updated_at,
                            deleted_at, deleted_reason ON works
            BEGIN
                UPDATE work_stats
                SET computed_at = CASE
                    WHEN OLD.kind IS NOT NEW.kind
                      OR OLD.title IS NOT NEW.title
                      OR OLD.source_path IS NOT NEW.source_path
                      OR OLD.meta_json IS NOT NEW.meta_json
                      OR OLD.deleted_at IS NOT NEW.deleted_at
                    THEN NULL ELSE computed_at END
                WHERE work_id = NEW.id;
                UPDATE catalog_state
                SET revision = revision + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
        ],
    },
    Migration {
        version: 18,
        name: "coser-picture-catalog-reconciliation-job",
        statements: &[r#"
            CREATE UNIQUE INDEX idx_jobs_one_active_catalog_reconciliation_coser_picture
            ON jobs(job_type)
            WHERE status IN ('queued', 'running')
              AND job_type = 'reconcile-catalog-coser-picture'
            "#],
    },
    Migration {
        version: 19,
        name: "audio-gallery-catalog-reconciliation-jobs",
        statements: &[
            r#"
            CREATE UNIQUE INDEX idx_jobs_one_active_catalog_reconciliation_audio
            ON jobs(job_type)
            WHERE status IN ('queued', 'running')
              AND job_type = 'reconcile-catalog-audio'
            "#,
            r#"
            CREATE UNIQUE INDEX idx_jobs_one_active_catalog_reconciliation_gallery
            ON jobs(job_type)
            WHERE status IN ('queued', 'running')
              AND job_type = 'reconcile-catalog-gallery'
            "#,
        ],
    },
    Migration {
        version: 20,
        name: "audio-grouping-mode",
        statements: &[r#"
            ALTER TABLE library_roots
            ADD COLUMN audio_grouping TEXT NOT NULL DEFAULT 'auto'
                CHECK(audio_grouping IN ('rj', 'folder', 'auto'))
            "#],
    },
    Migration {
        version: 21,
        name: "inventory-present-work-cover-index",
        statements: &[r#"
            CREATE INDEX idx_inventory_present_work_cover
            ON file_inventory(
                root_id,
                work_key,
                relative_path,
                size,
                fast_fingerprint,
                file_id
            )
            WHERE status = 'present' AND work_key IS NOT NULL
            "#],
    },
    Migration {
        version: 22,
        name: "bounded-archive-manifest-cache",
        statements: &[
            r#"
            CREATE TABLE archive_manifest_cache (
                cache_key TEXT PRIMARY KEY,
                source_size INTEGER NOT NULL CHECK(source_size >= 0),
                source_modified_nanos TEXT,
                pages_json TEXT NOT NULL,
                bytes INTEGER NOT NULL CHECK(bytes >= 0),
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )
            "#,
            r#"
            CREATE INDEX idx_archive_manifest_cache_eviction
            ON archive_manifest_cache(created_at, cache_key)
            "#,
            r#"
            CREATE TABLE archive_manifest_cache_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                resident_bytes INTEGER NOT NULL DEFAULT 0 CHECK(resident_bytes >= 0),
                resident_entries INTEGER NOT NULL DEFAULT 0 CHECK(resident_entries >= 0),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )
            "#,
            r#"
            INSERT INTO archive_manifest_cache_state (singleton, resident_bytes, resident_entries)
            VALUES (1, 0, 0)
            "#,
            r#"
            CREATE TRIGGER archive_manifest_cache_after_insert
            AFTER INSERT ON archive_manifest_cache
            BEGIN
                UPDATE archive_manifest_cache_state
                SET resident_bytes = resident_bytes + NEW.bytes,
                    resident_entries = resident_entries + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
            r#"
            CREATE TRIGGER archive_manifest_cache_after_update
            AFTER UPDATE OF bytes ON archive_manifest_cache
            WHEN OLD.bytes != NEW.bytes
            BEGIN
                UPDATE archive_manifest_cache_state
                SET resident_bytes = MAX(0, resident_bytes - OLD.bytes + NEW.bytes),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
            r#"
            CREATE TRIGGER archive_manifest_cache_after_delete
            AFTER DELETE ON archive_manifest_cache
            BEGIN
                UPDATE archive_manifest_cache_state
                SET resident_bytes = MAX(0, resident_bytes - OLD.bytes),
                    resident_entries = MAX(0, resident_entries - 1),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE singleton = 1;
            END
            "#,
        ],
    },
    Migration {
        version: 23,
        name: "legacy-search-index-revision-fence",
        statements: &[r#"
            INSERT OR IGNORE INTO search_index_state (
                index_name, schema_version, baseline_revision, applied_revision,
                indexed_documents, ready, status
            )
            VALUES ('production-v2', 1, 0, 0, 0, 0, 'empty')
            "#],
    },
    Migration {
        version: 24,
        name: "search-source-revision-fence",
        statements: &[
            r#"
            CREATE TABLE search_source_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                revision INTEGER NOT NULL DEFAULT 0 CHECK(revision >= 0),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            ) WITHOUT ROWID
            "#,
            r#"
            INSERT INTO search_source_state (singleton, revision)
            SELECT 1, revision FROM catalog_state WHERE singleton = 1
            "#,
            r#"
            ALTER TABLE search_outbox
            ADD COLUMN search_revision INTEGER NOT NULL DEFAULT 0
                CHECK(search_revision >= 0)
            "#,
            r#"
            UPDATE search_outbox
            SET search_revision = catalog_revision
            WHERE search_revision = 0
            "#,
            r#"
            UPDATE search_source_state
            SET revision = MAX(
                revision,
                COALESCE((SELECT MAX(search_revision) FROM search_outbox), 0)
            )
            WHERE singleton = 1
            "#,
            r#"
            UPDATE search_source_state
            SET revision = revision + 1,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE singleton = 1
              AND EXISTS (
                  SELECT 1 FROM search_outbox WHERE committed_at IS NULL
              )
            "#,
            r#"
            UPDATE search_outbox
            SET search_revision = (
                SELECT revision FROM search_source_state WHERE singleton = 1
            )
            WHERE committed_at IS NULL
            "#,
            r#"
            ALTER TABLE search_index_state
            ADD COLUMN baseline_search_revision INTEGER NOT NULL DEFAULT 0
                CHECK(baseline_search_revision >= 0)
            "#,
            r#"
            ALTER TABLE search_index_state
            ADD COLUMN applied_search_revision INTEGER NOT NULL DEFAULT 0
                CHECK(applied_search_revision >= 0)
            "#,
            r#"
            UPDATE search_index_state
            SET baseline_search_revision = baseline_revision,
                applied_search_revision = applied_revision
            "#,
            r#"
            ALTER TABLE search_reconciliation_state
            ADD COLUMN search_revision INTEGER NOT NULL DEFAULT 0
                CHECK(search_revision >= 0)
            "#,
            r#"
            ALTER TABLE search_reconciliation_state
            ADD COLUMN search_revision_after INTEGER NOT NULL DEFAULT 0
                CHECK(search_revision_after >= 0)
            "#,
            r#"
            ALTER TABLE search_reconciliation_state
            ADD COLUMN applied_search_revision INTEGER NOT NULL DEFAULT 0
                CHECK(applied_search_revision >= 0)
            "#,
            r#"
            ALTER TABLE search_reconciliation_state
            ADD COLUMN applied_search_revision_after INTEGER NOT NULL DEFAULT 0
                CHECK(applied_search_revision_after >= 0)
            "#,
            r#"
            UPDATE search_reconciliation_state
            SET search_revision = catalog_revision,
                search_revision_after = catalog_revision_after,
                applied_search_revision = applied_revision,
                applied_search_revision_after = applied_revision_after
            "#,
        ],
    },
    Migration {
        version: 25,
        name: "strm-archive-entry-assets",
        statements: &[
            r#"
            CREATE TABLE archive_asset_entries (
                parent_asset_id INTEGER NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
                entry_key TEXT NOT NULL,
                derived_asset_id INTEGER NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
                entry_path TEXT NOT NULL,
                source_version TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                PRIMARY KEY(parent_asset_id, entry_key),
                UNIQUE(derived_asset_id)
            )
            "#,
            r#"
            CREATE INDEX idx_archive_asset_entries_derived
            ON archive_asset_entries(derived_asset_id)
            "#,
        ],
    },
    Migration {
        version: 26,
        name: "strm-archive-entry-parent-cleanup",
        statements: &[r#"
            CREATE TRIGGER archive_asset_entries_parent_cleanup
            BEFORE DELETE ON assets
            WHEN EXISTS (
                SELECT 1 FROM archive_asset_entries
                WHERE parent_asset_id = OLD.id
            )
            BEGIN
                DELETE FROM assets
                WHERE id IN (
                    SELECT derived_asset_id
                    FROM archive_asset_entries
                    WHERE parent_asset_id = OLD.id
                );
            END
            "#],
    },
];

pub async fn ensure_table(pool: &Pool<Sqlite>) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            checksum TEXT NOT NULL,
            applied_at TEXT NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn validate_compatible(pool: &Pool<Sqlite>) -> Result<()> {
    validate_definitions()?;
    let latest_known = MIGRATIONS
        .last()
        .map(|migration| migration.version)
        .unwrap_or(0);
    let latest_applied =
        sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(version), 0) FROM schema_migrations")
            .fetch_one(pool)
            .await?;
    if latest_applied > latest_known {
        return Err(AppError::Other(format!(
            "database schema version {latest_applied} is newer than this application supports ({latest_known})"
        )));
    }

    for migration in MIGRATIONS {
        let applied =
            sqlx::query("SELECT name, checksum FROM schema_migrations WHERE version = ?1")
                .bind(migration.version)
                .fetch_optional(pool)
                .await?;
        let Some(applied) = applied else {
            continue;
        };
        let applied_name: String = applied.get("name");
        let applied_checksum: String = applied.get("checksum");
        let expected_checksum = checksum(migration);
        if applied_name != migration.name || applied_checksum != expected_checksum {
            return Err(AppError::Other(format!(
                "schema migration {} checksum mismatch: database has {applied_name}/{applied_checksum}, application expects {}/{expected_checksum}",
                migration.version, migration.name
            )));
        }
    }
    Ok(())
}

pub async fn apply_pending(pool: &Pool<Sqlite>) -> Result<()> {
    validate_compatible(pool).await?;
    for migration in MIGRATIONS {
        let already_applied =
            sqlx::query_scalar::<_, i64>("SELECT 1 FROM schema_migrations WHERE version = ?1")
                .bind(migration.version)
                .fetch_optional(pool)
                .await?
                .is_some();
        if already_applied {
            continue;
        }

        let mut transaction = pool.begin().await?;
        for statement in migration.statements {
            sqlx::query(statement).execute(&mut *transaction).await?;
        }
        sqlx::query(
            r#"
            INSERT INTO schema_migrations (version, name, checksum, applied_at)
            VALUES (?1, ?2, ?3, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            "#,
        )
        .bind(migration.version)
        .bind(migration.name)
        .bind(checksum(migration))
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
    }
    Ok(())
}

pub async fn current_version(pool: &Pool<Sqlite>) -> Result<i64> {
    Ok(
        sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(version), 0) FROM schema_migrations")
            .fetch_one(pool)
            .await?,
    )
}

/// Read the schema version on an already-open connection.  Health and
/// diagnostic snapshots use this form so their several small facts share one
/// SQLite read snapshot instead of checking out a pool connection per fact.
pub async fn current_version_in(connection: &mut SqliteConnection) -> Result<i64> {
    Ok(
        sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(version), 0) FROM schema_migrations")
            .fetch_one(&mut *connection)
            .await?,
    )
}

fn validate_definitions() -> Result<()> {
    let mut previous = 0;
    for migration in MIGRATIONS {
        if migration.version <= previous {
            return Err(AppError::Other(
                "schema migrations must have strictly increasing positive versions".to_string(),
            ));
        }
        previous = migration.version;
    }
    Ok(())
}

fn checksum(migration: &Migration) -> String {
    let mut hasher = Sha256::new();
    hasher.update(migration.version.to_le_bytes());
    hasher.update([0]);
    hasher.update(migration.name.as_bytes());
    for statement in migration.statements {
        hasher.update([0]);
        hasher.update(statement.trim().as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    #[test]
    fn migration_versions_are_ordered_and_checksums_are_stable() {
        validate_definitions().unwrap();
        assert_eq!(MIGRATIONS[0].version, 1);
        assert_eq!(MIGRATIONS[1].version, 2);
        assert_eq!(MIGRATIONS[2].version, 3);
        assert_eq!(MIGRATIONS[3].version, 4);
        assert_eq!(MIGRATIONS[4].version, 5);
        assert_eq!(MIGRATIONS[5].version, 6);
        assert_eq!(MIGRATIONS[6].version, 7);
        assert_eq!(MIGRATIONS[7].version, 8);
        assert_eq!(MIGRATIONS[8].version, 9);
        assert_eq!(MIGRATIONS[9].version, 10);
        assert_eq!(MIGRATIONS[10].version, 11);
        assert_eq!(MIGRATIONS[11].version, 12);
        assert_eq!(MIGRATIONS[12].version, 13);
        assert_eq!(MIGRATIONS[13].version, 14);
        assert_eq!(MIGRATIONS[14].version, 15);
        assert_eq!(MIGRATIONS[15].version, 16);
        assert_eq!(MIGRATIONS[16].version, 17);
        assert_eq!(MIGRATIONS[17].version, 18);
        assert_eq!(MIGRATIONS[18].version, 19);
        assert_eq!(MIGRATIONS[19].version, 20);
        assert_eq!(MIGRATIONS[20].version, 21);
        assert_eq!(MIGRATIONS[21].version, 22);
        assert_eq!(MIGRATIONS[22].version, 23);
        assert_eq!(MIGRATIONS[23].version, 24);
        assert_eq!(MIGRATIONS[24].version, 25);
        assert_eq!(MIGRATIONS[25].version, 26);
        for migration in MIGRATIONS {
            assert_eq!(checksum(migration), checksum(migration));
            assert_eq!(checksum(migration).len(), 64);
        }
    }

    #[tokio::test]
    async fn older_binary_rejects_a_newer_schema_version() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        ensure_table(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO schema_migrations(version, name, checksum, applied_at) VALUES (999, 'future', 'future', 'now')",
        )
        .execute(&pool)
        .await
        .unwrap();
        let error = validate_compatible(&pool).await.unwrap_err().to_string();
        assert!(error.contains("newer than this application supports"));
    }

    #[tokio::test]
    async fn migration_checksum_drift_is_rejected() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        ensure_table(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO schema_migrations(version, name, checksum, applied_at) VALUES (1, 'legacy-schema-baseline', 'wrong', 'now')",
        )
        .execute(&pool)
        .await
        .unwrap();
        let error = validate_compatible(&pool).await.unwrap_err().to_string();
        assert!(error.contains("checksum mismatch"));
    }
}
