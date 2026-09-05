//! BitTorrent 种子索引库
//!
//! 存储 infohash、metadata、peer 统计等信息，支持全文搜索。
//!
//! 表结构：
//! - torrents: infohash 主键 + metadata + 统计
//! - torrent_files: 种子内文件列表
//! - torrent_peers: peer 统计快照
//! - torrent_trackers: tracker 列表

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Mutex;

/// 种子索引记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentIndex {
    /// infohash（hex 字符串）
    pub infohash: String,
    /// 种子名称
    pub name: String,
    /// 总大小（字节）
    pub total_length: i64,
    /// 分片大小（字节）
    pub piece_length: i64,
    /// 分片数量
    pub piece_count: i64,
    /// 文件数量
    pub file_count: i64,
    /// 是否为私有种子
    pub private: bool,
    /// 创建者
    pub created_by: Option<String>,
    /// 创建时间（Unix 时间戳）
    pub creation_date: Option<i64>,
    /// 评论
    pub comment: Option<String>,
    /// seeders 数量
    pub seeders: i64,
    /// leechers 数量
    pub leechers: i64,
    /// 完成下载次数
    pub completed: i64,
    /// 首次发现时间
    pub first_seen: i64,
    /// 最后更新时间
    pub last_updated: i64,
    /// 来源（dht / tracker / manual）
    pub source: String,
    /// 元数据是否已下载
    pub metadata_complete: bool,
}

/// 种子内文件记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentFile {
    pub infohash: String,
    pub file_index: i64,
    pub path: String,
    pub length: i64,
}

/// 索引库
pub struct TorrentIndexDb {
    conn: Mutex<Connection>,
}

impl TorrentIndexDb {
    /// 打开或创建索引库
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path).context("打开索引库失败")?;
        let db = TorrentIndexDb {
            conn: Mutex::new(conn),
        };
        db.init_schema()?;
        Ok(db)
    }

    /// 内存数据库（用于测试）
    pub fn open_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let db = TorrentIndexDb {
            conn: Mutex::new(conn),
        };
        db.init_schema()?;
        Ok(db)
    }

    /// 初始化表结构
    fn init_schema(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS torrents (
                infohash TEXT PRIMARY KEY,
                name TEXT NOT NULL DEFAULT '',
                total_length INTEGER NOT NULL DEFAULT 0,
                piece_length INTEGER NOT NULL DEFAULT 0,
                piece_count INTEGER NOT NULL DEFAULT 0,
                file_count INTEGER NOT NULL DEFAULT 0,
                private INTEGER NOT NULL DEFAULT 0,
                created_by TEXT,
                creation_date INTEGER,
                comment TEXT,
                seeders INTEGER NOT NULL DEFAULT 0,
                leechers INTEGER NOT NULL DEFAULT 0,
                completed INTEGER NOT NULL DEFAULT 0,
                first_seen INTEGER NOT NULL,
                last_updated INTEGER NOT NULL,
                source TEXT NOT NULL DEFAULT 'dht',
                metadata_complete INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS torrent_files (
                infohash TEXT NOT NULL,
                file_index INTEGER NOT NULL,
                path TEXT NOT NULL,
                length INTEGER NOT NULL,
                PRIMARY KEY (infohash, file_index),
                FOREIGN KEY (infohash) REFERENCES torrents(infohash) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS torrent_trackers (
                infohash TEXT NOT NULL,
                tracker TEXT NOT NULL,
                PRIMARY KEY (infohash, tracker),
                FOREIGN KEY (infohash) REFERENCES torrents(infohash) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_torrents_name ON torrents(name);
            CREATE INDEX IF NOT EXISTS idx_torrents_seeders ON torrents(seeders DESC);
            CREATE INDEX IF NOT EXISTS idx_torrents_last_updated ON torrents(last_updated DESC);
            CREATE INDEX IF NOT EXISTS idx_torrent_files_path ON torrent_files(path);

            -- FTS5 全文搜索索引（K4 优化）
            CREATE VIRTUAL TABLE IF NOT EXISTS torrents_fts USING fts5(
                infohash UNINDEXED,
                name,
                comment,
                created_by,
                content='torrents',
                content_rowid='rowid',
                tokenize='unicode61'
            );

            -- FTS 同步触发器
            CREATE TRIGGER IF NOT EXISTS torrents_ai AFTER INSERT ON torrents BEGIN
                INSERT INTO torrents_fts(rowid, infohash, name, comment, created_by)
                VALUES (new.rowid, new.infohash, new.name, new.comment, new.created_by);
            END;

            CREATE TRIGGER IF NOT EXISTS torrents_ad AFTER DELETE ON torrents BEGIN
                INSERT INTO torrents_fts(torrents_fts, rowid, infohash, name, comment, created_by)
                VALUES ('delete', old.rowid, old.infohash, old.name, old.comment, old.created_by);
            END;

            CREATE TRIGGER IF NOT EXISTS torrents_au AFTER UPDATE ON torrents BEGIN
                INSERT INTO torrents_fts(torrents_fts, rowid, infohash, name, comment, created_by)
                VALUES ('delete', old.rowid, old.infohash, old.name, old.comment, old.created_by);
                INSERT INTO torrents_fts(rowid, infohash, name, comment, created_by)
                VALUES (new.rowid, new.infohash, new.name, new.comment, new.created_by);
            END;
            "#,
        )
        .context("初始化表结构失败")?;
        Ok(())
    }

    /// 插入或更新种子索引
    pub fn upsert(&self, torrent: &TorrentIndex) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT INTO torrents (
                infohash, name, total_length, piece_length, piece_count,
                file_count, private, created_by, creation_date, comment,
                seeders, leechers, completed, first_seen, last_updated,
                source, metadata_complete
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
            ON CONFLICT(infohash) DO UPDATE SET
                name = excluded.name,
                total_length = excluded.total_length,
                piece_length = excluded.piece_length,
                piece_count = excluded.piece_count,
                file_count = excluded.file_count,
                private = excluded.private,
                created_by = excluded.created_by,
                creation_date = excluded.creation_date,
                comment = excluded.comment,
                seeders = MAX(seeders, excluded.seeders),
                leechers = MAX(leechers, excluded.leechers),
                completed = completed + excluded.completed,
                last_updated = excluded.last_updated,
                metadata_complete = CASE WHEN excluded.metadata_complete = 1 THEN 1 ELSE metadata_complete END
            "#,
            params![
                torrent.infohash,
                torrent.name,
                torrent.total_length,
                torrent.piece_length,
                torrent.piece_count,
                torrent.file_count,
                torrent.private as i64,
                torrent.created_by,
                torrent.creation_date,
                torrent.comment,
                torrent.seeders,
                torrent.leechers,
                torrent.completed,
                torrent.first_seen,
                torrent.last_updated,
                torrent.source,
                torrent.metadata_complete as i64,
            ],
        )
        .context("插入种子索引失败")?;
        Ok(())
    }

    /// 根据 infohash 查询
    pub fn get(&self, infohash: &str) -> Result<Option<TorrentIndex>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT * FROM torrents WHERE infohash = ?1")?;
        let mut rows = stmt.query(params![infohash])?;
        if let Some(row) = rows.next()? {
            Ok(Some(Self::row_to_torrent(row)?))
        } else {
            Ok(None)
        }
    }

    /// 全文搜索（按名称和文件路径）
    pub fn search(&self, query: &str, limit: i64, offset: i64) -> Result<Vec<TorrentIndex>> {
        let conn = self.conn.lock().unwrap();

        // 优先使用 FTS5 全文搜索（K4 优化）
        let fts_result = conn.prepare(
            r#"
            SELECT t.* FROM torrents t
            JOIN torrents_fts f ON f.rowid = t.rowid
            WHERE torrents_fts MATCH ?1
            ORDER BY bm25(torrents_fts), t.seeders DESC
            LIMIT ?2 OFFSET ?3
            "#,
        ).and_then(|mut stmt| {
            let rows = stmt.query_map(params![query, limit, offset], |row| {
                Self::row_to_torrent(row)
            })?;
            let mut results = vec![];
            for row in rows {
                results.push(row?);
            }
            Ok(results)
        });

        match fts_result {
            Ok(results) if !results.is_empty() => Ok(results),
            _ => {
                // FTS5 无结果或不可用，回退到 LIKE 查询（同时搜索文件路径）
                let pattern = format!("%{}%", query);
                let mut stmt = conn.prepare(
                    r#"
                    SELECT DISTINCT t.* FROM torrents t
                    LEFT JOIN torrent_files f ON f.infohash = t.infohash
                    WHERE t.name LIKE ?1 OR f.path LIKE ?1 OR t.comment LIKE ?1
                    ORDER BY t.seeders DESC, t.last_updated DESC
                    LIMIT ?2 OFFSET ?3
                    "#,
                )?;
                let rows = stmt.query_map(params![pattern, limit, offset], |row| {
                    Self::row_to_torrent(row)
                })?;
                let mut results = vec![];
                for row in rows {
                    results.push(row?);
                }
                Ok(results)
            }
        }
    }

    /// 获取热门种子（按 seeders 排序）
    pub fn get_top(&self, limit: i64) -> Result<Vec<TorrentIndex>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM torrents ORDER BY seeders DESC, last_updated DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], |row| Self::row_to_torrent(row))?;
        let mut results = vec![];
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// 获取最新种子（按 first_seen 排序）
    pub fn get_recent(&self, limit: i64) -> Result<Vec<TorrentIndex>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM torrents ORDER BY first_seen DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], |row| Self::row_to_torrent(row))?;
        let mut results = vec![];
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// 更新 peer 统计
    pub fn update_peer_stats(
        &self,
        infohash: &str,
        seeders: i64,
        leechers: i64,
        completed: i64,
    ) -> Result<()> {
        let now = current_timestamp();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            UPDATE torrents SET
                seeders = ?2,
                leechers = ?3,
                completed = completed + ?4,
                last_updated = ?5
            WHERE infohash = ?1
            "#,
            params![infohash, seeders, leechers, completed, now],
        )?;
        Ok(())
    }

    /// 插入文件列表
    pub fn insert_files(&self, infohash: &str, files: &[TorrentFile]) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        // 先删除旧文件
        conn.execute("DELETE FROM torrent_files WHERE infohash = ?1", params![infohash])?;
        for (idx, file) in files.iter().enumerate() {
            conn.execute(
                "INSERT INTO torrent_files (infohash, file_index, path, length) VALUES (?1, ?2, ?3, ?4)",
                params![infohash, idx as i64, file.path, file.length],
            )?;
        }
        Ok(())
    }

    /// 获取种子的文件列表
    pub fn get_files(&self, infohash: &str) -> Result<Vec<TorrentFile>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT infohash, file_index, path, length FROM torrent_files WHERE infohash = ?1 ORDER BY file_index",
        )?;
        let rows = stmt.query_map(params![infohash], |row| {
            Ok(TorrentFile {
                infohash: row.get(0)?,
                file_index: row.get(1)?,
                path: row.get(2)?,
                length: row.get(3)?,
            })
        })?;
        let mut results = vec![];
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// 删除种子
    pub fn delete(&self, infohash: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM torrents WHERE infohash = ?1", params![infohash])?;
        Ok(())
    }

    /// 统计信息
    pub fn stats(&self) -> Result<IndexStats> {
        let conn = self.conn.lock().unwrap();
        let total: i64 = conn.query_row("SELECT COUNT(*) FROM torrents", [], |r| r.get(0))?;
        let with_metadata: i64 = conn
            .query_row("SELECT COUNT(*) FROM torrents WHERE metadata_complete = 1", [], |r| r.get(0))?;
        let total_seeders: i64 = conn
            .query_row("SELECT COALESCE(SUM(seeders), 0) FROM torrents", [], |r| r.get(0))?;
        let total_size: i64 = conn
            .query_row("SELECT COALESCE(SUM(total_length), 0) FROM torrents", [], |r| r.get(0))?;
        Ok(IndexStats {
            total_torrents: total,
            with_metadata,
            total_seeders,
            total_size,
        })
    }

    /// 将数据库行转换为 TorrentIndex
    fn row_to_torrent(row: &rusqlite::Row) -> rusqlite::Result<TorrentIndex> {
        Ok(TorrentIndex {
            infohash: row.get(0)?,
            name: row.get(1)?,
            total_length: row.get(2)?,
            piece_length: row.get(3)?,
            piece_count: row.get(4)?,
            file_count: row.get(5)?,
            private: row.get::<_, i64>(6)? != 0,
            created_by: row.get(7)?,
            creation_date: row.get(8)?,
            comment: row.get(9)?,
            seeders: row.get(10)?,
            leechers: row.get(11)?,
            completed: row.get(12)?,
            first_seen: row.get(13)?,
            last_updated: row.get(14)?,
            source: row.get(15)?,
            metadata_complete: row.get::<_, i64>(16)? != 0,
        })
    }
}

/// 索引库统计
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    pub total_torrents: i64,
    pub with_metadata: i64,
    pub total_seeders: i64,
    pub total_size: i64,
}

/// 当前 Unix 时间戳（秒）
fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

impl TorrentIndex {
    /// 创建新的索引记录（最小信息）
    pub fn new_stub(infohash: &str, source: &str) -> Self {
        let now = current_timestamp();
        TorrentIndex {
            infohash: infohash.to_string(),
            name: String::new(),
            total_length: 0,
            piece_length: 0,
            piece_count: 0,
            file_count: 0,
            private: false,
            created_by: None,
            creation_date: None,
            comment: None,
            seeders: 0,
            leechers: 0,
            completed: 0,
            first_seen: now,
            last_updated: now,
            source: source.to_string(),
            metadata_complete: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_torrent(infohash: &str, name: &str) -> TorrentIndex {
        let now = current_timestamp();
        TorrentIndex {
            infohash: infohash.to_string(),
            name: name.to_string(),
            total_length: 1024 * 1024 * 100,
            piece_length: 262144,
            piece_count: 400,
            file_count: 1,
            private: false,
            created_by: Some("spde".to_string()),
            creation_date: Some(now),
            comment: None,
            seeders: 50,
            leechers: 10,
            completed: 100,
            first_seen: now,
            last_updated: now,
            source: "dht".to_string(),
            metadata_complete: true,
        }
    }

    #[test]
    fn test_upsert_and_get() {
        let db = TorrentIndexDb::open_memory().unwrap();
        let torrent = make_test_torrent("abc123", "Test Torrent");

        db.upsert(&torrent).unwrap();
        let fetched = db.get("abc123").unwrap().unwrap();

        assert_eq!(fetched.infohash, "abc123");
        assert_eq!(fetched.name, "Test Torrent");
        assert_eq!(fetched.seeders, 50);
        assert!(fetched.metadata_complete);
    }

    #[test]
    fn test_upsert_update() {
        let db = TorrentIndexDb::open_memory().unwrap();
        let mut torrent = make_test_torrent("abc123", "Old Name");
        torrent.seeders = 10;
        db.upsert(&torrent).unwrap();

        // 更新
        torrent.name = "New Name".to_string();
        torrent.seeders = 100;
        db.upsert(&torrent).unwrap();

        let fetched = db.get("abc123").unwrap().unwrap();
        assert_eq!(fetched.name, "New Name");
        assert_eq!(fetched.seeders, 100); // MAX(10, 100) = 100
    }

    #[test]
    fn test_search() {
        let db = TorrentIndexDb::open_memory().unwrap();
        db.upsert(&make_test_torrent("aaa", "Ubuntu 22.04 ISO")).unwrap();
        db.upsert(&make_test_torrent("bbb", "Fedora 38 Workstation")).unwrap();
        db.upsert(&make_test_torrent("ccc", "Debian 12 DVD")).unwrap();

        let results = db.search("Ubuntu", 10, 0).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].infohash, "aaa");

        let results = db.search("Linux", 10, 0).unwrap();
        assert_eq!(results.len(), 0); // 没有匹配
    }

    #[test]
    fn test_get_top() {
        let db = TorrentIndexDb::open_memory().unwrap();
        let mut t1 = make_test_torrent("aaa", "Low Seeders");
        t1.seeders = 5;
        let mut t2 = make_test_torrent("bbb", "High Seeders");
        t2.seeders = 500;
        db.upsert(&t1).unwrap();
        db.upsert(&t2).unwrap();

        let top = db.get_top(10).unwrap();
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].infohash, "bbb"); // 高 seeders 排第一
    }

    #[test]
    fn test_update_peer_stats() {
        let db = TorrentIndexDb::open_memory().unwrap();
        db.upsert(&make_test_torrent("abc", "Test")).unwrap();

        db.update_peer_stats("abc", 100, 20, 5).unwrap();

        let fetched = db.get("abc").unwrap().unwrap();
        assert_eq!(fetched.seeders, 100);
        assert_eq!(fetched.leechers, 20);
        assert_eq!(fetched.completed, 105); // 100 + 5
    }

    #[test]
    fn test_files() {
        let db = TorrentIndexDb::open_memory().unwrap();
        db.upsert(&make_test_torrent("abc", "Multi-file")).unwrap();

        let files = vec![
            TorrentFile {
                infohash: "abc".to_string(),
                file_index: 0,
                path: "dir/file1.txt".to_string(),
                length: 1000,
            },
            TorrentFile {
                infohash: "abc".to_string(),
                file_index: 1,
                path: "dir/file2.txt".to_string(),
                length: 2000,
            },
        ];
        db.insert_files("abc", &files).unwrap();

        let fetched = db.get_files("abc").unwrap();
        assert_eq!(fetched.len(), 2);
        assert_eq!(fetched[0].path, "dir/file1.txt");
    }

    #[test]
    fn test_delete() {
        let db = TorrentIndexDb::open_memory().unwrap();
        db.upsert(&make_test_torrent("abc", "Test")).unwrap();
        assert!(db.get("abc").unwrap().is_some());

        db.delete("abc").unwrap();
        assert!(db.get("abc").unwrap().is_none());
    }

    #[test]
    fn test_stats() {
        let db = TorrentIndexDb::open_memory().unwrap();
        db.upsert(&make_test_torrent("aaa", "Test 1")).unwrap();
        db.upsert(&make_test_torrent("bbb", "Test 2")).unwrap();

        let stats = db.stats().unwrap();
        assert_eq!(stats.total_torrents, 2);
        assert_eq!(stats.with_metadata, 2);
        assert_eq!(stats.total_seeders, 100);
    }

    #[test]
    fn test_new_stub() {
        let stub = TorrentIndex::new_stub("abc123", "dht");
        assert_eq!(stub.infohash, "abc123");
        assert_eq!(stub.source, "dht");
        assert!(!stub.metadata_complete);
        assert_eq!(stub.name, "");
    }
}
