//! pk 统计与监控
//!
//! 聚合索引库、任务调度、服务注册等各模块的统计信息。
//! 提供 Prometheus 指标端点。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

/// 全局统计计数器
static STATS: OnceLock<PkStats> = OnceLock::new();

/// pk 全局统计
pub struct PkStats {
    /// API 请求总数
    pub api_requests: AtomicU64,
    /// API 请求错误数
    pub api_errors: AtomicU64,
    /// 种子索引总数
    pub indexed_torrents: AtomicU64,
    /// metadata 下载成功数
    pub metadata_downloads: AtomicU64,
    /// metadata 下载失败数
    pub metadata_failures: AtomicU64,
    /// PDC 同步次数
    pub pdc_syncs: AtomicU64,
    /// 活跃任务数
    pub active_tasks: AtomicU64,
    /// 已注册服务数
    pub registered_services: AtomicU64,
}

impl PkStats {
    /// 获取全局统计实例
    pub fn global() -> &'static PkStats {
        STATS.get_or_init(|| PkStats {
            api_requests: AtomicU64::new(0),
            api_errors: AtomicU64::new(0),
            indexed_torrents: AtomicU64::new(0),
            metadata_downloads: AtomicU64::new(0),
            metadata_failures: AtomicU64::new(0),
            pdc_syncs: AtomicU64::new(0),
            active_tasks: AtomicU64::new(0),
            registered_services: AtomicU64::new(0),
        })
    }

    /// 记录 API 请求
    pub fn record_api_request(&self) {
        self.api_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录 API 错误
    pub fn record_api_error(&self) {
        self.api_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录 metadata 下载成功
    pub fn record_metadata_download(&self) {
        self.metadata_downloads.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录 metadata 下载失败
    pub fn record_metadata_failure(&self) {
        self.metadata_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录 PDC 同步
    pub fn record_pdc_sync(&self) {
        self.pdc_syncs.fetch_add(1, Ordering::Relaxed);
    }

    /// 设置索引种子数
    pub fn set_indexed_torrents(&self, count: u64) {
        self.indexed_torrents.store(count, Ordering::Relaxed);
    }

    /// 设置活跃任务数
    pub fn set_active_tasks(&self, count: u64) {
        self.active_tasks.store(count, Ordering::Relaxed);
    }

    /// 设置已注册服务数
    pub fn set_registered_services(&self, count: u64) {
        self.registered_services.store(count, Ordering::Relaxed);
    }

    /// 获取统计快照
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            api_requests: self.api_requests.load(Ordering::Relaxed),
            api_errors: self.api_errors.load(Ordering::Relaxed),
            indexed_torrents: self.indexed_torrents.load(Ordering::Relaxed),
            metadata_downloads: self.metadata_downloads.load(Ordering::Relaxed),
            metadata_failures: self.metadata_failures.load(Ordering::Relaxed),
            pdc_syncs: self.pdc_syncs.load(Ordering::Relaxed),
            active_tasks: self.active_tasks.load(Ordering::Relaxed),
            registered_services: self.registered_services.load(Ordering::Relaxed),
        }
    }

    /// 生成 Prometheus 格式指标
    pub fn to_prometheus(&self) -> String {
        let s = self.snapshot();
        format!(
            r#"# HELP pk_api_requests_total Total API requests
# TYPE pk_api_requests_total counter
pk_api_requests_total {}
# HELP pk_api_errors_total Total API errors
# TYPE pk_api_errors_total counter
pk_api_errors_total {}
# HELP pk_indexed_torrents Number of indexed torrents
# TYPE pk_indexed_torrents gauge
pk_indexed_torrents {}
# HELP pk_metadata_downloads_total Total metadata downloads
# TYPE pk_metadata_downloads_total counter
pk_metadata_downloads_total {}
# HELP pk_metadata_failures_total Total metadata download failures
# TYPE pk_metadata_failures_total counter
pk_metadata_failures_total {}
# HELP pk_pdc_syncs_total Total PDC syncs
# TYPE pk_pdc_syncs_total counter
pk_pdc_syncs_total {}
# HELP pk_active_tasks Number of active tasks
# TYPE pk_active_tasks gauge
pk_active_tasks {}
# HELP pk_registered_services Number of registered services
# TYPE pk_registered_services gauge
pk_registered_services {}
"#,
            s.api_requests,
            s.api_errors,
            s.indexed_torrents,
            s.metadata_downloads,
            s.metadata_failures,
            s.pdc_syncs,
            s.active_tasks,
            s.registered_services,
        )
    }
}

/// 统计快照
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StatsSnapshot {
    pub api_requests: u64,
    pub api_errors: u64,
    pub indexed_torrents: u64,
    pub metadata_downloads: u64,
    pub metadata_failures: u64,
    pub pdc_syncs: u64,
    pub active_tasks: u64,
    pub registered_services: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_counter() {
        let stats = PkStats::global();
        let before = stats.snapshot().api_requests;

        stats.record_api_request();
        stats.record_api_request();
        stats.record_api_error();

        let after = stats.snapshot();
        assert_eq!(after.api_requests, before + 2);
        assert_eq!(after.api_errors, before + 1); // 注意：全局共享，可能有其他测试影响
    }

    #[test]
    fn test_metadata_stats() {
        let stats = PkStats::global();
        let before = stats.snapshot().metadata_downloads;

        stats.record_metadata_download();
        stats.record_metadata_failure();

        let after = stats.snapshot();
        assert_eq!(after.metadata_downloads, before + 1);
        assert_eq!(after.metadata_failures, before + 1);
    }

    #[test]
    fn test_gauge_set() {
        let stats = PkStats::global();
        stats.set_indexed_torrents(1000);
        stats.set_active_tasks(5);
        stats.set_registered_services(3);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.indexed_torrents, 1000);
        assert_eq!(snapshot.active_tasks, 5);
        assert_eq!(snapshot.registered_services, 3);
    }

    #[test]
    fn test_prometheus_format() {
        let stats = PkStats::global();
        stats.set_indexed_torrents(42);

        let output = stats.to_prometheus();
        assert!(output.contains("pk_indexed_torrents 42"));
        assert!(output.contains("# HELP pk_api_requests_total"));
        assert!(output.contains("# TYPE pk_api_requests_total counter"));
    }

    #[test]
    fn test_snapshot_serialize() {
        let snapshot = StatsSnapshot {
            api_requests: 100,
            api_errors: 5,
            indexed_torrents: 50,
            metadata_downloads: 30,
            metadata_failures: 2,
            pdc_syncs: 10,
            active_tasks: 3,
            registered_services: 2,
        };

        let json = serde_json::to_string(&snapshot).unwrap();
        assert!(json.contains("\"api_requests\":100"));
        assert!(json.contains("\"indexed_torrents\":50"));

        let parsed: StatsSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.api_requests, 100);
        assert_eq!(parsed.indexed_torrents, 50);
    }
}
