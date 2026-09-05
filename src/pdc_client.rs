//! PDC (PeerDiscoveryCenter) 客户端
//!
//! pk 通过 PDC 的 REST API 获取 peer 统计信息，更新种子索引库。
//!
//! PDC API 端点：
//! - GET /api/v1/stats - 全局统计
//! - GET /api/v1/cache/{infohash} - 查询缓存的 peer
//! - GET /api/v1/health - 健康检查

use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

/// PDC 客户端配置
#[derive(Debug, Clone)]
pub struct PdcClientConfig {
    /// PDC 服务地址（如 http://127.0.0.1:6880）
    pub base_url: String,
    /// 请求超时
    pub timeout: Duration,
}

impl Default for PdcClientConfig {
    fn default() -> Self {
        PdcClientConfig {
            base_url: "http://127.0.0.1:6880".to_string(),
            timeout: Duration::from_secs(10),
        }
    }
}

/// PDC 客户端
pub struct PdcClient {
    config: PdcClientConfig,
    client: Client,
}

impl PdcClient {
    /// 创建新的 PDC 客户端
    pub fn new(config: PdcClientConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .context("构建 HTTP 客户端失败")?;
        Ok(PdcClient { config, client })
    }

    /// 使用默认配置创建
    pub fn with_default() -> Result<Self> {
        Self::new(PdcClientConfig::default())
    }

    /// 健康检查
    pub async fn health_check(&self) -> bool {
        let url = format!("{}/api/v1/health", self.config.base_url);
        match self.client.get(&url).send().await {
            Ok(resp) => resp.status().is_success(),
            Err(e) => {
                warn!("[pdc] 健康检查失败: {}", e);
                false
            }
        }
    }

    /// 获取 PDC 全局统计
    pub async fn get_stats(&self) -> Result<PdcStats> {
        let url = format!("{}/api/v1/stats", self.config.base_url);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("获取 PDC 统计失败: {}", url))?;

        if !resp.status().is_success() {
            return Err(anyhow::anyhow!("PDC 返回状态: {}", resp.status()));
        }

        let stats: PdcStats = resp.json().await.context("解析 PDC 统计失败")?;
        debug!("[pdc] 统计: nodes={}, infohashes={}", stats.dht_nodes, stats.dht_infohashes);
        Ok(stats)
    }

    /// 查询某个 infohash 的缓存 peer
    pub async fn get_cached_peers(&self, infohash: &str) -> Result<Vec<PdcPeer>> {
        let url = format!("{}/api/v1/cache/{}", self.config.base_url, infohash);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("查询 PDC 缓存 peer 失败: {}", url))?;

        if resp.status() == 404 {
            return Ok(vec![]);
        }

        if !resp.status().is_success() {
            return Err(anyhow::anyhow!("PDC 返回状态: {}", resp.status()));
        }

        let peers: Vec<PdcPeer> = resp.json().await.context("解析 PDC peer 失败")?;
        Ok(peers)
    }

    /// 从 PDC 同步某个 infohash 的 peer 统计到索引库
    ///
    /// 返回 (seeders, leechers) 估算值
    pub async fn sync_infohash_stats(&self, infohash: &str) -> Result<(i64, i64)> {
        let peers = self.get_cached_peers(infohash).await?;

        // 简单估算：缓存中的 peer 数量 * 放大系数
        // 实际 PDC 缓存只是全网的一小部分
        let total = peers.len() as i64;
        let seeders = peers.iter().filter(|p| p.is_seeder).count() as i64;
        let leechers = total - seeders;

        info!(
            "[pdc] 同步 {}: seeders={}, leechers={} (缓存样本)",
            infohash, seeders, leechers
        );

        Ok((seeders, leechers))
    }
}

/// PDC 全局统计
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PdcStats {
    #[serde(default)]
    pub dht_nodes: u64,
    #[serde(default)]
    pub dht_infohashes: u64,
    #[serde(default)]
    pub dht_peers: u64,
    #[serde(default)]
    pub tracker_peers: u64,
    #[serde(default)]
    pub tracker_infohashes: u64,
    #[serde(default)]
    pub cache_peers: u64,
    #[serde(default)]
    pub cache_infohashes: u64,
    #[serde(default)]
    pub banned_peers: u64,
    #[serde(default)]
    pub crawler_infohashes: u64,
    #[serde(default)]
    pub crawler_peers: u64,
}

/// PDC 缓存的 peer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PdcPeer {
    pub ip: String,
    pub port: u16,
    #[serde(default)]
    pub is_seeder: bool,
    #[serde(default)]
    pub last_seen: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = PdcClientConfig::default();
        assert_eq!(config.base_url, "http://127.0.0.1:6880");
    }

    #[test]
    fn test_client_creation() {
        let client = PdcClient::with_default();
        assert!(client.is_ok());
    }

    #[test]
    fn test_pdc_stats_deserialize() {
        let json = r#"{
            "dht_nodes": 5000,
            "dht_infohashes": 100000,
            "dht_peers": 500000,
            "tracker_peers": 10000,
            "tracker_infohashes": 5000,
            "cache_peers": 50000,
            "cache_infohashes": 20000,
            "banned_peers": 100,
            "crawler_infohashes": 80000,
            "crawler_peers": 400000
        }"#;
        let stats: PdcStats = serde_json::from_str(json).unwrap();
        assert_eq!(stats.dht_nodes, 5000);
        assert_eq!(stats.dht_infohashes, 100000);
        assert_eq!(stats.banned_peers, 100);
    }

    #[test]
    fn test_pdc_peer_deserialize() {
        let json = r#"{"ip":"1.2.3.4","port":6881,"is_seeder":true,"last_seen":1700000000}"#;
        let peer: PdcPeer = serde_json::from_str(json).unwrap();
        assert_eq!(peer.ip, "1.2.3.4");
        assert_eq!(peer.port, 6881);
        assert!(peer.is_seeder);
    }
}
