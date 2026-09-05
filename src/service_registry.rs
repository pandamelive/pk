//! 服务注册中心
//!
//! 维护所有已注册 Agent 的 serve 模式地址与能力信息，
//! 供其他 Agent 查询并建立点对点连接。
//!
//! 服务注册信息为内存存储，重启后由各 Agent 重新注册。
//! 节点心跳时同步更新服务健康状态，心跳超时自动标记为 unhealthy。

use crate::models::{ServiceAgentInfo, ServiceChangedEvent};
use std::collections::HashMap;
use tokio::sync::RwLock;
use uuid::Uuid;

/// 服务注册中心
pub struct ServiceRegistry {
    services: RwLock<HashMap<Uuid, ServiceAgentInfo>>,
}

impl ServiceRegistry {
    pub fn new() -> Self {
        Self {
            services: RwLock::new(HashMap::new()),
        }
    }

    /// 注册或更新服务，返回变更事件（用于 WebSocket 广播）
    pub async fn register(&self, info: ServiceAgentInfo) -> ServiceChangedEvent {
        let mut services = self.services.write().await;
        let is_new = !services.contains_key(&info.agent_id);
        services.insert(info.agent_id, info.clone());

        if is_new {
            ServiceChangedEvent {
                agent_id: info.agent_id,
                change_type: "up".to_string(),
                agent_type: info.agent_type,
                capabilities: info.capabilities,
                host: Some(info.host),
                port: Some(info.port),
                region: info.region,
                health: info.health,
                load: info.load,
            }
        } else {
            ServiceChangedEvent {
                agent_id: info.agent_id,
                change_type: "updated".to_string(),
                agent_type: info.agent_type,
                capabilities: info.capabilities,
                host: Some(info.host),
                port: Some(info.port),
                region: info.region,
                health: info.health,
                load: info.load,
            }
        }
    }

    /// 注销服务（节点下线），返回变更事件
    pub async fn unregister(&self, agent_id: Uuid) -> Option<ServiceChangedEvent> {
        let mut services = self.services.write().await;
        services.remove(&agent_id).map(|info| ServiceChangedEvent {
            agent_id,
            change_type: "down".to_string(),
            agent_type: info.agent_type,
            capabilities: Vec::new(),
            host: None,
            port: None,
            region: None,
            health: "unhealthy".to_string(),
            load: 0.0,
        })
    }

    /// 更新服务健康状态（心跳时调用）
    pub async fn update_health(&self, agent_id: Uuid, health: &str, load: f32) {
        let mut services = self.services.write().await;
        if let Some(info) = services.get_mut(&agent_id) {
            info.health = health.to_string();
            info.load = load;
            info.last_heartbeat = Some(chrono::Utc::now().to_rfc3339());
        }
    }

    /// 标记超时节点为 unhealthy
    pub async fn mark_timeout_unhealthy(&self, cutoff_secs: i64) -> Vec<Uuid> {
        let mut services = self.services.write().await;
        let now = chrono::Utc::now();
        let mut changed = Vec::new();
        for info in services.values_mut() {
            if info.health == "healthy" {
                if let Some(last) = &info.last_heartbeat {
                    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(last) {
                        let dt_utc: chrono::DateTime<chrono::Utc> = dt.into();
                        if (now - dt_utc).num_seconds() > cutoff_secs {
                            info.health = "unhealthy".to_string();
                            changed.push(info.agent_id);
                        }
                    }
                }
            }
        }
        changed
    }

    /// 查询服务（按能力/类型/健康/区域过滤）
    pub async fn query(
        &self,
        capability: Option<&str>,
        agent_type: Option<&str>,
        health: Option<&str>,
        region: Option<&str>,
    ) -> Vec<ServiceAgentInfo> {
        let services = self.services.read().await;
        services
            .values()
            .filter(|info| {
                if let Some(cap) = capability {
                    if !info.has_capability(cap) {
                        return false;
                    }
                }
                if let Some(at) = agent_type {
                    if info.agent_type != at {
                        return false;
                    }
                }
                if let Some(h) = health {
                    if info.health != h {
                        return false;
                    }
                }
                if let Some(r) = region {
                    if info.region.as_deref() != Some(r) {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect()
    }

    /// 获取单个服务信息
    pub async fn get(&self, agent_id: Uuid) -> Option<ServiceAgentInfo> {
        let services = self.services.read().await;
        services.get(&agent_id).cloned()
    }

    /// 列出所有服务
    pub async fn list(&self) -> Vec<ServiceAgentInfo> {
        let services = self.services.read().await;
        services.values().cloned().collect()
    }

    /// 服务总数
    pub async fn count(&self) -> usize {
        let services = self.services.read().await;
        services.len()
    }

    /// 健康服务数
    pub async fn healthy_count(&self) -> usize {
        let services = self.services.read().await;
        services.values().filter(|s| s.is_healthy()).count()
    }
}

impl Default for ServiceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_info(id: Uuid) -> ServiceAgentInfo {
        ServiceAgentInfo {
            agent_id: id,
            name: "pdc-01".to_string(),
            agent_type: "pdc".to_string(),
            host: "10.0.0.5".to_string(),
            port: 6881,
            capabilities: vec![
                "tracker".to_string(),
                "dht".to_string(),
                "cache".to_string(),
            ],
            health: "healthy".to_string(),
            load: 0.3,
            region: Some("cn-east".to_string()),
            version: "0.1.0".to_string(),
            last_heartbeat: Some(chrono::Utc::now().to_rfc3339()),
        }
    }

    #[tokio::test]
    async fn register_and_query() {
        let reg = ServiceRegistry::new();
        let id = Uuid::new_v4();
        let info = sample_info(id);

        let event = reg.register(info.clone()).await;
        assert_eq!(event.change_type, "up");
        assert_eq!(reg.count().await, 1);

        // 按能力查询
        let results = reg.query(Some("tracker"), None, None, None).await;
        assert_eq!(results.len(), 1);

        let results = reg.query(Some("pex"), None, None, None).await;
        assert_eq!(results.len(), 0);

        // 按类型查询
        let results = reg.query(None, Some("pdc"), None, None).await;
        assert_eq!(results.len(), 1);

        let results = reg.query(None, Some("spde"), None, None).await;
        assert_eq!(results.len(), 0);

        // 按健康状态查询
        let results = reg.query(None, None, Some("healthy"), None).await;
        assert_eq!(results.len(), 1);

        // 按区域查询
        let results = reg.query(None, None, None, Some("cn-east")).await;
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn register_again_is_update() {
        let reg = ServiceRegistry::new();
        let id = Uuid::new_v4();
        let info = sample_info(id);

        let event1 = reg.register(info.clone()).await;
        assert_eq!(event1.change_type, "up");

        let event2 = reg.register(info.clone()).await;
        assert_eq!(event2.change_type, "updated");
        assert_eq!(reg.count().await, 1);
    }

    #[tokio::test]
    async fn unregister() {
        let reg = ServiceRegistry::new();
        let id = Uuid::new_v4();
        reg.register(sample_info(id)).await;
        assert_eq!(reg.count().await, 1);

        let event = reg.unregister(id).await;
        assert!(event.is_some());
        assert_eq!(event.unwrap().change_type, "down");
        assert_eq!(reg.count().await, 0);

        // 再次注销返回 None
        assert!(reg.unregister(id).await.is_none());
    }

    #[tokio::test]
    async fn update_health() {
        let reg = ServiceRegistry::new();
        let id = Uuid::new_v4();
        reg.register(sample_info(id)).await;

        reg.update_health(id, "unhealthy", 0.9).await;
        let info = reg.get(id).await.unwrap();
        assert_eq!(info.health, "unhealthy");
        assert!((info.load - 0.9).abs() < 0.001);
    }
}
