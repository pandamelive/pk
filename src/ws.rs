use crate::models::*;
use crate::scheduler;
use crate::store::AppState;
use axum::extract::ws::{Message, Utf8Bytes, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

// ── WebSocket 消息协议 ───────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    ConfigChanged,
    NewTask,
    HeartbeatAck {
        timestamp: String,
    },
    Error {
        message: String,
    },
    /// 服务变更通知（服务注册中心推送，Agent 收到后更新本地服务缓存）
    ServiceChanged {
        agent_id: Uuid,
        change_type: String,
        agent_type: String,
        #[serde(default)]
        capabilities: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        port: Option<u16>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        region: Option<String>,
        #[serde(default)]
        health: String,
        #[serde(default)]
        load: f32,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    Register {
        node_id: Uuid,
        hostname: String,
        platform: String,
        arch: String,
        version: String,
        /// Agent 类型（如 spde / pdc，用于服务注册中心分类，可选）
        #[serde(default)]
        agent_type: Option<String>,
        /// serve 模式监听地址（用于 Agent 间点对点通信，可选）
        #[serde(default, skip_serializing_if = "Option::is_none")]
        serve_host: Option<String>,
        /// serve 模式监听端口（可选）
        #[serde(default, skip_serializing_if = "Option::is_none")]
        serve_port: Option<u16>,
        /// 区域/机房标识（可选）
        #[serde(default, skip_serializing_if = "Option::is_none")]
        region: Option<String>,
        /// 能力标识列表（用于服务注册中心，可选）
        #[serde(default)]
        capability_tags: Vec<String>,
    },
    Heartbeat {
        node_id: Uuid,
        active_tasks: u32,
        bytes_downloaded: u64,
    },
    Pong,
}

// ── 连接管理 ──────────────────────────────────────────────

pub struct NodeConn {
    pub tx: mpsc::UnboundedSender<Message>,
    pub node_id: Option<Uuid>,
}

pub struct WsManager {
    conns: RwLock<HashMap<Uuid, NodeConn>>,
}

impl WsManager {
    pub fn new() -> Self {
        Self {
            conns: RwLock::new(HashMap::new()),
        }
    }

    pub async fn register(&self, conn_id: Uuid, tx: mpsc::UnboundedSender<Message>) {
        let mut conns = self.conns.write().await;
        conns.insert(conn_id, NodeConn { tx, node_id: None });
    }

    pub async fn bind_node(&self, conn_id: Uuid, node_id: Uuid) {
        let mut conns = self.conns.write().await;
        if let Some(c) = conns.get_mut(&conn_id) {
            c.node_id = Some(node_id);
        }
    }

    pub async fn remove(&self, conn_id: Uuid) {
        let mut conns = self.conns.write().await;
        conns.remove(&conn_id);
    }

    /// 向所有已注册的节点广播消息
    pub async fn broadcast(&self, msg: &ServerMsg) {
        let text = serde_json::to_string(msg).unwrap_or_default();
        let conns = self.conns.read().await;
        for c in conns.values() {
            if c.node_id.is_some() {
                let _ = c.tx.send(Message::Text(text.clone().into()));
            }
        }
    }
}

// ── 通知函数 ──────────────────────────────────────────────

/// 通知所有节点：配置已变更（节点应重新拉取 config）
pub async fn notify_config_changed(state: &Arc<AppState>) {
    state.ws_mgr.broadcast(&ServerMsg::ConfigChanged).await;
}

/// 通知所有节点：共享待下发池有新任务（空闲节点去 claim 领取）
pub async fn notify_new_task(state: &Arc<AppState>) {
    state.ws_mgr.broadcast(&ServerMsg::NewTask).await;
}

/// 通知所有节点：服务注册中心有变更（Agent 上下线/能力更新）
pub async fn notify_service_changed(
    state: &Arc<AppState>,
    event: &crate::models::ServiceChangedEvent,
) {
    let msg = ServerMsg::ServiceChanged {
        agent_id: event.agent_id,
        change_type: event.change_type.clone(),
        agent_type: event.agent_type.clone(),
        capabilities: event.capabilities.clone(),
        host: event.host.clone(),
        port: event.port,
        region: event.region.clone(),
        health: event.health.clone(),
        load: event.load,
    };
    state.ws_mgr.broadcast(&msg).await;
}

// ── WebSocket 处理 ────────────────────────────────────────

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let conn_id = Uuid::new_v4();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

    state.ws_mgr.register(conn_id, tx).await;
    tracing::info!("[ws] 新连接 conn_id={}", conn_id);

    // 发送任务：从 mpsc 接收消息并发送给客户端
    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    // 接收任务：处理客户端消息
    while let Some(Ok(msg)) = receiver.next().await {
        match msg {
            Message::Text(text) => {
                if let Err(e) = handle_client_msg(&state, conn_id, &text).await {
                    tracing::warn!("[ws] 处理消息失败: {}", e);
                }
            }
            Message::Binary(_) => {}
            Message::Ping(_) => {}
            Message::Pong(_) => {}
            Message::Close(_) => break,
        }
    }

    state.ws_mgr.remove(conn_id).await;
    send_task.abort();
    tracing::info!("[ws] 连接关闭 conn_id={}", conn_id);
}

async fn handle_client_msg(state: &Arc<AppState>, conn_id: Uuid, text: &str) -> anyhow::Result<()> {
    let msg: ClientMsg = serde_json::from_str(text)?;
    match msg {
        ClientMsg::Register {
            node_id,
            hostname,
            platform,
            arch,
            version,
            agent_type,
            serve_host,
            serve_port,
            region,
            capability_tags,
        } => {
            state.ws_mgr.bind_node(conn_id, node_id).await;
            let now = Utc::now();
            state
                .with_transaction(|conn| {
                    let exists: bool = conn
                        .query_row(
                            "SELECT COUNT(*) FROM nodes WHERE id = ?1",
                            params![node_id.to_string()],
                            |r| r.get::<_, i64>(0),
                        )
                        .map(|c| c > 0)
                        .unwrap_or(false);
                    if exists {
                        conn.execute(
                            "UPDATE nodes SET hostname=?1, platform=?2, arch=?3, version=?4, status='online', last_seen=?5 WHERE id=?6",
                            params![hostname, platform, arch, version, now.to_rfc3339(), node_id.to_string()],
                        )?;
                    } else {
                        conn.execute(
                            "INSERT INTO nodes VALUES (?1,?2,?3,?4,?5,'online',?6,?6,'[]',0,0,NULL)",
                            params![node_id.to_string(), hostname, platform, arch, version, now.to_rfc3339()],
                        )?;
                    }
                    Ok(())
                })
                .await?;

            // 如果提供了 serve 地址，注册到服务注册中心并广播
            if let (Some(host), Some(port)) = (serve_host, serve_port) {
                let at = agent_type.unwrap_or_else(|| "unknown".to_string());
                let info = crate::models::ServiceAgentInfo {
                    agent_id: node_id,
                    name: hostname.clone(),
                    agent_type: at,
                    host,
                    port,
                    capabilities: capability_tags,
                    health: "healthy".to_string(),
                    load: 0.0,
                    region,
                    version: version.clone(),
                    last_heartbeat: Some(now.to_rfc3339()),
                };
                let event = state.service_registry.register(info).await;
                notify_service_changed(state, &event).await;
            }

            tracing::info!("[ws] 节点注册 node_id={} hostname={}", node_id, hostname);
        }
        ClientMsg::Heartbeat {
            node_id,
            active_tasks,
            bytes_downloaded,
        } => {
            let now = Utc::now();
            state
                .with_transaction(|conn| {
                    conn.execute(
                        "UPDATE nodes SET status='online', last_seen=?1, active_tasks=?2, bytes_downloaded=?3 WHERE id=?4",
                        params![now.to_rfc3339(), active_tasks, bytes_downloaded, node_id.to_string()],
                    )?;
                    Ok(())
                })
                .await?;
            // 同步更新服务注册中心健康状态
            let load = if active_tasks > 0 {
                (active_tasks as f32 / 4.0).min(1.0)
            } else {
                0.0
            };
            state
                .service_registry
                .update_health(node_id, "healthy", load)
                .await;
        }
        ClientMsg::Pong => {}
    }
    Ok(())
}

// ── 给节点生成任务配置（claim 领取后使用） ────────────────

pub fn build_node_task_config(state: &AppState, node_task: &scheduler::NodeTask) -> NodeConfig {
    let defaults = &state.cfg.spde_defaults;
    let t = &node_task.task;
    let mut item = TaskItem {
        url: t.url.clone(),
        filename: t.filename.clone(),
        save_path: defaults.save_path.clone(),
        max_concurrent: defaults.max_concurrent,
        connections_per_file: defaults.connections_per_file,
        retry_times: defaults.retry_times,
        timeout: defaults.timeout,
        dry_run: defaults.dry_run,
        skip_tls_verify: defaults.skip_tls_verify,
        resume: defaults.resume,
        http_proxy: defaults.http_proxy.clone(),
        https_proxy: defaults.https_proxy.clone(),
    };
    let o = &t.overrides;
    if let Some(v) = o.max_concurrent {
        item.max_concurrent = v;
    }
    if let Some(v) = o.connections_per_file {
        item.connections_per_file = v;
    }
    if let Some(v) = o.retry_times {
        item.retry_times = v;
    }
    if let Some(v) = o.timeout {
        item.timeout = v;
    }
    if let Some(v) = o.skip_tls_verify {
        item.skip_tls_verify = v;
    }
    if let Some(v) = o.dry_run {
        item.dry_run = v;
    }
    if let Some(v) = o.save_path.clone() {
        item.save_path = v;
    }
    NodeConfig {
        dispatch_id: node_task.dispatch_id,
        master: format!(
            "http://127.0.0.1:{}",
            state.cfg.listen.split(':').last().unwrap_or("5566")
        ),
        token: if state.cfg.token.is_empty() {
            None
        } else {
            Some(state.cfg.token.clone())
        },
        tasks: vec![item],
    }
}
