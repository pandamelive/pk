use crate::models::*;
use crate::scheduler;
use crate::store::AppState;
use axum::extract::ws::{Message, WebSocket};
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
    HeartbeatAck { timestamp: String },
    Error { message: String },
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
    state
        .ws_mgr
        .broadcast(&ServerMsg::ConfigChanged)
        .await;
}

/// 通知所有节点：共享待下发池有新任务（空闲节点去 claim 领取）
pub async fn notify_new_task(state: &Arc<AppState>) {
    state.ws_mgr.broadcast(&ServerMsg::NewTask).await;
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
    if let Some(v) = o.max_concurrent { item.max_concurrent = v; }
    if let Some(v) = o.connections_per_file { item.connections_per_file = v; }
    if let Some(v) = o.retry_times { item.retry_times = v; }
    if let Some(v) = o.timeout { item.timeout = v; }
    if let Some(v) = o.skip_tls_verify { item.skip_tls_verify = v; }
    if let Some(v) = o.dry_run { item.dry_run = v; }
    if let Some(v) = o.save_path.clone() { item.save_path = v; }
    NodeConfig {
        dispatch_id: node_task.dispatch_id,
        master: format!("http://127.0.0.1:{}", state.cfg.listen.split(':').last().unwrap_or("5566")),
        token: if state.cfg.token.is_empty() { None } else { Some(state.cfg.token.clone()) },
        tasks: vec![item],
    }
}
