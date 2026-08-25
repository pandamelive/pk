use crate::models::*;
use crate::scheduler;
use crate::store::AppState;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

// ── 消息协议 ──────────────────────────────────────────────

/// PK → SPDE
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    /// 配置可能变化，SPDE 应重新拉取 config.yaml
    ConfigChanged,
    /// 保活心跳
    Ping,
}

/// SPDE → PK
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    Pong,
    /// 节点状态上报（替代 HTTP 心跳）
    Status {
        #[serde(default)]
        active_tasks: u32,
        #[serde(default)]
        bytes_downloaded: u64,
        #[serde(default)]
        busy: bool,
        #[serde(default)]
        last_error: Option<String>,
    },
    /// 任务开始执行（替代 /agent/ack）
    TaskStarted { dispatch_id: Uuid },
    /// 任务完成报告（替代 /agent/report）
    TaskReport(WsTaskReport),
}

#[derive(Debug, Clone, Deserialize)]
pub struct WsTaskReport {
    pub dispatch_id: Option<Uuid>,
    pub task_id: Option<Uuid>,
    pub task_name: String,
    pub url: String,
    pub filename: String,
    pub file_size: u64,
    pub downloaded_bytes: u64,
    pub elapsed_secs: f64,
    pub avg_speed_mbps: f64,
    pub status: String,
    pub success_chunks: u64,
    pub failed_chunks: u64,
    pub error_msg: Option<String>,
}

// ── 连接管理 ──────────────────────────────────────────────

pub type WsSender = mpsc::Sender<Message>;

pub struct WsManager {
    inner: RwLock<HashMap<Uuid, Vec<WsSender>>>,
}

impl WsManager {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub async fn register(&self, node_id: Uuid, tx: WsSender) {
        let mut g = self.inner.write().await;
        g.entry(node_id).or_default().push(tx);
        tracing::info!("ws connected: {node_id} (total nodes: {})", g.len());
    }

    pub async fn unregister(&self, node_id: Uuid, tx: &WsSender) {
        let mut g = self.inner.write().await;
        if let Some(senders) = g.get_mut(&node_id) {
            senders.retain(|s| !s.same_channel(tx));
            if senders.is_empty() {
                g.remove(&node_id);
            }
        }
        tracing::info!("ws disconnected: {node_id} (total nodes: {})", g.len());
    }

    /// 向所有在线节点广播消息
    pub async fn broadcast_all(&self, msg: Message) {
        let g = self.inner.read().await;
        for senders in g.values() {
            for tx in senders {
                let _ = tx.send(msg.clone()).await;
            }
        }
    }
}

// ── WebSocket 端点 ────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct WsQuery {
    pub node_id: Uuid,
}

pub async fn agent_ws(
    State(state): State<Arc<AppState>>,
    Query(q): Query<WsQuery>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(state, q.node_id, socket))
}

async fn handle_socket(state: Arc<AppState>, node_id: Uuid, socket: WebSocket) {
    // 验证节点已注册
    let exists = state
        .snapshot()
        .await
        .nodes
        .iter()
        .any(|n| n.id == node_id);
    if !exists {
        tracing::warn!("ws rejected: node {node_id} not registered");
        return;
    }

    let (tx, mut rx) = mpsc::channel::<Message>(32);
    state.ws_mgr.register(node_id, tx.clone()).await;

    let (mut ws_sink, mut ws_stream) = socket.split();

    // 写任务：从 mpsc 读消息发给客户端
    let write_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_sink.send(msg).await.is_err() {
                break;
            }
        }
    });

    // ping 保活：每 30s 发一次
    let ping_tx = tx.clone();
    let ping_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            interval.tick().await;
            let ping = match serde_json::to_string(&ServerMsg::Ping) {
                Ok(s) => s,
                Err(_) => continue,
            };
            if ping_tx.send(Message::Text(ping.into())).await.is_err() {
                break;
            }
        }
    });

    // 读循环：处理客户端消息
    while let Some(msg_result) = ws_stream.next().await {
        match msg_result {
            Ok(Message::Text(text)) => {
                if let Ok(client_msg) = serde_json::from_str::<ClientMsg>(&text) {
                    handle_client_msg(&state, node_id, client_msg).await;
                }
            }
            Ok(Message::Close(_)) => break,
            Ok(_) => {}
            Err(e) => {
                tracing::debug!("ws recv error: {e}");
                break;
            }
        }
    }

    // 清理
    state.ws_mgr.unregister(node_id, &tx).await;
    write_task.abort();
    ping_task.abort();
}

async fn handle_client_msg(state: &Arc<AppState>, node_id: Uuid, msg: ClientMsg) {
    match msg {
        ClientMsg::Pong => {}

        ClientMsg::Status {
            active_tasks,
            bytes_downloaded,
            busy,
            last_error,
        } => {
            let now = Utc::now();
            let _ = state
                .with_mut(|snap| {
                    if let Some(n) = snap.nodes.iter_mut().find(|n| n.id == node_id) {
                        n.last_seen = now;
                        n.active_tasks = active_tasks;
                        n.bytes_downloaded = bytes_downloaded;
                        n.last_error = last_error;
                        n.status = if busy {
                            NodeStatus::Busy
                        } else {
                            NodeStatus::Online
                        };
                    }
                })
                .await;
        }

        ClientMsg::TaskStarted { dispatch_id } => {
            let _ = state
                .with_mut(|snap| scheduler::mark_running(snap, dispatch_id))
                .await;
        }

        ClientMsg::TaskReport(req) => {
            let report = AgentReportReq {
                node_id,
                dispatch_id: req.dispatch_id,
                task_id: req.task_id,
                task_name: req.task_name,
                url: req.url,
                filename: req.filename,
                file_size: req.file_size,
                downloaded_bytes: req.downloaded_bytes,
                elapsed_secs: req.elapsed_secs,
                avg_speed_mbps: req.avg_speed_mbps,
                status: req.status,
                success_chunks: req.success_chunks,
                failed_chunks: req.failed_chunks,
                error_msg: req.error_msg,
            };
            let _ = state
                .with_mut(|snap| scheduler::apply_report(snap, report))
                .await;
            // 任务完成后 config 可能变化（dispatch 状态变更）
            notify_config_changed(state).await;
        }
    }
}

// ── 通知 ──────────────────────────────────────────────────

/// 任务/配置变更后调用：通知所有在线 WebSocket 节点重新拉取 config
pub async fn notify_config_changed(state: &Arc<AppState>) {
    let msg = match serde_json::to_string(&ServerMsg::ConfigChanged) {
        Ok(s) => s,
        Err(_) => return,
    };
    state
        .ws_mgr
        .broadcast_all(Message::Text(msg.into()))
        .await;
}
