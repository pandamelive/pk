use crate::config::SpdeDefaults;
use crate::models::*;
use crate::scheduler;
use crate::spde_cfg;
use crate::store::{artifact_filename, detect_host_platform, AppState};
use crate::ws;
use crate::workflow_scheduler;
use anyhow::Result;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::Utc;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

// ── 通用响应 ──────────────────────────────────────────────

#[derive(Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn ok(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }
    pub fn err(msg: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(msg.into()),
        }
    }
}

pub type ApiResult<T> = Result<Json<ApiResponse<T>>, AppError>;

pub struct AppError(anyhow::Error);
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<()>::err(self.0.to_string())),
        )
            .into_response()
    }
}
impl<E: Into<anyhow::Error>> From<E> for AppError {
    fn from(e: E) -> Self {
        Self(e.into())
    }
}

// ── 鉴权中间件 ────────────────────────────────────────────

pub fn check_auth(state: &AppState, token: Option<&str>) -> Result<(), AppError> {
    if !state.cfg.token.is_empty() {
        match token {
            Some(t) if t == state.cfg.token => {}
            _ => {
                return Err(AppError(anyhow::anyhow!("未授权")));
            }
        }
    }
    Ok(())
}

// ── 节点管理 ──────────────────────────────────────────────

pub async fn list_nodes(State(state): State<Arc<AppState>>) -> ApiResult<Vec<Node>> {
    let mut nodes = state
        .with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT id, hostname, platform, arch, version, status, last_seen, registered_at, labels, active_tasks, bytes_downloaded, last_error FROM nodes ORDER BY registered_at DESC")?;
            let nodes: Vec<Node> = stmt
                .query_map([], |r| {
                    let labels_str: String = r.get(8)?;
                    Ok(Node {
                        id: r.get::<_, String>(0)?.parse().unwrap(),
                        hostname: r.get(1)?,
                        platform: r.get(2)?,
                        arch: r.get(3)?,
                        version: r.get(4)?,
                        status: match r.get::<_, String>(5)?.as_str() {
                            "online" => NodeStatus::Online,
                            _ => NodeStatus::Offline,
                        },
                        last_seen: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(6)?).ok().map(|dt| dt.with_timezone(&Utc)).unwrap_or(Utc::now()),
                        registered_at: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(7)?).ok().map(|dt| dt.with_timezone(&Utc)).unwrap_or(Utc::now()),
                        labels: serde_json::from_str(&labels_str).unwrap_or_default(),
                        active_tasks: r.get(9)?,
                        bytes_downloaded: r.get(10)?,
                        last_error: r.get(11)?,
                        total_speed_bps: 0,
                        active_tasks_progress: Vec::new(),
                    })
                })?
                .filter_map(|r| r.ok())
                .collect();
            Ok(nodes)
        })
        .await?;

    // 从内存态填充实时状态
    let realtime_map = state.ws_mgr.get_all_realtime().await;
    for node in &mut nodes {
        if let Some(rt) = realtime_map.get(&node.id) {
            node.total_speed_bps = rt.total_speed_bps;
            node.active_tasks_progress = rt.active_tasks.clone();
        }
    }

    Ok(Json(ApiResponse::ok(nodes)))
}

/// 查询单个节点实时状态
pub async fn get_node_realtime(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<crate::ws::NodeRealtime> {
    let rt = state.ws_mgr.get_node_realtime(id).await.unwrap_or_default();
    Ok(Json(ApiResponse::ok(rt)))
}

pub async fn delete_node(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<()> {
    let now = Utc::now().to_rfc3339();
    state
        .with_transaction(|conn| {
            // 记录到 deleted_nodes，该节点再次注册时需要审批
            conn.execute(
                "INSERT OR REPLACE INTO deleted_nodes (node_id, deleted_at, reason) VALUES (?1, ?2, 'manual_delete')",
                params![id.to_string(), now],
            )?;
            conn.execute("DELETE FROM nodes WHERE id = ?1", params![id.to_string()])?;
            Ok(())
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::ok(())))
}

/// 批量清理离线节点（DELETE /api/v1/nodes）
pub async fn purge_offline_nodes(State(state): State<Arc<AppState>>) -> ApiResult<u64> {
    let now = Utc::now().to_rfc3339();
    let deleted = state
        .with_transaction(|conn| {
            // 先查出所有 offline 节点的 id，记录到 deleted_nodes
            let offline_ids: Vec<String> = conn
                .prepare("SELECT id FROM nodes WHERE status = 'offline'")?
                .query_map([], |r| r.get::<_, String>(0))?
                .filter_map(|r| r.ok())
                .collect();
            for nid in &offline_ids {
                conn.execute(
                    "INSERT OR REPLACE INTO deleted_nodes (node_id, deleted_at, reason) VALUES (?1, ?2, 'purge_offline')",
                    params![nid, now],
                )?;
            }
            let n = conn.execute("DELETE FROM nodes WHERE status = 'offline'", [])?;
            Ok(n as u64)
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::ok(deleted)))
}

// ── 节点审批（删除后再次注册需人工同意） ──────────────────

/// 同意待审批节点
pub async fn approve_node(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<()> {
    state
        .with_transaction(|conn| {
            // 状态改为 online
            conn.execute(
                "UPDATE nodes SET status='online' WHERE id = ?1 AND status='pending'",
                params![id.to_string()],
            )?;
            // 从 deleted_nodes 移除，以后重启不再标记为 pending
            conn.execute("DELETE FROM deleted_nodes WHERE node_id = ?1", params![id.to_string()])?;
            Ok(())
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::ok(())))
}

/// 拒绝待审批节点
pub async fn reject_node(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<()> {
    state
        .with_transaction(|conn| {
            // 拒绝不删除节点，保持 pending 状态，用户可随时再点同意
            // 在 labels 中添加 rejected 标记，便于前端识别
            let labels_str: String = conn
                .query_row("SELECT labels FROM nodes WHERE id = ?1", params![id.to_string()], |r| r.get(0))
                .unwrap_or_else(|_| "[]".to_string());
            let mut labels: Vec<String> = serde_json::from_str(&labels_str).unwrap_or_default();
            if !labels.iter().any(|l| l == "rejected") {
                labels.push("rejected".to_string());
            }
            conn.execute(
                "UPDATE nodes SET labels=?1 WHERE id=?2 AND status='pending'",
                params![serde_json::to_string(&labels)?, id.to_string()],
            )?;
            Ok(())
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::ok(())))
}

// ── 任务管理 ──────────────────────────────────────────────

pub async fn list_tasks(State(state): State<Arc<AppState>>) -> ApiResult<Vec<Task>> {
    let tasks = state
        .with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT id, name, url, filename, enable, created_at, note, overrides FROM tasks ORDER BY created_at DESC")?;
            let tasks: Vec<Task> = stmt
                .query_map([], |r| {
                    let overrides_str: String = r.get(7)?;
                    Ok(Task {
                        id: r.get::<_, String>(0)?.parse().unwrap(),
                        name: r.get(1)?,
                        url: r.get(2)?,
                        filename: r.get(3)?,
                        enable: r.get::<_, i64>(4)? != 0,
                        created_at: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(5)?).ok().map(|dt| dt.with_timezone(&Utc)).unwrap_or(Utc::now()),
                        note: r.get(6)?,
                        overrides: serde_json::from_str(&overrides_str).unwrap_or_default(),
                    })
                })?
                .filter_map(|r| r.ok())
                .collect();
            Ok(tasks)
        })
        .await?;
    Ok(Json(ApiResponse::ok(tasks)))
}

pub async fn create_task(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateTaskReq>,
) -> ApiResult<Task> {
    let task = Task {
        id: Uuid::new_v4(),
        name: req.name,
        url: req.url,
        filename: req.filename,
        enable: req.enable,
        created_at: Utc::now(),
        note: req.note,
        overrides: req.overrides,
    };
    state
        .with_transaction(|conn| {
            conn.execute(
                "INSERT INTO tasks VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    task.id.to_string(),
                    task.name,
                    task.url,
                    task.filename,
                    task.enable,
                    task.created_at.to_rfc3339(),
                    task.note,
                    serde_json::to_string(&task.overrides)?,
                ],
            )?;
            Ok(())
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
        Ok(Json(ApiResponse::ok(task)))
}

pub async fn update_task(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    Json(req): Json<UpdateTaskReq>,
) -> ApiResult<Task> {
    let task = state
        .with_transaction(|conn| {
            let existing: Option<Task> = conn
                .query_row(
                    "SELECT id, name, url, filename, enable, created_at, note, overrides FROM tasks WHERE id = ?1",
                    params![id.to_string()],
                    |r| {
                        let overrides_str: String = r.get(7)?;
                        Ok(Task {
                            id: r.get::<_, String>(0)?.parse().unwrap(),
                            name: r.get(1)?,
                            url: r.get(2)?,
                            filename: r.get(3)?,
                            enable: r.get::<_, i64>(4)? != 0,
                            created_at: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(5)?).ok().map(|dt| dt.with_timezone(&Utc)).unwrap_or(Utc::now()),
                            note: r.get(6)?,
                            overrides: serde_json::from_str(&overrides_str).unwrap_or_default(),
                        })
                    },
                )
                .ok();
            let Some(mut task) = existing else {
                return Err(anyhow::anyhow!("任务不存在"));
            };
            if let Some(name) = req.name { task.name = name; }
            if let Some(url) = req.url { task.url = url; }
            if let Some(filename) = req.filename { task.filename = filename; }
            if let Some(enable) = req.enable { task.enable = enable; }
            if let Some(note) = req.note { task.note = note; }
            if let Some(overrides) = req.overrides { task.overrides = overrides; }
            conn.execute(
                "UPDATE tasks SET name=?1, url=?2, filename=?3, enable=?4, note=?5, overrides=?6 WHERE id=?7",
                params![
                    task.name, task.url, task.filename, task.enable, task.note,
                    serde_json::to_string(&task.overrides)?, id.to_string(),
                ],
            )?;
            Ok(task)
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
        Ok(Json(ApiResponse::ok(task)))
}

pub async fn delete_task(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<()> {
    state
        .with_transaction(|conn| {
            scheduler::cancel_task(conn, id)?;
            conn.execute("DELETE FROM tasks WHERE id = ?1", params![id.to_string()])?;
            Ok(())
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
        Ok(Json(ApiResponse::ok(())))
}

// ── 运行记录 ──────────────────────────────────────────────

#[derive(Deserialize)]
pub struct RunQuery {
    #[serde(default = "default_limit")]
    pub limit: i64,
    pub node_id: Option<Uuid>,
}

fn default_limit() -> i64 {
    200
}

pub async fn list_runs(
    State(state): State<Arc<AppState>>,
    Query(q): Query<RunQuery>,
) -> ApiResult<Vec<RunRecord>> {
    let runs = state
        .with_conn(|conn| {
            let runs: Vec<RunRecord> = if let Some(nid) = q.node_id {
                let mut stmt = conn.prepare("SELECT id, task_id, dispatch_id, node_id, task_name, url, filename, file_size, downloaded_bytes, elapsed_secs, avg_speed_mbps, status, success_chunks, failed_chunks, error_msg, timestamp FROM runs WHERE node_id = ?1 ORDER BY timestamp DESC LIMIT ?2")?;
                let rows: Vec<RunRecord> = stmt.query_map(params![nid.to_string(), q.limit], map_run)?.filter_map(|r| r.ok()).collect();
                rows
            } else {
                let mut stmt = conn.prepare("SELECT id, task_id, dispatch_id, node_id, task_name, url, filename, file_size, downloaded_bytes, elapsed_secs, avg_speed_mbps, status, success_chunks, failed_chunks, error_msg, timestamp FROM runs ORDER BY timestamp DESC LIMIT ?1")?;
                let rows: Vec<RunRecord> = stmt.query_map(params![q.limit], map_run)?.filter_map(|r| r.ok()).collect();
                rows
            };
            Ok(runs)
        })
        .await?;
    Ok(Json(ApiResponse::ok(runs)))
}

fn map_run(r: &rusqlite::Row) -> rusqlite::Result<RunRecord> {
    Ok(RunRecord {
        id: r.get::<_, String>(0)?.parse().unwrap(),
        task_id: r.get::<_, Option<String>>(1)?.and_then(|s| s.parse().ok()),
        dispatch_id: r.get::<_, Option<String>>(2)?.and_then(|s| s.parse().ok()),
        node_id: r.get::<_, String>(3)?.parse().unwrap(),
        task_name: r.get(4)?,
        url: r.get(5)?,
        filename: r.get(6)?,
        file_size: r.get(7)?,
        downloaded_bytes: r.get(8)?,
        elapsed_secs: r.get(9)?,
        avg_speed_mbps: r.get(10)?,
        status: r.get(11)?,
        success_chunks: r.get(12)?,
        failed_chunks: r.get(13)?,
        error_msg: r.get(14)?,
        timestamp: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(15)?).ok().map(|dt| dt.with_timezone(&Utc)).unwrap_or(Utc::now()),
    })
}

// ── 总览 / 默认配置 ───────────────────────────────────────

pub async fn get_overview(State(state): State<Arc<AppState>>) -> ApiResult<Overview> {
    state.refresh_online().await;
    let ov = state.with_conn(|conn| scheduler::overview(conn)).await?;
    Ok(Json(ApiResponse::ok(ov)))
}

/// 版本信息接口（pcdn-keeper 场景下返回 pk + spde 组合版本）
#[derive(Debug, Serialize)]
pub struct VersionInfo {
    pub pk_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spde_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pcdn_keeper_version: Option<String>,
}

pub async fn get_version() -> ApiResult<VersionInfo> {
    let info = VersionInfo {
        pk_version: env!("CARGO_PKG_VERSION").to_string(),
        spde_version: std::env::var("SPDE_VERSION").ok(),
        pcdn_keeper_version: std::env::var("PCDN_KEEPER_VERSION").ok(),
    };
    Ok(Json(ApiResponse::ok(info)))
}

pub async fn get_defaults(State(state): State<Arc<AppState>>) -> ApiResult<SpdeDefaults> {
    Ok(Json(ApiResponse::ok(state.cfg.spde_defaults.clone())))
}

/// 节点拉取 YAML 格式配置（SPDE 节点调用）
pub async fn get_node_config_yaml(
    State(state): State<Arc<AppState>>,
    Path(node_id): Path<Uuid>,
) -> impl IntoResponse {
    let master_url = format!("http://{}", state.cfg.listen);
    let heartbeat_interval = state.cfg.heartbeat_timeout_secs / 3;
    let yaml = spde_cfg::render_config(&state.cfg.spde_defaults, &[], &master_url, node_id, heartbeat_interval);
    ([("content-type", "application/yaml; charset=utf-8")], yaml).into_response()
}

// ── Agent 接口（节点调用） ────────────────────────────────

/// 判断是否为内部地址（本地回环或内网IP）
fn is_internal_addr(ip: &std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            v4.is_loopback() || v4.is_private() || v4.is_link_local()
        }
        std::net::IpAddr::V6(v6) => {
            v6.is_loopback() || v6.is_unspecified()
        }
    }
}

pub async fn agent_register(
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    Json(mut req): Json<AgentRegisterReq>,
) -> ApiResult<AgentRegisterResp> {
    let node_id = req.node_id.unwrap_or_else(Uuid::new_v4);
    let now = Utc::now();

    // 自动标记内部节点：本地回环或内网IP连接的agent
    if is_internal_addr(&addr.ip()) && !req.labels.iter().any(|l| l == "internal=true") {
        req.labels.push("internal=true".to_string());
    }
    state
        .with_transaction(|conn| {
            // 检查该节点是否被删除过（删除过的节点再次注册需要审批）
            let was_deleted: bool = conn
                .query_row("SELECT COUNT(*) FROM deleted_nodes WHERE node_id = ?1", params![node_id.to_string()], |r| r.get::<_, i64>(0))
                .map(|c| c > 0)
                .unwrap_or(false);

            let exists: bool = conn
                .query_row("SELECT COUNT(*) FROM nodes WHERE id = ?1", params![node_id.to_string()], |r| r.get::<_, i64>(0))
                .map(|c| c > 0)
                .unwrap_or(false);

            if exists {
                // 已存在的节点：pending/busy 状态保持不变，其他更新为 online
                let current_status: String = conn
                    .query_row("SELECT status FROM nodes WHERE id = ?1", params![node_id.to_string()], |r| r.get(0))
                    .unwrap_or_else(|_| "online".to_string());
                let new_status = match current_status.as_str() {
                    "pending" | "busy" => current_status.as_str(),
                    _ => "online",
                };
                conn.execute(
                    "UPDATE nodes SET hostname=?1, platform=?2, arch=?3, version=?4, status=?5, last_seen=?6, labels=?7 WHERE id=?8",
                    params![req.hostname, req.platform, req.arch, req.version, new_status, now.to_rfc3339(), serde_json::to_string(&req.labels)?, node_id.to_string()],
                )?;
            } else {
                // 新节点：被删过的状态为 pending（待审批），否则为 online（自动通过）
                let init_status = if was_deleted { "pending" } else { "online" };
                conn.execute(
                    "INSERT INTO nodes VALUES (?1,?2,?3,?4,?5,?6,?7,?7,?8,0,0,NULL)",
                    params![node_id.to_string(), req.hostname, req.platform, req.arch, req.version, init_status, now.to_rfc3339(), serde_json::to_string(&req.labels)?],
                )?;
                if was_deleted {
                    tracing::info!("节点 {} 曾被删除，再次注册标记为 pending 待审批", node_id);
                }
            }
            Ok(())
        })
        .await?;

    state.frontend_ws_mgr.notify_update();
    Ok(Json(ApiResponse::ok(AgentRegisterResp {
        node_id,
        poll_interval_secs: state.cfg.heartbeat_timeout_secs / 2,
        master_listen: state.cfg.listen.clone(),
    })))
}

pub async fn agent_heartbeat(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AgentHeartbeatReq>,
) -> ApiResult<()> {
    let now = Utc::now();
    state
        .with_transaction(|conn| {
            // 节点不存在时直接忽略（被删除的节点不会通过心跳自动恢复，必须重新 register）
            let exists: bool = conn
                .query_row("SELECT COUNT(*) FROM nodes WHERE id = ?1", params![req.node_id.to_string()], |r| r.get::<_, i64>(0))
                .map(|c| c > 0)
                .unwrap_or(false);
            if !exists {
                return Ok(());
            }
            // pending 状态的节点保持 pending（待审批），只更新 last_seen，不改为 online
            let current_status: String = conn
                .query_row("SELECT status FROM nodes WHERE id = ?1", params![req.node_id.to_string()], |r| r.get(0))
                .unwrap_or_else(|_| "online".to_string());
            if current_status == "pending" {
                conn.execute(
                    "UPDATE nodes SET last_seen=?1, active_tasks=?2, bytes_downloaded=?3 WHERE id=?4",
                    params![now.to_rfc3339(), req.active_tasks, req.bytes_downloaded, req.node_id.to_string()],
                )?;
            } else {
                // 根据活跃任务数和最大并发上限决定状态：达到上限=busy，否则=online
                let max_concurrent = state.cfg.spde_defaults.max_concurrent;
                let new_status = if req.active_tasks >= max_concurrent { "busy" } else { "online" };
                conn.execute(
                    "UPDATE nodes SET status=?1, last_seen=?2, active_tasks=?3, bytes_downloaded=?4 WHERE id=?5",
                    params![new_status, now.to_rfc3339(), req.active_tasks, req.bytes_downloaded, req.node_id.to_string()],
                )?;
            }
            Ok(())
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
        Ok(Json(ApiResponse::ok(())))
}

pub async fn agent_fetch_config(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AgentFetchReq>,
) -> ApiResult<NodeConfig> {
    let now = Utc::now();
    state
        .with_transaction(|conn| {
            // 节点不存在时直接忽略（不自动创建，必须通过 register 注册）
            let exists: bool = conn
                .query_row("SELECT COUNT(*) FROM nodes WHERE id = ?1", params![req.node_id.to_string()], |r| r.get::<_, i64>(0))
                .map(|c| c > 0)
                .unwrap_or(false);
            if !exists {
                return Ok(());
            }
            // pending/busy 状态的节点保持不变，只更新 last_seen，不改为 online
            let current_status: String = conn
                .query_row("SELECT status FROM nodes WHERE id = ?1", params![req.node_id.to_string()], |r| r.get(0))
                .unwrap_or_else(|_| "online".to_string());
            if current_status == "pending" || current_status == "busy" {
                conn.execute("UPDATE nodes SET last_seen=?1 WHERE id=?2", params![now.to_rfc3339(), req.node_id.to_string()])?;
            } else {
                conn.execute("UPDATE nodes SET status='online', last_seen=?1 WHERE id=?2", params![now.to_rfc3339(), req.node_id.to_string()])?;
            }
            Ok(())
        })
        .await?;

    // 拉模式：节点通过 /agent/claim 领取任务，这里返回空配置
    let cfg = NodeConfig {
        dispatch_id: Uuid::nil(),
        master: format!("http://127.0.0.1:{}", state.cfg.listen.split(':').last().unwrap_or("5566")),
        token: if state.cfg.token.is_empty() { None } else { Some(state.cfg.token.clone()) },
        tasks: vec![],
    };
    state.frontend_ws_mgr.notify_update();
        Ok(Json(ApiResponse::ok(cfg)))
}

pub async fn agent_report(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AgentReportReq>,
) -> ApiResult<RunRecord> {
    let rec = state
        .with_transaction(|conn| scheduler::apply_report(conn, &req))
        .await?;
    state.frontend_ws_mgr.notify_update();
        Ok(Json(ApiResponse::ok(rec)))
}

/// 节点从共享待下发池领取一个任务
pub async fn agent_claim(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AgentClaimReq>,
) -> Response {
    let result = state
        .with_transaction(|conn| scheduler::claim_task(conn, req.node_id))
        .await;

    match result {
        Ok(Some(node_task)) => {
            let resp = ClaimTaskResp {
                dispatch_id: node_task.dispatch_id,
                task_id: node_task.task.id,
                name: node_task.task.name,
                url: node_task.task.url,
                filename: node_task.task.filename,
                overrides: node_task.task.overrides,
            };
             state.frontend_ws_mgr.notify_update();
                         (StatusCode::OK, Json(ApiResponse::ok(resp))).into_response()
        }
        Ok(None) => {
            // 池子空，返回 204
            (StatusCode::NO_CONTENT, Json(ApiResponse::<()>::ok(()))).into_response()
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::err(e.to_string()))).into_response()
        }
    }
}

// ── 工作流管理 ────────────────────────────────────────────

pub async fn list_workflows(State(state): State<Arc<AppState>>) -> ApiResult<Vec<Workflow>> {
    let wfs = state
        .with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT id, name, enable, schedule, task_ids, target, node_ids, next_run_at, last_run_at, last_run_status, created_at FROM workflows ORDER BY created_at DESC")?;
            let wfs: Vec<Workflow> = stmt.query_map([], map_workflow)?.filter_map(|r| r.ok()).collect();
            Ok(wfs)
        })
        .await?;
    Ok(Json(ApiResponse::ok(wfs)))
}

fn map_workflow(r: &rusqlite::Row) -> rusqlite::Result<Workflow> {
    let schedule_str: String = r.get(3)?;
    let task_ids_str: String = r.get(4)?;
    let target_str: String = r.get(5)?;
    let node_ids_str: String = r.get(6)?;
    Ok(Workflow {
        id: r.get::<_, String>(0)?.parse().unwrap(),
        name: r.get(1)?,
        enable: r.get::<_, i64>(2)? != 0,
        schedule: serde_json::from_str(&schedule_str).unwrap_or(WorkflowSchedule::Once { at: Utc::now() }),
        task_ids: serde_json::from_str(&task_ids_str).unwrap_or_default(),
        target: match target_str.as_str() {
            "all" => AssignmentTarget::All,
            "nodes" => AssignmentTarget::Nodes,
            _ => AssignmentTarget::Any,
        },
        node_ids: serde_json::from_str(&node_ids_str).unwrap_or_default(),
        next_run_at: r.get::<_, Option<String>>(7)?.and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok().map(|dt| dt.with_timezone(&Utc))),
        last_run_at: r.get::<_, Option<String>>(8)?.and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok().map(|dt| dt.with_timezone(&Utc))),
        last_run_status: r.get(9)?,
        created_at: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(10)?).ok().map(|dt| dt.with_timezone(&Utc)).unwrap_or(Utc::now()),
    })
}

pub async fn create_workflow(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateWorkflowReq>,
) -> ApiResult<Workflow> {
    let now = Utc::now();
    let next_run = workflow_scheduler::compute_next_run(&req.schedule, None, now);
    let wf = Workflow {
        id: Uuid::new_v4(),
        name: req.name,
        enable: req.enable,
        schedule: req.schedule,
        task_ids: req.task_ids,
        target: req.target,
        node_ids: req.node_ids,
        next_run_at: next_run,
        last_run_at: None,
        last_run_status: None,
        created_at: now,
    };
    state
        .with_transaction(|conn| {
            conn.execute(
                "INSERT INTO workflows VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                params![
                    wf.id.to_string(), wf.name, wf.enable,
                    serde_json::to_string(&wf.schedule)?,
                    serde_json::to_string(&wf.task_ids)?,
                    serde_json::to_string(&wf.target)?,
                    serde_json::to_string(&wf.node_ids)?,
                    wf.next_run_at.map(|t| t.to_rfc3339()),
                    None::<String>, None::<String>,
                    wf.created_at.to_rfc3339(),
                ],
            )?;
            Ok(())
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
        Ok(Json(ApiResponse::ok(wf)))
}

pub async fn update_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    Json(req): Json<UpdateWorkflowReq>,
) -> ApiResult<Workflow> {
    let wf = state
        .with_transaction(|conn| {
            let existing: Option<Workflow> = conn
                .query_row("SELECT id, name, enable, schedule, task_ids, target, node_ids, next_run_at, last_run_at, last_run_status, created_at FROM workflows WHERE id = ?1", params![id.to_string()], map_workflow)
                .ok();
            let Some(mut wf) = existing else {
                return Err(anyhow::anyhow!("工作流不存在"));
            };
            if let Some(name) = req.name { wf.name = name; }
            if let Some(enable) = req.enable { wf.enable = enable; }
            if let Some(schedule) = req.schedule {
                wf.schedule = schedule;
                wf.next_run_at = workflow_scheduler::compute_next_run(&wf.schedule, None, Utc::now());
            }
            if let Some(task_ids) = req.task_ids { wf.task_ids = task_ids; }
            if let Some(target) = req.target { wf.target = target; }
            if let Some(node_ids) = req.node_ids { wf.node_ids = node_ids; }
            conn.execute(
                "UPDATE workflows SET name=?1, enable=?2, schedule=?3, task_ids=?4, target=?5, node_ids=?6, next_run_at=?7 WHERE id=?8",
                params![
                    wf.name, wf.enable,
                    serde_json::to_string(&wf.schedule)?,
                    serde_json::to_string(&wf.task_ids)?,
                    serde_json::to_string(&wf.target)?,
                    serde_json::to_string(&wf.node_ids)?,
                    wf.next_run_at.map(|t| t.to_rfc3339()),
                    id.to_string(),
                ],
            )?;
            Ok(wf)
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
        Ok(Json(ApiResponse::ok(wf)))
}

pub async fn delete_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<()> {
    state
        .with_transaction(|conn| {
            conn.execute("DELETE FROM workflow_runs WHERE workflow_id = ?1", params![id.to_string()])?;
            conn.execute("DELETE FROM workflows WHERE id = ?1", params![id.to_string()])?;
            Ok(())
        })
        .await?;
    state.frontend_ws_mgr.notify_update();
        Ok(Json(ApiResponse::ok(())))
}

pub async fn get_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<WorkflowDetail> {
    let wf = state
        .with_conn(|conn| {
            let wf = conn
                .query_row("SELECT id, name, enable, schedule, task_ids, target, node_ids, next_run_at, last_run_at, last_run_status, created_at FROM workflows WHERE id = ?1", params![id.to_string()], map_workflow)
                .ok();
            Ok(wf)
        })
        .await?;
    let Some(wf) = wf else {
        return Err(AppError(anyhow::anyhow!("工作流不存在")));
    };

    let runs = state
        .with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT id, workflow_id, workflow_name, triggered_at, status, task_count, success_count, failed_count, dispatch_ids, error_msg FROM workflow_runs WHERE workflow_id = ?1 ORDER BY triggered_at DESC LIMIT 50")?;
            let runs: Vec<WorkflowRun> = stmt.query_map(params![id.to_string()], map_workflow_run)?.filter_map(|r| r.ok()).collect();
            Ok(runs)
        })
        .await?;

    Ok(Json(ApiResponse::ok(WorkflowDetail { workflow: wf, runs })))
}

fn map_workflow_run(r: &rusqlite::Row) -> rusqlite::Result<WorkflowRun> {
    let dispatch_ids_str: String = r.get(8)?;
    Ok(WorkflowRun {
        id: r.get::<_, String>(0)?.parse().unwrap(),
        workflow_id: r.get::<_, String>(1)?.parse().unwrap(),
        workflow_name: r.get(2)?,
        triggered_at: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(3)?).ok().map(|dt| dt.with_timezone(&Utc)).unwrap_or(Utc::now()),
        status: r.get(4)?,
        task_count: r.get(5)?,
        success_count: r.get(6)?,
        failed_count: r.get(7)?,
        dispatch_ids: serde_json::from_str(&dispatch_ids_str).unwrap_or_default(),
        error_msg: r.get(9)?,
    })
}

pub async fn trigger_workflow_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<WorkflowRun> {
    let run = workflow_scheduler::trigger_workflow(&state, id).await?;
    match run {
        state.frontend_ws_mgr.notify_update();
        Some(r) => Ok(Json(ApiResponse::ok(r))),
        None => Err(AppError(anyhow::anyhow!("工作流不存在"))),
    }
}

pub async fn list_workflow_runs(
    State(state): State<Arc<AppState>>,
) -> ApiResult<Vec<WorkflowRun>> {
    let runs = state
        .with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT id, workflow_id, workflow_name, triggered_at, status, task_count, success_count, failed_count, dispatch_ids, error_msg FROM workflow_runs ORDER BY triggered_at DESC LIMIT 100")?;
            let runs: Vec<WorkflowRun> = stmt.query_map([], map_workflow_run)?.filter_map(|r| r.ok()).collect();
            Ok(runs)
        })
        .await?;
    Ok(Json(ApiResponse::ok(runs)))
}

// ── 下发记录（执行页面） ──────────────────────────────────

pub async fn list_dispatches(State(state): State<Arc<AppState>>) -> ApiResult<Vec<Dispatch>> {
    let dispatches = state
        .with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT id, task_id, node_id, state, created_at, updated_at, claimed_at, target, allowed_nodes FROM dispatches ORDER BY created_at DESC LIMIT 500")?;
            let dispatches: Vec<Dispatch> = stmt
                .query_map([], |r| {
                    let target_str: String = r.get(7)?;
                    let allowed_str: String = r.get(8)?;
                    Ok(Dispatch {
                        id: r.get::<_, String>(0)?.parse().unwrap(),
                        task_id: r.get::<_, String>(1)?.parse().unwrap(),
                        node_id: r.get::<_, Option<String>>(2)?.and_then(|s| s.parse().ok()),
                        state: match r.get::<_, String>(3)?.as_str() {
                            "acked" => DispatchState::Acked,
                            "running" => DispatchState::Running,
                            "success" => DispatchState::Success,
                            "failed" => DispatchState::Failed,
                            "cancelled" => DispatchState::Cancelled,
                            _ => DispatchState::Pending,
                        },
                        created_at: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(4)?).ok().map(|dt| dt.with_timezone(&Utc)).unwrap_or(Utc::now()),
                        updated_at: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(5)?).ok().map(|dt| dt.with_timezone(&Utc)).unwrap_or(Utc::now()),
                        claimed_at: r.get::<_, Option<String>>(6)?.and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok().map(|dt| dt.with_timezone(&Utc))),
                        target: match target_str.as_str() {
                            "all" => AssignmentTarget::All,
                            "nodes" => AssignmentTarget::Nodes,
                            _ => AssignmentTarget::Any,
                        },
                        allowed_nodes: serde_json::from_str(&allowed_str).unwrap_or_default(),
                    })
                })?
                .filter_map(|r| r.ok())
                .collect();
            Ok(dispatches)
        })
        .await?;
    Ok(Json(ApiResponse::ok(dispatches)))
}

// ── 二进制分发 ─────────────────────────────────

pub async fn serve_artifact(
    State(state): State<Arc<AppState>>,
    Path(platform): Path<String>,
) -> Response {
    let filename = match artifact_filename(&platform) {
        Some(f) => f,
        None => return (StatusCode::BAD_REQUEST, "不支持的平台").into_response(),
    };
    let file_path = state.artifacts_dir.join(filename);
    if file_path.exists() {
        let content = tokio::fs::read(&file_path).await.unwrap_or_default();
        (
            [("content-type", "application/octet-stream")],
            [("content-disposition", format!("attachment; filename=\"{}\"", filename))],
            content,
        )
            .into_response()
    } else {
        (StatusCode::NOT_FOUND, format!("未找到 {} 平台的二进制", platform)).into_response()
    }
}

pub async fn host_info() -> Json<ApiResponse<HostInfo>> {
    let (platform, arch) = detect_host_platform();
    Json(ApiResponse::ok(HostInfo { platform, arch }))
}

// ── 路由 ──────────────────────────────────────────────────

pub fn router(state: Arc<AppState>) -> axum::Router {
    use axum::routing::{delete, get, post, put};
    axum::Router::new()
        // 总览
        .route("/api/v1/overview", get(get_overview))
        .route("/api/v1/version", get(get_version))
        .route("/api/v1/defaults", get(get_defaults))
        .route("/api/v1/host-info", get(host_info))
        // 节点
        .route("/api/v1/nodes", get(list_nodes).delete(purge_offline_nodes))
        .route("/api/v1/nodes/{id}", delete(delete_node))
        .route("/api/v1/nodes/{id}/approve", post(approve_node))
        .route("/api/v1/nodes/{id}/reject", post(reject_node))
        .route("/api/v1/nodes/{id}/config.yaml", get(get_node_config_yaml))
        .route("/api/v1/nodes/{id}/realtime", get(get_node_realtime))
        // 任务
        .route("/api/v1/tasks", get(list_tasks).post(create_task))
        .route("/api/v1/tasks/{id}", put(update_task).delete(delete_task))
        // 运行记录
        .route("/api/v1/runs", get(list_runs))
        // 下发记录
        .route("/api/v1/dispatches", get(list_dispatches))
        // 工作流
        .route("/api/v1/workflows", get(list_workflows).post(create_workflow))
        .route("/api/v1/workflows/{id}", get(get_workflow).put(update_workflow).delete(delete_workflow))
        .route("/api/v1/workflows/{id}/trigger", post(trigger_workflow_handler))
        .route("/api/v1/workflow-runs", get(list_workflow_runs))
        // Agent
        .route("/api/v1/agent/register", post(agent_register))
        .route("/api/v1/agent/heartbeat", post(agent_heartbeat))
        .route("/api/v1/agent/config", post(agent_fetch_config))
        .route("/api/v1/agent/report", post(agent_report))
        .route("/api/v1/agent/claim", post(agent_claim))
        // WebSocket
        .route("/api/v1/agent/ws", get(ws::ws_handler))
        .route("/api/v1/realtime/ws", get(ws::frontend_ws_handler))
        // 二进制分发
        .route("/api/v1/artifacts/{platform}", get(serve_artifact))
        .with_state(state)
}
