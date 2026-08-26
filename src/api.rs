use crate::models::*;
use crate::scheduler;
use crate::spde_cfg;
use crate::store::{artifact_filename, AppState};
use crate::workflow_scheduler::{self, compute_next_run};
use crate::ws::{self, WsQuery};
use axum::body::Body;
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use uuid::Uuid;

pub fn router(state: Arc<AppState>) -> Router {
    let api = Router::new()
        .route("/overview", get(overview))
        .route("/nodes", get(list_nodes).delete(delete_offline_nodes))
        .route("/nodes/{id}", get(get_node).delete(delete_node))
        .route("/tasks", get(list_tasks).post(create_task))
        .route("/tasks/{id}", get(get_task).patch(patch_task).delete(delete_task))
        .route("/tasks/{id}/cancel", post(cancel_task))
        .route("/workflows", get(list_workflows).post(create_workflow))
        .route(
            "/workflows/{id}",
            get(get_workflow).put(update_workflow).delete(delete_workflow),
        )
        .route("/workflows/{id}/trigger", post(trigger_workflow))
        .route("/workflows/{id}/runs", get(list_workflow_runs))
        .route("/dispatches", get(list_dispatches))
        .route("/runs", get(list_runs))
        .route("/defaults", get(get_defaults).put(put_defaults))
        .route("/artifacts", get(list_artifacts))
        .route("/artifacts/{platform}", get(download_artifact))
        .route("/nodes/{id}/config.yaml", get(node_config))
        .route("/agent/register", post(agent_register))
        .route("/agent/heartbeat", post(agent_heartbeat))
        .route("/agent/report", post(agent_report))
        .route("/agent/ws", get(ws::agent_ws))
        .route("/agent/{id}/ack/{dispatch_id}", post(agent_ack_running));

    Router::new()
        .nest("/api/v1", api)
        .route("/install.sh", get(install_sh))
        .route("/install.ps1", get(install_ps1))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .layer(middleware::from_fn_with_state(state.clone(), auth_mw))
        .with_state(state)
}

async fn auth_mw(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let token = state.cfg.token.trim();
    if token.is_empty() {
        return Ok(next.run(req).await);
    }
    let path = req.uri().path();
    if path == "/"
        || path.starts_with("/assets/")
        || path == "/install.sh"
        || path == "/install.ps1"
        || path.starts_with("/api/v1/artifacts/")
    {
        return Ok(next.run(req).await);
    }
    let ok = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|v| v == format!("Bearer {token}") || v == token)
        .unwrap_or(false);
    if ok {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

type ApiResult<T> = Result<T, (StatusCode, String)>;

fn err(code: StatusCode, msg: impl Into<String>) -> (StatusCode, String) {
    (code, msg.into())
}

fn config_hash(yaml: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(yaml.as_bytes());
    format!("{:x}", hasher.finalize())
}

async fn overview(State(s): State<Arc<AppState>>) -> Json<Overview> {
    s.refresh_online().await;
    let snap = s.snapshot().await;
    Json(scheduler::overview(&snap))
}

async fn list_nodes(State(s): State<Arc<AppState>>) -> Json<Vec<Node>> {
    s.refresh_online().await;
    let mut nodes = s.snapshot().await.nodes;
    nodes.sort_by(|a, b| b.last_seen.cmp(&a.last_seen));
    Json(nodes)
}

async fn get_node(State(s): State<Arc<AppState>>, Path(id): Path<Uuid>) -> ApiResult<Json<Node>> {
    s.refresh_online().await;
    s.snapshot()
        .await
        .nodes
        .into_iter()
        .find(|n| n.id == id)
        .map(Json)
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "node not found"))
}

async fn delete_node(State(s): State<Arc<AppState>>, Path(id): Path<Uuid>) -> ApiResult<StatusCode> {
    s.with_mut(|snap| {
        snap.nodes.retain(|n| n.id != id);
    })
    .await
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_offline_nodes(State(s): State<Arc<AppState>>) -> ApiResult<Json<serde_json::Value>> {
    s.refresh_online().await;
    let n = s
        .with_mut(|snap| {
            let before = snap.nodes.len();
            snap.nodes.retain(|n| n.status != NodeStatus::Offline);
            before - snap.nodes.len()
        })
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({ "removed": n })))
}

// ==================== 任务（任务池） ====================

async fn list_tasks(State(s): State<Arc<AppState>>) -> Json<Vec<Task>> {
    let mut tasks = s.snapshot().await.tasks;
    tasks.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    Json(tasks)
}

async fn get_task(State(s): State<Arc<AppState>>, Path(id): Path<Uuid>) -> ApiResult<Json<Task>> {
    s.snapshot()
        .await
        .tasks
        .into_iter()
        .find(|t| t.id == id)
        .map(Json)
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "task not found"))
}

async fn create_task(
    State(s): State<Arc<AppState>>,
    Json(req): Json<CreateTaskReq>,
) -> ApiResult<Json<Task>> {
    if req.name.trim().is_empty() || req.url.trim().is_empty() || req.filename.trim().is_empty() {
        return Err(err(StatusCode::BAD_REQUEST, "name/url/filename required"));
    }
    let now = Utc::now();
    let task = Task {
        id: Uuid::new_v4(),
        name: req.name,
        url: req.url,
        filename: req.filename,
        enable: req.enable,
        created_at: now,
        note: req.note,
        overrides: req.overrides,
    };
    let out = s
        .with_mut(|snap| {
            snap.tasks.push(task.clone());
            task
        })
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(out))
}

#[derive(Debug, Deserialize)]
pub struct PatchTaskReq {
    pub name: Option<String>,
    pub url: Option<String>,
    pub filename: Option<String>,
    pub enable: Option<bool>,
    pub note: Option<String>,
    pub overrides: Option<TaskOverrides>,
}

async fn patch_task(
    State(s): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    Json(req): Json<PatchTaskReq>,
) -> ApiResult<Json<Task>> {
    let t = s
        .with_mut(|snap| {
            let t = snap.tasks.iter_mut().find(|t| t.id == id)?;
            if let Some(v) = req.name {
                t.name = v;
            }
            if let Some(v) = req.url {
                t.url = v;
            }
            if let Some(v) = req.filename {
                t.filename = v;
            }
            if let Some(v) = req.enable {
                t.enable = v;
            }
            if let Some(v) = req.note {
                t.note = v;
            }
            if let Some(o) = req.overrides {
                t.overrides = o;
            }
            Some(t.clone())
        })
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    ws::notify_config_changed(&s).await;
    t.map(Json)
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "task not found"))
}

#[derive(Debug, Deserialize)]
struct DeleteTaskQuery {
    #[serde(default)]
    delete_file: bool,
}

async fn delete_task(
    State(s): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    Query(q): Query<DeleteTaskQuery>,
) -> ApiResult<StatusCode> {
    let (filename, save_path, node_ids) = {
        let snap = s.snapshot().await;
        let task = snap.tasks.iter().find(|t| t.id == id);
        let fname = task.map(|t| t.filename.clone()).unwrap_or_default();
        let spath = task
            .and_then(|t| t.overrides.save_path.clone())
            .unwrap_or_else(|| s.cfg.spde_defaults.save_path.clone());
        let nids: Vec<Uuid> = snap
            .dispatches
            .iter()
            .filter(|d| d.task_id == id)
            .map(|d| d.node_id)
            .collect();
        (fname, spath, nids)
    };

    s.with_mut(|snap| {
        scheduler::cancel_task(snap, id);
        snap.tasks.retain(|t| t.id != id);
    })
    .await
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if q.delete_file && !filename.is_empty() {
        for nid in &node_ids {
            ws::notify_delete_file(&s, *nid, &filename, &save_path).await;
        }
    }

    ws::notify_config_changed(&s).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn cancel_task(
    State(s): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<serde_json::Value>> {
    s.with_mut(|snap| scheduler::cancel_task(snap, id))
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    ws::notify_config_changed(&s).await;
    Ok(Json(serde_json::json!({ "ok": true })))
}

// ==================== 工作流 ====================

async fn list_workflows(State(s): State<Arc<AppState>>) -> Json<Vec<Workflow>> {
    let mut wfs = s.snapshot().await.workflows;
    wfs.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    Json(wfs)
}

async fn get_workflow(State(s): State<Arc<AppState>>, Path(id): Path<Uuid>) -> ApiResult<Json<Workflow>> {
    s.snapshot()
        .await
        .workflows
        .into_iter()
        .find(|w| w.id == id)
        .map(Json)
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "workflow not found"))
}

async fn create_workflow(
    State(s): State<Arc<AppState>>,
    Json(req): Json<CreateWorkflowReq>,
) -> ApiResult<Json<Workflow>> {
    if req.name.trim().is_empty() {
        return Err(err(StatusCode::BAD_REQUEST, "name required"));
    }
    if req.task_ids.is_empty() {
        return Err(err(StatusCode::BAD_REQUEST, "at least one task required"));
    }
    let now = Utc::now();
    let next_run = if req.enable {
        compute_next_run(&req.schedule, None, now)
    } else {
        None
    };
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
    let out = s
        .with_mut(|snap| {
            snap.workflows.push(wf.clone());
            wf
        })
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(out))
}

async fn update_workflow(
    State(s): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    Json(req): Json<UpdateWorkflowReq>,
) -> ApiResult<Json<Workflow>> {
    let now = Utc::now();
    let wf = s
        .with_mut(|snap| {
            let wf = snap.workflows.iter_mut().find(|w| w.id == id)?;
            if let Some(v) = req.name {
                wf.name = v;
            }
            if let Some(v) = req.enable {
                wf.enable = v;
            }
            if let Some(v) = req.schedule {
                wf.schedule = v;
            }
            if let Some(v) = req.task_ids {
                wf.task_ids = v;
            }
            if let Some(v) = req.target {
                wf.target = v;
            }
            if let Some(v) = req.node_ids {
                wf.node_ids = v;
            }
            // 重新计算下次执行时间
            wf.next_run_at = if wf.enable {
                compute_next_run(&wf.schedule, wf.last_run_at, now)
            } else {
                None
            };
            Some(wf.clone())
        })
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    wf.map(Json)
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "workflow not found"))
}

async fn delete_workflow(
    State(s): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<StatusCode> {
    s.with_mut(|snap| {
        snap.workflows.retain(|w| w.id != id);
        snap.workflow_runs.retain(|r| r.workflow_id != id);
    })
    .await
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn trigger_workflow(
    State(s): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<WorkflowRun>> {
    let run = workflow_scheduler::trigger_workflow(&s, id)
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "workflow not found"))?;

    ws::notify_config_changed(&s).await;
    Ok(Json(run))
}

#[derive(Debug, Deserialize)]
struct WorkflowRunsQuery {
    limit: Option<usize>,
}

async fn list_workflow_runs(
    State(s): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    Query(q): Query<WorkflowRunsQuery>,
) -> ApiResult<Json<Vec<WorkflowRun>>> {
    let exists = s.snapshot().await.workflows.iter().any(|w| w.id == id);
    if !exists {
        return Err(err(StatusCode::NOT_FOUND, "workflow not found"));
    }
    let mut runs: Vec<WorkflowRun> = s
        .snapshot()
        .await
        .workflow_runs
        .into_iter()
        .filter(|r| r.workflow_id == id)
        .collect();
    runs.sort_by(|a, b| b.triggered_at.cmp(&a.triggered_at));
    let limit = q.limit.unwrap_or(100).min(500);
    runs.truncate(limit);
    Ok(Json(runs))
}

// ==================== 其他 ====================

async fn list_dispatches(State(s): State<Arc<AppState>>) -> Json<Vec<Dispatch>> {
    let mut d = s.snapshot().await.dispatches;
    d.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    Json(d)
}

#[derive(Debug, Deserialize)]
struct RunsQuery {
    limit: Option<usize>,
    node_id: Option<Uuid>,
}

async fn list_runs(
    State(s): State<Arc<AppState>>,
    Query(q): Query<RunsQuery>,
) -> Json<Vec<RunRecord>> {
    let mut runs = s.snapshot().await.runs;
    if let Some(nid) = q.node_id {
        runs.retain(|r| r.node_id == nid);
    }
    runs.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    let limit = q.limit.unwrap_or(200).min(2000);
    runs.truncate(limit);
    Json(runs)
}

async fn get_defaults(State(s): State<Arc<AppState>>) -> Json<crate::config::SpdeDefaults> {
    Json(s.cfg.spde_defaults.clone())
}

async fn put_defaults(
    State(s): State<Arc<AppState>>,
    Json(_req): Json<crate::config::SpdeDefaults>,
) -> ApiResult<Json<serde_json::Value>> {
    let _ = s;
    Err(err(
        StatusCode::NOT_IMPLEMENTED,
        "edit pk config.yaml spde_defaults and restart",
    ))
}

async fn list_artifacts(State(s): State<Arc<AppState>>) -> Json<Vec<serde_json::Value>> {
    let mut out = Vec::new();
    let platforms = [
        "windows-x86_64",
        "linux-x86_64",
        "linux-aarch64",
        "macos-x86_64",
        "macos-aarch64",
    ];
    for p in platforms {
        if let Some(name) = artifact_filename(p) {
            let path = s.artifacts_dir.join(name);
            let present = path.exists();
            let size = if present {
                std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0)
            } else {
                0
            };
            out.push(serde_json::json!({
                "platform": p,
                "filename": name,
                "present": present,
                "size": size,
            }));
        }
    }
    Json(out)
}

async fn download_artifact(
    State(s): State<Arc<AppState>>,
    Path(platform): Path<String>,
) -> ApiResult<Response> {
    let name = artifact_filename(&platform)
        .ok_or_else(|| err(StatusCode::BAD_REQUEST, "unknown platform"))?;
    let path = s.artifacts_dir.join(name);
    if !path.exists() {
        return Err(err(
            StatusCode::NOT_FOUND,
            format!("artifact missing: put {name} into pk-data/artifacts/"),
        ));
    }
    let bytes = tokio::fs::read(&path)
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        "application/octet-stream".parse().unwrap(),
    );
    headers.insert(
        header::CONTENT_DISPOSITION,
        format!("attachment; filename=\"{name}\"").parse().unwrap(),
    );
    Ok((headers, bytes).into_response())
}

async fn node_config(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> ApiResult<Response> {
    let snap = s.snapshot().await;
    if !snap.nodes.iter().any(|n| n.id == id) {
        return Err(err(StatusCode::NOT_FOUND, "node not found"));
    }
    let master = public_base(&headers);
    let tasks = scheduler::active_tasks_for_node(&snap, id);
    let yaml = spde_cfg::render_config(&s.cfg.spde_defaults, &tasks, &master, id, 5);
    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        header::CONTENT_TYPE,
        "text/yaml; charset=utf-8".parse().unwrap(),
    );
    Ok((resp_headers, yaml).into_response())
}

async fn agent_register(
    State(s): State<Arc<AppState>>,
    Json(req): Json<AgentRegisterReq>,
) -> ApiResult<Json<AgentRegisterResp>> {
    let now = Utc::now();
    let node_id = req.node_id.unwrap_or_else(Uuid::new_v4);
    s.with_mut(|snap| {
        if let Some(n) = snap.nodes.iter_mut().find(|n| n.id == node_id) {
            n.hostname = req.hostname.clone();
            n.platform = req.platform.clone();
            n.arch = req.arch.clone();
            n.version = req.version.clone();
            n.status = NodeStatus::Online;
            n.last_seen = now;
            n.labels = req.labels.clone();
        } else {
            snap.nodes.push(Node {
                id: node_id,
                hostname: req.hostname.clone(),
                platform: req.platform.clone(),
                arch: req.arch,
                version: req.version,
                status: NodeStatus::Online,
                last_seen: now,
                registered_at: now,
                labels: req.labels,
                active_tasks: 0,
                bytes_downloaded: 0,
                last_error: None,
            });
        }
    })
    .await
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(AgentRegisterResp {
        node_id,
        poll_interval_secs: 5,
        master_listen: s.cfg.listen.clone(),
    }))
}

async fn agent_heartbeat(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<AgentHeartbeatReq>,
) -> ApiResult<Json<AgentHeartbeatResp>> {
    s.refresh_online().await;
    let now = Utc::now();
    let node_id = req.node_id;
    let master = public_base(&headers);

    let (exists, status) = s
        .with_mut(|snap| {
            let status = if req.busy {
                NodeStatus::Busy
            } else {
                NodeStatus::Online
            };
            if let Some(n) = snap.nodes.iter_mut().find(|n| n.id == node_id) {
                n.last_seen = now;
                n.active_tasks = req.active_tasks;
                n.bytes_downloaded = req.bytes_downloaded;
                n.last_error = req.last_error.clone();
                n.status = status.clone();
                (true, status)
            } else {
                (false, status)
            }
        })
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if !exists {
        return Err(err(StatusCode::NOT_FOUND, "node not registered"));
    }

    let snap = s.snapshot().await;
    let tasks = scheduler::active_tasks_for_node(&snap, node_id);
    let yaml = spde_cfg::render_config(&s.cfg.spde_defaults, &tasks, &master, node_id, 5);
    let config_version = config_hash(&yaml);
    let config_path = format!("/api/v1/nodes/{}/config.yaml", node_id);

    Ok(Json(AgentHeartbeatResp {
        status,
        config_version,
        config_path,
    }))
}

async fn agent_report(
    State(s): State<Arc<AppState>>,
    Json(req): Json<AgentReportReq>,
) -> ApiResult<Json<RunRecord>> {
    let rec = s
        .with_mut(|snap| scheduler::apply_report(snap, req))
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    ws::notify_config_changed(&s).await;
    Ok(Json(rec))
}

async fn agent_ack_running(
    State(s): State<Arc<AppState>>,
    Path((_id, dispatch_id)): Path<(Uuid, Uuid)>,
) -> ApiResult<StatusCode> {
    s.with_mut(|snap| scheduler::mark_running(snap, dispatch_id))
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

fn public_base(headers: &HeaderMap) -> String {
    let host = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get(header::HOST))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("127.0.0.1:5566");
    let proto = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("http");
    format!("{proto}://{host}")
}

async fn install_sh(headers: HeaderMap) -> impl IntoResponse {
    let base = public_base(&headers);
    let body = format!(
        r#"#!/bin/sh
set -e
BASE="{base}"
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
case "$OS-$ARCH" in
  linux-x86_64) PLAT=linux-x86_64 ;;
  linux-amd64) PLAT=linux-x86_64 ;;
  linux-aarch64|linux-arm64) PLAT=linux-aarch64 ;;
  darwin-x86_64) PLAT=macos-x86_64 ;;
  darwin-arm64) PLAT=macos-aarch64 ;;
  *) echo "unsupported $OS $ARCH"; exit 1 ;;
esac
mkdir -p spde-node/bin
curl -fsSL "$BASE/api/v1/artifacts/$PLAT" -o spde-node/bin/spde
chmod +x spde-node/bin/spde
echo "SPDE downloaded. Run:"
echo "  ./spde-node/bin/spde agent --master $BASE"
"#
    );
    (
        [(header::CONTENT_TYPE, "text/x-shellscript; charset=utf-8")],
        body,
    )
}

async fn install_ps1(headers: HeaderMap) -> impl IntoResponse {
    let base = public_base(&headers);
    let body = format!(
        r#"$ErrorActionPreference = "Stop"
$Base = "{base}"
$Plat = "windows-x86_64"
New-Item -ItemType Directory -Force -Path "spde-node\bin" | Out-Null
Invoke-WebRequest -Uri "$Base/api/v1/artifacts/$Plat" -OutFile "spde-node\bin\spde.exe"
Write-Host "SPDE downloaded. Run:"
Write-Host "  .\spde-node\bin\spde.exe agent --master $Base"
"#
    );
    ([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], body)
}

pub async fn merge_web(app: Router) -> Router {
    crate::web::mount(app)
}

#[allow(dead_code)]
fn _body(_: Body) {}

#[allow(dead_code)]
fn _ws_query_check(_: WsQuery) {}
