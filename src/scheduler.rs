use crate::models::*;
use anyhow::Result;
use chrono::Utc;
use rusqlite::{params, Connection};
use uuid::Uuid;

/// 分配给某节点的任务（含 dispatch_id，用于生成 config 和回报告）
#[derive(Debug, Clone)]
pub struct NodeTask {
    pub dispatch_id: Uuid,
    pub task: Task,
}

/// 创建一条待下发 dispatch（进入共享待下发池，不绑定节点）
pub fn create_pending_dispatch(
    conn: &Connection,
    task_id: Uuid,
    target: &AssignmentTarget,
    allowed_nodes: &[Uuid],
) -> Result<Option<Uuid>> {
    // 检查任务存在且启用
    let task_exists: bool = conn
        .query_row(
            "SELECT enable FROM tasks WHERE id = ?1",
            params![task_id.to_string()],
            |r| r.get::<_, i64>(0),
        )
        .map(|v| v != 0)
        .unwrap_or(false);
    if !task_exists {
        return Ok(None);
    }

    let now = Utc::now().to_rfc3339();
    let dispatch_id = Uuid::new_v4();
    conn.execute(
        "INSERT INTO dispatches VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
        params![
            dispatch_id.to_string(),
            task_id.to_string(),
            None::<String>, // node_id
            "pending",
            now,
            now,
            None::<String>, // claimed_at
            serde_json::to_string(target)?,
            serde_json::to_string(allowed_nodes)?,
        ],
    )?;
    Ok(Some(dispatch_id))
}

/// 执行工作流：按 task_ids 次序依次下发每个任务，返回运行记录
pub fn execute_workflow(conn: &Connection, wf: &Workflow) -> Result<WorkflowRun> {
    let now = Utc::now();
    let now_str = now.to_rfc3339();
    let mut dispatch_ids = Vec::new();
    let mut task_count = 0u32;

    for &task_id in &wf.task_ids {
        if let Some(did) = create_pending_dispatch(conn, task_id, &wf.target, &wf.node_ids)? {
            task_count += 1;
            dispatch_ids.push(did);
        }
    }

    let run = WorkflowRun {
        id: Uuid::new_v4(),
        workflow_id: wf.id,
        workflow_name: wf.name.clone(),
        triggered_at: now,
        status: if task_count > 0 {
            "running".into()
        } else {
            "failed".into()
        },
        task_count,
        success_count: 0,
        failed_count: 0,
        dispatch_ids: dispatch_ids.clone(),
        error_msg: if task_count == 0 {
            Some("任务全部禁用或不存在".into())
        } else {
            None
        },
    };

    conn.execute(
        "INSERT INTO workflow_runs VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        params![
            run.id.to_string(),
            run.workflow_id.to_string(),
            run.workflow_name,
            now_str,
            run.status,
            run.task_count,
            run.success_count,
            run.failed_count,
            serde_json::to_string(&dispatch_ids)?,
            run.error_msg,
        ],
    )?;

    Ok(run)
}

/// 节点从共享待下发池领取一个任务（原子操作，单进程天然串行）
pub fn claim_task(conn: &Connection, node_id: Uuid) -> Result<Option<NodeTask>> {
    // 节点必须是 online 状态（pending 待审批节点不能领取任务）
    let node_online: bool = conn
        .query_row(
            "SELECT status FROM nodes WHERE id = ?1",
            params![node_id.to_string()],
            |r| r.get::<_, String>(0),
        )
        .map(|s| s == "online")
        .unwrap_or(false);
    if !node_online {
        return Ok(None);
    }

    let now = Utc::now().to_rfc3339();

    // 找到最早的 Pending 且该节点有权限领取的 dispatch
    let mut stmt = conn.prepare("SELECT id, task_id, target, allowed_nodes FROM dispatches WHERE state = 'pending' AND node_id IS NULL ORDER BY created_at ASC")?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
        ))
    })?;

    for row in rows {
        let (did_str, tid_str, target_str, allowed_str) = row?;
        let target: AssignmentTarget = serde_json::from_str(&target_str)?;
        let allowed: Vec<Uuid> = serde_json::from_str(&allowed_str).unwrap_or_default();

        let can_claim = match target {
            AssignmentTarget::Any | AssignmentTarget::All => true,
            AssignmentTarget::Nodes => allowed.contains(&node_id),
        };
        if !can_claim {
            continue;
        }

        // 领取：绑定节点，状态改为 running
        conn.execute(
            "UPDATE dispatches SET node_id = ?1, state = 'running', claimed_at = ?2, updated_at = ?2 WHERE id = ?3",
            params![node_id.to_string(), now, did_str],
        )?;

        let dispatch_id: Uuid = did_str.parse()?;
        let _task_id: Uuid = tid_str.parse()?;

        // 获取任务详情
        let task = conn.query_row(
            "SELECT id, name, url, filename, enable, created_at, note, overrides FROM tasks WHERE id = ?1",
            params![tid_str],
            |r| {
                let overrides_str: String = r.get(7)?;
                Ok(Task {
                    id: r.get::<_, String>(0)?.parse().unwrap(),
                    name: r.get(1)?,
                    url: r.get(2)?,
                    filename: r.get(3)?,
                    enable: r.get::<_, i64>(4)? != 0,
                    created_at: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(5)?).map(|dt| dt.with_timezone(&Utc)).unwrap_or(Utc::now()),
                    note: r.get(6)?,
                    overrides: serde_json::from_str(&overrides_str).unwrap_or_default(),
                })
            },
        ).ok();

        if let Some(task) = task {
            return Ok(Some(NodeTask { dispatch_id, task }));
        }
    }

    Ok(None)
}

/// 超时回收：Running 状态的 dispatch 如果节点超时未上报，回收到待下发池
pub fn reclaim_timeout_tasks(conn: &Connection, timeout_secs: i64) -> Result<()> {
    let now = Utc::now();
    let cutoff = (now - chrono::Duration::seconds(timeout_secs)).to_rfc3339();
    let cutoff2x = (now - chrono::Duration::seconds(timeout_secs * 2)).to_rfc3339();

    // 节点离线 或 超时过久（2倍超时），回收到待下发池
    conn.execute(
        "UPDATE dispatches SET state = 'pending', node_id = NULL, claimed_at = NULL, updated_at = ?1
         WHERE state = 'running' AND claimed_at IS NOT NULL AND (
            claimed_at < ?2
            OR (claimed_at < ?3 AND node_id IN (SELECT id FROM nodes WHERE status = 'offline'))
         )",
        params![now.to_rfc3339(), cutoff2x, cutoff],
    )?;
    Ok(())
}

/// 修复卡住的工作流运行记录：状态为 running 但所有关联 dispatch 都已是终态
pub fn fix_stuck_workflow_runs(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare("SELECT id, workflow_id, dispatch_ids, task_count FROM workflow_runs WHERE status = 'running'")?;
    let runs: Vec<(String, String, String, i64)> = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

    for (wr_id, wf_id, dispatch_ids_str, _task_count) in runs {
        let dispatch_ids: Vec<Uuid> = serde_json::from_str(&dispatch_ids_str).unwrap_or_default();
        if dispatch_ids.is_empty() {
            continue;
        }

        let mut success = 0u32;
        let mut failed = 0u32;
        let mut pending_or_running = 0u32;

        for did in &dispatch_ids {
            let state: Option<String> = conn
                .query_row(
                    "SELECT state FROM dispatches WHERE id = ?1",
                    params![did.to_string()],
                    |r| r.get(0),
                )
                .ok();
            match state.as_deref() {
                Some("success") => success += 1,
                Some("failed") | Some("cancelled") | None => failed += 1,
                _ => pending_or_running += 1,
            }
        }

        if pending_or_running > 0 {
            continue;
        }

        let status = if failed == 0 {
            "success"
        } else if success == 0 {
            "failed"
        } else {
            "partial"
        };

        conn.execute(
            "UPDATE workflow_runs SET status = ?1, success_count = ?2, failed_count = ?3 WHERE id = ?4",
            params![status, success, failed, wr_id],
        )?;

        // 同步更新工作流的 last_run_status（只更新最近一次运行）
        let last_status: Option<String> = conn
            .query_row(
                "SELECT status FROM workflow_runs WHERE workflow_id = ?1 ORDER BY triggered_at DESC LIMIT 1",
                params![wf_id],
                |r| r.get(0),
            )
            .ok();
        if let Some(s) = last_status {
            conn.execute(
                "UPDATE workflows SET last_run_status = ?1 WHERE id = ?2",
                params![s, wf_id],
            )?;
        }
    }

    Ok(())
}

/// 取出该节点当前正在执行的任务（用于节点重启后恢复，只返回 Running）
pub fn active_tasks_for_node(conn: &Connection, node_id: Uuid) -> Result<Vec<NodeTask>> {
    let mut stmt = conn.prepare(
        "SELECT d.id, d.task_id, t.name, t.url, t.filename, t.enable, t.created_at, t.note, t.overrides
         FROM dispatches d JOIN tasks t ON d.task_id = t.id
         WHERE d.node_id = ?1 AND d.state = 'running' AND t.enable = 1",
    )?;
    let tasks = stmt
        .query_map(params![node_id.to_string()], |r| {
            let overrides_str: String = r.get(8)?;
            Ok(NodeTask {
                dispatch_id: r.get::<_, String>(0)?.parse().unwrap(),
                task: Task {
                    id: r.get::<_, String>(1)?.parse().unwrap(),
                    name: r.get(2)?,
                    url: r.get(3)?,
                    filename: r.get(4)?,
                    enable: true,
                    created_at: chrono::DateTime::parse_from_rfc3339(&r.get::<_, String>(6)?)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or(Utc::now()),
                    note: r.get(7)?,
                    overrides: serde_json::from_str(&overrides_str).unwrap_or_default(),
                },
            })
        })?
        .filter_map(|r| r.ok())
        .collect();
    Ok(tasks)
}

pub fn apply_report(conn: &Connection, req: &AgentReportReq) -> Result<RunRecord> {
    let now = Utc::now();
    let now_str = now.to_rfc3339();

    if let Some(did) = req.dispatch_id {
        let state = if req.status == "failed" {
            "failed"
        } else {
            "success"
        };
        conn.execute(
            "UPDATE dispatches SET state = ?1, updated_at = ?2 WHERE id = ?3",
            params![state, now_str, did.to_string()],
        )?;

        // 更新关联的工作流运行记录
        let wfrs: Vec<(String, i64, i64, i64, String)> = conn
            .prepare("SELECT id, task_count, success_count, failed_count, workflow_id FROM workflow_runs WHERE status = 'running'")?
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, String>(4)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .collect();

        for (wr_id, task_count, mut success_count, mut failed_count, wf_id) in wfrs {
            // 检查这个 workflow_run 是否包含这个 dispatch_id
            let dispatch_ids_str: String = conn
                .query_row(
                    "SELECT dispatch_ids FROM workflow_runs WHERE id = ?1",
                    params![wr_id],
                    |r| r.get(0),
                )
                .unwrap_or_default();
            let dispatch_ids: Vec<Uuid> =
                serde_json::from_str(&dispatch_ids_str).unwrap_or_default();
            if !dispatch_ids.contains(&did) {
                continue;
            }

            if req.status == "failed" {
                failed_count += 1;
            } else {
                success_count += 1;
            }
            let total = success_count + failed_count;
            if total >= task_count {
                let status = if failed_count == 0 {
                    "success"
                } else if success_count == 0 {
                    "failed"
                } else {
                    "partial"
                };
                conn.execute(
                    "UPDATE workflow_runs SET status = ?1, success_count = ?2, failed_count = ?3 WHERE id = ?4",
                    params![status, success_count, failed_count, wr_id],
                )?;
                conn.execute(
                    "UPDATE workflows SET last_run_status = ?1 WHERE id = ?2",
                    params![status, wf_id],
                )?;
            } else {
                conn.execute(
                    "UPDATE workflow_runs SET success_count = ?1, failed_count = ?2 WHERE id = ?3",
                    params![success_count, failed_count, wr_id],
                )?;
            }
        }
    }

    let rec = RunRecord {
        id: Uuid::new_v4(),
        task_id: req.task_id,
        dispatch_id: req.dispatch_id,
        node_id: req.node_id,
        task_name: req.task_name.clone(),
        url: req.url.clone(),
        filename: req.filename.clone(),
        file_size: req.file_size,
        downloaded_bytes: req.downloaded_bytes,
        elapsed_secs: req.elapsed_secs,
        avg_speed_mbps: req.avg_speed_mbps,
        status: req.status.clone(),
        success_chunks: req.success_chunks,
        failed_chunks: req.failed_chunks,
        error_msg: req.error_msg.clone(),
        timestamp: now,
    };

    conn.execute(
        "INSERT INTO runs VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
        params![
            rec.id.to_string(),
            rec.task_id.map(|u| u.to_string()),
            rec.dispatch_id.map(|u| u.to_string()),
            rec.node_id.to_string(),
            rec.task_name,
            rec.url,
            rec.filename,
            rec.file_size,
            rec.downloaded_bytes,
            rec.elapsed_secs,
            rec.avg_speed_mbps,
            rec.status,
            rec.success_chunks,
            rec.failed_chunks,
            rec.error_msg,
            now_str,
        ],
    )?;

    Ok(rec)
}

pub fn mark_running(conn: &Connection, dispatch_id: Uuid) -> Result<()> {
    let now = Utc::now().to_rfc3339();
    conn.execute(
        "UPDATE dispatches SET state = 'running', updated_at = ?1 WHERE id = ?2",
        params![now, dispatch_id.to_string()],
    )?;
    Ok(())
}

pub fn cancel_task(conn: &Connection, task_id: Uuid) -> Result<()> {
    let now = Utc::now().to_rfc3339();
    conn.execute(
        "UPDATE dispatches SET state = 'cancelled', updated_at = ?1 WHERE task_id = ?2 AND state IN ('pending', 'acked', 'running')",
        params![now, task_id.to_string()],
    )?;
    Ok(())
}

pub fn overview(conn: &Connection) -> Result<Overview> {
    let nodes_total: i64 = conn.query_row("SELECT COUNT(*) FROM nodes", [], |r| r.get(0))?;
    let nodes_online: i64 = conn.query_row(
        "SELECT COUNT(*) FROM nodes WHERE status IN ('online', 'busy')",
        [],
        |r| r.get(0),
    )?;
    let tasks_total: i64 = conn.query_row("SELECT COUNT(*) FROM tasks", [], |r| r.get(0))?;
    let tasks_running: i64 = conn.query_row(
        "SELECT COUNT(*) FROM dispatches WHERE state IN ('running', 'acked')",
        [],
        |r| r.get(0),
    )?;
    let workflows_total: i64 =
        conn.query_row("SELECT COUNT(*) FROM workflows", [], |r| r.get(0))?;
    let workflows_active: i64 =
        conn.query_row("SELECT COUNT(*) FROM workflows WHERE enable = 1", [], |r| {
            r.get(0)
        })?;
    let dispatches_pending: i64 = conn.query_row(
        "SELECT COUNT(*) FROM dispatches WHERE state = 'pending' AND node_id IS NULL",
        [],
        |r| r.get(0),
    )?;
    let bytes_downloaded: i64 = conn.query_row(
        "SELECT COALESCE(SUM(downloaded_bytes), 0) FROM runs",
        [],
        |r| r.get(0),
    )?;
    let runs_success: i64 = conn.query_row(
        "SELECT COUNT(*) FROM runs WHERE status IN ('success', 'skipped')",
        [],
        |r| r.get(0),
    )?;
    let runs_failed: i64 = conn.query_row(
        "SELECT COUNT(*) FROM runs WHERE status = 'failed'",
        [],
        |r| r.get(0),
    )?;
    let (speed_sum, speed_n): (f64, i64) = conn.query_row(
        "SELECT COALESCE(SUM(avg_speed_mbps), 0), COUNT(*) FROM runs WHERE avg_speed_mbps > 0",
        [],
        |r| Ok((r.get::<_, f64>(0)?, r.get::<_, i64>(1)?)),
    )?;

    Ok(Overview {
        version: env!("CARGO_PKG_VERSION").to_string(),
        nodes_total: nodes_total as usize,
        nodes_online: nodes_online as usize,
        nodes_offline: (nodes_total - nodes_online).max(0) as usize,
        tasks_total: tasks_total as usize,
        tasks_running: tasks_running as usize,
        workflows_total: workflows_total as usize,
        workflows_active: workflows_active as usize,
        dispatches_pending: dispatches_pending as usize,
        bytes_downloaded: bytes_downloaded as u64,
        runs_success: runs_success as usize,
        runs_failed: runs_failed as usize,
        avg_speed_mbps: if speed_n > 0 {
            speed_sum / speed_n as f64
        } else {
            0.0
        },
    })
}
