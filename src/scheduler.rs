use crate::models::*;
use chrono::Utc;
use uuid::Uuid;

/// 分配给某节点的任务（含 dispatch_id，用于回报告）
#[derive(Debug, Clone)]
pub struct NodeTask {
    pub dispatch_id: Uuid,
    pub task: Task,
}

/// 创建一条待下发 dispatch（进入共享待下发池，不绑定节点）
pub fn create_pending_dispatch(
    snap: &mut Snapshot,
    task_id: Uuid,
    target: &AssignmentTarget,
    allowed_nodes: &[Uuid],
) -> bool {
    let Some(task) = snap.tasks.iter().find(|t| t.id == task_id).cloned() else {
        return false;
    };
    if !task.enable {
        return false;
    }
    let now = Utc::now();
    snap.dispatches.push(Dispatch {
        id: Uuid::new_v4(),
        task_id,
        node_id: None,
        state: DispatchState::Pending,
        created_at: now,
        updated_at: now,
        claimed_at: None,
        target: target.clone(),
        allowed_nodes: allowed_nodes.to_vec(),
    });
    true
}

/// 执行工作流：每个任务创建一条待下发 dispatch 进入共享池
pub fn execute_workflow(snap: &mut Snapshot, wf: &Workflow) -> WorkflowRun {
    let now = Utc::now();
    let mut dispatch_ids = Vec::new();
    let mut task_count = 0u32;

    for &task_id in &wf.task_ids {
        if create_pending_dispatch(snap, task_id, &wf.target, &wf.node_ids) {
            task_count += 1;
            if let Some(d) = snap.dispatches.last() {
                dispatch_ids.push(d.id);
            }
        }
    }

    WorkflowRun {
        id: Uuid::new_v4(),
        workflow_id: wf.id,
        workflow_name: wf.name.clone(),
        triggered_at: now,
        status: if task_count > 0 { "running".into() } else { "failed".into() },
        task_count,
        success_count: 0,
        failed_count: 0,
        dispatch_ids,
        error_msg: if task_count == 0 {
            Some("任务全部禁用或不存在".into())
        } else {
            None
        },
    }
}

/// 节点从共享待下发池领取一个任务（原子操作，单进程天然串行）
/// 返回领到的任务详情，池子空或无权限则返回 None
pub fn claim_task(snap: &mut Snapshot, node_id: Uuid) -> Option<NodeTask> {
    // 节点必须在线
    let node_online = snap
        .nodes
        .iter()
        .any(|n| n.id == node_id && n.status != NodeStatus::Offline);
    if !node_online {
        return None;
    }

    let now = Utc::now();
    // 找到最早的 Pending 且该节点有权限领取的 dispatch
    let idx = snap.dispatches.iter().position(|d| {
        if d.state != DispatchState::Pending || d.node_id.is_some() {
            return false;
        }
        match d.target {
            AssignmentTarget::Any | AssignmentTarget::All => true,
            AssignmentTarget::Nodes => d.allowed_nodes.contains(&node_id),
        }
    })?;

    let d = &mut snap.dispatches[idx];
    d.node_id = Some(node_id);
    d.state = DispatchState::Running;
    d.claimed_at = Some(now);
    d.updated_at = now;
    let dispatch_id = d.id;

    let task = snap
        .tasks
        .iter()
        .find(|t| t.id == d.task_id && t.enable)?
        .clone();

    Some(NodeTask {
        dispatch_id,
        task,
    })
}

/// 超时回收：Running 状态的 dispatch 如果节点超时未上报，回收到待下发池
/// timeout_secs: 领取后超过此秒数未完成则判定超时
pub fn reclaim_timeout_tasks(snap: &mut Snapshot, timeout_secs: i64) {
    let now = Utc::now();
    for d in snap.dispatches.iter_mut() {
        if d.state != DispatchState::Running {
            continue;
        }
        let Some(claimed_at) = d.claimed_at else {
            continue;
        };
        let elapsed = (now - claimed_at).num_seconds();
        if elapsed <= timeout_secs {
            continue;
        }
        // 检查节点是否还在线
        let node_offline = d
            .node_id
            .and_then(|nid| snap.nodes.iter().find(|n| n.id == nid))
            .map(|n| n.status == NodeStatus::Offline)
            .unwrap_or(true);
        // 节点离线 或 超时过久（2倍超时），回收到待下发池
        if node_offline || elapsed > timeout_secs * 2 {
            d.state = DispatchState::Pending;
            d.node_id = None;
            d.claimed_at = None;
            d.updated_at = now;
        }
    }
}

/// 修复卡住的工作流运行记录：状态为 running 但所有关联 dispatch 都已是终态
/// （被取消的 dispatch 不会触发 apply_report，会导致工作流运行记录卡死在 running）
pub fn fix_stuck_workflow_runs(snap: &mut Snapshot) {
    for wr in snap.workflow_runs.iter_mut() {
        if wr.status != "running" {
            continue;
        }
        if wr.dispatch_ids.is_empty() {
            continue;
        }

        let mut success = 0u32;
        let mut failed = 0u32;
        let mut pending_or_running = 0u32;

        for did in &wr.dispatch_ids {
            match snap.dispatches.iter().find(|d| d.id == *did) {
                Some(d) => match d.state {
                    DispatchState::Success => success += 1,
                    DispatchState::Failed => failed += 1,
                    DispatchState::Cancelled => failed += 1, // 被取消的算失败
                    _ => pending_or_running += 1,
                },
                None => {
                    // dispatch 不存在了，算失败
                    failed += 1;
                }
            }
        }

        // 还有未完成的 dispatch，不修复
        if pending_or_running > 0 {
            continue;
        }

        // 所有 dispatch 都是终态，更新状态
        wr.success_count = success;
        wr.failed_count = failed;
        wr.status = if failed == 0 {
            "success".into()
        } else if success == 0 {
            "failed".into()
        } else {
            "partial".into()
        };

        // 同步更新工作流的 last_run_status
        if let Some(wf) = snap.workflows.iter_mut().find(|w| w.id == wr.workflow_id) {
            wf.last_run_status = Some(wr.status.clone());
        }
    }
}

/// 取出该节点当前正在执行的任务（用于节点重启后恢复，只返回 Running）
pub fn active_tasks_for_node(snap: &Snapshot, node_id: Uuid) -> Vec<NodeTask> {
    snap.dispatches
        .iter()
        .filter(|d| d.node_id == Some(node_id))
        .filter(|d| d.state == DispatchState::Running)
        .filter_map(|d| {
            let task = snap
                .tasks
                .iter()
                .find(|t| t.id == d.task_id && t.enable)?;
            Some(NodeTask {
                dispatch_id: d.id,
                task: task.clone(),
            })
        })
        .collect()
}

pub fn apply_report(snap: &mut Snapshot, req: AgentReportReq) -> RunRecord {
    let now = Utc::now();
    if let Some(did) = req.dispatch_id {
        if let Some(d) = snap.dispatches.iter_mut().find(|d| d.id == did) {
            d.state = if req.status == "failed" {
                DispatchState::Failed
            } else {
                DispatchState::Success
            };
            d.updated_at = now;
        }
    }

    // 更新关联的工作流运行记录
    if let Some(did) = req.dispatch_id {
        for wr in snap.workflow_runs.iter_mut() {
            if wr.dispatch_ids.contains(&did) {
                if req.status == "failed" {
                    wr.failed_count += 1;
                } else {
                    wr.success_count += 1;
                }
                let total = wr.success_count + wr.failed_count;
                if total >= wr.task_count {
                    wr.status = if wr.failed_count == 0 {
                        "success".into()
                    } else if wr.success_count == 0 {
                        "failed".into()
                    } else {
                        "partial".into()
                    };
                    // 同步更新工作流的 last_run_status
                    if let Some(wf) = snap.workflows.iter_mut().find(|w| w.id == wr.workflow_id) {
                        wf.last_run_status = Some(wr.status.clone());
                    }
                }
            }
        }
    }

    let rec = RunRecord {
        id: Uuid::new_v4(),
        task_id: req.task_id,
        dispatch_id: req.dispatch_id,
        node_id: req.node_id,
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
        timestamp: now,
    };
    snap.runs.push(rec.clone());
    rec
}

pub fn mark_running(snap: &mut Snapshot, dispatch_id: Uuid) {
    let now = Utc::now();
    if let Some(d) = snap.dispatches.iter_mut().find(|d| d.id == dispatch_id) {
        d.state = DispatchState::Running;
        d.updated_at = now;
    }
}

pub fn cancel_task(snap: &mut Snapshot, task_id: Uuid) {
    let now = Utc::now();
    for d in snap.dispatches.iter_mut() {
        if d.task_id == task_id
            && matches!(
                d.state,
                DispatchState::Pending | DispatchState::Acked | DispatchState::Running
            )
        {
            d.state = DispatchState::Cancelled;
            d.updated_at = now;
        }
    }
}

pub fn overview(snap: &Snapshot) -> Overview {
    let nodes_online = snap
        .nodes
        .iter()
        .filter(|n| n.status != NodeStatus::Offline)
        .count();
    let bytes_downloaded = snap.runs.iter().map(|r| r.downloaded_bytes).sum();
    let runs_success = snap
        .runs
        .iter()
        .filter(|r| r.status == "success" || r.status == "skipped")
        .count();
    let runs_failed = snap.runs.iter().filter(|r| r.status == "failed").count();
    let speed_sum: f64 = snap
        .runs
        .iter()
        .filter(|r| r.avg_speed_mbps > 0.0)
        .map(|r| r.avg_speed_mbps)
        .sum();
    let speed_n = snap.runs.iter().filter(|r| r.avg_speed_mbps > 0.0).count();
    let tasks_running = snap
        .dispatches
        .iter()
        .filter(|d| matches!(d.state, DispatchState::Running | DispatchState::Acked))
        .count();
    Overview {
        version: env!("CARGO_PKG_VERSION").to_string(),
        nodes_total: snap.nodes.len(),
        nodes_online,
        nodes_offline: snap.nodes.len().saturating_sub(nodes_online),
        tasks_total: snap.tasks.len(),
        tasks_running,
        workflows_total: snap.workflows.len(),
        workflows_active: snap.workflows.iter().filter(|w| w.enable).count(),
        // 待下发 = 共享池中未被领取的 Pending dispatch
        dispatches_pending: snap
            .dispatches
            .iter()
            .filter(|d| d.state == DispatchState::Pending && d.node_id.is_none())
            .count(),
        bytes_downloaded,
        runs_success,
        runs_failed,
        avg_speed_mbps: if speed_n > 0 {
            speed_sum / speed_n as f64
        } else {
            0.0
        },
    }
}
