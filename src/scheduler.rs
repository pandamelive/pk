use crate::models::*;
use chrono::Utc;
use uuid::Uuid;

/// 分配给某节点的任务（含 dispatch_id，用于生成 config 和回报告）
#[derive(Debug, Clone)]
pub struct NodeTask {
    pub dispatch_id: Uuid,
    pub task: Task,
}

/// 按任务目标选出应收节点，并写入 pending dispatch（已有未完成的不重复发）。
pub fn dispatch_task(snap: &mut Snapshot, task_id: Uuid) -> usize {
    let Some(task) = snap.tasks.iter().find(|t| t.id == task_id).cloned() else {
        return 0;
    };
    if !task.enable {
        return 0;
    }

    let online: Vec<Uuid> = snap
        .nodes
        .iter()
        .filter(|n| n.status != NodeStatus::Offline)
        .map(|n| n.id)
        .collect();

    let targets: Vec<Uuid> = match task.target {
        AssignmentTarget::All => online,
        AssignmentTarget::Nodes => task
            .node_ids
            .iter()
            .copied()
            .filter(|id| online.contains(id))
            .collect(),
        AssignmentTarget::Any => pick_least_loaded(snap, &online).into_iter().collect(),
    };

    let mut created = 0usize;
    let now = Utc::now();
    for node_id in targets {
        let exists = snap.dispatches.iter().any(|d| {
            d.task_id == task_id
                && d.node_id == node_id
                && matches!(
                    d.state,
                    DispatchState::Pending | DispatchState::Acked | DispatchState::Running
                )
        });
        if exists {
            continue;
        }
        snap.dispatches.push(Dispatch {
            id: Uuid::new_v4(),
            task_id,
            node_id,
            state: DispatchState::Pending,
            created_at: now,
            updated_at: now,
        });
        created += 1;
    }

    if let Some(t) = snap.tasks.iter_mut().find(|t| t.id == task_id) {
        if created > 0 && t.status == TaskStatus::Draft {
            t.status = TaskStatus::Queued;
        }
    }
    created
}

fn pick_least_loaded(snap: &Snapshot, online: &[Uuid]) -> Option<Uuid> {
    online
        .iter()
        .copied()
        .min_by_key(|id| {
            let pending = snap
                .dispatches
                .iter()
                .filter(|d| {
                    d.node_id == *id
                        && matches!(
                            d.state,
                            DispatchState::Pending | DispatchState::Acked | DispatchState::Running
                        )
                })
                .count();
            let node_active = snap
                .nodes
                .iter()
                .find(|n| n.id == *id)
                .map(|n| n.active_tasks)
                .unwrap_or(0);
            (pending as u32).saturating_add(node_active)
        })
}

/// 取出该节点当前应执行的任务列表（基于 active dispatch 过滤）。
/// 生成 config.yaml 时调用，确保 only 被分配到此节点且未完成的任务出现在 direct_tasks 中。
pub fn active_tasks_for_node(snap: &Snapshot, node_id: Uuid) -> Vec<NodeTask> {
    snap.dispatches
        .iter()
        .filter(|d| d.node_id == node_id)
        .filter(|d| {
            matches!(
                d.state,
                DispatchState::Pending | DispatchState::Acked | DispatchState::Running
            )
        })
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
    if let Some(tid) = req.task_id {
        refresh_task_status(snap, tid);
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

fn refresh_task_status(snap: &mut Snapshot, task_id: Uuid) {
    let related: Vec<_> = snap
        .dispatches
        .iter()
        .filter(|d| d.task_id == task_id)
        .map(|d| d.state.clone())
        .collect();
    if related.is_empty() {
        return;
    }
    let all_done = related.iter().all(|s| {
        matches!(
            s,
            DispatchState::Success | DispatchState::Failed | DispatchState::Cancelled
        )
    });
    let any_fail = related.iter().any(|s| *s == DispatchState::Failed);
    let any_run = related
        .iter()
        .any(|s| matches!(s, DispatchState::Running | DispatchState::Acked));
    if let Some(t) = snap.tasks.iter_mut().find(|t| t.id == task_id) {
        t.status = if !all_done {
            if any_run {
                TaskStatus::Running
            } else {
                TaskStatus::Queued
            }
        } else if any_fail {
            TaskStatus::Failed
        } else {
            TaskStatus::Completed
        };
    }
}

pub fn mark_running(snap: &mut Snapshot, dispatch_id: Uuid) {
    let now = Utc::now();
    let mut task_id = None;
    if let Some(d) = snap.dispatches.iter_mut().find(|d| d.id == dispatch_id) {
        d.state = DispatchState::Running;
        d.updated_at = now;
        task_id = Some(d.task_id);
    }
    if let Some(tid) = task_id {
        if let Some(t) = snap.tasks.iter_mut().find(|t| t.id == tid) {
            t.status = TaskStatus::Running;
        }
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
    if let Some(t) = snap.tasks.iter_mut().find(|t| t.id == task_id) {
        t.status = TaskStatus::Cancelled;
        t.enable = false;
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
    Overview {
        nodes_total: snap.nodes.len(),
        nodes_online,
        nodes_offline: snap.nodes.len().saturating_sub(nodes_online),
        tasks_total: snap.tasks.len(),
        tasks_running: snap
            .tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Running)
            .count(),
        dispatches_pending: snap
            .dispatches
            .iter()
            .filter(|d| d.state == DispatchState::Pending)
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
