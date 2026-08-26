use crate::models::*;
use crate::scheduler::execute_workflow;
use crate::store::AppState;
use anyhow::Result;
use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use cron::Schedule;
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::{self, Duration as TokioDuration};
use uuid::Uuid;

/// 启动后台工作流调度器，每 30 秒扫描一次
pub async fn start(state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut interval = time::interval(TokioDuration::from_secs(30));
        loop {
            interval.tick().await;
            if let Err(e) = scan_and_trigger(&state).await {
                tracing::warn!("workflow scheduler error: {}", e);
            }
        }
    });
}

/// 扫描所有启用的工作流，到点则触发执行
async fn scan_and_trigger(state: &Arc<AppState>) -> Result<()> {
    let now = Utc::now();
    let mut to_trigger: Vec<(Uuid, DateTime<Utc>)> = Vec::new();

    {
        let snap = state.snapshot().await;
        for wf in &snap.workflows {
            if !wf.enable {
                continue;
            }
            // 计算下次执行时间
            let next = compute_next_run(&wf.schedule, wf.last_run_at, now);
            if let Some(next_time) = next {
                if next_time <= now {
                    to_trigger.push((wf.id, next_time));
                }
            }
        }
    }

    for (wf_id, _scheduled) in to_trigger {
        trigger_workflow(state, wf_id).await?;
    }

    // 更新所有工作流的 next_run_at
    state
        .with_mut(|snap| {
            let now = Utc::now();
            for wf in &mut snap.workflows {
                if wf.enable {
                    wf.next_run_at = compute_next_run(&wf.schedule, wf.last_run_at, now);
                } else {
                    wf.next_run_at = None;
                }
            }
        })
        .await?;

    Ok(())
}

/// 手动触发指定工作流
pub async fn trigger_workflow(state: &Arc<AppState>, wf_id: Uuid) -> Result<Option<WorkflowRun>> {
    let wf_clone = {
        let snap = state.snapshot().await;
        snap.workflows.iter().find(|w| w.id == wf_id).cloned()
    };

    let Some(wf) = wf_clone else {
        return Ok(None);
    };

    let now = Utc::now();
    let run = state
        .with_mut(|snap| {
            let run = execute_workflow(snap, &wf);
            // 更新工作流的最后执行信息
            if let Some(w) = snap.workflows.iter_mut().find(|w| w.id == wf.id) {
                w.last_run_at = Some(now);
                w.last_run_status = Some(run.status.clone());
            }
            snap.workflow_runs.push(run.clone());
            // 限制运行记录数量，保留最近 500 条
            if snap.workflow_runs.len() > 500 {
                let drain_to = snap.workflow_runs.len() - 500;
                snap.workflow_runs.drain(0..drain_to);
            }
            run
        })
        .await?;

    Ok(Some(run))
}

/// 计算定时规则的下次执行时间
pub fn compute_next_run(
    schedule: &WorkflowSchedule,
    last_run: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    match schedule {
        WorkflowSchedule::Once { at } => {
            if *at > now {
                Some(*at)
            } else {
                None // 一次性任务已过，不再执行
            }
        }
        WorkflowSchedule::Interval { seconds } => {
            let base = last_run.unwrap_or(now);
            let next = base + Duration::seconds(*seconds as i64);
            if next <= now {
                // 如果计算出的下次时间已过，从现在开始算
                Some(now + Duration::seconds(*seconds as i64))
            } else {
                Some(next)
            }
        }
        WorkflowSchedule::Cron { expression } => {
            match Schedule::from_str(expression) {
                Ok(sched) => sched.upcoming(Utc).next(),
                Err(_) => None,
            }
        }
        WorkflowSchedule::Daily { hour, minute } => {
            next_daily(now, *hour, *minute)
        }
        WorkflowSchedule::Weekly {
            weekday,
            hour,
            minute,
        } => next_weekly(now, *weekday, *hour, *minute),
    }
}

fn next_daily(now: DateTime<Utc>, hour: u32, minute: u32) -> Option<DateTime<Utc>> {
    let today = now
        .with_hour(hour)?
        .with_minute(minute)?
        .with_second(0)?
        .with_nanosecond(0)?;
    if today > now {
        Some(today)
    } else {
        Some(today + Duration::days(1))
    }
}

fn next_weekly(
    now: DateTime<Utc>,
    weekday: u8,
    hour: u32,
    minute: u32,
) -> Option<DateTime<Utc>> {
    // chrono: weekday() 返回 0=Mon ... 6=Sun
    // 我们约定 0=Sun ... 6=Sat，需要转换
    let target_chrono = if weekday == 0 { 6 } else { weekday - 1 };
    let current_chrono = now.weekday().num_days_from_monday();
    let mut days_ahead = (target_chrono as i64 - current_chrono as i64 + 7) % 7;

    let candidate = now
        .with_hour(hour)?
        .with_minute(minute)?
        .with_second(0)?
        .with_nanosecond(0)?
        + Duration::days(days_ahead);

    if candidate <= now {
        days_ahead += 7;
        Some(
            now.with_hour(hour)?
                .with_minute(minute)?
                .with_second(0)?
                .with_nanosecond(0)?
                + Duration::days(days_ahead),
        )
    } else {
        Some(candidate)
    }
}
