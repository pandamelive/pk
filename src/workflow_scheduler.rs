use crate::models::*;
use crate::scheduler;
use crate::store::AppState;
use crate::ws;
use anyhow::Result;
use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use cron::Schedule;
use rusqlite::params;
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::{self, Duration as TokioDuration};
use uuid::Uuid;

/// 启动后台工作流调度器，每 30 秒扫描一次；同时启动超时回收任务
pub async fn start(state: Arc<AppState>) {
    // 工作流定时触发
    let state_clone = state.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(TokioDuration::from_secs(30));
        loop {
            interval.tick().await;
            if let Err(e) = scan_and_trigger(&state_clone).await {
                tracing::warn!("workflow scheduler error: {}", e);
            }
        }
    });

    // 超时回收 + 修复卡住的工作流运行记录
    tokio::spawn(async move {
        let mut interval = time::interval(TokioDuration::from_secs(60));
        loop {
            interval.tick().await;
            if let Err(e) = state
                .with_transaction(|conn| {
                    scheduler::reclaim_timeout_tasks(conn, 180)?; // 3分钟超时回收
                    scheduler::fix_stuck_workflow_runs(conn)?;
                    Ok(())
                })
                .await
            {
                tracing::warn!("reclaim/fix error: {}", e);
            }
        }
    });
}

/// 扫描所有启用的工作流，到点则触发执行
async fn scan_and_trigger(state: &Arc<AppState>) -> Result<()> {
    let now = Utc::now();
    let to_trigger: Vec<Uuid> = state
        .with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT id, next_run_at FROM workflows WHERE enable = 1")?;
            let ids: Vec<Uuid> = stmt
                .query_map([], |r| {
                    let id_str: String = r.get(0)?;
                    let next_str: Option<String> = r.get(1)?;
                    Ok((id_str, next_str))
                })?
                .filter_map(|r| r.ok())
                .filter(|(_, next_str)| {
                    next_str
                        .as_ref()
                        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                        .map(|dt| dt.with_timezone(&Utc) <= now)
                        .unwrap_or(false)
                })
                .filter_map(|(id_str, _)| id_str.parse().ok())
                .collect();
            Ok(ids)
        })
        .await?;

    for wf_id in to_trigger {
        trigger_workflow(state, wf_id).await?;
    }

    Ok(())
}

/// 手动触发指定工作流
pub async fn trigger_workflow(state: &Arc<AppState>, wf_id: Uuid) -> Result<Option<WorkflowRun>> {
    let wf = state
        .with_conn(|conn| {
            let wf = conn.query_row(
                "SELECT id, name, enable, schedule, task_ids, target, node_ids, next_run_at, last_run_at, last_run_status, created_at FROM workflows WHERE id = ?1",
                params![wf_id.to_string()],
                |r| {
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
                        target: serde_json::from_str(&target_str).unwrap_or_default(),
                        node_ids: serde_json::from_str(&node_ids_str).unwrap_or_default(),
                        next_run_at: r.get::<_, Option<String>>(7)?.and_then(|s| DateTime::parse_from_rfc3339(&s).ok().map(|dt| dt.with_timezone(&Utc))),
                        last_run_at: r.get::<_, Option<String>>(8)?.and_then(|s| DateTime::parse_from_rfc3339(&s).ok().map(|dt| dt.with_timezone(&Utc))),
                        last_run_status: r.get(9)?,
                        created_at: DateTime::parse_from_rfc3339(&r.get::<_, String>(10)?).ok().map(|dt| dt.with_timezone(&Utc)).unwrap_or(Utc::now()),
                    })
                },
            );
            Ok(wf.ok())
        })
        .await?;

    let Some(wf) = wf else {
        return Ok(None);
    };

    let now = Utc::now();
    let run = state
        .with_transaction(|conn| {
            let run = scheduler::execute_workflow(conn, &wf)?;
            // 更新工作流的最后执行信息和下次执行时间
            let next_run = compute_next_run(&wf.schedule, Some(now), now);
            conn.execute(
                "UPDATE workflows SET last_run_at = ?1, last_run_status = ?2, next_run_at = ?3 WHERE id = ?4",
                params![
                    now.to_rfc3339(),
                    run.status.clone(),
                    next_run.map(|t| t.to_rfc3339()),
                    wf.id.to_string(),
                ],
            )?;
            Ok(run)
        })
        .await?;

    // 通知所有节点：共享待下发池有新任务，空闲节点去 claim 领取
    ws::notify_new_task(state).await;

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
        WorkflowSchedule::Cron { expression } => match Schedule::from_str(expression) {
            Ok(sched) => sched.upcoming(Utc).next(),
            Err(_) => None,
        },
        WorkflowSchedule::Daily { hour, minute } => next_daily(now, *hour, *minute),
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
