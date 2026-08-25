use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Online,
    Offline,
    Busy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Draft,
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AssignmentTarget {
    /// 所有在线节点各执行一份
    All,
    /// 调度到负载最低的一个节点
    Any,
    /// 指定节点
    Nodes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: Uuid,
    pub hostname: String,
    pub platform: String,
    pub arch: String,
    pub version: String,
    pub status: NodeStatus,
    pub last_seen: DateTime<Utc>,
    pub registered_at: DateTime<Utc>,
    pub labels: Vec<String>,
    pub active_tasks: u32,
    pub bytes_downloaded: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskOverrides {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrent: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connections_per_file: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_times: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skip_tls_verify: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dry_run: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub save_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: Uuid,
    pub name: String,
    pub url: String,
    pub filename: String,
    pub enable: bool,
    pub target: AssignmentTarget,
    pub node_ids: Vec<Uuid>,
    pub status: TaskStatus,
    pub created_at: DateTime<Utc>,
    pub note: String,
    #[serde(default)]
    pub overrides: TaskOverrides,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DispatchState {
    Pending,
    Acked,
    Running,
    Success,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dispatch {
    pub id: Uuid,
    pub task_id: Uuid,
    pub node_id: Uuid,
    pub state: DispatchState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub id: Uuid,
    pub task_id: Option<Uuid>,
    pub dispatch_id: Option<Uuid>,
    pub node_id: Uuid,
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
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Snapshot {
    pub nodes: Vec<Node>,
    pub tasks: Vec<Task>,
    pub dispatches: Vec<Dispatch>,
    pub runs: Vec<RunRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRegisterReq {
    pub node_id: Option<Uuid>,
    pub hostname: String,
    pub platform: String,
    pub arch: String,
    pub version: String,
    #[serde(default)]
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRegisterResp {
    pub node_id: Uuid,
    pub poll_interval_secs: u64,
    pub master_listen: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentHeartbeatReq {
    pub node_id: Uuid,
    #[serde(default)]
    pub active_tasks: u32,
    #[serde(default)]
    pub bytes_downloaded: u64,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub busy: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentHeartbeatResp {
    pub status: NodeStatus,
    /// config.yaml 内容的 SHA256，变化时 SPDE 应重新拉取 config_path
    pub config_version: String,
    /// 该节点 config.yaml 的拉取路径
    pub config_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentReportReq {
    pub node_id: Uuid,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTaskReq {
    pub name: String,
    pub url: String,
    pub filename: String,
    #[serde(default = "default_true")]
    pub enable: bool,
    #[serde(default)]
    pub target: AssignmentTarget,
    #[serde(default)]
    pub node_ids: Vec<Uuid>,
    #[serde(default)]
    pub note: String,
    /// 创建后立即调度下发
    #[serde(default = "default_true")]
    pub dispatch_now: bool,
    #[serde(default)]
    pub overrides: TaskOverrides,
}

fn default_true() -> bool {
    true
}

impl Default for AssignmentTarget {
    fn default() -> Self {
        AssignmentTarget::Any
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Overview {
    pub version: String,
    pub nodes_total: usize,
    pub nodes_online: usize,
    pub nodes_offline: usize,
    pub tasks_total: usize,
    pub tasks_running: usize,
    pub dispatches_pending: usize,
    pub bytes_downloaded: u64,
    pub runs_success: usize,
    pub runs_failed: usize,
    pub avg_speed_mbps: f64,
}
