use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Online,
    Offline,
    Busy,
    Pending,
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

impl Default for AssignmentTarget {
    fn default() -> Self {
        AssignmentTarget::Any
    }
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
    /// 节点最大并发任务数（None=用全局默认）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrent: Option<u32>,
    /// 节点最大带宽上限 bps（None=不限制）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bandwidth_bps: Option<u64>,
    /// 节点通用能力参数（JSON，灵活扩展）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<serde_json::Value>,
    /// 实时总速度（bps），从内存态填充
    #[serde(default)]
    pub total_speed_bps: u64,
    /// 活跃任务实时进度列表，从内存态填充
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub active_tasks_progress: Vec<crate::ws::TaskProgressState>,
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

/// 任务池中的任务定义（仅描述下载内容，不含调度信息）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: Uuid,
    pub name: String,
    pub url: String,
    pub filename: String,
    pub enable: bool,
    pub created_at: DateTime<Utc>,
    pub note: String,
    #[serde(default)]
    pub overrides: TaskOverrides,
}

/// 工作流定时规则
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkflowSchedule {
    /// 一次性执行
    Once { at: DateTime<Utc> },
    /// 固定间隔（秒）
    Interval { seconds: u64 },
    /// Cron 表达式（5段标准 cron）
    Cron { expression: String },
    /// 每天固定时间
    Daily { hour: u32, minute: u32 },
    /// 每周固定时间（weekday: 0=周日 ... 6=周六）
    Weekly { weekday: u8, hour: u32, minute: u32 },
}

/// 工作流：定时规则 + 任务集合 + 节点选择
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    pub id: Uuid,
    pub name: String,
    pub enable: bool,
    pub schedule: WorkflowSchedule,
    pub task_ids: Vec<Uuid>,
    pub target: AssignmentTarget,
    pub node_ids: Vec<Uuid>,
    pub next_run_at: Option<DateTime<Utc>>,
    pub last_run_at: Option<DateTime<Utc>>,
    pub last_run_status: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// 工作流单次执行记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRun {
    pub id: Uuid,
    pub workflow_id: Uuid,
    pub workflow_name: String,
    pub triggered_at: DateTime<Utc>,
    /// running / success / failed / partial
    pub status: String,
    pub task_count: u32,
    pub success_count: u32,
    pub failed_count: u32,
    pub dispatch_ids: Vec<Uuid>,
    pub error_msg: Option<String>,
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
    /// Pending 时为 None（共享待下发池），节点领取后绑定
    #[serde(default)]
    pub node_id: Option<Uuid>,
    pub state: DispatchState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// 领取时间，用于超时回收判断
    #[serde(default)]
    pub claimed_at: Option<DateTime<Utc>>,
    /// 领取权限控制（any/all=任意节点，nodes=仅白名单）
    #[serde(default)]
    pub target: AssignmentTarget,
    /// target=nodes 时的允许节点列表
    #[serde(default)]
    pub allowed_nodes: Vec<Uuid>,
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
    pub workflows: Vec<Workflow>,
    pub workflow_runs: Vec<WorkflowRun>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateTaskReq {
    pub name: String,
    pub url: String,
    pub filename: String,
    #[serde(default = "default_true")]
    pub enable: bool,
    #[serde(default)]
    pub note: String,
    #[serde(default)]
    pub overrides: TaskOverrides,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateWorkflowReq {
    pub name: String,
    #[serde(default = "default_true")]
    pub enable: bool,
    pub schedule: WorkflowSchedule,
    pub task_ids: Vec<Uuid>,
    #[serde(default)]
    pub target: AssignmentTarget,
    #[serde(default)]
    pub node_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpdateWorkflowReq {
    pub name: Option<String>,
    pub enable: Option<bool>,
    pub schedule: Option<WorkflowSchedule>,
    pub task_ids: Option<Vec<Uuid>>,
    pub target: Option<AssignmentTarget>,
    pub node_ids: Option<Vec<Uuid>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpdateTaskReq {
    pub name: Option<String>,
    pub url: Option<String>,
    pub filename: Option<String>,
    pub enable: Option<bool>,
    pub note: Option<String>,
    pub overrides: Option<TaskOverrides>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentFetchReq {
    pub node_id: Uuid,
    #[serde(default)]
    pub hostname: Option<String>,
}

/// 下发给节点的单个下载任务配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskItem {
    pub url: String,
    pub filename: String,
    pub save_path: String,
    pub max_concurrent: u32,
    pub connections_per_file: u32,
    pub retry_times: u32,
    pub timeout: u64,
    pub dry_run: bool,
    pub skip_tls_verify: bool,
    pub resume: bool,
    #[serde(default)]
    pub http_proxy: String,
    #[serde(default)]
    pub https_proxy: String,
}

/// 节点领取任务后返回的完整配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub dispatch_id: Uuid,
    pub master: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    pub tasks: Vec<TaskItem>,
}

/// claim 接口返回给 spde 节点的任务信息（与 spde 的 ClaimResp 对应）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimTaskResp {
    pub dispatch_id: Uuid,
    pub task_id: Uuid,
    pub name: String,
    pub url: String,
    pub filename: String,
    pub overrides: TaskOverrides,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowDetail {
    pub workflow: Workflow,
    pub runs: Vec<WorkflowRun>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HostInfo {
    pub platform: String,
    pub arch: String,
}

fn default_true() -> bool {
    true
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
    /// 节点上报的最大并发任务数
    #[serde(default)]
    pub max_concurrent: Option<u32>,
    /// 节点上报的最大带宽上限 bps
    #[serde(default)]
    pub max_bandwidth_bps: Option<u64>,
    /// 节点上报的通用能力参数（JSON，灵活扩展）
    #[serde(default)]
    pub capabilities: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRegisterResp {
    pub node_id: Uuid,
    pub poll_interval_secs: u64,
    pub master_listen: String,
    /// 节点注册后的状态（online/pending）
    pub status: String,
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

/// 节点领取待下发任务请求
#[derive(Debug, Clone, Deserialize)]
pub struct AgentClaimReq {
    pub node_id: Uuid,
}

/// 节点领取待下发任务响应（领到任务时返回 200 + 此结构，没任务返回 204）
#[derive(Debug, Clone, Serialize)]
pub struct AgentClaimResp {
    pub dispatch_id: Uuid,
    pub task_id: Uuid,
    pub name: String,
    pub url: String,
    pub filename: String,
    pub overrides: TaskOverrides,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Overview {
    pub version: String,
    pub nodes_total: usize,
    pub nodes_online: usize,
    pub nodes_offline: usize,
    pub tasks_total: usize,
    pub tasks_running: usize,
    pub workflows_total: usize,
    pub workflows_active: usize,
    pub dispatches_pending: usize,
    pub bytes_downloaded: u64,
    pub runs_success: usize,
    pub runs_failed: usize,
    pub avg_speed_mbps: f64,
}
