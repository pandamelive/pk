const $ = (s) => document.querySelector(s);
const $$ = (s) => document.querySelectorAll(s);

const API_BASE = "";
let currentView = "dash";
let currentWorkflowId = null;
let cachedTasks = [];
let cachedNodes = [];
let cachedWorkflows = [];
let cachedDispatches = [];

async function api(path, opts = {}) {
  const headers = { "Content-Type": "application/json" };
  const token = $("#token")?.value?.trim();
  if (token) headers["Authorization"] = token.startsWith("Bearer ") ? token : `Bearer ${token}`;
  const res = await fetch(API_BASE + path, { ...opts, headers: { ...headers, ...(opts.headers || {}) } });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`${res.status} ${text || res.statusText}`);
  }
  if (res.status === 204) return null;
  const json = await res.json();
  return json.data !== undefined ? json.data : json;
}

const fmtBytes = (b) => {
  if (!b) return "0 B";
  const u = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
  return b.toFixed(1) + " " + u[i];
};
const fmtTime = (t) => new Date(t).toLocaleString();

function dot(s) {
  return `<span class="dot ${s}"></span>`;
}
function pill(s) {
  return `<span class="pill ${s}">${s}</span>`;
}

// ==================== 默认值 ====================
function applyDefaults(d) {
  const setPH = (name, val) => {
    const el = document.querySelector(`[name="${name}"]`);
    if (el && val !== undefined && val !== null) el.placeholder = `默认 ${val}`;
  };
  setPH("connections_per_file", d.connections_per_file);
  setPH("retry_times", d.retry_times);
  setPH("timeout", d.timeout);
  setPH("max_concurrent", d.max_concurrent);
  setPH("save_path", d.save_path);
  document.querySelectorAll(".def-hint").forEach((el) => {
    const key = el.dataset.def;
    if (key in d) {
      if (key === "dry_run") {
        el.textContent = `（默认：${d[key] ? "不落盘" : "落盘"}）`;
      } else {
        el.textContent = `（默认：${d[key] ? "开启" : "关闭"}）`;
      }
    }
  });
}

// ==================== 总览 ====================
function renderKpis(o) {
  if (o.version) $("#pk-version").textContent = "v" + o.version;
  const items = [
    ["在线节点", `${o.nodes_online} / ${o.nodes_total}`],
    ["运行中任务", String(o.tasks_running)],
    ["工作流", `${o.workflows_active} / ${o.workflows_total}`],
    ["待下发", String(o.dispatches_pending)],
    ["累计下载", fmtBytes(o.bytes_downloaded)],
    ["成功次数", String(o.runs_success)],
    ["失败次数", String(o.runs_failed)],
    ["平均速度", (o.avg_speed_mbps || 0).toFixed(1) + " MB/s"],
  ];
  $("#kpis").innerHTML = items.map(([k, v]) => `<div class="kpi"><span>${k}</span><b>${v}</b></div>`).join("");
}

// ==================== 节点 ====================
function renderNodes(nodes) {
  $("#node-body").innerHTML = nodes.map((n) => {
    const isPending = n.status === "pending";
    const mc = n.max_concurrent ?? "默认";
    const mb = n.max_bandwidth_bps ? (n.max_bandwidth_bps / 1024 / 1024).toFixed(0) + " MB/s" : "不限";
    const actions = isPending
      ? `<button class="ghost" data-approve="${n.id}">同意</button><button class="danger" data-reject="${n.id}">拒绝</button>`
      : `<button class="ghost" data-edit-cap="${n.id}">能力</button><button class="ghost" data-del-node="${n.id}">移除</button>`;
    return `<tr>
    <td>${dot(n.status)}${pill(n.status)}</td>
    <td>${n.hostname}<div class="mono">${n.id}</div></td>
    <td>${n.platform} / ${n.arch}</td>
    <td>${n.version}</td>
    <td>${n.active_tasks} / ${mc}</td>
    <td>${mb}</td>
    <td>${fmtBytes(n.bytes_downloaded)}</td>
    <td>${fmtTime(n.last_seen)}</td>
    <td class="actions">${actions}</td>
  </tr>`;
  }).join("");
}

// 编辑节点能力参数
async function editNodeCapabilities(nodeId) {
  const node = cachedNodes?.find((n) => n.id === nodeId);
  if (!node) return;
  const mc = prompt("最大并发任务数（留空=用全局默认）:", node.max_concurrent ?? "");
  if (mc === null) return;
  const mb = prompt("最大带宽上限 MB/s（留空=不限）:", node.max_bandwidth_bps ? (node.max_bandwidth_bps / 1024 / 1024).toFixed(0) : "");
  if (mb === null) return;
  const body = {};
  if (mc.trim()) body.max_concurrent = parseInt(mc);
  if (mb.trim()) body.max_bandwidth_bps = parseInt(mb) * 1024 * 1024;
  await api(`/api/v1/nodes/${nodeId}/capabilities`, { method: "PUT", body: JSON.stringify(body) });
  refresh();
}

// ==================== 任务（任务池） ====================
function renderTasks(tasks) {
  $("#task-body").innerHTML = tasks.map((t) => `<tr>
    <td>${t.enable ? pill("enabled") : pill("disabled")}</td>
    <td>${t.name}<div class="mono">${t.url}</div></td>
    <td>${t.filename}</td>
    <td class="mono">${t.url.substring(0, 40)}${t.url.length > 40 ? "..." : ""}</td>
    <td>${fmtTime(t.created_at)}</td>
    <td class="actions">
      <button class="ghost" data-cancel="${t.id}">取消</button>
      <button class="danger" data-del-task="${t.id}">删除</button>
    </td>
  </tr>`).join("");
}

function buildOverridesFromForm(fd) {
  const overrides = {};
  const num = (k) => {
    const v = fd.get(k);
    return v !== null && v.toString().trim() !== "" ? Number(v) : undefined;
  };
  const mc = num("max_concurrent"); if (mc !== undefined) overrides.max_concurrent = mc;
  const cpf = num("connections_per_file"); if (cpf !== undefined) overrides.connections_per_file = cpf;
  const rt = num("retry_times"); if (rt !== undefined) overrides.retry_times = rt;
  const to = num("timeout"); if (to !== undefined) overrides.timeout = to;
  const sp = fd.get("save_path")?.toString().trim(); if (sp) overrides.save_path = sp;
  if (fd.get("skip_tls_verify") === "on") overrides.skip_tls_verify = true;
  if (fd.get("dry_run") === "on") overrides.dry_run = false;
  return overrides;
}

// ==================== 工作流 ====================
function scheduleLabel(s) {
  if (!s) return "-";
  switch (s.type) {
    case "once": return `一次性 ${fmtTime(s.at)}`;
    case "interval": return `每 ${Math.round(s.seconds / 60)} 分钟`;
    case "cron": return `Cron ${s.expression}`;
    case "daily": return `每天 ${String(s.hour).padStart(2, "0")}:${String(s.minute).padStart(2, "0")}`;
    case "weekly": {
      const days = ["周日", "周一", "周二", "周三", "周四", "周五", "周六"];
      return `每周${days[s.weekday] || s.weekday} ${String(s.hour).padStart(2, "0")}:${String(s.minute).padStart(2, "0")}`;
    }
    default: return s.type;
  }
}

function renderWorkflows(wfs) {
  $("#workflow-body").innerHTML = wfs.map((w) => `<tr>
    <td>${w.enable ? pill("enabled") : pill("disabled")}</td>
    <td><a href="#" class="wf-link" data-wf-id="${w.id}">${w.name}</a></td>
    <td>${scheduleLabel(w.schedule)}</td>
    <td>${w.next_run_at ? fmtTime(w.next_run_at) : "-"}</td>
    <td>${w.task_ids.length}</td>
    <td>${w.target}</td>
    <td>${w.last_run_status ? pill(w.last_run_status) : "-"}</td>
    <td class="actions">
      <button class="ghost" data-wf-toggle="${w.id}">${w.enable ? "禁用" : "启用"}</button>
      <button class="ghost" data-wf-trigger="${w.id}">触发</button>
      <button class="danger" data-wf-del="${w.id}">删除</button>
    </td>
  </tr>`).join("");
}

function renderTaskPicker(tasks) {
  const list = $("#task-picker-list");
  if (!list) return;
  list.innerHTML = tasks.map((t) =>
    `<label class="chk-row"><input type="checkbox" name="wf_task" value="${t.id}" /> ${t.name} <span class="mono">${t.filename}</span></label>`
  ).join("") || `<div class="hint">暂无任务，请先在「任务」页面创建。</div>`;
}

function renderNodePicker(nodes) {
  const list = $("#node-picker-list");
  if (!list) return;
  const online = nodes.filter((n) => n.status !== "offline");
  list.innerHTML = online.map((n) =>
    `<label class="chk-row"><input type="checkbox" name="wf_node" value="${n.id}" /> ${n.hostname} <span class="mono">${n.platform}</span></label>`
  ).join("") || `<div class="hint">暂无在线节点。</div>`;
}

function buildScheduleFromForm(fd) {
  const type = fd.get("schedule_type");
  switch (type) {
    case "once": {
      const at = fd.get("once_at");
      if (!at) throw new Error("请选择执行时间");
      return { type: "once", at: new Date(at).toISOString() };
    }
    case "interval": {
      const mins = Number(fd.get("interval_minutes") || 60);
      return { type: "interval", seconds: mins * 60 };
    }
    case "daily": {
      const t = fd.get("daily_time") || "02:00";
      const [h, m] = t.split(":").map(Number);
      return { type: "daily", hour: h, minute: m };
    }
    case "weekly": {
      const day = Number(fd.get("weekly_day") || 1);
      const t = fd.get("weekly_time") || "02:00";
      const [h, m] = t.split(":").map(Number);
      return { type: "weekly", weekday: day, hour: h, minute: m };
    }
    case "cron": {
      const expr = fd.get("cron_expr")?.trim();
      if (!expr) throw new Error("请输入 Cron 表达式");
      return { type: "cron", expression: expr };
    }
    default:
      throw new Error("未知定时规则类型");
  }
}

async function renderWorkflowDetail(wfId) {
  try {
    const detail = await api(`/api/v1/workflows/${wfId}`);
    const wf = detail.workflow;
    const runs = detail.runs;
    currentWorkflowId = wfId;
    $("#wf-detail-name").textContent = wf.name;
    $("#wf-detail-meta").innerHTML = `
      <div><b>状态：</b>${wf.enable ? "启用" : "禁用"}</div>
      <div><b>定时规则：</b>${scheduleLabel(wf.schedule)}</div>
      <div><b>下次执行：</b>${wf.next_run_at ? fmtTime(wf.next_run_at) : "-"}</div>
      <div><b>上次执行：</b>${wf.last_run_at ? fmtTime(wf.last_run_at) : "-"} ${wf.last_run_status ? pill(wf.last_run_status) : ""}</div>
      <div><b>节点策略：</b>${wf.target}${wf.target === "nodes" && wf.node_ids.length ? ` (${wf.node_ids.length} 个)` : ""}</div>
      <div><b>创建时间：</b>${fmtTime(wf.created_at)}</div>
    `;
    const taskNames = wf.task_ids.map((id) => {
      const t = cachedTasks.find((x) => x.id === id);
      return t ? t.name : id.substring(0, 8);
    });
    $("#wf-detail-tasks").innerHTML = taskNames.map((n, i) =>
      `<div class="row-item"><span class="mono">${i + 1}.</span> ${n}</div>`
    ).join("") || `<div class="hint">无任务</div>`;
    $("#wf-detail-runs").innerHTML = runs.map((r) => `<tr>
      <td>${fmtTime(r.triggered_at)}</td>
      <td>${pill(r.status)}</td>
      <td>${r.task_count}</td>
      <td>${r.success_count}</td>
      <td>${r.failed_count}</td>
    </tr>`).join("") || `<tr><td colspan="5" class="hint">暂无执行记录</td></tr>`;
    switchView("workflow-detail");
  } catch (e) {
    alert("加载工作流详情失败: " + e.message);
  }
}

// ==================== 记录 ====================
function renderRuns(runs) {
  $("#run-body").innerHTML = runs.map((r) => `<tr>
    <td>${fmtTime(r.timestamp)}</td>
    <td>${r.task_name}<div class="mono">${r.filename}</div></td>
    <td class="mono">${r.node_id}</td>
    <td>${pill(r.status)}</td>
    <td>${fmtBytes(r.file_size)}</td>
    <td>${fmtBytes(r.downloaded_bytes)}</td>
    <td>${(r.elapsed_secs || 0).toFixed(1)}s</td>
    <td>${(r.avg_speed_mbps || 0).toFixed(1)} MB/s</td>
  </tr>`).join("");
}

// ==================== 下发 ====================
function renderArtifacts(arts) {
  $("#arts").innerHTML = arts.map((a) =>
    `<div class="row-item"><span>${a.platform}</span><span class="mono">${a.filename}</span>${a.present ? pill("success") : pill("offline")}<span>${a.present ? fmtBytes(a.size) : "未放入"}</span></div>`
  ).join("");
}

// ==================== 执行（待下发池 + 执行中） ====================
function renderExecution(dispatches, tasks, nodes) {
  const taskMap = new Map(tasks.map((t) => [t.id, t]));
  const nodeMap = new Map(nodes.map((n) => [n.id, n]));

  const pending = dispatches.filter((d) => d.state === "pending" && !d.node_id);
  const running = dispatches.filter((d) => d.state === "running" || d.state === "acked");
  const done = dispatches.filter((d) => d.state === "success" || d.state === "failed");

  // KPI
  $("#exec-kpis").innerHTML = [
    ["任务总数", String(dispatches.length)],
    ["待下发", String(pending.length)],
    ["执行中", String(running.length)],
    ["已完成", String(done.length)],
    ["成功", String(dispatches.filter((d) => d.state === "success").length)],
    ["失败", String(dispatches.filter((d) => d.state === "failed").length)],
  ].map(([k, v]) => `<div class="kpi"><span>${k}</span><b>${v}</b></div>`).join("");

  $("#exec-pending-count").textContent = pending.length;
  $("#exec-running-count").textContent = running.length;
  $("#exec-done-count").textContent = done.length;

  // 待下发列表
  $("#exec-pending-body").innerHTML = pending
    .sort((a, b) => new Date(a.created_at) - new Date(b.created_at))
    .map((d) => {
      const t = taskMap.get(d.task_id);
      const targetLabel = d.target === "nodes" ? `指定节点(${d.allowed_nodes?.length || 0})` : d.target === "all" ? "全部在线" : "任一空闲";
      return `<tr>
        <td>${t ? t.name : "未知任务"}<div class="mono">${t ? t.url.substring(0, 50) : d.task_id}</div></td>
        <td>${t ? t.filename : "-"}</td>
        <td>${fmtTime(d.created_at)}</td>
        <td>${pill(targetLabel)}</td>
      </tr>`;
    })
    .join("") || `<tr><td colspan="4" class="hint">待下发池为空</td></tr>`;

  // 执行中列表（卡片式布局，详细实时进度）
  const now = Date.now();
  // 从所有节点的 active_tasks_progress 构建进度映射（key: dispatch_id）
  const progressMap = new Map();
  for (const n of nodes) {
    if (n.active_tasks_progress && Array.isArray(n.active_tasks_progress)) {
      for (const p of n.active_tasks_progress) {
        progressMap.set(p.dispatch_id, p);
      }
    }
  }

  const runningCards = running
    .sort((a, b) => new Date(a.claimed_at || a.updated_at) - new Date(b.claimed_at || b.updated_at))
    .map((d) => {
      const t = taskMap.get(d.task_id);
      const n = d.node_id ? nodeMap.get(d.node_id) : null;
      const claimed = d.claimed_at ? new Date(d.claimed_at) : null;
      const elapsed = claimed ? Math.floor((now - claimed.getTime()) / 1000) : 0;
      const elapsedStr = elapsed > 60 ? `${Math.floor(elapsed / 60)}分${elapsed % 60}秒` : `${elapsed}秒`;

      // 从节点实时进度中查找（dispatch_id 可能是字符串或 UUID）
      const prog = progressMap.get(d.id) || progressMap.get(String(d.id));
      const percent = prog ? prog.percent : 0;
      const downloaded = prog ? prog.downloaded_bytes : 0;
      const totalSize = prog ? prog.total_size : 0;
      const speed = prog ? prog.speed_bps : 0;
      const connections = prog ? prog.active_connections : 0;
      const progElapsed = prog ? prog.elapsed_secs : elapsed;
      const progElapsedStr = progElapsed > 60 ? `${Math.floor(progElapsed / 60)}分${Math.floor(progElapsed % 60)}秒` : `${Math.floor(progElapsed)}秒`;

      return `<div class="run-card">
        <div class="run-card-head">
          <div class="run-card-title">
            ${t ? t.name : "未知任务"}
            <div class="mono">${t ? t.filename : d.task_id}</div>
          </div>
          <div class="run-card-node">
            <b>${n ? n.hostname : (d.node_id || "-")}</b>
            <div class="mono">${n ? n.platform : ""}</div>
          </div>
        </div>
        <div class="progress-bar">
          <div class="progress-fill" style="width: ${Math.min(percent, 100).toFixed(1)}%"></div>
        </div>
        <div class="progress-meta">
          <div class="item">
            <span class="label">进度</span>
            <span class="value">${percent.toFixed(1)}%</span>
          </div>
          <div class="item">
            <span class="label">已下载 / 总大小</span>
            <span class="value">${fmtBytes(downloaded)} / ${totalSize > 0 ? fmtBytes(totalSize) : "?"}</span>
          </div>
          <div class="item">
            <span class="label">下载速度</span>
            <span class="value speed">${fmtBytes(speed)}/s</span>
          </div>
          <div class="item">
            <span class="label">连接数 / 已耗时</span>
            <span class="value">${connections} 连接 / ${progElapsedStr}</span>
          </div>
        </div>
      </div>`;
    })
    .join("");

  $("#exec-running-cards").innerHTML = runningCards || `<div class="run-card-empty">暂无执行中任务</div>`;

  // 最近完成列表
  $("#exec-done-body").innerHTML = done
    .sort((a, b) => new Date(b.updated_at) - new Date(a.updated_at))
    .slice(0, 50)
    .map((d) => {
      const t = taskMap.get(d.task_id);
      const n = d.node_id ? nodeMap.get(d.node_id) : null;
      return `<tr>
        <td>${pill(d.state)}</td>
        <td>${t ? t.name : "未知任务"}<div class="mono">${t ? t.filename : d.task_id}</div></td>
        <td>${n ? n.hostname : `<span class="mono">${d.node_id || "-"}</span>`}</td>
        <td>${fmtTime(d.updated_at)}</td>
        <td>${d.claimed_at ? fmtTime(d.claimed_at) : "-"}</td>
      </tr>`;
    })
    .join("") || `<tr><td colspan="5" class="hint">暂无完成记录</td></tr>`;
}

// ==================== 导航 ====================
function switchView(view) {
  currentView = view;
  $$(".view").forEach((v) => v.classList.add("hidden"));
  const el = $(`#view-${view}`);
  if (el) el.classList.remove("hidden");
  $$(".nav").forEach((b) => b.classList.toggle("on", b.dataset.view === view));
  const titles = { dash: "总览", nodes: "节点", tasks: "任务", workflows: "调度", execution: "执行", "workflow-detail": "工作流详情", runs: "记录", ship: "部署hub" };
  $("#title").textContent = titles[view] || view;
}

// ==================== refresh ====================
async function refresh() {
  $("#clock").textContent = new Date().toLocaleString();
  try {
    const [ov, nodes, tasks, runs, arts, defaults, workflows, dispatches] = await Promise.all([
      api("/api/v1/overview"),
      api("/api/v1/nodes"),
      api("/api/v1/tasks"),
      api("/api/v1/runs?limit=100"),
      api("/api/v1/artifacts").catch(() => []),
      api("/api/v1/defaults").catch(() => null),
      api("/api/v1/workflows").catch(() => []),
      api("/api/v1/dispatches").catch(() => []),
    ]);
    cachedTasks = tasks;
    cachedNodes = nodes;
    cachedWorkflows = workflows;
    cachedDispatches = dispatches;
    if (defaults) applyDefaults(defaults);
    renderKpis(ov);
    $("#dash-nodes").innerHTML = (nodes.slice(0, 8).map((n) =>
      `<div class="row-item">${dot(n.status)}<span>${n.hostname}</span><span class="mono">${n.platform}</span>${pill(n.status)}</div>`
    ).join("")) || `<div class="hint">还没有节点。用 agent 接入或执行安装脚本。</div>`;
    $("#dash-runs").innerHTML = (runs.slice(0, 8).map((r) =>
      `<div class="row-item"><span>${r.task_name}</span>${pill(r.status)}<span class="mono">${fmtBytes(r.downloaded_bytes)}</span></div>`
    ).join("")) || `<div class="hint">暂无运行记录。</div>`;
    renderNodes(nodes);
    renderTasks(tasks);
    renderWorkflows(workflows);
    renderRuns(runs);
    renderArtifacts(arts);
    renderExecution(dispatches, tasks, nodes);
    // 重渲染任务/节点选择器前保存勾选状态，避免刷新丢失
    const checkedTasks = Array.from($$('[name="wf_task"]:checked')).map((el) => el.value);
    const checkedNodes = Array.from($$('[name="wf_node"]:checked')).map((el) => el.value);
    renderTaskPicker(tasks);
    renderNodePicker(nodes);
    checkedTasks.forEach((id) => { const el = document.querySelector(`[name="wf_task"][value="${id}"]`); if (el) el.checked = true; });
    checkedNodes.forEach((id) => { const el = document.querySelector(`[name="wf_node"][value="${id}"]`); if (el) el.checked = true; });
    // 如果当前在工作流详情页，刷新详情
    if (currentView === "workflow-detail" && currentWorkflowId) {
      try {
        const detail = await api(`/api/v1/workflows/${currentWorkflowId}`);
        const wf = detail.workflow;
        const wfRuns = detail.runs;
        $("#wf-detail-meta").innerHTML = `
          <div><b>状态：</b>${wf.enable ? "启用" : "禁用"}</div>
          <div><b>定时规则：</b>${scheduleLabel(wf.schedule)}</div>
          <div><b>下次执行：</b>${wf.next_run_at ? fmtTime(wf.next_run_at) : "-"}</div>
          <div><b>上次执行：</b>${wf.last_run_at ? fmtTime(wf.last_run_at) : "-"} ${wf.last_run_status ? pill(wf.last_run_status) : ""}</div>
          <div><b>节点策略：</b>${wf.target}</div>
          <div><b>创建时间：</b>${fmtTime(wf.created_at)}</div>
        `;
        $("#wf-detail-runs").innerHTML = wfRuns.map((r) => `<tr>
          <td>${fmtTime(r.triggered_at)}</td>
          <td>${pill(r.status)}</td>
          <td>${r.task_count}</td>
          <td>${r.success_count}</td>
          <td>${r.failed_count}</td>
        </tr>`).join("") || `<tr><td colspan="5" class="hint">暂无执行记录</td></tr>`;
      } catch (_) {}
    }
    // 安装脚本提示
    const origin = location.origin;
    $("#install-hint").textContent =
`# Linux / macOS
curl -fsSL ${origin}/install.sh | sh
./spde-node/bin/spde agent --master ${origin}

# Windows PowerShell
irm ${origin}/install.ps1 | iex
.\\spde-node\\bin\\spde.exe agent --master ${origin}

# 已有二进制时：
spde agent --master ${origin}`;
  } catch (e) {
    console.warn(e);
  }
}

// ==================== 事件监听 ====================

// 导航
$$(".nav").forEach((btn) => {
  btn.addEventListener("click", () => switchView(btn.dataset.view));
});

// 通用点击（删除节点、删除任务、取消任务、触发工作流、删除工作流、工作流链接）
document.addEventListener("click", async (e) => {
  const t = e.target;
  try {
    if (t.dataset.delNode) {
      const row = t.closest("tr");
      const nodeName = row?.querySelector("td:nth-child(2)")?.textContent?.trim()?.split("\n")[0]?.trim() || "该节点";
      if (!confirm("确认移除节点「" + nodeName + "」？\n\n删除后节点将失去心跳，spde 会重新注册为待审批状态。")) return;
      await api(`/api/v1/nodes/${t.dataset.delNode}`, { method: "DELETE" });
    } else if (t.dataset.editCap) {
      await editNodeCapabilities(t.dataset.editCap);
    } else if (t.dataset.approve) {
      await api(`/api/v1/nodes/${t.dataset.approve}/approve`, { method: "POST" });
    } else if (t.dataset.reject) {
      if (!confirm("拒绝该节点？节点仍保留在列表中，可随时点同意。")) return;
      await api(`/api/v1/nodes/${t.dataset.reject}/reject`, { method: "POST" });
    } else if (t.dataset.delTask) {
      const taskName = t.closest("tr")?.querySelector("td:nth-child(2)")?.textContent?.trim() || "该任务";
      const delFile = confirm(`删除任务「${taskName}」？\n\n点击「确定」同时删除已下载的文件，点击「取消」仅删除任务记录。`);
      const url = delFile
        ? `/api/v1/tasks/${t.dataset.delTask}?delete_file=true`
        : `/api/v1/tasks/${t.dataset.delTask}`;
      await api(url, { method: "DELETE" });
    } else if (t.dataset.cancel) {
      await api(`/api/v1/tasks/${t.dataset.cancel}/cancel`, { method: "POST" });
    } else if (t.dataset.wfTrigger) {
      await api(`/api/v1/workflows/${t.dataset.wfTrigger}/trigger`, { method: "POST" });
      alert("已触发");
    } else if (t.dataset.wfToggle) {
      const wf = cachedWorkflows.find((w) => w.id === t.dataset.wfToggle);
      const newEnable = wf ? !wf.enable : true;
      await api(`/api/v1/workflows/${t.dataset.wfToggle}`, {
        method: "PUT",
        body: JSON.stringify({ enable: newEnable }),
      });
    } else if (t.dataset.wfDel) {
      if (!confirm("确定删除此工作流？")) return;
      await api(`/api/v1/workflows/${t.dataset.wfDel}`, { method: "DELETE" });
    } else if (t.classList.contains("wf-link")) {
      e.preventDefault();
      renderWorkflowDetail(t.dataset.wfId);
    } else if (t.id === "purge-offline") {
      await api("/api/v1/nodes", { method: "DELETE" });
    } else {
      return;
    }
    refresh();
  } catch (err) {
    alert(err.message);
  }
});

// 任务表单提交
$("#task-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const fd = new FormData(e.target);
  if (!fd.get("name") || !fd.get("url") || !fd.get("filename")) {
    alert("名称/URL/文件名必填");
    return;
  }
  const overrides = buildOverridesFromForm(fd);
  try {
    await api("/api/v1/tasks", {
      method: "POST",
      body: JSON.stringify({
        name: fd.get("name"),
        url: fd.get("url"),
        filename: fd.get("filename"),
        enable: fd.get("enable_init") !== "false",
        note: "",
        overrides,
      }),
    });
    e.target.reset();
    refresh();
  } catch (err) {
    alert(err.message);
  }
});

// 定时规则类型切换
$("#schedule-type")?.addEventListener("change", (e) => {
  const type = e.target.value;
  $$(".sch-field").forEach((el) => {
    el.classList.toggle("hidden", el.dataset.sch !== type);
  });
});

// 节点策略切换
document.querySelector('[name="target"]')?.addEventListener("change", (e) => {
  const wrap = $("#node-picker-wrap");
  if (wrap) wrap.classList.toggle("hidden", e.target.value !== "nodes");
});

// 工作流表单提交
$("#workflow-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  try {
    const fd = new FormData(e.target);
    const name = fd.get("name")?.toString().trim();
    if (!name) { alert("工作流名称必填"); return; }
    const taskIds = Array.from($$('[name="wf_task"]:checked')).map((el) => el.value);
    if (taskIds.length === 0) { alert("请至少选择一个任务"); return; }
    const target = fd.get("target") || "any";
    const nodeIds = target === "nodes" ? Array.from($$('[name="wf_node"]:checked')).map((el) => el.value) : [];
    const schedule = buildScheduleFromForm(fd);
    await api("/api/v1/workflows", {
      method: "POST",
      body: JSON.stringify({
        name,
        enable: fd.get("enable_init") !== "false",
        schedule,
        task_ids: taskIds,
        target,
        node_ids: nodeIds,
      }),
    });
    e.target.reset();
    $$(".sch-field").forEach((el) => el.classList.toggle("hidden", el.dataset.sch !== "interval"));
    $("#node-picker-wrap")?.classList.add("hidden");
    refresh();
  } catch (err) {
    alert("创建失败: " + err.message);
  }
});

// 工作流详情页按钮
$("#wf-back")?.addEventListener("click", () => switchView("workflows"));
$("#wf-trigger")?.addEventListener("click", async () => {
  if (!currentWorkflowId) return;
  try {
    await api(`/api/v1/workflows/${currentWorkflowId}/trigger`, { method: "POST" });
    alert("已触发");
    refresh();
  } catch (e) { alert(e.message); }
});
$("#wf-toggle")?.addEventListener("click", async () => {
  if (!currentWorkflowId) return;
  try {
    const wf = await api(`/api/v1/workflows/${currentWorkflowId}`);
    await api(`/api/v1/workflows/${currentWorkflowId}`, {
      method: "PUT",
      body: JSON.stringify({ enable: !wf.enable }),
    });
    refresh();
  } catch (e) { alert(e.message); }
});
$("#wf-delete")?.addEventListener("click", async () => {
  if (!currentWorkflowId) return;
  if (!confirm("确定删除此工作流？")) return;
  try {
    await api(`/api/v1/workflows/${currentWorkflowId}`, { method: "DELETE" });
    currentWorkflowId = null;
    switchView("workflows");
    refresh();
  } catch (e) { alert(e.message); }
});

// 加载版本信息（pcdn-keeper 场景显示 pk + spde 组合版本）
async function loadVersion() {
  try {
    const v = await api("/api/v1/version");
    const el = document.getElementById("rail-version");
    if (!el) return;
    if (v.pcdn_keeper_version) {
      el.textContent = `pcdn-keeper ${v.pcdn_keeper_version}`;
    } else if (v.spde_version) {
      el.textContent = `pk v${v.pk_version} / spde v${v.spde_version}`;
    } else {
      el.textContent = `pk v${v.pk_version}`;
    }
  } catch (e) {
    const el = document.getElementById("rail-version");
    if (el) el.textContent = "pk · version unknown";
  }
}

loadVersion();
refresh();
setInterval(refresh, 5000);
// 右上角时钟每秒更新
setInterval(() => { const el = $("#clock"); if (el) el.textContent = new Date().toLocaleString(); }, 1000);

// ==================== WebSocket 实时推送 ====================
let wsRealtime = null;
let wsReconnectTimer = null;

function connectRealtimeWS() {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  const wsUrl = `${proto}//${location.host}/api/v1/realtime/ws`;
  try {
    wsRealtime = new WebSocket(wsUrl);
  } catch (e) {
    scheduleReconnect();
    return;
  }

  wsRealtime.onopen = () => {
    console.log("[realtime-ws] 已连接");
    if (wsReconnectTimer) {
      clearTimeout(wsReconnectTimer);
      wsReconnectTimer = null;
    }
  };

  wsRealtime.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.type === "realtime") {
        handleRealtimeData(msg);
      }
    } catch (e) {
      // 忽略解析错误
    }
  };

  wsRealtime.onclose = () => {
    console.log("[realtime-ws] 连接关闭，5秒后重连");
    scheduleReconnect();
  };

  wsRealtime.onerror = () => {
    console.log("[realtime-ws] 连接错误");
    wsRealtime?.close();
  };
}

function scheduleReconnect() {
  if (wsReconnectTimer) return;
  wsReconnectTimer = setTimeout(() => {
    wsReconnectTimer = null;
    connectRealtimeWS();
  }, 5000);
}

// 全量刷新节流（避免WebSocket频繁触发导致HTTP请求风暴）
let lastFullRefresh = 0;
const FULL_REFRESH_INTERVAL = 2000; // 最少2秒一次全量刷新

function handleRealtimeData(msg) {
  const realtimeNodes = msg.nodes || [];

  // 1. 立即更新节点实时字段（速度、进度）- 50ms级别，进度条流畅
  if (cachedNodes && Array.isArray(cachedNodes)) {
    for (const node of cachedNodes) {
      const rt = realtimeNodes.find((n) => n.node_id === node.id);
      if (rt) {
        node.total_speed_bps = rt.total_speed_bps;
        node.active_tasks_progress = rt.active_tasks || [];
        node.status = rt.status;
        node.last_seen = rt.last_seen;
      }
    }
  }

  // 2. 立即重渲染当前页面（用更新后的节点数据）- 进度条、速度实时显示
  if (currentView === "nodes") {
    renderNodes(cachedNodes || []);
  } else if (currentView === "execution") {
    if (cachedDispatches && cachedTasks && cachedNodes) {
      renderExecution(cachedDispatches, cachedTasks, cachedNodes);
    }
  } else if (currentView === "dash") {
    if (cachedNodes) {
      $("#dash-nodes").innerHTML = (cachedNodes.slice(0, 8).map((n) =>
        `<div class="row-item">${dot(n.status)}<span>${n.hostname}</span><span class="mono">${n.platform}</span>${pill(n.status)}</div>`
      ).join("")) || `<div class="hint">还没有节点。用 agent 接入或执行安装脚本。</div>`;
    }
  }

  // 3. 节流全量刷新 - 确保任务状态、分发列表、工作流等也是最新的
  //    任何数据变化（创建任务、状态变更等）都会触发WebSocket推送
  //    全量刷新最多2秒一次，避免HTTP请求风暴
  const now = Date.now();
  if (now - lastFullRefresh >= FULL_REFRESH_INTERVAL) {
    lastFullRefresh = now;
    refresh();
  }
}

// 启动 WebSocket 连接
connectRealtimeWS();
