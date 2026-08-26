const $ = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => [...r.querySelectorAll(s)];

const tokenInput = $("#token");
tokenInput.value = localStorage.getItem("pk_token") || "";
tokenInput.addEventListener("change", () => localStorage.setItem("pk_token", tokenInput.value.trim()));

async function api(path, opts = {}) {
  const headers = { ...(opts.headers || {}) };
  const t = tokenInput.value.trim();
  if (t) headers.Authorization = `Bearer ${t}`;
  if (opts.body && typeof opts.body === "object" && !(opts.body instanceof FormData)) {
    headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(opts.body);
  }
  const res = await fetch(path, { ...opts, headers });
  if (res.status === 204) return null;
  const text = await res.text();
  if (!res.ok) throw new Error(text || res.statusText);
  if (!text) return null;
  try { return JSON.parse(text); } catch { return text; }
}

function fmtBytes(n) {
  n = Number(n) || 0;
  if (n < 1024) return n + " B";
  if (n < 1024 ** 2) return (n / 1024).toFixed(1) + " KB";
  if (n < 1024 ** 3) return (n / 1024 ** 2).toFixed(1) + " MB";
  return (n / 1024 ** 3).toFixed(2) + " GB";
}
function fmtTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleString();
}
function pill(status) {
  const cls = {
    online: "ok", offline: "", busy: "run",
    success: "ok", skipped: "ok", failed: "fail",
    running: "run", queued: "run", completed: "ok", cancelled: "",
    draft: "", pending: "run", acked: "run",
  }[status] || "";
  return `<span class="pill ${cls}">${status}</span>`;
}
function dot(status) {
  const c = status === "online" ? "online" : status === "busy" ? "busy" : "offline";
  return `<span class="dot ${c}"></span>`;
}

$$(".nav").forEach((b) => {
  b.addEventListener("click", () => {
    $$(".nav").forEach((x) => x.classList.remove("on"));
    b.classList.add("on");
    const v = b.dataset.view;
    $$(".view").forEach((el) => el.classList.add("hidden"));
    $(`#view-${v}`).classList.remove("hidden");
    const names = { dash: "总览", nodes: "节点", tasks: "任务", runs: "记录", ship: "下发" };
    $("#title").textContent = names[v];
  });
});

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

function renderKpis(o) {
  if (o.version) $("#pk-version").textContent = "v" + o.version;
  const items = [
    ["在线节点", `${o.nodes_online} / ${o.nodes_total}`],
    ["运行中任务", String(o.tasks_running)],
    ["待下发", String(o.dispatches_pending)],
    ["累计下载", fmtBytes(o.bytes_downloaded)],
    ["成功次数", String(o.runs_success)],
    ["失败次数", String(o.runs_failed)],
    ["平均速度", (o.avg_speed_mbps || 0).toFixed(1) + " MB/s"],
    ["任务总数", String(o.tasks_total)],
  ];
  $("#kpis").innerHTML = items.map(([k, v]) => `<div class="kpi"><span>${k}</span><b>${v}</b></div>`).join("");
}

async function refresh() {
  $("#clock").textContent = new Date().toLocaleString();
  try {
    const [ov, nodes, tasks, runs, arts, defaults] = await Promise.all([
      api("/api/v1/overview"),
      api("/api/v1/nodes"),
      api("/api/v1/tasks"),
      api(" /api/v1/runs".trim()),
      api("/api/v1/artifacts"),
      api("/api/v1/defaults").catch(() => null),
    ]);
    if (defaults) applyDefaults(defaults);
    renderKpis(ov);
    $("#dash-nodes").innerHTML = (nodes.slice(0, 8).map((n) =>
      `<div class="row-item">${dot(n.status)}<span>${n.hostname}</span><span class="mono">${n.platform}</span>${pill(n.status)}</div>`
    ).join("")) || `<div class="hint">还没有节点。用 agent 接入或执行安装脚本。</div>`;
    $("#dash-runs").innerHTML = (runs.slice(0, 8).map((r) =>
      `<div class="row-item"><span>${r.task_name}</span>${pill(r.status)}<span class="mono">${fmtBytes(r.downloaded_bytes)}</span></div>`
    ).join("")) || `<div class="hint">暂无运行记录。</div>`;

    $("#node-body").innerHTML = nodes.map((n) => `<tr>
      <td>${dot(n.status)}${pill(n.status)}</td>
      <td>${n.hostname}<div class="mono">${n.id}</div></td>
      <td>${n.platform} / ${n.arch}</td>
      <td>${n.version}</td>
      <td>${n.active_tasks}</td>
      <td>${fmtBytes(n.bytes_downloaded)}</td>
      <td>${fmtTime(n.last_seen)}</td>
      <td class="actions"><button class="ghost" data-del-node="${n.id}">移除</button></td>
    </tr>`).join("");

    $("#task-body").innerHTML = tasks.map((t) => `<tr>
      <td>${pill(t.status)}</td>
      <td>${t.name}<div class="mono">${t.url}</div></td>
      <td>${t.filename}</td>
      <td>${t.target}${t.node_ids?.length ? ` (${t.node_ids.length})` : ""}</td>
      <td>${fmtTime(t.created_at)}</td>
      <td class="actions">
        <button class="ghost" data-dispatch="${t.id}">下发</button>
        <button class="ghost" data-cancel="${t.id}">取消</button>
        <button class="danger" data-del-task="${t.id}">删除</button>
      </td>
    </tr>`).join("");

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

    $("#arts").innerHTML = arts.map((a) =>
      `<div class="row-item"><span>${a.platform}</span><span class="mono">${a.filename}</span>${a.present ? pill("success") : pill("offline")}<span>${a.present ? fmtBytes(a.size) : "未放入"}</span></div>`
    ).join("");

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

document.addEventListener("click", async (e) => {
  const t = e.target;
  try {
    if (t.dataset.delNode) {
      await api(`/api/v1/nodes/${t.dataset.delNode}`, { method: "DELETE" });
    } else if (t.dataset.delTask) {
      const taskName = t.closest("tr")?.querySelector("td:nth-child(2)")?.textContent?.trim() || "该任务";
      const delFile = confirm(`删除任务「${taskName}」？\n\n点击「确定」同时删除已下载的文件，点击「取消」仅删除任务记录。`);
      const url = delFile
        ? `/api/v1/tasks/${t.dataset.delTask}?delete_file=true`
        : `/api/v1/tasks/${t.dataset.delTask}`;
      await api(url, { method: "DELETE" });
    } else if (t.dataset.dispatch) {
      await api(`/api/v1/tasks/${t.dataset.dispatch}/dispatch`, { method: "POST" });
    } else if (t.dataset.cancel) {
      await api(`/api/v1/tasks/${t.dataset.cancel}/cancel`, { method: "POST" });
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

$("#task-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const fd = new FormData(e.target);
  const node_ids = (fd.get("node_ids") || "")
    .toString()
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  // 构建任务级参数覆盖（只填了的才传）
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
  // 勾选"落盘"→ dry_run=false；不勾选→不传，用 config 默认值（默认 dry_run=true 不落盘）
  if (fd.get("dry_run") === "on") overrides.dry_run = false;

  try {
    await api("/api/v1/tasks", {
      method: "POST",
      body: {
        name: fd.get("name"),
        url: fd.get("url"),
        filename: fd.get("filename"),
        enable: fd.get("enable") === "on",
        dispatch_now: fd.get("dispatch_now") === "on",
        target: fd.get("target"),
        node_ids,
        note: "",
        overrides,
      },
    });
    e.target.reset();
    e.target.enable.checked = true;
    e.target.dispatch_now.checked = true;
    refresh();
  } catch (err) {
    alert(err.message);
  }
});

refresh();
setInterval(refresh, 3000);
