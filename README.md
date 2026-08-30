# PK

PandaNetPL 生态主控：生成、下发并控制 [SPDE](https://github.com/pandamelive/spde) 节点。Rust 实现，内置 Web UI。

## 生态标准

本项目属于 **PandaNetOS 生态项目群**，遵循全系统权威标准仓库 [PandaNetOS](https://github.com/PandaNetOS/PandaNetOS) 的规范。

### 标准库路径约定

强制依赖 `pandanetos` 共享标准库，使用 **path 依赖**，目录布局固定：

```
<workspace>/
├── PandaNetOS/              # 标准库仓库（必须与 pk 同级）
│   └── crates/pandanetos/
└── pk/                      # 本仓库
    └── Cargo.toml           # pandanetos = { path = "../PandaNetOS/crates/pandanetos" }
```

`Cargo.toml` 中的依赖声明：

```toml
[dependencies]
pandanetos = { path = "../PandaNetOS/crates/pandanetos" }
```

> 克隆本仓库后，需同时克隆 `PandaNetOS/PandaNetOS` 到同级目录，否则 `cargo build` 会因找不到 path 依赖而失败。

### 规范要求

- **强制依赖** `pandanetos` 共享库，统一协议路径常量（`protocol::paths`）、响应格式（`ApiResponse`/`ApiError`）、错误码与配置标准，**禁止**维护私有协议与常量。
- **标准一致性**：API 路径、响应格式、文件布局与文档规范均以 PandaNetOS《标准规范》为准。
- 一行导入所有常用类型：`use pandanetos::prelude::*;`

- 节点注册 / 心跳 / 在线状态
- 下载任务创建与调度（任一节点 / 全部节点 / 指定节点）
- 运行记录与流量统计
- 按平台下发 SPDE 二进制与生成 `config.yaml`
- 节点 `spde agent --master` 拉任务并回报告

## 快速开始

```bash
cargo build --release
./target/release/pk serve
```

浏览器打开 `http://127.0.0.1:5566`。

工作目录在二进制同级的 `pk-controlcenter/`：

```
pk-controlcenter/
├── config.yaml          # 主控配置
└── pk-data/
    ├── state.json       # 节点 / 任务 / 调度 / 运行记录
    └── artifacts/       # 放入各平台 spde 二进制后即可下发
```

## 节点接入

在已编译的 SPDE 上：

```bash
spde agent --master http://<pk-host>:5566
```

或把对应平台二进制放到 `pk-data/artifacts/` 后，在节点上执行：

```bash
curl -fsSL http://<pk-host>:5566/install.sh | sh
```

Windows：

```powershell
irm http://<pk-host>:5566/install.ps1 | iex
```

## 调度策略

| target | 行为 |
|---|---|
| `any` | 下发到当前负载最低的一个在线节点 |
| `all` | 每个在线节点各执行一份 |
| `nodes` | 仅下发到指定 `node_ids` |

节点通过心跳领取 pending dispatch，完成后 `POST /api/v1/agent/report`。

## HTTP API（节选）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/v1/overview` | 汇总 |
| GET | `/api/v1/nodes` | 节点列表 |
| POST | `/api/v1/tasks` | 创建任务 |
| POST | `/api/v1/tasks/{id}/dispatch` | 立即调度 |
| GET | `/api/v1/runs` | 运行记录 |
| GET | `/api/v1/artifacts/{platform}` | 下载 SPDE 二进制 |
| GET | `/api/v1/nodes/{id}/config.yaml` | 生成该节点配置 |
| POST | `/api/v1/agent/register` | 节点注册 |
| POST | `/api/v1/agent/heartbeat` | 心跳 + 领任务 |
| POST | `/api/v1/agent/report` | 回写结果 |

`config.yaml` 里 `token` 非空时，上述 API 需要 `Authorization: Bearer <token>`。

## 配置

```yaml
listen: "0.0.0.0:5566"
heartbeat_timeout_secs: 45
token: ""
spde_defaults:
  max_concurrent: 4
  connections_per_file: 8
  save_path: "./download"
```
