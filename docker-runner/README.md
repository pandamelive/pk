# Docker 化 GitHub Actions Rust Runner

预装 Rust 工具链、sccache 编译缓存、cross 交叉编译的自托管 GitHub Actions Runner。

## 快速开始

### 1. 获取 Runner Token

在 GitHub 仓库页面：
`Settings` → `Actions` → `Runners` → `New self-hosted runner` → 复制 token

或者用 PAT 一键生成：
```bash
curl -s -X POST \
  -H "Authorization: token 你的PAT" \
  https://api.github.com/repos/pandamelive/pk/actions/runners/registration-token \
  | jq -r .token
```

### 2. 配置环境变量

```bash
cp .env.example .env
# 编辑 .env，填入 REPO_URL 和 RUNNER_TOKEN
```

### 3. 构建并启动

```bash
docker compose up -d --build
```

### 4. 验证

去 GitHub 仓库 `Settings → Actions → Runners`，能看到 runner 状态为 Idle。

## 在 Workflow 中使用

```yaml
jobs:
  build:
    runs-on: [self-hosted, docker, rust]
    steps:
      - uses: actions/checkout@v4
      - run: cargo build --release
```

## 特性

- **Rust stable**：预装 x86_64 linux gnu/musl、aarch64 musl、wasm32 target
- **sccache**：编译缓存持久化到 Docker volume，增量编译快 5-10 倍
- **cross**：支持交叉编译 aarch64 等目标
- **mold 链接器**：比默认 ld 快数倍
- **自动注销**：容器停止时自动从 GitHub 移除 runner
- **非 root 运行**：安全

## 跑多个 Runner 实例

```bash
docker compose up -d --scale rust-runner=3
```

每个实例会自动用不同的名称（带 hostname 后缀）。

## 注意事项

- RUNNER_TOKEN 有效期 1 小时，过期了重新生成
- 组织级 runner 把 REPO_URL 改成 `https://github.com/你的组织名`
- cross 交叉编译需要挂载 docker.sock（已配置）
- sccache 缓存默认存在 Docker volume 里，不会随容器重建丢失
