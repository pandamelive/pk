use crate::config::PkConfig;
use crate::models::*;
use crate::ws::WsManager;
use anyhow::{Context, Result};
use chrono::Utc;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

pub struct AppState {
    pub cfg: PkConfig,
    pub work_root: PathBuf,
    pub data_dir: PathBuf,
    pub artifacts_dir: PathBuf,
    pub ws_mgr: WsManager,
    inner: RwLock<Snapshot>,
}

impl AppState {
    pub async fn open(cfg: PkConfig, work_root: PathBuf) -> Result<Arc<Self>> {
        let data_dir = cfg.resolve_data_dir(&work_root);
        tokio::fs::create_dir_all(&data_dir)
            .await
            .context("create pk-data")?;
        let artifacts_dir = data_dir.join("artifacts");
        tokio::fs::create_dir_all(&artifacts_dir)
            .await
            .context("create artifacts")?;
        write_artifact_readme(&artifacts_dir).await?;

        let snap_path = data_dir.join("state.json");
        let inner = if snap_path.exists() {
            let text = tokio::fs::read_to_string(&snap_path).await?;
            serde_json::from_str(&text).unwrap_or_default()
        } else {
            Snapshot::default()
        };

        Ok(Arc::new(Self {
            cfg,
            work_root,
            data_dir,
            artifacts_dir,
            ws_mgr: WsManager::new(),
            inner: RwLock::new(inner),
        }))
    }

    pub fn state_path(&self) -> PathBuf {
        self.data_dir.join("state.json")
    }

    pub async fn persist(&self) -> Result<()> {
        // 确保 data_dir 存在（防止运行时目录被删导致持久化失败）
        tokio::fs::create_dir_all(&self.data_dir)
            .await
            .context("create data_dir for persist")?;
        let snap = self.inner.read().await.clone();
        let tmp = self.data_dir.join("state.json.tmp");
        let json = serde_json::to_string_pretty(&snap)?;
        tokio::fs::write(&tmp, json).await?;
        tokio::fs::rename(&tmp, self.state_path()).await?;
        Ok(())
    }

    pub async fn snapshot(&self) -> Snapshot {
        self.inner.read().await.clone()
    }

    pub async fn with_mut<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Snapshot) -> T,
    {
        let mut g = self.inner.write().await;
        let out = f(&mut g);
        drop(g);
        self.persist().await?;
        Ok(out)
    }

    pub fn heartbeat_timeout(&self) -> chrono::Duration {
        chrono::Duration::seconds(self.cfg.heartbeat_timeout_secs as i64)
    }

    pub async fn refresh_online(&self) {
        let timeout = self.heartbeat_timeout();
        let now = Utc::now();
        let mut g = self.inner.write().await;
        for n in &mut g.nodes {
            if now.signed_duration_since(n.last_seen) > timeout {
                n.status = NodeStatus::Offline;
                n.active_tasks = 0;
            }
        }
    }
}

async fn write_artifact_readme(dir: &Path) -> Result<()> {
    let p = dir.join("README.txt");
    if p.exists() {
        return Ok(());
    }
    let body = r#"将各平台 SPDE 二进制放到本目录，文件名约定：

  spde-windows-x86_64.exe
  spde-linux-x86_64
  spde-linux-aarch64
  spde-macos-x86_64
  spde-macos-aarch64

节点可通过 GET /api/v1/artifacts/<platform> 拉取，例如：
  windows-x86_64 / linux-x86_64 / linux-aarch64 / macos-x86_64 / macos-aarch64
"#;
    tokio::fs::write(p, body).await?;
    Ok(())
}

pub fn artifact_filename(platform: &str) -> Option<&'static str> {
    match platform {
        "windows-x86_64" => Some("spde-windows-x86_64.exe"),
        "linux-x86_64" => Some("spde-linux-x86_64"),
        "linux-aarch64" => Some("spde-linux-aarch64"),
        "macos-x86_64" => Some("spde-macos-x86_64"),
        "macos-aarch64" => Some("spde-macos-aarch64"),
        _ => None,
    }
}

pub fn detect_host_platform() -> (String, String) {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let platform = match (os, arch) {
        ("windows", "x86_64") => "windows-x86_64",
        ("linux", "x86_64") => "linux-x86_64",
        ("linux", "aarch64") => "linux-aarch64",
        ("macos", "x86_64") => "macos-x86_64",
        ("macos", "aarch64") => "macos-aarch64",
        _ => "unknown",
    };
    (platform.into(), arch.into())
}

pub fn new_uuid() -> Uuid {
    Uuid::new_v4()
}
