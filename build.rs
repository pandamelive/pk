use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    // 构建时间（UNIX时间戳，运行时再格式化）
    let build_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    println!("cargo:rustc-env=PK_BUILD_TIMESTAMP={}", build_timestamp);

    // Git commit
    let git_commit = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=PK_GIT_COMMIT={}", git_commit);

    // Rust 版本
    let rust_version = Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=PK_RUST_VERSION={}", rust_version);

    // 目标三元组
    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=PK_TARGET_TRIPLE={}", target);

    println!("cargo:rerun-if-changed=build.rs");
}
