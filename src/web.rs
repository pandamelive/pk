use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use std::path::PathBuf;

const INDEX: &str = include_str!("../web/index.html");
const CSS: &str = include_str!("../web/style.css");
const JS: &str = include_str!("../web/app.js");
const FAVICON: &[u8] = include_bytes!("../web/favicon.png");

/// 开发模式：设置 PK_DEV_WEB=1 后从本地 web/ 目录实时读取，改完刷新浏览器即可
fn dev_mode() -> bool {
    std::env::var("PK_DEV_WEB")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false)
}

fn dev_read(filename: &str) -> Option<String> {
    if !dev_mode() {
        return None;
    }
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("web")
        .join(filename);
    std::fs::read_to_string(&path).ok()
}

fn dev_read_bytes(filename: &str) -> Option<Vec<u8>> {
    if !dev_mode() {
        return None;
    }
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("web")
        .join(filename);
    std::fs::read(&path).ok()
}

pub fn mount(app: Router) -> Router {
    app.route("/", get(index))
        .route("/favicon.ico", get(favicon))
        .route("/favicon.png", get(favicon))
        .route("/assets/style.css", get(css))
        .route("/assets/app.js", get(js))
}

async fn index() -> impl IntoResponse {
    let body = dev_read("index.html").unwrap_or_else(|| INDEX.to_string());
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], body)
}

async fn css() -> impl IntoResponse {
    let body = dev_read("style.css").unwrap_or_else(|| CSS.to_string());
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], body)
}

async fn js() -> impl IntoResponse {
    let body = dev_read("app.js").unwrap_or_else(|| JS.to_string());
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        body,
    )
}

async fn favicon() -> impl IntoResponse {
    let body = dev_read_bytes("favicon.png").unwrap_or_else(|| FAVICON.to_vec());
    ([(header::CONTENT_TYPE, "image/png")], body)
}

pub fn not_found() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "not found")
}
