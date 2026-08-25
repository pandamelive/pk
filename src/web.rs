use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;

const INDEX: &str = include_str!("../web/index.html");
const CSS: &str = include_str!("../web/style.css");
const JS: &str = include_str!("../web/app.js");

pub fn mount(app: Router) -> Router {
    app.route("/", get(index))
        .route("/assets/style.css", get(css))
        .route("/assets/app.js", get(js))
}

async fn index() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        INDEX,
    )
}

async fn css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        CSS,
    )
}

async fn js() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        JS,
    )
}

pub fn not_found() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "not found")
}
