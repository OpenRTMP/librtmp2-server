//! Per-client HTTP rate limiting (in-process sliding window).

use axum::extract::{ConnectInfo, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<Mutex<HashMap<String, Vec<Instant>>>>,
    window: Duration,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            window: Duration::from_secs(60),
        }
    }

    fn check(&self, key: &str, max_requests: usize) -> bool {
        let now = Instant::now();
        let mut guard = self.inner.lock().unwrap();
        let entries = guard.entry(key.to_string()).or_default();
        entries.retain(|t| now.duration_since(*t) < self.window);
        if entries.len() >= max_requests {
            return false;
        }
        entries.push(now);
        true
    }
}

fn limit_for_path(path: &str) -> usize {
    if path.starts_with("/api/") {
        120
    } else if path.starts_with("/stats") {
        30
    } else {
        60
    }
}

fn client_ip(request: &Request) -> IpAddr {
    request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0.ip())
        .unwrap_or(IpAddr::from([127, 0, 0, 1]))
}

pub async fn middleware(
    State(limiter): State<RateLimiter>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    let key = format!(
        "{}:{}",
        client_ip(&request),
        path.split('/').nth(1).unwrap_or("")
    );
    let max = limit_for_path(path);
    if !limiter.check(&key, max) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [("Content-Type", "text/plain; charset=utf-8")],
            "rate limit exceeded",
        )
            .into_response();
    }
    next.run(request).await
}
