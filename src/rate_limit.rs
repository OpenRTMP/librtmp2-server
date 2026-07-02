//! Per-client HTTP rate limiting (in-process sliding window).

use axum::extract::{ConnectInfo, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<Mutex<HashMap<String, Vec<Instant>>>>,
    window: Duration,
    trusted_proxies: Arc<Vec<IpAddr>>,
}

impl RateLimiter {
    pub fn new(trusted_proxies: Vec<IpAddr>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            window: Duration::from_secs(60),
            trusted_proxies: Arc::new(trusted_proxies),
        }
    }

    fn check(&self, key: &str, max_requests: usize) -> bool {
        let now = Instant::now();
        let mut guard = self.inner.lock();
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

fn client_ip(request: &Request, trusted_proxies: &[IpAddr]) -> IpAddr {
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0.ip())
        .unwrap_or(IpAddr::from([127, 0, 0, 1]));

    if trusted_proxies.iter().any(|proxy| *proxy == peer) {
        if let Some(xff) = request
            .headers()
            .get("X-Forwarded-For")
            .and_then(|v| v.to_str().ok())
        {
            if let Some(client) = xff
                .split(',')
                .map(str::trim)
                .find(|part| !part.is_empty())
            {
                if let Ok(ip) = client.parse::<IpAddr>() {
                    return ip;
                }
            }
        }
    }

    peer
}

pub async fn middleware(
    State(limiter): State<RateLimiter>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    let peer = client_ip(&request, limiter.trusted_proxies.as_slice());
    let key = format!(
        "{}:{}",
        peer,
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
