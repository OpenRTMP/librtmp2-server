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

/// Cap tracked client keys so a scan with many spoofed IPs cannot exhaust RAM.
const MAX_TRACKED_KEYS: usize = 10_000;

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

    /// Drop client keys whose sliding window has fully expired.
    fn purge_expired(&self, guard: &mut HashMap<String, Vec<Instant>>, now: Instant) {
        guard.retain(|_, entries| {
            entries.retain(|t| {
                now.checked_duration_since(*t)
                    .map_or(true, |age| age < self.window)
            });
            !entries.is_empty()
        });
    }

    fn check(&self, key: &str, max_requests: usize) -> bool {
        if max_requests == 0 {
            return false;
        }

        let mut guard = self.inner.lock();
        let now = Instant::now();

        if let Some(entries) = guard.get_mut(key) {
            entries.retain(|t| {
                now.checked_duration_since(*t)
                    .map_or(true, |age| age < self.window)
            });
            if entries.len() >= max_requests {
                return false;
            }
            if !entries.is_empty() {
                entries.push(now);
                return true;
            }
            guard.remove(key);
        }

        if guard.len() >= MAX_TRACKED_KEYS {
            self.purge_expired(&mut guard, now);
            if guard.len() >= MAX_TRACKED_KEYS {
                return false;
            }
        }

        guard.insert(key.to_string(), vec![now]);
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

    if trusted_proxies.contains(&peer)
        && let Some(xff) = request
            .headers()
            .get("X-Forwarded-For")
            .and_then(|v| v.to_str().ok())
        && let Some(client) = xff.split(',').map(str::trim).find(|part| !part.is_empty())
        && let Ok(ip) = client.parse::<IpAddr>()
    {
        return ip;
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
    let key = format!("{}:{}", peer, path.split('/').nth(1).unwrap_or(""));
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn expired_one_shot_clients_do_not_permanently_fill_tracking_map() {
        let limiter = RateLimiter::new(Vec::new());
        let max = 120;

        for i in 0..MAX_TRACKED_KEYS {
            let key = format!("10.{}.{}:api", i / 256, i % 256);
            assert!(limiter.check(&key, max), "seed request {i} should succeed");
        }
        assert_eq!(limiter.inner.lock().len(), MAX_TRACKED_KEYS);

        {
            let mut guard = limiter.inner.lock();
            let stale = Instant::now() - Duration::from_secs(61);
            for entries in guard.values_mut() {
                entries.clear();
                entries.push(stale);
            }
        }

        assert!(
            limiter.check("203.0.113.1:api", max),
            "new client must be admitted after stale entries age out"
        );
        assert!(limiter.inner.lock().len() <= MAX_TRACKED_KEYS);
    }

    #[test]
    fn active_clients_are_not_evicted_when_map_is_at_capacity() {
        let limiter = RateLimiter::new(Vec::new());
        let max = 120;
        let now = Instant::now();
        let limited_key = "198.51.100.0:api";

        {
            let mut guard = limiter.inner.lock();
            guard.insert(
                limited_key.to_string(),
                vec![now - Duration::from_secs(30); max],
            );
            for i in 1..MAX_TRACKED_KEYS {
                guard.insert(
                    format!("198.51.100.{i}:api"),
                    vec![now - Duration::from_secs(30)],
                );
            }
        }

        assert!(
            !limiter.check("203.0.113.9:api", max),
            "full active map should reject new clients instead of evicting active buckets"
        );
        assert_eq!(limiter.inner.lock().len(), MAX_TRACKED_KEYS);
        assert!(
            !limiter.check(limited_key, max),
            "eviction must not reset an active client's bucket"
        );
    }

    #[test]
    fn trusted_proxy_uses_x_forwarded_for() {
        use axum::body::Body;

        let proxy = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let mut request = Request::builder()
            .uri("/api/v1/health")
            .header("X-Forwarded-For", "203.0.113.5, 10.0.0.1")
            .body(Body::empty())
            .unwrap();
        request
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from(([10, 0, 0, 1], 12345))));

        let ip = client_ip(&request, &[proxy]);
        assert_eq!(ip, IpAddr::V4(Ipv4Addr::new(203, 0, 113, 5)));
    }
}
