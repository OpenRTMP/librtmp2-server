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

/// Configurable HTTP rate-limit buckets (loaded from config / env).
#[derive(Debug, Clone)]
pub struct HttpRateLimitConfig {
    pub window: Duration,
    pub api_max: usize,
    pub stats_max: usize,
    pub default_max: usize,
}

impl Default for HttpRateLimitConfig {
    fn default() -> Self {
        Self {
            window: Duration::from_secs(60),
            api_max: 120,
            stats_max: 30,
            default_max: 60,
        }
    }
}

#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<Mutex<HashMap<String, Vec<Instant>>>>,
    config: HttpRateLimitConfig,
    trusted_proxies: Arc<Vec<IpAddr>>,
}

impl RateLimiter {
    pub fn new(config: HttpRateLimitConfig, trusted_proxies: Vec<IpAddr>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            config,
            trusted_proxies: Arc::new(trusted_proxies),
        }
    }

    fn timestamp_in_window(&self, now: Instant, timestamp: Instant) -> bool {
        now.checked_duration_since(timestamp)
            .is_none_or(|age| age < self.config.window)
    }

    fn active_request_count(&self, entries: &[Instant], now: Instant) -> usize {
        entries
            .iter()
            .copied()
            .filter(|t| self.timestamp_in_window(now, *t))
            .count()
    }

    /// Drop client keys whose sliding window has fully expired.
    fn purge_expired(&self, guard: &mut HashMap<String, Vec<Instant>>, now: Instant) {
        guard.retain(|_, entries| {
            entries.retain(|t| self.timestamp_in_window(now, *t));
            !entries.is_empty()
        });
    }

    /// Remove the least-recently-active bucket that is not currently
    /// rate-limited. Actively throttled buckets are never evicted — dropping
    /// one would reset its window and let a client immediately resume.
    fn evict_oldest_eligible_client(
        &self,
        guard: &mut HashMap<String, Vec<Instant>>,
        now: Instant,
        max_requests: usize,
    ) -> bool {
        let Some(oldest_key) = guard
            .iter()
            .filter(|(_, entries)| self.active_request_count(entries, now) < max_requests)
            .min_by_key(|(_, entries)| entries.last().copied().unwrap_or_else(Instant::now))
            .map(|(key, _)| key.clone())
        else {
            return false;
        };
        guard.remove(&oldest_key);
        true
    }

    fn check(&self, key: &str, max_requests: usize) -> bool {
        if max_requests == 0 {
            return false;
        }

        let mut guard = self.inner.lock();
        let now = Instant::now();

        if let Some(entries) = guard.get_mut(key) {
            entries.retain(|t| self.timestamp_in_window(now, *t));
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
            if guard.len() >= MAX_TRACKED_KEYS
                && !self.evict_oldest_eligible_client(&mut guard, now, max_requests)
            {
                // Every tracked bucket is actively rate-limited; fail closed
                // rather than freeing a throttled bucket.
                return false;
            }
        }

        guard.insert(key.to_string(), vec![now]);
        true
    }

    fn limit_for_path(&self, path: &str) -> usize {
        if path.starts_with("/api/") {
            self.config.api_max
        } else if path.starts_with("/stats") {
            self.config.stats_max
        } else {
            self.config.default_max
        }
    }
}

fn client_ip(request: &Request, trusted_proxies: &[IpAddr]) -> IpAddr {
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0.ip())
        .unwrap_or(IpAddr::from([127, 0, 0, 1]));

    resolve_client_ip(
        peer,
        request.headers().get("X-Forwarded-For"),
        trusted_proxies,
    )
}

/// Resolve the client IP for access logs / rate limits, honoring
/// `X-Forwarded-For` only when the direct peer is a configured trusted proxy.
pub fn resolve_client_ip(
    peer: IpAddr,
    x_forwarded_for: Option<&axum::http::HeaderValue>,
    trusted_proxies: &[IpAddr],
) -> IpAddr {
    if trusted_proxies.contains(&peer) {
        // Use the rightmost address: the one appended by the immediate trusted
        // proxy ($proxy_add_x_forwarded_for), not client-controlled leftmost entries.
        //
        // X-Real-IP is deliberately NOT trusted here: unlike XFF, which the
        // trusted proxy appends to, X-Real-IP is commonly just passed through
        // unmodified by proxies that don't set it themselves, which would let
        // a client pick an arbitrary rate-limit bucket by setting it directly.
        if let Some(xff) = x_forwarded_for.and_then(|v| v.to_str().ok())
            && let Some(rightmost) = xff.split(',').map(str::trim).rfind(|part| !part.is_empty())
        {
            match rightmost.parse::<IpAddr>() {
                Ok(client) => return client,
                Err(_) => crate::log_warn!(
                    "rate_limit: trusted proxy {peer} sent unparsable X-Forwarded-For hop '{rightmost}', falling back to peer IP"
                ),
            }
        }
    }

    peer
}

/// Client IP from an accepted socket + request headers (same rules as the
/// rate-limit middleware).
pub fn client_ip_from_connect(
    addr: SocketAddr,
    headers: &axum::http::HeaderMap,
    trusted_proxies: &[IpAddr],
) -> IpAddr {
    resolve_client_ip(addr.ip(), headers.get("X-Forwarded-For"), trusted_proxies)
}

pub async fn middleware(
    State(limiter): State<RateLimiter>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path().to_string();
    let method = request.method().as_str().to_string();
    let peer = client_ip(&request, limiter.trusted_proxies.as_slice());
    let key = format!("{}:{}", peer, path.split('/').nth(1).unwrap_or(""));
    let max = limiter.limit_for_path(&path);
    if !limiter.check(&key, max) {
        crate::log_warn!("HTTP: {method} {path} from {peer} → 429 rate limit exceeded");
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

    fn test_limiter() -> RateLimiter {
        RateLimiter::new(HttpRateLimitConfig::default(), Vec::new())
    }

    #[test]
    fn expired_one_shot_clients_do_not_permanently_fill_tracking_map() {
        let limiter = test_limiter();
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
    fn active_clients_are_evicted_via_lru_when_map_is_at_capacity() {
        let limiter = test_limiter();
        let max = 120;
        let now = Instant::now();
        let limited_key = "198.51.100.0:api";

        {
            let mut guard = limiter.inner.lock();
            guard.insert(
                limited_key.to_string(),
                vec![now - Duration::from_secs(1); max],
            );
            for i in 1..MAX_TRACKED_KEYS {
                guard.insert(
                    format!("198.51.100.{i}:api"),
                    vec![now - Duration::from_secs(30)],
                );
            }
        }

        assert!(
            limiter.check("203.0.113.9:api", max),
            "full map should evict an eligible bucket and admit a new client"
        );
        assert_eq!(limiter.inner.lock().len(), MAX_TRACKED_KEYS);
        assert!(
            !limiter.check(limited_key, max),
            "actively rate-limited bucket must not be reset by eviction"
        );
    }

    #[test]
    fn saturated_map_fails_closed_when_every_bucket_is_rate_limited() {
        let limiter = test_limiter();
        let max = 3;
        let now = Instant::now();

        {
            let mut guard = limiter.inner.lock();
            for i in 0..MAX_TRACKED_KEYS {
                guard.insert(format!("203.0.113.{i}:api"), vec![now; max]);
            }
        }

        assert!(
            !limiter.check("203.0.113.255:api", max),
            "new client must be denied when every tracked bucket is throttled"
        );
    }

    #[test]
    fn stats_limit_uses_configured_bucket() {
        let limiter = RateLimiter::new(
            HttpRateLimitConfig {
                stats_max: 5,
                ..HttpRateLimitConfig::default()
            },
            Vec::new(),
        );
        for i in 0..5 {
            assert!(
                limiter.check("127.0.0.1:stats", 5),
                "stats request {i} should succeed"
            );
        }
        assert!(
            !limiter.check("127.0.0.1:stats", 5),
            "sixth stats request should be rate limited"
        );
    }

    #[test]
    fn trusted_proxy_uses_rightmost_x_forwarded_for() {
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
        assert_eq!(ip, IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
    }

    #[test]
    fn trusted_proxy_ignores_client_supplied_leftmost_xff() {
        use axum::body::Body;

        let proxy = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let mut request = Request::builder()
            .uri("/api/v1/health")
            .header("X-Forwarded-For", "198.51.100.99, 203.0.113.5")
            .body(Body::empty())
            .unwrap();
        request
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from(([10, 0, 0, 1], 12345))));

        let ip = client_ip(&request, &[proxy]);
        assert_eq!(ip, IpAddr::V4(Ipv4Addr::new(203, 0, 113, 5)));
    }

    #[test]
    fn trusted_proxy_ignores_x_real_ip() {
        // A proxy that only forwards X-Real-IP unmodified (rather than setting
        // it itself) would let a client pick an arbitrary rate-limit bucket by
        // sending this header directly, so it must never be trusted.
        use axum::body::Body;

        let proxy = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let mut request = Request::builder()
            .uri("/api/v1/health")
            .header("X-Real-IP", "203.0.113.5")
            .header("X-Forwarded-For", "198.51.100.99, 10.0.0.1")
            .body(Body::empty())
            .unwrap();
        request
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from(([10, 0, 0, 1], 12345))));

        let ip = client_ip(&request, &[proxy]);
        assert_eq!(ip, IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
    }
}
