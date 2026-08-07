//! Per-client HTTP rate limiting (in-process sliding window).

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderMap, StatusCode};
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
    /// Live API bearer token (shared with AppState; refreshed on cluster join).
    /// Used only to preview whether an `/api/*` request is authenticated for
    /// rate-limit bucket selection — handlers still enforce auth independently.
    api_token: Arc<parking_lot::RwLock<String>>,
}

impl RateLimiter {
    pub fn new(
        config: HttpRateLimitConfig,
        trusted_proxies: Vec<IpAddr>,
        api_token: Arc<parking_lot::RwLock<String>>,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            config,
            trusted_proxies: Arc::new(trusted_proxies),
            api_token,
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

    /// Classify a request into a rate-limit bucket and its per-window cap.
    ///
    /// `/api/*` requests without a valid Bearer token — including the public
    /// `GET /api/v1/health` — never draw from `api_max`. Rate limiting runs
    /// before authentication, so if any unauthenticated `/api/*` request
    /// shared the authenticated bucket, an attacker on the client's IP could
    /// exhaust the admin API budget without a token at all (not just via
    /// health): they would just hit a protected route and let its 401 count
    /// against the same budget as a real admin call. Authenticated requests
    /// (including an authenticated health check) share the trusted `api`
    /// budget as before.
    ///
    /// `/stats` and `/stats-nginx` keep independent budgets per exact path —
    /// they are different endpoints with different legitimate polling
    /// clients, and collapsing them onto one shared bucket would let traffic
    /// to one starve the other's allowance.
    fn bucket_for(&self, path: &str, authenticated: bool) -> (usize, String) {
        if path.starts_with("/api/") {
            if authenticated {
                (self.config.api_max, "api".to_string())
            } else {
                (self.config.default_max, "public".to_string())
            }
        } else if matches!(path, "/stats" | "/stats-nginx") {
            (self.config.stats_max, path.to_string())
        } else {
            (self.config.default_max, "default".to_string())
        }
    }
}

/// Constant-time string equality so token validation does not leak the
/// secret one byte at a time via response timing.
pub(crate) fn ct_str_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let n = a.len().max(b.len());
    let mut diff = (a.len() ^ b.len()) as u8;
    for i in 0..n {
        let ca = a.get(i).copied().unwrap_or(0);
        let cb = b.get(i).copied().unwrap_or(0);
        diff |= ca ^ cb;
    }
    diff == 0
}

/// Preview whether `headers` carries the configured API bearer token, for
/// rate-limit bucket selection only. This intentionally mirrors the real
/// auth check (`http::bearer_ok`) so the two never drift apart — a request
/// classified here as authenticated must also pass the handler's own check.
pub(crate) fn bearer_authenticated(headers: &HeaderMap, token: &str) -> bool {
    if token.is_empty() {
        return false;
    }
    let Some(hdr) = headers.get("Authorization").and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let Some(tok) = hdr.strip_prefix("Bearer ") else {
        return false;
    };
    ct_str_eq(tok.trim(), token)
}

fn client_ip(request: &Request, trusted_proxies: &[IpAddr]) -> IpAddr {
    // Without a real peer address (e.g. `ConnectInfo` missing from a
    // non-standard embedding), there is no basis for deciding whether the
    // peer is a trusted proxy, so X-Forwarded-For must not be honored.
    let Some(peer) = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0.ip())
    else {
        return IpAddr::from([127, 0, 0, 1]);
    };

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

pub async fn middleware(
    State(limiter): State<RateLimiter>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    let peer = client_ip(&request, limiter.trusted_proxies.as_slice());
    let token = limiter.api_token.read().clone();
    let authenticated = bearer_authenticated(request.headers(), &token);
    let (max, bucket) = limiter.bucket_for(path, authenticated);
    let key = format!("{peer}:{bucket}");
    if !limiter.check(&key, max) {
        let method = request.method().as_str();
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
    use std::sync::Arc;

    fn token(s: &str) -> Arc<parking_lot::RwLock<String>> {
        Arc::new(parking_lot::RwLock::new(s.to_string()))
    }

    fn test_limiter() -> RateLimiter {
        RateLimiter::new(HttpRateLimitConfig::default(), Vec::new(), token("test-token"))
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
    fn unauthenticated_api_requests_use_a_bucket_separate_from_authenticated_api() {
        let limiter = RateLimiter::new(
            HttpRateLimitConfig {
                api_max: 3,
                default_max: 60,
                ..HttpRateLimitConfig::default()
            },
            Vec::new(),
            token("test-token"),
        );

        let (health_max, health_bucket) = limiter.bucket_for("/api/v1/health", false);
        let (api_max, api_bucket) = limiter.bucket_for("/api/v1/streams", true);
        assert_eq!(health_bucket, "public");
        assert_eq!(api_bucket, "api");
        assert_eq!(health_max, 60);
        assert_eq!(api_max, 3);

        for i in 0..3 {
            assert!(
                limiter.check("127.0.0.1:api", api_max),
                "api request {i} should succeed"
            );
        }
        assert!(
            !limiter.check("127.0.0.1:api", api_max),
            "api bucket should be exhausted"
        );
        assert!(
            limiter.check("127.0.0.1:public", health_max),
            "public bucket must remain independent of the authenticated api bucket"
        );
    }

    #[test]
    fn unauthenticated_requests_to_protected_routes_do_not_exhaust_admin_budget() {
        // Regression: rate limiting runs before auth, so an unauthenticated
        // request to any /api/* route (not just health) must not be able to
        // exhaust the authenticated admin budget for a shared client IP.
        let limiter = RateLimiter::new(
            HttpRateLimitConfig {
                api_max: 3,
                default_max: 3,
                ..HttpRateLimitConfig::default()
            },
            Vec::new(),
            token("test-token"),
        );

        for i in 0..3 {
            let (max, bucket) = limiter.bucket_for("/api/v1/streams", false);
            assert_eq!(bucket, "public");
            assert!(
                limiter.check(&format!("127.0.0.1:{bucket}"), max),
                "unauthenticated request {i} should succeed"
            );
        }
        let (max, bucket) = limiter.bucket_for("/api/v1/streams", false);
        assert!(
            !limiter.check(&format!("127.0.0.1:{bucket}"), max),
            "public bucket should now be exhausted"
        );

        let (api_max, api_bucket) = limiter.bucket_for("/api/v1/streams", true);
        assert_eq!(api_bucket, "api");
        assert!(
            limiter.check(&format!("127.0.0.1:{api_bucket}"), api_max),
            "authenticated admin API request must still succeed after unauthenticated \
             requests exhausted the public bucket"
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
            token("test-token"),
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
    fn stats_and_stats_nginx_have_independent_budgets() {
        let limiter = RateLimiter::new(
            HttpRateLimitConfig {
                stats_max: 2,
                ..HttpRateLimitConfig::default()
            },
            Vec::new(),
            token("test-token"),
        );

        let (max, stats_bucket) = limiter.bucket_for("/stats", false);
        let (_, nginx_bucket) = limiter.bucket_for("/stats-nginx", false);
        assert_ne!(
            stats_bucket, nginx_bucket,
            "/stats and /stats-nginx must not share a rate-limit bucket"
        );

        for i in 0..2 {
            assert!(
                limiter.check(&format!("127.0.0.1:{stats_bucket}"), max),
                "/stats request {i} should succeed"
            );
        }
        assert!(
            !limiter.check(&format!("127.0.0.1:{stats_bucket}"), max),
            "/stats bucket should now be exhausted"
        );
        assert!(
            limiter.check(&format!("127.0.0.1:{nginx_bucket}"), max),
            "/stats-nginx must still have its own budget after /stats was exhausted"
        );
    }

    #[test]
    fn unmatched_stats_prefixed_paths_use_the_default_bucket_not_a_fresh_one() {
        // Regression: a naive `starts_with("/stats")` classification would
        // key the bucket by the full (attacker-controlled) path, letting a
        // client mint an unlimited number of fresh `stats_max`-sized buckets
        // via unique unmatched paths like `/stats-<random>`. Only the two
        // real stats routes get their own bucket; everything else — matched
        // or not — shares "default".
        let limiter = RateLimiter::new(
            HttpRateLimitConfig {
                stats_max: 5,
                default_max: 60,
                ..HttpRateLimitConfig::default()
            },
            Vec::new(),
            token("test-token"),
        );

        let (max_a, bucket_a) = limiter.bucket_for("/stats-abc123", false);
        let (max_b, bucket_b) = limiter.bucket_for("/stats-xyz789", false);
        assert_eq!(bucket_a, "default");
        assert_eq!(bucket_b, "default");
        assert_eq!(max_a, 60);
        assert_eq!(max_b, 60);
    }

    #[test]
    fn bearer_authenticated_matches_configured_token_only() {
        use axum::body::Body;

        let authorized = Request::builder()
            .uri("/api/v1/streams")
            .header("Authorization", "Bearer secret")
            .body(Body::empty())
            .unwrap();
        assert!(bearer_authenticated(authorized.headers(), "secret"));

        let wrong_token = Request::builder()
            .uri("/api/v1/streams")
            .header("Authorization", "Bearer wrong")
            .body(Body::empty())
            .unwrap();
        assert!(!bearer_authenticated(wrong_token.headers(), "secret"));

        let missing_header = Request::builder()
            .uri("/api/v1/streams")
            .body(Body::empty())
            .unwrap();
        assert!(!bearer_authenticated(missing_header.headers(), "secret"));

        let empty_token = Request::builder()
            .uri("/api/v1/streams")
            .header("Authorization", "Bearer secret")
            .body(Body::empty())
            .unwrap();
        assert!(!bearer_authenticated(empty_token.headers(), ""));
    }

    #[test]
    fn ct_str_eq_matches_and_differs() {
        assert!(ct_str_eq("abc", "abc"));
        assert!(!ct_str_eq("abc", "abd"));
        assert!(!ct_str_eq("abc", "abcd"));
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
