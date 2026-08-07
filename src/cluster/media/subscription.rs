//! Subscription reference counting per remote node + stream.

use std::collections::HashMap;

use parking_lot::Mutex;

#[derive(Default)]
pub struct SubscriptionTable {
    /// (peer_node, app, stream) -> refcount
    inner: Mutex<HashMap<(u64, String, String), usize>>,
}

impl SubscriptionTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment; returns true if this is the first subscription (need SUBSCRIBE wire msg).
    pub fn add(&self, peer: u64, app: &str, stream: &str) -> bool {
        let mut g = self.inner.lock();
        let key = (peer, app.to_string(), stream.to_string());
        let e = g.entry(key).or_insert(0);
        *e += 1;
        *e == 1
    }

    /// Decrement; returns true if count hit zero (need UNSUBSCRIBE).
    pub fn remove(&self, peer: u64, app: &str, stream: &str) -> bool {
        let mut g = self.inner.lock();
        let key = (peer, app.to_string(), stream.to_string());
        match g.get_mut(&key) {
            Some(c) if *c > 1 => {
                *c -= 1;
                false
            }
            Some(_) => {
                g.remove(&key);
                true
            }
            None => false,
        }
    }

    pub fn peers_for_stream(&self, app: &str, stream: &str) -> Vec<u64> {
        self.inner
            .lock()
            .iter()
            .filter(|((_, a, s), _)| a == app && s == stream)
            .map(|((peer, _, _), _)| *peer)
            .collect()
    }

    pub fn streams_for_peer(&self, peer: u64) -> Vec<(String, String)> {
        self.inner
            .lock()
            .iter()
            .filter(|((p, _, _), _)| *p == peer)
            .map(|((_, a, s), _)| (a.clone(), s.clone()))
            .collect()
    }

    /// Drop every subscription entry for `peer` (inbound connection teardown).
    pub fn clear_peer(&self, peer: u64) {
        let mut g = self.inner.lock();
        g.retain(|(p, _, _), _| *p != peer);
    }

    pub fn count(&self) -> usize {
        self.inner.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refcount_dedup() {
        let t = SubscriptionTable::new();
        assert!(t.add(2, "live", "s1"));
        assert!(!t.add(2, "live", "s1"));
        assert!(!t.remove(2, "live", "s1"));
        assert!(t.remove(2, "live", "s1"));
    }
}
