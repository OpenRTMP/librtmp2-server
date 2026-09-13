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
        self.add_with(peer, app, stream, || Ok(()))
            .expect("add_with with empty before cannot fail")
    }

    /// Like [`Self::add`], but runs `before` while the table lock is held so a
    /// caller can enqueue InitCache before fan-out can observe the new ref.
    /// If `before` fails, the refcount is left unchanged.
    pub fn add_with<F>(&self, peer: u64, app: &str, stream: &str, before: F) -> Result<bool, ()>
    where
        F: FnOnce() -> Result<(), ()>,
    {
        let mut g = self.inner.lock();
        before()?;
        let key = (peer, app.to_string(), stream.to_string());
        let e = g.entry(key).or_insert(0);
        *e += 1;
        Ok(*e == 1)
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

    /// Drop the entry only when this peer holds a sole ref (`count == 1`).
    /// Used to roll back a failed first `Subscribe` without stranding concurrent
    /// holders that already observed `add == false` and expect wire state.
    pub fn remove_if_sole(&self, peer: u64, app: &str, stream: &str) -> bool {
        let mut g = self.inner.lock();
        let key = (peer, app.to_string(), stream.to_string());
        match g.get(&key).copied() {
            Some(1) => {
                g.remove(&key);
                true
            }
            _ => false,
        }
    }

    pub fn peers_for_stream(&self, app: &str, stream: &str) -> Vec<u64> {
        let mut out = Vec::new();
        self.for_each_peer(app, stream, |peer| out.push(peer));
        out
    }

    /// Call `f` for each peer subscribed to this stream while the table lock
    /// is held, so registration cannot interleave mid-fan-out.
    pub fn for_each_peer(&self, app: &str, stream: &str, mut f: impl FnMut(u64)) {
        let g = self.inner.lock();
        for ((peer, a, s), _) in g.iter() {
            if a == app && s == stream {
                f(*peer);
            }
        }
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

    /// Drop every ref for this peer+stream (exhausted Subscribe NACK retries).
    pub fn clear_entry(&self, peer: u64, app: &str, stream: &str) {
        self.inner
            .lock()
            .remove(&(peer, app.to_string(), stream.to_string()));
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

    #[test]
    fn add_with_abort_skips_increment() {
        let t = SubscriptionTable::new();
        assert!(t.add_with(2, "live", "s1", || Err(())).is_err());
        assert_eq!(t.count(), 0);
        assert!(t.add_with(2, "live", "s1", || Ok(())).unwrap());
        assert_eq!(t.count(), 1);
    }
}
