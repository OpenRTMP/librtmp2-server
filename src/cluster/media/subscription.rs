//! Subscription reference counting per remote node + stream.
//!
//! The two directions are tracked separately: [`SubscriptionTable`] holds
//! owner-side inbound fan-out refs (a peer subscribed to a stream this node
//! serves), while [`OutboundSubscriptions`] holds subscriber-side refs (local
//! viewers subscribed to a stream a remote peer serves). One `(peer, app,
//! stream)` triple can legitimately hold a ref in both directions at once,
//! e.g. across an ownership handoff, so sharing one counter made a re-subscribe
//! look already wired and never sent the `Subscribe`.

use std::collections::{HashMap, HashSet};

use parking_lot::Mutex;

#[derive(Default)]
pub struct SubscriptionTable {
    /// (peer_node, app, stream) -> inbound refcount
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

    /// Whether this peer holds any inbound ref for the stream.
    pub fn has(&self, peer: u64, app: &str, stream: &str) -> bool {
        self.inner
            .lock()
            .contains_key(&(peer, app.to_string(), stream.to_string()))
    }

    /// Call `f` for each peer subscribed to this stream while the table lock
    /// is held, so registration cannot interleave mid-fan-out.
    pub fn for_each_peer(&self, app: &str, stream: &str, mut f: impl FnMut(u64)) {
        let g = self.inner.lock();
        for (peer, a, s) in g.keys() {
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

#[derive(Clone, Default)]
struct OutboundEntry {
    /// Sessions (local player ids, or the standby replica token) that hold a
    /// ref for this key. A set makes a repeated subscribe from the same
    /// session idempotent instead of leaking a ref that no unsubscribe can
    /// ever release.
    owners: HashSet<String>,
    /// A `Subscribe` for this key was enqueued on the *current* peer
    /// connection. Cleared on reconnect/replacement and when the owner rejects
    /// an attempt, so only an actually queued wire subscription enables the
    /// "already subscribed" fast path.
    wired: bool,
}

/// Subscriber-side (outbound) subscription refcounts for local viewers.
///
/// Kept apart from [`SubscriptionTable`] so the peer's own inbound
/// `Subscribe` cannot satisfy this node's outbound "already subscribed"
/// fast path, and so releasing an outbound ref cannot decrement inbound
/// fan-out state.
#[derive(Default)]
pub struct OutboundSubscriptions {
    inner: Mutex<HashMap<(u64, String, String), OutboundEntry>>,
}

impl OutboundSubscriptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one ref owned by `owner`; returns `true` when this is the first ref
    /// for the key (a wire `Subscribe` is needed). Re-adding the same owner is
    /// a no-op.
    pub fn add(&self, owner: &str, peer: u64, app: &str, stream: &str) -> bool {
        let mut g = self.inner.lock();
        let e = g
            .entry((peer, app.to_string(), stream.to_string()))
            .or_default();
        let inserted = e.owners.insert(owner.to_string());
        inserted && e.owners.len() == 1
    }

    /// Remove `owner`'s ref; returns `true` when the last ref is gone (wire
    /// `Unsubscribe` needed). Unknown owners are a no-op so a session that was
    /// never subscribed cannot release somebody else's ref.
    pub fn remove(&self, owner: &str, peer: u64, app: &str, stream: &str) -> bool {
        let mut g = self.inner.lock();
        let key = (peer, app.to_string(), stream.to_string());
        let Some(e) = g.get_mut(&key) else {
            return false;
        };
        if !e.owners.remove(owner) {
            return false;
        }
        if e.owners.is_empty() {
            g.remove(&key);
            true
        } else {
            false
        }
    }

    /// Drop the entry only when `owner` is its sole holder. Used to roll back a
    /// failed first `Subscribe` without stranding concurrent holders.
    pub fn remove_if_sole(&self, owner: &str, peer: u64, app: &str, stream: &str) -> bool {
        let mut g = self.inner.lock();
        let key = (peer, app.to_string(), stream.to_string());
        if g.get(&key)
            .is_some_and(|e| e.owners.len() == 1 && e.owners.contains(owner))
        {
            g.remove(&key);
            true
        } else {
            false
        }
    }

    /// Number of refs held for this key (test/diagnostic helper).
    pub fn ref_count(&self, peer: u64, app: &str, stream: &str) -> usize {
        self.inner
            .lock()
            .get(&(peer, app.to_string(), stream.to_string()))
            .map_or(0, |e| e.owners.len())
    }

    pub fn has(&self, peer: u64, app: &str, stream: &str) -> bool {
        self.inner
            .lock()
            .contains_key(&(peer, app.to_string(), stream.to_string()))
    }

    /// Drop every ref for this key (ownership moved away from this peer).
    pub fn remove_all(&self, peer: u64, app: &str, stream: &str) -> bool {
        self.inner
            .lock()
            .remove(&(peer, app.to_string(), stream.to_string()))
            .is_some()
    }

    /// Whether a wire `Subscribe` is enqueued on the current connection.
    pub fn is_wired(&self, peer: u64, app: &str, stream: &str) -> bool {
        self.inner
            .lock()
            .get(&(peer, app.to_string(), stream.to_string()))
            .is_some_and(|e| e.wired)
    }

    /// Record whether a wire `Subscribe` is enqueued on the current
    /// connection. No-op for keys without a ref.
    pub fn set_wired(&self, peer: u64, app: &str, stream: &str, wired: bool) {
        if let Some(e) = self
            .inner
            .lock()
            .get_mut(&(peer, app.to_string(), stream.to_string()))
        {
            e.wired = wired;
        }
    }

    /// A new connection carries no wire state: forget every `wired` flag for
    /// this peer's keys (the refcounts survive the swap).
    pub fn clear_wired(&self, peer: u64) {
        for ((p, _, _), e) in self.inner.lock().iter_mut() {
            if *p == peer {
                e.wired = false;
            }
        }
    }

    pub fn peers_for_stream(&self, app: &str, stream: &str) -> Vec<u64> {
        self.inner
            .lock()
            .iter()
            .filter(|((_, a, s), _)| a == app && s == stream)
            .map(|((p, _, _), _)| *p)
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

    /// Drop every outbound entry for `peer` (peer disconnected).
    pub fn clear_peer(&self, peer: u64) {
        self.inner.lock().retain(|(p, _, _), _| *p != peer);
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

    #[test]
    fn outbound_refs_and_wire_state_are_independent_of_inbound_refs() {
        let inbound = SubscriptionTable::new();
        let outbound = OutboundSubscriptions::new();

        // A peer subscribing to a stream this node owns does not count as an
        // outbound subscribe for that same peer.
        assert!(inbound.add(2, "live", "s"));
        assert!(!outbound.has(2, "live", "s"));
        assert!(!outbound.is_wired(2, "live", "s"));

        // First outbound ref needs a wire Subscribe; a second only bumps.
        assert!(outbound.add("viewer-a", 2, "live", "s"));
        outbound.set_wired(2, "live", "s", true);
        assert!(outbound.is_wired(2, "live", "s"));
        // The same session re-subscribing is idempotent (no leaked ref).
        assert!(!outbound.add("viewer-a", 2, "live", "s"));
        assert_eq!(outbound.ref_count(2, "live", "s"), 1);
        // A different session adds a real second ref.
        assert!(!outbound.add("viewer-b", 2, "live", "s"));
        assert_eq!(outbound.ref_count(2, "live", "s"), 2);
        assert!(!outbound.remove("viewer-a", 2, "live", "s"));
        // The last ref sends Unsubscribe and leaves the inbound ref alone.
        assert!(outbound.remove("viewer-b", 2, "live", "s"));
        assert!(!outbound.has(2, "live", "s"));
        assert!(inbound.has(2, "live", "s"));

        // A new connection has no wire state.
        outbound.add("viewer-a", 2, "live", "s");
        outbound.set_wired(2, "live", "s", true);
        outbound.clear_wired(2);
        assert!(!outbound.is_wired(2, "live", "s"));
        assert!(outbound.has(2, "live", "s"));

        // Sole-ref rollback does not remove concurrent holders.
        outbound.add("viewer-b", 2, "live", "s");
        assert!(!outbound.remove_if_sole("viewer-a", 2, "live", "s"));
        assert!(!outbound.remove("viewer-a", 2, "live", "s"));
        assert!(outbound.remove_if_sole("viewer-b", 2, "live", "s"));
        assert!(!outbound.has(2, "live", "s"));
    }
}
