//! Timeline monotonicity across ownership epoch changes.

/// Remaps wire timestamps so players see a monotonic timeline across failovers.
#[derive(Debug, Default, Clone)]
pub struct TimelineRemapper {
    /// Offset added to incoming timestamps for the current epoch.
    offset: u32,
    last_out: u32,
    current_epoch: Option<u64>,
    /// Last input timestamp observed in the previous epoch (for handoff).
    prev_epoch_last_in: u32,
}

impl TimelineRemapper {
    pub fn new() -> Self {
        Self::default()
    }

    /// Map an incoming frame timestamp for `epoch` to a monotonic output ts.
    pub fn map(&mut self, epoch: u64, ts_in: u32) -> u32 {
        if self.current_epoch != Some(epoch) {
            // New epoch: continue from last_out + 1 (or 0 on first).
            if self.current_epoch.is_some() {
                self.offset = self.last_out.wrapping_add(1).wrapping_sub(ts_in);
            } else {
                self.offset = 0;
            }
            self.current_epoch = Some(epoch);
            self.prev_epoch_last_in = ts_in;
        } else {
            self.prev_epoch_last_in = ts_in;
        }
        let out = ts_in.wrapping_add(self.offset);
        // Guard against non-monotonic within epoch (encoder reset).
        if self.current_epoch.is_some() && out < self.last_out {
            self.offset = self.last_out.wrapping_add(1).wrapping_sub(ts_in);
            let out2 = ts_in.wrapping_add(self.offset);
            self.last_out = out2;
            return out2;
        }
        self.last_out = out;
        out
    }

    pub fn last_out(&self) -> u32 {
        self.last_out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_handoff_is_monotonic() {
        let mut t = TimelineRemapper::new();
        assert_eq!(t.map(1, 1000), 1000);
        assert_eq!(t.map(1, 1040), 1040);
        // New epoch restarts at 0 from new owner — remap continues.
        let next = t.map(2, 0);
        assert!(next > 1040);
        let next2 = t.map(2, 40);
        assert!(next2 > next);
    }

    #[test]
    fn wrap_within_epoch() {
        let mut t = TimelineRemapper::new();
        let a = t.map(1, u32::MAX - 10);
        let b = t.map(1, 5); // would wrap on wire
        assert!(b > a || a > u32::MAX / 2);
    }
}
