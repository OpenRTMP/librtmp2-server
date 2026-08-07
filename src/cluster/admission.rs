//! Ingress admission based on local load / bandwidth hysteresis.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::cluster::config::ClusterConfig;
use crate::cluster::health::{HealthTracker, NodeHealthState};

/// Abstraction for provider-agnostic ingress eligibility.
pub trait IngressEligibility: Send + Sync {
    fn should_accept_new_publish(&self) -> bool;
    fn should_accept_new_play(&self) -> bool;
    fn mark_draining(&self);
    fn mark_ready(&self);
}

pub struct AdmissionController {
    config: ClusterConfig,
    health: Arc<HealthTracker>,
    draining: AtomicBool,
    /// Last measured interface utilization 0.0–1.0 (or configured capacity).
    load: parking_lot::Mutex<f64>,
}

impl AdmissionController {
    pub fn new(config: ClusterConfig, health: Arc<HealthTracker>) -> Arc<Self> {
        Arc::new(Self {
            config,
            health,
            draining: AtomicBool::new(false),
            load: parking_lot::Mutex::new(0.0),
        })
    }

    pub fn update_load(&self, load: f64) {
        *self.load.lock() = load.clamp(0.0, 1.0);
        let load = *self.load.lock();
        if !self.draining.load(Ordering::Relaxed) && load >= self.config.drain_threshold {
            self.draining.store(true, Ordering::Relaxed);
            self.health.set_local(NodeHealthState::Draining);
            crate::log_info!(
                "Cluster admission: entering DRAINING (load={load:.2} >= {:.2})",
                self.config.drain_threshold
            );
        } else if self.draining.load(Ordering::Relaxed) && load <= self.config.resume_threshold {
            self.draining.store(false, Ordering::Relaxed);
            if self.health.local() == NodeHealthState::Draining {
                self.health.set_local(NodeHealthState::Ready);
            }
            crate::log_info!(
                "Cluster admission: resuming READY (load={load:.2} <= {:.2})",
                self.config.resume_threshold
            );
        }
    }

    pub fn current_load(&self) -> f64 {
        *self.load.lock()
    }

    pub fn force_drain(&self) {
        self.draining.store(true, Ordering::Relaxed);
        self.health.set_local(NodeHealthState::Draining);
    }

    pub fn force_resume(&self) {
        self.draining.store(false, Ordering::Relaxed);
        self.health.set_local(NodeHealthState::Ready);
    }
}

impl IngressEligibility for AdmissionController {
    fn should_accept_new_publish(&self) -> bool {
        let state = self.health.local();
        matches!(state, NodeHealthState::Ready | NodeHealthState::Learner)
            && !self.draining.load(Ordering::Relaxed)
    }

    fn should_accept_new_play(&self) -> bool {
        !matches!(
            self.health.local(),
            NodeHealthState::Down | NodeHealthState::Isolated | NodeHealthState::Leaving
        )
    }

    fn mark_draining(&self) {
        self.force_drain();
    }

    fn mark_ready(&self) {
        self.force_resume();
    }
}

/// Measure interface RX+TX bytes/sec when possible. Returns utilization vs
/// `bandwidth_max_mbps` (0.0 if interface missing / unsupported).
pub fn measure_interface_utilization(iface: &str, max_mbps: f64) -> Result<f64, String> {
    if iface.is_empty() || max_mbps <= 0.0 {
        return Ok(0.0);
    }
    #[cfg(target_os = "linux")]
    {
        let rx_path = format!("/sys/class/net/{iface}/statistics/rx_bytes");
        let tx_path = format!("/sys/class/net/{iface}/statistics/tx_bytes");
        let rx1: u64 = std::fs::read_to_string(&rx_path)
            .map_err(|e| format!("interface '{iface}' missing or unreadable: {e}"))?
            .trim()
            .parse()
            .unwrap_or(0);
        let tx1: u64 = std::fs::read_to_string(&tx_path)
            .map_err(|e| format!("interface '{iface}' missing or unreadable: {e}"))?
            .trim()
            .parse()
            .unwrap_or(0);
        std::thread::sleep(std::time::Duration::from_millis(200));
        let rx2: u64 = std::fs::read_to_string(&rx_path)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(rx1);
        let tx2: u64 = std::fs::read_to_string(&tx_path)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(tx1);
        let bytes_per_sec = (rx2.saturating_sub(rx1) + tx2.saturating_sub(tx1)) as f64 / 0.2;
        let mbps = bytes_per_sec * 8.0 / 1_000_000.0;
        return Ok((mbps / max_mbps).clamp(0.0, 1.0));
    }
    #[cfg(target_os = "windows")]
    {
        // Best-effort via Get-NetAdapterStatistics; clear error if missing.
        let output = std::process::Command::new("powershell")
            .args([
                "-NoProfile",
                "-Command",
                &format!(
                    "(Get-NetAdapterStatistics -Name '{iface}' -ErrorAction Stop | \
                     Select-Object -ExpandProperty ReceivedBytes), \
                     (Get-NetAdapterStatistics -Name '{iface}' -ErrorAction Stop | \
                     Select-Object -ExpandProperty SentBytes)"
                ),
            ])
            .output()
            .map_err(|e| format!("bandwidth probe failed: {e}"))?;
        if !output.status.success() {
            return Err(format!(
                "interface '{iface}' missing or unreadable: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        // Without a second sample, report 0 (caller can blend with capacity).
        let _ = output;
        return Ok(0.0);
    }
    #[cfg(not(any(target_os = "linux", target_os = "windows")))]
    {
        Err(format!(
            "bandwidth measurement unsupported on this OS (interface '{iface}')"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::config::ClusterConfig;
    use std::time::Duration;

    #[test]
    fn hysteresis_drain_and_resume() {
        let mut cfg = ClusterConfig::default();
        cfg.drain_threshold = 0.8;
        cfg.resume_threshold = 0.5;
        let health = HealthTracker::new(Duration::from_millis(100));
        health.set_local(NodeHealthState::Ready);
        let adm = AdmissionController::new(cfg, health);
        assert!(adm.should_accept_new_publish());
        adm.update_load(0.9);
        assert!(!adm.should_accept_new_publish());
        adm.update_load(0.7);
        assert!(!adm.should_accept_new_publish());
        adm.update_load(0.4);
        assert!(adm.should_accept_new_publish());
    }
}
