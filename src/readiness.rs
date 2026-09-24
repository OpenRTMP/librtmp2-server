//! Persistent `epoll(7)` readiness set for the RTMP poll loop (Linux).
//!
//! `server::wait_for_readiness_or_timeout` rebuilt a `pollfd` array from
//! every listener and connection on every tick and handed the whole array to
//! `poll(2)`, which the kernel then scans in full on each call -- O(total
//! connections) of kernel work per tick even when only the publisher's socket
//! has data. This keeps one epoll instance registered with the same fds and
//! only issues `epoll_ctl` for the connections whose interest actually
//! changed since the last tick (new/closed connections, and players whose
//! outbound backlog started or stopped needing `EPOLLOUT`), so a wait costs
//! O(ready fds) in the kernel.

use std::collections::{HashMap, HashSet};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

/// What one fd is registered for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Interest {
    /// `None` for a listener.
    conn_id: Option<u64>,
    events: u32,
}

pub(crate) struct EpollReadiness {
    epfd: OwnedFd,
    registered: HashMap<i32, Interest>,
    events: Vec<libc::epoll_event>,
}

impl EpollReadiness {
    pub(crate) fn new() -> std::io::Result<Self> {
        let fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self {
            epfd: unsafe { OwnedFd::from_raw_fd(fd) },
            registered: HashMap::new(),
            events: Vec::new(),
        })
    }

    /// Wait until a registered fd is ready or `timeout_ms` elapses. Same
    /// contract as `server::poll_readiness`: reports the `conn_id`s that
    /// are readable (or errored/hung up) and whether a listener is, and
    /// `None` only when the wait itself failed ("assume everyone is
    /// readable").
    pub(crate) fn wait(
        &mut self,
        server: &librtmp2::server::Server,
        timeout_ms: u64,
    ) -> Option<crate::server::Wake> {
        if self.sync(server).is_err() {
            std::thread::sleep(std::time::Duration::from_millis(timeout_ms));
            return None;
        }
        let capacity = self.registered.len().max(1);
        self.events.clear();
        self.events
            .resize(capacity, libc::epoll_event { events: 0, u64: 0 });
        let timeout = timeout_ms.min(i32::MAX as u64) as i32;
        let n = loop {
            let rc = unsafe {
                libc::epoll_wait(
                    self.epfd.as_raw_fd(),
                    self.events.as_mut_ptr(),
                    capacity as i32,
                    timeout,
                )
            };
            if rc >= 0 {
                break rc as usize;
            }
            if std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                std::thread::sleep(std::time::Duration::from_millis(timeout_ms));
                return None;
            }
            // EINTR: retry with the full timeout, as the poll(2) path does.
        };

        const READY_MASK: u32 = (libc::EPOLLIN | libc::EPOLLERR | libc::EPOLLHUP) as u32;
        let mut ready = HashSet::new();
        let mut listener_ready = false;
        for ev in &self.events[..n] {
            let fd = ev.u64 as i32;
            let Some(interest) = self.registered.get(&fd) else {
                continue;
            };
            if ev.events & READY_MASK == 0 {
                // EPOLLOUT only: the wake itself is the point (the caller
                // flushes every connection); nothing new to recv.
                continue;
            }
            match interest.conn_id {
                Some(conn_id) => {
                    ready.insert(conn_id);
                }
                None => listener_ready = true,
            }
        }
        Some(crate::server::Wake {
            ready,
            listener_ready,
        })
    }

    /// Bring the epoll registrations in line with the server's current
    /// listeners and connections.
    fn sync(&mut self, server: &librtmp2::server::Server) -> std::io::Result<()> {
        let at_connection_cap = server.config.max_connections > 0
            && server.connections.len() >= server.config.max_connections as usize;
        let mut desired: HashMap<i32, Interest> =
            HashMap::with_capacity(server.connections.len() + 2);
        if !at_connection_cap {
            for fd in server.listener_fds() {
                desired.insert(
                    fd,
                    Interest {
                        conn_id: None,
                        events: libc::EPOLLIN as u32,
                    },
                );
            }
        }
        for conn in server.connections.iter().filter(|c| c.client_fd >= 0) {
            // Outbound bytes still queued: also wake on writability so they
            // get flushed as soon as the peer's window opens. Only while
            // bytes are pending, so drained connections can't busy-loop.
            let events = if conn.send_buffer.available() > 0 {
                (libc::EPOLLIN | libc::EPOLLOUT) as u32
            } else {
                libc::EPOLLIN as u32
            };
            desired.insert(
                conn.client_fd,
                Interest {
                    conn_id: Some(conn.conn_id),
                    events,
                },
            );
        }

        let stale: Vec<i32> = self
            .registered
            .keys()
            .filter(|fd| !desired.contains_key(fd))
            .copied()
            .collect();
        for fd in stale {
            self.registered.remove(&fd);
            // A closed fd is already gone from the epoll set; ENOENT/EBADF
            // here are expected and harmless.
            let _ = self.ctl(libc::EPOLL_CTL_DEL, fd, 0);
        }

        for (fd, want) in desired {
            match self.registered.get(&fd) {
                Some(have) if *have == want => continue,
                Some(_) => {
                    // Interest changed, or the fd was closed and reused by a
                    // new connection (the kernel dropped the old
                    // registration on close, so MOD reports ENOENT).
                    if let Err(e) = self.ctl(libc::EPOLL_CTL_MOD, fd, want.events) {
                        if e.raw_os_error() != Some(libc::ENOENT) {
                            return Err(e);
                        }
                        self.ctl(libc::EPOLL_CTL_ADD, fd, want.events)?;
                    }
                }
                None => {
                    if let Err(e) = self.ctl(libc::EPOLL_CTL_ADD, fd, want.events) {
                        if e.raw_os_error() != Some(libc::EEXIST) {
                            return Err(e);
                        }
                        self.ctl(libc::EPOLL_CTL_MOD, fd, want.events)?;
                    }
                }
            }
            self.registered.insert(fd, want);
        }
        Ok(())
    }

    fn ctl(&self, op: i32, fd: i32, events: u32) -> std::io::Result<()> {
        let mut ev = libc::epoll_event {
            events,
            u64: fd as u64,
        };
        let rc = unsafe { libc::epoll_ctl(self.epfd.as_raw_fd(), op, fd, &mut ev) };
        if rc < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}
