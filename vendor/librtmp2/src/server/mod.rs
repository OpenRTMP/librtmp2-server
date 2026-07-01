//! RTMP server listener
//!
//! Mirrors `src/server/server.h` and `src/server/server.c`.

use std::collections::HashMap;
use std::net::TcpListener;
use std::os::unix::io::{AsRawFd, IntoRawFd};

use crate::net;
use crate::session::conn::Conn;
use crate::transport::{TlsCtx, Transport};
use crate::types::*;

/// Cached codec headers and last keyframe for a (app, stream_name) pair.
/// Replayed to players that join after the publisher has already sent headers.
struct StreamCache {
    avc_header: Option<Vec<u8>>,
    aac_header: Option<Vec<u8>>,
    /// (timestamp, payload) of the most recent IDR keyframe.
    last_keyframe: Option<(u32, Vec<u8>)>,
}

/// Server object.
pub struct Server {
    pub config: ServerConfig,
    pub running: bool,
    pub server_fd: i32,
    pub connections: Vec<Conn>,
    pub tls_ctx: Option<TlsCtx>,
    /// Fired for every audio/video frame on every connection.
    pub on_frame_cb: Option<fn(&Frame)>,
    /// Fired when a client completes the AMF `connect` exchange.
    pub on_connect_cb: Option<fn()>,
    listener: Option<TcpListener>,
    stream_cache: HashMap<(String, String), StreamCache>,
}

impl Server {
    /// Create a new server.
    pub fn new(config: ServerConfig) -> Result<Self> {
        let tls_ctx = if config.tls_enabled != 0 {
            if config.tls_cert_file.is_null() || config.tls_key_file.is_null() {
                return Err(ErrorCode::Internal);
            }
            let cert = unsafe { std::ffi::CStr::from_ptr(config.tls_cert_file as *const std::ffi::c_char) };
            let key = unsafe { std::ffi::CStr::from_ptr(config.tls_key_file as *const std::ffi::c_char) };
            Some(TlsCtx::new_server(
                cert.to_str().unwrap_or(""),
                key.to_str().unwrap_or(""),
            )?)
        } else {
            None
        };

        Ok(Self {
            config,
            running: false,
            server_fd: -1,
            connections: Vec::new(),
            tls_ctx,
            on_frame_cb: None,
            on_connect_cb: None,
            listener: None,
            stream_cache: HashMap::new(),
        })
    }

    /// Start listening on the given address ("host:port", default port 1935).
    pub fn listen(&mut self, bind_addr: &str) -> Result<()> {
        let mut host = String::new();
        let mut port = String::new();
        net::split_host_port(bind_addr, &mut host, &mut port, "1935")?;
        let addr = if host.is_empty() {
            format!("0.0.0.0:{port}")
        } else if host.contains(':') {
            format!("[{host}]:{port}")
        } else {
            format!("{host}:{port}")
        };

        let listener = TcpListener::bind(&addr).map_err(|_| ErrorCode::Io)?;
        listener.set_nonblocking(true).map_err(|_| ErrorCode::Io)?;

        self.server_fd = listener.as_raw_fd();
        self.listener = Some(listener);
        self.running = true;
        Ok(())
    }

    /// Poll for events (non-blocking).
    pub fn poll(&mut self, timeout_ms: i32) -> Result<()> {
        if !self.running {
            return Err(ErrorCode::Internal);
        }
        self.accept_new_connections();
        self.process_connections()?;
        if timeout_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(timeout_ms as u64));
        }
        Ok(())
    }

    /// Stop the server.
    pub fn stop(&mut self) {
        self.running = false;
        self.listener = None;
    }

    /// Accept any pending inbound connections (non-blocking).
    fn accept_new_connections(&mut self) {
        let Some(listener) = self.listener.as_ref() else {
            return;
        };
        loop {
            if self.config.max_connections > 0
                && self.connections.len() >= self.config.max_connections as usize
            {
                break;
            }
            match listener.accept() {
                Ok((stream, _addr)) => {
                    let transport = if let Some(ref ctx) = self.tls_ctx {
                        // TlsCtx::accept() takes ownership of the fd, sets the socket
                        // to blocking for the handshake, then restores non-blocking.
                        // On error the fd is already closed inside accept(); skip the conn.
                        match ctx.accept(stream.into_raw_fd()) {
                            Ok(t) => t,
                            Err(_) => continue,
                        }
                    } else {
                        let _ = stream.set_nonblocking(true);
                        Transport::new_plain(stream.into_raw_fd())
                    };
                    let conn_fd = transport.fd();
                    let mut conn = Conn::new();
                    conn.client_fd = conn_fd;
                    conn.transport = Some(transport);
                    conn.on_frame_cb = self.on_frame_cb;
                    conn.on_connect_cb = self.on_connect_cb;
                    self.connections.push(conn);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }
    }

    /// Process all active connections: drain readable bytes, drive the
    /// protocol state machine, relay frames from publishers to players,
    /// flush pending writes, and reap closed peers.
    pub fn process_connections(&mut self) -> Result<()> {
        let mut buf = [0u8; 65536];
        let mut closed = Vec::new();

        // Drive recv/processing for every connection.
        for (i, conn) in self.connections.iter_mut().enumerate() {
            loop {
                let Some(transport) = conn.transport.as_mut() else {
                    closed.push(i);
                    break;
                };
                let mut again = 0i32;
                let n = transport.recv(&mut buf, &mut again);
                if n > 0 {
                    if conn.recv(&buf[..n as usize]).is_err() {
                        closed.push(i);
                        break;
                    }
                } else if n == 0 {
                    closed.push(i);
                    break;
                } else if again != 0 {
                    break;
                } else {
                    closed.push(i);
                    break;
                }
            }
        }

        // Collect all frames queued by publishers, then relay them to players
        // on the same (app, stream_name) pair.
        let relay_frames: Vec<_> = self
            .connections
            .iter_mut()
            .flat_map(|c| c.pending_relay.drain(..))
            .collect();

        // Replay cached codec headers and last keyframe to newly-joined players
        // using the pre-batch cache state, so init frames always precede live
        // frames from the current batch.
        for conn in self.connections.iter_mut() {
            if !conn.needs_init_frames {
                continue;
            }
            let Some(ref stream) = conn.current_stream else {
                continue;
            };
            if !stream.is_playing || !conn.relay_enabled {
                continue;
            }
            conn.needs_init_frames = false;
            let key = (conn.app.clone(), stream.name.clone());
            if let Some(cache) = self.stream_cache.get(&key) {
                if let Some(ref hdr) = cache.avc_header.clone() {
                    let _ = conn.send_frame(FrameType::Video, 0, hdr);
                }
                if let Some(ref hdr) = cache.aac_header.clone() {
                    let _ = conn.send_frame(FrameType::Audio, 0, hdr);
                }
                if let Some((ts, ref kf)) = cache.last_keyframe.clone() {
                    let _ = conn.send_frame(FrameType::Video, ts, kf);
                }
            }
        }

        // Update per-stream cache and relay each frame in order so players
        // receive frames in the same sequence the publisher sent them.
        for frame in &relay_frames {
            let key = (frame.app.clone(), frame.stream_name.clone());
            let cache = self.stream_cache.entry(key).or_insert(StreamCache {
                avc_header: None,
                aac_header: None,
                last_keyframe: None,
            });
            match frame.frame_type {
                FrameType::Video => {
                    if frame.payload.len() >= 2 {
                        if frame.payload[0] == 0x17 && frame.payload[1] == 0x00 {
                            cache.avc_header = Some(frame.payload.clone());
                        } else if frame.payload[0] == 0x17 && frame.payload[1] == 0x01 {
                            cache.last_keyframe = Some((frame.timestamp, frame.payload.clone()));
                        }
                    }
                }
                FrameType::Audio => {
                    if frame.payload.len() >= 2
                        && (frame.payload[0] & 0xF0) == 0xA0
                        && frame.payload[1] == 0x00
                    {
                        cache.aac_header = Some(frame.payload.clone());
                    }
                }
                _ => {}
            }

            for conn in self.connections.iter_mut() {
                let is_player = conn.relay_enabled
                    && conn
                        .current_stream
                        .as_ref()
                        .map(|s| s.is_playing && s.name == frame.stream_name)
                        .unwrap_or(false);
                if !is_player || conn.app != frame.app {
                    continue;
                }
                let _ = conn.send_frame(frame.frame_type, frame.timestamp, &frame.payload);
            }
        }

        // Flush all connections.
        for (i, conn) in self.connections.iter_mut().enumerate() {
            if conn.flush().is_err() {
                closed.push(i);
            }
        }

        // A connection that errors on both recv and flush gets pushed twice.
        // Sort then dedup so each index is removed exactly once.
        closed.sort_unstable();
        closed.dedup();
        for i in closed.into_iter().rev() {
            // Evict the cache when a publisher closes so stale headers/keyframes
            // do not persist for the next publisher reusing the same key.
            if let Some(ref stream) = self.connections[i].current_stream {
                if stream.is_publishing {
                    self.stream_cache
                        .remove(&(self.connections[i].app.clone(), stream.name.clone()));
                }
            }
            self.connections.remove(i);
        }
        Ok(())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.running = false;
    }
}
