//! Outbound RTMP client
//!
//! Mirrors `src/client/client.h` and `src/client/client.c`.

use std::net::TcpStream;
use std::os::unix::io::IntoRawFd;

use crate::buffer::Buffer;
use crate::chunk::reader::{chunk_read, ChunkMessage};
use crate::chunk::state::ChunkRegistry;
use crate::chunk::writer::chunk_write;
use crate::handshake::{self, Handshake};
use crate::message::command;
use crate::message::control;
use crate::message::message as msg_dispatch;
use crate::net;
use crate::transport::Transport;
use crate::types::*;

/// Handshake payload size (mirrors `handshake::HANDSHAKE_SIZE`, which is private).
const HANDSHAKE_SIZE: usize = 1536;

/// Max time to wait for the peer to send more data before giving up.
const RECV_POLL_TIMEOUT_MS: i32 = 10_000;

/// Client connection states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum ClientState {
    Disconnected = 0,
    Handshaking,
    Connected,
    AppConnected,
    StreamCreated,
    Publishing,
    Playing,
}

/// RTMP client object.
pub struct Client {
    pub client_fd: i32,
    pub transport: Option<Transport>,
    pub handshake: Handshake,
    pub state: ClientState,
    pub send_buffer: Buffer,
    pub recv_buffer: Buffer,
    pub chunk_reg: ChunkRegistry,
    pub stream_id: u32,
    pub app: String,
    pub stream_key: String,
    pub on_frame_cb: Option<fn(&Frame)>,
}

impl Client {
    /// Create a new client.
    pub fn new() -> Self {
        Self {
            client_fd: -1,
            transport: None,
            handshake: Handshake::default(),
            state: ClientState::Disconnected,
            send_buffer: Buffer::new(),
            recv_buffer: Buffer::new(),
            chunk_reg: ChunkRegistry::new(),
            stream_id: 0,
            app: String::new(),
            stream_key: String::new(),
            on_frame_cb: None,
        }
    }

    /// Connect to an RTMP server at `rtmp://host[:port]/app/streamKey`.
    ///
    /// Performs the real TCP connect, the legacy C0/C1/C2 handshake, then
    /// the `connect` + `createStream` AMF0 command exchange.
    pub fn connect(&mut self, url: &str) -> Result<()> {
        let (host, port, app, stream_key) = parse_rtmp_url(url)?;
        self.reset_session_state();

        let stream = TcpStream::connect((host.as_str(), port)).map_err(|_| ErrorCode::Io)?;
        let mut transport = Transport::new_plain(stream.into_raw_fd());

        self.state = ClientState::Handshaking;
        if let Err(e) = self.do_handshake(&mut transport) {
            // transport drops here, closing the fd via Transport::drop
            return Err(e);
        }

        self.client_fd = transport.fd();
        self.transport = Some(transport);
        self.app = app.clone();
        self.stream_key = stream_key;
        self.state = ClientState::Connected;

        if let Err(e) = self.do_amf_connect(&app, &host, port) {
            self.reset_session_state();
            return Err(e);
        }
        Ok(())
    }

    /// Begin publishing.
    pub fn publish(&mut self) -> Result<()> {
        if self.state != ClientState::AppConnected {
            return Err(ErrorCode::Protocol);
        }
        let mut amf = Buffer::with_capacity(256);
        command::build_publish(&mut amf, &self.stream_key, &self.app)?;
        self.send_command_msg(self.stream_id, amf.as_slice())?;
        self.wait_for_command("onStatus")?;
        self.state = ClientState::Publishing;
        Ok(())
    }

    /// Run the AMF connect + createStream exchange. Separated from `connect()`
    /// so the transport is already stored before we enter, letting the caller
    /// call `reset_session_state()` (which drops the transport) on any error.
    fn do_amf_connect(&mut self, app: &str, host: &str, port: u16) -> Result<()> {
        let tc_url = format!("rtmp://{host}:{port}/{app}");
        let mut connect_amf = Buffer::with_capacity(512);
        command::build_connect(&mut connect_amf, app, &tc_url, "", "", "FMLE/3.0", 0, 0)?;
        self.send_command_msg(0, connect_amf.as_slice())?;
        let mut result = self.wait_for_command("_result")?;
        command::read_connect_result(&mut result)?;

        let mut create_stream_amf = Buffer::with_capacity(64);
        command::build_create_stream(&mut create_stream_amf, 2.0)?;
        self.send_command_msg(0, create_stream_amf.as_slice())?;
        let mut create_result = self.wait_for_command("_result")?;
        let (_txn, stream_id) = command::read_create_stream_result(&mut create_result)?;
        self.stream_id = stream_id as u32;

        self.state = ClientState::AppConnected;
        Ok(())
    }

    /// Begin playing.
    pub fn play(&mut self) -> Result<()> {
        if self.state != ClientState::AppConnected {
            return Err(ErrorCode::Protocol);
        }
        let mut amf = Buffer::with_capacity(256);
        command::build_play(&mut amf, &self.stream_key)?;
        self.send_command_msg(self.stream_id, amf.as_slice())?;
        self.wait_for_command("onStatus")?;
        self.state = ClientState::Playing;
        Ok(())
    }

    /// Send a frame while publishing.
    pub fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        if self.state != ClientState::Publishing {
            return Err(ErrorCode::Protocol);
        }

        let mut cmsg = ChunkMessage::default();
        cmsg.timestamp = frame.timestamp;
        cmsg.msg_length = frame.size;
        cmsg.msg_stream_id = self.stream_id;

        if frame.frame_type == FrameType::Audio {
            cmsg.csid = 4;
            cmsg.msg_type_id = 0x08; // AUDIO
        } else {
            cmsg.csid = 6;
            cmsg.msg_type_id = 0x09; // VIDEO
        }
        cmsg.fmt = 0;

        let payload = unsafe { std::slice::from_raw_parts(frame.data, frame.size as usize) };
        chunk_write(
            &mut self.send_buffer,
            &cmsg,
            payload,
            frame.size as usize,
            128,
        )?;

        // Flush
        let data = self.send_buffer.peek().to_vec();
        if let Some(ref mut transport) = self.transport {
            transport.send(&data)?;
        }
        self.send_buffer.reset();

        Ok(())
    }

    /// Poll for incoming data while playing.
    pub fn poll(&mut self, timeout_ms: i32) -> Result<()> {
        if self.state != ClientState::Playing {
            return Err(ErrorCode::Protocol);
        }

        // Scope the mutable transport borrow to the recv phase only.
        let poll_fd = {
            let Some(t) = self.transport.as_ref() else {
                return Err(ErrorCode::Internal);
            };
            t.fd()
        };
        let mut pfd = libc::pollfd {
            fd: poll_fd,
            events: libc::POLLIN,
            revents: 0,
        };
        unsafe { libc::poll(&mut pfd, 1, timeout_ms.max(0)) };

        let mut buf = [0u8; 65536];
        loop {
            let (n, again) = {
                let Some(t) = self.transport.as_mut() else {
                    return Err(ErrorCode::Internal);
                };
                let mut again = 0i32;
                let n = t.recv(&mut buf, &mut again);
                (n, again)
            };
            if n > 0 {
                self.recv_buffer
                    .write(&buf[..n as usize])
                    .map_err(|_| ErrorCode::Internal)?;
            } else if n == 0 {
                return Err(ErrorCode::Io);
            } else if again != 0 {
                break;
            } else {
                break;
            }
        }

        loop {
            let mut msg = ChunkMessage::default();
            let mut payload_ptr: *const u8 = std::ptr::null();
            let mut payload_len = 0;
            match chunk_read(
                &mut self.recv_buffer,
                &mut self.chunk_reg,
                None,
                &mut msg,
                &mut payload_ptr,
                &mut payload_len,
            ) {
                Ok(1) if msg.is_complete => {
                    if msg.msg_type_id == msg_dispatch::RTMP_MSG_SET_CHUNK_SIZE {
                        let payload = if payload_ptr.is_null() || payload_len == 0 {
                            &[][..]
                        } else {
                            unsafe { std::slice::from_raw_parts(payload_ptr, payload_len) }
                        };
                        if let Ok(cs) = control::read_set_chunk_size(payload) {
                            self.chunk_reg.set_all_chunk_size(cs);
                        }
                    } else if msg.msg_type_id == msg_dispatch::RTMP_MSG_AUDIO
                        || msg.msg_type_id == msg_dispatch::RTMP_MSG_VIDEO
                    {
                        if let Some(ref cb) = self.on_frame_cb {
                            let payload = if payload_ptr.is_null() || payload_len == 0 {
                                &[][..]
                            } else {
                                unsafe { std::slice::from_raw_parts(payload_ptr, payload_len) }
                            };
                            let mut frame = Frame {
                                frame_type: if msg.msg_type_id == msg_dispatch::RTMP_MSG_AUDIO {
                                    FrameType::Audio
                                } else {
                                    FrameType::Video
                                },
                                timestamp: msg.timestamp,
                                ..Default::default()
                            };
                            frame.data = payload.as_ptr();
                            frame.size = payload.len() as u32;
                            cb(&frame);
                        }
                    }
                }
                Ok(_) => break,
                Err(_) => return Err(ErrorCode::Chunk),
            }
        }

        Ok(())
    }

    // ── Internal helpers ──

    /// Drop any prior socket and reset all protocol state before a new connect.
    /// Prevents stale recv/send buffers, chunk registry entries, and handshake
    /// state from a previous (failed) session polluting the next attempt.
    fn reset_session_state(&mut self) {
        // Drop transport first: it owns and closes the fd.
        self.transport = None;
        self.client_fd = -1;
        self.recv_buffer.reset();
        self.send_buffer.reset();
        self.chunk_reg.destroy();
        self.chunk_reg.init();
        handshake::client_init(&mut self.handshake);
        self.state = ClientState::Disconnected;
        self.stream_id = 0;
    }

    /// Drive the legacy C0/C1/C2 client handshake to completion over `transport`.
    fn do_handshake(&mut self, transport: &mut Transport) -> Result<()> {
        handshake::client_init(&mut self.handshake);
        handshake::client_generate_c0c1(&mut self.handshake)?;
        let c0c1 = self.handshake.out.peek().to_vec();
        transport.send(&c0c1)?;
        self.handshake.out.reset();

        let s0s1 = read_exact(transport, 1 + HANDSHAKE_SIZE)?;
        let mut buf = Buffer::new();
        buf.write(&s0s1).map_err(|_| ErrorCode::Internal)?;
        handshake::client_read_s0(&mut self.handshake, &mut buf)?;
        handshake::client_read_s1(&mut self.handshake, &mut buf)?;

        let c2 = self.handshake.out.peek().to_vec();
        transport.send(&c2)?;
        self.handshake.out.reset();

        let s2 = read_exact(transport, HANDSHAKE_SIZE)?;
        let mut buf2 = Buffer::new();
        buf2.write(&s2).map_err(|_| ErrorCode::Internal)?;
        handshake::client_read_s2(&mut self.handshake, &mut buf2)?;

        Ok(())
    }

    fn send_command_msg(&mut self, msg_stream_id: u32, amf_data: &[u8]) -> Result<()> {
        let mut cmsg = ChunkMessage::default();
        cmsg.csid = 3;
        cmsg.fmt = 0;
        cmsg.msg_length = amf_data.len() as u32;
        cmsg.msg_type_id = 0x14; // AMF0_COMMAND
        cmsg.msg_stream_id = msg_stream_id;
        chunk_write(&mut self.send_buffer, &cmsg, amf_data, amf_data.len(), 128)?;

        let data = self.send_buffer.peek().to_vec();
        if let Some(ref mut transport) = self.transport {
            transport.send(&data)?;
        }
        self.send_buffer.reset();
        Ok(())
    }

    /// Block until an AMF0 command named `want` is received, returning its payload buffer.
    fn wait_for_command(&mut self, want: &str) -> Result<Buffer> {
        for _ in 0..64 {
            let (msg, payload) = self.recv_message()?;
            if msg.msg_type_id != msg_dispatch::RTMP_MSG_AMF0_COMMAND {
                continue;
            }
            let mut buf = Buffer::from_slice(&payload);
            let mut name_buf = [0u8; 64];
            if command::peek_name(&mut buf, &mut name_buf).is_err() {
                continue;
            }
            let name = std::str::from_utf8(&name_buf)
                .unwrap_or("")
                .trim_end_matches('\0');
            if name == want {
                return Ok(buf);
            }
        }
        Err(ErrorCode::Timeout)
    }

    /// Block until one fully-reassembled chunk message is available.
    fn recv_message(&mut self) -> Result<(ChunkMessage, Vec<u8>)> {
        loop {
            let mut msg = ChunkMessage::default();
            let mut payload_ptr: *const u8 = std::ptr::null();
            let mut payload_len = 0;
            match chunk_read(
                &mut self.recv_buffer,
                &mut self.chunk_reg,
                None,
                &mut msg,
                &mut payload_ptr,
                &mut payload_len,
            ) {
                Ok(1) if msg.is_complete => {
                    let payload = if payload_ptr.is_null() || payload_len == 0 {
                        Vec::new()
                    } else {
                        unsafe { std::slice::from_raw_parts(payload_ptr, payload_len) }.to_vec()
                    };
                    if msg.msg_type_id == msg_dispatch::RTMP_MSG_SET_CHUNK_SIZE {
                        if let Ok(cs) = control::read_set_chunk_size(&payload) {
                            self.chunk_reg.set_all_chunk_size(cs);
                        }
                        continue;
                    }
                    return Ok((msg, payload));
                }
                Ok(_) => {}
                Err(_) => return Err(ErrorCode::Chunk),
            }

            // Scope mutable transport borrow tightly to avoid conflict with
            // other self fields (recv_buffer) used after the borrow ends.
            let mut tmp = [0u8; 4096];
            let (n, again, t_fd) = {
                let t = self.transport.as_mut().ok_or(ErrorCode::Internal)?;
                let mut again = 0i32;
                let n = t.recv(&mut tmp, &mut again);
                (n, again, t.fd())
            };
            if n > 0 {
                self.recv_buffer
                    .write(&tmp[..n as usize])
                    .map_err(|_| ErrorCode::Internal)?;
            } else if n == 0 {
                return Err(ErrorCode::Io);
            } else if again != 0 {
                let mut pfd = libc::pollfd {
                    fd: t_fd,
                    events: libc::POLLIN,
                    revents: 0,
                };
                let rc = unsafe { libc::poll(&mut pfd, 1, RECV_POLL_TIMEOUT_MS) };
                if rc == 0 {
                    return Err(ErrorCode::Timeout);
                }
            } else {
                return Err(ErrorCode::Io);
            }
        }
    }
}

/// Block until exactly `n` bytes have been read from `transport`.
fn read_exact(transport: &mut Transport, n: usize) -> Result<Vec<u8>> {
    let mut out = vec![0u8; n];
    let mut got = 0;
    while got < n {
        let mut again = 0i32;
        let r = transport.recv(&mut out[got..], &mut again);
        if r > 0 {
            got += r as usize;
        } else if r == 0 {
            return Err(ErrorCode::Io);
        } else if again != 0 {
            let mut pfd = libc::pollfd {
                fd: transport.fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            let rc = unsafe { libc::poll(&mut pfd, 1, RECV_POLL_TIMEOUT_MS) };
            if rc == 0 {
                return Err(ErrorCode::Timeout);
            }
        } else {
            return Err(ErrorCode::Io);
        }
    }
    Ok(out)
}

/// Parse `rtmp://host[:port]/app/streamKey` into (host, port, app, stream_key).
fn parse_rtmp_url(url: &str) -> Result<(String, u16, String, String)> {
    let rest = url.strip_prefix("rtmp://").ok_or(ErrorCode::Internal)?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i + 1..]),
        None => (rest, ""),
    };

    let mut host = String::new();
    let mut port_str = String::new();
    net::split_host_port(authority, &mut host, &mut port_str, "1935")?;
    let port: u16 = port_str.parse().map_err(|_| ErrorCode::Internal)?;

    let mut parts = path.splitn(2, '/');
    let app = parts.next().unwrap_or("").to_string();
    let stream_key = parts.next().unwrap_or("").to_string();

    if app.is_empty() || stream_key.is_empty() {
        return Err(ErrorCode::Internal);
    }

    Ok((host, port, app, stream_key))
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // The Transport owns the fd when set; only close directly if there is
        // no transport (e.g. the fd was set but connecting failed before the
        // transport was stored, which cannot currently happen — this guard is
        // here for correctness if the two ever diverge).
        if self.transport.is_none() && self.client_fd >= 0 {
            unsafe {
                libc::close(self.client_fd);
            }
        }
    }
}
