//! Connection management
//!
//! Mirrors `src/session/conn.h` and `src/session/conn.c`.

use std::sync::Mutex;

use crate::buffer::Buffer;
use crate::chunk::reader::{chunk_read, ChunkMessage};
use crate::chunk::state::ChunkRegistry;
use crate::chunk::writer::chunk_write;
use crate::handshake::{self, Handshake, HandshakeState};
use crate::message::command;
use crate::message::message as msg_dispatch;
use crate::session::state_machine;
use crate::session::stream::Stream;
use crate::transport::Transport;
use crate::types::*;

/// Maximum streams per connection
pub const MAX_STREAMS_PER_CONN: u32 = 16;

/// Server window ack size
const SERVER_WINDOW_ACK_SIZE: u32 = 2_500_000;
/// Server peer bandwidth
const SERVER_PEER_BANDWIDTH: u32 = 2_500_000;
/// Peer bandwidth limit type (dynamic)
const PEER_BANDWIDTH_DYNAMIC: u8 = 2;

/// A frame queued for relay to player connections.
pub struct RelayFrame {
    pub frame_type: FrameType,
    pub timestamp: u32,
    pub payload: Vec<u8>,
    /// App name from the publisher's RTMP connect.
    pub app: String,
    /// Stream name from the publisher.
    pub stream_name: String,
}

/// Connection object.
pub struct Conn {
    pub state: ConnState,
    pub handshake: Handshake,
    pub recv_buffer: Buffer,
    pub send_buffer: Buffer,
    pub chunk_reg: ChunkRegistry,
    pub chunk_size: u32,
    pub window_ack_size: u32,
    pub bytes_received: u32,
    pub bytes_at_last_ack: u32,
    pub client_fd: i32,
    pub transport: Option<Transport>,
    pub app: String,
    pub next_stream_id: u32,
    pub current_stream: Option<Box<Stream>>,
    pub connect_cb_fired: bool,
    pub send_mutex: Mutex<()>,
    /// Frames received from a publisher, waiting to be relayed to players.
    pub pending_relay: Vec<RelayFrame>,
    /// Set when a player just joined; the server replays cached codec headers
    /// and the last keyframe before forwarding live frames.
    pub needs_init_frames: bool,
    /// FourCC / codec string of the first video frame seen on this connection.
    /// Populated from the FLV/E-RTMP header on the first inbound video frame.
    pub detected_video_codec: Option<String>,
    /// Codec string of the first audio frame seen on this connection.
    pub detected_audio_codec: Option<String>,
    /// Set by the application after publish/play authorization succeeds.
    /// While false, inbound media is not queued for relay and players do not
    /// receive cached or live frames from unauthorized sessions.
    pub relay_enabled: bool,
    // Callbacks
    pub on_frame_cb: Option<fn(&Frame)>,
    /// Fired once when the RTMP `connect` command completes successfully.
    pub on_connect_cb: Option<fn()>,
}

impl Conn {
    /// Create a new connection.
    pub fn new() -> Self {
        let mut chunk_reg = ChunkRegistry::new();
        chunk_reg.init();

        Self {
            state: ConnState::TcpAccepted,
            handshake: Handshake::default(),
            recv_buffer: Buffer::new(),
            send_buffer: Buffer::new(),
            chunk_reg,
            chunk_size: 128,
            window_ack_size: 0,
            bytes_received: 0,
            bytes_at_last_ack: 0,
            client_fd: -1,
            transport: None,
            app: String::new(),
            next_stream_id: 0,
            current_stream: None,
            connect_cb_fired: false,
            send_mutex: Mutex::new(()),
            pending_relay: Vec::new(),
            needs_init_frames: false,
            detected_video_codec: None,
            detected_audio_codec: None,
            relay_enabled: false,
            on_frame_cb: None,
            on_connect_cb: None,
        }
    }

    /// Get the file descriptor.
    pub fn get_fd(&self) -> i32 {
        self.client_fd
    }

    /// Feed received data into the connection.
    pub fn recv(&mut self, data: &[u8]) -> Result<()> {
        self.recv_buffer
            .write(data)
            .map_err(|_| ErrorCode::Internal)?;
        self.bytes_received = self.bytes_received.wrapping_add(data.len() as u32);

        let mut max_iter = 65536;
        let mut no_progress = 0;

        while max_iter > 0 {
            max_iter -= 1;
            let avail = self.recv_buffer.available();
            if avail == 0 && self.state != ConnState::Handshake {
                break;
            }
            let before = avail;
            let rc = self.process();
            if rc < 0 {
                return Err(match rc {
                    -1 => ErrorCode::Io,
                    -2 => ErrorCode::Timeout,
                    -3 => ErrorCode::Protocol,
                    -4 => ErrorCode::Handshake,
                    -5 => ErrorCode::Chunk,
                    -6 => ErrorCode::Amf,
                    -7 => ErrorCode::Unsupported,
                    -8 => ErrorCode::Auth,
                    -9 => ErrorCode::Internal,
                    _ => ErrorCode::Internal,
                });
            }
            if rc == 0 {
                let after = self.recv_buffer.available();
                if after == before {
                    no_progress += 1;
                    if no_progress > 3 {
                        break;
                    }
                } else {
                    no_progress = 0;
                }
                if after == 0 && self.state < ConnState::Closing {
                    break;
                }
            } else {
                no_progress = 0;
            }
        }

        // Send acknowledgement if window exceeded.
        // wrapping_sub handles the case where bytes_received has wrapped past 0.
        if self.window_ack_size > 0
            && self.bytes_received.wrapping_sub(self.bytes_at_last_ack) >= self.window_ack_size
        {
            self.send_acknowledgement(self.bytes_received)?;
            self.bytes_at_last_ack = self.bytes_received;
        }

        Ok(())
    }

    /// Process one step of the connection state machine.
    pub fn process(&mut self) -> i32 {
        match self.state {
            ConnState::TcpAccepted | ConnState::Handshake => self.do_handshake(),
            ConnState::Connected
            | ConnState::AppConnected
            | ConnState::StreamCreated
            | ConnState::Publishing
            | ConnState::Playing
            | ConnState::CapsNegotiated => self.read_messages(),
            ConnState::Closing | ConnState::Closed => 0,
        }
    }

    /// Perform the RTMP handshake.
    pub fn do_handshake(&mut self) -> i32 {
        match self.handshake.state {
            HandshakeState::ServerWaitC0 => {
                handshake::server_init(&mut self.handshake);
                match handshake::server_read_c0(&mut self.handshake, &mut self.recv_buffer) {
                    Ok(()) => {
                        self.state = ConnState::Handshake;
                        self.do_handshake_recurse()
                    }
                    Err(ErrorCode::Io) => 0,
                    Err(e) => e as i32,
                }
            }
            HandshakeState::ServerWaitC1 => self.do_handshake_recurse(),
            HandshakeState::ServerWaitC2 => {
                match handshake::server_read_c2(&mut self.handshake, &mut self.recv_buffer) {
                    Ok(()) => {
                        self.state = ConnState::Connected;
                        1
                    }
                    Err(ErrorCode::Io) => 0,
                    Err(e) => e as i32,
                }
            }
            HandshakeState::Done => {
                self.state = ConnState::Connected;
                1
            }
            _ => -1,
        }
    }

    fn do_handshake_recurse(&mut self) -> i32 {
        match handshake::server_read_c1(&mut self.handshake, &mut self.recv_buffer) {
            Ok(()) => {
                // Queue S0+S1+S2; flush() drains without blocking the poll loop.
                // Only reset handshake.out after both writes succeed so bytes
                // are not lost on a buffer-append failure.
                if self.client_fd >= 0 {
                    let s0 = [0x03u8];
                    if self.send_buffer.write(&s0).is_err() {
                        return ErrorCode::Internal as i32;
                    }
                    let out_data = self.handshake.out.peek();
                    if self.send_buffer.write(out_data).is_err() {
                        return ErrorCode::Internal as i32;
                    }
                }
                self.handshake.out.reset();
                1
            }
            Err(ErrorCode::Io) => 0,
            Err(e) => e as i32,
        }
    }

    /// Read and dispatch messages.
    pub fn read_messages(&mut self) -> i32 {
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
                Ok(0) => break,
                Ok(1) => {
                    if msg.is_complete {
                        // Dispatch message
                        let payload_slice = if payload_ptr.is_null() || payload_len == 0 {
                            &[]
                        } else {
                            unsafe { std::slice::from_raw_parts(payload_ptr, payload_len) }
                        };
                        let _ = self.handle_message(&msg, payload_slice);
                        let _ = self.flush();
                    }
                }
                Ok(_) => break,
                Err(_) => return -1,
            }
        }
        1
    }

    /// Handle a reassembled message.
    fn handle_message(&mut self, msg: &ChunkMessage, payload: &[u8]) -> Result<()> {
        match msg.msg_type_id {
            msg_dispatch::RTMP_MSG_AMF0_COMMAND => self.handle_command(payload),
            msg_dispatch::RTMP_MSG_AUDIO => {
                if self.relay_enabled
                    && self
                        .current_stream
                        .as_ref()
                        .map(|s| s.is_publishing)
                        .unwrap_or(false)
                {
                    // Detect audio codec from the first audio frame.
                    if self.detected_audio_codec.is_none() {
                        self.detected_audio_codec = detect_audio_codec(payload);
                    }

                    let stream_name = self
                        .current_stream
                        .as_ref()
                        .map(|s| s.name.clone())
                        .unwrap_or_default();
                    // Push first so the payload lives in stable heap storage
                    // owned by pending_relay before we hand a raw pointer to
                    // the FFI callback. Moving a Vec doesn't move its heap
                    // buffer, so the pointer remains valid for the connection
                    // lifetime (or until pending_relay is drained).
                    self.pending_relay.push(RelayFrame {
                        frame_type: FrameType::Audio,
                        timestamp: msg.timestamp,
                        payload: payload.to_vec(),
                        app: self.app.clone(),
                        stream_name,
                    });
                    if let Some(cb) = self.on_frame_cb {
                        let relay = self.pending_relay.last().unwrap();
                        let mut frame = Frame {
                            frame_type: FrameType::Audio,
                            timestamp: msg.timestamp,
                            ..Default::default()
                        };
                        frame.data = relay.payload.as_ptr();
                        frame.size = relay.payload.len() as u32;
                        cb(&frame);
                    }
                }
                Ok(())
            }
            msg_dispatch::RTMP_MSG_VIDEO => {
                if self.relay_enabled
                    && self
                        .current_stream
                        .as_ref()
                        .map(|s| s.is_publishing)
                        .unwrap_or(false)
                {
                    // Detect video codec from the first video frame.
                    if self.detected_video_codec.is_none() {
                        self.detected_video_codec = detect_video_codec(payload);
                    }

                    let stream_name = self
                        .current_stream
                        .as_ref()
                        .map(|s| s.name.clone())
                        .unwrap_or_default();
                    self.pending_relay.push(RelayFrame {
                        frame_type: FrameType::Video,
                        timestamp: msg.timestamp,
                        payload: payload.to_vec(),
                        app: self.app.clone(),
                        stream_name,
                    });
                    if let Some(cb) = self.on_frame_cb {
                        let relay = self.pending_relay.last().unwrap();
                        let mut frame = Frame {
                            frame_type: FrameType::Video,
                            timestamp: msg.timestamp,
                            ..Default::default()
                        };
                        frame.data = relay.payload.as_ptr();
                        frame.size = relay.payload.len() as u32;
                        cb(&frame);
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Handle an AMF0 command message.
    pub fn handle_command(&mut self, payload: &[u8]) -> Result<()> {
        let mut buf = Buffer::from_slice(payload);

        let mut name_buf = [0u8; 64];
        if command::peek_name(&mut buf, &mut name_buf).is_err() {
            return Ok(());
        }

        let name = std::str::from_utf8(&name_buf)
            .unwrap_or("")
            .trim_end_matches('\0');

        match name {
            "connect" => {
                let mut info = ConnectInfo::default();
                command::read_connect(&mut buf, &mut info)?;
                let app_len = info.app.iter().position(|&b| b == 0).unwrap_or(0);
                self.app = std::str::from_utf8(&info.app[..app_len])
                    .unwrap_or("")
                    .to_string();
                let _ = state_machine::conn_transition(&mut self.state, ConnState::AppConnected);
                self.send_connect_response(info.transaction_id)?;
                // Fire on_connect callback once per connection.
                if !self.connect_cb_fired {
                    self.connect_cb_fired = true;
                    if let Some(cb) = self.on_connect_cb {
                        cb();
                    }
                }
            }
            "createStream" => {
                // Must have completed the AMF 'connect' exchange first.
                if self.state < ConnState::AppConnected {
                    return self.send_onstatus(
                        0,
                        "error",
                        "NetStream.Failed",
                        "connect required before createStream",
                    );
                }
                let txn = command::read_create_stream(&mut buf)?;
                if self.next_stream_id >= MAX_STREAMS_PER_CONN {
                    self.send_onstatus(0, "error", "NetStream.Failed", "Too many streams")?;
                } else {
                    self.next_stream_id += 1;
                    let stream_id = self.next_stream_id;
                    self.current_stream = Some(Box::new(Stream::new(stream_id)));
                    let _ =
                        state_machine::conn_transition(&mut self.state, ConnState::StreamCreated);
                    self.send_create_stream_response(txn, stream_id)?;
                }
            }
            "publish" => {
                let mut stream_name = [0u8; 256];
                let mut publish_type = [0u8; 64];
                command::read_publish(&mut buf, &mut stream_name, &mut publish_type)?;
                let name_str = std::str::from_utf8(&stream_name)
                    .unwrap_or("")
                    .trim_end_matches('\0')
                    .to_string();
                if self.current_stream.is_none() {
                    self.send_onstatus(
                        0,
                        "error",
                        "NetStream.Publish.BadConnection",
                        "No stream created",
                    )?;
                } else {
                    if let Some(ref mut stream) = self.current_stream {
                        stream.is_publishing = true;
                        stream.name = name_str;
                    }
                    let _ = state_machine::conn_transition(&mut self.state, ConnState::Publishing);
                    let sid = self
                        .current_stream
                        .as_ref()
                        .map(|s| s.stream_id)
                        .unwrap_or(0);
                    self.send_onstatus(sid, "status", "NetStream.Publish.Start", "Publishing")?;
                }
            }
            "play" => {
                let mut stream_name = [0u8; 256];
                command::read_play(&mut buf, &mut stream_name)?;
                let name_str = std::str::from_utf8(&stream_name)
                    .unwrap_or("")
                    .trim_end_matches('\0')
                    .to_string();
                if self.current_stream.is_none() {
                    self.send_onstatus(
                        0,
                        "error",
                        "NetStream.Play.BadConnection",
                        "No stream created",
                    )?;
                } else {
                    if let Some(ref mut stream) = self.current_stream {
                        stream.is_playing = true;
                        stream.name = name_str;
                    }
                    self.needs_init_frames = true;
                    let _ = state_machine::conn_transition(&mut self.state, ConnState::Playing);
                    let sid = self
                        .current_stream
                        .as_ref()
                        .map(|s| s.stream_id)
                        .unwrap_or(0);
                    self.send_onstatus(sid, "status", "NetStream.Play.Start", "Playing")?;
                }
            }
            "FCPublish" | "FCUnpublish" | "releaseStream" | "deleteStream" => {
                // Ignored
            }
            _ => {}
        }

        Ok(())
    }

    /// Send a connect response.
    pub fn send_connect_response(&mut self, transaction_id: f64) -> Result<()> {
        // WindowAckSize
        let win = SERVER_WINDOW_ACK_SIZE.to_be_bytes();
        self.send_control(0x05, &win)?;

        // SetPeerBandwidth
        let mut bw = [0u8; 5];
        let bw_val = SERVER_PEER_BANDWIDTH.to_be_bytes();
        bw[..4].copy_from_slice(&bw_val);
        bw[4] = PEER_BANDWIDTH_DYNAMIC;
        self.send_control(0x06, &bw)?;

        // SetChunkSize
        let cs = self.chunk_size.to_be_bytes();
        self.send_control(0x01, &cs)?;

        // AMF0 _result: name, txn, properties object, information object.
        let mut amf_buf = Buffer::with_capacity(512);
        crate::amf::amf0::write_string(&mut amf_buf, "_result")?;
        crate::amf::amf0::write_number(&mut amf_buf, transaction_id)?;
        crate::amf::amf0::write_null(&mut amf_buf)?;
        crate::amf::amf0::write_object_begin(&mut amf_buf)?;
        crate::amf::amf0::write_object_key(&mut amf_buf, "level")?;
        crate::amf::amf0::write_string(&mut amf_buf, "status")?;
        crate::amf::amf0::write_object_key(&mut amf_buf, "code")?;
        crate::amf::amf0::write_string(&mut amf_buf, "NetConnection.Connect.Success")?;
        crate::amf::amf0::write_object_key(&mut amf_buf, "description")?;
        crate::amf::amf0::write_string(&mut amf_buf, "Connection succeeded.")?;
        crate::amf::amf0::write_object_end(&mut amf_buf)?;

        self.send_command(0, amf_buf.as_slice())
    }

    /// Send a createStream response.
    pub fn send_create_stream_response(
        &mut self,
        transaction_id: f64,
        stream_id: u32,
    ) -> Result<()> {
        let mut amf_buf = Buffer::with_capacity(256);
        command::build_create_stream_result(&mut amf_buf, transaction_id, stream_id as f64)?;
        self.send_command(0, amf_buf.as_slice())
    }

    /// Send an onStatus command.
    pub fn send_onstatus(
        &mut self,
        stream_id: u32,
        level: &str,
        code: &str,
        description: &str,
    ) -> Result<()> {
        let mut amf_buf = Buffer::with_capacity(512);
        command::build_onstatus(&mut amf_buf, level, code, description)?;
        self.send_command(stream_id, amf_buf.as_slice())
    }

    /// Flush the send buffer using non-blocking I/O. Partial sends are left
    /// in the buffer for the next call — the server poll loop retries each
    /// iteration so a slow peer cannot stall other connections.
    pub fn flush(&mut self) -> Result<()> {
        if self.client_fd < 0 || self.send_buffer.available() == 0 {
            return Ok(());
        }
        let Some(ref mut transport) = self.transport else {
            return Ok(());
        };
        while self.send_buffer.available() > 0 {
            let pending = self.send_buffer.peek();
            let n = transport.try_send(pending, &mut 0i32)?;
            if n == 0 {
                break;
            }
            self.send_buffer.drain(n);
        }
        Ok(())
    }

    /// Send an audio or video frame to this connection (for player relay).
    pub fn send_frame(
        &mut self,
        frame_type: FrameType,
        timestamp: u32,
        payload: &[u8],
    ) -> Result<()> {
        let stream_id = self
            .current_stream
            .as_ref()
            .map(|s| s.stream_id)
            .unwrap_or(1);

        let mut cmsg = ChunkMessage::default();
        cmsg.timestamp = timestamp;
        cmsg.msg_length = payload.len() as u32;
        cmsg.msg_stream_id = stream_id;
        cmsg.fmt = 0;

        if frame_type == FrameType::Audio {
            cmsg.csid = 4;
            cmsg.msg_type_id = 0x08; // AUDIO
        } else {
            cmsg.csid = 6;
            cmsg.msg_type_id = 0x09; // VIDEO
        }

        chunk_write(
            &mut self.send_buffer,
            &cmsg,
            payload,
            payload.len(),
            self.chunk_size as usize,
        )
    }

    // ── Internal helpers ──

    fn send_control(&mut self, ty: u8, data: &[u8]) -> Result<()> {
        let mut msg = ChunkMessage::default();
        msg.csid = 2;
        msg.fmt = 0;
        msg.msg_length = data.len() as u32;
        msg.msg_type_id = ty;
        msg.msg_stream_id = 0;
        chunk_write(
            &mut self.send_buffer,
            &msg,
            data,
            data.len(),
            self.chunk_size as usize,
        )
    }

    fn send_command(&mut self, msg_stream_id: u32, amf_data: &[u8]) -> Result<()> {
        let mut cmd_msg = ChunkMessage::default();
        cmd_msg.csid = 3;
        cmd_msg.fmt = 0;
        cmd_msg.timestamp = 0;
        cmd_msg.msg_length = amf_data.len() as u32;
        cmd_msg.msg_type_id = 0x14; // AMF0_COMMAND
        cmd_msg.msg_stream_id = msg_stream_id;
        chunk_write(
            &mut self.send_buffer,
            &cmd_msg,
            amf_data,
            amf_data.len(),
            self.chunk_size as usize,
        )
    }

    fn send_acknowledgement(&mut self, seq: u32) -> Result<()> {
        let b = seq.to_be_bytes();
        self.send_control(0x03, &b)
    }
}

impl Default for Conn {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        // The Transport owns and closes the fd (plain: explicit close in Transport::drop;
        // TLS: via SslStream<TcpStream> drop). Only close client_fd directly when
        // no transport was ever assigned (e.g., error before assignment).
        if self.transport.is_none() && self.client_fd >= 0 {
            unsafe {
                libc::close(self.client_fd);
            }
        }
    }
}

// ── Codec detection helpers ──

/// Infer a FourCC-style codec string from the first byte(s) of an FLV video
/// payload. Returns `None` for unrecognised / empty payloads.
fn detect_video_codec(payload: &[u8]) -> Option<String> {
    if payload.is_empty() {
        return None;
    }
    if payload[0] & 0x80 != 0 {
        // E-RTMP v1 extended video tag: FourCC in bytes 1-4.
        if payload.len() >= 5 {
            if let Ok(s) = std::str::from_utf8(&payload[1..5]) {
                return Some(s.to_string());
            }
        }
        return None;
    }
    // Legacy FLV codec ID in lower nibble.
    Some(match payload[0] & 0x0F {
        7 => "avc1".to_string(),
        12 => "hvc1".to_string(),
        13 => "av01".to_string(),
        _ => return None,
    })
}

/// Infer a codec string from the first byte(s) of an FLV audio payload.
fn detect_audio_codec(payload: &[u8]) -> Option<String> {
    if payload.is_empty() {
        return None;
    }
    // E-RTMP v1 extended audio tag: high nibble 0x9 with FourCC in bytes 1-4.
    if (payload[0] & 0xF0) == 0x90 && payload.len() >= 5 {
        if let Ok(s) = std::str::from_utf8(&payload[1..5]) {
            return Some(s.to_string());
        }
    }
    // Legacy audio codec ID in high nibble.
    Some(match (payload[0] >> 4) & 0x0F {
        10 => "mp4a".to_string(),
        2 => "mp3".to_string(),
        14 => "Opus".to_string(),
        _ => return None,
    })
}
