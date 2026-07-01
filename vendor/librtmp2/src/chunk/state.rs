//! Chunk stream state/registry
//!
//! Mirrors `src/chunk/chunk_state.h` and `src/chunk/chunk_state.c`.

use crate::buffer::Buffer;
use crate::types::ErrorCode;
use crate::types::Result;

/// Default chunk size per RTMP spec
pub const DEFAULT_CHUNK_SIZE: u32 = 128;
/// Upper bound on distinct chunk streams
pub const MAX_CHUNK_STREAMS: usize = 4096;
/// Max reassembly bytes per connection
pub const MAX_REASSEMBLY_BYTES_PER_CONN: usize = 32 * 1024 * 1024;

/// Per-chunk-stream state.
#[derive(Debug)]
pub struct ChunkStream {
    pub csid: u32,
    /// peer's chunk size (SetChunkSize)
    pub chunk_size: u32,
    /// running timestamp for this chunk stream
    pub type0_timestamp: u32,
    pub type0_msg_length: u32,
    pub type0_msg_type_id: u8,
    pub type0_msg_stream_id: u32,
    /// current message uses extended timestamps
    pub type0_ext_ts: bool,
    /// bytes read so far for current message
    pub reassembly_bytes_read: u32,
    /// buffer for reassembling partial messages
    pub reassembly_buf: Buffer,
    pub in_use: bool,
}

impl Default for ChunkStream {
    fn default() -> Self {
        Self {
            csid: 0,
            chunk_size: DEFAULT_CHUNK_SIZE,
            type0_timestamp: 0,
            type0_msg_length: 0,
            type0_msg_type_id: 0,
            type0_msg_stream_id: 0,
            type0_ext_ts: false,
            reassembly_bytes_read: 0,
            reassembly_buf: Buffer::new(),
            in_use: false,
        }
    }
}

impl ChunkStream {
    /// Reset stream state, keeping the reassembly buffer allocated.
    pub fn reset(&mut self, default_chunk_size: u32) {
        self.type0_timestamp = 0;
        self.type0_msg_length = 0;
        self.type0_msg_type_id = 0;
        self.type0_msg_stream_id = 0;
        self.type0_ext_ts = false;
        self.reassembly_bytes_read = 0;
        self.reassembly_buf.reset();
        self.chunk_size = default_chunk_size;
    }
}

/// Per-connection chunk-stream registry.
#[derive(Debug)]
pub struct ChunkRegistry {
    pub streams: Vec<ChunkStream>,
    /// Chunk size applied to new streams
    pub default_chunk_size: u32,
    pub initialized: bool,
}

impl Default for ChunkRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkRegistry {
    /// Create a new chunk registry.
    pub fn new() -> Self {
        Self {
            streams: Vec::new(),
            default_chunk_size: DEFAULT_CHUNK_SIZE,
            initialized: true,
        }
    }

    /// Initialize the registry.
    pub fn init(&mut self) {
        self.initialized = true;
        self.default_chunk_size = DEFAULT_CHUNK_SIZE;
    }

    /// Get or create a chunk stream for the given csid.
    pub fn get_or_create(&mut self, csid: u32) -> Result<&mut ChunkStream> {
        // Check if this csid is already open.
        let idx = self.streams.iter().position(|s| s.csid == csid && s.in_use);
        if let Some(i) = idx {
            return Ok(&mut self.streams[i]);
        }

        // Reuse a free slot before growing the vec; this prevents the stream
        // count from monotonically climbing to MAX_CHUNK_STREAMS on connections
        // that open and close many streams across their lifetime.
        if let Some(i) = self.streams.iter().position(|s| !s.in_use) {
            self.streams[i].csid = csid;
            self.streams[i].in_use = true;
            self.streams[i].reset(self.default_chunk_size);
            return Ok(&mut self.streams[i]);
        }

        if self.streams.len() >= MAX_CHUNK_STREAMS {
            return Err(ErrorCode::Chunk);
        }

        let mut stream = ChunkStream::default();
        stream.csid = csid;
        stream.in_use = true;
        stream.chunk_size = self.default_chunk_size;
        self.streams.push(stream);
        let last = self.streams.len() - 1;
        Ok(&mut self.streams[last])
    }

    /// Get a chunk stream by csid (returns None if not found).
    pub fn get(&self, csid: u32) -> Option<&ChunkStream> {
        self.streams.iter().find(|s| s.csid == csid && s.in_use)
    }

    /// Get a mutable chunk stream by csid.
    pub fn get_mut(&mut self, csid: u32) -> Option<&mut ChunkStream> {
        self.streams.iter_mut().find(|s| s.csid == csid && s.in_use)
    }

    /// Set chunk size for all streams.
    pub fn set_all_chunk_size(&mut self, chunk_size: u32) {
        self.default_chunk_size = chunk_size;
        for stream in &mut self.streams {
            if stream.in_use {
                stream.chunk_size = chunk_size;
            }
        }
    }

    /// Check if a stream can grow its reassembly buffer.
    pub fn can_grow_reassembly(&self, cs: &ChunkStream, additional: u32) -> Result<()> {
        let total: usize = self
            .streams
            .iter()
            .filter(|s| s.in_use)
            .map(|s| s.reassembly_buf.available())
            .sum();
        if total + additional as usize > MAX_REASSEMBLY_BYTES_PER_CONN {
            return Err(ErrorCode::Chunk);
        }
        Ok(())
    }

    /// Reset a specific stream.
    pub fn reset_stream(&mut self, csid: u32) {
        if let Some(stream) = self.streams.iter_mut().find(|s| s.csid == csid && s.in_use) {
            stream.reset(self.default_chunk_size);
        }
    }

    /// Destroy the registry.
    pub fn destroy(&mut self) {
        self.streams.clear();
        self.initialized = false;
    }
}
