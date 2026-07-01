//! Interop test: ingest a live stream from a real RTMP publisher (ffmpeg).
//!
//! Listens on a TCP port and waits for an external publisher to handshake,
//! publish, and push H.264 video + AAC audio. Every byte of each delivered
//! frame is touched (so an ASan build catches any over-read), and the test
//! succeeds once at least `min_frames` video AND audio frames have arrived.
//! Rust port of the old `tests/interop/test_ffmpeg_ingest.c`.
//!
//! Exit codes: 0 = success, 1 = setup error, 2 = timed out without enough
//! frames, 3 = success criteria met but no multi-chunk (large) frame was seen
//! when one was required.
//!
//! Usage: run_ffmpeg_ingest [bind_addr:port] [timeout_s] [min_frames] [min_large_frame_bytes]

use std::env;
use std::process::ExitCode;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use librtmp2::server::Server;
use librtmp2::types::*;

static VIDEO_FRAMES: AtomicI64 = AtomicI64::new(0);
static AUDIO_FRAMES: AtomicI64 = AtomicI64::new(0);
static TOTAL_BYTES: AtomicUsize = AtomicUsize::new(0);
static MAX_FRAME: AtomicUsize = AtomicUsize::new(0);

fn on_frame(frame: &Frame) {
    if !frame.data.is_null() && frame.size > 0 {
        let payload = unsafe { std::slice::from_raw_parts(frame.data, frame.size as usize) };
        let sum: u64 = payload.iter().map(|&b| b as u64).sum();
        std::hint::black_box(sum);
    }
    TOTAL_BYTES.fetch_add(frame.size as usize, Ordering::SeqCst);
    MAX_FRAME.fetch_max(frame.size as usize, Ordering::SeqCst);
    match frame.frame_type {
        FrameType::Video => VIDEO_FRAMES.fetch_add(1, Ordering::SeqCst),
        FrameType::Audio => AUDIO_FRAMES.fetch_add(1, Ordering::SeqCst),
        _ => 0,
    };
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    let bind_addr = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:11935".to_string());
    let timeout_s: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(25);
    let min_frames: i64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1);
    let min_large: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);

    let config = ServerConfig {
        max_connections: 4,
        chunk_size: 4096,
        tls_enabled: 0,
        tls_cert_file: std::ptr::null(),
        tls_key_file: std::ptr::null(),
        tls_ca_file: std::ptr::null(),
        tls_insecure: 0,
    };

    let Ok(mut server) = Server::new(config) else {
        eprintln!("[interop] server_create failed");
        return ExitCode::from(1);
    };
    server.on_frame_cb = Some(on_frame);
    if server.listen(&bind_addr).is_err() {
        eprintln!("[interop] listen failed on {bind_addr}");
        return ExitCode::from(1);
    }
    println!("[interop] listening on {bind_addr} (timeout {timeout_s}s)");

    let deadline = Instant::now() + Duration::from_secs(timeout_s);
    let mut success = false;
    while Instant::now() < deadline {
        if server.poll(200).is_err() {
            break;
        }
        if VIDEO_FRAMES.load(Ordering::SeqCst) >= min_frames
            && AUDIO_FRAMES.load(Ordering::SeqCst) >= min_frames
        {
            success = true;
            break;
        }
    }

    let video = VIDEO_FRAMES.load(Ordering::SeqCst);
    let audio = AUDIO_FRAMES.load(Ordering::SeqCst);
    let bytes = TOTAL_BYTES.load(Ordering::SeqCst);
    let max_frame = MAX_FRAME.load(Ordering::SeqCst);
    success |= video >= min_frames && audio >= min_frames;
    println!("[interop] video={video} audio={audio} bytes={bytes} max_frame={max_frame}");

    if !success {
        eprintln!(
            "[interop] FAIL: timed out (video={video} audio={audio}, need {min_frames} each)"
        );
        return ExitCode::from(2);
    }
    if min_large > 0 && max_frame < min_large {
        eprintln!(
            "[interop] FAIL: no frame >= {min_large} bytes seen (max was {max_frame}); \
             multi-chunk reassembly not exercised"
        );
        return ExitCode::from(3);
    }
    println!("[interop] PASS: received video and audio from real publisher");
    ExitCode::SUCCESS
}
