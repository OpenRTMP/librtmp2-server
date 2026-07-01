//! Interop test: play (pull) a live stream from a real RTMP server
//! (mediamtx, fed by ffmpeg).
//!
//! Connects as an RTMP client, issues play, and pumps incoming messages.
//! Every byte of each delivered frame is touched (so an ASan build catches
//! any over-read). Succeeds once at least one video AND one audio frame
//! arrive. Rust port of the old `tests/interop/test_play_pull.c`.
//!
//! Exit codes: 0 = success, 1 = setup/connect error, 2 = timed out without
//! both a video and an audio frame.
//!
//! Usage: run_play_pull rtmp://host:port/app/stream [timeout_seconds]

use std::env;
use std::process::ExitCode;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use librtmp2::client::Client;
use librtmp2::types::*;

static VIDEO_FRAMES: AtomicI64 = AtomicI64::new(0);
static AUDIO_FRAMES: AtomicI64 = AtomicI64::new(0);
static TOTAL_BYTES: AtomicUsize = AtomicUsize::new(0);

fn on_frame(frame: &Frame) {
    if !frame.data.is_null() && frame.size > 0 {
        let payload = unsafe { std::slice::from_raw_parts(frame.data, frame.size as usize) };
        let sum: u64 = payload.iter().map(|&b| b as u64).sum();
        std::hint::black_box(sum);
    }
    TOTAL_BYTES.fetch_add(frame.size as usize, Ordering::SeqCst);
    match frame.frame_type {
        FrameType::Video => VIDEO_FRAMES.fetch_add(1, Ordering::SeqCst),
        FrameType::Audio => AUDIO_FRAMES.fetch_add(1, Ordering::SeqCst),
        _ => 0,
    };
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    let url = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "rtmp://127.0.0.1:1935/live/test".to_string());
    let timeout_s: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(25);

    let mut client = Client::new();
    client.on_frame_cb = Some(on_frame);

    println!("[interop-play] connecting to {url}");
    if client.connect(&url).is_err() {
        eprintln!("[interop-play] connect failed");
        return ExitCode::from(1);
    }
    if client.play().is_err() {
        eprintln!("[interop-play] play failed");
        return ExitCode::from(1);
    }
    println!("[interop-play] play started, pumping frames");

    let deadline = Instant::now() + Duration::from_secs(timeout_s);
    let mut success = false;
    while Instant::now() < deadline {
        if let Err(e) = client.poll(200) {
            eprintln!("[interop-play] poll error {e:?}");
            break;
        }
        if VIDEO_FRAMES.load(Ordering::SeqCst) > 0 && AUDIO_FRAMES.load(Ordering::SeqCst) > 0 {
            success = true;
            break;
        }
    }

    let video = VIDEO_FRAMES.load(Ordering::SeqCst);
    let audio = AUDIO_FRAMES.load(Ordering::SeqCst);
    let bytes = TOTAL_BYTES.load(Ordering::SeqCst);
    success |= video > 0 && audio > 0;
    println!("[interop-play] video={video} audio={audio} bytes={bytes}");

    if success {
        println!("[interop-play] PASS: pulled video and audio from real RTMP server");
        return ExitCode::SUCCESS;
    }
    eprintln!("[interop-play] FAIL: timed out (video={video} audio={audio})");
    ExitCode::from(2)
}
