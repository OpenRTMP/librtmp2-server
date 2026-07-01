//! Minimal RTMP server: accepts connections, ack publish/play, and logs
//! every audio/video frame it receives. Rust port of the old
//! `examples/minimal_server/minimal_server.c`.

use std::env;
use std::sync::atomic::{AtomicBool, Ordering};

use librtmp2::server::Server;
use librtmp2::types::*;

static RUNNING: AtomicBool = AtomicBool::new(true);

fn on_frame(frame: &Frame) {
    let kind = match frame.frame_type {
        FrameType::Audio => "audio",
        FrameType::Video => "video",
        _ => "other",
    };
    println!(
        "frame: type={kind} timestamp={} size={}",
        frame.timestamp, frame.size
    );
}

extern "C" fn handle_sigint(_sig: i32) {
    RUNNING.store(false, Ordering::SeqCst);
}

fn main() {
    let bind_addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:1935".to_string());

    let config = ServerConfig {
        max_connections: 16,
        chunk_size: 128,
        tls_enabled: 0,
        tls_cert_file: std::ptr::null(),
        tls_key_file: std::ptr::null(),
        tls_ca_file: std::ptr::null(),
        tls_insecure: 0,
    };

    let mut server = Server::new(config).expect("failed to create server");
    server.on_frame_cb = Some(on_frame);
    server.listen(&bind_addr).expect("failed to listen");
    println!("listening on {bind_addr}");

    unsafe {
        libc::signal(
            libc::SIGINT,
            handle_sigint as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGTERM,
            handle_sigint as *const () as libc::sighandler_t,
        );
    }

    while RUNNING.load(Ordering::SeqCst) {
        if let Err(e) = server.poll(100) {
            eprintln!("poll error: {e:?}");
            break;
        }
    }

    server.stop();
    println!("server stopped");
}
