//! End-to-end smoke test for the real TCP socket glue: a `Server` listens on
//! loopback, a `Client` connects, performs the RTMP handshake + connect +
//! createStream + publish exchange over real sockets, and sends one video
//! frame that the server's `on_frame_cb` should observe.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use librtmp2::client::Client;
use librtmp2::server::Server;
use librtmp2::types::*;

static FRAMES_RECEIVED: AtomicUsize = AtomicUsize::new(0);

const SENT_FRAME_BYTE: u8 = 0xAB;
const SENT_FRAME_LEN: usize = 32;

fn on_frame(frame: &Frame) {
    if frame.size as usize == SENT_FRAME_LEN && !frame.data.is_null() {
        let payload = unsafe { std::slice::from_raw_parts(frame.data, frame.size as usize) };
        if payload.iter().all(|&b| b == SENT_FRAME_BYTE) {
            FRAMES_RECEIVED.fetch_add(1, Ordering::SeqCst);
        }
    }
}

fn plain_config() -> ServerConfig {
    ServerConfig {
        max_connections: 8,
        chunk_size: 128,
        tls_enabled: 0,
        tls_cert_file: std::ptr::null(),
        tls_key_file: std::ptr::null(),
        tls_ca_file: std::ptr::null(),
        tls_insecure: 0,
    }
}

#[test]
fn server_client_publish_over_real_sockets() {
    let mut server = Server::new(plain_config()).unwrap();
    server.listen("127.0.0.1:19661").unwrap();
    server.on_frame_cb = Some(on_frame);

    let (setup_tx, setup_rx) = std::sync::mpsc::channel();
    let client_thread = thread::spawn(move || {
        let mut client = Client::new();
        let result = (|| -> std::result::Result<(), librtmp2::types::ErrorCode> {
            client.connect("rtmp://127.0.0.1:19661/live/stream1")?;
            client.publish()?;

            let data = [SENT_FRAME_BYTE; SENT_FRAME_LEN];
            let frame = Frame {
                frame_type: FrameType::Video,
                timestamp: 0,
                composition_time: 0,
                size: data.len() as u32,
                data: data.as_ptr(),
                audio_codec: AudioCodec::default(),
                audio_sample_rate: 0,
                audio_channels: 0,
                audio_bit_depth: 0,
                audio_fourcc: FourCc::default(),
                video_codec: VideoCodec::H264,
                video_fourcc: FourCc::default(),
                video_frame_type: 1,
                is_metadata: 0,
            };
            client.send_frame(&frame)?;
            Ok(())
        })();
        let _ = setup_tx.send(result.is_ok());
        result.unwrap();
        thread::sleep(Duration::from_millis(200));
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(setup_ok) = setup_rx.try_recv() {
            assert!(setup_ok, "client setup failed");
        }
        if FRAMES_RECEIVED.load(Ordering::SeqCst) > 0 || Instant::now() >= deadline {
            break;
        }
        server.poll(20).unwrap();
    }

    client_thread.join().unwrap();
    assert!(
        FRAMES_RECEIVED.load(Ordering::SeqCst) > 0,
        "server never observed the published frame"
    );
}
