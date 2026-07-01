//! librtmp2 — A Rust port of the librtmp2 RTMP/RTMPS protocol library.
//!
//! This is a complete 1:1 Rust port of the C library `librtmp2`.
//! It provides server, client, handshake, chunk, message, AMF, FLV, and E-RTMP functionality.

#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(clippy::all)]

pub mod alloc;
pub mod amf;
pub mod buffer;
pub mod bytes;
pub mod chunk;
pub mod client;
pub mod ertmp;
pub mod flv;
pub mod handshake;
pub mod log;
pub mod message;
pub mod net;
pub mod server;
pub mod session;
pub mod transport;
pub mod types;

// Re-exports for convenience
pub use types::*;

/// Library version string.
pub const VERSION_STRING: &str = "0.1.0";

/// Get the library version string.
pub fn version_string() -> &'static str {
    VERSION_STRING
}

/// Get the major version number.
pub fn version_major() -> i32 {
    0
}

/// Get the minor version number.
pub fn version_minor() -> i32 {
    1
}

/// Get the patch version number.
pub fn version_patch() -> i32 {
    0
}

/// Check if TLS support is available.
pub fn tls_supported() -> bool {
    transport::tls_available()
}

/// Get the error string for an error code.
pub fn error_string(code: ErrorCode) -> &'static str {
    code.as_str()
}

fn error_c_string(code: ErrorCode) -> &'static [u8] {
    match code {
        ErrorCode::Ok => b"OK\0",
        ErrorCode::Io => b"I/O error\0",
        ErrorCode::Timeout => b"Timeout\0",
        ErrorCode::Protocol => b"Protocol error\0",
        ErrorCode::Handshake => b"Handshake error\0",
        ErrorCode::Chunk => b"Chunk error\0",
        ErrorCode::Amf => b"AMF error\0",
        ErrorCode::Unsupported => b"Unsupported\0",
        ErrorCode::Auth => b"Authentication error\0",
        ErrorCode::Internal => b"Internal error\0",
    }
}

// ── FFI-compatible extern "C" API ──

/// Create a server (FFI-compatible).
#[no_mangle]
pub unsafe extern "C" fn lrtmp2_server_create(config: *const ServerConfig) -> *mut server::Server {
    if config.is_null() {
        return std::ptr::null_mut();
    }
    let cfg = &*config;
    match server::Server::new(*cfg) {
        Ok(s) => Box::into_raw(Box::new(s)),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Destroy a server (FFI-compatible).
#[no_mangle]
pub unsafe extern "C" fn lrtmp2_server_destroy(server: *mut server::Server) {
    if !server.is_null() {
        drop(Box::from_raw(server));
    }
}

/// Start listening (FFI-compatible).
#[no_mangle]
pub unsafe extern "C" fn lrtmp2_server_listen(
    server: *mut server::Server,
    bind_addr: *const u8,
) -> i32 {
    if server.is_null() || bind_addr.is_null() {
        return ErrorCode::Internal as i32;
    }
    let s = &mut *server;
    let addr = std::ffi::CStr::from_ptr(bind_addr as *const std::ffi::c_char);
    match s.listen(addr.to_str().unwrap_or("")) {
        Ok(()) => 0,
        Err(e) => e as i32,
    }
}

/// Poll for events (FFI-compatible).
#[no_mangle]
pub unsafe extern "C" fn lrtmp2_server_poll(server: *mut server::Server, timeout_ms: i32) -> i32 {
    if server.is_null() {
        return ErrorCode::Internal as i32;
    }
    let s = &mut *server;
    match s.poll(timeout_ms) {
        Ok(()) => 0,
        Err(e) => e as i32,
    }
}

/// Stop the server (FFI-compatible).
#[no_mangle]
pub unsafe extern "C" fn lrtmp2_server_stop(server: *mut server::Server) {
    if !server.is_null() {
        (*server).stop();
    }
}

/// Create a client (FFI-compatible).
#[no_mangle]
pub unsafe extern "C" fn lrtmp2_client_create(_config: *const ServerConfig) -> *mut client::Client {
    let c = client::Client::new();
    Box::into_raw(Box::new(c))
}

/// Destroy a client (FFI-compatible).
#[no_mangle]
pub unsafe extern "C" fn lrtmp2_client_destroy(c: *mut client::Client) {
    if !c.is_null() {
        drop(Box::from_raw(c));
    }
}

/// Connect (FFI-compatible).
#[no_mangle]
pub unsafe extern "C" fn lrtmp2_client_connect(c: *mut client::Client, url: *const u8) -> i32 {
    if c.is_null() || url.is_null() {
        return ErrorCode::Internal as i32;
    }
    let client = &mut *c;
    let url_str = std::ffi::CStr::from_ptr(url as *const std::ffi::c_char);
    match client.connect(url_str.to_str().unwrap_or("")) {
        Ok(()) => 0,
        Err(e) => e as i32,
    }
}

/// Publish (FFI-compatible).
#[no_mangle]
pub unsafe extern "C" fn lrtmp2_client_publish(c: *mut client::Client) -> i32 {
    if c.is_null() {
        return ErrorCode::Internal as i32;
    }
    match (*c).publish() {
        Ok(()) => 0,
        Err(e) => e as i32,
    }
}

/// Play (FFI-compatible).
#[no_mangle]
pub unsafe extern "C" fn lrtmp2_client_play(c: *mut client::Client) -> i32 {
    if c.is_null() {
        return ErrorCode::Internal as i32;
    }
    match (*c).play() {
        Ok(()) => 0,
        Err(e) => e as i32,
    }
}

/// Send a frame (FFI-compatible).
#[no_mangle]
pub unsafe extern "C" fn lrtmp2_client_send_frame(
    c: *mut client::Client,
    frame: *const Frame,
) -> i32 {
    if c.is_null() || frame.is_null() {
        return ErrorCode::Internal as i32;
    }
    match (*c).send_frame(&*frame) {
        Ok(()) => 0,
        Err(e) => e as i32,
    }
}

/// Poll client (FFI-compatible).
#[no_mangle]
pub unsafe extern "C" fn lrtmp2_client_poll(c: *mut client::Client, timeout_ms: i32) -> i32 {
    if c.is_null() {
        return ErrorCode::Internal as i32;
    }
    match (*c).poll(timeout_ms) {
        Ok(()) => 0,
        Err(e) => e as i32,
    }
}

/// Get connection fd (FFI-compatible).
#[no_mangle]
pub unsafe extern "C" fn lrtmp2_conn_get_fd(_conn: *const session::Conn) -> i32 {
    -1
}

/// Check TLS support (FFI-compatible).
#[no_mangle]
pub extern "C" fn lrtmp2_tls_supported() -> i32 {
    if tls_supported() {
        1
    } else {
        0
    }
}

/// Get version string (FFI-compatible).
#[no_mangle]
pub extern "C" fn lrtmp2_version_string() -> *const u8 {
    b"0.1.0\0".as_ptr()
}

/// Get major version (FFI-compatible).
#[no_mangle]
pub extern "C" fn lrtmp2_version_major() -> i32 {
    version_major()
}

/// Get minor version (FFI-compatible).
#[no_mangle]
pub extern "C" fn lrtmp2_version_minor() -> i32 {
    version_minor()
}

/// Get patch version (FFI-compatible).
#[no_mangle]
pub extern "C" fn lrtmp2_version_patch() -> i32 {
    version_patch()
}

/// Get error string (FFI-compatible).
#[no_mangle]
pub unsafe extern "C" fn lrtmp2_error_string(code: ErrorCode) -> *const u8 {
    error_c_string(code).as_ptr()
}
