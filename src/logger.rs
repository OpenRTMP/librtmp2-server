//! Simple level-filtered logging to stderr or a file.

use parking_lot::Mutex;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Level {
    Error = 0,
    Warn = 1,
    Info = 2,
    Debug = 3,
}

static LEVEL: AtomicU8 = AtomicU8::new(Level::Info as u8);
static FILE: Mutex<Option<LogFile>> = Mutex::new(None);
/// Bumped on every `init()` so a stale background flusher thread from a
/// prior `init()` call knows to exit instead of racing a newer one.
static GENERATION: AtomicU64 = AtomicU64::new(0);

/// Info/Debug lines are buffered and flushed at most this often, so
/// high-frequency access logs don't force a write syscall per line. Error/Warn
/// lines are always flushed immediately since they're rare and operators
/// rely on seeing them right away, including right before a crash.
const FLUSH_INTERVAL: Duration = Duration::from_millis(200);

struct LogFile {
    // `File` has no internal buffer, so wrap it in `BufWriter` — otherwise
    // every `write_all` is its own syscall regardless of how often `flush`
    // is called, defeating the point of throttling flushes.
    file: BufWriter<std::fs::File>,
    last_flush: Instant,
}

/// Open the configured log path for append. On Unix, `O_NOFOLLOW` blocks a
/// local attacker from planting a symlink at `LOG_FILE` to append server log
/// lines into another file (e.g. the SQLite database in the same directory).
#[cfg(unix)]
fn open_log_file(file_path: &str) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    OpenOptions::new()
        .create(true)
        .append(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(file_path)
}

#[cfg(not(unix))]
fn open_log_file(file_path: &str) -> std::io::Result<std::fs::File> {
    OpenOptions::new().create(true).append(true).open(file_path)
}

pub fn init(level: i32, file_path: &str) {
    LEVEL.store(level.clamp(0, 3) as u8, Ordering::Relaxed);
    if !file_path.is_empty() {
        match open_log_file(file_path) {
            Ok(file) => {
                *FILE.lock() = Some(LogFile {
                    file: BufWriter::new(file),
                    last_flush: Instant::now(),
                });
                let my_generation = GENERATION.fetch_add(1, Ordering::SeqCst) + 1;
                std::thread::spawn(move || {
                    loop {
                        std::thread::sleep(FLUSH_INTERVAL);
                        if GENERATION.load(Ordering::SeqCst) != my_generation {
                            return;
                        }
                        let mut guard = FILE.lock();
                        match guard.as_mut() {
                            Some(lf) => {
                                let _ = lf.file.flush();
                            }
                            None => return,
                        }
                    }
                });
            }
            Err(e) => eprintln!("WARN  failed to open log file '{file_path}': {e}"),
        }
    }
}

/// Route librtmp2's protocol-layer log lines (handshake, connect, close
/// reasons) into this logger at the same level filter. Without this the
/// library writes to stderr on its own, bypassing `LOG_FILE`, and its debug
/// lines are never enabled.
pub fn bridge_librtmp2() {
    use librtmp2::log::LogLevel;
    let lib_level = match Level::from_u8(LEVEL.load(Ordering::Relaxed)) {
        Level::Error => LogLevel::Error,
        Level::Warn => LogLevel::Warn,
        Level::Info => LogLevel::Info,
        Level::Debug => LogLevel::Debug,
    };
    librtmp2::log::set_level(lib_level);
    librtmp2::log::set_callback(forward_librtmp2_line, std::ptr::null_mut());
}

fn forward_librtmp2_line(level: librtmp2::log::LogLevel, msg: &str, _userdata: *mut u8) {
    use librtmp2::log::LogLevel;
    let level = match level {
        LogLevel::Error => Level::Error,
        LogLevel::Warn => Level::Warn,
        LogLevel::Info => Level::Info,
        LogLevel::Debug => Level::Debug,
    };
    log(level, &format!("librtmp2: {}", strip_librtmp2_level(msg)));
}

/// librtmp2 formats lines as `[LEVEL] file:line: message`; this logger
/// prints its own level, so drop the library's.
fn strip_librtmp2_level(msg: &str) -> &str {
    msg.strip_prefix('[')
        .and_then(|rest| rest.split_once("] "))
        .map_or(msg, |(_, body)| body)
}

impl Level {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Level::Error,
            1 => Level::Warn,
            2 => Level::Info,
            _ => Level::Debug,
        }
    }
}

pub fn close() {
    let mut guard = FILE.lock();
    if let Some(lf) = guard.as_mut() {
        let _ = lf.file.flush();
    }
    *guard = None;
}

/// Escapes all control characters (newlines, carriage returns, ANSI escape
/// sequences, etc.) so that untrusted input (e.g. the RTMP `app`/stream name
/// from an unauthenticated peer) cannot forge fake log lines or inject
/// terminal escape sequences into a log file tailed/cat'd to a terminal.
fn sanitize_for_log(msg: &str) -> String {
    use std::fmt::Write;

    let mut out = String::with_capacity(msg.len());
    for c in msg.chars() {
        if c.is_control() {
            let _ = write!(out, "\\x{:02x}", c as u32);
        } else {
            out.push(c);
        }
    }
    out
}

fn write_line(level: Level, prefix: &str, msg: &str) {
    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
    let safe_msg = sanitize_for_log(msg);
    let line = format!("[{now}] {prefix} {safe_msg}\n");
    let mut guard = FILE.lock();
    if let Some(lf) = guard.as_mut() {
        let _ = lf.file.write_all(line.as_bytes());
        if level <= Level::Warn || lf.last_flush.elapsed() >= FLUSH_INTERVAL {
            let _ = lf.file.flush();
            lf.last_flush = Instant::now();
        }
    } else {
        drop(guard);
        let _ = std::io::stderr().write_all(line.as_bytes());
        let _ = std::io::stderr().flush();
    }
}

pub fn enabled(level: Level) -> bool {
    (level as u8) <= LEVEL.load(Ordering::Relaxed)
}

pub fn log(level: Level, msg: &str) {
    if !enabled(level) {
        return;
    }
    let prefix = match level {
        Level::Error => "ERROR",
        Level::Warn => "WARN ",
        Level::Info => "INFO ",
        Level::Debug => "DEBUG",
    };
    write_line(level, prefix, msg);
}

#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => {
        if $crate::logger::enabled($crate::logger::Level::Error) {
            $crate::logger::log($crate::logger::Level::Error, &format!($($arg)*));
        }
    };
}
#[macro_export]
macro_rules! log_warn {
    ($($arg:tt)*) => {
        if $crate::logger::enabled($crate::logger::Level::Warn) {
            $crate::logger::log($crate::logger::Level::Warn, &format!($($arg)*));
        }
    };
}
#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => {
        if $crate::logger::enabled($crate::logger::Level::Info) {
            $crate::logger::log($crate::logger::Level::Info, &format!($($arg)*));
        }
    };
}
#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => {
        if $crate::logger::enabled($crate::logger::Level::Debug) {
            $crate::logger::log($crate::logger::Level::Debug, &format!($($arg)*));
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn librtmp2_level_prefix_is_stripped() {
        assert_eq!(
            strip_librtmp2_level("[WARN] conn.rs:12: RTMP conn 1 [1.2.3.4:5]: closed"),
            "conn.rs:12: RTMP conn 1 [1.2.3.4:5]: closed"
        );
        assert_eq!(strip_librtmp2_level("no prefix"), "no prefix");
    }

    #[test]
    fn sanitize_escapes_newlines_and_carriage_returns() {
        assert_eq!(sanitize_for_log("a\nb\rc"), "a\\x0ab\\x0dc");
    }

    #[test]
    fn sanitize_escapes_ansi_escape_sequences() {
        // A crafted `app` name could otherwise inject terminal control
        // sequences (e.g. to hide/rewrite prior log output) into a log file
        // that an operator tails with a real terminal.
        let malicious = "app\x1b[31mFAKE ERROR\x1b[0m";
        let safe = sanitize_for_log(malicious);
        assert!(!safe.contains('\x1b'));
        assert!(safe.contains("\\x1b"));
    }

    #[test]
    fn sanitize_leaves_normal_text_and_utf8_untouched() {
        let msg = "stream 'café' app='live'";
        assert_eq!(sanitize_for_log(msg), msg);
    }

    #[cfg(unix)]
    #[test]
    fn init_rejects_symlink_log_file() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real.log");
        let link = dir.path().join("link.log");
        std::fs::write(&real, b"seed\n").unwrap();
        std::os::unix::fs::symlink(&real, &link).unwrap();

        init(3, link.to_str().unwrap());
        assert!(
            FILE.lock().is_none(),
            "symlinked LOG_FILE must not be opened for append"
        );
    }
}
