//! Simple level-filtered logging to stderr or a file.

use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Level {
    Error = 0,
    Warn = 1,
    Info = 2,
    Debug = 3,
}

static LEVEL: AtomicU8 = AtomicU8::new(Level::Info as u8);
static FILE: Mutex<Option<File>> = Mutex::new(None);

pub fn init(level: i32, file_path: &str) {
    LEVEL.store(level.clamp(0, 3) as u8, Ordering::Relaxed);
    if !file_path.is_empty() {
        match OpenOptions::new().create(true).append(true).open(file_path) {
            Ok(f) => *FILE.lock() = Some(f),
            Err(e) => eprintln!("WARN  failed to open log file '{file_path}': {e}"),
        }
    }
}

pub fn close() {
    *FILE.lock() = None;
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

fn write_line(prefix: &str, msg: &str) {
    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
    let safe_msg = sanitize_for_log(msg);
    let line = format!("[{now}] {prefix} {safe_msg}\n");
    let mut guard = FILE.lock();
    if let Some(f) = guard.as_mut() {
        let _ = f.write_all(line.as_bytes());
        let _ = f.flush();
    } else {
        drop(guard);
        eprint!("{line}");
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
    write_line(prefix, msg);
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
}
