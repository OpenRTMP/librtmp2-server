//! Simple level-filtered logging to stderr or a file.

use std::fs::{File, OpenOptions};
use std::io::Write;
use parking_lot::Mutex;
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

fn write_line(prefix: &str, msg: &str) {
    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
    let safe_msg = msg.replace('\r', "\\r").replace('\n', "\\n");
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
