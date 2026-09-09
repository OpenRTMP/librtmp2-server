from pathlib import Path

path = Path("src/media_output.rs")
text = path.read_text()
old = '''#[cfg(not(unix))]
fn terminate_hook_child(child: &mut Child) {
    terminate_child(child);
}
'''
new = '''#[cfg(windows)]
fn terminate_hook_child(child: &mut Child) {
    if child.try_wait().ok().flatten().is_none() {
        let pid = child.id().to_string();
        let killed_tree = Command::new("taskkill")
            .args(["/PID", &pid, "/T", "/F"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success());
        if !killed_tree {
            let _ = child.kill();
        }
    }
    let _ = child.wait();
}

#[cfg(not(any(unix, windows)))]
fn terminate_hook_child(child: &mut Child) {
    terminate_child(child);
}
'''
if text.count(old) != 1:
    raise SystemExit(f"expected one Windows hook fallback, found {text.count(old)}")
path.write_text(text.replace(old, new, 1))
