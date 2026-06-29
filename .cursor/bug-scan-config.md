# Bug Scan Configuration

## SCAN_TARGET_OVERRIDE

Leave empty to follow the rotating module schedule in `bug-scan-progress.md`.
Set to a module name (e.g. `db`, `http`, `rtmp_callbacks`) to force-scan that module.

```
SCAN_TARGET_OVERRIDE=
```

## Modules

Scan order matches `bug-scan-progress.md`. Each module includes its source file(s) and related headers/tests.
