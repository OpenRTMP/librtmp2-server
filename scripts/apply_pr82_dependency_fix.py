from pathlib import Path

REV = "ad021f97631cd2033abaee43e711bc406ca10c17"
SHORT = REV[:7]


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one match, found {count}: {old!r}")
    file.write_text(text.replace(old, new, 1))


replace_once(
    "README.md",
    "`librtmp2` 0.4.0 is not on crates.io yet. Until it is published, this repo pulls it from the git branch `feat/ertmp-v2-multitrack-0.4.0` (see `Cargo.toml`). After merge and crates.io release, change the dependency to a registry-only line:",
    f"`librtmp2` 0.4.0 is not on crates.io yet. Until it is published, this repo pins the validated commit `{SHORT}` from librtmp2 PR #128 (see `Cargo.toml`) so builds remain reproducible. After merge and crates.io release, change the dependency to a registry-only line:",
)

replace_once(
    "AGENTS.md",
    "- **Dependency source:** `librtmp2 = \"0.4.0\"` is resolved from git branch `feat/ertmp-v2-multitrack-0.4.0` until the crate is on crates.io. After merge + publish, switch `Cargo.toml` to a registry-only pin (`librtmp2 = { version = \"0.4.0\", features = [\"tls\"] }`) and refresh `Cargo.lock`. It does **not** use the sibling `../librtmp2` checkout unless you add a `[patch]` override locally.",
    f"- **Dependency source:** `librtmp2 = \"0.4.0\"` is pinned to validated git commit `{SHORT}` until the crate is on crates.io. After merge + publish, switch `Cargo.toml` to a registry-only pin (`librtmp2 = {{ version = \"0.4.0\", features = [\"tls\"] }}`) and refresh `Cargo.lock`. It does **not** use the sibling `../librtmp2` checkout unless you add a `[patch]` override locally.",
)

replace_once(
    "CHANGELOG.md",
    "### Changed\n- Track `librtmp2` 0.4.0 from git branch `feat/ertmp-v2-multitrack-0.4.0` instead of\n  a fixed commit rev (follows ongoing librtmp2 PR work until release).",
    f"### Changed\n- Pin `librtmp2` 0.4.0 to validated commit `{SHORT}` from librtmp2 PR #128 instead of\n  following the moving feature branch, keeping server builds reproducible until release.",
)

replace_once(
    "CHANGELOG.md",
    "  (`OpenRTMP/librtmp2` @ `d064938`); switch back to a crates.io version pin after\n  release.",
    f"  (`OpenRTMP/librtmp2` @ `{SHORT}`); switch back to a crates.io version pin after\n  release.",
)

print(f"Updated documentation for librtmp2 {REV}")
