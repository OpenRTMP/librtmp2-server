# Operations: SQLite data and API token

`librtmp2-server` keeps its persistent state in one SQLite database. The path
must be supplied through `LRTMP2_DB` or `LRTMP2_DB_PATH`; the Docker image uses
`/data/server.db`.

This guide uses offline backups and restores: stop the server before copying
the database and keep it stopped until the copy or restore is complete. The
server enables SQLite WAL mode, so a running database may also have
`server.db-wal` and `server.db-shm` files. Copying only `server.db` while the
server is running is not a consistent backup.

## What the database contains

The database contains:

- the Bearer API token in the `settings` table;
- stream definitions, including publish, play, and stats keys;
- per-viewer play keys;
- publisher/player session rows and collected stats samples.

Listener settings, TLS certificates and keys, logs, and the server `.env` file
are not stored in SQLite. Back them up separately when they are part of the
deployment.

## Support status

| Operation | Status |
| --- | --- |
| Stop the server, copy the complete database directory, then restart | Supported |
| Restore a stopped server from a complete offline backup | Supported |
| Move a stopped Docker volume or bind-mounted data directory | Supported |
| Use `LRTMP2_API_TOKEN` to seed an empty database on first startup | Supported |
| Change `LRTMP2_API_TOKEN` after a token is already stored | Not a rotation mechanism; the stored database token wins |
| Replace the token directly in SQLite while the server is stopped | Manual; no supported rotation CLI or API exists yet |
| Copy only `server.db` while the server is running | Unsupported |

## Native backup

First identify the exact database path used by the running service. In the
examples below, replace `/var/lib/librtmp2-server/server.db` with that path.

1. Stop the server with the same supervisor used to start it:

   ```bash
   sudo systemctl stop <service-name>
   ```

2. Create a private backup directory and copy the database plus any WAL
   sidecars:

   ```bash
   DB=/var/lib/librtmp2-server/server.db
   BACKUP_DIR="$HOME/librtmp2-backup-$(date -u +%Y%m%dT%H%M%SZ)"
   umask 077
   mkdir -p "$BACKUP_DIR"
   sudo cp -a -- "$DB" "$BACKUP_DIR/server.db"
   for suffix in -wal -shm; do
     if [ -e "${DB}${suffix}" ]; then
       sudo cp -a -- "${DB}${suffix}" "$BACKUP_DIR/server.db${suffix}"
     fi
   done
   ```

3. Start the server and verify it:

   ```bash
   sudo systemctl start <service-name>
   curl --fail --silent --show-error http://127.0.0.1:8080/api/v1/health
   ```

Store the backup as sensitive data: it contains the API token and every stream
key.

## Native restore

Restore into a new path first. This keeps the current database available for a
quick rollback.

1. Stop the server and make a fresh pre-restore backup using the procedure
   above.
2. Copy the selected backup into a new private directory:

   ```bash
   DB=/var/lib/librtmp2-server/server.db
   BACKUP_DIR=/path/to/selected/backup
   RESTORE_DIR=/var/lib/librtmp2-server-restored
   umask 077
   sudo install -d -m 700 "$RESTORE_DIR"
   sudo find "$BACKUP_DIR" -maxdepth 1 -name 'server.db*' \
     -exec cp -a -t "$RESTORE_DIR" -- {} +
   sudo chown --reference="$(dirname "$DB")" "$RESTORE_DIR"
   sudo find "$RESTORE_DIR" -maxdepth 1 -name 'server.db*' \
     -exec chown --reference="$DB" -- {} + \
     -exec chmod 600 -- {} +
   sudo test -f "$RESTORE_DIR/server.db" || {
     echo "Restore failed: $RESTORE_DIR/server.db not found" >&2
     exit 1
   }
   ```

   `BACKUP_DIR` must point at the backup you selected to restore, not
   necessarily the fresh pre-restore backup made in the previous step. The
   `test -f` check guards against pointing the server at an empty
   `RESTORE_DIR`, which would otherwise make it silently bootstrap a brand
   new, empty database and token.

3. Stop the server again (the backup procedure's step 3 started it). Point
   `LRTMP2_DB` at `/var/lib/librtmp2-server-restored/server.db`. The server
   reads `LRTMP2_DB` before `LRTMP2_DB_PATH`, so if the service environment
   already sets `LRTMP2_DB_PATH`, either update `LRTMP2_DB` to override it or
   unset/align `LRTMP2_DB_PATH` too — otherwise the service keeps using the
   old database. Then start the server.
4. Run the verification checks below. Keep the old directory unchanged until
   verification succeeds.

To roll back, stop the server, restore the previous database path in the
service environment, and start it again.

## Docker named-volume backup

The quickstart container is named `librtmp2-server` and mounts a named volume
at `/data`. Confirm the actual volume name before continuing:

```bash
docker inspect librtmp2-server \
  --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}'
```

Set `DATA_VOLUME` to that output and `IMAGE` to the exact image tag or digest
used by the container:

```bash
DATA_VOLUME=librtmp2-server-data
IMAGE=<same-image-tag-or-digest-as-the-server>
BACKUP_DIR="$PWD/backups"
ARCHIVE="librtmp2-data-$(date -u +%Y%m%dT%H%M%SZ).tar.gz"
umask 077
mkdir -p "$BACKUP_DIR"

docker stop librtmp2-server
docker run --rm --user 0 --entrypoint sh \
  -v "$DATA_VOLUME":/data:ro \
  -v "$BACKUP_DIR":/backup \
  "$IMAGE" \
  -c "set -eu; tar -czf /backup/$ARCHIVE -C /data ."
test -s "$BACKUP_DIR/$ARCHIVE" || {
  echo "Backup failed: $BACKUP_DIR/$ARCHIVE is missing or empty" >&2
  exit 1
}
docker start librtmp2-server
```

Each run writes a distinct, timestamped archive so re-running the backup never
overwrites a prior recovery point. This archives the whole volume, including
any WAL sidecars.

## Docker named-volume restore or migration

Restore into a new named volume instead of overwriting the existing one. Set
`ARCHIVE` to the filename of the backup you selected to restore, not
necessarily the most recent one:

```bash
RESTORE_VOLUME=librtmp2-server-data-restored
ARCHIVE=librtmp2-data-<selected-backup-timestamp>.tar.gz
docker volume create "$RESTORE_VOLUME"

docker run --rm --user 0 --entrypoint sh \
  -v "$RESTORE_VOLUME":/data \
  -v "$BACKUP_DIR":/backup:ro \
  "$IMAGE" \
  -c "set -eu
      tar -xzf /backup/$ARCHIVE -C /data
      chown -R openrtmp:openrtmp /data
      chmod 700 /data
      for f in /data/server.db*; do
        [ -e \"\$f\" ] && chmod 600 \"\$f\"
      done
      test -f /data/server.db"
```

Stop the old container, but keep it (do not remove it) so it stays available
for rollback. Create the replacement under a temporary name — for example
`librtmp2-server-restored` — with the same ports and environment but mounting
`"$RESTORE_VOLUME":/data`. For Compose, change the volume mapping to the
restored external volume and set `container_name` to the temporary name before
running `docker compose up -d`.

For migration to another host, copy `librtmp2-data.tar.gz` over a secure
channel, create a new volume there, and run the same restore command. Do not
start both copies against a shared database or expose both with the same stream
keys.

Keep the old stopped container and volume until the restored deployment passes
the verification checks below. Only then remove the old container and rename
the replacement to `librtmp2-server` (or update the Compose `container_name`
and re-apply). Rollback before that point is starting the old container again;
rollback after that point is switching the replacement container back to the
old volume.

## Docker bind mounts

For a bind mount such as `-v /srv/librtmp2:/data`, stop the container and
archive the complete host directory:

```bash
DATA_DIR=/srv/librtmp2
BACKUP_DIR="$PWD/backups"
ARCHIVE="librtmp2-data-$(date -u +%Y%m%dT%H%M%SZ).tar.gz"
umask 077
mkdir -p "$BACKUP_DIR"

docker stop librtmp2-server
sudo tar -czf "$BACKUP_DIR/$ARCHIVE" -C "$DATA_DIR" .
test -s "$BACKUP_DIR/$ARCHIVE" || {
  echo "Backup failed: $BACKUP_DIR/$ARCHIVE is missing or empty" >&2
  exit 1
}
docker start librtmp2-server
```

Each run writes a distinct, timestamped archive so re-running the backup never
overwrites a prior recovery point.

Restore into a new host directory, preserve restrictive permissions, and mount
that directory at `/data` in the replacement container. Set `ARCHIVE` to the
filename of the backup you selected to restore, not necessarily the most
recent one:

```bash
RESTORE_DIR=/srv/librtmp2-restored
ARCHIVE=librtmp2-data-<selected-backup-timestamp>.tar.gz
sudo install -d -m 700 "$RESTORE_DIR"
sudo tar -xzf "$BACKUP_DIR/$ARCHIVE" -C "$RESTORE_DIR"
sudo test -f "$RESTORE_DIR/server.db" || {
  echo "Restore failed: $RESTORE_DIR/server.db not found" >&2
  exit 1
}

docker run --rm --user 0 --entrypoint sh \
  -v "$RESTORE_DIR":/data \
  "$IMAGE" \
  -c 'chown -R openrtmp:openrtmp /data
      chmod 700 /data
      for f in /data/server.db*; do
        [ -e "$f" ] && chmod 600 "$f"
      done'
```

## API token lifecycle

On startup the server reads the `api_token` row from SQLite first:

- If a non-empty token is stored, that token is used. A different
  `LRTMP2_API_TOKEN` value only produces a warning.
- If the database has no token, a valid `LRTMP2_API_TOKEN` process environment
  variable is stored and used.
- If neither exists, the server generates a 64-character hexadecimal token,
  stores it, and prints it once to stderr.

The `.env` config loader intentionally ignores `LRTMP2_API_TOKEN`; it must be a
real process environment variable during the first startup of an empty
database. Restoring a database also restores its token.

### Manual in-place token replacement

There is currently no supported API or CLI for rotating the token while
preserving the rest of the database. If an in-place rotation is required, the
following is a manual operation grounded in the current `settings` schema.
Schedule downtime and take a backup first.

1. Stop `librtmp2-server` and the `librtmp2-server-panel`.
2. Run this on the stopped database. It generates a token without putting it
   in shell history or process arguments, writes it to a mode-`0600` file, and
   updates exactly the existing `api_token` row:

   ```bash
   DB=/var/lib/librtmp2-server/server.db
   TOKEN_FILE="$HOME/librtmp2-new-api-token"
   umask 077

   python3 - "$DB" "$TOKEN_FILE" <<'PY'
   import os
   import secrets
   import sqlite3
   import sys

   db_path, token_path = sys.argv[1:]
   conn = sqlite3.connect(db_path)
   row = conn.execute(
       "SELECT val FROM settings WHERE key = 'api_token'"
   ).fetchone()
   if row is None or not row[0]:
       raise SystemExit("database does not contain a non-empty api_token row")

   new_token = secrets.token_hex(32)
   fd = os.open(token_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
   try:
       with os.fdopen(fd, "w", encoding="ascii") as token_file:
           token_file.write(new_token + "\n")
           token_file.flush()
           os.fsync(token_file.fileno())
       with conn:
           changed = conn.execute(
               "UPDATE settings SET val = ? WHERE key = 'api_token'",
               (new_token,),
           ).rowcount
       if changed != 1:
           raise RuntimeError(f"expected one updated row, got {changed}")
   except BaseException:
       try:
           os.unlink(token_path)
       except FileNotFoundError:
           pass
       raise
   finally:
       conn.close()
   PY
   ```

3. Set `LRTMP2_API_TOKEN` in the panel's `.env` to the value in `TOKEN_FILE`.
   Keep the token out of command-line arguments and shell history.
4. Start the server, then start the panel, and run both verification requests
   below.
5. After every client has been updated, securely remove `TOKEN_FILE`.

If verification fails, stop the server and panel, restore the pre-rotation
database backup, restore the old panel token, and start the server followed by
the panel.

## Verification

The public health check proves that the HTTP listener is available:

```bash
curl --fail --silent --show-error \
  http://127.0.0.1:8080/api/v1/health
```

Then read the token without placing it in curl's process arguments and verify
an authenticated request:

```bash
read -r -s -p 'API token: ' API_TOKEN
printf '\n'
curl --fail --silent --show-error --config - \
  http://127.0.0.1:8080/api/v1/streams <<EOF
header = "Authorization: Bearer ${API_TOKEN}"
EOF
unset API_TOKEN
```

For a restore or migration, compare the returned stream IDs with the source
deployment. A public health response alone does not prove that the intended
database or API token was restored.
