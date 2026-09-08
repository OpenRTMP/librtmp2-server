# Media outputs: recording, HLS, push relay and exec

`librtmp2-server` can optionally consume the same authenticated publisher media
that is already relayed to RTMP players and fan it out to server-side outputs.
All features are disabled by default.

The RTMP poll thread never performs disk or FFmpeg writes. It clones the
bounded `librtmp2` relay export into independent bounded worker queues, so a
slow recording disk or upstream push target cannot block RTMP ingest/local
player relay. If an individual output falls behind its queue budget, that
output is disabled for the current publisher session instead of stalling the
server.

The official Docker image includes FFmpeg. Native installations need `ffmpeg`
in `PATH`, or must set `MEDIA_FFMPEG_BIN` / `LRTMP2_MEDIA_FFMPEG_BIN`.

## Recording

```env
MEDIA_RECORDING_ENABLED=true
MEDIA_RECORDING_PATH=/data/recordings
```

A publisher session is stored as:

```text
/data/recordings/<stream_id>/<unix-milliseconds>.flv
```

Recording is implemented directly by `librtmp2-server`: RTMP audio, video and
script/metadata payloads are written as FLV tags with their original RTMP
timestamps. It does not start FFmpeg and does not consume a playback/viewer
slot.

The file is closed when publishing stops, the stream is deleted, the socket
closes, or the server shuts down.

## HLS

```env
MEDIA_HLS_ENABLED=true
MEDIA_HLS_PATH=/data/hls
MEDIA_HLS_TIME_SECS=4
MEDIA_HLS_LIST_SIZE=6
MEDIA_HLS_SEGMENT_TYPE=fmp4
MEDIA_HLS_TRANSCODE=false
MEDIA_HLS_REQUIRE_KEY=true
```

For every local publisher, the server feeds an FLV stream to an FFmpeg worker.
The default is stream copy (`-c copy`) so compatible source codecs are not
re-encoded. Set `MEDIA_HLS_TRANSCODE=true` to produce H.264/AAC using FFmpeg.

`MEDIA_HLS_SEGMENT_TYPE` accepts:

- `fmp4` — `index.m3u8`, `init.mp4`, and `.m4s` media segments (default)
- `mpegts` — `index.m3u8` and `.ts` media segments

Generated playlists are served through the existing HTTP listener:

```text
http://server:8080/hls/<stream_id>/index.m3u8?key=<play_key>
```

When `MEDIA_HLS_REQUIRE_KEY=true` (the default), the key must be an enabled
viewer/play key belonging to that stream. This includes additional viewer keys
created through the REST API/panel, not only the stream's original default play
key. The server rewrites relative segment and init-file URIs in the returned
playlist so the key is carried to subsequent HLS requests.

Set `MEDIA_HLS_REQUIRE_KEY=false` only when the HLS endpoint is intentionally
public or is protected by another trusted reverse proxy/auth layer.

HLS files are deliberately limited to `.m3u8`, `.m4s`, `.mp4`, and `.ts`, and
request paths are checked for traversal before touching the filesystem.

## RTMP/RTMPS push relay

Configure one or more upstream destinations separated by semicolons:

```env
MEDIA_PUSH_TARGETS=rtmp://backup.example/live/{stream_id}
```

A target without a selector applies to every stream. Prefix a target with an
exact stream ID or stream name followed by `|` to restrict it:

```env
MEDIA_PUSH_TARGETS=mystream|rtmps://primary.example/live/secret;other|rtmp://backup.example/live/{stream_id}
```

Supported URL placeholders are:

```text
{stream_id}
{stream_name}
{app}
```

Only `rtmp://` and `rtmps://` destinations are accepted. Full destination URLs
are never logged because they commonly contain stream keys.

The default is stream copy. Optional H.264/AAC transcoding can be enabled with:

```env
MEDIA_PUSH_TRANSCODE=true
```

Each push destination receives an independent worker/queue. One stalled
upstream therefore cannot block another push destination, recording, HLS, or
the RTMP ingest loop.

## Exec hooks

Two trusted administrator-configured hooks are available:

```env
MEDIA_EXEC_PUBLISH=/opt/openrtmp/on-publish.sh
MEDIA_EXEC_PUBLISH_DONE=/opt/openrtmp/on-publish-done.sh
```

`MEDIA_EXEC_PUBLISH` starts when an authenticated local publisher session is
activated. If it is still running when that publisher session ends, the server
terminates it. `MEDIA_EXEC_PUBLISH_DONE` is started once at session teardown.

Commands are executed through the platform shell because these values are
explicit administrator configuration. Untrusted stream values are **not**
interpolated into the shell command. Instead, hooks receive environment
variables:

```text
OPENRTMP_EVENT                  publish | publish_done
OPENRTMP_STREAM_ID
OPENRTMP_STREAM_NAME
OPENRTMP_APP
OPENRTMP_PUBLISHER_CONN_ID
OPENRTMP_RECORDING_FILE         empty when recording is disabled
OPENRTMP_HLS_PLAYLIST           empty when HLS is disabled
```

For example:

```sh
#!/bin/sh
printf '%s %s\n' "$OPENRTMP_EVENT" "$OPENRTMP_STREAM_ID" >> /data/publish-events.log
```

Treat hook commands/scripts as privileged server configuration. Do not expose a
REST endpoint that allows untrusted users to set these commands.

## Queue budget and failure behavior

```env
MEDIA_QUEUE_MB=32
```

The value is clamped to 1–512 MiB. It controls the server's media relay-export
budget used by these outputs and the byte budget of each individual output
queue. A per-output message-count bound is applied as well.

If a worker cannot keep up, its queue is disconnected for the current publisher
session. The server logs the affected output but continues ingesting and
serving other outputs. A new publisher session starts fresh workers.

In cluster mode the existing cluster media-export budget and the media-output
budget share one `librtmp2` relay-export buffer sized to the larger configured
budget. Only the node that owns the real local publisher starts recording/HLS/
push/exec outputs; media injected from another cluster node is not exported to
those outputs again.

## Process-environment overrides

Every file-config key above has an `LRTMP2_` process-environment form which
takes precedence. Examples:

```text
MEDIA_HLS_ENABLED              -> LRTMP2_MEDIA_HLS_ENABLED
MEDIA_PUSH_TARGETS             -> LRTMP2_MEDIA_PUSH_TARGETS
MEDIA_EXEC_PUBLISH             -> LRTMP2_MEDIA_EXEC_PUBLISH
MEDIA_QUEUE_MB                 -> LRTMP2_MEDIA_QUEUE_MB
```

This is useful for Docker/Portainer deployments where secrets such as RTMP push
destination URLs should be supplied as container environment variables instead
of committed to a config file.
