#!/bin/sh
set -eu

VERSION_FILE="${OPENRTMP_VERSION_FILE:-/usr/local/share/openrtmp/VERSION}"
VERSION="unknown"
if [ -r "$VERSION_FILE" ]; then
    VERSION="$(cat "$VERSION_FILE")"
fi

case "$VERSION" in
    v[0-9]*) DISPLAY_VERSION="$VERSION" ;;
    [0-9]*) DISPLAY_VERSION="v$VERSION" ;;
    *) DISPLAY_VERSION="$VERSION" ;;
esac

cat <<'BANNER'
   ___                   ____ ______ __  __ ____
  / _ \ _ __   ___ _ __ |  _ \_   _|  \/  |  _ \
 | | | | '_ \ / _ \ '_ \| |_) || | | |\/| | |_) |
 | |_| | |_) |  __/ | | |  _ < | | | |  | |  __/
  \___/| .__/ \___|_| |_|_| \_\|_| |_|  |_|_|
       |_|
BANNER
printf '  Service: librtmp2-server\n'
printf '  Version: %s\n\n' "$DISPLAY_VERSION"

if [ "$#" -eq 0 ] || [ "${1#-}" != "$1" ]; then
    set -- librtmp2-server "$@"
fi

exec "$@"
