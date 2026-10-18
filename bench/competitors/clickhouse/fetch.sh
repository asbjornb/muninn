#!/usr/bin/env bash
# Fetch the official ClickHouse static binary (same build the clickhouse/clickhouse-server
# image ships) into bench/tools/clickhouse/, verifying the release's sha512.
# Used when Docker is not available; the adapter also honours $CLICKHOUSE_BIN.
set -euo pipefail
VERSION="${CLICKHOUSE_VERSION:-26.7.34.3}"
DEST="$(cd "$(dirname "$0")/../.." && pwd)/tools/clickhouse"
mkdir -p "$DEST/download"
cd "$DEST/download"
BASE="https://github.com/ClickHouse/ClickHouse/releases/download/v${VERSION}-stable"
F="clickhouse-common-static-${VERSION}-amd64.tgz"
[ -f "$F" ] || curl -fL --retry 3 -o "$F" "$BASE/$F"
curl -fsL -o "$F.sha512" "$BASE/$F.sha512"
sha512sum -c "$F.sha512"
tar -xzf "$F" --wildcard '*/usr/bin/clickhouse' --strip-components=3 -C "$DEST" 2>/dev/null \
  || tar -xzf "$F" -C "$DEST"
BIN=$(find "$DEST" -type f -name clickhouse -path '*bin*' | head -1)
[ -n "$BIN" ] || BIN="$DEST/clickhouse"
ln -sf "$BIN" "$DEST/clickhouse-bin"
"$DEST/clickhouse-bin" --version
