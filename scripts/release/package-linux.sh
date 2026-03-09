#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
DIST_DIR="$ROOT_DIR/dist"
BIN_NAME="p2panda-file-sharing-gui"
ASSETS_DIR="$ROOT_DIR/file-sharing/assets"
VERSION="${1:?usage: package-linux.sh <version>}"
ARCH="${ARCH:-x86_64}"

cd "$ROOT_DIR"
rm -rf "$DIST_DIR/linux" "$DIST_DIR/linux-tar"
mkdir -p "$DIST_DIR/linux"

cargo build --release --locked -p "$BIN_NAME"

TARGET_DIR="$(cargo metadata --format-version 1 --no-deps 2>/dev/null | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')"
TARGET_DIR="${TARGET_DIR:-$ROOT_DIR/target}"

TAR_STAGING="$DIST_DIR/linux-tar/p2panda-file-sharing-${VERSION}-linux-${ARCH}"
mkdir -p "$TAR_STAGING"
cp "$TARGET_DIR/release/$BIN_NAME" "$TAR_STAGING/"
chmod +x "$TAR_STAGING/$BIN_NAME"
cp "$ASSETS_DIR/p2panda-file-sharing.desktop" "$TAR_STAGING/"
cp "$ASSETS_DIR/icon.svg" "$TAR_STAGING/p2panda-file-sharing.svg"

cat >"$TAR_STAGING/README.txt" <<EOF
p2panda File Sharing $VERSION

Run:
  ./p2panda-file-sharing-gui
EOF

tar -czf \
  "$DIST_DIR/p2panda-file-sharing-${VERSION}-linux-${ARCH}.tar.gz" \
  -C "$DIST_DIR/linux-tar" \
  "p2panda-file-sharing-${VERSION}-linux-${ARCH}"

# cargo-appimage looks for ./icon.png in cwd to embed as the app icon.
# Convert the SVG to PNG so appimagetool finds it.
rsvg-convert -w 256 -h 256 "$ASSETS_DIR/icon.svg" > "$ROOT_DIR/file-sharing/icon.png"
(cd "$ROOT_DIR/file-sharing" && cargo appimage --locked)
rm -f "$ROOT_DIR/file-sharing/icon.png"
APPIMAGE_SOURCE="$(find "$TARGET_DIR" -type f -name '*.AppImage' | sort | tail -n 1)"

if [[ -z "$APPIMAGE_SOURCE" ]]; then
  echo "failed to locate AppImage output" >&2
  exit 1
fi

cp "$APPIMAGE_SOURCE" "$DIST_DIR/p2panda-file-sharing-${VERSION}-linux-${ARCH}.AppImage"
chmod +x "$DIST_DIR/p2panda-file-sharing-${VERSION}-linux-${ARCH}.AppImage"

echo "$DIST_DIR/p2panda-file-sharing-${VERSION}-linux-${ARCH}.AppImage"
echo "$DIST_DIR/p2panda-file-sharing-${VERSION}-linux-${ARCH}.tar.gz"
