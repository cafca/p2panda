#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
DIST_DIR="$ROOT_DIR/dist"
APP_NAME="p2panda File Sharing"
BIN_NAME="p2panda-file-sharing-gui"
ASSETS_DIR="$ROOT_DIR/file-sharing/assets"
VERSION="${1:?usage: package-macos.sh <version>}"

cd "$ROOT_DIR"
rm -rf "$DIST_DIR/macos"
mkdir -p "$DIST_DIR/macos"

cargo build --release --locked -p "$BIN_NAME"

TARGET_DIR="$(cargo metadata --format-version 1 --no-deps 2>/dev/null | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')"
TARGET_DIR="${TARGET_DIR:-$ROOT_DIR/target}"

APP_DIR="$DIST_DIR/macos/$APP_NAME.app"
mkdir -p "$APP_DIR/Contents/MacOS" "$APP_DIR/Contents/Resources"
cp "$TARGET_DIR/release/$BIN_NAME" "$APP_DIR/Contents/MacOS/$BIN_NAME"
chmod +x "$APP_DIR/Contents/MacOS/$BIN_NAME"
cp "$ASSETS_DIR/icon.svg" "$APP_DIR/Contents/Resources/AppIcon.svg"

cat >"$APP_DIR/Contents/Info.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleDisplayName</key>
  <string>$APP_NAME</string>
  <key>CFBundleExecutable</key>
  <string>$BIN_NAME</string>
  <key>CFBundleIdentifier</key>
  <string>org.p2panda.file-sharing</string>
  <key>CFBundleName</key>
  <string>$APP_NAME</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>CFBundleShortVersionString</key>
  <string>$VERSION</string>
  <key>CFBundleVersion</key>
  <string>$VERSION</string>
  <key>LSMinimumSystemVersion</key>
  <string>12.0</string>
</dict>
</plist>
EOF

codesign --force --deep --sign - "$APP_DIR"

DMG_NAME="p2panda-file-sharing-${VERSION}-macos.dmg"
ZIP_NAME="p2panda-file-sharing-${VERSION}-macos.zip"
hdiutil create \
  -volname "$APP_NAME" \
  -srcfolder "$APP_DIR" \
  -ov \
  -format UDZO \
  "$DIST_DIR/$DMG_NAME"

ditto -c -k --sequesterRsrc --keepParent "$APP_DIR" "$DIST_DIR/$ZIP_NAME"

echo "$DIST_DIR/$DMG_NAME"
echo "$DIST_DIR/$ZIP_NAME"
