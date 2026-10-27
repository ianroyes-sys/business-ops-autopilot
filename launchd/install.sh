#!/bin/bash
# Fill the LaunchAgent template for this checkout and load it (macOS).
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"; NODE="$(command -v node)"
DEST="$HOME/Library/LaunchAgents/com.example.business-ops.plist"
sed -e "s#__REPO__#$REPO#g" -e "s#__NODE__#$NODE#g" "$REPO/launchd/com.example.business-ops.plist.template" > "$DEST"
launchctl bootout "gui/$(id -u)" "$DEST" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$DEST"
echo "Loaded $DEST (tick every 900s). Log: /tmp/business-ops.log"
