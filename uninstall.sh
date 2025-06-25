#!/usr/bin/env bash
set -euo pipefail

echo "🧹 Uninstalling BinaryFlux Jump Server..."
echo "----------------------------------------"

SERVICE_NAME="binaryflux-jumpserver"
INSTALL_DIR="/opt/binaryflux-jumpserver"
CONFIG_FILE="$INSTALL_DIR/config.yml"

# 1. Stop and disable the systemd service
echo "⛔ Stopping service..."
sudo systemctl stop "$SERVICE_NAME" || true

echo "🚫 Disabling service..."
sudo systemctl disable "$SERVICE_NAME" || true

# 2. Remove systemd unit file
echo "🗑️ Removing service definition..."
sudo rm -f /etc/systemd/system/"$SERVICE_NAME".service
sudo systemctl daemon-reload

# 3. Remove installed directory
echo "🧽 Removing installation directory: $INSTALL_DIR"
sudo rm -rf "$INSTALL_DIR"

echo "✅ Uninstallation complete."
