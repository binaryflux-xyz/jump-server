#!/usr/bin/env bash
set -euo pipefail

echo "🛰️ Installing BinaryFlux Jump Server..."
echo "----------------------------------------"

INSTALL_DIR="/opt/binaryflux-jumpserver"

# 1. Create installation directory
echo "📁 Creating target directory: $INSTALL_DIR"
sudo rm -rf "$INSTALL_DIR"
sudo mkdir -p "$INSTALL_DIR"
sudo cp -r ./* "$INSTALL_DIR"
sudo chown -R root:root "$INSTALL_DIR"

# 2. Copy binaryflux-jumpserver.service to systemd
echo "🔧 Installing systemd service..."
sudo cp "$INSTALL_DIR/binaryflux-jumpserver.service" /etc/systemd/system/binaryflux-jumpserver.service

# 3. Reload systemd
sudo systemctl daemon-reexec
sudo systemctl daemon-reload

# 4. Enable and start the service
echo "🚀 Enabling and starting service..."
sudo systemctl enable binaryflux-jumpserver
sudo systemctl start binaryflux-jumpserver

# 5. Status check
echo "✅ Jump server installed and running!"
echo "🔍 To check status:   sudo systemctl status binaryflux-jumpserver"
echo "📄 To view logs:      journalctl -u binaryflux-jumpserver -f"
