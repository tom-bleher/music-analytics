#!/bin/bash
#
# Music Analytics - Installation Script
#
# Installs the music tracker daemon and its dependencies.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="music-tracker"
SYSTEMD_USER_DIR="$HOME/.config/systemd/user"

echo "Music Analytics Installer"
echo "========================="
echo

# Check for Python 3
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is required but not installed."
    exit 1
fi

PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo "Found Python $PYTHON_VERSION"

# Create virtual environment if it doesn't exist
if [ ! -d "$SCRIPT_DIR/venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$SCRIPT_DIR/venv"
fi

# Install dependencies
echo "Installing dependencies..."
"$SCRIPT_DIR/venv/bin/pip" install --quiet --upgrade pip
"$SCRIPT_DIR/venv/bin/pip" install --quiet -r "$SCRIPT_DIR/requirements.txt"

# Create systemd user directory if needed
mkdir -p "$SYSTEMD_USER_DIR"

# Generate and install systemd service
echo "Installing systemd service..."
cat > "$SYSTEMD_USER_DIR/$SERVICE_NAME.service" << EOF
[Unit]
Description=Music Tracker - MPRIS listening history daemon
Documentation=https://github.com/tom-bleher/music-analytics
After=graphical-session.target

[Service]
Type=simple
ExecStart=$SCRIPT_DIR/venv/bin/python $SCRIPT_DIR/music_tracker.py
# Restart after idle exit to check for new players
Restart=always
RestartSec=10

# Environment
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=default.target
EOF

# Reload systemd and enable service
systemctl --user daemon-reload
systemctl --user enable "$SERVICE_NAME.service"
systemctl --user restart "$SERVICE_NAME.service"

echo
echo "Installation complete!"
echo
echo "The music tracker is now running and will start automatically on login."
echo "It monitors MPRIS-compatible music players and logs your listening history."
echo
echo "Useful commands:"
echo "  Status:   systemctl --user status $SERVICE_NAME"
echo "  Logs:     journalctl --user -u $SERVICE_NAME -f"
echo "  Stop:     systemctl --user stop $SERVICE_NAME"
echo "  Disable:  systemctl --user disable $SERVICE_NAME"
echo
echo "To view your stats, run: ./music_stats.py"
