#!/bin/bash
#
# Music Analytics - Uninstallation Script
#
# Removes the systemd service. Does not delete your listening history or the code.
#

set -e

SERVICE_NAME="music-tracker"
SYSTEMD_USER_DIR="$HOME/.config/systemd/user"
SERVICE_FILE="$SYSTEMD_USER_DIR/$SERVICE_NAME.service"

echo "Music Analytics Uninstaller"
echo "==========================="
echo

# Stop and disable service if it exists
if [ -f "$SERVICE_FILE" ]; then
    echo "Stopping service..."
    systemctl --user stop "$SERVICE_NAME.service" 2>/dev/null || true

    echo "Disabling service..."
    systemctl --user disable "$SERVICE_NAME.service" 2>/dev/null || true

    echo "Removing service file..."
    rm -f "$SERVICE_FILE"

    systemctl --user daemon-reload

    echo
    echo "Service removed successfully."
else
    echo "Service not installed."
fi

echo
echo "Note: Your listening history database and source code were not deleted."
echo "To completely remove everything, delete this directory."
