# Music Analytics

Local "Spotify Wrapped"-style analytics for Linux. Tracks listening history from any MPRIS-compatible player (Amberol, Spotify, VLC, etc.) and generates detailed statistics.

## Features

- **Automatic tracking** via MPRIS D-Bus monitoring
- **Smart scrobbling** (30s minimum, 50% or 4min threshold)
- **Rich analytics**: top artists/albums/songs, listening streaks, sessions, skip rate
- **Personality insights**: discover your listener type (Explorer, Loyalist, Night Owl, etc.)
- **Milestones**: track achievements as you listen

## Installation

```bash
# Clone
git clone https://github.com/tom-bleher/music-analytics.git ~/.local/share/music-analytics
cd ~/.local/share/music-analytics

# Setup
python3 -m venv venv
venv/bin/pip install dbus-next

# Install service
cp music-tracker.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now music-tracker

# Add CLI to PATH
mkdir -p ~/.local/bin
ln -s ~/.local/share/music-analytics/music_stats.py ~/.local/bin/music-stats
```

## Usage

```bash
music-stats                # This year's stats
music-stats --week         # Last 7 days
music-stats --month        # This month
music-stats --all-time     # Everything

# Advanced
music-stats --deep         # Streaks, sessions, skip rate
music-stats --milestones   # Achievements
music-stats --personality  # Listener type analysis
music-stats --fun-facts    # Fun insights
music-stats --full         # All of the above
```

## Service Management

```bash
systemctl --user status music-tracker    # Check status
journalctl --user -u music-tracker -f    # Live logs
systemctl --user restart music-tracker   # Restart
```

## How It Works

The `music-tracker` daemon monitors D-Bus for MPRIS players. When you play music, it captures metadata (artist, title, album, duration) and logs plays to a local SQLite database after meeting the minimum play threshold.

## Requirements

- Python 3.8+
- `dbus-next`
- systemd (for service management)
- Any MPRIS-compatible music player

## License

MIT
