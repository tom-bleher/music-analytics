# Music Analytics

Local "Spotify Wrapped"-style analytics for Linux. Tracks listening history from any MPRIS-compatible player (Amberol, Spotify, VLC, etc.) and generates detailed statistics.

## Features

- **Automatic tracking** via MPRIS D-Bus monitoring
- **Smart scrobbling** (30s minimum, 50% or 4min threshold)
- **Rich metadata capture**: genre, label, release date, MusicBrainz IDs, BPM
- **Library scanner**: extract metadata from local music files
- **Analytics**: top artists/albums/songs, listening streaks, sessions, skip rate
- **Personality insights**: discover your listener type
- **Milestones**: track achievements as you listen

## Installation

```bash
# Clone
git clone https://github.com/tom-bleher/music-analytics.git ~/.local/share/music-analytics
cd ~/.local/share/music-analytics

# Setup
python3 -m venv venv
venv/bin/pip install dbus-next mutagen

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
# Listening stats
music-stats                # This year's stats
music-stats --week         # Last 7 days
music-stats --all-time     # Everything
music-stats --full         # All analytics

# Library management
music-stats --scan ~/Music # Scan music folder
music-stats --library      # Show library stats

# Advanced analytics
music-stats --deep         # Streaks, sessions, genres, decades
music-stats --milestones   # Achievements
music-stats --personality  # Listener type analysis
```

## Captured Metadata

From MPRIS (live playback):
- Title, artist, album, duration
- Genre, composer, album artist
- Track/disc number, release date
- BPM, user rating, MusicBrainz IDs

From library scan:
- All of the above, plus:
- ISRC, barcode, record label
- Release country, release type
- ReplayGain values, bit rate

## Service Management

```bash
systemctl --user status music-tracker    # Check status
journalctl --user -u music-tracker -f    # Live logs
systemctl --user restart music-tracker   # Restart
```

## Requirements

- Python 3.8+
- `dbus-next` (MPRIS monitoring)
- `mutagen` (library scanning)
- systemd (service management)
- Any MPRIS-compatible music player

## License

MIT
