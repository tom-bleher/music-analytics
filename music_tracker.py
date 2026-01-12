#!/usr/bin/env python3
"""
Music Tracker Daemon

Monitors MPRIS D-Bus events and logs listening history to SQLite.
"""

import asyncio
import logging
import signal
import subprocess
import sys
import time
from datetime import datetime
from typing import Optional, List, Tuple
from urllib.parse import unquote, urlparse

from dbus_next.aio import MessageBus
from dbus_next import BusType, Variant

try:
    import pulsectl
    PULSECTL_AVAILABLE = True
except ImportError:
    PULSECTL_AVAILABLE = False

import db

# Configuration
MIN_PLAY_SECONDS = 30  # Minimum seconds to count as a play
MIN_PLAY_PERCENT = 0.5  # Or 50% of the track
TRACK_LOCAL_ONLY = True  # Only track local music files (file:// URLs)

# Known local-only players (these don't stream, so assume local)
# These players may not provide xesam:url metadata
LOCAL_ONLY_PLAYERS = {
    "io.bassi.Amberol",      # Amberol - GNOME local music player
    "org.gnome.Lollypop",    # Lollypop
    "org.gnome.Music",       # GNOME Music
    "audacious",             # Audacious
    "deadbeef",              # DeaDBeeF
    "quodlibet",             # Quod Libet
    "clementine",            # Clementine
    "strawberry",            # Strawberry
    "rhythmbox",             # Rhythmbox (mostly local)
    "elisa",                 # Elisa
    "sayonara",              # Sayonara
    "cantata",               # Cantata (MPD client)
}

MPRIS_PREFIX = "org.mpris.MediaPlayer2."
MPRIS_PATH = "/org/mpris/MediaPlayer2"
MPRIS_PLAYER_IFACE = "org.mpris.MediaPlayer2.Player"
DBUS_PROPS_IFACE = "org.freedesktop.DBus.Properties"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


class TrackState:
    """Tracks the current playing state with full metadata."""

    def __init__(self):
        # Core fields
        self.title: Optional[str] = None
        self.artist: Optional[str] = None
        self.album: Optional[str] = None
        self.duration_us: Optional[int] = None  # microseconds
        self.file_path: Optional[str] = None
        self.start_time: Optional[float] = None
        self.is_playing: bool = False
        self.is_local: bool = False  # True if source is a local file

        # Extended metadata
        self.genre: Optional[str] = None
        self.album_artist: Optional[str] = None
        self.track_number: Optional[int] = None
        self.disc_number: Optional[int] = None
        self.release_date: Optional[str] = None
        self.art_url: Optional[str] = None
        self.user_rating: Optional[float] = None
        self.bpm: Optional[int] = None
        self.composer: Optional[str] = None
        self.musicbrainz_track_id: Optional[str] = None

        # Seek tracking
        self.seek_count: int = 0
        self.intro_skipped: bool = False
        self.seek_forward_ms: int = 0
        self.seek_backward_ms: int = 0
        self.last_position_us: int = 0  # Last known position for seek detection

    def reset(self):
        self.title = None
        self.artist = None
        self.album = None
        self.duration_us = None
        self.file_path = None
        self.start_time = None
        self.is_playing = False
        self.is_local = False
        self.genre = None
        self.album_artist = None
        self.track_number = None
        self.disc_number = None
        self.release_date = None
        self.art_url = None
        self.user_rating = None
        self.bpm = None
        self.composer = None
        self.musicbrainz_track_id = None
        # Reset seek tracking
        self.seek_count = 0
        self.intro_skipped = False
        self.seek_forward_ms = 0
        self.seek_backward_ms = 0
        self.last_position_us = 0

    def set_metadata(self, metadata: dict, player_name: Optional[str] = None):
        """Update track info from MPRIS metadata."""
        # Core fields
        self.title = get_variant_value(metadata.get("xesam:title"))

        # Artist (array of strings)
        artists = get_variant_value(metadata.get("xesam:artist"))
        if isinstance(artists, list) and artists:
            self.artist = artists[0]
        else:
            self.artist = artists if isinstance(artists, str) else None

        self.album = get_variant_value(metadata.get("xesam:album"))
        self.duration_us = get_variant_value(metadata.get("mpris:length"))

        # File path from URL and local source detection
        url = get_variant_value(metadata.get("xesam:url"))
        self.is_local = is_local_source(metadata, player_name)
        if url:
            parsed = urlparse(url)
            if parsed.scheme == "file":
                self.file_path = unquote(parsed.path)
            else:
                self.file_path = url

        # Genre (array of strings -> comma-separated)
        genres = get_variant_value(metadata.get("xesam:genre"))
        if isinstance(genres, list) and genres:
            self.genre = ", ".join(genres)
        elif isinstance(genres, str):
            self.genre = genres
        else:
            self.genre = None

        # Album artist (array of strings)
        album_artists = get_variant_value(metadata.get("xesam:albumArtist"))
        if isinstance(album_artists, list) and album_artists:
            self.album_artist = album_artists[0]
        elif isinstance(album_artists, str):
            self.album_artist = album_artists
        else:
            self.album_artist = None

        # Track and disc numbers
        self.track_number = get_variant_value(metadata.get("xesam:trackNumber"))
        self.disc_number = get_variant_value(metadata.get("xesam:discNumber"))

        # Release date (ISO 8601 or just year)
        self.release_date = get_variant_value(metadata.get("xesam:contentCreated"))

        # Art URL
        self.art_url = get_variant_value(metadata.get("mpris:artUrl"))

        # User rating (0.0 - 1.0)
        self.user_rating = get_variant_value(metadata.get("xesam:userRating"))

        # BPM
        self.bpm = get_variant_value(metadata.get("xesam:audioBPM"))

        # Composer (array of strings)
        composers = get_variant_value(metadata.get("xesam:composer"))
        if isinstance(composers, list) and composers:
            self.composer = ", ".join(composers)
        elif isinstance(composers, str):
            self.composer = composers
        else:
            self.composer = None

        # MusicBrainz track ID
        self.musicbrainz_track_id = get_variant_value(
            metadata.get("xesam:musicBrainzTrackID")
        )

    def should_log(self) -> bool:
        """Check if current play meets minimum thresholds.

        Rules (similar to Last.fm):
        - Must play at least 30 seconds
        - AND either: 50% of track, OR 4+ minutes played, OR duration unknown
        - If TRACK_LOCAL_ONLY is True, only local files are logged
        """
        if not self.title or not self.start_time:
            return False

        # Skip non-local sources if TRACK_LOCAL_ONLY is enabled
        if TRACK_LOCAL_ONLY and not self.is_local:
            return False

        played_seconds = time.time() - self.start_time

        # Must play at least MIN_PLAY_SECONDS
        if played_seconds < MIN_PLAY_SECONDS:
            return False

        # If duration unknown, 30 seconds is enough
        if not self.duration_us:
            return True

        duration_seconds = self.duration_us / 1_000_000

        # Either 50% of track OR 4 minutes played
        if played_seconds >= duration_seconds * MIN_PLAY_PERCENT:
            return True
        if played_seconds >= 240:  # 4 minutes
            return True

        return False

    def get_played_ms(self) -> int:
        """Get milliseconds played."""
        if not self.start_time:
            return 0
        return int((time.time() - self.start_time) * 1000)

    def on_seeked(self, new_position_us: int):
        """Handle a seek event.

        Args:
            new_position_us: New position in microseconds
        """
        self.seek_count += 1

        # Calculate seek delta
        delta_us = new_position_us - self.last_position_us
        delta_ms = delta_us // 1000

        if delta_us > 0:
            self.seek_forward_ms += delta_ms
        else:
            self.seek_backward_ms += abs(delta_ms)

        # Check if intro was skipped (seeked past first 15 seconds from near start)
        if self.last_position_us < 5_000_000 and new_position_us > 15_000_000:
            self.intro_skipped = True

        self.last_position_us = new_position_us


def get_variant_value(v):
    """Extract value from D-Bus Variant."""
    if isinstance(v, Variant):
        return v.value
    return v


def is_local_source(metadata: dict, player_name: Optional[str] = None) -> bool:
    """Check if the source is a local file based on MPRIS metadata.

    Args:
        metadata: MPRIS metadata dictionary
        player_name: Optional player name (e.g., "io.bassi.Amberol")

    Returns:
        True if the source is a local file (file:// URL or known local player), False otherwise
    """
    # Check if player is a known local-only player
    if player_name:
        # Extract the player identifier from full bus name
        # e.g., "org.mpris.MediaPlayer2.io.bassi.Amberol" -> "io.bassi.Amberol"
        player_id = player_name.replace(MPRIS_PREFIX, "") if player_name.startswith(MPRIS_PREFIX) else player_name
        if player_id in LOCAL_ONLY_PLAYERS:
            return True

    url = get_variant_value(metadata.get("xesam:url"))
    if not url:
        # No URL provided and not a known local player - likely a streaming service
        return False

    # Parse the URL and check the scheme
    parsed = urlparse(url)

    # Local files have file:// scheme
    if parsed.scheme == "file":
        return True

    # Also accept paths that start with / (some players provide raw paths)
    if parsed.scheme == "" and url.startswith("/"):
        return True

    # Everything else is non-local (http, https, spotify, etc.)
    return False


def get_pulse_volume_for_player(player_name: str) -> Optional[float]:
    """Get the PulseAudio/PipeWire volume for a player.

    Args:
        player_name: MPRIS player name (e.g., "org.mpris.MediaPlayer2.io.bassi.Amberol")

    Returns:
        Volume as float 0.0-1.0, or None if not found
    """
    if not PULSECTL_AVAILABLE:
        return None

    # Extract app name from MPRIS name
    # e.g., "org.mpris.MediaPlayer2.io.bassi.Amberol" -> "amberol"
    app_name = player_name.replace(MPRIS_PREFIX, "").lower()
    # Handle dotted names like "io.bassi.Amberol" -> "amberol"
    if "." in app_name:
        app_name = app_name.split(".")[-1].lower()

    try:
        with pulsectl.Pulse("music-tracker") as pulse:
            for sink_input in pulse.sink_input_list():
                # Check various properties that might contain the app name
                props = sink_input.proplist
                sink_app_name = props.get("application.name", "").lower()
                sink_binary = props.get("application.process.binary", "").lower()

                if app_name in sink_app_name or app_name in sink_binary:
                    # Return the volume (average of all channels)
                    volumes = sink_input.volume.values
                    if volumes:
                        return sum(volumes) / len(volumes)
    except Exception as e:
        log.debug(f"Could not get PulseAudio volume: {e}")

    return None


def get_season(dt: datetime) -> str:
    """Get the season for a given date (Northern Hemisphere)."""
    month = dt.month
    if month in (3, 4, 5):
        return "spring"
    elif month in (6, 7, 8):
        return "summer"
    elif month in (9, 10, 11):
        return "fall"
    else:
        return "winter"


def get_active_window() -> Optional[str]:
    """Get the currently focused window name."""
    try:
        # Try xdotool for X11
        result = subprocess.run(
            ["xdotool", "getactivewindow", "getwindowname"],
            capture_output=True, text=True, timeout=1
        )
        if result.returncode == 0:
            return result.stdout.strip()[:200]  # Limit length
    except Exception:
        pass

    try:
        # Try wlrctl for Wayland
        result = subprocess.run(
            ["wlrctl", "toplevel", "focus"],
            capture_output=True, text=True, timeout=1
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()[:200]
    except Exception:
        pass

    return None


def get_screen_state() -> Optional[int]:
    """Check if screen is on (1) or off (0)."""
    try:
        # Try using gnome-screensaver
        result = subprocess.run(
            ["gnome-screensaver-command", "-q"],
            capture_output=True, text=True, timeout=1
        )
        if result.returncode == 0:
            return 0 if "is active" in result.stdout.lower() else 1
    except Exception:
        pass

    try:
        # Try using xset for X11
        result = subprocess.run(
            ["xset", "q"],
            capture_output=True, text=True, timeout=1
        )
        if result.returncode == 0:
            # Check if monitor is in standby/suspend/off
            if "Monitor is Off" in result.stdout:
                return 0
            return 1
    except Exception:
        pass

    return None


def get_power_state() -> Optional[int]:
    """Check if on battery (1) or plugged in (0)."""
    try:
        # Check /sys/class/power_supply
        import os
        power_supplies = os.listdir("/sys/class/power_supply/")
        for ps in power_supplies:
            if ps.startswith("BAT"):
                status_path = f"/sys/class/power_supply/{ps}/status"
                if os.path.exists(status_path):
                    with open(status_path) as f:
                        status = f.read().strip().lower()
                    return 1 if status == "discharging" else 0
    except Exception:
        pass

    try:
        # Try upower
        result = subprocess.run(
            ["upower", "-i", "/org/freedesktop/UPower/devices/battery_BAT0"],
            capture_output=True, text=True, timeout=2
        )
        if result.returncode == 0:
            if "state:" in result.stdout.lower():
                if "discharging" in result.stdout.lower():
                    return 1
                return 0
    except Exception:
        pass

    return None


def get_context() -> dict:
    """Get current listening context."""
    now = datetime.now()
    return {
        "hour_of_day": now.hour,
        "day_of_week": now.weekday(),  # 0=Monday, 6=Sunday
        "is_weekend": 1 if now.weekday() >= 5 else 0,
        "season": get_season(now),
        "active_window": get_active_window(),
        "screen_on": get_screen_state(),
        "on_battery": get_power_state(),
    }


class MprisMonitor:
    """Monitors MPRIS players on D-Bus."""

    def __init__(self):
        self.bus: Optional[MessageBus] = None
        self.tracked_players: dict[str, TrackState] = {}
        self.player_props: dict[str, any] = {}  # Store props interfaces for volume queries
        self.running = True

    async def start(self):
        """Start monitoring."""
        log.info("Connecting to session bus...")
        self.bus = await MessageBus(bus_type=BusType.SESSION).connect()

        # Watch for new players
        await self.watch_name_changes()

        # Find existing players
        await self.discover_players()

        log.info("Music tracker started. Monitoring MPRIS players...")

        # Keep running
        while self.running:
            await asyncio.sleep(1)

    async def stop(self):
        """Stop monitoring and log any in-progress plays."""
        self.running = False
        for player_name, state in self.tracked_players.items():
            if state.is_playing and state.should_log():
                self.log_play(player_name, state)

    async def watch_name_changes(self):
        """Watch for MPRIS players appearing/disappearing."""
        introspection = await self.bus.introspect("org.freedesktop.DBus", "/org/freedesktop/DBus")
        dbus_obj = self.bus.get_proxy_object("org.freedesktop.DBus", "/org/freedesktop/DBus", introspection)
        dbus_iface = dbus_obj.get_interface("org.freedesktop.DBus")

        dbus_iface.on_name_owner_changed(self.on_name_owner_changed)

    def on_name_owner_changed(self, name: str, old_owner: str, new_owner: str):
        """Handle player appearing/disappearing."""
        if not name.startswith(MPRIS_PREFIX):
            return

        if new_owner and not old_owner:
            # New player appeared
            log.info(f"Player appeared: {name}")
            asyncio.create_task(self.add_player(name))
        elif old_owner and not new_owner:
            # Player disappeared
            log.info(f"Player disappeared: {name}")
            if name in self.tracked_players:
                state = self.tracked_players[name]
                if state.is_playing and state.should_log():
                    self.log_play(name, state)
                del self.tracked_players[name]
                if name in self.player_props:
                    del self.player_props[name]

    async def discover_players(self):
        """Find existing MPRIS players."""
        introspection = await self.bus.introspect("org.freedesktop.DBus", "/org/freedesktop/DBus")
        dbus_obj = self.bus.get_proxy_object("org.freedesktop.DBus", "/org/freedesktop/DBus", introspection)
        dbus_iface = dbus_obj.get_interface("org.freedesktop.DBus")

        names = await dbus_iface.call_list_names()
        for name in names:
            if name.startswith(MPRIS_PREFIX):
                await self.add_player(name)

    async def add_player(self, name: str):
        """Start monitoring a player."""
        if name in self.tracked_players:
            return

        try:
            introspection = await self.bus.introspect(name, MPRIS_PATH)
            player_obj = self.bus.get_proxy_object(name, MPRIS_PATH, introspection)
            props_iface = player_obj.get_interface(DBUS_PROPS_IFACE)

            state = TrackState()
            self.tracked_players[name] = state
            self.player_props[name] = props_iface  # Store for volume queries

            # Get initial state
            try:
                metadata = await props_iface.call_get(MPRIS_PLAYER_IFACE, "Metadata")
                state.set_metadata(get_variant_value(metadata), name)
            except Exception:
                pass

            try:
                playback_status = await props_iface.call_get(MPRIS_PLAYER_IFACE, "PlaybackStatus")
                status = get_variant_value(playback_status)
                if status == "Playing":
                    state.is_playing = True
                    state.start_time = time.time()
                    log.info(f"[{name}] Already playing: {state.artist} - {state.title}")
            except Exception:
                pass

            # Subscribe to property changes
            def make_handler(player_name: str):
                def handler(iface: str, changed: dict, invalidated: list):
                    if iface == MPRIS_PLAYER_IFACE:
                        self.on_player_properties_changed(player_name, changed)
                return handler

            props_iface.on_properties_changed(make_handler(name))

            # Subscribe to Seeked signal for seek tracking
            try:
                player_iface = player_obj.get_interface(MPRIS_PLAYER_IFACE)

                def make_seeked_handler(player_name: str):
                    def handler(position_us: int):
                        self.on_player_seeked(player_name, position_us)
                    return handler

                player_iface.on_seeked(make_seeked_handler(name))
                log.info(f"Now monitoring (with seek tracking): {name}")
            except Exception as e:
                log.info(f"Now monitoring: {name} (seek tracking unavailable: {e})")

        except Exception as e:
            log.error(f"Failed to monitor {name}: {e}")

    def on_player_properties_changed(self, player_name: str, changed: dict):
        """Handle property changes from a player."""
        if player_name not in self.tracked_players:
            return

        state = self.tracked_players[player_name]

        # Check for metadata change (new track)
        if "Metadata" in changed:
            old_title = state.title
            metadata = get_variant_value(changed["Metadata"])

            # Log previous track if it qualifies
            if state.is_playing and state.should_log():
                self.log_play(player_name, state)

            # Update to new track
            state.set_metadata(metadata, player_name)
            state.start_time = time.time() if state.is_playing else None

            if state.title and state.title != old_title:
                local_info = "" if state.is_local else " (non-local, won't track)"
                log.info(f"[{player_name}] Track changed: {state.artist} - {state.title}{local_info}")

        # Check for playback status change
        if "PlaybackStatus" in changed:
            status = get_variant_value(changed["PlaybackStatus"])

            if status == "Playing" and not state.is_playing:
                state.is_playing = True
                state.start_time = time.time()
                log.info(f"[{player_name}] Playing: {state.artist} - {state.title}")

            elif status in ("Paused", "Stopped") and state.is_playing:
                if state.should_log():
                    self.log_play(player_name, state)
                state.is_playing = False
                state.start_time = None
                log.info(f"[{player_name}] {status}")

    def on_player_seeked(self, player_name: str, position_us: int):
        """Handle seek event from a player."""
        if player_name not in self.tracked_players:
            return

        state = self.tracked_players[player_name]
        state.on_seeked(position_us)
        log.debug(
            f"[{player_name}] Seeked to {position_us // 1_000_000}s "
            f"(total seeks: {state.seek_count})"
        )

    def log_play(self, player_name: str, state: TrackState):
        """Log a play to the database with full metadata and volume."""
        if not state.title:
            return

        duration_ms = state.duration_us // 1000 if state.duration_us else None
        played_ms = state.get_played_ms()

        # Get volume information
        app_volume = None
        system_volume = None
        effective_volume = None

        # Try to get MPRIS app volume
        if player_name in self.player_props:
            try:
                props_iface = self.player_props[player_name]
                # This is a sync call in an async context, but it's fast
                import asyncio
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # We're in an async context, need to handle this carefully
                    # For simplicity, we'll try a synchronous approach
                    pass  # Will rely on system volume
            except Exception as e:
                log.debug(f"Could not get MPRIS volume: {e}")

        # Get PulseAudio system volume
        system_volume = get_pulse_volume_for_player(player_name)

        # Calculate effective volume
        if app_volume is not None and system_volume is not None:
            effective_volume = app_volume * system_volume
        elif system_volume is not None:
            # If we only have system volume, use it as effective
            effective_volume = system_volume
        elif app_volume is not None:
            effective_volume = app_volume

        seek_info = ""
        if state.seek_count > 0:
            seek_info = f", {state.seek_count} seeks"
            if state.intro_skipped:
                seek_info += ", intro skipped"

        vol_info = ""
        if effective_volume is not None:
            vol_info = f", vol: {effective_volume:.0%}"

        # Get context information
        context = get_context()

        log.info(
            f"Logging play: {state.artist} - {state.title} "
            f"({played_ms // 1000}s played{seek_info}{vol_info})"
        )

        db.log_play(
            title=state.title,
            artist=state.artist,
            album=state.album,
            duration_ms=duration_ms,
            played_ms=played_ms,
            file_path=state.file_path,
            genre=state.genre,
            album_artist=state.album_artist,
            track_number=state.track_number,
            disc_number=state.disc_number,
            release_date=state.release_date,
            art_url=state.art_url,
            user_rating=state.user_rating,
            bpm=state.bpm,
            composer=state.composer,
            musicbrainz_track_id=state.musicbrainz_track_id,
            seek_count=state.seek_count if state.seek_count > 0 else None,
            intro_skipped=1 if state.intro_skipped else None,
            seek_forward_ms=state.seek_forward_ms if state.seek_forward_ms > 0 else None,
            seek_backward_ms=state.seek_backward_ms if state.seek_backward_ms > 0 else None,
            app_volume=app_volume,
            system_volume=system_volume,
            effective_volume=effective_volume,
            hour_of_day=context["hour_of_day"],
            day_of_week=context["day_of_week"],
            is_weekend=context["is_weekend"],
            season=context["season"],
            active_window=context["active_window"],
            screen_on=context["screen_on"],
            on_battery=context["on_battery"],
            player_name=player_name.replace(MPRIS_PREFIX, ""),
            is_local=1 if state.is_local else 0,
        )


async def main():
    monitor = MprisMonitor()

    def handle_signal(sig):
        log.info(f"Received signal {sig}, shutting down...")
        asyncio.create_task(monitor.stop())

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))

    try:
        await monitor.start()
    except Exception as e:
        log.error(f"Error: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
