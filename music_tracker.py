#!/usr/bin/env python3
"""
Music Tracker Daemon

Monitors MPRIS D-Bus events and logs listening history to SQLite.
"""

import asyncio
import logging
import signal
import sys
import time
from typing import Optional
from urllib.parse import unquote, urlparse

from dbus_next.aio import MessageBus
from dbus_next import BusType, Variant

import db

# Configuration
MIN_PLAY_SECONDS = 30  # Minimum seconds to count as a play
MIN_PLAY_PERCENT = 0.5  # Or 50% of the track

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
    """Tracks the current playing state."""

    def __init__(self):
        self.title: Optional[str] = None
        self.artist: Optional[str] = None
        self.album: Optional[str] = None
        self.duration_us: Optional[int] = None  # microseconds
        self.file_path: Optional[str] = None
        self.start_time: Optional[float] = None
        self.is_playing: bool = False

    def reset(self):
        self.title = None
        self.artist = None
        self.album = None
        self.duration_us = None
        self.file_path = None
        self.start_time = None
        self.is_playing = False

    def set_metadata(self, metadata: dict):
        """Update track info from MPRIS metadata."""
        self.title = get_variant_value(metadata.get("xesam:title"))

        artists = get_variant_value(metadata.get("xesam:artist"))
        if isinstance(artists, list) and artists:
            self.artist = artists[0]
        else:
            self.artist = artists if isinstance(artists, str) else None

        self.album = get_variant_value(metadata.get("xesam:album"))
        self.duration_us = get_variant_value(metadata.get("mpris:length"))

        url = get_variant_value(metadata.get("xesam:url"))
        if url:
            parsed = urlparse(url)
            if parsed.scheme == "file":
                self.file_path = unquote(parsed.path)
            else:
                self.file_path = url

    def should_log(self) -> bool:
        """Check if current play meets minimum thresholds.

        Rules (similar to Last.fm):
        - Must play at least 30 seconds
        - AND either: 50% of track, OR 4+ minutes played, OR duration unknown
        """
        if not self.title or not self.start_time:
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


def get_variant_value(v):
    """Extract value from D-Bus Variant."""
    if isinstance(v, Variant):
        return v.value
    return v


class MprisMonitor:
    """Monitors MPRIS players on D-Bus."""

    def __init__(self):
        self.bus: Optional[MessageBus] = None
        self.tracked_players: dict[str, TrackState] = {}
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
                self.log_play(state)

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
                    self.log_play(state)
                del self.tracked_players[name]

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

            # Get initial state
            try:
                metadata = await props_iface.call_get(MPRIS_PLAYER_IFACE, "Metadata")
                state.set_metadata(get_variant_value(metadata))
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
            log.info(f"Now monitoring: {name}")

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
                self.log_play(state)

            # Update to new track
            state.set_metadata(metadata)
            state.start_time = time.time() if state.is_playing else None

            if state.title and state.title != old_title:
                log.info(f"[{player_name}] Track changed: {state.artist} - {state.title}")

        # Check for playback status change
        if "PlaybackStatus" in changed:
            status = get_variant_value(changed["PlaybackStatus"])

            if status == "Playing" and not state.is_playing:
                state.is_playing = True
                state.start_time = time.time()
                log.info(f"[{player_name}] Playing: {state.artist} - {state.title}")

            elif status in ("Paused", "Stopped") and state.is_playing:
                if state.should_log():
                    self.log_play(state)
                state.is_playing = False
                state.start_time = None
                log.info(f"[{player_name}] {status}")

    def log_play(self, state: TrackState):
        """Log a play to the database."""
        if not state.title:
            return

        duration_ms = state.duration_us // 1000 if state.duration_us else None
        played_ms = state.get_played_ms()

        log.info(
            f"Logging play: {state.artist} - {state.title} "
            f"({played_ms // 1000}s played)"
        )

        db.log_play(
            title=state.title,
            artist=state.artist,
            album=state.album,
            duration_ms=duration_ms,
            played_ms=played_ms,
            file_path=state.file_path,
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
