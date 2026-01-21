#!/usr/bin/env python3
"""Analytics for music listening data.

This module provides functions for analyzing listening patterns including:
- Time-based analytics: streaks, sessions, time-of-day analysis
- Artist insight analytics: discovery rate, loyalty, one-hit wonders, etc.
- Track behavior analytics: skip rates, repeat obsessions, album completion
"""

import sqlite3
from datetime import datetime, timedelta
from typing import Optional, TypedDict
from collections import defaultdict

from db import get_connection


class StreakInfo(TypedDict):
    """Type definition for streak information."""
    current_streak: int
    longest_streak: int
    longest_streak_start: Optional[str]
    longest_streak_end: Optional[str]
    streak_history: list[dict]


class SessionInfo(TypedDict):
    """Type definition for session information."""
    total_sessions: int
    avg_session_length_minutes: float
    longest_session_minutes: float
    longest_session_start: Optional[str]
    longest_session_end: Optional[str]
    total_listening_minutes: float


class NightOwlScore(TypedDict):
    """Type definition for night owl score."""
    night_owl_percentage: float
    night_plays: int
    total_plays: int
    night_listening_minutes: float
    total_listening_minutes: float


class BiggestDay(TypedDict):
    """Type definition for biggest listening day."""
    date: str
    play_count: int
    listening_minutes: float
    top_artist: Optional[str]
    top_track: Optional[str]


class HourlyHeatmap(TypedDict):
    """Type definition for hourly heatmap data."""
    hours: dict[int, int]
    peak_hour: int
    peak_hour_plays: int
    quietest_hour: int
    quietest_hour_plays: int


def get_listening_streaks(
    conn: sqlite3.Connection,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> StreakInfo:
    """Find consecutive days with listening activity.

    A streak is defined as consecutive calendar days where at least one
    track was played.

    Args:
        conn: SQLite database connection with row_factory set.
        start_date: Start of the date range (inclusive). If None, no lower bound.
        end_date: End of the date range (inclusive). If None, no upper bound.

    Returns:
        StreakInfo dict containing:
            - current_streak: Number of consecutive days up to today (or end_date)
            - longest_streak: Maximum consecutive days in the range
            - longest_streak_start: Start date of longest streak (ISO format)
            - longest_streak_end: End date of longest streak (ISO format)
            - streak_history: List of all streaks with start, end, and length
    """
    # Build query to get distinct dates with plays
    query = """
        SELECT DISTINCT DATE(timestamp) as play_date
        FROM plays
        WHERE 1=1
    """
    params = []

    if start_date:
        query += " AND timestamp >= ?"
        params.append(start_date.isoformat())
    if end_date:
        query += " AND timestamp <= ?"
        params.append(end_date.isoformat())

    query += " ORDER BY play_date ASC"

    cursor = conn.execute(query, params)
    rows = cursor.fetchall()

    if not rows:
        return StreakInfo(
            current_streak=0,
            longest_streak=0,
            longest_streak_start=None,
            longest_streak_end=None,
            streak_history=[],
        )

    # Convert to date objects
    dates = [datetime.strptime(row["play_date"], "%Y-%m-%d").date() for row in rows]

    # Find all streaks
    streaks = []
    streak_start = dates[0]
    streak_end = dates[0]

    for i in range(1, len(dates)):
        if dates[i] - dates[i - 1] == timedelta(days=1):
            # Continue streak
            streak_end = dates[i]
        else:
            # End current streak, start new one
            streak_length = (streak_end - streak_start).days + 1
            streaks.append({
                "start": streak_start.isoformat(),
                "end": streak_end.isoformat(),
                "length": streak_length,
            })
            streak_start = dates[i]
            streak_end = dates[i]

    # Don't forget the last streak
    streak_length = (streak_end - streak_start).days + 1
    streaks.append({
        "start": streak_start.isoformat(),
        "end": streak_end.isoformat(),
        "length": streak_length,
    })

    # Find longest streak
    longest = max(streaks, key=lambda s: s["length"])

    # Calculate current streak (streak that includes today or end_date)
    reference_date = (end_date.date() if end_date else datetime.now().date())
    current_streak = 0

    if streaks:
        last_streak = streaks[-1]
        last_streak_end = datetime.strptime(last_streak["end"], "%Y-%m-%d").date()
        # Current streak if it ends today or yesterday (still active)
        if reference_date - last_streak_end <= timedelta(days=1):
            current_streak = last_streak["length"]
            # Add today if not in the data yet
            if last_streak_end < reference_date:
                # Check if there's a play today
                pass  # Current streak stays as is if no play today yet

    return StreakInfo(
        current_streak=current_streak,
        longest_streak=longest["length"],
        longest_streak_start=longest["start"],
        longest_streak_end=longest["end"],
        streak_history=streaks,
    )


def get_sessions(
    conn: sqlite3.Connection,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    gap_minutes: int = 30,
) -> SessionInfo:
    """Group plays into listening sessions based on time gaps.

    A new session starts when there's a gap of more than `gap_minutes`
    between consecutive plays.

    Args:
        conn: SQLite database connection with row_factory set.
        start_date: Start of the date range (inclusive). If None, no lower bound.
        end_date: End of the date range (inclusive). If None, no upper bound.
        gap_minutes: Minutes of inactivity that defines a new session. Default 30.

    Returns:
        SessionInfo dict containing:
            - total_sessions: Number of distinct listening sessions
            - avg_session_length_minutes: Average session duration
            - longest_session_minutes: Duration of the longest session
            - longest_session_start: Start time of longest session (ISO format)
            - longest_session_end: End time of longest session (ISO format)
            - total_listening_minutes: Total listening time across all sessions
    """
    query = """
        SELECT timestamp, played_ms, duration_ms
        FROM plays
        WHERE 1=1
    """
    params = []

    if start_date:
        query += " AND timestamp >= ?"
        params.append(start_date.isoformat())
    if end_date:
        query += " AND timestamp <= ?"
        params.append(end_date.isoformat())

    query += " ORDER BY timestamp ASC"

    cursor = conn.execute(query, params)
    rows = cursor.fetchall()

    if not rows:
        return SessionInfo(
            total_sessions=0,
            avg_session_length_minutes=0.0,
            longest_session_minutes=0.0,
            longest_session_start=None,
            longest_session_end=None,
            total_listening_minutes=0.0,
        )

    gap_threshold = timedelta(minutes=gap_minutes)
    sessions = []

    # Start first session
    current_session_start = datetime.fromisoformat(rows[0]["timestamp"])
    current_session_end = current_session_start
    current_session_listening_ms = rows[0]["played_ms"] or rows[0]["duration_ms"] or 0

    for i in range(1, len(rows)):
        play_time = datetime.fromisoformat(rows[i]["timestamp"])
        play_duration_ms = rows[i]["played_ms"] or rows[i]["duration_ms"] or 0

        # Calculate gap from end of previous track
        prev_play_duration = rows[i - 1]["played_ms"] or rows[i - 1]["duration_ms"] or 0
        prev_end = datetime.fromisoformat(rows[i - 1]["timestamp"]) + timedelta(
            milliseconds=prev_play_duration
        )
        gap = play_time - prev_end

        if gap > gap_threshold:
            # End current session, start new one
            session_duration = (current_session_end - current_session_start).total_seconds() / 60
            session_duration += (current_session_listening_ms / 1000 / 60)  # Add last track
            sessions.append({
                "start": current_session_start.isoformat(),
                "end": current_session_end.isoformat(),
                "duration_minutes": session_duration,
                "listening_ms": current_session_listening_ms,
            })
            current_session_start = play_time
            current_session_end = play_time
            current_session_listening_ms = play_duration_ms
        else:
            # Continue session
            current_session_end = play_time
            current_session_listening_ms += play_duration_ms

    # Don't forget the last session
    session_duration = (current_session_end - current_session_start).total_seconds() / 60
    # For last session, add duration of final track
    last_track_duration_ms = rows[-1]["played_ms"] or rows[-1]["duration_ms"] or 0
    session_duration += last_track_duration_ms / 1000 / 60
    sessions.append({
        "start": current_session_start.isoformat(),
        "end": current_session_end.isoformat(),
        "duration_minutes": session_duration,
        "listening_ms": current_session_listening_ms,
    })

    # Calculate statistics
    total_sessions = len(sessions)
    total_listening_ms = sum(s["listening_ms"] for s in sessions)
    total_listening_minutes = total_listening_ms / 1000 / 60

    avg_session_length = sum(s["duration_minutes"] for s in sessions) / total_sessions

    longest_session = max(sessions, key=lambda s: s["duration_minutes"])

    return SessionInfo(
        total_sessions=total_sessions,
        avg_session_length_minutes=round(avg_session_length, 2),
        longest_session_minutes=round(longest_session["duration_minutes"], 2),
        longest_session_start=longest_session["start"],
        longest_session_end=longest_session["end"],
        total_listening_minutes=round(total_listening_minutes, 2),
    )


def get_night_owl_score(
    conn: sqlite3.Connection,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> NightOwlScore:
    """Calculate the percentage of listening activity between midnight and 6am.

    This metric indicates how much of a "night owl" the user is based on
    their listening habits.

    Args:
        conn: SQLite database connection with row_factory set.
        start_date: Start of the date range (inclusive). If None, no lower bound.
        end_date: End of the date range (inclusive). If None, no upper bound.

    Returns:
        NightOwlScore dict containing:
            - night_owl_percentage: Percentage of plays between 00:00-06:00
            - night_plays: Number of plays during night hours
            - total_plays: Total number of plays
            - night_listening_minutes: Total listening time during night hours
            - total_listening_minutes: Total listening time overall
    """
    base_query = """
        SELECT
            COUNT(*) as play_count,
            COALESCE(SUM(COALESCE(played_ms, duration_ms, 0)), 0) as total_ms
        FROM plays
        WHERE 1=1
    """
    params = []

    if start_date:
        base_query += " AND timestamp >= ?"
        params.append(start_date.isoformat())
    if end_date:
        base_query += " AND timestamp <= ?"
        params.append(end_date.isoformat())

    # Get total plays
    cursor = conn.execute(base_query, params)
    total_row = cursor.fetchone()
    total_plays = total_row["play_count"]
    total_ms = total_row["total_ms"]

    if total_plays == 0:
        return NightOwlScore(
            night_owl_percentage=0.0,
            night_plays=0,
            total_plays=0,
            night_listening_minutes=0.0,
            total_listening_minutes=0.0,
        )

    # Get night plays (00:00 - 06:00) using local time from hour_of_day column
    night_query = base_query + " AND hour_of_day IS NOT NULL AND hour_of_day < 6"
    cursor = conn.execute(night_query, params)
    night_row = cursor.fetchone()
    night_plays = night_row["play_count"]
    night_ms = night_row["total_ms"]

    night_percentage = (night_plays / total_plays) * 100 if total_plays > 0 else 0

    return NightOwlScore(
        night_owl_percentage=round(night_percentage, 2),
        night_plays=night_plays,
        total_plays=total_plays,
        night_listening_minutes=round(night_ms / 1000 / 60, 2),
        total_listening_minutes=round(total_ms / 1000 / 60, 2),
    )


def get_biggest_listening_day(
    conn: sqlite3.Connection,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> Optional[BiggestDay]:
    """Find the day with the most listening activity.

    Activity is measured by total listening time (played_ms or duration_ms).

    Args:
        conn: SQLite database connection with row_factory set.
        start_date: Start of the date range (inclusive). If None, no lower bound.
        end_date: End of the date range (inclusive). If None, no upper bound.

    Returns:
        BiggestDay dict containing:
            - date: The date with most listening (ISO format)
            - play_count: Number of plays on that day
            - listening_minutes: Total listening time in minutes
            - top_artist: Most played artist on that day
            - top_track: Most played track on that day
        Returns None if no plays found.
    """
    query = """
        SELECT
            DATE(timestamp) as play_date,
            COUNT(*) as play_count,
            SUM(COALESCE(played_ms, duration_ms, 0)) as total_ms
        FROM plays
        WHERE 1=1
    """
    params = []

    if start_date:
        query += " AND timestamp >= ?"
        params.append(start_date.isoformat())
    if end_date:
        query += " AND timestamp <= ?"
        params.append(end_date.isoformat())

    query += " GROUP BY play_date ORDER BY total_ms DESC LIMIT 1"

    cursor = conn.execute(query, params)
    row = cursor.fetchone()

    if not row:
        return None

    biggest_date = row["play_date"]

    # Get top artist for that day
    artist_query = """
        SELECT artist, COUNT(*) as cnt
        FROM plays
        WHERE DATE(timestamp) = ? AND artist IS NOT NULL
        GROUP BY artist
        ORDER BY cnt DESC
        LIMIT 1
    """
    cursor = conn.execute(artist_query, [biggest_date])
    artist_row = cursor.fetchone()
    top_artist = artist_row["artist"] if artist_row else None

    # Get top track for that day
    track_query = """
        SELECT title, artist, COUNT(*) as cnt
        FROM plays
        WHERE DATE(timestamp) = ?
        GROUP BY title, artist
        ORDER BY cnt DESC
        LIMIT 1
    """
    cursor = conn.execute(track_query, [biggest_date])
    track_row = cursor.fetchone()
    top_track = None
    if track_row:
        if track_row["artist"]:
            top_track = f"{track_row['artist']} - {track_row['title']}"
        else:
            top_track = track_row["title"]

    return BiggestDay(
        date=biggest_date,
        play_count=row["play_count"],
        listening_minutes=round(row["total_ms"] / 1000 / 60, 2),
        top_artist=top_artist,
        top_track=top_track,
    )


def get_hourly_heatmap(
    conn: sqlite3.Connection,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> HourlyHeatmap:
    """Generate a 24-hour breakdown of listening activity.

    Returns play counts for each hour of the day (0-23), useful for
    visualizing listening patterns.

    Args:
        conn: SQLite database connection with row_factory set.
        start_date: Start of the date range (inclusive). If None, no lower bound.
        end_date: End of the date range (inclusive). If None, no upper bound.

    Returns:
        HourlyHeatmap dict containing:
            - hours: Dict mapping hour (0-23) to play count
            - peak_hour: Hour with most plays
            - peak_hour_plays: Play count during peak hour
            - quietest_hour: Hour with fewest plays
            - quietest_hour_plays: Play count during quietest hour
    """
    query = """
        SELECT
            hour_of_day as hour,
            COUNT(*) as play_count
        FROM plays
        WHERE hour_of_day IS NOT NULL
    """
    params = []

    if start_date:
        query += " AND timestamp >= ?"
        params.append(start_date.isoformat())
    if end_date:
        query += " AND timestamp <= ?"
        params.append(end_date.isoformat())

    query += " GROUP BY hour_of_day ORDER BY hour_of_day"

    cursor = conn.execute(query, params)
    rows = cursor.fetchall()

    # Initialize all hours to 0
    hours = {h: 0 for h in range(24)}

    for row in rows:
        hours[row["hour"]] = row["play_count"]

    # Find peak and quietest hours
    if any(hours.values()):
        peak_hour = max(hours, key=hours.get)
        peak_hour_plays = hours[peak_hour]
        quietest_hour = min(hours, key=hours.get)
        quietest_hour_plays = hours[quietest_hour]
    else:
        peak_hour = 0
        peak_hour_plays = 0
        quietest_hour = 0
        quietest_hour_plays = 0

    return HourlyHeatmap(
        hours=hours,
        peak_hour=peak_hour,
        peak_hour_plays=peak_hour_plays,
        quietest_hour=quietest_hour,
        quietest_hour_plays=quietest_hour_plays,
    )


# =============================================================================
# Session and Behavior Analytics
# =============================================================================


class ListeningSession(TypedDict):
    """Type definition for a listening session."""
    start_time: str
    end_time: str
    duration_minutes: float
    track_count: int
    artists: list[str]


class AlbumListeningPattern(TypedDict):
    """Type definition for album listening pattern."""
    album: str
    artist: str
    pattern: str  # 'sequential' or 'shuffle'
    sequential_plays: int
    total_plays: int
    sequential_percentage: float


def get_listening_sessions(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    gap_minutes: int = 30,
) -> list[ListeningSession]:
    """Group plays into listening sessions based on time gaps.

    A new session starts when there's a gap of more than `gap_minutes`
    between consecutive plays.

    Args:
        start_date: Start of the date range (inclusive). If None, no lower bound.
        end_date: End of the date range (inclusive). If None, no upper bound.
        gap_minutes: Minutes of inactivity that defines a new session. Default 30.

    Returns:
        List of ListeningSession dicts containing:
            - start_time: ISO format start time of session
            - end_time: ISO format end time of session
            - duration_minutes: Total duration of the session
            - track_count: Number of tracks played in the session
            - artists: List of unique artists played in the session
    """
    conn = get_connection()
    try:
        query = """
            SELECT timestamp, played_ms, duration_ms, artist
            FROM plays
            WHERE 1=1
        """
        params = []

        if start_date:
            query += " AND timestamp >= ?"
            params.append(start_date.isoformat())
        if end_date:
            query += " AND timestamp <= ?"
            params.append(end_date.isoformat())

        query += " ORDER BY timestamp ASC"

        cursor = conn.execute(query, params)
        rows = cursor.fetchall()

        if not rows:
            return []

        gap_threshold = timedelta(minutes=gap_minutes)
        sessions: list[ListeningSession] = []

        # Start first session
        current_session_start = datetime.fromisoformat(rows[0]["timestamp"])
        current_session_end = current_session_start
        current_session_tracks = 1
        current_session_artists: set[str] = set()
        if rows[0]["artist"]:
            current_session_artists.add(rows[0]["artist"])
        current_session_listening_ms = rows[0]["played_ms"] or rows[0]["duration_ms"] or 0

        for i in range(1, len(rows)):
            play_time = datetime.fromisoformat(rows[i]["timestamp"])
            play_duration_ms = rows[i]["played_ms"] or rows[i]["duration_ms"] or 0

            # Calculate gap from end of previous track
            prev_play_duration = rows[i - 1]["played_ms"] or rows[i - 1]["duration_ms"] or 0
            prev_end = datetime.fromisoformat(rows[i - 1]["timestamp"]) + timedelta(
                milliseconds=prev_play_duration
            )
            gap = play_time - prev_end

            if gap > gap_threshold:
                # End current session, start new one
                session_duration = (current_session_end - current_session_start).total_seconds() / 60
                # Add duration of last track in session
                last_track_ms = rows[i - 1]["played_ms"] or rows[i - 1]["duration_ms"] or 0
                session_duration += last_track_ms / 1000 / 60

                sessions.append(ListeningSession(
                    start_time=current_session_start.isoformat(),
                    end_time=current_session_end.isoformat(),
                    duration_minutes=round(session_duration, 2),
                    track_count=current_session_tracks,
                    artists=sorted(list(current_session_artists)),
                ))

                # Start new session
                current_session_start = play_time
                current_session_end = play_time
                current_session_tracks = 1
                current_session_artists = set()
                if rows[i]["artist"]:
                    current_session_artists.add(rows[i]["artist"])
                current_session_listening_ms = play_duration_ms
            else:
                # Continue session
                current_session_end = play_time
                current_session_tracks += 1
                if rows[i]["artist"]:
                    current_session_artists.add(rows[i]["artist"])
                current_session_listening_ms += play_duration_ms

        # Don't forget the last session
        session_duration = (current_session_end - current_session_start).total_seconds() / 60
        last_track_ms = rows[-1]["played_ms"] or rows[-1]["duration_ms"] or 0
        session_duration += last_track_ms / 1000 / 60

        sessions.append(ListeningSession(
            start_time=current_session_start.isoformat(),
            end_time=current_session_end.isoformat(),
            duration_minutes=round(session_duration, 2),
            track_count=current_session_tracks,
            artists=sorted(list(current_session_artists)),
        ))

        return sessions
    finally:
        conn.close()


def get_behavior_skip_rate(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> dict:
    """Calculate skip rate: percentage of tracks where played_ms < 50% of duration_ms.

    Args:
        start_date: Start of the date range (inclusive). If None, no lower bound.
        end_date: End of the date range (inclusive). If None, no upper bound.

    Returns:
        dict with keys:
            - skip_rate: Percentage of tracks skipped (played < 50%)
            - total_plays: Total number of plays with valid duration data
            - skipped_plays: Number of plays that were skipped
            - most_skipped_tracks: List of most frequently skipped tracks
    """
    conn = get_connection()
    try:
        query = """
            SELECT title, artist, played_ms, duration_ms
            FROM plays
            WHERE played_ms IS NOT NULL AND duration_ms IS NOT NULL AND duration_ms > 0
        """
        params = []

        if start_date:
            query += " AND timestamp >= ?"
            params.append(start_date.isoformat())
        if end_date:
            query += " AND timestamp <= ?"
            params.append(end_date.isoformat())

        cursor = conn.execute(query, params)
        rows = cursor.fetchall()

        if not rows:
            return {
                "skip_rate": 0.0,
                "total_plays": 0,
                "skipped_plays": 0,
                "most_skipped_tracks": [],
            }

        skipped_count = 0
        skip_counts: dict[tuple[str, str], int] = defaultdict(int)

        for row in rows:
            completion_ratio = row["played_ms"] / row["duration_ms"]
            if completion_ratio < 0.5:
                skipped_count += 1
                key = (row["title"] or "Unknown", row["artist"] or "Unknown")
                skip_counts[key] += 1

        total_plays = len(rows)
        skip_rate = (skipped_count / total_plays) * 100 if total_plays > 0 else 0

        # Get most skipped tracks
        most_skipped = sorted(
            [{"title": k[0], "artist": k[1], "skip_count": v} for k, v in skip_counts.items()],
            key=lambda x: x["skip_count"],
            reverse=True,
        )[:10]

        return {
            "skip_rate": round(skip_rate, 2),
            "total_plays": total_plays,
            "skipped_plays": skipped_count,
            "most_skipped_tracks": most_skipped,
        }
    finally:
        conn.close()


def get_completion_rate(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> dict:
    """Calculate average percentage of tracks completed.

    Args:
        start_date: Start of the date range (inclusive). If None, no lower bound.
        end_date: End of the date range (inclusive). If None, no upper bound.

    Returns:
        dict with keys:
            - average_completion: Average percentage of track completed
            - total_plays: Total number of plays with valid duration data
            - full_completions: Number of plays where >= 90% was played
            - partial_plays: Number of plays where < 90% was played
    """
    conn = get_connection()
    try:
        query = """
            SELECT played_ms, duration_ms
            FROM plays
            WHERE played_ms IS NOT NULL AND duration_ms IS NOT NULL AND duration_ms > 0
        """
        params = []

        if start_date:
            query += " AND timestamp >= ?"
            params.append(start_date.isoformat())
        if end_date:
            query += " AND timestamp <= ?"
            params.append(end_date.isoformat())

        cursor = conn.execute(query, params)
        rows = cursor.fetchall()

        if not rows:
            return {
                "average_completion": 0.0,
                "total_plays": 0,
                "full_completions": 0,
                "partial_plays": 0,
            }

        completion_percentages = []
        full_completions = 0
        partial_plays = 0

        for row in rows:
            ratio = min(row["played_ms"] / row["duration_ms"], 1.0)
            completion_percentages.append(ratio * 100)
            if ratio >= 0.9:
                full_completions += 1
            else:
                partial_plays += 1

        avg_completion = sum(completion_percentages) / len(completion_percentages)

        return {
            "average_completion": round(avg_completion, 2),
            "total_plays": len(rows),
            "full_completions": full_completions,
            "partial_plays": partial_plays,
        }
    finally:
        conn.close()


def get_repeat_plays(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> dict:
    """Find tracks played multiple times in the same session.

    A session is defined by a 30-minute gap between plays.

    Args:
        start_date: Start of the date range (inclusive). If None, no lower bound.
        end_date: End of the date range (inclusive). If None, no upper bound.

    Returns:
        dict with keys:
            - repeat_tracks: List of tracks repeated within sessions
            - total_repeats: Total number of repeat plays
            - sessions_with_repeats: Number of sessions containing repeats
    """
    # Get all sessions first
    sessions = get_listening_sessions(start_date, end_date)

    conn = get_connection()
    try:
        repeat_tracks: dict[tuple[str, str], dict] = defaultdict(
            lambda: {"repeat_count": 0, "sessions_with_repeats": 0}
        )
        total_repeats = 0
        sessions_with_repeats = 0

        for session in sessions:
            session_start = session["start_time"]
            session_end = session["end_time"]

            # Get tracks in this session
            query = """
                SELECT title, artist, COUNT(*) as play_count
                FROM plays
                WHERE timestamp >= ? AND timestamp <= ?
                GROUP BY title, artist
                HAVING COUNT(*) > 1
            """
            cursor = conn.execute(query, [session_start, session_end])
            repeated_in_session = cursor.fetchall()

            if repeated_in_session:
                sessions_with_repeats += 1
                for row in repeated_in_session:
                    key = (row["title"] or "Unknown", row["artist"] or "Unknown")
                    repeats = row["play_count"] - 1  # First play isn't a repeat
                    repeat_tracks[key]["repeat_count"] += repeats
                    repeat_tracks[key]["sessions_with_repeats"] += 1
                    total_repeats += repeats

        # Format results
        repeat_list = sorted(
            [
                {
                    "title": k[0],
                    "artist": k[1],
                    "repeat_count": v["repeat_count"],
                    "sessions_with_repeats": v["sessions_with_repeats"],
                }
                for k, v in repeat_tracks.items()
            ],
            key=lambda x: x["repeat_count"],
            reverse=True,
        )

        return {
            "repeat_tracks": repeat_list[:20],
            "total_repeats": total_repeats,
            "sessions_with_repeats": sessions_with_repeats,
        }
    finally:
        conn.close()


def get_behavior_discovery_rate(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> dict:
    """Calculate percentage of plays that are first-time artists.

    An artist is considered "first-time" if this is their first play ever
    (not just in the date range).

    Args:
        start_date: Start of the date range (inclusive). If None, no lower bound.
        end_date: End of the date range (inclusive). If None, no upper bound.

    Returns:
        dict with keys:
            - discovery_rate: Percentage of plays that are first-time artists
            - total_plays: Total number of plays in the range
            - first_time_plays: Number of plays that were first-time artists
            - new_artists: List of newly discovered artists
    """
    conn = get_connection()
    try:
        # Build query for plays in the date range
        query = """
            SELECT timestamp, artist
            FROM plays
            WHERE artist IS NOT NULL
        """
        params = []

        if start_date:
            query += " AND timestamp >= ?"
            params.append(start_date.isoformat())
        if end_date:
            query += " AND timestamp <= ?"
            params.append(end_date.isoformat())

        query += " ORDER BY timestamp ASC"

        cursor = conn.execute(query, params)
        rows = cursor.fetchall()

        if not rows:
            return {
                "discovery_rate": 0.0,
                "total_plays": 0,
                "first_time_plays": 0,
                "new_artists": [],
            }

        # For each play, check if the artist had any plays before this one
        first_time_plays = 0
        new_artists: list[dict] = []
        seen_new_artists: set[str] = set()

        for row in rows:
            artist = row["artist"]
            play_time = row["timestamp"]

            # Check if this artist has any plays before this timestamp
            check_query = """
                SELECT COUNT(*) as count FROM plays
                WHERE artist = ? AND timestamp < ?
            """
            check_cursor = conn.execute(check_query, [artist, play_time])
            previous_plays = check_cursor.fetchone()["count"]

            if previous_plays == 0:
                first_time_plays += 1
                if artist not in seen_new_artists:
                    seen_new_artists.add(artist)
                    new_artists.append({
                        "artist": artist,
                        "first_play": play_time,
                    })

        total_plays = len(rows)
        discovery_rate = (first_time_plays / total_plays) * 100 if total_plays > 0 else 0

        return {
            "discovery_rate": round(discovery_rate, 2),
            "total_plays": total_plays,
            "first_time_plays": first_time_plays,
            "new_artists": new_artists,
        }
    finally:
        conn.close()


def get_album_listening_patterns(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> dict:
    """Detect sequential album plays vs shuffle listening patterns.

    Sequential listening is defined as playing tracks from the same album
    in order (based on track numbers if available, or consecutive timestamps).

    Args:
        start_date: Start of the date range (inclusive). If None, no lower bound.
        end_date: End of the date range (inclusive). If None, no upper bound.

    Returns:
        dict with keys:
            - albums: List of AlbumListeningPattern dicts
            - overall_sequential_rate: Percentage of album plays that were sequential
            - sequential_albums: Number of albums listened to sequentially
            - shuffle_albums: Number of albums listened to in shuffle mode
    """
    conn = get_connection()
    try:
        # Get all plays with album info, ordered by timestamp
        query = """
            SELECT timestamp, title, artist, album, track_number
            FROM plays
            WHERE album IS NOT NULL AND album != ''
        """
        params = []

        if start_date:
            query += " AND timestamp >= ?"
            params.append(start_date.isoformat())
        if end_date:
            query += " AND timestamp <= ?"
            params.append(end_date.isoformat())

        query += " ORDER BY timestamp ASC"

        cursor = conn.execute(query, params)
        rows = cursor.fetchall()

        if not rows:
            return {
                "albums": [],
                "overall_sequential_rate": 0.0,
                "sequential_albums": 0,
                "shuffle_albums": 0,
            }

        # Group plays by album
        album_plays: dict[tuple[str, str], list] = defaultdict(list)
        for row in rows:
            key = (row["album"], row["artist"] or "Unknown")
            album_plays[key].append({
                "timestamp": row["timestamp"],
                "title": row["title"],
                "track_number": row["track_number"],
            })

        albums: list[AlbumListeningPattern] = []
        sequential_albums = 0
        shuffle_albums = 0

        for (album, artist), plays in album_plays.items():
            if len(plays) < 2:
                continue

            # Count sequential plays (consecutive plays from same album)
            sequential_count = 0
            for i in range(1, len(plays)):
                prev_time = datetime.fromisoformat(plays[i - 1]["timestamp"])
                curr_time = datetime.fromisoformat(plays[i]["timestamp"])

                # Check if plays are within 15 minutes (likely same listening session)
                time_diff = (curr_time - prev_time).total_seconds() / 60
                if time_diff <= 15:
                    # Check track number order if available
                    prev_track = plays[i - 1]["track_number"]
                    curr_track = plays[i]["track_number"]

                    if prev_track is not None and curr_track is not None:
                        if curr_track == prev_track + 1:
                            sequential_count += 1
                    else:
                        # Without track numbers, consider consecutive plays as sequential
                        sequential_count += 1

            total_plays = len(plays)
            sequential_pct = (sequential_count / (total_plays - 1)) * 100 if total_plays > 1 else 0

            # Determine pattern
            pattern = "sequential" if sequential_pct >= 50 else "shuffle"
            if pattern == "sequential":
                sequential_albums += 1
            else:
                shuffle_albums += 1

            albums.append(AlbumListeningPattern(
                album=album,
                artist=artist,
                pattern=pattern,
                sequential_plays=sequential_count,
                total_plays=total_plays,
                sequential_percentage=round(sequential_pct, 2),
            ))

        # Sort by total plays descending
        albums.sort(key=lambda x: x["total_plays"], reverse=True)

        total_albums = sequential_albums + shuffle_albums
        overall_sequential_rate = (sequential_albums / total_albums) * 100 if total_albums > 0 else 0

        return {
            "albums": albums[:20],
            "overall_sequential_rate": round(overall_sequential_rate, 2),
            "sequential_albums": sequential_albums,
            "shuffle_albums": shuffle_albums,
        }
    finally:
        conn.close()


# Convenience function to run all analytics
def get_time_analytics(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> dict:
    """Run all time-based analytics and return combined results.

    This is a convenience function that runs all analytics functions
    and returns their results in a single dictionary.

    Args:
        start_date: Start of the date range (inclusive). If None, no lower bound.
        end_date: End of the date range (inclusive). If None, no upper bound.

    Returns:
        Dictionary containing results from all analytics functions.
    """
    conn = get_connection()
    try:
        return {
            "streaks": get_listening_streaks(conn, start_date, end_date),
            "sessions": get_sessions(conn, start_date, end_date),
            "night_owl": get_night_owl_score(conn, start_date, end_date),
            "biggest_day": get_biggest_listening_day(conn, start_date, end_date),
            "hourly_heatmap": get_hourly_heatmap(conn, start_date, end_date),
        }
    finally:
        conn.close()


# =============================================================================
# Track Behavior Analytics
# =============================================================================


def get_skip_rate(
    conn: sqlite3.Connection,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> dict:
    """
    Calculate skip rate for tracks within a date range.

    A track is considered "skipped" if played_ms < 30000 (30 seconds).

    Args:
        conn: Database connection with row_factory set.
        start_date: Start of the date range (inclusive).
        end_date: End of the date range (inclusive).

    Returns:
        dict with keys:
            - skip_percentage: Percentage of plays that were skips.
            - total_plays: Total number of plays in the range.
            - total_skips: Number of skipped plays.
            - most_skipped: List of (title, artist, skip_count) tuples,
                            sorted by skip count descending.
    """
    query = """
        SELECT title, artist, played_ms
        FROM plays
        WHERE played_ms IS NOT NULL
    """
    params = []

    if start_date:
        query += " AND timestamp >= ?"
        params.append(start_date.isoformat())
    if end_date:
        query += " AND timestamp <= ?"
        params.append(end_date.isoformat())

    cursor = conn.execute(query, params)
    rows = cursor.fetchall()

    total_plays = len(rows)
    if total_plays == 0:
        return {
            "skip_percentage": 0.0,
            "total_plays": 0,
            "total_skips": 0,
            "most_skipped": [],
        }

    skip_counts = defaultdict(int)
    total_skips = 0

    for row in rows:
        if row["played_ms"] < 30000:
            total_skips += 1
            key = (row["title"], row["artist"])
            skip_counts[key] += 1

    skip_percentage = (total_skips / total_plays) * 100

    # Sort by skip count descending
    most_skipped = sorted(
        [(title, artist, count) for (title, artist), count in skip_counts.items()],
        key=lambda x: x[2],
        reverse=True,
    )

    return {
        "skip_percentage": round(skip_percentage, 2),
        "total_plays": total_plays,
        "total_skips": total_skips,
        "most_skipped": most_skipped,
    }


def get_repeat_obsessions(
    conn: sqlite3.Connection,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> dict:
    """
    Find tracks played multiple times in short periods (same day).

    Identifies "obsession" tracks that users replay within the same day.

    Args:
        conn: Database connection with row_factory set.
        start_date: Start of the date range (inclusive).
        end_date: End of the date range (inclusive).

    Returns:
        dict with keys:
            - most_repeated: List of dicts with title, artist, total_repeats,
                             and days_with_repeats, sorted by total repeats.
            - days_with_obsessions: Number of days where any track was repeated.
    """
    query = """
        SELECT title, artist, DATE(timestamp) as play_date, COUNT(*) as daily_count
        FROM plays
        WHERE 1=1
    """
    params = []

    if start_date:
        query += " AND timestamp >= ?"
        params.append(start_date.isoformat())
    if end_date:
        query += " AND timestamp <= ?"
        params.append(end_date.isoformat())

    query += " GROUP BY title, artist, DATE(timestamp) HAVING COUNT(*) > 1"

    cursor = conn.execute(query, params)
    rows = cursor.fetchall()

    # Aggregate repeats per track
    track_repeats = defaultdict(lambda: {"total_repeats": 0, "days_with_repeats": 0})
    days_with_obsessions = set()

    for row in rows:
        key = (row["title"], row["artist"])
        # Repeat count is plays minus 1 (first play isn't a repeat)
        repeat_count = row["daily_count"] - 1
        track_repeats[key]["total_repeats"] += repeat_count
        track_repeats[key]["days_with_repeats"] += 1
        days_with_obsessions.add(row["play_date"])

    most_repeated = sorted(
        [
            {
                "title": title,
                "artist": artist,
                "total_repeats": data["total_repeats"],
                "days_with_repeats": data["days_with_repeats"],
            }
            for (title, artist), data in track_repeats.items()
        ],
        key=lambda x: x["total_repeats"],
        reverse=True,
    )

    return {
        "most_repeated": most_repeated,
        "days_with_obsessions": len(days_with_obsessions),
    }


def get_album_completion(
    conn: sqlite3.Connection,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> dict:
    """
    Calculate album completion rates based on unique tracks played.

    For each album, determines the percentage of unique tracks played
    vs the total unique tracks seen for that album in the database.

    Args:
        conn: Database connection with row_factory set.
        start_date: Start of the date range (inclusive).
        end_date: End of the date range (inclusive).

    Returns:
        dict with keys:
            - albums: List of dicts with album, artist, tracks_played,
                      total_tracks, and completion_percentage.
            - average_completion: Average completion percentage across all albums.
    """
    # First, get total unique tracks per album (from all time)
    total_tracks_query = """
        SELECT album, artist, COUNT(DISTINCT title) as total_tracks
        FROM plays
        WHERE album IS NOT NULL AND album != ''
        GROUP BY album, artist
    """
    cursor = conn.execute(total_tracks_query)
    album_totals = {
        (row["album"], row["artist"]): row["total_tracks"]
        for row in cursor.fetchall()
    }

    # Now get unique tracks played in the date range
    range_query = """
        SELECT album, artist, COUNT(DISTINCT title) as tracks_played
        FROM plays
        WHERE album IS NOT NULL AND album != ''
    """
    params = []

    if start_date:
        range_query += " AND timestamp >= ?"
        params.append(start_date.isoformat())
    if end_date:
        range_query += " AND timestamp <= ?"
        params.append(end_date.isoformat())

    range_query += " GROUP BY album, artist"

    cursor = conn.execute(range_query, params)
    rows = cursor.fetchall()

    albums = []
    for row in rows:
        key = (row["album"], row["artist"])
        total_tracks = album_totals.get(key, row["tracks_played"])
        completion = (row["tracks_played"] / total_tracks * 100) if total_tracks > 0 else 0

        albums.append({
            "album": row["album"],
            "artist": row["artist"],
            "tracks_played": row["tracks_played"],
            "total_tracks": total_tracks,
            "completion_percentage": round(completion, 2),
        })

    # Sort by completion percentage descending
    albums.sort(key=lambda x: x["completion_percentage"], reverse=True)

    average_completion = (
        sum(a["completion_percentage"] for a in albums) / len(albums)
        if albums else 0
    )

    return {
        "albums": albums,
        "average_completion": round(average_completion, 2),
    }


def get_average_track_length(
    conn: sqlite3.Connection,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> dict:
    """
    Calculate the average duration of played tracks.

    Args:
        conn: Database connection with row_factory set.
        start_date: Start of the date range (inclusive).
        end_date: End of the date range (inclusive).

    Returns:
        dict with keys:
            - average_duration_ms: Average track duration in milliseconds.
            - average_duration_formatted: Human-readable format (MM:SS).
            - total_tracks: Number of tracks with duration data.
            - shortest_track: Dict with title, artist, duration_ms of shortest.
            - longest_track: Dict with title, artist, duration_ms of longest.
    """
    query = """
        SELECT title, artist, duration_ms
        FROM plays
        WHERE duration_ms IS NOT NULL AND duration_ms > 0
    """
    params = []

    if start_date:
        query += " AND timestamp >= ?"
        params.append(start_date.isoformat())
    if end_date:
        query += " AND timestamp <= ?"
        params.append(end_date.isoformat())

    cursor = conn.execute(query, params)
    rows = cursor.fetchall()

    if not rows:
        return {
            "average_duration_ms": 0,
            "average_duration_formatted": "0:00",
            "total_tracks": 0,
            "shortest_track": None,
            "longest_track": None,
        }

    durations = [(row["title"], row["artist"], row["duration_ms"]) for row in rows]
    total_duration = sum(d[2] for d in durations)
    avg_duration = total_duration / len(durations)

    # Find shortest and longest
    shortest = min(durations, key=lambda x: x[2])
    longest = max(durations, key=lambda x: x[2])

    # Format average duration as MM:SS
    avg_seconds = int(avg_duration / 1000)
    formatted = f"{avg_seconds // 60}:{avg_seconds % 60:02d}"

    return {
        "average_duration_ms": round(avg_duration, 2),
        "average_duration_formatted": formatted,
        "total_tracks": len(durations),
        "shortest_track": {
            "title": shortest[0],
            "artist": shortest[1],
            "duration_ms": shortest[2],
        },
        "longest_track": {
            "title": longest[0],
            "artist": longest[1],
            "duration_ms": longest[2],
        },
    }


def get_full_listens_vs_partial(
    conn: sqlite3.Connection,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> dict:
    """
    Compare full listens (played_ms >= 90% of duration_ms) vs partial listens.

    Args:
        conn: Database connection with row_factory set.
        start_date: Start of the date range (inclusive).
        end_date: End of the date range (inclusive).

    Returns:
        dict with keys:
            - full_listens: Number of plays with >= 90% completion.
            - partial_listens: Number of plays with < 90% completion.
            - full_listen_percentage: Percentage of full listens.
            - partial_listen_percentage: Percentage of partial listens.
            - total_plays: Total plays with valid duration data.
            - average_completion_percentage: Average played_ms/duration_ms ratio.
    """
    query = """
        SELECT title, artist, duration_ms, played_ms
        FROM plays
        WHERE duration_ms IS NOT NULL AND duration_ms > 0
          AND played_ms IS NOT NULL
    """
    params = []

    if start_date:
        query += " AND timestamp >= ?"
        params.append(start_date.isoformat())
    if end_date:
        query += " AND timestamp <= ?"
        params.append(end_date.isoformat())

    cursor = conn.execute(query, params)
    rows = cursor.fetchall()

    if not rows:
        return {
            "full_listens": 0,
            "partial_listens": 0,
            "full_listen_percentage": 0.0,
            "partial_listen_percentage": 0.0,
            "total_plays": 0,
            "average_completion_percentage": 0.0,
        }

    full_listens = 0
    partial_listens = 0
    completion_ratios = []

    for row in rows:
        ratio = row["played_ms"] / row["duration_ms"]
        completion_ratios.append(min(ratio, 1.0) * 100)  # Cap at 100%

        if ratio >= 0.9:
            full_listens += 1
        else:
            partial_listens += 1

    total = len(rows)
    avg_completion = sum(completion_ratios) / len(completion_ratios)

    return {
        "full_listens": full_listens,
        "partial_listens": partial_listens,
        "full_listen_percentage": round((full_listens / total) * 100, 2),
        "partial_listen_percentage": round((partial_listens / total) * 100, 2),
        "total_plays": total,
        "average_completion_percentage": round(avg_completion, 2),
    }


# Convenience function to run all track behavior analytics
def get_track_behavior_analytics(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> dict:
    """Run all track behavior analytics and return combined results.

    This is a convenience function that runs all track behavior analytics
    functions and returns their results in a single dictionary.

    Args:
        start_date: Start of the date range (inclusive). If None, no lower bound.
        end_date: End of the date range (inclusive). If None, no upper bound.

    Returns:
        Dictionary containing results from all track behavior analytics functions.
    """
    conn = get_connection()
    try:
        return {
            "skip_rate": get_skip_rate(conn, start_date, end_date),
            "repeat_obsessions": get_repeat_obsessions(conn, start_date, end_date),
            "album_completion": get_album_completion(conn, start_date, end_date),
            "average_track_length": get_average_track_length(conn, start_date, end_date),
            "full_vs_partial": get_full_listens_vs_partial(conn, start_date, end_date),
        }
    finally:
        conn.close()


# =============================================================================
# Artist Insight Analytics
# =============================================================================


def get_discovery_rate(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
) -> dict:
    """Calculate % of artists that are "new" (first play ever in this period) vs returning.

    Args:
        conn: Database connection with row_factory set.
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.

    Returns:
        Dict with discovery statistics:
        {
            "total_artists": int,
            "new_artists": int,
            "returning_artists": int,
            "discovery_rate": float (percentage),
            "new_artist_list": list of artist names,
            "returning_artist_list": list of artist names
        }
    """
    # Get all unique artists played in the period
    cursor = conn.execute(
        """
        SELECT DISTINCT artist
        FROM plays
        WHERE timestamp >= ? AND timestamp <= ? AND artist IS NOT NULL
        """,
        (start_date, end_date),
    )
    artists_in_period = {row["artist"] for row in cursor.fetchall()}

    if not artists_in_period:
        return {
            "total_artists": 0,
            "new_artists": 0,
            "returning_artists": 0,
            "discovery_rate": 0.0,
            "new_artist_list": [],
            "returning_artist_list": [],
        }

    # For each artist, check if they had any plays before the period
    new_artists = []
    returning_artists = []

    for artist in artists_in_period:
        cursor = conn.execute(
            """
            SELECT COUNT(*) as count
            FROM plays
            WHERE artist = ? AND timestamp < ?
            """,
            (artist, start_date),
        )
        plays_before = cursor.fetchone()["count"]

        if plays_before == 0:
            new_artists.append(artist)
        else:
            returning_artists.append(artist)

    total = len(artists_in_period)
    discovery_rate = (len(new_artists) / total * 100) if total > 0 else 0.0

    return {
        "total_artists": total,
        "new_artists": len(new_artists),
        "returning_artists": len(returning_artists),
        "discovery_rate": round(discovery_rate, 2),
        "new_artist_list": sorted(new_artists),
        "returning_artist_list": sorted(returning_artists),
    }


def get_artist_loyalty(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
    top_n: int = 10,
) -> list[dict]:
    """For top artists in the period, find their earliest play date to show listening history.

    Args:
        conn: Database connection with row_factory set.
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        top_n: Number of top artists to analyze.

    Returns:
        List of dicts:
        [
            {
                "artist": str,
                "plays_in_period": int,
                "first_ever_play": str (datetime),
                "days_listening": int,
                "total_plays_ever": int
            },
            ...
        ]
    """
    # Get top artists by play count in the period
    cursor = conn.execute(
        """
        SELECT artist, COUNT(*) as play_count
        FROM plays
        WHERE timestamp >= ? AND timestamp <= ? AND artist IS NOT NULL
        GROUP BY artist
        ORDER BY play_count DESC
        LIMIT ?
        """,
        (start_date, end_date, top_n),
    )
    top_artists = cursor.fetchall()

    results = []
    today = datetime.now()

    for row in top_artists:
        artist = row["artist"]
        plays_in_period = row["play_count"]

        # Find first ever play of this artist
        cursor = conn.execute(
            """
            SELECT MIN(timestamp) as first_play, COUNT(*) as total_plays
            FROM plays
            WHERE artist = ?
            """,
            (artist,),
        )
        stats = cursor.fetchone()
        first_play_str = stats["first_play"]
        total_plays = stats["total_plays"]

        # Calculate days since first listen
        if first_play_str:
            try:
                first_play = datetime.fromisoformat(
                    first_play_str.replace("Z", "+00:00")
                )
                days_listening = (today - first_play.replace(tzinfo=None)).days
            except (ValueError, AttributeError):
                days_listening = 0
        else:
            days_listening = 0

        results.append({
            "artist": artist,
            "plays_in_period": plays_in_period,
            "first_ever_play": first_play_str,
            "days_listening": days_listening,
            "total_plays_ever": total_plays,
        })

    return results


def get_one_hit_wonders(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
) -> list[dict]:
    """Find artists with only 1 play ever (not just in the period).

    Args:
        conn: Database connection with row_factory set.
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.

    Returns:
        List of dicts:
        [
            {
                "artist": str,
                "title": str,
                "album": str or None,
                "played_on": str (datetime)
            },
            ...
        ]
    """
    # Get artists played in the period who have exactly 1 play ever
    cursor = conn.execute(
        """
        SELECT p.artist, p.title, p.album, p.timestamp as played_on
        FROM plays p
        WHERE p.artist IS NOT NULL
          AND p.timestamp >= ? AND p.timestamp <= ?
          AND p.artist IN (
              SELECT artist
              FROM plays
              WHERE artist IS NOT NULL
              GROUP BY artist
              HAVING COUNT(*) = 1
          )
        ORDER BY p.artist
        """,
        (start_date, end_date),
    )

    results = []
    for row in cursor.fetchall():
        results.append({
            "artist": row["artist"],
            "title": row["title"],
            "album": row["album"],
            "played_on": row["played_on"],
        })

    return results


def get_monthly_top_artists(
    conn: sqlite3.Connection,
    year: int,
) -> list[dict]:
    """Return top artist for each month to show taste evolution over a year.

    Args:
        conn: Database connection with row_factory set.
        year: Year to analyze (e.g., 2024).

    Returns:
        List of dicts:
        [
            {
                "month": int (1-12),
                "month_name": str,
                "top_artist": str or None,
                "play_count": int,
                "runner_up": str or None,
                "runner_up_count": int or None
            },
            ...
        ]
    """
    month_names = [
        "",
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]

    results = []

    for month in range(1, 13):
        start_date = f"{year}-{month:02d}-01"
        if month == 12:
            end_date = f"{year + 1}-01-01"
        else:
            end_date = f"{year}-{month + 1:02d}-01"

        # Get top 2 artists for this month
        cursor = conn.execute(
            """
            SELECT artist, COUNT(*) as play_count
            FROM plays
            WHERE timestamp >= ? AND timestamp < ? AND artist IS NOT NULL
            GROUP BY artist
            ORDER BY play_count DESC
            LIMIT 2
            """,
            (start_date, end_date),
        )
        top_artists = cursor.fetchall()

        if not top_artists:
            results.append({
                "month": month,
                "month_name": month_names[month],
                "top_artist": None,
                "play_count": 0,
                "runner_up": None,
                "runner_up_count": None,
            })
        else:
            result = {
                "month": month,
                "month_name": month_names[month],
                "top_artist": top_artists[0]["artist"],
                "play_count": top_artists[0]["play_count"],
                "runner_up": None,
                "runner_up_count": None,
            }
            if len(top_artists) > 1:
                result["runner_up"] = top_artists[1]["artist"]
                result["runner_up_count"] = top_artists[1]["play_count"]
            results.append(result)

    return results


def get_artist_deep_cuts(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
    min_artist_plays: int = 10,
    max_track_plays: int = 2,
    top_artists: int = 10,
) -> list[dict]:
    """For artists with many plays, find their least-played tracks (deep cuts).

    Args:
        conn: Database connection with row_factory set.
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        min_artist_plays: Minimum plays an artist needs to be considered.
        max_track_plays: Maximum plays a track can have to be a "deep cut".
        top_artists: Number of top artists to analyze.

    Returns:
        List of dicts:
        [
            {
                "artist": str,
                "total_plays": int,
                "unique_tracks": int,
                "deep_cuts": [
                    {
                        "title": str,
                        "album": str or None,
                        "play_count": int,
                        "first_played": str (datetime)
                    },
                    ...
                ]
            },
            ...
        ]
    """
    # Get top artists with enough plays in the period
    cursor = conn.execute(
        """
        SELECT artist, COUNT(*) as total_plays, COUNT(DISTINCT title) as unique_tracks
        FROM plays
        WHERE timestamp >= ? AND timestamp <= ? AND artist IS NOT NULL
        GROUP BY artist
        HAVING total_plays >= ?
        ORDER BY total_plays DESC
        LIMIT ?
        """,
        (start_date, end_date, min_artist_plays, top_artists),
    )
    artists = cursor.fetchall()

    results = []

    for artist_row in artists:
        artist = artist_row["artist"]

        # Find deep cuts: tracks with few plays ever (not just in period)
        cursor = conn.execute(
            """
            SELECT title, album, COUNT(*) as play_count, MIN(timestamp) as first_played
            FROM plays
            WHERE artist = ? AND title IS NOT NULL
            GROUP BY title
            HAVING play_count <= ?
            ORDER BY play_count ASC, first_played DESC
            LIMIT 5
            """,
            (artist, max_track_plays),
        )
        deep_cuts = cursor.fetchall()

        results.append({
            "artist": artist,
            "total_plays": artist_row["total_plays"],
            "unique_tracks": artist_row["unique_tracks"],
            "deep_cuts": [
                {
                    "title": row["title"],
                    "album": row["album"],
                    "play_count": row["play_count"],
                    "first_played": row["first_played"],
                }
                for row in deep_cuts
            ],
        })

    return results


# =============================================================================
# Artist Insight Convenience Wrappers
# =============================================================================


def discovery_rate(start_date: str, end_date: str) -> dict:
    """Convenience wrapper for get_discovery_rate."""
    conn = get_connection()
    try:
        return get_discovery_rate(conn, start_date, end_date)
    finally:
        conn.close()


def artist_loyalty(start_date: str, end_date: str, top_n: int = 10) -> list[dict]:
    """Convenience wrapper for get_artist_loyalty."""
    conn = get_connection()
    try:
        return get_artist_loyalty(conn, start_date, end_date, top_n)
    finally:
        conn.close()


def one_hit_wonders(start_date: str, end_date: str) -> list[dict]:
    """Convenience wrapper for get_one_hit_wonders."""
    conn = get_connection()
    try:
        return get_one_hit_wonders(conn, start_date, end_date)
    finally:
        conn.close()


def monthly_top_artists(year: int) -> list[dict]:
    """Convenience wrapper for get_monthly_top_artists."""
    conn = get_connection()
    try:
        return get_monthly_top_artists(conn, year)
    finally:
        conn.close()


def artist_deep_cuts(
    start_date: str,
    end_date: str,
    min_artist_plays: int = 10,
    max_track_plays: int = 2,
    top_artists: int = 10,
) -> list[dict]:
    """Convenience wrapper for get_artist_deep_cuts."""
    conn = get_connection()
    try:
        return get_artist_deep_cuts(
            conn, start_date, end_date, min_artist_plays, max_track_plays, top_artists
        )
    finally:
        conn.close()


def get_artist_analytics(
    start_date: str,
    end_date: str,
    year: Optional[int] = None,
) -> dict:
    """Run all artist analytics and return combined results.

    Args:
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        year: Year for monthly top artists (defaults to year from start_date).

    Returns:
        Dictionary containing results from all artist analytics functions.
    """
    if year is None:
        year = int(start_date[:4])

    conn = get_connection()
    try:
        return {
            "discovery_rate": get_discovery_rate(conn, start_date, end_date),
            "artist_loyalty": get_artist_loyalty(conn, start_date, end_date),
            "one_hit_wonders": get_one_hit_wonders(conn, start_date, end_date),
            "monthly_top_artists": get_monthly_top_artists(conn, year),
            "artist_deep_cuts": get_artist_deep_cuts(conn, start_date, end_date),
        }
    finally:
        conn.close()


# =============================================================================
# MILESTONES & ACHIEVEMENTS
# =============================================================================

def get_milestones(conn: sqlite3.Connection) -> list[dict]:
    """
    Check for achieved milestones/achievements.

    Args:
        conn: SQLite database connection with row_factory set.

    Returns a list of dictionaries with:
        - name: The milestone name
        - description: A fun description
        - achieved_date: When the milestone was reached
        - icon: A symbol for the milestone
        - category: Category of the milestone (plays, artists, songs, hours)
        - threshold: The threshold value that was reached
    """
    milestones = []

    # Define milestone thresholds
    play_milestones = [
        (100, "Century Club", "You've hit 100 total plays! The journey has begun.", "[100]"),
        (500, "High Fidelity", "500 plays and counting! Music is clearly your thing.", "[500]"),
        (1000, "Thousand Play Legend", "1,000 plays! You're a certified music lover!", "[1K]"),
        (5000, "Music Marathon Master", "5,000 plays! That's some serious dedication!", "[5K]"),
        (10000, "Ten Thousand Titan", "10,000 plays! You could run your own radio station!", "[10K]"),
    ]

    artist_milestones = [
        (10, "Curious Ears", "You've explored 10 different artists!", "[10A]"),
        (50, "Genre Hopper", "50 unique artists in your library! Eclectic taste!", "[50A]"),
        (100, "Taste Explorer", "100 artists! Your musical palette is impressively vast!", "[100A]"),
        (250, "Festival Curator", "250 artists! You could book your own music festival!", "[250A]"),
    ]

    song_milestones = [
        (100, "Song Collector", "100 unique songs in your collection!", "[100S]"),
        (500, "Playlist Pro", "500 unique songs! That's a serious playlist!", "[500S]"),
        (1000, "Track Titan", "1,000 unique songs! You're a walking jukebox!", "[1KS]"),
        (2500, "Melody Master", "2,500 songs! You've heard more than most will in years!", "[2.5KS]"),
    ]

    hours_milestones = [
        (10, "Getting Started", "10 hours of music! The journey begins!", "[10H]"),
        (50, "Dedicated Listener", "50 hours! That's over 2 full days of music!", "[50H]"),
        (100, "Century Hours", "100 hours! Music is clearly part of your life!", "[100H]"),
        (500, "Audiophile Status", "500 hours! You've spent almost 21 days listening!", "[500H]"),
        (1000, "Legendary Listener", "1,000 hours! That's 41+ days of pure music!", "[1KH]"),
    ]

    # Check total plays milestones
    cursor = conn.execute("SELECT COUNT(*) as count FROM plays")
    total_plays = cursor.fetchone()['count']

    for threshold, name, description, icon in play_milestones:
        if total_plays >= threshold:
            # Find when this milestone was achieved
            date_cursor = conn.execute("""
                SELECT timestamp FROM plays
                ORDER BY timestamp ASC
                LIMIT 1 OFFSET ?
            """, (threshold - 1,))
            row = date_cursor.fetchone()
            achieved_date = row['timestamp'] if row else None

            milestones.append({
                'name': name,
                'description': description,
                'achieved_date': achieved_date,
                'icon': icon,
                'category': 'plays',
                'threshold': threshold,
            })

    # Check unique artists milestones
    cursor = conn.execute("SELECT COUNT(DISTINCT artist) as count FROM plays WHERE artist IS NOT NULL")
    unique_artists = cursor.fetchone()['count']

    for threshold, name, description, icon in artist_milestones:
        if unique_artists >= threshold:
            # Find approximate date when this was achieved (when Nth unique artist was first played)
            date_cursor = conn.execute("""
                SELECT MAX(first_play) as achieved_date FROM (
                    SELECT artist, MIN(timestamp) as first_play
                    FROM plays WHERE artist IS NOT NULL
                    GROUP BY artist
                    ORDER BY first_play ASC
                    LIMIT ?
                )
            """, (threshold,))
            row = date_cursor.fetchone()
            achieved_date = row['achieved_date'] if row else None

            milestones.append({
                'name': name,
                'description': description,
                'achieved_date': achieved_date,
                'icon': icon,
                'category': 'artists',
                'threshold': threshold,
            })

    # Check unique songs milestones
    cursor = conn.execute("SELECT COUNT(DISTINCT title || '-' || COALESCE(artist, '')) as count FROM plays")
    unique_songs = cursor.fetchone()['count']

    for threshold, name, description, icon in song_milestones:
        if unique_songs >= threshold:
            date_cursor = conn.execute("""
                SELECT MAX(first_play) as achieved_date FROM (
                    SELECT title, artist, MIN(timestamp) as first_play
                    FROM plays
                    GROUP BY title, artist
                    ORDER BY first_play ASC
                    LIMIT ?
                )
            """, (threshold,))
            row = date_cursor.fetchone()
            achieved_date = row['achieved_date'] if row else None

            milestones.append({
                'name': name,
                'description': description,
                'achieved_date': achieved_date,
                'icon': icon,
                'category': 'songs',
                'threshold': threshold,
            })

    # Check hours listened milestones
    cursor = conn.execute("SELECT SUM(played_ms) as total_ms FROM plays")
    total_ms = cursor.fetchone()['total_ms'] or 0
    total_hours = total_ms / 1000 / 3600

    for threshold, name, description, icon in hours_milestones:
        if total_hours >= threshold:
            # Find when cumulative hours reached this threshold
            threshold_ms = threshold * 3600 * 1000
            date_cursor = conn.execute("""
                SELECT timestamp FROM (
                    SELECT timestamp,
                           SUM(played_ms) OVER (ORDER BY timestamp) as cumulative_ms
                    FROM plays
                    WHERE played_ms IS NOT NULL
                )
                WHERE cumulative_ms >= ?
                LIMIT 1
            """, (threshold_ms,))
            row = date_cursor.fetchone()
            achieved_date = row['timestamp'] if row else None

            milestones.append({
                'name': name,
                'description': description,
                'achieved_date': achieved_date,
                'icon': icon,
                'category': 'hours',
                'threshold': threshold,
            })

    # Sort by achieved date
    milestones.sort(key=lambda x: x['achieved_date'] or '9999')

    return milestones


# =============================================================================
# LISTENING PERSONALITY
# =============================================================================

def get_listening_personality(
    conn: sqlite3.Connection,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> dict:
    """
    Analyze listening patterns and assign a "listener type" personality.

    Args:
        conn: SQLite database connection with row_factory set.
        start_date: Start date as ISO string (YYYY-MM-DD). If None, no lower bound.
        end_date: End date as ISO string (YYYY-MM-DD). If None, no upper bound.

    Returns a dictionary with:
        - primary_type: Main personality type
        - secondary_type: Secondary trait (if applicable)
        - description: Fun description of the listener
        - traits: List of specific traits/behaviors detected
        - scores: Raw scores for each personality dimension
    """
    where_clause = "WHERE 1=1"
    params = []

    if start_date:
        where_clause += " AND timestamp >= ?"
        params.append(start_date)
    if end_date:
        where_clause += " AND timestamp <= ?"
        params.append(end_date)

    # Get basic stats for the period
    cursor = conn.execute(f"""
        SELECT
            COUNT(*) as total_plays,
            COUNT(DISTINCT artist) as unique_artists,
            COUNT(DISTINCT title || '-' || COALESCE(artist, '')) as unique_songs,
            COUNT(DISTINCT album) as unique_albums,
            SUM(played_ms) as total_ms
        FROM plays
        {where_clause}
    """, params)
    basic = cursor.fetchone()

    if basic['total_plays'] == 0:
        return {
            'primary_type': 'Silent Observer',
            'secondary_type': None,
            'description': "You haven't listened to much music in this period. Time to hit play!",
            'traits': [],
            'scores': {},
        }

    scores = {}
    traits = []

    # === EXPLORER SCORE ===
    # High discovery rate: many unique songs/artists relative to total plays
    discovery_rate = basic['unique_songs'] / max(basic['total_plays'], 1)
    artist_discovery_rate = basic['unique_artists'] / max(basic['total_plays'], 1)
    scores['explorer'] = (discovery_rate * 50) + (artist_discovery_rate * 50)

    if discovery_rate > 0.7:
        traits.append("You're always discovering new tracks!")
    if artist_discovery_rate > 0.3:
        traits.append("You love exploring new artists")

    # === LOYALIST SCORE ===
    # Few artists, many repeats per artist
    cursor = conn.execute(f"""
        SELECT artist, COUNT(*) as plays
        FROM plays
        {where_clause}
        AND artist IS NOT NULL
        GROUP BY artist
        ORDER BY plays DESC
        LIMIT 5
    """, params)
    top_artist_plays = cursor.fetchall()

    if top_artist_plays:
        top_artist_share = sum(r['plays'] for r in top_artist_plays) / max(basic['total_plays'], 1)
        avg_plays_per_artist = basic['total_plays'] / max(basic['unique_artists'], 1)
        scores['loyalist'] = (top_artist_share * 60) + min(avg_plays_per_artist / 10, 40)

        if top_artist_share > 0.5:
            traits.append(f"You're devoted to your favorites (top 5 artists = {top_artist_share:.0%} of plays)")
        if avg_plays_per_artist > 20:
            traits.append("You really know what you like!")
    else:
        scores['loyalist'] = 0

    # === NIGHT OWL SCORE === (use hour_of_day for local time)
    cursor = conn.execute(f"""
        SELECT
            SUM(CASE WHEN hour_of_day BETWEEN 22 AND 23
                     OR hour_of_day BETWEEN 0 AND 4
                THEN 1 ELSE 0 END) as late_night,
            COUNT(*) as total
        FROM plays
        {where_clause}
        AND hour_of_day IS NOT NULL
    """, params)
    time_dist = cursor.fetchone()

    if time_dist['total'] > 0:
        late_night_ratio = time_dist['late_night'] / time_dist['total']
        scores['night_owl'] = late_night_ratio * 100

        if late_night_ratio > 0.3:
            traits.append("The night is when your playlist comes alive!")
        if late_night_ratio > 0.5:
            traits.append("You're definitely a creature of the night")
    else:
        scores['night_owl'] = 0

    # === EARLY BIRD SCORE === (use hour_of_day for local time)
    cursor = conn.execute(f"""
        SELECT
            SUM(CASE WHEN hour_of_day BETWEEN 5 AND 9
                THEN 1 ELSE 0 END) as early_morning,
            COUNT(*) as total
        FROM plays
        {where_clause}
        AND hour_of_day IS NOT NULL
    """, params)
    morning_dist = cursor.fetchone()

    if morning_dist['total'] > 0:
        early_ratio = morning_dist['early_morning'] / morning_dist['total']
        scores['early_bird'] = early_ratio * 100

        if early_ratio > 0.25:
            traits.append("Music with your morning coffee is a must!")
    else:
        scores['early_bird'] = 0

    # === COMPLETIONIST SCORE ===
    # High album completion rate
    cursor = conn.execute(f"""
        SELECT
            album,
            artist,
            COUNT(DISTINCT title) as songs_played,
            COUNT(*) as total_plays
        FROM plays
        {where_clause}
        AND album IS NOT NULL
        AND album != ''
        GROUP BY album, artist
        HAVING songs_played >= 5
    """, params)
    album_completions = cursor.fetchall()

    if album_completions:
        avg_songs_per_album = sum(r['songs_played'] for r in album_completions) / len(album_completions)
        high_completion_albums = sum(1 for r in album_completions if r['songs_played'] >= 8)
        scores['completionist'] = (avg_songs_per_album * 5) + (high_completion_albums * 10)

        if avg_songs_per_album >= 6:
            traits.append("You like to hear albums the way artists intended")
        if high_completion_albums >= 3:
            traits.append(f"You've deep-dived into {high_completion_albums} albums!")
    else:
        scores['completionist'] = 0

    # === BINGE LISTENER SCORE ===
    # Listens to same song many times in a row
    cursor = conn.execute(f"""
        SELECT title, artist, COUNT(*) as plays
        FROM plays
        {where_clause}
        GROUP BY title, artist
        ORDER BY plays DESC
        LIMIT 1
    """, params)
    top_song = cursor.fetchone()

    if top_song and top_song['plays'] > 10:
        binge_score = min(top_song['plays'] / 5, 50)
        scores['binge_listener'] = binge_score

        if top_song['plays'] > 20:
            traits.append(f'"{top_song["title"]}" is clearly your anthem!')
    else:
        scores['binge_listener'] = 0

    # === WEEKEND WARRIOR SCORE === (use day_of_week for local time)
    # day_of_week uses Python convention: 0=Monday, so weekend is 5=Saturday, 6=Sunday
    cursor = conn.execute(f"""
        SELECT
            SUM(CASE WHEN day_of_week IN (5, 6)
                THEN 1 ELSE 0 END) as weekend,
            COUNT(*) as total
        FROM plays
        {where_clause}
        AND day_of_week IS NOT NULL
    """, params)
    weekend_dist = cursor.fetchone()

    if weekend_dist['total'] > 0:
        weekend_ratio = weekend_dist['weekend'] / weekend_dist['total']
        # Expected is about 28.5% (2/7 days), so scale accordingly
        scores['weekend_warrior'] = max(0, (weekend_ratio - 0.285) * 200)

        if weekend_ratio > 0.4:
            traits.append("Weekends are for serious listening sessions")
    else:
        scores['weekend_warrior'] = 0

    # === ECLECTIC SCORE ===
    # Variety in listening patterns (changes frequently)
    cursor = conn.execute(f"""
        SELECT COUNT(DISTINCT DATE(timestamp)) as listening_days
        FROM plays
        {where_clause}
    """, params)
    listening_days = cursor.fetchone()['listening_days']

    if listening_days > 0:
        variety_per_day = basic['unique_songs'] / listening_days
        scores['eclectic'] = min(variety_per_day * 5, 100)

        if variety_per_day > 15:
            traits.append("You never listen to the same thing twice in a day!")
    else:
        scores['eclectic'] = 0

    # Determine primary and secondary types
    personality_types = {
        'explorer': ("The Explorer", "You're on a never-ending quest for new sounds. Your library is a treasure map of musical discovery!"),
        'loyalist': ("The Loyalist", "When you find an artist you love, you stick with them. Your dedication is legendary!"),
        'night_owl': ("The Night Owl", "Your best listening happens when the world is asleep. The night is your concert hall."),
        'early_bird': ("The Early Bird", "Nothing starts the day right like your favorite tunes. Morning music is your ritual!"),
        'completionist': ("The Completionist", "Skip a track? Never! You appreciate albums as complete artistic statements."),
        'binge_listener': ("The Obsessive", "When you love a song, you REALLY love it. Repeat button is your best friend!"),
        'weekend_warrior': ("The Weekend Warrior", "You save your serious listening for when you have time to really enjoy it."),
        'eclectic': ("The Eclectic", "Your taste knows no bounds. Variety is the spice of your musical life!"),
    }

    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    primary_key = sorted_scores[0][0] if sorted_scores else 'explorer'
    secondary_key = sorted_scores[1][0] if len(sorted_scores) > 1 and sorted_scores[1][1] > 20 else None

    primary_name, primary_desc = personality_types.get(primary_key, ("Music Lover", "You just love music!"))
    secondary_name = personality_types.get(secondary_key, (None, None))[0] if secondary_key else None

    return {
        'primary_type': primary_name,
        'secondary_type': secondary_name,
        'description': primary_desc,
        'traits': traits,
        'scores': scores,
    }


# =============================================================================
# FUN FACTS
# =============================================================================

def get_fun_facts(
    conn: sqlite3.Connection,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> list[str]:
    """
    Generate interesting and fun facts about listening habits.

    Args:
        conn: SQLite database connection with row_factory set.
        start_date: Start date as ISO string (YYYY-MM-DD). If None, no lower bound.
        end_date: End date as ISO string (YYYY-MM-DD). If None, no upper bound.

    Returns a list of fun fact strings.
    """
    where_clause = "WHERE 1=1"
    params = []

    if start_date:
        where_clause += " AND timestamp >= ?"
        params.append(start_date)
    if end_date:
        where_clause += " AND timestamp <= ?"
        params.append(end_date)

    facts = []

    # Total listening time facts
    cursor = conn.execute(f"""
        SELECT SUM(played_ms) as total_ms, COUNT(*) as total_plays
        FROM plays
        {where_clause}
    """, params)
    totals = cursor.fetchone()
    total_ms = totals['total_ms'] or 0
    total_plays = totals['total_plays'] or 0

    if total_plays == 0:
        return ["No listening data found for this period. Time to press play!"]

    total_hours = total_ms / 1000 / 3600
    total_minutes = total_ms / 1000 / 60

    # Hours comparison facts
    if total_hours >= 1:
        # Calculate equivalent albums (assuming ~45 min average album)
        equivalent_albums = total_minutes / 45
        facts.append(f"You could have listened to {equivalent_albums:.0f} full albums with your {total_hours:.1f} hours of music!")

    if total_hours >= 24:
        days = total_hours / 24
        facts.append(f"That's {days:.1f} full days of non-stop music!")

    if total_hours >= 100:
        marathon_movies = total_hours / 2.5  # Average movie length
        facts.append(f"Instead of music, you could have watched {marathon_movies:.0f} movies!")

    # Top song facts
    cursor = conn.execute(f"""
        SELECT title, artist, COUNT(*) as plays, SUM(played_ms) as total_ms
        FROM plays
        {where_clause}
        GROUP BY title, artist
        ORDER BY plays DESC
        LIMIT 1
    """, params)
    top_song = cursor.fetchone()

    if top_song and top_song['plays'] >= 5:
        song_hours = (top_song['total_ms'] or 0) / 1000 / 3600
        song_title = top_song['title']
        song_artist = top_song['artist'] or 'Unknown Artist'
        facts.append(f'Your #1 song "{song_title}" by {song_artist} was played {top_song["plays"]} times - that\'s {song_hours:.1f} hours!')

        if top_song['plays'] >= 20:
            facts.append(f'If "{song_title}" was stuck in your head, it would have been there for {top_song["plays"]} repeat cycles!')

    # Discovery facts - new artists in this period vs before
    if start_date:
        cursor = conn.execute(f"""
            SELECT COUNT(DISTINCT artist) as new_artists
            FROM plays
            {where_clause}
            AND artist IS NOT NULL
            AND artist NOT IN (
                SELECT DISTINCT artist FROM plays
                WHERE timestamp < ? AND artist IS NOT NULL
            )
        """, params + [start_date])
        new_artists = cursor.fetchone()['new_artists']

        if new_artists > 0:
            facts.append(f"You discovered {new_artists} new artist{'s' if new_artists != 1 else ''} in this period!")

    # Streak facts
    cursor = conn.execute(f"""
        SELECT DATE(timestamp) as play_date, COUNT(*) as plays
        FROM plays
        {where_clause}
        GROUP BY DATE(timestamp)
        ORDER BY plays DESC
        LIMIT 1
    """, params)
    biggest_day = cursor.fetchone()

    if biggest_day and biggest_day['plays'] >= 10:
        facts.append(f"Your biggest listening day had {biggest_day['plays']} plays on {biggest_day['play_date']}!")

    # Time of day facts (use hour_of_day for local time)
    cursor = conn.execute(f"""
        SELECT hour_of_day as hour, COUNT(*) as plays
        FROM plays
        {where_clause}
        AND hour_of_day IS NOT NULL
        GROUP BY hour_of_day
        ORDER BY plays DESC
        LIMIT 1
    """, params)
    peak_hour = cursor.fetchone()

    if peak_hour:
        hour = peak_hour['hour']
        if hour == 0:
            time_str = "midnight"
        elif hour < 12:
            time_str = f"{hour} AM"
        elif hour == 12:
            time_str = "noon"
        else:
            time_str = f"{hour - 12} PM"
        facts.append(f"Your peak listening hour is {time_str}!")

    # Unique combinations
    cursor = conn.execute(f"""
        SELECT COUNT(DISTINCT title || '-' || COALESCE(artist, '')) as unique_songs,
               COUNT(DISTINCT artist) as unique_artists,
               COUNT(DISTINCT album) as unique_albums
        FROM plays
        {where_clause}
    """, params)
    uniques = cursor.fetchone()

    if uniques['unique_artists'] >= 10:
        facts.append(f"You listened to {uniques['unique_artists']} different artists - that's a diverse taste!")

    if uniques['unique_albums'] >= 20:
        facts.append(f"You explored {uniques['unique_albums']} different albums!")

    # Long song fact
    cursor = conn.execute(f"""
        SELECT title, artist, duration_ms
        FROM plays
        {where_clause}
        AND duration_ms IS NOT NULL
        ORDER BY duration_ms DESC
        LIMIT 1
    """, params)
    longest_song = cursor.fetchone()

    if longest_song and longest_song['duration_ms']:
        duration_min = longest_song['duration_ms'] / 1000 / 60
        if duration_min >= 7:
            facts.append(f'The longest track you played was "{longest_song["title"]}" at {duration_min:.1f} minutes!')

    # Average session length estimate
    cursor = conn.execute(f"""
        SELECT COUNT(*) as plays, COUNT(DISTINCT DATE(timestamp)) as days
        FROM plays
        {where_clause}
    """, params)
    session_data = cursor.fetchone()

    if session_data['days'] > 0:
        avg_plays_per_day = session_data['plays'] / session_data['days']
        if avg_plays_per_day >= 5:
            facts.append(f"On average, you played {avg_plays_per_day:.1f} tracks per listening day!")

    # Artist loyalty fact
    cursor = conn.execute(f"""
        SELECT artist, COUNT(*) as plays
        FROM plays
        {where_clause}
        AND artist IS NOT NULL
        GROUP BY artist
        ORDER BY plays DESC
        LIMIT 1
    """, params)
    top_artist = cursor.fetchone()

    if top_artist and total_plays > 0:
        artist_percent = (top_artist['plays'] / total_plays) * 100
        if artist_percent >= 10:
            facts.append(f'{top_artist["artist"]} accounted for {artist_percent:.1f}% of your listening!')

    return facts


# =============================================================================
# YEAR IN REVIEW
# =============================================================================

def get_year_in_review_summary(conn: sqlite3.Connection, year: int) -> dict:
    """
    Generate a comprehensive yearly summary combining all metrics.

    Args:
        conn: SQLite database connection with row_factory set.
        year: The year to generate the review for.

    Returns a dictionary with:
        - year: The year being reviewed
        - total_stats: Basic statistics
        - top_artists: Top 10 artists
        - top_songs: Top 10 songs
        - top_albums: Top 10 albums
        - personality: Listening personality analysis
        - milestones_earned: Milestones achieved this year
        - fun_facts: List of fun facts
        - monthly_breakdown: Stats by month
        - listening_journey: Key moments/highlights
    """
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31 23:59:59"

    where_clause = "WHERE timestamp >= ? AND timestamp <= ?"
    params = [start_date, end_date]

    # === TOTAL STATS ===
    cursor = conn.execute(f"""
        SELECT
            COUNT(*) as total_plays,
            COUNT(DISTINCT artist) as unique_artists,
            COUNT(DISTINCT album) as unique_albums,
            COUNT(DISTINCT title || '-' || COALESCE(artist, '')) as unique_songs,
            SUM(played_ms) as total_ms,
            COUNT(DISTINCT DATE(timestamp)) as listening_days
        FROM plays
        {where_clause}
    """, params)
    total_stats = dict(cursor.fetchone())

    if total_stats['total_plays'] == 0:
        return {
            'year': year,
            'total_stats': total_stats,
            'top_artists': [],
            'top_songs': [],
            'top_albums': [],
            'personality': get_listening_personality(conn, start_date, end_date),
            'milestones_earned': [],
            'fun_facts': ["No listening data for this year yet!"],
            'monthly_breakdown': [],
            'listening_journey': [],
        }

    total_stats['total_hours'] = (total_stats['total_ms'] or 0) / 1000 / 3600

    # === TOP ARTISTS ===
    cursor = conn.execute(f"""
        SELECT artist, COUNT(*) as plays, SUM(played_ms) as total_ms
        FROM plays
        {where_clause}
        AND artist IS NOT NULL
        GROUP BY artist
        ORDER BY plays DESC
        LIMIT 10
    """, params)
    top_artists = [dict(row) for row in cursor.fetchall()]

    # === TOP SONGS ===
    cursor = conn.execute(f"""
        SELECT title, artist, COUNT(*) as plays, SUM(played_ms) as total_ms
        FROM plays
        {where_clause}
        GROUP BY title, artist
        ORDER BY plays DESC
        LIMIT 10
    """, params)
    top_songs = [dict(row) for row in cursor.fetchall()]

    # === TOP ALBUMS ===
    cursor = conn.execute(f"""
        SELECT album, artist, COUNT(*) as plays,
               COUNT(DISTINCT title) as unique_tracks,
               SUM(played_ms) as total_ms
        FROM plays
        {where_clause}
        AND album IS NOT NULL AND album != ''
        GROUP BY album, artist
        ORDER BY plays DESC
        LIMIT 10
    """, params)
    top_albums = [dict(row) for row in cursor.fetchall()]

    # === PERSONALITY ===
    personality = get_listening_personality(conn, start_date, end_date)

    # === MILESTONES EARNED THIS YEAR ===
    all_milestones = get_milestones(conn)
    milestones_earned = [
        m for m in all_milestones
        if m['achieved_date'] and m['achieved_date'].startswith(str(year))
    ]

    # === FUN FACTS ===
    fun_facts = get_fun_facts(conn, start_date, end_date)

    # === MONTHLY BREAKDOWN ===
    cursor = conn.execute(f"""
        SELECT
            strftime('%m', timestamp) as month,
            COUNT(*) as plays,
            SUM(played_ms) as total_ms,
            COUNT(DISTINCT artist) as unique_artists
        FROM plays
        {where_clause}
        GROUP BY month
        ORDER BY month
    """, params)
    monthly_breakdown = []
    month_names = ['', 'January', 'February', 'March', 'April', 'May', 'June',
                   'July', 'August', 'September', 'October', 'November', 'December']

    for row in cursor.fetchall():
        month_num = int(row['month'])
        monthly_breakdown.append({
            'month': month_names[month_num],
            'month_num': month_num,
            'plays': row['plays'],
            'hours': (row['total_ms'] or 0) / 1000 / 3600,
            'unique_artists': row['unique_artists'],
        })

    # === LISTENING JOURNEY (Key Moments) ===
    listening_journey = []

    # First play of the year
    cursor = conn.execute(f"""
        SELECT title, artist, timestamp
        FROM plays
        {where_clause}
        ORDER BY timestamp ASC
        LIMIT 1
    """, params)
    first_play = cursor.fetchone()
    if first_play:
        listening_journey.append({
            'moment': 'First Song of the Year',
            'title': first_play['title'],
            'artist': first_play['artist'],
            'date': first_play['timestamp'],
        })

    # Last play of the year (or most recent)
    cursor = conn.execute(f"""
        SELECT title, artist, timestamp
        FROM plays
        {where_clause}
        ORDER BY timestamp DESC
        LIMIT 1
    """, params)
    last_play = cursor.fetchone()
    if last_play:
        listening_journey.append({
            'moment': 'Most Recent Song',
            'title': last_play['title'],
            'artist': last_play['artist'],
            'date': last_play['timestamp'],
        })

    # Biggest listening day
    cursor = conn.execute(f"""
        SELECT DATE(timestamp) as date, COUNT(*) as plays, SUM(played_ms) as total_ms
        FROM plays
        {where_clause}
        GROUP BY DATE(timestamp)
        ORDER BY plays DESC
        LIMIT 1
    """, params)
    biggest_day = cursor.fetchone()
    if biggest_day:
        listening_journey.append({
            'moment': 'Biggest Listening Day',
            'plays': biggest_day['plays'],
            'hours': (biggest_day['total_ms'] or 0) / 1000 / 3600,
            'date': biggest_day['date'],
        })

    # First new artist discovery of the year
    cursor = conn.execute(f"""
        SELECT artist, MIN(timestamp) as first_play
        FROM plays
        {where_clause}
        AND artist IS NOT NULL
        AND artist NOT IN (
            SELECT DISTINCT artist FROM plays
            WHERE timestamp < ? AND artist IS NOT NULL
        )
        GROUP BY artist
        ORDER BY first_play ASC
        LIMIT 1
    """, params + [start_date])
    first_discovery = cursor.fetchone()
    if first_discovery and first_discovery['artist']:
        listening_journey.append({
            'moment': 'First New Artist Discovery',
            'artist': first_discovery['artist'],
            'date': first_discovery['first_play'],
        })

    return {
        'year': year,
        'total_stats': total_stats,
        'top_artists': top_artists,
        'top_songs': top_songs,
        'top_albums': top_albums,
        'personality': personality,
        'milestones_earned': milestones_earned,
        'fun_facts': fun_facts,
        'monthly_breakdown': monthly_breakdown,
        'listening_journey': listening_journey,
    }


# =============================================================================
# DISPLAY HELPERS FOR FUN STATS
# =============================================================================

def format_milestone(milestone: dict) -> str:
    """Format a milestone for display."""
    date_str = milestone['achieved_date'][:10] if milestone['achieved_date'] else 'Unknown'
    return f"{milestone['icon']} {milestone['name']}: {milestone['description']} (Achieved: {date_str})"


def display_milestones(conn: sqlite3.Connection):
    """Print all achieved milestones."""
    milestones = get_milestones(conn)

    if not milestones:
        print("\nNo milestones achieved yet. Keep listening!\n")
        return

    print("\n" + "=" * 60)
    print("  YOUR ACHIEVEMENTS")
    print("=" * 60 + "\n")

    for milestone in milestones:
        print(f"  {format_milestone(milestone)}")

    print(f"\n  Total: {len(milestones)} milestones unlocked!\n")


def display_personality(
    conn: sqlite3.Connection,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Print personality analysis."""
    personality = get_listening_personality(conn, start_date, end_date)

    print("\n" + "=" * 60)
    print("  YOUR LISTENING PERSONALITY")
    print("=" * 60 + "\n")

    print(f"  You are: {personality['primary_type']}")
    if personality['secondary_type']:
        print(f"  With a hint of: {personality['secondary_type']}")
    print()
    print(f"  {personality['description']}")

    if personality['traits']:
        print("\n  Your traits:")
        for trait in personality['traits']:
            print(f"    * {trait}")

    print()


def display_fun_facts(
    conn: sqlite3.Connection,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Print fun facts."""
    facts = get_fun_facts(conn, start_date, end_date)

    print("\n" + "=" * 60)
    print("  FUN FACTS")
    print("=" * 60 + "\n")

    for i, fact in enumerate(facts, 1):
        print(f"  {i}. {fact}")

    print()


def display_year_in_review(conn: sqlite3.Connection, year: int):
    """Print comprehensive year in review."""
    review = get_year_in_review_summary(conn, year)

    print("\n" + "*" * 60)
    print(f"           YOUR {year} MUSIC YEAR IN REVIEW")
    print("*" * 60)

    # Total stats
    stats = review['total_stats']
    print("\n  THE NUMBERS:")
    print(f"    Total plays: {stats['total_plays']:,}")
    print(f"    Hours listened: {stats.get('total_hours', 0):.1f}")
    print(f"    Unique artists: {stats['unique_artists']:,}")
    print(f"    Unique songs: {stats['unique_songs']:,}")
    print(f"    Unique albums: {stats['unique_albums']:,}")
    print(f"    Days with music: {stats['listening_days']:,}")

    # Top artists
    if review['top_artists']:
        print("\n  YOUR TOP ARTISTS:")
        for i, artist in enumerate(review['top_artists'][:5], 1):
            hours = (artist.get('total_ms') or 0) / 1000 / 3600
            print(f"    {i}. {artist['artist']} ({artist['plays']} plays, {hours:.1f}h)")

    # Top songs
    if review['top_songs']:
        print("\n  YOUR TOP SONGS:")
        for i, song in enumerate(review['top_songs'][:5], 1):
            print(f"    {i}. \"{song['title']}\" by {song['artist'] or 'Unknown'} ({song['plays']} plays)")

    # Personality
    print(f"\n  YOUR LISTENING PERSONALITY: {review['personality']['primary_type']}")
    print(f"    {review['personality']['description']}")

    # Milestones earned this year
    if review['milestones_earned']:
        print(f"\n  MILESTONES UNLOCKED IN {year}:")
        for m in review['milestones_earned']:
            print(f"    {m['icon']} {m['name']}")

    # Fun facts
    if review['fun_facts']:
        print("\n  FUN FACTS:")
        for fact in review['fun_facts'][:5]:
            print(f"    * {fact}")

    # Journey highlights
    if review['listening_journey']:
        print("\n  KEY MOMENTS:")
        for moment in review['listening_journey']:
            if 'title' in moment:
                print(f"    {moment['moment']}: \"{moment['title']}\" by {moment.get('artist', 'Unknown')} ({moment['date'][:10]})")
            elif 'artist' in moment:
                print(f"    {moment['moment']}: {moment['artist']} ({moment['date'][:10]})")
            elif 'plays' in moment:
                print(f"    {moment['moment']}: {moment['plays']} plays ({moment['date']})")

    print("\n" + "*" * 60 + "\n")


if __name__ == "__main__":
    # Example usage
    import json
    import argparse

    parser = argparse.ArgumentParser(
        description="Music analytics - stats, milestones, and fun facts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  analytics.py                        Show time-based analytics
  analytics.py --milestones           Show all your achievements
  analytics.py --personality          Analyze your listening personality
  analytics.py --fun-facts            Show fun facts about your listening
  analytics.py --year-review 2025     Full year in review for 2025
  analytics.py --all                  Show everything!
        """
    )

    parser.add_argument('--milestones', action='store_true', help='Show achieved milestones')
    parser.add_argument('--personality', action='store_true', help='Show your listening personality')
    parser.add_argument('--fun-facts', action='store_true', help='Show fun facts')
    parser.add_argument('--year-review', type=int, metavar='YEAR', help='Show year in review')
    parser.add_argument('--all', action='store_true', help='Show all analytics')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD) for personality/facts')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD) for personality/facts')

    args = parser.parse_args()

    conn = get_connection()

    # If no specific flags, show time-based analytics (original behavior)
    if not any([args.milestones, args.personality, args.fun_facts, args.year_review, args.all]):
        print("Running time-based analytics...")
        print("-" * 50)
        results = get_time_analytics()
        print(json.dumps(results, indent=2, default=str))

        print("\nRunning track behavior analytics...")
        print("-" * 50)

        behavior_results = get_track_behavior_analytics()
        print(json.dumps(behavior_results, indent=2, default=str))

        print("\n" + "=" * 50)
        print("Running artist insight analytics...")
        print("-" * 50)

        # Example: analyze the current year
        current_year = datetime.now().year
        artist_results = get_artist_analytics(
            f"{current_year}-01-01",
            f"{current_year}-12-31",
            current_year,
        )
        print(json.dumps(artist_results, indent=2, default=str))
    else:
        if args.all:
            # Show current year review and milestones
            current_year = datetime.now().year
            display_year_in_review(conn, current_year)
            display_milestones(conn)

        if args.milestones:
            display_milestones(conn)
        if args.personality:
            display_personality(conn, args.start_date, args.end_date)
        if args.fun_facts:
            display_fun_facts(conn, args.start_date, args.end_date)
        if args.year_review:
            display_year_in_review(conn, args.year_review)

    conn.close()
