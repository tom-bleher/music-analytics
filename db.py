#!/usr/bin/env python3
"""Database utilities for music analytics."""

import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional, List

DB_PATH = Path(__file__).parent / "listens.db"


def get_connection() -> sqlite3.Connection:
    """Get a database connection with row factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initialize the database schema."""
    conn = get_connection()

    # Create main plays table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS plays (
            id INTEGER PRIMARY KEY,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            title TEXT NOT NULL,
            artist TEXT,
            album TEXT,
            duration_ms INTEGER,
            played_ms INTEGER,
            file_path TEXT
        )
    """)

    # Add new columns if they don't exist (migration)
    new_columns = [
        ("genre", "TEXT"),
        ("album_artist", "TEXT"),
        ("track_number", "INTEGER"),
        ("disc_number", "INTEGER"),
        ("release_date", "TEXT"),
        ("art_url", "TEXT"),
        ("user_rating", "REAL"),
        ("bpm", "INTEGER"),
        ("composer", "TEXT"),
        ("musicbrainz_track_id", "TEXT"),
        # Seek tracking
        ("seek_count", "INTEGER"),
        ("intro_skipped", "INTEGER"),  # 1 if seeked past first 15s
        ("seek_forward_ms", "INTEGER"),
        ("seek_backward_ms", "INTEGER"),
        # Volume tracking
        ("app_volume", "REAL"),  # MPRIS app volume (0.0-1.0)
        ("system_volume", "REAL"),  # PulseAudio volume (0.0-1.0)
        ("effective_volume", "REAL"),  # Combined volume (app × system)
        # Context tracking
        ("hour_of_day", "INTEGER"),  # 0-23
        ("day_of_week", "INTEGER"),  # 0=Monday, 6=Sunday
        ("is_weekend", "INTEGER"),  # 1 if Saturday/Sunday
        ("season", "TEXT"),  # spring, summer, fall, winter
        ("active_window", "TEXT"),  # Focused app while listening
        ("screen_on", "INTEGER"),  # 1 if screen is on
        ("on_battery", "INTEGER"),  # 1 if on battery power
        ("player_name", "TEXT"),  # Which player was used
        ("is_local", "INTEGER"),  # 1 for local files, 0 for streaming/non-local
    ]

    # Get existing columns
    cursor = conn.execute("PRAGMA table_info(plays)")
    existing_columns = {row[1] for row in cursor.fetchall()}

    # Add missing columns
    for col_name, col_type in new_columns:
        if col_name not in existing_columns:
            conn.execute(f"ALTER TABLE plays ADD COLUMN {col_name} {col_type}")

    # Create indexes
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_plays_timestamp ON plays(timestamp)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_plays_artist ON plays(artist)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_plays_genre ON plays(genre)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_plays_album ON plays(album)
    """)

    # Create audio_features table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS audio_features (
            file_path TEXT PRIMARY KEY,
            tempo REAL,
            energy REAL,
            danceability REAL,
            valence REAL,
            acousticness REAL,
            instrumentalness REAL,
            speechiness REAL,
            loudness REAL,
            key INTEGER,
            mode INTEGER,
            time_signature INTEGER,
            analyzed_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_audio_features_analyzed
        ON audio_features(analyzed_at)
    """)

    conn.commit()
    conn.close()


def log_play(
    title: str,
    artist: Optional[str] = None,
    album: Optional[str] = None,
    duration_ms: Optional[int] = None,
    played_ms: Optional[int] = None,
    file_path: Optional[str] = None,
    genre: Optional[str] = None,
    album_artist: Optional[str] = None,
    track_number: Optional[int] = None,
    disc_number: Optional[int] = None,
    release_date: Optional[str] = None,
    art_url: Optional[str] = None,
    user_rating: Optional[float] = None,
    bpm: Optional[int] = None,
    composer: Optional[str] = None,
    musicbrainz_track_id: Optional[str] = None,
    seek_count: Optional[int] = None,
    intro_skipped: Optional[int] = None,
    seek_forward_ms: Optional[int] = None,
    seek_backward_ms: Optional[int] = None,
    app_volume: Optional[float] = None,
    system_volume: Optional[float] = None,
    effective_volume: Optional[float] = None,
    hour_of_day: Optional[int] = None,
    day_of_week: Optional[int] = None,
    is_weekend: Optional[int] = None,
    season: Optional[str] = None,
    active_window: Optional[str] = None,
    screen_on: Optional[int] = None,
    on_battery: Optional[int] = None,
    player_name: Optional[str] = None,
    is_local: Optional[int] = None,
):
    """Log a play to the database."""
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO plays (
            title, artist, album, duration_ms, played_ms, file_path,
            genre, album_artist, track_number, disc_number, release_date,
            art_url, user_rating, bpm, composer, musicbrainz_track_id,
            seek_count, intro_skipped, seek_forward_ms, seek_backward_ms,
            app_volume, system_volume, effective_volume,
            hour_of_day, day_of_week, is_weekend, season,
            active_window, screen_on, on_battery, player_name, is_local
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            title, artist, album, duration_ms, played_ms, file_path,
            genre, album_artist, track_number, disc_number, release_date,
            art_url, user_rating, bpm, composer, musicbrainz_track_id,
            seek_count, intro_skipped, seek_forward_ms, seek_backward_ms,
            app_volume, system_volume, effective_volume,
            hour_of_day, day_of_week, is_weekend, season,
            active_window, screen_on, on_battery, player_name, is_local
        ),
    )
    conn.commit()
    conn.close()


def get_plays(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
):
    """Get plays within a date range."""
    conn = get_connection()

    query = "SELECT * FROM plays WHERE 1=1"
    params = []

    if start_date:
        query += " AND timestamp >= ?"
        params.append(start_date.isoformat())
    if end_date:
        query += " AND timestamp <= ?"
        params.append(end_date.isoformat())

    query += " ORDER BY timestamp DESC"

    cursor = conn.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return rows


def get_genre_stats(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
):
    """Get play counts by genre."""
    conn = get_connection()

    where_clause = "WHERE genre IS NOT NULL"
    params = []

    if start_date:
        where_clause += " AND timestamp >= ?"
        params.append(start_date.isoformat())
    if end_date:
        where_clause += " AND timestamp <= ?"
        params.append(end_date.isoformat())

    cursor = conn.execute(f"""
        SELECT genre, COUNT(*) as play_count, SUM(played_ms) as total_ms
        FROM plays
        {where_clause}
        GROUP BY genre
        ORDER BY play_count DESC
    """, params)

    rows = cursor.fetchall()
    conn.close()
    return rows


def get_release_year_stats(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
):
    """Get play counts by release year."""
    conn = get_connection()

    where_clause = "WHERE release_date IS NOT NULL"
    params = []

    if start_date:
        where_clause += " AND timestamp >= ?"
        params.append(start_date.isoformat())
    if end_date:
        where_clause += " AND timestamp <= ?"
        params.append(end_date.isoformat())

    cursor = conn.execute(f"""
        SELECT
            SUBSTR(release_date, 1, 4) as year,
            COUNT(*) as play_count,
            SUM(played_ms) as total_ms
        FROM plays
        {where_clause}
        GROUP BY year
        ORDER BY year DESC
    """, params)

    rows = cursor.fetchall()
    conn.close()
    return rows


def save_audio_features(file_path: str, features: dict):
    """Save audio features for a file to the database."""
    conn = get_connection()
    conn.execute(
        """
        INSERT OR REPLACE INTO audio_features (
            file_path, tempo, energy, danceability, valence,
            acousticness, instrumentalness, speechiness, loudness,
            key, mode, time_signature, analyzed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            file_path,
            features.get('tempo'),
            features.get('energy'),
            features.get('danceability'),
            features.get('valence'),
            features.get('acousticness'),
            features.get('instrumentalness'),
            features.get('speechiness'),
            features.get('loudness'),
            features.get('key'),
            features.get('mode'),
            features.get('time_signature'),
            datetime.now().isoformat(),
        ),
    )
    conn.commit()
    conn.close()


def get_audio_features(file_path: str) -> Optional[dict]:
    """Get audio features for a file from the database."""
    conn = get_connection()
    cursor = conn.execute(
        "SELECT * FROM audio_features WHERE file_path = ?",
        (file_path,)
    )
    row = cursor.fetchone()
    conn.close()

    if row:
        return {
            'file_path': row['file_path'],
            'tempo': row['tempo'],
            'energy': row['energy'],
            'danceability': row['danceability'],
            'valence': row['valence'],
            'acousticness': row['acousticness'],
            'instrumentalness': row['instrumentalness'],
            'speechiness': row['speechiness'],
            'loudness': row['loudness'],
            'key': row['key'],
            'mode': row['mode'],
            'time_signature': row['time_signature'],
            'analyzed_at': row['analyzed_at'],
        }
    return None


def is_file_analyzed(file_path: str) -> bool:
    """Check if a file has already been analyzed."""
    conn = get_connection()
    cursor = conn.execute(
        "SELECT 1 FROM audio_features WHERE file_path = ?",
        (file_path,)
    )
    result = cursor.fetchone() is not None
    conn.close()
    return result


def get_all_audio_features() -> List[dict]:
    """Get all audio features from the database."""
    conn = get_connection()
    cursor = conn.execute("SELECT * FROM audio_features")
    rows = cursor.fetchall()
    conn.close()

    return [
        {
            'file_path': row['file_path'],
            'tempo': row['tempo'],
            'energy': row['energy'],
            'danceability': row['danceability'],
            'valence': row['valence'],
            'acousticness': row['acousticness'],
            'instrumentalness': row['instrumentalness'],
            'speechiness': row['speechiness'],
            'loudness': row['loudness'],
            'key': row['key'],
            'mode': row['mode'],
            'time_signature': row['time_signature'],
            'analyzed_at': row['analyzed_at'],
        }
        for row in rows
    ]


def delete_non_local_plays() -> int:
    """Delete ONLY explicitly non-local plays from the database.

    SAFE: Only deletes plays that are EXPLICITLY identified as non-local:
    - is_local = 0 (explicitly marked as non-local)
    - file_path starts with http://, https://, spotify:, etc.

    Does NOT delete plays with NULL values (preserves legacy/unknown data).

    Returns:
        Number of rows deleted
    """
    conn = get_connection()

    # Count before deletion for reporting
    cursor = conn.execute("""
        SELECT COUNT(*) FROM plays
        WHERE is_local = 0
           OR (file_path IS NOT NULL AND (
               file_path LIKE 'http://%'
               OR file_path LIKE 'https://%'
               OR file_path LIKE 'spotify:%'
               OR file_path LIKE 'deezer:%'
               OR file_path LIKE 'tidal:%'
           ))
    """)
    count = cursor.fetchone()[0]

    # Delete only explicitly non-local plays
    conn.execute("""
        DELETE FROM plays
        WHERE is_local = 0
           OR (file_path IS NOT NULL AND (
               file_path LIKE 'http://%'
               OR file_path LIKE 'https://%'
               OR file_path LIKE 'spotify:%'
               OR file_path LIKE 'deezer:%'
               OR file_path LIKE 'tidal:%'
           ))
    """)

    conn.commit()
    conn.close()

    return count


def get_non_local_plays_count() -> int:
    """Get count of explicitly non-local plays in the database."""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT COUNT(*) FROM plays
        WHERE is_local = 0
           OR (file_path IS NOT NULL AND (
               file_path LIKE 'http://%'
               OR file_path LIKE 'https://%'
               OR file_path LIKE 'spotify:%'
               OR file_path LIKE 'deezer:%'
               OR file_path LIKE 'tidal:%'
           ))
    """)
    count = cursor.fetchone()[0]
    conn.close()
    return count


# Initialize database on import
init_db()
