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
):
    """Log a play to the database."""
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO plays (
            title, artist, album, duration_ms, played_ms, file_path,
            genre, album_artist, track_number, disc_number, release_date,
            art_url, user_rating, bpm, composer, musicbrainz_track_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            title, artist, album, duration_ms, played_ms, file_path,
            genre, album_artist, track_number, disc_number, release_date,
            art_url, user_rating, bpm, composer, musicbrainz_track_id
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


# Initialize database on import
init_db()
