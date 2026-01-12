#!/usr/bin/env python3
"""Database utilities for music analytics."""

import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional

DB_PATH = Path(__file__).parent / "listens.db"


def get_connection() -> sqlite3.Connection:
    """Get a database connection with row factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initialize the database schema."""
    conn = get_connection()
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
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_plays_timestamp ON plays(timestamp)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_plays_artist ON plays(artist)
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
):
    """Log a play to the database."""
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO plays (title, artist, album, duration_ms, played_ms, file_path)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (title, artist, album, duration_ms, played_ms, file_path),
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


# Initialize database on import
init_db()
