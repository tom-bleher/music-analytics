#!/usr/bin/env python3
"""
Library Scanner

Scans music files and extracts metadata for the local library cache.
"""

import argparse
import os
import sqlite3
import sys
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

try:
    import mutagen
    from mutagen.flac import FLAC
    from mutagen.mp3 import MP3
    from mutagen.mp4 import MP4
    from mutagen.oggvorbis import OggVorbis
    from mutagen.oggopus import OggOpus
except ImportError:
    print("Error: mutagen library required. Install with: pip install mutagen")
    sys.exit(1)

DB_PATH = Path(__file__).parent / "listens.db"

# Supported audio extensions
AUDIO_EXTENSIONS = {'.mp3', '.flac', '.ogg', '.opus', '.m4a', '.mp4', '.aac', '.wav'}


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_library_table():
    """Create the library metadata table."""
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS library (
            id INTEGER PRIMARY KEY,
            file_path TEXT UNIQUE NOT NULL,
            title TEXT,
            artist TEXT,
            album TEXT,
            album_artist TEXT,
            genre TEXT,
            composer TEXT,
            track_number INTEGER,
            track_total INTEGER,
            disc_number INTEGER,
            disc_total INTEGER,
            duration_ms INTEGER,
            release_date TEXT,
            original_date TEXT,
            label TEXT,
            isrc TEXT,
            barcode TEXT,
            musicbrainz_track_id TEXT,
            musicbrainz_album_id TEXT,
            musicbrainz_artist_id TEXT,
            musicbrainz_release_group_id TEXT,
            release_country TEXT,
            release_type TEXT,
            replaygain_track_gain REAL,
            replaygain_album_gain REAL,
            bit_rate INTEGER,
            sample_rate INTEGER,
            channels INTEGER,
            file_size INTEGER,
            last_scanned DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_library_artist ON library(artist)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_library_album ON library(album)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_library_genre ON library(genre)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_library_file_path ON library(file_path)")
    conn.commit()
    conn.close()


def get_tag_value(tags: Dict, *keys) -> Optional[str]:
    """Get first matching tag value, trying multiple key variations."""
    if not tags:
        return None
    for key in keys:
        # Try exact match
        if key in tags:
            val = tags[key]
            if isinstance(val, list):
                return str(val[0]) if val else None
            return str(val) if val else None
        # Try case-insensitive
        for k, v in tags.items():
            if k.lower() == key.lower():
                if isinstance(v, list):
                    return str(v[0]) if v else None
                return str(v) if v else None
    return None


def get_tag_int(tags: Dict, *keys) -> Optional[int]:
    """Get tag value as integer."""
    val = get_tag_value(tags, *keys)
    if val:
        try:
            # Handle "3/14" format
            if '/' in val:
                val = val.split('/')[0]
            return int(val)
        except (ValueError, TypeError):
            pass
    return None


def get_tag_float(tags: Dict, *keys) -> Optional[float]:
    """Get tag value as float."""
    val = get_tag_value(tags, *keys)
    if val:
        try:
            # Remove "dB" suffix if present
            val = val.replace('dB', '').replace(' ', '')
            return float(val)
        except (ValueError, TypeError):
            pass
    return None


def extract_metadata(file_path: str) -> Optional[Dict[str, Any]]:
    """Extract metadata from an audio file."""
    try:
        audio = mutagen.File(file_path, easy=False)
        if audio is None:
            return None

        # Get tags based on file type
        if hasattr(audio, 'tags') and audio.tags:
            tags = dict(audio.tags)
        else:
            tags = {}

        # Flatten ID3 tags (MP3)
        flat_tags = {}
        for key, val in tags.items():
            if hasattr(val, 'text'):
                flat_tags[key] = val.text
            else:
                flat_tags[key] = val

        # Get audio info
        info = audio.info if hasattr(audio, 'info') else None

        metadata = {
            'file_path': file_path,
            'title': get_tag_value(flat_tags, 'TITLE', 'TIT2', '\xa9nam', 'title'),
            'artist': get_tag_value(flat_tags, 'ARTIST', 'TPE1', '\xa9ART', 'artist'),
            'album': get_tag_value(flat_tags, 'ALBUM', 'TALB', '\xa9alb', 'album'),
            'album_artist': get_tag_value(flat_tags, 'ALBUMARTIST', 'ALBUM ARTIST', 'album_artist', 'TPE2', 'aART'),
            'genre': get_tag_value(flat_tags, 'GENRE', 'TCON', '\xa9gen', 'genre'),
            'composer': get_tag_value(flat_tags, 'COMPOSER', 'TCOM', '\xa9wrt', 'composer'),
            'track_number': get_tag_int(flat_tags, 'TRACKNUMBER', 'track', 'TRCK', 'trkn'),
            'track_total': get_tag_int(flat_tags, 'TRACKTOTAL', 'TOTALTRACKS', 'tracktotal'),
            'disc_number': get_tag_int(flat_tags, 'DISCNUMBER', 'disc', 'TPOS', 'disk'),
            'disc_total': get_tag_int(flat_tags, 'DISCTOTAL', 'TOTALDISCS', 'disctotal'),
            'release_date': get_tag_value(flat_tags, 'DATE', 'TDRC', '\xa9day', 'date', 'YEAR'),
            'original_date': get_tag_value(flat_tags, 'ORIGINALDATE', 'ORIGINALYEAR', 'TDOR'),
            'label': get_tag_value(flat_tags, 'LABEL', 'PUBLISHER', 'TPUB', 'publisher'),
            'isrc': get_tag_value(flat_tags, 'ISRC', 'TSRC'),
            'barcode': get_tag_value(flat_tags, 'BARCODE', 'UPC'),
            'musicbrainz_track_id': get_tag_value(flat_tags, 'MUSICBRAINZ_TRACKID', 'musicbrainz_trackid', 'MusicBrainz Track Id'),
            'musicbrainz_album_id': get_tag_value(flat_tags, 'MUSICBRAINZ_ALBUMID', 'musicbrainz_albumid', 'MusicBrainz Album Id'),
            'musicbrainz_artist_id': get_tag_value(flat_tags, 'MUSICBRAINZ_ARTISTID', 'musicbrainz_artistid', 'MusicBrainz Artist Id'),
            'musicbrainz_release_group_id': get_tag_value(flat_tags, 'MUSICBRAINZ_RELEASEGROUPID', 'musicbrainz_releasegroupid'),
            'release_country': get_tag_value(flat_tags, 'RELEASECOUNTRY', 'MusicBrainz Album Release Country'),
            'release_type': get_tag_value(flat_tags, 'RELEASETYPE', 'MUSICBRAINZ_ALBUMTYPE', 'MusicBrainz Album Type'),
            'replaygain_track_gain': get_tag_float(flat_tags, 'REPLAYGAIN_TRACK_GAIN', 'replaygain_track_gain'),
            'replaygain_album_gain': get_tag_float(flat_tags, 'REPLAYGAIN_ALBUM_GAIN', 'replaygain_album_gain'),
            'duration_ms': int(info.length * 1000) if info and hasattr(info, 'length') else None,
            'bit_rate': getattr(info, 'bitrate', None),
            'sample_rate': getattr(info, 'sample_rate', None),
            'channels': getattr(info, 'channels', None),
            'file_size': os.path.getsize(file_path),
        }

        return metadata

    except Exception as e:
        print(f"  Error reading {file_path}: {e}", file=sys.stderr)
        return None


def scan_directory(music_dir: str, verbose: bool = False) -> tuple[int, int, int]:
    """Scan a directory for music files and store metadata."""
    music_path = Path(music_dir).expanduser().resolve()

    if not music_path.exists():
        print(f"Error: Directory not found: {music_path}")
        return 0, 0, 0

    init_library_table()
    conn = get_connection()

    scanned = 0
    added = 0
    updated = 0
    errors = 0

    print(f"Scanning: {music_path}")
    print()

    for root, dirs, files in os.walk(music_path):
        for filename in files:
            ext = Path(filename).suffix.lower()
            if ext not in AUDIO_EXTENSIONS:
                continue

            file_path = os.path.join(root, filename)
            scanned += 1

            if verbose:
                print(f"  [{scanned}] {filename}")

            metadata = extract_metadata(file_path)
            if metadata is None:
                errors += 1
                continue

            # Check if file already exists
            existing = conn.execute(
                "SELECT id FROM library WHERE file_path = ?",
                (file_path,)
            ).fetchone()

            if existing:
                # Update existing record
                conn.execute("""
                    UPDATE library SET
                        title = ?, artist = ?, album = ?, album_artist = ?,
                        genre = ?, composer = ?, track_number = ?, track_total = ?,
                        disc_number = ?, disc_total = ?, duration_ms = ?,
                        release_date = ?, original_date = ?, label = ?, isrc = ?,
                        barcode = ?, musicbrainz_track_id = ?, musicbrainz_album_id = ?,
                        musicbrainz_artist_id = ?, musicbrainz_release_group_id = ?,
                        release_country = ?, release_type = ?, replaygain_track_gain = ?,
                        replaygain_album_gain = ?, bit_rate = ?, sample_rate = ?,
                        channels = ?, file_size = ?, last_scanned = CURRENT_TIMESTAMP
                    WHERE file_path = ?
                """, (
                    metadata['title'], metadata['artist'], metadata['album'],
                    metadata['album_artist'], metadata['genre'], metadata['composer'],
                    metadata['track_number'], metadata['track_total'],
                    metadata['disc_number'], metadata['disc_total'],
                    metadata['duration_ms'], metadata['release_date'],
                    metadata['original_date'], metadata['label'], metadata['isrc'],
                    metadata['barcode'], metadata['musicbrainz_track_id'],
                    metadata['musicbrainz_album_id'], metadata['musicbrainz_artist_id'],
                    metadata['musicbrainz_release_group_id'], metadata['release_country'],
                    metadata['release_type'], metadata['replaygain_track_gain'],
                    metadata['replaygain_album_gain'], metadata['bit_rate'],
                    metadata['sample_rate'], metadata['channels'],
                    metadata['file_size'], file_path
                ))
                updated += 1
            else:
                # Insert new record
                conn.execute("""
                    INSERT INTO library (
                        file_path, title, artist, album, album_artist, genre,
                        composer, track_number, track_total, disc_number, disc_total,
                        duration_ms, release_date, original_date, label, isrc, barcode,
                        musicbrainz_track_id, musicbrainz_album_id, musicbrainz_artist_id,
                        musicbrainz_release_group_id, release_country, release_type,
                        replaygain_track_gain, replaygain_album_gain, bit_rate,
                        sample_rate, channels, file_size
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    file_path, metadata['title'], metadata['artist'], metadata['album'],
                    metadata['album_artist'], metadata['genre'], metadata['composer'],
                    metadata['track_number'], metadata['track_total'],
                    metadata['disc_number'], metadata['disc_total'],
                    metadata['duration_ms'], metadata['release_date'],
                    metadata['original_date'], metadata['label'], metadata['isrc'],
                    metadata['barcode'], metadata['musicbrainz_track_id'],
                    metadata['musicbrainz_album_id'], metadata['musicbrainz_artist_id'],
                    metadata['musicbrainz_release_group_id'], metadata['release_country'],
                    metadata['release_type'], metadata['replaygain_track_gain'],
                    metadata['replaygain_album_gain'], metadata['bit_rate'],
                    metadata['sample_rate'], metadata['channels'], metadata['file_size']
                ))
                added += 1

            # Commit every 100 files
            if scanned % 100 == 0:
                conn.commit()
                if not verbose:
                    print(f"  Scanned {scanned} files...", end='\r')

    conn.commit()
    conn.close()

    print(f"\nScan complete!")
    print(f"  Total scanned: {scanned}")
    print(f"  New files:     {added}")
    print(f"  Updated:       {updated}")
    print(f"  Errors:        {errors}")

    return scanned, added, updated


def get_library_stats() -> Dict[str, Any]:
    """Get statistics about the library."""
    conn = get_connection()

    stats = {}

    # Total tracks
    stats['total_tracks'] = conn.execute("SELECT COUNT(*) FROM library").fetchone()[0]

    # Unique artists
    stats['unique_artists'] = conn.execute(
        "SELECT COUNT(DISTINCT artist) FROM library WHERE artist IS NOT NULL"
    ).fetchone()[0]

    # Unique albums
    stats['unique_albums'] = conn.execute(
        "SELECT COUNT(DISTINCT album) FROM library WHERE album IS NOT NULL"
    ).fetchone()[0]

    # Total duration
    total_ms = conn.execute(
        "SELECT SUM(duration_ms) FROM library WHERE duration_ms IS NOT NULL"
    ).fetchone()[0] or 0
    stats['total_hours'] = total_ms / 1000 / 3600

    # Total size
    total_bytes = conn.execute(
        "SELECT SUM(file_size) FROM library WHERE file_size IS NOT NULL"
    ).fetchone()[0] or 0
    stats['total_gb'] = total_bytes / 1024 / 1024 / 1024

    # Genre breakdown
    stats['genres'] = conn.execute("""
        SELECT genre, COUNT(*) as count
        FROM library
        WHERE genre IS NOT NULL
        GROUP BY genre
        ORDER BY count DESC
        LIMIT 10
    """).fetchall()

    # Label breakdown
    stats['labels'] = conn.execute("""
        SELECT label, COUNT(*) as count
        FROM library
        WHERE label IS NOT NULL
        GROUP BY label
        ORDER BY count DESC
        LIMIT 10
    """).fetchall()

    # Decade breakdown
    stats['decades'] = conn.execute("""
        SELECT
            SUBSTR(COALESCE(original_date, release_date), 1, 3) || '0s' as decade,
            COUNT(*) as count
        FROM library
        WHERE release_date IS NOT NULL OR original_date IS NOT NULL
        GROUP BY decade
        ORDER BY decade DESC
    """).fetchall()

    # Files with MusicBrainz IDs
    stats['musicbrainz_coverage'] = conn.execute(
        "SELECT COUNT(*) FROM library WHERE musicbrainz_track_id IS NOT NULL"
    ).fetchone()[0]

    conn.close()
    return stats


def display_library_stats():
    """Display library statistics."""
    init_library_table()
    stats = get_library_stats()

    if stats['total_tracks'] == 0:
        print("No library data. Run: music-stats --scan ~/Music")
        return

    print("\n" + "=" * 50)
    print("  LIBRARY STATISTICS")
    print("=" * 50)

    print(f"\n  Total tracks:    {stats['total_tracks']:,}")
    print(f"  Unique artists:  {stats['unique_artists']:,}")
    print(f"  Unique albums:   {stats['unique_albums']:,}")
    print(f"  Total duration:  {stats['total_hours']:.1f} hours")
    print(f"  Total size:      {stats['total_gb']:.2f} GB")
    print(f"  MusicBrainz IDs: {stats['musicbrainz_coverage']:,} tracks")

    if stats['genres']:
        print("\n  TOP GENRES:")
        for row in stats['genres']:
            print(f"    {row['genre'][:30]:<30} {row['count']:>5}")

    if stats['labels']:
        print("\n  TOP LABELS:")
        for row in stats['labels']:
            print(f"    {row['label'][:30]:<30} {row['count']:>5}")

    if stats['decades']:
        print("\n  BY DECADE:")
        for row in stats['decades']:
            if row['decade'] and row['decade'] != 's':
                print(f"    {row['decade']:<10} {row['count']:>5}")

    print()


def main():
    parser = argparse.ArgumentParser(
        description="Scan music library and extract metadata",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  library-scan ~/Music          Scan music folder
  library-scan ~/Music -v       Verbose output
  library-scan --stats          Show library statistics
        """
    )

    parser.add_argument('path', nargs='?', help='Path to music directory')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')
    parser.add_argument('--stats', action='store_true', help='Show library statistics')

    args = parser.parse_args()

    if args.stats:
        display_library_stats()
    elif args.path:
        scan_directory(args.path, verbose=args.verbose)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
