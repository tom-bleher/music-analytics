#!/usr/bin/env python3
"""
Music Stats CLI

Display listening statistics from the music analytics database.
"""

import argparse
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path(__file__).parent / "listens.db"

# Import analytics functions
try:
    import analytics
    HAS_ANALYTICS = True
except ImportError:
    HAS_ANALYTICS = False


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def format_duration(ms: int) -> str:
    """Format milliseconds as human-readable duration."""
    if ms is None:
        return "?"
    seconds = ms // 1000
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def format_hours(ms: int) -> str:
    """Format milliseconds as hours."""
    hours = ms / 1000 / 3600
    return f"{hours:.1f}"


def print_section(title: str):
    """Print a section header."""
    print(f"\n{'=' * 50}")
    print(f"  {title}")
    print('=' * 50)


def print_bar(value: int, max_value: int, width: int = 30) -> str:
    """Generate a text-based bar."""
    if max_value == 0:
        return ""
    filled = int((value / max_value) * width)
    return "█" * filled + "░" * (width - filled)


def get_stats(start_date: datetime = None, end_date: datetime = None):
    """Calculate all statistics for the given date range."""
    conn = get_connection()

    where_clause = "WHERE 1=1"
    params = []

    if start_date:
        where_clause += " AND timestamp >= ?"
        params.append(start_date.isoformat())
    if end_date:
        where_clause += " AND timestamp <= ?"
        params.append(end_date.isoformat())

    # Total stats
    total = conn.execute(f"""
        SELECT
            COUNT(*) as play_count,
            SUM(played_ms) as total_ms,
            COUNT(DISTINCT artist) as unique_artists,
            COUNT(DISTINCT album) as unique_albums,
            COUNT(DISTINCT title) as unique_songs
        FROM plays
        {where_clause}
    """, params).fetchone()

    # Top artists by play count
    top_artists = conn.execute(f"""
        SELECT
            artist,
            COUNT(*) as play_count,
            SUM(played_ms) as total_ms
        FROM plays
        {where_clause}
        AND artist IS NOT NULL
        GROUP BY artist
        ORDER BY play_count DESC
        LIMIT 10
    """, params).fetchall()

    # Top albums
    top_albums = conn.execute(f"""
        SELECT
            album,
            artist,
            COUNT(*) as play_count,
            SUM(played_ms) as total_ms
        FROM plays
        {where_clause}
        AND album IS NOT NULL
        GROUP BY album, artist
        ORDER BY play_count DESC
        LIMIT 10
    """, params).fetchall()

    # Top songs
    top_songs = conn.execute(f"""
        SELECT
            title,
            artist,
            COUNT(*) as play_count,
            SUM(played_ms) as total_ms
        FROM plays
        {where_clause}
        GROUP BY title, artist
        ORDER BY play_count DESC
        LIMIT 10
    """, params).fetchall()

    # Plays by hour of day
    hourly = conn.execute(f"""
        SELECT
            CAST(strftime('%H', timestamp) AS INTEGER) as hour,
            COUNT(*) as play_count
        FROM plays
        {where_clause}
        GROUP BY hour
        ORDER BY hour
    """, params).fetchall()

    # Plays by day of week
    daily = conn.execute(f"""
        SELECT
            CAST(strftime('%w', timestamp) AS INTEGER) as dow,
            COUNT(*) as play_count
        FROM plays
        {where_clause}
        GROUP BY dow
        ORDER BY dow
    """, params).fetchall()

    conn.close()

    return {
        'total': total,
        'top_artists': top_artists,
        'top_albums': top_albums,
        'top_songs': top_songs,
        'hourly': hourly,
        'daily': daily,
    }


def display_stats(stats: dict, period_name: str):
    """Display statistics in the terminal."""
    total = stats['total']

    if total['play_count'] == 0:
        print(f"\nNo listening data found for {period_name}.")
        print("Start playing music with Amberol while music-tracker is running!")
        return

    # Header
    print(f"\n{'*' * 50}")
    print(f"     MUSIC WRAPPED - {period_name.upper()}")
    print('*' * 50)

    # Overview
    print_section("OVERVIEW")
    total_hours = (total['total_ms'] or 0) / 1000 / 3600
    print(f"  Total plays:      {total['play_count']:,}")
    print(f"  Listening time:   {total_hours:.1f} hours")
    print(f"  Unique artists:   {total['unique_artists']:,}")
    print(f"  Unique albums:    {total['unique_albums']:,}")
    print(f"  Unique songs:     {total['unique_songs']:,}")

    # Top Artists
    if stats['top_artists']:
        print_section("TOP 10 ARTISTS")
        max_plays = stats['top_artists'][0]['play_count'] if stats['top_artists'] else 1
        for i, row in enumerate(stats['top_artists'], 1):
            bar = print_bar(row['play_count'], max_plays, 20)
            time_str = format_hours(row['total_ms'] or 0)
            print(f"  {i:2}. {row['artist'][:30]:<30} {bar} {row['play_count']:>4} plays ({time_str}h)")

    # Top Albums
    if stats['top_albums']:
        print_section("TOP 10 ALBUMS")
        max_plays = stats['top_albums'][0]['play_count'] if stats['top_albums'] else 1
        for i, row in enumerate(stats['top_albums'], 1):
            bar = print_bar(row['play_count'], max_plays, 20)
            album_display = f"{row['album'][:25]}" if row['album'] else "Unknown Album"
            artist_display = f"by {row['artist'][:15]}" if row['artist'] else ""
            print(f"  {i:2}. {album_display:<25} {artist_display:<18} {bar} {row['play_count']:>3}")

    # Top Songs
    if stats['top_songs']:
        print_section("TOP 10 SONGS")
        max_plays = stats['top_songs'][0]['play_count'] if stats['top_songs'] else 1
        for i, row in enumerate(stats['top_songs'], 1):
            bar = print_bar(row['play_count'], max_plays, 15)
            title_display = row['title'][:28] if row['title'] else "Unknown"
            artist_display = row['artist'][:15] if row['artist'] else ""
            print(f"  {i:2}. {title_display:<28} - {artist_display:<15} {bar} {row['play_count']:>3}")

    # Listening by hour
    if stats['hourly']:
        print_section("LISTENING BY HOUR")
        hourly_dict = {row['hour']: row['play_count'] for row in stats['hourly']}
        max_hourly = max(hourly_dict.values()) if hourly_dict else 1

        periods = [
            ("Morning (6-12)", range(6, 12)),
            ("Afternoon (12-18)", range(12, 18)),
            ("Evening (18-24)", range(18, 24)),
            ("Night (0-6)", range(0, 6)),
        ]

        for pname, hours in periods:
            count = sum(hourly_dict.get(h, 0) for h in hours)
            bar = print_bar(count, max_hourly * 6, 25)
            print(f"  {pname:<20} {bar} {count:>4}")

    # Listening by day
    if stats['daily']:
        print_section("LISTENING BY DAY OF WEEK")
        day_names = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
        daily_dict = {row['dow']: row['play_count'] for row in stats['daily']}
        max_daily = max(daily_dict.values()) if daily_dict else 1

        for dow in range(7):
            count = daily_dict.get(dow, 0)
            bar = print_bar(count, max_daily, 30)
            print(f"  {day_names[dow]:<4} {bar} {count:>4}")

    print(f"\n{'*' * 50}\n")


def display_advanced_stats(start_date: datetime, end_date: datetime, period_name: str):
    """Display advanced analytics from the analytics module."""
    if not HAS_ANALYTICS:
        print("Advanced analytics not available.")
        return

    conn = get_connection()

    # Time-based patterns
    print_section("LISTENING STREAKS")
    try:
        streaks = analytics.get_listening_streaks(conn, start_date, end_date)
        print(f"  Current streak:   {streaks['current_streak']} days")
        print(f"  Longest streak:   {streaks['longest_streak']} days")
        if streaks['longest_streak_start']:
            print(f"    ({streaks['longest_streak_start']} to {streaks['longest_streak_end']})")
    except Exception as e:
        print(f"  Error: {e}")

    print_section("LISTENING SESSIONS")
    try:
        sessions = analytics.get_sessions(conn, start_date, end_date)
        print(f"  Total sessions:   {sessions['total_sessions']}")
        print(f"  Avg session:      {sessions['avg_session_length_minutes']:.0f} minutes")
        print(f"  Longest session:  {sessions['longest_session_minutes']:.0f} minutes")
    except Exception as e:
        print(f"  Error: {e}")

    print_section("NIGHT OWL SCORE")
    try:
        night_owl = analytics.get_night_owl_score(conn, start_date, end_date)
        pct = night_owl['night_owl_percentage']
        bar = print_bar(int(pct), 100, 20)
        print(f"  Night listening:  {bar} {pct:.1f}%")
        print(f"  ({night_owl['night_plays']} plays between midnight-6am)")
    except Exception as e:
        print(f"  Error: {e}")

    print_section("BIGGEST LISTENING DAY")
    try:
        biggest = analytics.get_biggest_listening_day(conn, start_date, end_date)
        if biggest['date']:
            print(f"  Date:             {biggest['date']}")
            print(f"  Plays:            {biggest['play_count']}")
            print(f"  Listening time:   {biggest['listening_minutes']:.0f} minutes")
            if biggest['top_artist']:
                print(f"  Top artist:       {biggest['top_artist']}")
    except Exception as e:
        print(f"  Error: {e}")

    # Track behavior
    print_section("TRACK BEHAVIOR")
    try:
        skip_rate = analytics.get_skip_rate(conn, start_date, end_date)
        print(f"  Skip rate:        {skip_rate['skip_percentage']:.1f}%")
        print(f"  Skipped tracks:   {skip_rate['total_skips']} / {skip_rate['total_plays']}")
    except Exception as e:
        print(f"  Error: {e}")

    try:
        full_vs_partial = analytics.get_full_listens_vs_partial(conn, start_date, end_date)
        print(f"  Full listens:     {full_vs_partial['full_listen_percentage']:.1f}%")
        print(f"  Avg completion:   {full_vs_partial['average_completion_percentage']:.1f}%")
    except Exception as e:
        pass

    try:
        avg_length = analytics.get_average_track_length(conn, start_date, end_date)
        print(f"  Avg track length: {avg_length['average_duration_formatted']}")
    except Exception as e:
        pass

    # Artist insights
    print_section("ARTIST INSIGHTS")
    try:
        discovery = analytics.get_discovery_rate(conn, start_date, end_date)
        print(f"  Discovery rate:   {discovery['discovery_rate']:.1f}%")
        print(f"  New artists:      {discovery['new_artists']}")
        print(f"  Returning:        {discovery['returning_artists']}")
    except Exception as e:
        print(f"  Error: {e}")

    try:
        one_hits = analytics.get_one_hit_wonders(conn, start_date, end_date)
        print(f"  One-hit wonders:  {len(one_hits)} artists")
    except Exception as e:
        pass

    # Genre breakdown
    print_section("TOP GENRES")
    try:
        import db as db_module
        genres = db_module.get_genre_stats(start_date, end_date)
        if genres:
            max_plays = genres[0]['play_count'] if genres else 1
            for i, row in enumerate(genres[:10], 1):
                if row['genre']:
                    bar = print_bar(row['play_count'], max_plays, 20)
                    print(f"  {i:2}. {row['genre'][:30]:<30} {bar} {row['play_count']:>3}")
        else:
            print("  No genre data available yet")
    except Exception as e:
        print(f"  No genre data: {e}")

    # Release year breakdown
    print_section("MUSIC BY DECADE")
    try:
        import db as db_module
        years = db_module.get_release_year_stats(start_date, end_date)
        if years:
            # Group by decade
            decades = {}
            for row in years:
                if row['year']:
                    decade = row['year'][:3] + "0s"
                    decades[decade] = decades.get(decade, 0) + row['play_count']
            if decades:
                max_plays = max(decades.values())
                for decade in sorted(decades.keys(), reverse=True):
                    bar = print_bar(decades[decade], max_plays, 20)
                    print(f"  {decade:<10} {bar} {decades[decade]:>4}")
        else:
            print("  No release date data available yet")
    except Exception as e:
        print(f"  No release data: {e}")

    conn.close()


def display_milestones():
    """Display achievements and milestones."""
    if not HAS_ANALYTICS:
        print("Milestones not available.")
        return

    conn = get_connection()

    print_section("MILESTONES & ACHIEVEMENTS")
    try:
        milestones = analytics.get_milestones(conn)
        if milestones:
            for m in milestones:
                icon = m.get('icon', '🏆')
                name = m.get('name', 'Achievement')
                desc = m.get('description', '')
                date = m.get('achieved_date', '')
                print(f"  {icon} {name}")
                if desc:
                    print(f"      {desc}")
                if date:
                    print(f"      Achieved: {date}")
                print()
        else:
            print("  No milestones achieved yet. Keep listening!")
    except Exception as e:
        print(f"  Error: {e}")

    conn.close()


def display_personality(start_date: datetime, end_date: datetime):
    """Display listening personality analysis."""
    if not HAS_ANALYTICS:
        print("Personality analysis not available.")
        return

    conn = get_connection()

    print_section("YOUR LISTENING PERSONALITY")
    try:
        personality = analytics.get_listening_personality(conn, start_date, end_date)
        primary = personality.get('primary_type', 'Unknown')
        secondary = personality.get('secondary_type')
        desc = personality.get('description', '')

        print(f"  You are: {primary}")
        if secondary:
            print(f"  With hints of: {secondary}")
        if desc:
            print(f"\n  {desc}")

        traits = personality.get('traits', [])
        if traits:
            print("\n  Key traits:")
            for trait in traits[:5]:
                print(f"    - {trait}")
    except Exception as e:
        print(f"  Error: {e}")

    conn.close()


def display_fun_facts(start_date: datetime, end_date: datetime):
    """Display fun facts about listening habits."""
    if not HAS_ANALYTICS:
        print("Fun facts not available.")
        return

    conn = get_connection()

    print_section("FUN FACTS")
    try:
        facts = analytics.get_fun_facts(conn, start_date, end_date)
        if isinstance(facts, list):
            for fact in facts:
                print(f"  * {fact}")
        elif isinstance(facts, dict):
            for key, value in facts.items():
                print(f"  * {value}")
    except Exception as e:
        print(f"  Error: {e}")

    conn.close()


def display_monthly_evolution(year: int):
    """Display how taste evolved month by month."""
    if not HAS_ANALYTICS:
        print("Monthly evolution not available.")
        return

    conn = get_connection()

    print_section(f"MONTHLY TOP ARTISTS - {year}")
    try:
        monthly = analytics.get_monthly_top_artists(conn, year)
        for m in monthly:
            month_name = m.get('month_name', f"Month {m.get('month', '?')}")
            top_artist = m.get('top_artist', '-')
            plays = m.get('play_count', 0)
            if top_artist and top_artist != '-':
                bar = print_bar(plays, max(x.get('play_count', 0) for x in monthly) or 1, 15)
                print(f"  {month_name[:3]:<3}  {top_artist[:25]:<25} {bar} {plays:>3}")
            else:
                print(f"  {month_name[:3]:<3}  -")
    except Exception as e:
        print(f"  Error: {e}")

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Display your music listening statistics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  music-stats                Show this year's stats
  music-stats --month        Show this month's stats
  music-stats --week         Show last 7 days
  music-stats --year 2025    Show stats for 2025
  music-stats --all-time     Show all-time stats
  music-stats --deep         Show advanced analytics
  music-stats --milestones   Show achievements
  music-stats --personality  Show listening personality
  music-stats --fun-facts    Show fun facts
  music-stats --evolution    Show monthly taste evolution
  music-stats --full         Show everything
        """
    )

    parser.add_argument('--year', type=int, help='Show stats for a specific year')
    parser.add_argument('--month', action='store_true', help='Show stats for current month')
    parser.add_argument('--week', action='store_true', help='Show stats for last 7 days')
    parser.add_argument('--all-time', action='store_true', help='Show all-time stats')

    # Advanced options
    parser.add_argument('--deep', action='store_true', help='Show advanced analytics')
    parser.add_argument('--milestones', action='store_true', help='Show achievements')
    parser.add_argument('--personality', action='store_true', help='Show listening personality')
    parser.add_argument('--fun-facts', action='store_true', help='Show fun facts')
    parser.add_argument('--evolution', action='store_true', help='Show monthly taste evolution')
    parser.add_argument('--full', action='store_true', help='Show all stats including advanced')

    args = parser.parse_args()

    now = datetime.now()

    if args.all_time:
        start_date = None
        end_date = None
        period_name = "All Time"
    elif args.week:
        start_date = now - timedelta(days=7)
        end_date = now
        period_name = "Last 7 Days"
    elif args.month:
        start_date = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_date = now
        period_name = now.strftime("%B %Y")
    elif args.year:
        start_date = datetime(args.year, 1, 1)
        end_date = datetime(args.year, 12, 31, 23, 59, 59)
        period_name = str(args.year)
    else:
        # Default: current year
        start_date = datetime(now.year, 1, 1)
        end_date = now
        period_name = str(now.year)

    if not DB_PATH.exists():
        print("No listening data found yet.")
        print("Make sure music-tracker is running and play some music!")
        return

    # Always show basic stats
    stats = get_stats(start_date, end_date)
    display_stats(stats, period_name)

    # Show advanced stats if requested
    if args.deep or args.full:
        display_advanced_stats(start_date, end_date, period_name)

    if args.milestones or args.full:
        display_milestones()

    if args.personality or args.full:
        display_personality(start_date, end_date)

    if args.fun_facts or args.full:
        display_fun_facts(start_date, end_date)

    if args.evolution or args.full:
        year = args.year if args.year else now.year
        display_monthly_evolution(year)


if __name__ == "__main__":
    main()
