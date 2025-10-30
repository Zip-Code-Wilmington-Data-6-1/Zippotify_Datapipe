import json
import pandas as pd
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from models import DimSong, DimUser, DimLocation, DimArtist, DimSongArtist, DimTime, FactPlays
from database import SessionLocal, engine
from datetime import datetime, timezone
import gc

def count_unique_records_in_files(files, field_name):
    """Count unique records across all files for a given field"""
    unique_items = set()
    for file_path in files:
        try:
            with open(file_path, "r") as f:
                for line in f:
                    event = json.loads(line)
                    value = event.get(field_name)
                    if value:
                        unique_items.add(value)
        except FileNotFoundError:
            print(f"Warning: File {file_path} not found, skipping...")
            continue
    return len(unique_items)

def is_table_fully_loaded(table_class, files, field_name):
    """Check if a table is already fully loaded by comparing record counts"""
    session = SessionLocal()
    try:
        db_count = session.query(func.count(getattr(table_class, table_class.__table__.primary_key.columns.keys()[0]))).scalar()
        file_count = count_unique_records_in_files(files, field_name)
        
        print(f"  {table_class.__tablename__}: DB={db_count}, Files={file_count}")
        return db_count >= file_count and db_count > 0
    finally:
        session.close()

def is_known_duo_or_group(artist_name):
    """Check if the artist name is a known duo or group that shouldn't be split"""
    known_duos_and_groups = {
        "Big & Rich", "Simon & Garfunkel", "Hall & Oates", "Sonny & Cher",
        "Captain & Tennille", "Tears for Fears", "Salt-N-Pepa", "OutKast",
        "Black Eyed Peas", "Destiny's Child", "Lil' Jon & Ludacris",
        "Big Boi & Lil Jon", "Biz Markie & Slick Rick",
    }
    return artist_name in known_duos_and_groups

def split_artists(artist_name):
    """Split artist names while respecting known duos/groups"""
    if is_known_duo_or_group(artist_name):
        return [artist_name.strip()]

    # Handle parenthetical collaborations like "3T (Duet with Michael Jackson)"
    if " (duet with " in artist_name.lower() and ")" in artist_name:
        duet_start = artist_name.lower().find(" (duet with ")
        duet_end = artist_name.find(")", duet_start)
        if duet_end > duet_start:
            first_artist = artist_name[:duet_start].strip()
            second_artist = artist_name[duet_start + 12:duet_end].strip()
            return [first_artist, second_artist]

    # List of separators to split on (order matters)
    separators = [
        ' / ',
        ' duet with ',
        ' feat. ',
        ' featuring ',
        ' ft. ',
        ' with ',
        ' & ',  # This should be last to handle known duos first
    ]

    # Split using all separators
    import re
    pattern = '|'.join(map(re.escape, separators))
    split_names = [name.strip() for name in re.split(pattern, artist_name) if name.strip()]

    # Truncate artist names to prevent overflow
    max_length = 255
    split_names = [name[:max_length] for name in split_names]

    return split_names if split_names else [artist_name.strip()]

def load_users_and_locations(jsonl_path, chunk_size=5000):
    # Fetch existing user_ids and locations
    existing_users = set(pd.read_sql_query("SELECT user_id FROM dim_user", engine)['user_id'])
    existing_locs = set(
        tuple(x) for x in pd.read_sql_query(
            "SELECT city, state, latitude, longitude FROM dim_location", engine
        ).values.tolist()
    )

    new_users = []
    new_locs = []
    with open(jsonl_path, "r") as f:
        for line in f:
            try:
                event = json.loads(line)
                # Users
                user_id = event.get('userId')
                if user_id and user_id not in existing_users:
                    new_users.append({
                        'user_id': user_id,
                        'first_name': event.get('firstName'),
                        'last_name': event.get('lastName'),
                        'gender': event.get('gender'),
                        'registration_ts': pd.to_datetime(event.get('registration'), unit='ms', utc=True) if event.get('registration') else None,
                        'birthday': event.get('birth')
                    })
                    existing_users.add(user_id)
                # Locations
                loc_tuple = (event.get('city'), event.get('state'), event.get('lat'), event.get('lon'))
                if all(loc_tuple) and loc_tuple not in existing_locs:
                    new_locs.append({
                        'city': event.get('city'),
                        'state': event.get('state'),
                        'latitude': event.get('lat'),
                        'longitude': event.get('lon')
                    })
                    existing_locs.add(loc_tuple)
                # Chunk insert
                if len(new_users) >= chunk_size:
                    pd.DataFrame(new_users).to_sql('dim_user', engine, if_exists='append', index=False)
                    print(f"Inserted {len(new_users)} new users")
                    new_users.clear()
                if len(new_locs) >= chunk_size:
                    pd.DataFrame(new_locs).to_sql('dim_location', engine, if_exists='append', index=False)
                    print(f"Inserted {len(new_locs)} new locations")
                    new_locs.clear()
            except Exception as e:
                print(f"Skipping line due to error: {e}")
    # Insert any remaining
    if new_users:
        pd.DataFrame(new_users).to_sql('dim_user', engine, if_exists='append', index=False)
        print(f"Inserted {len(new_users)} new users (final chunk)")
    if new_locs:
        pd.DataFrame(new_locs).to_sql('dim_location', engine, if_exists='append', index=False)
        print(f"Inserted {len(new_locs)} new locations (final chunk)")

def load_artists(jsonl_paths, chunk_size=5000):
    import gc
    # Fetch all existing artist names in a set (if too large, consider a DB index and check per chunk)
    try:
        existing = set(pd.read_sql_query("SELECT artist_name FROM dim_artist", engine)['artist_name'])
    except Exception as e:
        print("Error fetching existing artists:", e)
        existing = set()

    new_artists = set()
    for path in jsonl_paths:
        with open(path, "r") as f:
            chunk = []
            for line in f:
                try:
                    event = json.loads(line)
                    artist_field = event.get('artist')
                    if artist_field:
                        for single_artist in split_artists(artist_field):
                            if single_artist not in existing and single_artist not in new_artists:
                                chunk.append(single_artist)
                                new_artists.add(single_artist)
                    if len(chunk) >= chunk_size:
                        df_chunk = pd.DataFrame({'artist_name': chunk})
                        df_chunk.to_sql('dim_artist', engine, if_exists='append', index=False)
                        print(f"Inserted {len(chunk)} new artists")
                        chunk.clear()
                        gc.collect()
                except Exception as e:
                    print(f"Skipping line due to error: {e}")
            # Insert any remaining artists in the last chunk
            if chunk:
                df_chunk = pd.DataFrame({'artist_name': chunk})
                df_chunk.to_sql('dim_artist', engine, if_exists='append', index=False)
                print(f"Inserted {len(chunk)} new artists (final chunk)")
                gc.collect()

def load_songs(jsonl_paths, chunk_size=5000):
    existing = set(pd.read_sql_query("SELECT song_title FROM dim_song", engine)['song_title'])
    new_songs = set()
    for path in jsonl_paths:
        with open(path, "r") as f:
            chunk = []
            for line in f:
                try:
                    event = json.loads(line)
                    song_title = event.get('song')
                    if song_title and song_title not in existing and song_title not in new_songs:
                        chunk.append({'song_title': song_title[:255]})
                        new_songs.add(song_title)
                    if len(chunk) >= chunk_size:
                        pd.DataFrame(chunk).to_sql('dim_song', engine, if_exists='append', index=False)
                        print(f"Inserted {len(chunk)} new songs")
                        chunk.clear()
                except Exception as e:
                    print(f"Skipping line due to error: {e}")
            if chunk:
                pd.DataFrame(chunk).to_sql('dim_song', engine, if_exists='append', index=False)
                print(f"Inserted {len(chunk)} new songs (final chunk)")

def load_song_artist_relationships(jsonl_paths, chunk_size=5000):
    session = SessionLocal()
    songs_df = pd.read_sql_table('dim_song', engine)
    artists_df = pd.read_sql_table('dim_artist', engine)
    song_title_to_id = dict(zip(songs_df['song_title'], songs_df['song_id']))
    artist_name_to_id = dict(zip(artists_df['artist_name'], artists_df['artist_id']))
    existing = set(
        tuple(x) for x in pd.read_sql_query(
            "SELECT song_id, artist_id FROM dim_song_artist", engine
        ).values.tolist()
    )
    rels = []
    for path in jsonl_paths:
        with open(path, "r") as f:
            for line in f:
                try:
                    event = json.loads(line)
                    song_title = event.get('song')
                    artist_name = event.get('artist')
                    if not song_title or not artist_name:
                        continue
                    song_id = song_title_to_id.get(song_title)
                    for single_artist in split_artists(artist_name):
                        artist_id = artist_name_to_id.get(single_artist)
                        if song_id and artist_id and (song_id, artist_id) not in existing:
                            rels.append({'song_id': song_id, 'artist_id': artist_id})
                            existing.add((song_id, artist_id))
                        if len(rels) >= chunk_size:
                            pd.DataFrame(rels).to_sql('dim_song_artist', engine, if_exists='append', index=False)
                            print(f"Inserted {len(rels)} new song-artist relationships")
                            rels.clear()
                except Exception as e:
                    print(f"Skipping line due to error: {e}")
    if rels:
        pd.DataFrame(rels).to_sql('dim_song_artist', engine, if_exists='append', index=False)
        print(f"Inserted {len(rels)} new song-artist relationships (final chunk)")
    session.close()

def get_or_create_time(session, dt):
    date = dt.date()
    hour = dt.hour
    weekday = dt.strftime('%A')
    month = dt.month
    year = dt.year
    existing = session.query(DimTime).filter_by(
        date=date,
        hour=hour,
        weekday=weekday,
        month=month,
        year=year
    ).first()
    if existing:
        return existing
    dim_time = DimTime(
        date=date,
        hour=hour,
        weekday=weekday,
        month=month,
        year=year
    )
    session.add(dim_time)
    session.commit()
    return dim_time

def load_fact_plays(jsonl_path, chunk_size=5000):
    session = SessionLocal()
    users_df = pd.read_sql_table('dim_user', engine)
    songs_df = pd.read_sql_table('dim_song', engine)
    artists_df = pd.read_sql_table('dim_artist', engine)
    locations_df = pd.read_sql_table('dim_location', engine)
    times_df = pd.read_sql_table('dim_time', engine)
    df = pd.read_json(jsonl_path, lines=True)
    df = df.dropna(subset=['userId', 'song', 'artist', 'city', 'state', 'lat', 'lon', 'ts'])
    df = df.rename(columns={
        'userId': 'user_id',
        'song': 'song_title',
        'artist': 'artist_name',
        'city': 'city',
        'state': 'state',
        'lat': 'latitude',
        'lon': 'longitude',
        'sessionId': 'session_id',
        'level': 'user_level',
        'duration': 'duration_ms',
    })
    # Convert duration from seconds to ms (if not already in ms)
    if 'duration_ms' in df.columns:
        df['duration_ms'] = (df['duration_ms'] * 1000).astype(int)
    df = df.merge(users_df[['user_id']], on='user_id', how='inner')
    df = df.merge(songs_df[['song_id', 'song_title']], on='song_title', how='inner')
    df = df.merge(artists_df[['artist_id', 'artist_name']], on='artist_name', how='inner')
    df = df.merge(locations_df[['location_id', 'city', 'state', 'latitude', 'longitude']], on=['city', 'state', 'latitude', 'longitude'], how='inner')
    # Handle time dimension
    fact_plays = []
    for _, row in df.iterrows():
        ts = row['ts']
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        dim_time = get_or_create_time(session, dt)
        fact_plays.append({
            'user_id': row['user_id'],
            'song_id': row['song_id'],
            'artist_id': row['artist_id'],
            'location_id': row['location_id'],
            'time_key': dim_time.time_key,
            'duration_ms': row.get('duration_ms'),
            'session_id': str(row.get('session_id')) if not pd.isna(row.get('session_id')) else None,
            'user_level': row.get('user_level')
        })
    fact_plays_df = pd.DataFrame(fact_plays)
    print(f"Inserting {len(fact_plays_df)} new fact plays")
    for start in range(0, len(fact_plays_df), chunk_size):
        chunk = fact_plays_df.iloc[start:start+chunk_size]
        chunk.to_sql('fact_plays', engine, if_exists='append', index=False)
    session.close()

if __name__ == "__main__":
    from database import engine
    from models import Base

    # Create tables if they don't exist
    Base.metadata.create_all(bind=engine)
    print("Tables created successfully!")

    # List your input files (adjust as needed)
    files = [
        "output/auth_events.json",
        "output/listen_events.json",
        "output/page_view_events.json",
        "output/status_change_events.json"
    ]

    print("\nChecking which tables need to be loaded...")

    # Users & Locations
    session = SessionLocal()
    user_count = session.query(func.count(DimUser.user_id)).scalar()
    location_count = session.query(func.count(DimLocation.location_id)).scalar()
    session.close()
    if user_count == 0 or location_count == 0:
        print("Loading users and locations...")
        # Use pandas to concatenate all files for efficient loading
        for file in files:
            try:
                load_users_and_locations(file)
            except Exception as e:
                print(f"Error loading users/locations from {file}: {e}")
    else:
        print(f"✓ Users and locations already loaded (Users: {user_count}, Locations: {location_count})")

    # Artists
    if not is_table_fully_loaded(DimArtist, files, "artist"):
        print("Loading artists...")
        try:
            load_artists(files)
        except Exception as e:
            print(f"Error loading artists: {e}")
    else:
        print("✓ Artists already loaded.")

    # Songs
    if not is_table_fully_loaded(DimSong, files, "song"):
        print("Loading songs...")
        try:
            load_songs(files)
        except Exception as e:
            print(f"Error loading songs: {e}")
    else:
        print("✓ Songs already loaded.")

    # Song-Artist Relationships
    if not is_table_fully_loaded(DimSongArtist, files, "song"):
        print("Loading song-artist relationships...")
        try:
            load_song_artist_relationships(files)
        except Exception as e:
            print(f"Error loading song-artist relationships: {e}")
    else:
        print("✓ Song-artist relationships already loaded.")

    # Fact Plays
    if not is_table_fully_loaded(FactPlays, files, "song"):
        print("Loading fact plays...")
        for file in files:
            try:
                load_fact_plays(file)
            except Exception as e:
                print(f"Error loading fact plays from {file}: {e}")
    else:
        print("✓ Fact plays already loaded.")

    print("\nData loading complete!")
