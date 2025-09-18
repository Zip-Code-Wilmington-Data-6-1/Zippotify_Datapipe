import json
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from models import DimSong, DimUser, DimLocation, DimArtist, DimSongArtist, DimTime, FactPlays
from database import SessionLocal
from datetime import datetime, timezone

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

def load_users_and_locations(jsonl_path):
    session = SessionLocal()
    seen_users = set()
    seen_locations = {}

    with open(jsonl_path, "r") as f:
        for line in f:
            event = json.loads(line)
            # --- USERS ---
            user_id = event.get("userId")
            if user_id and user_id not in seen_users:
                user = DimUser(
                    user_id=user_id,
                    first_name=event.get("firstName"),
                    last_name=event.get("lastName"),
                    gender=event.get("gender"),
                    registration_ts=datetime.fromtimestamp(event["registration"]/1000, tz=timezone.utc) if event.get("registration") else None,
                    birthday=event.get("birth")
                )
                session.merge(user)
                seen_users.add(user_id)

            # --- LOCATIONS ---
            loc_key = (event.get("city"), event.get("state"), event.get("lat"), event.get("lon"))
            if all(loc_key) and loc_key not in seen_locations:
                location = DimLocation(
                    location_id=None,
                    city=event.get("city"),
                    state=event.get("state"),
                    latitude=event.get("lat"),
                    longitude=event.get("lon")
                )
                session.add(location)
                seen_locations[loc_key] = location

    session.commit()
    session.close()

def load_artists(jsonl_paths):
    """Load artists from multiple files at once to avoid duplicates"""
    session = SessionLocal()
    seen_artists = set(
        name for (name,) in session.query(DimArtist.artist_name).all()
    )

    for jsonl_path in jsonl_paths:
        print(f"Processing artists from {jsonl_path}...")
        try:
            with open(jsonl_path, "r") as f:
                for line in f:
                    event = json.loads(line)
                    artist_name = event.get("artist")
                    if artist_name and artist_name not in seen_artists:
                        # Truncate long artist names and add indicator
                        if len(artist_name) > 255:
                            print(f"Warning: Truncating long artist name: {artist_name[:100]}...")
                            artist_name = artist_name[:252] + "..."
                        
                        # Check again after truncation
                        if artist_name not in seen_artists:
                            artist = DimArtist(artist_name=artist_name)
                            session.add(artist)
                            seen_artists.add(artist_name)
        except FileNotFoundError:
            print(f"Warning: File {jsonl_path} not found, skipping...")
            continue

    session.commit()
    session.close()

def load_songs(jsonl_paths):
    """Load songs from multiple files at once to avoid duplicates"""
    session = SessionLocal()
    seen_songs = set(
        title for (title,) in session.query(DimSong.song_title).all()
    )
    
    for jsonl_path in jsonl_paths:
        print(f"Processing songs from {jsonl_path}...")
        try:
            with open(jsonl_path, "r") as f:
                for line in f:
                    event = json.loads(line)
                    song_title = event.get("song")
                    if song_title and song_title not in seen_songs:
                        # Truncate long song titles
                        if len(song_title) > 255:
                            print(f"Warning: Truncating long song title: {song_title[:100]}...")
                            song_title = song_title[:252] + "..."
                        
                        if song_title not in seen_songs:
                            song = DimSong(song_title=song_title)
                            session.add(song)
                            seen_songs.add(song_title)
        except FileNotFoundError:
            print(f"Warning: File {jsonl_path} not found, skipping...")
            continue
    
    session.commit()
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
    
    # Handle other separators
    separators = [
        (' / ', ' / '), (' duet with ', ' duet with '), (' feat. ', ' feat. '),
        (' featuring ', ' featuring '), (' ft. ', ' ft. '), (' with ', ' with '),
        (' & ', ' & ')  # This should be last to handle known duos first
    ]
    
    for separator_lower, separator_original in separators:
        if separator_lower in artist_name.lower():
            pos = artist_name.lower().find(separator_lower)
            first_artist = artist_name[:pos].strip()
            second_artist = artist_name[pos + len(separator_lower):].strip()
            return [first_artist, second_artist]
    
    return [artist_name.strip()]

def load_song_artist_relationships(jsonl_paths):
    session = SessionLocal()
    
    # Check if relationships are already loaded
    existing_count = session.query(func.count(DimSongArtist.song_id)).scalar()
    if existing_count > 0:
        print(f"Song-artist relationships already exist ({existing_count} records). Checking if complete...")
    
    # Load all songs and artists into memory for faster lookup
    print("Loading songs and artists into memory...")
    songs_dict = {song.song_title: song for song in session.query(DimSong).all()}
    artists_dict = {artist.artist_name: artist for artist in session.query(DimArtist).all()}
    
    relationships_added = 0
    relationships_skipped = 0
    existing_relationships = set()
    
    # Load existing relationships to avoid duplicates
    for rel in session.query(DimSongArtist).all():
        existing_relationships.add((rel.song_id, rel.artist_id))
    
    for jsonl_path in jsonl_paths:
        print(f"Processing relationships from {jsonl_path}...")
        try:
            with open(jsonl_path, "r") as f:
                for line in f:
                    event = json.loads(line)
                    song_title = event.get("song")
                    artist_name = event.get("artist")
                    
                    if song_title and artist_name:
                        # Get song from memory
                        song = songs_dict.get(song_title)
                        if not song:
                            continue
                        
                        # Split artist names
                        artist_names = split_artists(artist_name)
                        
                        # Insert relationships for each artist
                        for single_artist_name in artist_names:
                            if not single_artist_name:
                                continue
                                
                            artist = artists_dict.get(single_artist_name)
                            if artist:
                                relationship_key = (song.song_id, artist.artist_id)
                                if relationship_key not in existing_relationships:
                                    song_artist = DimSongArtist(
                                        song_id=song.song_id,
                                        artist_id=artist.artist_id
                                    )
                                    session.add(song_artist)
                                    existing_relationships.add(relationship_key)
                                    relationships_added += 1
                                else:
                                    relationships_skipped += 1
        except FileNotFoundError:
            print(f"Warning: File {jsonl_path} not found, skipping...")
            continue
    
    print(f"Added {relationships_added} song-artist relationships")
    print(f"Skipped {relationships_skipped} existing relationships")
    session.commit()
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

def load_fact_plays(jsonl_path):
    session = SessionLocal()
    # Preload dimension tables for fast lookup
    users = {u.user_id: u for u in session.query(DimUser).all()}
    songs = {s.song_title: s for s in session.query(DimSong).all()}
    artists = {a.artist_name: a for a in session.query(DimArtist).all()}
    locations = {(l.city, l.state, float(l.latitude), float(l.longitude)): l for l in session.query(DimLocation).all()}
    times = {}  # cache for datetime to time_key

    with open(jsonl_path, "r") as f:
        for line in f:
            event = json.loads(line)
            user_id = event.get("userId")
            song_title = event.get("song")
            artist_name = event.get("artist")
            city, state, lat, lon = event.get("city"), event.get("state"), event.get("lat"), event.get("lon")
            ts = event.get("ts") or event.get("timestamp") or event.get("play_ts") or event.get("registration")
            if not (user_id and song_title and artist_name and city and state and lat and lon and ts):
                continue

            # Convert timestamp to datetime
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            # Get or create DimTime
            if dt not in times:
                dim_time = get_or_create_time(session, dt)
                times[dt] = dim_time
            else:
                dim_time = times[dt]

            # Get dimension IDs
            user = users.get(user_id)
            song = songs.get(song_title)
            artist = artists.get(artist_name)
            location = locations.get((city, state, float(lat), float(lon)))

            if not (user and song and artist and location and dim_time):
                continue

            fact_play = FactPlays(
                user_id=user.user_id,
                song_id=song.song_id,
                artist_id=artist.artist_id,
                location_id=location.location_id,
                time_key=dim_time.time_key
            )
            session.add(fact_play)

    session.commit()
    session.close()

if __name__ == "__main__":
    # First create all tables
    from database import engine
    from models import Base
    Base.metadata.create_all(bind=engine)
    print("Tables created successfully!")
    
    # Define file list
    files = [
        "data/sample/listen_events_head.jsonl",
        "output/auth_events.json",
        "output/listen_events.json",
        "output/page_view_events.json",
        "output/status_change_events.json"
    ]
    
    print("\nChecking which tables need to be loaded...")
    
    # Check users (more complex due to combined user/location function)
    session = SessionLocal()
    user_count = session.query(func.count(DimUser.user_id)).scalar()
    location_count = session.query(func.count(DimLocation.location_id)).scalar()
    session.close()
    
    if user_count == 0 or location_count == 0:
        print("Loading users and locations...")
        for file in files:
            load_users_and_locations(file)
    else:
        print(f"✓ Users and locations already loaded (Users: {user_count}, Locations: {location_count})")
    
    # Check artists
    if not is_table_fully_loaded(DimArtist, files, "artist"):
        print("Loading artists...")
        load_artists(files)
    else:
        print("✓ Artists already fully loaded")
    
    # Check songs  
    if not is_table_fully_loaded(DimSong, files, "song"):
        print("Loading songs...")
        load_songs(files)
    else:
        print("✓ Songs already fully loaded")
    
    # Always check relationships as they depend on other tables
    print("Loading song-artist relationships...")
    load_song_artist_relationships(files)

    print("\nData loading complete!")