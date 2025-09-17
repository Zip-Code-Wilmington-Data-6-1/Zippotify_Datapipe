import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import DimSong, DimUser, DimLocation, DimArtist, DimSongArtist
from database import SessionLocal
from datetime import datetime, timezone

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

def load_artists(jsonl_path):
    session = SessionLocal()
    seen_artists = set(
        name for (name,) in session.query(DimArtist.artist_name).all()
    )

    with open(jsonl_path, "r") as f:
        for line in f:
            event = json.loads(line)
            artist_name = event.get("artist")
            if artist_name and artist_name not in seen_artists:
                artist = DimArtist(
                    artist_name=artist_name
                )
                session.add(artist)
                seen_artists.add(artist_name)

    session.commit()
    session.close()

def load_songs(jsonl_path):
    session = SessionLocal()
    seen_songs = set()
    with open(jsonl_path, "r") as f:
        for line in f:
            event = json.loads(line)
            song_title = event.get("song")
            if song_title and song_title not in seen_songs:
                song = DimSong(song_title=song_title)
                session.merge(song)
                seen_songs.add(song_title)
    
    session.commit()
    session.close()

def is_known_duo_or_group(artist_name):
    """Check if the artist name is a known duo or group that shouldn't be split"""
    known_duos_and_groups = {
        "Big & Rich",
        "Simon & Garfunkel", 
        "Hall & Oates",
        "Sonny & Cher",
        "Captain & Tennille",
        "Tears for Fears",
        "Salt-N-Pepa",
        "OutKast",
        "Black Eyed Peas",
        "Destiny's Child",
        "Lil' Jon & Ludacris",
        "Big Boi & Lil Jon",
        "Biz Markie & Slick Rick",
        # Add more as needed
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
            second_artist = artist_name[duet_start + 12:duet_end].strip()  # 12 = len(" (duet with ")
            return [first_artist, second_artist]
    
    # Handle other separators
    separators = [
        (' / ', ' / '),
        (' duet with ', ' duet with '),
        (' feat. ', ' feat. '),
        (' featuring ', ' featuring '),
        (' ft. ', ' ft. '),
        (' with ', ' with '),
        (' & ', ' & ')  # This should be last to handle known duos first
    ]
    
    for separator_lower, separator_original in separators:
        if separator_lower in artist_name.lower():
            pos = artist_name.lower().find(separator_lower)
            first_artist = artist_name[:pos].strip()
            second_artist = artist_name[pos + len(separator_lower):].strip()
            return [first_artist, second_artist]
    
    return [artist_name.strip()]

def load_song_artist_relationships(jsonl_path):
    session = SessionLocal()
    
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
    
    print(f"Added {relationships_added} song-artist relationships")
    print(f"Skipped {relationships_skipped} existing relationships")
    session.commit()
    session.close()

if __name__ == "__main__":
    # First create all tables
    from database import engine
    from models import Base
    Base.metadata.create_all(bind=engine)
    print("Tables created successfully!")
    
    # Load data in the correct order
    print("Loading users and locations...")
    load_users_and_locations("data/sample/listen_events_head.jsonl")
    
    print("Loading artists...")
    load_artists("data/sample/listen_events_head.jsonl")
    
    print("Loading songs...")
    load_songs("data/sample/listen_events_head.jsonl")
    
    print("Loading song-artist relationships...")
    load_song_artist_relationships("data/sample/listen_events_head.jsonl")
    
    print("Data loading complete!")