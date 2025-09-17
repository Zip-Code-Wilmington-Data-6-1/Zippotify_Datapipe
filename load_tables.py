import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import DimSong, DimUser, DimLocation, DimArtist  # Add DimArtist, DimSong
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

if __name__ == "__main__":
    load_users_and_locations("data/sample/listen_events_head.jsonl")
    load_artists("data/sample/listen_events_head.jsonl")
    load_songs("data/sample/listen_events_head.jsonl")