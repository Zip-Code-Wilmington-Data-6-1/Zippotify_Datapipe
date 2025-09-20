# filepath: Zippotify_Datapipe/api.py
from fastapi import FastAPI, Depends, HTTPException
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text, func, desc
from database import SessionLocal

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        
from models import DimArtist, DimLocation, DimUser, DimSong, DimGenre, DimSongGenre, DimTime, FactPlays
from pydantic import BaseModel
from datetime import datetime, date

app = FastAPI()

class User(BaseModel):
    user_id: int
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    gender: Optional[str] = None
    registration_ts: Optional[datetime] = None  # Changed from int to datetime
    birthday: Optional[int] = None              # Made optional since it can be None

    class Config:
        from_attributes = True  # Updated from orm_mode for Pydantic v2

class Artist(BaseModel):
    artist_id: int
    artist_name: str

    class Config:
        from_attributes = True

class Location(BaseModel):  # Capitalized class name following convention
    location_id: int
    city: Optional[str] = None
    state: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    class Config:
        from_attributes = True

class Song(BaseModel):
    song_id: int
    song_title: str

    class Config:
        from_attributes = True

class Genre(BaseModel):
    genre_id: int
    genre_name: str

    class Config:
        from_attributes = True

class SongGenre(BaseModel):
    song_id: int
    genre_id: int

    class Config:
        from_attributes = True

class Time(BaseModel):
    time_key: int
    date: date
    hour: int
    weekday: str
    month: int
    year: int

    class Config:
        from_attributes = True

class FactPlay(BaseModel):
    play_id: int
    user_id: int
    song_id: int
    artist_id: int
    location_id: int
    time_key: int
    duration_ms: Optional[int] = None
    session_id: Optional[str] = None
    user_level: Optional[str] = None

    class Config:
        from_attributes = True

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/users", response_model=List[User])
def get_users(db: Session = Depends(get_db)):
    return db.query(DimUser).all()

@app.get("/users/{user_id}", response_model=User)
def get_user(user_id: int, db: Session = Depends(get_db)):
    user = db.query(DimUser).filter(DimUser.user_id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user

@app.get("/artists", response_model=List[Artist])
def get_artists(db: Session = Depends(get_db)):
    return db.query(DimArtist).all()

@app.get("/artists/{artist_id}", response_model=Artist)
def get_artist(artist_id: int, db: Session = Depends(get_db)):
    artist = db.query(DimArtist).filter(DimArtist.artist_id == artist_id).first()
    if not artist:
        raise HTTPException(status_code=404, detail="Artist not found")
    return artist

@app.get("/locations", response_model=List[Location])
def get_locations(db: Session = Depends(get_db)):
    return db.query(DimLocation).all()

@app.get("/locations/{location_id}", response_model=Location)
def get_location(location_id: int, db: Session = Depends(get_db)):
    location = db.query(DimLocation).filter(DimLocation.location_id == location_id).first()
    if not location:
        raise HTTPException(status_code=404, detail="Location not found")
    return location

@app.get("/songs", response_model=List[Song])
def get_songs(db: Session = Depends(get_db)):
    return db.query(DimSong).all()

@app.get("/songs/{song_id}", response_model=Song)
def get_song(song_id: int, db: Session = Depends(get_db)):
    song = db.query(DimSong).filter(DimSong.song_id == song_id).first()
    if not song:
        raise HTTPException(status_code=404, detail="Song not found")
    return song

@app.get("/genres", response_model=List[Genre])
def get_genres(db: Session = Depends(get_db)):
    return db.query(DimGenre).all()

@app.get("/genres/{genre_id}", response_model=Genre)
def get_genre(genre_id: int, db: Session = Depends(get_db)):
    genre = db.query(DimGenre).filter(DimGenre.genre_id == genre_id).first()
    if not genre:
        raise HTTPException(status_code=404, detail="Genre not found")
    return genre

@app.get("/song_genres", response_model=List[SongGenre])
def get_song_genres(db: Session = Depends(get_db)):
    return db.query(DimSongGenre).all()

@app.get("/song_genres/{song_id}", response_model=List[Genre])
def get_genres_for_song(song_id: int, db: Session = Depends(get_db)):
    song_genres = db.query(DimSongGenre).filter(DimSongGenre.song_id == song_id).all()
    genre_ids = [sg.genre_id for sg in song_genres]
    return db.query(DimGenre).filter(DimGenre.genre_id.in_(genre_ids)).all()

@app.get("/times", response_model=List[Time])
def get_times(db: Session = Depends(get_db)):
    return db.query(DimTime).all()

@app.get("/fact_plays", response_model=List[FactPlay])
def get_fact_plays(db: Session = Depends(get_db)):
    return db.query(FactPlays).all()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/health/db")
def health_db():
    try:
        with SessionLocal() as s:
            s.execute(text("SELECT 1"))
        return {"db": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Example: Metadata endpoint
@app.get("/metadata")
def get_metadata(db: Session = Depends(get_db)):
    total_users = db.query(DimUser).count()
    total_listen_events = db.query(FactPlays).count()
    total_page_views = db.query(FactPlays).filter(FactPlays.session_id.isnot(None)).count()  # Example logic
    total_status_changes = db.query(FactPlays).filter(FactPlays.user_level.isnot(None)).count()  # Example logic

    # Get min/max date from DimTime
    date_range = db.query(func.min(DimTime.date), func.max(DimTime.date)).first()
    return {
        "generated_at": datetime.now().isoformat(),
        "total_users": total_users,
        "total_listen_events": total_listen_events,
        "total_page_views": total_page_views,
        "total_status_changes": total_status_changes,
        "date_range": {"start": str(date_range[0]), "end": str(date_range[1])}
    }

# Example: Daily active users
@app.get("/user_analytics/daily_active_users")
def get_daily_active_users(db: Session = Depends(get_db)):
    # Count distinct users per day
    results = (
        db.query(DimTime.date, func.count(func.distinct(FactPlays.user_id)).label("active_users"))
        .join(FactPlays, FactPlays.time_key == DimTime.time_key)
        .group_by(DimTime.date)
        .order_by(DimTime.date)
        .all()
    )
    return [{"date": str(r[0]), "active_users": r[1]} for r in results]

# Example: Age distribution
@app.get("/user_analytics/age_distribution")
def get_age_distribution(db: Session = Depends(get_db)):
    # Example: group users by age (assuming birthday is year of birth)
    current_year = datetime.now().year
    results = (
        db.query((current_year - DimUser.birthday).label("age"), func.count(DimUser.user_id))
        .group_by("age")
        .order_by("age")
        .all()
    )
    return [{"age": r[0], "count": r[1]} for r in results if r[0] is not None]

# Example: Subscription levels
@app.get("/user_analytics/subscription_levels")
def get_subscription_levels(db: Session = Depends(get_db)):
    # Get distinct users by subscription level from FactPlays
    results = (
        db.query(FactPlays.user_level, func.count(func.distinct(FactPlays.user_id)).label("user_count"))
        .group_by(FactPlays.user_level)
        .all()
    )
    subscription_levels = {}
    for level, count in results:
        if level:  # Only count non-null levels
            subscription_levels[level] = count
    return subscription_levels

# Example: Genre popularity
@app.get("/content_analytics/genre_popularity")
def get_genre_popularity(db: Session = Depends(get_db)):
    results = (
        db.query(DimGenre.genre_name, func.count(FactPlays.play_id).label("play_count"))
        .join(DimSongGenre, DimSongGenre.genre_id == DimGenre.genre_id)
        .join(FactPlays, FactPlays.song_id == DimSongGenre.song_id)
        .group_by(DimGenre.genre_name)
        .order_by(desc("play_count"))
        .all()
    )
    return [{"genre": r[0], "play_count": r[1]} for r in results]

# Example: Top artists
@app.get("/content_analytics/top_artists")
def get_top_artists(db: Session = Depends(get_db)):
    results = (
        db.query(DimArtist.artist_name, func.count(FactPlays.play_id).label("play_count"))
        .join(FactPlays, FactPlays.artist_id == DimArtist.artist_id)
        .group_by(DimArtist.artist_name)
        .order_by(desc("play_count"))
        .limit(10)
        .all()
    )
    return [{"artist": r[0], "play_count": r[1]} for r in results]

# Example: Top songs by state
@app.get("/content_analytics/top_songs_by_state")
def get_top_songs_by_state(db: Session = Depends(get_db)):
    results = (
        db.query(DimLocation.state, DimSong.song_title, func.count(FactPlays.play_id).label("play_count"))
        .join(FactPlays, FactPlays.location_id == DimLocation.location_id)
        .join(DimSong, FactPlays.song_id == DimSong.song_id)
        .group_by(DimLocation.state, DimSong.song_title)
        .order_by(DimLocation.state, desc("play_count"))
        .all()
    )
    # Group by state
    from collections import defaultdict
    state_songs = defaultdict(list)
    for state, song, count in results:
        state_songs[state].append({"song": song, "play_count": count})
    return state_songs

# Top song per state
@app.get("/content_analytics/top_song_per_state")
def get_top_song_per_state(db: Session = Depends(get_db)):
    from sqlalchemy import func
    subq = (
        db.query(
            FactPlays.location_id,
            FactPlays.song_id,
            func.count(FactPlays.play_id).label("play_count")
        )
        .group_by(FactPlays.location_id, FactPlays.song_id)
        .subquery()
    )
    results = (
        db.query(DimLocation.state, DimSong.song_title, subq.c.play_count)
        .join(subq, subq.c.location_id == DimLocation.location_id)
        .join(DimSong, subq.c.song_id == DimSong.song_id)
        .order_by(DimLocation.state, subq.c.play_count.desc())
        .all()
    )
    # For each state, pick the top song
    top_per_state = {}
    for state, song, count in results:
        if state not in top_per_state:
            top_per_state[state] = {"song": song, "play_count": count}
    return top_per_state

# Top artists by state
@app.get("/content_analytics/top_artists_by_state")
def get_top_artists_by_state(db: Session = Depends(get_db)):
    results = (
        db.query(DimLocation.state, DimArtist.artist_name, func.count(FactPlays.play_id).label("play_count"))
        .join(FactPlays, FactPlays.location_id == DimLocation.location_id)
        .join(DimArtist, FactPlays.artist_id == DimArtist.artist_id)
        .group_by(DimLocation.state, DimArtist.artist_name)
        .order_by(DimLocation.state, desc("play_count"))
        .all()
    )
    from collections import defaultdict
    state_artists = defaultdict(list)
    for state, artist, count in results:
        state_artists[state].append({"artist": artist, "play_count": count})
    return state_artists

# Top artist per state
@app.get("/content_analytics/top_artist_per_state")
def get_top_artist_per_state(db: Session = Depends(get_db)):
    from sqlalchemy import func
    subq = (
        db.query(
            FactPlays.location_id,
            FactPlays.artist_id,
            func.count(FactPlays.play_id).label("play_count")
        )
        .group_by(FactPlays.location_id, FactPlays.artist_id)
        .subquery()
    )
    results = (
        db.query(DimLocation.state, DimArtist.artist_name, subq.c.play_count)
        .join(subq, subq.c.location_id == DimLocation.location_id)
        .join(DimArtist, subq.c.artist_id == DimArtist.artist_id)
        .order_by(DimLocation.state, subq.c.play_count.desc())
        .all()
    )
    # For each state, pick the top artist
    top_per_state = {}
    for state, artist, count in results:
        if state not in top_per_state:
            top_per_state[state] = {"artist": artist, "play_count": count}
    return top_per_state

# Average plays per session
@app.get("/content_analytics/average_plays_per_session")
def get_average_plays_per_session(db: Session = Depends(get_db)):
    total_plays = db.query(FactPlays.session_id, func.count(FactPlays.play_id)).group_by(FactPlays.session_id).all()
    if not total_plays:
        return {"average_plays_per_session": 0.0}
    avg = sum([c for _, c in total_plays]) / len(total_plays)
    return {"average_plays_per_session": avg}

# Engagement by subscription level
@app.get("/engagement_analytics/by_subscription_level")
def get_engagement_by_level(db: Session = Depends(get_db)):
    results = (
        db.query(FactPlays.user_level, func.count(FactPlays.play_id).label("play_count"))
        .group_by(FactPlays.user_level)
        .all()
    )
    return [{"user_level": r[0], "play_count": r[1]} for r in results if r[0] is not None]

# Hourly patterns
@app.get("/engagement_analytics/hourly_patterns")
def get_hourly_patterns(db: Session = Depends(get_db)):
    results = (
        db.query(DimTime.hour, func.count(FactPlays.play_id).label("play_count"))
        .join(FactPlays, FactPlays.time_key == DimTime.time_key)
        .group_by(DimTime.hour)
        .order_by(DimTime.hour)
        .all()
    )
    return [{"hour": r[0], "play_count": r[1]} for r in results]

# Geographic distribution
@app.get("/engagement_analytics/geographic_distribution")
def get_geographic_distribution(db: Session = Depends(get_db)):
    results = (
        db.query(DimLocation.state, DimLocation.city, func.count(FactPlays.play_id).label("play_count"))
        .join(FactPlays, FactPlays.location_id == DimLocation.location_id)
        .group_by(DimLocation.state, DimLocation.city)
        .order_by(DimLocation.state, DimLocation.city)
        .all()
    )
    return [
        {"state": r[0], "city": r[1], "play_count": r[2]} for r in results
    ]

# User profiles (basic example)
@app.get("/user_profiles")
def get_user_profiles(db: Session = Depends(get_db)):
    users = db.query(DimUser).all()
    return [
        {
            "user_id": u.user_id,
            "first_name": u.first_name,
            "last_name": u.last_name,
            "gender": u.gender,
            "registration_ts": u.registration_ts,
            "birthday": u.birthday,
            "user_level": getattr(u, 'user_level', None)
        }
        for u in users
    ]

# Popular artist by state (all-time)
@app.get("/content_analytics/popular_artist_by_state")
def get_popular_artist_by_state(db: Session = Depends(get_db)):
    from sqlalchemy import func
    subq = (
        db.query(
            FactPlays.location_id,
            FactPlays.artist_id,
            func.count(FactPlays.play_id).label("play_count")
        )
        .group_by(FactPlays.location_id, FactPlays.artist_id)
        .subquery()
    )
    results = (
        db.query(DimLocation.state, DimArtist.artist_name, subq.c.play_count)
        .join(subq, subq.c.location_id == DimLocation.location_id)
        .join(DimArtist, subq.c.artist_id == DimArtist.artist_id)
        .order_by(DimLocation.state, subq.c.play_count.desc())
        .all()
    )
    # For each state, pick the top artist
    top_per_state = {}
    for state, artist, count in results:
        if state not in top_per_state:
            top_per_state[state] = {"artist": artist, "play_count": count}
    return top_per_state

# Most popular artist by year
@app.get("/content_analytics/most_popular_artist_by_year")
def get_most_popular_artist_by_year(db: Session = Depends(get_db)):
    from sqlalchemy import func
    subq = (
        db.query(
            DimTime.year,
            FactPlays.artist_id,
            func.count(FactPlays.play_id).label("play_count")
        )
        .join(DimTime, FactPlays.time_key == DimTime.time_key)
        .group_by(DimTime.year, FactPlays.artist_id)
        .subquery()
    )
    results = (
        db.query(subq.c.year, DimArtist.artist_name, subq.c.play_count)
        .join(DimArtist, subq.c.artist_id == DimArtist.artist_id)
        .order_by(subq.c.year, subq.c.play_count.desc())
        .all()
    )
    # For each year, pick the top artist
    top_per_year = {}
    for year, artist, count in results:
        if year not in top_per_year:
            top_per_year[year] = {"artist": artist, "play_count": count}
    return top_per_year

# NEW SPECIFIC LOCATION & TIME FILTERING ENDPOINTS

# Most popular artists by specific state
@app.get("/content_analytics/popular_artists_by_state/{state}")
def get_popular_artists_by_state(state: str, limit: int = 10, db: Session = Depends(get_db)):
    results = (
        db.query(DimArtist.artist_name, func.count(FactPlays.play_id).label("play_count"))
        .join(FactPlays, FactPlays.artist_id == DimArtist.artist_id)
        .join(DimLocation, FactPlays.location_id == DimLocation.location_id)
        .filter(DimLocation.state.ilike(f"%{state}%"))
        .group_by(DimArtist.artist_name)
        .order_by(desc("play_count"))
        .limit(limit)
        .all()
    )
    return [{"artist": r[0], "play_count": r[1]} for r in results]

# Most popular songs by specific state  
@app.get("/content_analytics/popular_songs_by_state/{state}")
def get_popular_songs_by_state(state: str, limit: int = 10, db: Session = Depends(get_db)):
    results = (
        db.query(DimSong.song_title, DimArtist.artist_name, func.count(FactPlays.play_id).label("play_count"))
        .join(FactPlays, FactPlays.song_id == DimSong.song_id)
        .join(DimArtist, FactPlays.artist_id == DimArtist.artist_id)
        .join(DimLocation, FactPlays.location_id == DimLocation.location_id)
        .filter(DimLocation.state.ilike(f"%{state}%"))
        .group_by(DimSong.song_title, DimArtist.artist_name)
        .order_by(desc("play_count"))
        .limit(limit)
        .all()
    )
    return [{"song": r[0], "artist": r[1], "play_count": r[2]} for r in results]

# Most popular artists by specific city
@app.get("/content_analytics/popular_artists_by_city/{city}")
def get_popular_artists_by_city(city: str, limit: int = 10, db: Session = Depends(get_db)):
    results = (
        db.query(DimArtist.artist_name, func.count(FactPlays.play_id).label("play_count"))
        .join(FactPlays, FactPlays.artist_id == DimArtist.artist_id)
        .join(DimLocation, FactPlays.location_id == DimLocation.location_id)
        .filter(DimLocation.city.ilike(f"%{city}%"))
        .group_by(DimArtist.artist_name)
        .order_by(desc("play_count"))
        .limit(limit)
        .all()
    )
    return [{"artist": r[0], "play_count": r[1]} for r in results]

# Most popular songs by specific city
@app.get("/content_analytics/popular_songs_by_city/{city}")
def get_popular_songs_by_city(city: str, limit: int = 10, db: Session = Depends(get_db)):
    results = (
        db.query(DimSong.song_title, DimArtist.artist_name, func.count(FactPlays.play_id).label("play_count"))
        .join(FactPlays, FactPlays.song_id == DimSong.song_id)
        .join(DimArtist, FactPlays.artist_id == DimArtist.artist_id)
        .join(DimLocation, FactPlays.location_id == DimLocation.location_id)
        .filter(DimLocation.city.ilike(f"%{city}%"))
        .group_by(DimSong.song_title, DimArtist.artist_name)
        .order_by(desc("play_count"))
        .limit(limit)
        .all()
    )
    return [{"song": r[0], "artist": r[1], "play_count": r[2]} for r in results]

# Most popular artists by specific date
@app.get("/content_analytics/popular_artists_by_date/{date}")
def get_popular_artists_by_date(date: str, limit: int = 10, db: Session = Depends(get_db)):
    results = (
        db.query(DimArtist.artist_name, func.count(FactPlays.play_id).label("play_count"))
        .join(FactPlays, FactPlays.artist_id == DimArtist.artist_id)
        .join(DimTime, FactPlays.time_key == DimTime.time_key)
        .filter(DimTime.date == date)
        .group_by(DimArtist.artist_name)
        .order_by(desc("play_count"))
        .limit(limit)
        .all()
    )
    return [{"artist": r[0], "play_count": r[1]} for r in results]

# Most popular songs by specific date
@app.get("/content_analytics/popular_songs_by_date/{date}")
def get_popular_songs_by_date(date: str, limit: int = 10, db: Session = Depends(get_db)):
    results = (
        db.query(DimSong.song_title, DimArtist.artist_name, func.count(FactPlays.play_id).label("play_count"))
        .join(FactPlays, FactPlays.song_id == DimSong.song_id)
        .join(DimArtist, FactPlays.artist_id == DimArtist.artist_id)
        .join(DimTime, FactPlays.time_key == DimTime.time_key)
        .filter(DimTime.date == date)
        .group_by(DimSong.song_title, DimArtist.artist_name)
        .order_by(desc("play_count"))
        .limit(limit)
        .all()
    )
    return [{"song": r[0], "artist": r[1], "play_count": r[2]} for r in results]

# Most popular artists by date range
@app.get("/content_analytics/popular_artists_by_date_range")
def get_popular_artists_by_date_range(start_date: str, end_date: str, limit: int = 10, db: Session = Depends(get_db)):
    results = (
        db.query(DimArtist.artist_name, func.count(FactPlays.play_id).label("play_count"))
        .join(FactPlays, FactPlays.artist_id == DimArtist.artist_id)
        .join(DimTime, FactPlays.time_key == DimTime.time_key)
        .filter(DimTime.date >= start_date)
        .filter(DimTime.date <= end_date)
        .group_by(DimArtist.artist_name)
        .order_by(desc("play_count"))
        .limit(limit)
        .all()
    )
    return [{"artist": r[0], "play_count": r[1]} for r in results]

# Most popular songs by date range
@app.get("/content_analytics/popular_songs_by_date_range")
def get_popular_songs_by_date_range(start_date: str, end_date: str, limit: int = 10, db: Session = Depends(get_db)):
    results = (
        db.query(DimSong.song_title, DimArtist.artist_name, func.count(FactPlays.play_id).label("play_count"))
        .join(FactPlays, FactPlays.song_id == DimSong.song_id)
        .join(DimArtist, FactPlays.artist_id == DimArtist.artist_id)
        .join(DimTime, FactPlays.time_key == DimTime.time_key)
        .filter(DimTime.date >= start_date)
        .filter(DimTime.date <= end_date)
        .group_by(DimSong.song_title, DimArtist.artist_name)
        .order_by(desc("play_count"))
        .limit(limit)
        .all()
    )
    return [{"song": r[0], "artist": r[1], "play_count": r[2]} for r in results]

# Combined filter: popular artists by state and date range
@app.get("/content_analytics/popular_artists_by_state_and_date")
def get_popular_artists_by_state_and_date(state: str, start_date: str, end_date: str, limit: int = 10, db: Session = Depends(get_db)):
    results = (
        db.query(DimArtist.artist_name, func.count(FactPlays.play_id).label("play_count"))
        .join(FactPlays, FactPlays.artist_id == DimArtist.artist_id)
        .join(DimLocation, FactPlays.location_id == DimLocation.location_id)
        .join(DimTime, FactPlays.time_key == DimTime.time_key)
        .filter(DimLocation.state.ilike(f"%{state}%"))
        .filter(DimTime.date >= start_date)
        .filter(DimTime.date <= end_date)
        .group_by(DimArtist.artist_name)
        .order_by(desc("play_count"))
        .limit(limit)
        .all()
    )
    return [{"artist": r[0], "play_count": r[1]} for r in results]