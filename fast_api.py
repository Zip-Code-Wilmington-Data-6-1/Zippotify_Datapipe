# filepath: Zippotify_Datapipe/api.py
from fastapi import FastAPI, Depends, HTTPException
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import SessionLocal
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
        with SessionLocal() as db:
            db.execute(text("SELECT 1"))
        return {"db": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))