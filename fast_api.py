# filepath: Zippotify_Datapipe/api.py
from fastapi import FastAPI, Depends, HTTPException
from typing import List, Optional
from sqlalchemy.orm import Session
from database import SessionLocal
from models import DimArtist, DimLocation, DimUser
from pydantic import BaseModel
from datetime import datetime

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