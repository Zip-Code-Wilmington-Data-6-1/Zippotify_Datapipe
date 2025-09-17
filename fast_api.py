# filepath: Zippotify_Datapipe/api.py
from fastapi import FastAPI, Depends
from typing import List
from sqlalchemy.orm import Session
from database import SessionLocal
from models import DimArtist, DimLocation, DimUser
from pydantic import BaseModel

app = FastAPI()

class User(BaseModel):
    user_id: int
    gender: str
    registration_ts: int  # You may want to use datetime here
    birthday: int

    class Config:
        orm_mode = True

class Artist(BaseModel):
    artist_id: int
    artist_name: str

    class Config:
        orm_mode = True

class location(BaseModel):
    location_id: int
    city: str
    state: str
    latitude: float
    longitude: float

    class Config:
        orm_mode = True



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
    if user:
        return user
    else:
        return {}

@app.get("/artists", response_model=List[Artist])
def get_artists(db: Session = Depends(get_db)):
    return db.query(DimArtist).all()

@app.get("/artists/{artist_id}", response_model=Artist)
def get_artist(artist_id: int, db: Session = Depends(get_db)):
    artist = db.query(DimArtist).filter(DimArtist.artist_id == artist_id).first()
    if artist:
        return artist
    else:
        return {}  

@app.get("/locations", response_model=List[location])
def get_locations(db: Session = Depends(get_db)):
    return db.query(DimLocation).all()


@app.get("/locations/{location_id}", response_model=location)
def get_location(location_id: int, db: Session = Depends(get_db)):
    location = db.query(DimLocation).filter(DimLocation.location_id == location_id).first()
    if location:
        return location
    else:
        return {}