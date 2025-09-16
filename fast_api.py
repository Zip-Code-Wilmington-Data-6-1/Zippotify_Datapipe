# filepath: Zippotify_Datapipe/api.py
from fastapi import FastAPI, Depends
from typing import List
from sqlalchemy.orm import Session
from database import SessionLocal
from models import DimUser
from pydantic import BaseModel

app = FastAPI()

class User(BaseModel):
    user_id: int
    gender: str
    registration_ts: int  # You may want to use datetime here
    birthday: int

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