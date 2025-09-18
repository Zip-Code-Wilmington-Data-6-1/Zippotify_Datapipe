from sqlalchemy import Column, Integer, String, BigInteger, TIMESTAMP, DECIMAL, ForeignKey, DateTime, Date
from sqlalchemy.orm import relationship
from database import Base

class DimUser(Base):
    __tablename__ = "dim_user"
    user_id = Column(Integer, primary_key=True, index=True)
    last_name = Column(String(100))
    first_name = Column(String(100))
    gender = Column(String(10))
    registration_ts = Column(TIMESTAMP)
    birthday = Column(BigInteger)

class DimLocation(Base):
    __tablename__ = "dim_location"
    location_id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String(100))
    state = Column(String(50))
    latitude = Column(DECIMAL(9, 6))
    longitude = Column(DECIMAL(9, 6))

class DimArtist(Base):
    __tablename__ = "dim_artist"
    artist_id = Column(Integer, primary_key=True, autoincrement=True)
    artist_name = Column(String(255), unique=True)
    songs = relationship("DimSongArtist", back_populates="artist")

class DimSong(Base):
    __tablename__ = "dim_song"
    song_id = Column(Integer, primary_key=True, autoincrement=True)
    song_title = Column(String(255))
    genres = relationship("DimSongGenre", back_populates="song")
    artists = relationship("DimSongArtist", back_populates="song")

class DimSongArtist(Base):
    __tablename__ = "dim_song_artist"
    song_id = Column(Integer, ForeignKey("dim_song.song_id"), primary_key=True)
    artist_id = Column(Integer, ForeignKey("dim_artist.artist_id"), primary_key=True)
    song = relationship("DimSong", back_populates="artists")
    artist = relationship("DimArtist", back_populates="songs")

class DimGenre(Base):
    __tablename__ = "dim_genre"
    genre_id = Column(Integer, primary_key=True, autoincrement=True)
    genre_name = Column(String(255), unique=True, nullable=False)
    songs = relationship("DimSongGenre", back_populates="genre")

class DimSongGenre(Base):
    __tablename__ = "dim_song_genre"
    song_id = Column(Integer, ForeignKey("dim_song.song_id"), primary_key=True)
    genre_id = Column(Integer, ForeignKey("dim_genre.genre_id"), primary_key=True)
    song = relationship("DimSong", back_populates="genres")
    genre = relationship("DimGenre", back_populates="songs")

class DimTime(Base):
    __tablename__ = "dim_time"
    time_key = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date)
    hour = Column(Integer)
    weekday = Column(String(10))
    month = Column(Integer)
    year = Column(Integer)

class FactPlays(Base):
    __tablename__ = "fact_plays"
    play_id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("dim_user.user_id"))
    song_id = Column(Integer, ForeignKey("dim_song.song_id"))
    artist_id = Column(Integer, ForeignKey("dim_artist.artist_id"))
    location_id = Column(Integer, ForeignKey("dim_location.location_id"))
    time_key = Column(Integer, ForeignKey("dim_time.time_key"))
    duration_ms = Column(Integer)
    session_id = Column(String(100))
    user_level = Column(String(20))
