import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from sqlalchemy.orm import Session
from models import DimSong, DimGenre, DimSongGenre
from database import SessionLocal

# Set up Spotify API
sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id=os.getenv("SPOTIFY_CLIENT_ID"),
    client_secret=os.getenv("SPOTIFY_CLIENT_SECRET")
))

def get_or_create_genre(session, genre_name):
    genre = session.query(DimGenre).filter_by(genre_name=genre_name).first()
    if not genre:
        genre = DimGenre(genre_name=genre_name)
        session.add(genre)
        session.commit()
    return genre

def main():
    session = SessionLocal()
    songs = session.query(DimSong).all()
    genre_cache = {}

    for song in songs:
        # Search for the song on Spotify
        results = sp.search(q=song.song_title, type='track', limit=1)
        items = results['tracks']['items']
        if not items:
            continue
        track = items[0]
        artist_ids = [artist['id'] for artist in track['artists']]

        # Get genres for each artist
        song_genres = set()
        for artist_id in artist_ids:
            if artist_id in genre_cache:
                genres = genre_cache[artist_id]
            else:
                artist = sp.artist(artist_id)
                genres = artist.get('genres', [])
                genre_cache[artist_id] = genres
            song_genres.update(genres)

        # Store genres and relationships
        for genre_name in song_genres:
            genre = get_or_create_genre(session, genre_name)
            # Link song and genre if not already linked
            exists = session.query(DimSongGenre).filter_by(song_id=song.song_id, genre_id=genre.genre_id).first()
            if not exists:
                link = DimSongGenre(song_id=song.song_id, genre_id=genre.genre_id)
                session.add(link)
        session.commit()

    session.close()

if __name__ == "__main__":
    main()