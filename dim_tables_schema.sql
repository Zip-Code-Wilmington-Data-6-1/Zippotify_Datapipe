-- User Dimension
CREATE TABLE
IF NOT EXISTS dim_user
(
    user_id INT PRIMARY KEY,
    last_name VARCHAR
(100),
    first_name VARCHAR
(100),
    gender VARCHAR
(10),
    registration_ts TIMESTAMP,
    birthday BIGINT
);

-- Artist Dimension
DROP TABLE IF EXISTS dim_artist
CASCADE;

CREATE TABLE
IF NOT EXISTS dim_artist
(
    artist_id SERIAL PRIMARY KEY,
    artist_name VARCHAR
(255) UNIQUE
);

-- Genre Dimension
CREATE TABLE
IF NOT EXISTS dim_genre
(
    genre_id SERIAL PRIMARY KEY,
    genre_name VARCHAR
(100) UNIQUE
);

-- Song Dimension
CREATE TABLE
IF NOT EXISTS dim_song
(
    song_id INT PRIMARY KEY,
    song_title VARCHAR
(255),
);

-- Song-Genre Bridge
DROP TABLE IF EXISTS dim_song_genre
CASCADE;
CREATE TABLE
IF NOT EXISTS dim_song_genre
(
    song_id VARCHAR
(36) NOT NULL,
    genre_id VARCHAR
(36) NOT NULL,
    PRIMARY KEY
(song_id, genre_id),
    FOREIGN KEY
(song_id) REFERENCES dim_song
(song_id),
    FOREIGN KEY
(genre_id) REFERENCES dim_genre
(genre_id)
);

-- Song-Artist Bridge
DROP TABLE IF EXISTS dim_song_artist
CASCADE;
CREATE TABLE
-- use MusicBrainz MBIDs (MusicBrainz Identifiers), which are UUID strings (like f27ec8db-af05-4f36-916e-3d57f91ecf5e)
IF NOT EXISTS dim_song_artist
(
    song_id VARCHAR
(36) NOT NULL,
    artist_id VARCHAR
(36) NOT NULL,
    PRIMARY KEY
(song_id, artist_id),
    FOREIGN KEY
(song_id) REFERENCES dim_song
(song_id),
    FOREIGN KEY
(artist_id) REFERENCES dim_artist
(artist_id)
);



DROP TABLE IF EXISTS dim_location
CASCADE;
-- Location Dimension
CREATE TABLE
IF NOT EXISTS dim_location
(
    location_id SERIAL PRIMARY KEY,
    city VARCHAR
(100),
    state VARCHAR
(50),
    latitude DECIMAL
(9,6),
    longitude DECIMAL
(9,6)
);

-- Time Dimension
CREATE TABLE
IF NOT EXISTS dim_time
(
    time_key INT PRIMARY KEY,
    date DATE,
    hour INT,
    weekday VARCHAR
(10),
    month INT,
    year INT
);

-- Fact Table for Plays
CREATE TABLE
IF NOT EXISTS fact_plays
(
    play_id BIGSERIAL PRIMARY KEY,
    user_id INT,
    song_id INT,
    artist_id INT,
    location_id INT,
    time_key INT,
    duration_ms INT,
    session_id VARCHAR
(100),
    user_level VARCHAR
(20),
    FOREIGN KEY
(user_id) REFERENCES dim_user
(user_id),
    FOREIGN KEY
(song_id) REFERENCES dim_song
(song_id),
    FOREIGN KEY
(artist_id) REFERENCES dim_artist
(artist_id),
    FOREIGN KEY
(location_id) REFERENCES dim_location
(location_id),
    FOREIGN KEY
(time_key) REFERENCES dim_time
(time_key)
);