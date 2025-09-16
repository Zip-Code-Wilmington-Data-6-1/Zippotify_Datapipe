-- User Dimension
CREATE TABLE dim_user
(
    user_id INT PRIMARY KEY,
    gender VARCHAR(10),
    registration_ts TIMESTAMP,
    birthday BIGINT,
    -- Optionally: age_bucket VARCHAR(20), subscription_history JSON
);

-- Artist Dimension
CREATE TABLE dim_artist
(
    artist_id INT PRIMARY KEY,
    artist_name VARCHAR(255)
);

-- Song Dimension
CREATE TABLE dim_song
(
    song_id INT PRIMARY KEY,
    song_title VARCHAR(255),
    artist_id INT,
    genre_id INT,
    FOREIGN KEY (artist_id) REFERENCES dim_artist(artist_id),
    FOREIGN KEY (genre_id) REFERENCES dim_genre(genre_id)
);

CREATE TABLE
IF NOT EXISTS dim_song_genre
(
    song_id INT,
    genre_id INT,
    PRIMARY KEY
(song_id, genre_id),
    FOREIGN KEY
(song_id) REFERENCES dim_song
(song_id),
    FOREIGN KEY
(genre_id) REFERENCES dim_genre
(genre_id)
);

CREATE TABLE
IF NOT EXISTS dim_song_artist
(
    song_id INT,
    artist_id INT,
    PRIMARY KEY
(song_id, artist_id),
    FOREIGN KEY
(song_id) REFERENCES dim_song
(song_id),
    FOREIGN KEY
(artist_id) REFERENCES dim_artist
(artist_id)
);

CREATE TABLE dim_genre
(
    genre_id SERIAL PRIMARY KEY,
    genre_name VARCHAR(100) UNIQUE
);

-- Location Dimension (optional)
CREATE TABLE dim_location
(
    location_id INT PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(50),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6)
);

-- Time Dimension (optional, for calendar breakdown)
CREATE TABLE dim_time
(
    time_key INT PRIMARY KEY,
    date DATE,
    hour INT,
    weekday VARCHAR(10),
    month INT,
    year INT
);

-- Fact Table for Plays
CREATE TABLE fact_plays
(
    play_id BIGSERIAL PRIMARY KEY,
    user_id INT,
    song_id INT,
    artist_id INT,
    location_id INT,
    time_key INT,
    duration_ms INT,
    session_id VARCHAR(100),
    user_level VARCHAR(20),
    -- Measures: play_count is always 1 per row
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
    FOREIGN KEY (song_id) REFERENCES dim_song(song_id),
    FOREIGN KEY (artist_id) REFERENCES dim_artist(artist_id),
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (time_key) REFERENCES dim_time(time_key)
);