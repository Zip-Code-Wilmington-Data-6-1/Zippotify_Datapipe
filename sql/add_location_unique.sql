ALTER TABLE IF EXISTS dim_location
  ADD CONSTRAINT uq_dim_location_geo UNIQUE (city, state, latitude, longitude);