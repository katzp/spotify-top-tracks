CREATE OR REPLACE TABLE spotify.top50_tracks_raw (
  json STRING
)
PARTITION BY _PARTITIONDATE