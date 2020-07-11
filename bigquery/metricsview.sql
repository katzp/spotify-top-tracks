CREATE OR REPLACE VIEW spotify.top50_track_metrics
AS
SELECT
  _PARTITIONDATE AS date
  , JSON_EXTRACT(json, "$.playlist_track_num") AS playlist_track_num
  , JSON_EXTRACT_SCALAR(json, "$.id") AS id
  , JSON_EXTRACT_SCALAR(json, "$.name") AS name
  , JSON_EXTRACT(json, "$.popularity") AS popularity
  , JSON_EXTRACT_SCALAR(json, "$.album") AS album
  , JSON_EXTRACT_SCALAR(json, "$.artist") AS artist
  , JSON_EXTRACT(json, "$.audio_features.danceability") AS danceability
  , JSON_EXTRACT(json, "$.audio_features.energy") AS energy
  , JSON_EXTRACT(json, "$.audio_features.key") AS key
  , JSON_EXTRACT(json, "$.audio_features.loudness") AS loudness
  , JSON_EXTRACT(json, "$.audio_features.mode") AS mode
  , JSON_EXTRACT(json, "$.audio_features.speechiness") AS speechiness
  , JSON_EXTRACT(json, "$.audio_features.acousticness") AS acousticness
  , JSON_EXTRACT(json, "$.audio_features.instrumentalness") AS instrumentalness
  , JSON_EXTRACT(json, "$.audio_features.liveness") AS liveness
  , JSON_EXTRACT(json, "$.audio_features.valence") AS valence
  , JSON_EXTRACT(json, "$.audio_features.tempo") AS tempo
  , JSON_EXTRACT(json, "$.audio_features.duration_ms") AS duration_ms
  , JSON_EXTRACT(json, "$.audio_features.time_signature") AS time_signature
FROM spotify.top50_tracks_raw