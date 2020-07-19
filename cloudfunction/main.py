from google.cloud import storage
from google.cloud import bigquery

from spotipy.spotify_client import SpotifyClient

from datetime import date
import json
import os
import time
import logging


def write_top50_data(spotify_client, playlist_id):
    # Get playlist track data
    track_data = spotify_client.get_playlist_items(playlist_id)
    # Gather track audio features
    track_ids = [track.get("id") for track in track_data]
    audio_feature_data = spotify_client.get_audio_features(track_ids)

    # Open file to write to
    file_date = date.today().strftime("%Y-%m-%d")
    file_timestamp = str(time.time_ns())
    file_name = "top50_audiofeatures_" + file_date + "_" + file_timestamp + ".json"

    with open(f"./temp/{file_name}", "w") as f:
        # Join track data and audio features and write
        for i, track in enumerate(track_data):
            temp_data = track_data[i]
            for audio_record in audio_feature_data:
                if audio_record.get("id") == track.get("id"):
                    temp_data["audio_features"] = audio_record
                    # Write temp data
                    f.write(json.dumps(temp_data) + "\n")

    return file_name


def upload_file_blob(gcs_client, bucket_name, file_name):
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_filename("./temp/" + file_name)


def load_bigquery(bq_client, dataset, table_name, blob_uri):
    # Define table
    table_ref = bq_client.dataset(dataset).table(table_name)
    # Define job config
    job_config = bigquery.LoadJobConfig()
    # To upload each row of json as string
    # use CSV format and arbitrary separator
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.field_delimiter = "^"

    # Submit job
    load_job = bq_client.load_table_from_uri(blob_uri, table_ref, job_config=job_config)
    # Waits for job to complete
    load_job.result()


def main():
    # Set parameter and create client
    spotify_playlist_id = "37i9dQZEVXbLRQDuF5jeBp"
    spotify_client = SpotifyClient(
        os.getenv("SPOTIFY_CLIENT_ID"), os.getenv("SPOTIFY_CLIENT_SECRET")
    )
    # Write temp file
    temp_file = write_top50_data(spotify_client, spotify_playlist_id)
    logging.info(f"Wrote file {temp_file}")

    # Upload file to blob
    gcs_client = storage.Client()
    try:
        upload_file_blob(gcs_client, "spotify-data", temp_file)
    except Exception as e:
        logging.error(e)

    # Load blob to BQ
    bq_client = bigquery.Client()
    blob_path = "gs://spotify-data/"
    try:
        load_bigquery(bq_client, "spotify", "top50_tracks_raw", blob_path + temp_file)
    except Exception as e:
        logging.error(e)


if __name__ == "__main__":
    main()
