import requests
import json
import base64


class SpotifyClient:
    def __init__(self, client_id, client_secret):
        # Get base64 encoded credentials
        cred_string = (client_id + ":" + client_secret).encode("ascii")
        creds_encoded = base64.b64encode(cred_string).decode("ascii")

        # Get access token from Spotify. Token lasts 1 hour
        token_url = "https://accounts.spotify.com/api/token"
        header = {"Authorization": f"Basic {creds_encoded}"}
        data = {"grant_type": "client_credentials"}

        try:
            r = requests.post(token_url, headers=header, data=data)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise SystemExit(e)

        self._access_token = r.json().get("access_token")

    def get_playlist_items(self, playlist_id):
        """Given a playlist ID returns track dimensional data.

        Args:
            playlist_id (str): playlist ID

        Returns:
            list: dictionary of song attributes
        """
        header = {"Authorization": f"Bearer {self._access_token}"}
        url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
        try:
            r = requests.get(url, headers=header)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise SystemExit(e)

        tracks = [
            {
                "playlist_track_num": i + 1,
                "id": item.get("track").get("id"),
                "name": item.get("track").get("name"),
                "popularity": item.get("track").get("popularity"),
                "album": item.get("track").get("album").get("name"),
                "artist": item.get("track").get("artists")[0].get("name"),
            }
            for i, item in enumerate(r.json().get("items"))
        ]

        return tracks

    def get_audio_features(self, track_ids):
        """Returns audio feature metrics for list of track IDs.

        Args:
            track_ids (list): list of track IDs

        Returns:
            list: list of dictionaries that provide track audio metrics.
        """
        header = {"Authorization": f"Bearer {self._access_token}"}
        query_params = {"ids": ",".join(track_ids)}
        url = "https://api.spotify.com/v1/audio-features"

        try:
            r = requests.get(url, headers=header, params=query_params)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise SystemExit(e)

        r.request.url
        return r.json().get("audio_features")
