import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import timedelta
from datetime import datetime
import boto3

def lambda_handler(event, context):
    # client_id and client_secret stored in Lambda environment variables.
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    spotify = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    # Can add multiple playlist into a list. 
    playlist_link = "https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M"
    playlist_uri = playlist_link.split("/")[-1]
    json_data = spotify.playlist_tracks(playlist_uri)
    filename = "spotify_data_" + str(datetime.now()) +".json"
    
    client = boto3.client('s3')
    client.put_object(
        Bucket="spotify-etl-bucket",
        Key="LandingZone/"+filename,
        Body=json.dumps(json_data)
        )
