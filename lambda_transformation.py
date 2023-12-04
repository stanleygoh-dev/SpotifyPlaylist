import json
import boto3
from datetime import timedelta
from datetime import datetime
from io import StringIO
import pandas as pd

def songs(data):
    num = 1
    songs_list = []

    for i in data['items']:
        date = str(datetime.now().date())
        song_rank = num
        song_name = i['track']['album']['name']
        song_duration = i['track']['duration_ms']
        song_popularity = i['track']['popularity']
        album_release_date = i['track']['album']['release_date']
        album_total_tracks = i['track']['album']['total_tracks']
        album_id = i['track']['album']['id']
        album_url = i['track']['album']['external_urls']['spotify']
        album_artist = i['track']['album']['artists']
        album_artist_list = []
        for artist in album_artist:
            name = artist['name']
            album_artist_list.append(name)
        
        album_element = {'date': date,
                         'song_rank': song_rank,
                         'song_name': song_name,
                         'song_duration': str(timedelta(seconds = song_duration/1000)).split(".")[0],
                         'song_popularity': song_popularity,
                         'album_release_date': album_release_date,
                         'album_total_tracks': album_total_tracks,
                         'album_id': album_id,
                         'album_artist_list': str(album_artist_list)[1:-1].replace("'",""),
                         'album_url': album_url}
        num +=1
        songs_list.append(album_element)
    return songs_list
    


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket="spotify-etl-bucket"
    Key="LandingZone/"
    
    spotify_data = []
    spotify_keys = []
    for i in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = i['Key']
        if file_key.split('.')[-1] == 'json':
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
    
    for data in spotify_data:
        songs_list = songs(data)
        songs_df = pd.DataFrame.from_dict(songs_list)
        songs_df = songs_df.drop_duplicates(subset = ['album_id'])
        songs_df['album_release_date'] = pd.to_datetime(songs_df['album_release_date'])
        
        songs_key="StagingZone/spotify_data_"+str(datetime.now()) +".csv"
        songs_buffer = StringIO()
        songs_df.to_csv(songs_buffer, index=False)
        songs_content = songs_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=songs_key, Body=songs_content)
        
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, "Archive/" + key.split("/")[-1])
        s3_resource.Object(Bucket, key).delete()