import requests as r
import pandas as pd
from youtube_transcript_api import YouTubeTranscriptApi
from pytube import YouTube

def get_transcript(video_id):
    #yt = YouTube(video_url)
    #video_id = yt.video_id

    #video_url = 'https://www.youtube.com/watch?v=' + video_id

    # Get the transcript
    transcript = YouTubeTranscriptApi.get_transcript(video_id)

    # Extract text from the transcript
    text = ''
    for entry in transcript:
        text += entry['text'] + ' '
    return text


def complexupload(final_docs, content_source_id, bcr_token):
    custom_base = [{k: v for k, v in e.items() if k != 'date' and k != 'contents' and k != 'type' and k != 'guid' and k != 'title' and k!= 'language' and k != 'author' and k != 'url' and k != 'geolocation' and k != 'engagementType'} for e in final_docs]
#print (custom)
    docsup = [{k: v for k, v in e.items() if k == 'date' or k == 'contents' or k == 'guid' or k == 'title' or k == 'language' or k == 'author' or k == 'url' or k == 'geolocation' or k == 'engagementType'} for e in final_docs]
    for x in range(0, len(docsup)):
        docsup[x]["custom"] = custom_base[x]

    chunks = [docsup[x:x+1000] for x in range(0, len(docsup), 1000)]
    for chunk in chunks:
        items = {"items": chunk,
                 "contentSource": content_source_id,
                 "requestUsage": "True"}
        header = {"authorization": f"bearer {bcr_token}"}
        upload_url = "https://api.brandwatch.com/content/upload"
        upload_mentions = r.post(upload_url, json=items, headers=header).json()
        print (upload_mentions)
        return upload_mentions


apikey = 'Enter key here'
#bcr_token = '#YNWA'
#channels = [{'name': 'Uber', 'handle': 'Uber', 'id': 'UCgnxoUwDmmyzeigmmcf0hZA', 'playlist_id': 'UUgnxoUwDmmyzeigmmcf0hZA'}]


channels = [
    {'name': 'The Late Show with Stephen Colbert', 'handle': 'ColbertLateShow', 'id': 'UCMtFAi84ehTSYSE9XoHefig', 'playlist_id': 'UUMtFAi84ehTSYSE9XoHefig'},
    {'name': 'Late Night with Seth Meyers', 'handle': 'LateNightSeth', 'id': 'UCVTyTA7-g9nopHeHbeuvpRA', 'playlist_id': 'UUVTyTA7-g9nopHeHbeuvpRA'}
    ]

### Fetch the videos

all_videos = []

for channel in channels:
    channel_videos = []
    video_url = f"https://www.googleapis.com/youtube/v3/playlistItems?"
    params = {'part':['snippet','contentDetails'], 'maxResults':50, 'playlistId':channel['playlist_id'], 'key': apikey}
    videos_call = r.get(video_url, params=params)
    for video_item in videos_call.json()['items']:
        video_info_metal = {'title': video_item['snippet']['title'],
                            'description': video_item['snippet']['description'],
                            'author': video_item['snippet']['videoOwnerChannelTitle'],
                            'date': video_item['snippet']['publishedAt'][:-1],
                            'url': 'https://www.youtube.com/watch?v=' +video_item['contentDetails']['videoId'],
                            'video_id': video_item['contentDetails']['videoId']
                           }
        channel_videos.append(video_info_metal)
        ### Fetch the metrics for the video
    all_videos.extend(channel_videos)

    




# Fetch the video engagement metrics and transcript

for video in all_videos:
    video_info_url = "https://www.googleapis.com/youtube/v3/videos"
    params = {'part':'statistics', 'id':video['video_id'], 'key': apikey}
    video_info_call =  r.get(video_info_url, params=params)
    metrics = {'yt_views': video_info_call.json()['items'][0]['statistics']['viewCount'] if 'viewCount' in video_info_call.json()['items'][0]['statistics'].keys() else 'Not available',
               'yt_likes': video_info_call.json()['items'][0]['statistics']['likeCount'] if 'likeCount' in video_info_call.json()['items'][0]['statistics'].keys() else 'Not available',
               'yt_favorites': video_info_call.json()['items'][0]['statistics']['favoriteCount'] if 'favoriteCount' in video_info_call.json()['items'][0]['statistics'].keys() else 'Not available',
               'yt_comments': video_info_call.json()['items'][0]['statistics']['commentCount'] if 'commentCount' in video_info_call.json()['items'][0]['statistics'].keys() else 'Not available'
              }
    video.update(metrics)
    try:
        transcript = get_transcript(video['video_id'])
        video.update({'transcript': transcript})
    except:
        transcript = 'no transcript available'
        video.update({'transcript': transcript})




print ('test')

### Enrich with mentions of your query


query_string = ['Ukraine']

for x in all_videos:
    full_text = x['transcript'].lower()
    for string in query_string:
        if full_text == 'no transcript available':
            x['query_match'] = 'transcript could not be obtained'
        elif full_text != 'no transcript available':
            if string.lower() in full_text:
                x['query_match'] = 'Yes'
            elif string.lower() not in full_text:
                x['query_match'] = 'No'

for x in all_videos:
    #x.pop('transcript')
    x.pop('description')


all_videos_df = pd.DataFrame(all_videos)

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
from gspread_dataframe import set_with_dataframe

### write to google sheets

### Load Data

scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/spreadsheets",
         "https://www.googleapis.com/auth/drive.file",
         "https://www.googleapis.com/auth/drive"
        ]

creds = ServiceAccountCredentials.from_json_keyfile_name('carlosquinto-fbc7148247ea.json', scope)

client = gspread.authorize(creds)

spreadsheet = client.open("Talk Show Videos")

worksheet = spreadsheet.worksheet("Full")  # Use sheet1 or use sheet's name like: spreadsheet.worksheet("Sheet1")

set_with_dataframe(worksheet, all_videos_df)#, row=1, col=1)

print ('test')

print ('break here')
