import os
import pandas as pd
from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv() 

CHANNEL_ID = os.environ.get("CHANNEL_ID")
API_KEY = os.environ.get("API_KEY")

youtube = build('youtube',
                'v3',
                developerKey=API_KEY
                )

channels_response = youtube.channels().list(
        part='statistics, contentDetails, brandingSettings',
        id=CHANNEL_ID
    ).execute()

# /////////////////
# チャンネル情報の取得
# /////////////////
def get_channel():
    channels_response = youtube.channels().list(
            part='snippet,statistics',
            id=CHANNEL_ID
        ).execute()

    channel_data = channels_response['items'][0]
    
    # 必要なデータを辞書形式で抽出
    channel_info = {
        'channel_id': channel_data['id'],
        'channel_name': channel_data['snippet']['title'],
        'subscriber_count': int(channel_data['statistics'].get('subscriberCount', 0)),
        'total_views': int(channel_data['statistics'].get('viewCount', 0)),
        'video_count': int(channel_data['statistics'].get('videoCount', 0)),
        'published_at': channel_data['snippet']['publishedAt']
    }

    channel_list = [channel_info]

    return channel_list

# /////////////////
# 動画情報の取得
# /////////////////
def get_video():
    playlist_id = channels_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    all_videos_data = []
    next_page_token = None

    while True:
        playlist_response = youtube.playlistItems().list(
            part='snippet,contentDetails',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        video_ids = []
        for item in playlist_response['items']:
            video_ids.append(item['contentDetails']['videoId'])

        if video_ids:
            videos_response = youtube.videos().list(
                part='snippet,statistics,contentDetails',
                id=','.join(video_ids)
            ).execute()

            # 各動画の詳細情報を抽出
            for video_data in videos_response['items']:
                video_id = video_data['id']
                title = video_data['snippet']['title']
                published_at = video_data['snippet']['publishedAt']
                view_count = video_data['statistics'].get('viewCount', 0)
                like_count = video_data['statistics'].get('likeCount', 0)
                comment_count = video_data['statistics'].get('commentCount', 0)
                # 動画の長さ
                duration = video_data['contentDetails']['duration']
                tags = video_data['snippet'].get('tags', [])

                all_videos_data.append({
                    'video_id': video_id,
                    'title': title,
                    'published_at': published_at,
                    'view_count': int(view_count),
                    'like_count': int(like_count),
                    'comment_count': int(comment_count),
                    'duration': duration,
                    'tags': tags
                })

        next_page_token = playlist_response.get('nextPageToken')

        if not next_page_token:
            break

    return all_videos_data

# /////////////////
# アップロードした動画のIDを取得
# /////////////////
def get_uploads_playlist_id(channel_id):
    channels_response = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    ).execute()
    return channels_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

# /////////////////
# コメント情報の取得
# /////////////////
def get_comments_for_video(video_id, max_comments_per_video=100):# 本来は100
    comments_data = []
    
    comment_threads_response = youtube.commentThreads().list(
        part='snippet',
        videoId=video_id,
        maxResults=min(100, max_comments_per_video),#本来は100
        pageToken=None,
        order='relevance'
    ).execute()

    for item in comment_threads_response['items']:
        comment = item['snippet']['topLevelComment']['snippet']
        comments_data.append({
            'video_id': video_id,
            'comment_id': item['id'],
            'author_display_name': comment['authorDisplayName'],
            'published_at': comment['publishedAt'],
            'text_display': comment['textDisplay'],
            'like_count': comment['likeCount']
        })
    
    return comments_data

# /////////////////
# 実行
# /////////////////
# df_channel = pd.DataFrame(get_channel())
# df_channel.to_csv('data/channel.csv', index=False, encoding='utf-8')
# print("チャンネル情報をchannel.csvに保存しました。")

# df_videos = pd.DataFrame(get_video())
# df_videos.to_csv('data/videos.csv', index=False, encoding='utf-8')
# print("動画情報をyoutube_videos.csvに保存しました。")

all_videos = get_video()
num_videos = 10 # 本来は50
sorted_videos = sorted(all_videos, key=lambda x: x['view_count'], reverse=True)
# 上位 num_videos 件を抽出
top_videos = sorted_videos[:num_videos]
print(top_videos)