import os
import pandas as pd
from apiclient.discovery import build
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
    channel_info = channels_response["items"][0]
    channel_statistics = channel_info["statistics"]
    channel_content = channel_info['contentDetails']
    channel_branding = channel_info['brandingSettings']

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

            for video_data in videos_response['items']:
                # 各動画の詳細情報を抽出
                video_id = video_data['id']
                title = video_data['snippet']['title']
                published_at = video_data['snippet']['publishedAt']
                view_count = video_data['statistics'].get('viewCount', 0) # 再生回数
                like_count = video_data['statistics'].get('likeCount', 0) # いいね数
                comment_count = video_data['statistics'].get('commentCount', 0) # コメント数
                # 動画の長さはISO 8601形式で取得されるので、適宜変換が必要
                duration = video_data['contentDetails']['duration']

                all_videos_data.append({
                    'video_id': video_id,
                    'title': title,
                    'published_at': published_at,
                    'view_count': int(view_count),
                    'like_count': int(like_count),
                    'comment_count': int(comment_count),
                    'duration': duration
                })

        next_page_token = playlist_response.get('nextPageToken')

        if not next_page_token:
            break

    # 取得した動画情報を表示（例として最初の5件）
    for i, video in enumerate(all_videos_data[:5]):
        print(f"\n--- 動画 {i+1} ---")
        print(f"動画ID: {video['video_id']}")
        print(f"タイトル: {video['title']}")
        print(f"公開日: {video['published_at']}")
        print(f"再生数: {video['view_count']}")
        print(f"いいね数: {video['like_count']}")
        print(f"コメント数: {video['comment_count']}")
        print(f"動画の長さ (ISO 8601): {video['duration']}")

# /////////////////
# コメント情報の取得
# /////////////////