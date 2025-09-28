import os
from io import StringIO
import boto3
import pandas as pd
from googleapiclient.discovery import build

# /////////////////
# 環境変数読み込み
# /////////////////
CHANNEL_ID = os.environ.get("CHANNEL_ID")
API_KEY = os.environ.get("API_KEY")
BUCKET_NAME = os.environ.get('BUCKET_NAME')
REGION_NAME = os.environ.get('REGION_NAME')

youtube = build('youtube',
                'v3',
                developerKey=API_KEY
                )

# /////////////////
# チャンネル情報の取得
# /////////////////
def get_channel():
    channels_response = youtube.channels().list(
            part='snippet,statistics',
            id=CHANNEL_ID
        ).execute()

    channel_data = channels_response['items'][0]
    
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
    channels_response = youtube.channels().list(
            part='statistics, contentDetails, brandingSettings',
            id=CHANNEL_ID
        ).execute()

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
                video_id = video_data['id']
                title = video_data['snippet']['title']
                published_at = video_data['snippet']['publishedAt']
                view_count = video_data['statistics'].get('viewCount', 0)
                like_count = video_data['statistics'].get('likeCount', 0)
                comment_count = video_data['statistics'].get('commentCount', 0)
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
                    'tags': ','.join(tags)
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
def get_uploads_playlist_id(channel_id):
    channels_response = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    ).execute()
    return channels_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

def get_comments_for_video(video_id, max_comments_per_video=100):
    comments_data = []
    
    comment_threads_response = youtube.commentThreads().list(
        part='snippet',
        videoId=video_id,
        maxResults=min(100, max_comments_per_video),
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

def lambda_handler(event, context):
    s3 = boto3.client('s3',
                      region_name=REGION_NAME)

    output_channel = StringIO()
    df_channel = pd.DataFrame(get_channel())

    df_channel.to_json(output_channel, orient='records', lines=True, force_ascii=False)
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key='data/raw_data/sukima_channel.json',
        Body=output_channel.getvalue()
    )
    print("チャンネル情報をchannel.csvに保存しました。")


    output_video = StringIO()
    df_videos = pd.DataFrame(get_video())

    df_videos.to_json(output_video, orient='records', lines=True, force_ascii=False)
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key='data/raw_data/sukima_video.json',
        Body=output_video.getvalue()
    )
    print("動画情報をyoutube_videos.csvに保存しました。")

    top_videos_df = df_videos.sort_values(by='view_count', ascending=False).head(10) #本来は100

    all_comments = []
    for index, row in top_videos_df.iterrows():
        video_id = row['video_id']
        video_title = row['title']
        comments = get_comments_for_video(video_id, max_comments_per_video=100)
        all_comments.extend(comments)

    output_comment = StringIO()
    df_comments = pd.DataFrame(all_comments)

    df_comments.to_json(output_comment, orient='records', lines=True, force_ascii=False)
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key='data/raw_data/sukima_comment.json',
        Body=output_comment.getvalue()
    )
    print(f"合計 {len(all_comments)} 件のコメントを保存しました。")