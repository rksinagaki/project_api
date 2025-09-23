import os
from googleapiclient.discovery import build
import pandas as pd
from dotenv import load_dotenv

load_dotenv() 

API_KEY = os.environ.get('API_KEY') # 'YOUR_API_KEY' を実際のAPIキーに置き換えてください
CHANNEL_ID = os.environ.get('CHANNEL_ID') # 'YOUR_CHANNEL_ID' を実際のチャンネルIDに置き換えてください

youtube = build('youtube', 'v3', developerKey=API_KEY)

def get_most_viewed_videos(channel_id, num_videos=50):
    all_videos = []
    next_page_token = None

    while True:
        # プレイリスト内の動画IDを取得
        playlist_items_response = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=get_uploads_playlist_id(channel_id),
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        video_ids = []
        for item in playlist_items_response['items']:
            video_ids.append(item['contentDetails']['videoId'])

        if not video_ids:
            break

        # 動画IDを使って動画の詳細情報を取得
        videos_response = youtube.videos().list(
            part='snippet,statistics',
            id=','.join(video_ids)
        ).execute()

        for video_data in videos_response['items']:
            all_videos.append({
                'video_id': video_data['id'],
                'title': video_data['snippet']['title'],
                'view_count': int(video_data['statistics'].get('viewCount', 0)),
                'like_count': int(video_data['statistics'].get('likeCount', 0)),
                'comment_count': int(video_data['statistics'].get('commentCount', 0))
            })

        next_page_token = playlist_items_response.get('nextPageToken')
        if not next_page_token:
            break
    
    print(f"全動画 {len(all_videos)} 件の情報を取得しました。")
    # 視聴回数でソートし、上位N件を抽出
    df_videos = pd.DataFrame(all_videos)
    df_videos_sorted = df_videos.sort_values(by='view_count', ascending=False)
    return df_videos_sorted.head(num_videos)

def get_uploads_playlist_id(channel_id):
    channels_response = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    ).execute()
    return channels_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

# ここの処理
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

if __name__ == '__main__':
    # 上位50件の動画情報を取得
    top_videos_df = get_most_viewed_videos(CHANNEL_ID, num_videos=5) #本来は50
    print(top_videos_df[['title', 'view_count']].to_string(index=False))

    all_comments = []
    for index, row in top_videos_df.iterrows(): # index,rowは基本的にiterrowsでは使う、表をインデックスと行に分けてfor文が回せる
        video_id = row['video_id']
        video_title = row['title']
        comments = get_comments_for_video(video_id, max_comments_per_video=100) # [{}, {}, {}, {}]という構造になっている
        all_comments.extend(comments) # extend()は要素を分解してかつ分解したものをリストに格納してくれる

    # 取得したコメントをDataFrameに変換してCSVに保存
    # if all_comments:
    #     df_comments = pd.DataFrame(all_comments)
    #     output_dir = '../data'
    #     os.makedirs(output_dir, exist_ok=True)
    #     df_comments.to_csv(os.path.join(output_dir, 'top_videos_comments.csv'), index=False, encoding='utf-8')
    #     print(f"合計 {len(all_comments)} 件のコメントを '{output_dir}/top_videos_comments.csv' に保存しました。")
    # else:
    #     print("\n取得できたコメントはありませんでした。")

    