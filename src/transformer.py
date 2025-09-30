from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, regexp_replace, trim, lit, sum
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, BooleanType

spark = SparkSession.builder.appName("MyPySparkApp").getOrCreate()
spark.sparkContext.setLogLevel("ERROR") 

# ////////////
# スキーマ設計
# ////////////
# channelデータのスキーマ設計
channel_schema = StructType([
    StructField("channel_id", StringType(), False),
    StructField("channel_name", StringType(), False),
    StructField("published_at", StringType(), False),# 後で処理
    StructField("subscriber_count", LongType(), False),
    StructField("total_views", LongType(), False),
    StructField("video_count", LongType(), False)
])

# videoデータのスキーマ設計
video_schema = StructType([
    StructField("video_id", StringType(), False),
    StructField("title", StringType(), False),
    StructField("published_at", StringType(), False),# 後で処理
    StructField("view_count", LongType(), False),
    StructField("like_count", LongType(), False),
    StructField("comment_count", LongType(), False),
    StructField("duration", StringType(), False),# 後で処理
    StructField("tags", StringType(), False)
])

# commentデータのスキーマ設計
comment_schema = StructType([
    StructField("video_id", StringType(), False),
    StructField("comment_id", StringType(), False),
    StructField("author_display_name", StringType(), False),
    StructField("published_at", StringType(), False),# 後で処理
    StructField("text_display", StringType(), False),
    StructField("like_count", LongType(), False)
])

# ////////////
# データの読み込み
# ////////////
df_channel = spark.read.schema(channel_schema).json("./data/channel.json")
df_video = spark.read.schema(video_schema).json("./data/videos.json")
df_comment = spark.read.schema(comment_schema).json("./data/top_videos_comments.json")

# ////////////
# データ型変換
# ////////////
# channelデータ型変更
# published_atコラムをデータ型に変換
df_channel = df_channel.withColumn(
    'published_at', 
    F.to_timestamp(F.col('published_at'), "yyyy-MM-dd\\'T\\'HH:mm:ssX")
)

# videoデータ型変更
# published_atコラムをデータ型に変換
df_video = df_video.withColumn(
    'published_at',
    F.to_timestamp(F.col('published_at'), "yyyy-MM-dd\\'T\\'HH:mm:ssX")
)

# durationを秒数に変換

# commentデータ型変更
# published_atコラムをデータ型に変換
df_comment.printSchema()
column_count = len(df_comment.columns)
print(column_count)
df_comment.show(5, truncate = False)

# ////////////
# 欠損、重複値処理
# ////////////


# ////////////
# データの格納
# ////////////

spark.stop()