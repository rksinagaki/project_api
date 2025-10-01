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
    F.col('published_at').cast('timestamp')
)

# videoデータ型変更
# published_atコラムをデータ型に変換
df_video = df_video.withColumn(
    'published_at',
    F.col('published_at').cast('timestamp')
)

# durationを秒数に変換
df_video = df_video.withColumn(
    "total_seconds",
    (
        F.coalesce(F.regexp_extract(F.col("duration"), "(\d+)H", 1).cast(LongType()), F.lit(0)) * 3600
    ) + (
        F.coalesce(F.regexp_extract(F.col("duration"), "(\d+)M", 1).cast(LongType()), F.lit(0)) * 60
    ) + (
        F.coalesce(F.regexp_extract(F.col("duration"), "(\d+)S", 1).cast(LongType()), F.lit(0))
    )
)

# commentデータ型変更
# published_atコラムをデータ型に変換
df_comment = df_comment.withColumn(
    'published_at',
    F.col('published_at').cast('timestamp')
)

# 確認
# df_video.printSchema()
# column_count = len(df_video.columns)
# print(column_count)
# df_video.show(5, truncate = False)

# ////////////
# 欠損、重複値処理
# ////////////
df_duplicates = df_comment.groupBy('comment_id').count().filter(F.col("count") > 1)
num_duplicate_keys = df_duplicates.count()
print(f"重複している {'comment_id'} の種類数: {num_duplicate_keys}")

# 欠損値の確認
null_check_exprs = [
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"null_count_{c}")
    for c in df_video.columns
]
# df_video.select(*null_check_exprs).show()

# カウントを実行し、結果を整形して表示
if null_check_exprs:
    null_df = df_video.select(*null_check_exprs).collect()[0].asDict()

    print(f"【欠損値チェック結果】")
    print("-----------------------------------")
    for c in df_video.columns:
        count = null_df.get(f"null_count_{c}", 0)
        ratio = null_df.get(f"null_ratio_{c}", 0.0)
        
        # NULL が一件でもあるカラムのみ表示（品質レポートをシンプルにするため）
        if count > 0:
            print(f"カラム '{c}': {count} 件 ({ratio:.2f}%)")
            
    print("-----------------------------------")

# ////////////
# データの格納
# ////////////

spark.stop()