from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, regexp_replace, trim, lit, sum
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
df_channel = spark.read.json("./data/channel.json")
df_channel.printSchema()
column_types = df_channel.dtypes
print(column_types)

column_count = len(df_channel.columns)
print(f"列数: {column_count}")


# videoデータのスキーマ設計
# df_video = spark.read.json("./data/videos.json")
# df_video.printSchema()
# df_video.limit(5).show(truncate = False)

# row_count = df_video.count()
# print(f"行数: {row_count}")

# column_count = len(df_video.columns)
# print(f"列数: {column_count}")

# column_types = df_video.dtypes
# print(column_types)

# commentデータのスキーマ設計


# ////////////
# データの読み込み
# ////////////


# ////////////
# データ型変換
# ////////////


# ////////////
# 欠損、重複値処理
# ////////////


# ////////////
# データの格納
# ////////////

spark.stop()