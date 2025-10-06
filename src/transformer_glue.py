from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, regexp_replace, trim, lit, sum
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, BooleanType
from pyspark.sql.window import Window
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark.sparkContext.setLogLevel("ERROR") 

S3_BASE_PATH_TRANSFORMED = "s3://sukima-youtube-bucket/data/transformed_data/" 
S3_BASE_PATH_RAW = "s3://sukima-youtube-bucket/data/raw_data/"

# ////////////
# DQの関数
# ////////////
def run_data_quality_check(df, glueContext, df_name):
    dyf_to_check = DynamicFrame.fromDF(df, glueContext, df_name)
    
    print(f"--- Running Data Quality Check for {df_name} ---")

    data_quality_output = dyf_to_check.evaluateDataQuality()

    dq_results = data_quality_output.data_quality_evaluation_results
    if not dq_results.success:
        print(f"!!! Data Quality Check FAILED for {df_name}. Errors found: {dq_results.num_errors} !!!")
    else:
        print(f"Data Quality Check PASSED for {df_name}.")
    return True

# ルールの読み込み
dqdl_ruleset_string = """
Rules = [
    # 1. channel_id は必ず存在すること (null, 欠損値ではない)
    IsComplete "channel_id",
    
    # 2. channel_id には重複がないこと (ユニーク制約)
    IsUnique "channel_id",
    
    # 3. published_at が90%以上埋まっていること (あなたの要望)
    Completeness "published_at" >= 0.90,
    
    # 4. created_at の値が有効な日付形式であること (型検証)
    IsOfType "created_at", "date"
]
"""

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
    StructField("published_at", StringType(), False),
    StructField("view_count", LongType(), False),
    StructField("like_count", LongType(), False),
    StructField("comment_count", LongType(), False),
    StructField("duration", StringType(), False),
    StructField("tags", StringType(), False)
])

# commentデータのスキーマ設計
comment_schema = StructType([
    StructField("video_id", StringType(), False),
    StructField("comment_id", StringType(), False),
    StructField("author_display_name", StringType(), False),
    StructField("published_at", StringType(), False),
    StructField("text_display", StringType(), False),
    StructField("like_count", LongType(), False)
])

# ////////////
# データの読み込み
# ////////////
df_channel = spark.read.schema(channel_schema).json(f"{S3_BASE_PATH_RAW}sukima_channel.json")
df_video = spark.read.schema(video_schema).json(f"{S3_BASE_PATH_RAW}sukima_video.json")
df_comment = spark.read.schema(comment_schema).json(f"{S3_BASE_PATH_RAW}sukima_comment.json")

# ////////////
# データ型変換
# ////////////
# channelデータ型変更
df_channel = df_channel.withColumn(
    'published_at',
    F.col('published_at').cast('timestamp')
)

# videoデータ型変更
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
df_comment = df_comment.withColumn(
    'published_at',
    F.col('published_at').cast('timestamp')
)

# ////////////
# 欠損、重複値処理(必ず欠損→重複の順番で処理を行う)
# ////////////
# 欠損値の処理
df_channel = df_channel.filter(F.col('channel_id').isNotNull())

df_video = df_video.filter(
    F.col('video_id').isNotNull() & 
    F.col('published_at').isNotNull()
)

df_comment = df_comment.filter(
    F.col('comment_id').isNotNull() &
    F.col('published_at').isNotNull()
)

# 重複値の処理
# video_id ごとにグループ化し、published_at_ts の降順（DESC）でソート
window_channel = Window.partitionBy('channel_id').orderBy(F.col('published_at').desc())
df_channel_ranked = df_channel.withColumn('rank', F.row_number().over(window_channel))
df_channel = df_channel_ranked.filter(F.col('rank') == 1).drop('rank')

window_video = Window.partitionBy('video_id').orderBy(F.col('published_at').desc())
df_video_ranked = df_video.withColumn('rank', F.row_number().over(window_video))
df_video = df_video_ranked.filter(F.col('rank') == 1).drop('rank')

window_comment = Window.partitionBy('comment_id').orderBy(F.col('published_at').desc())
df_comment_ranked = df_comment.withColumn('rank', F.row_number().over(window_comment))
df_comment = df_comment_ranked.filter(F.col('rank')==1).drop('rank')

# ////////////
# DataQualityの実行
# ////////////
run_data_quality_check(df_channel, glueContext, "channel")
run_data_quality_check(df_video, glueContext, "video")
run_data_quality_check(df_comment, glueContext, "comment")

# ////////////
# データの格納
# ////////////
df_channel.write.mode("overwrite").parquet(f"{S3_BASE_PATH_TRANSFORMED}sukima_transformed_channel")
df_video.write.mode("overwrite").parquet(f"{S3_BASE_PATH_TRANSFORMED}sukima_transformed_video")
df_comment.write.mode("overwrite").parquet(f"{S3_BASE_PATH_TRANSFORMED}sukima_transformed_comment")

spark.stop()