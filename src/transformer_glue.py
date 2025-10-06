import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, to_date, regexp_replace, trim, lit, sum
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, BooleanType
from pyspark.sql.window import Window
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality
import boto3

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_base_path_raw', 's3_base_path_transformed', 'dq_report_base_path', 'crawler_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.sparkContext.setLogLevel("ERROR") 

# ////////////
# 環境変数呼び出し
# ////////////
S3_BASE_PATH_RAW = args['s3_base_path_raw']
S3_BASE_PATH_TRANSFORMED = args['s3_base_path_transformed']
DQ_REPORT_BASE_PATH = args['dq_report_base_path']
CRAWLER_NAME = args['crawler_name']

# ////////////
# DQの関数
# ////////////
def run_data_quality_check(df, glueContext, df_name, result_s3_prefix):
    dyf_to_check = DynamicFrame.fromDF(df, glueContext, df_name)
    
    if df_name == "channel":
        dqdl_ruleset = """
        Rules = [
            IsComplete "channel_id",
            IsUnique "channel_id",
            Completeness "published_at" >= 0.90
        ]
        """
        
    elif df_name == "video":
        dqdl_ruleset = """
        Rules = [
            IsComplete "video_id",
            IsUnique "video_id",
            Completeness "total_seconds" >= 0.90,
            Completeness "published_at" >= 0.90
        ]
        """
        
    elif df_name == "comment":
        dqdl_ruleset = """
        Rules = [
            IsComplete "comment_id",
            IsUnique "comment_id",
            Completeness "published_at" >= 0.90
        ]
        """

    dq_results = EvaluateDataQuality.apply(
        frame=dyf_to_check,
        ruleset=dqdl_ruleset,
        publishing_options={
            "dataQualityEvaluationContext": df_name,
            "enableDataQualityResultsPublishing": True,
            "resultsS3Prefix": result_s3_prefix
        }
    )
    dq_df = dq_results.toDF()
    
    return dq_df

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
# S3_BASE_PATH = s3://sukima-youtube-bucket/data/raw_data/
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
run_data_quality_check(
    df_channel,
    glueContext,
    "channel",
    f"{DQ_REPORT_BASE_PATH}channel/"
    )
    
run_data_quality_check(
    df_video,
    glueContext,
    "video",
    f"{DQ_REPORT_BASE_PATH}video/"
    )

run_data_quality_check(
    df_comment,
    glueContext,
    "comment",
    f"{DQ_REPORT_BASE_PATH}comment/"
    )
    
# ////////////
# データの格納
# ////////////
df_channel.write.mode("overwrite").parquet(f"{S3_BASE_PATH_TRANSFORMED}sukima_transformed_channel")
df_video.write.mode("overwrite").parquet(f"{S3_BASE_PATH_TRANSFORMED}sukima_transformed_video")
df_comment.write.mode("overwrite").parquet(f"{S3_BASE_PATH_TRANSFORMED}sukima_transformed_comment")

# ////////////
# データカタログの更新
# ////////////
try:
    glue_client = boto3.client('glue')
    print(f"Attempting to start crawler: {CRAWLER_NAME}")

    glue_client.start_crawler(Name=CRAWLER_NAME)
    print("Crawler started successfully to update Data Catalog.")

except Exception as e:
    print(f"Warning: Error starting crawler {CRAWLER_NAME}: {e}")
    
job.commit()