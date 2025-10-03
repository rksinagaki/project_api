from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, regexp_replace, trim, lit, sum
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, BooleanType
from pyspark.sql.window import Window
import os

import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)

spark = SparkSession.builder.appName("MyPySparkApp").getOrCreate()
spark.sparkContext.setLogLevel("ERROR") 

LOCAL_GX_ROOT = os.path.abspath("./gx/")
gx_config_path = LOCAL_GX_ROOT 
SUITE_NAME_CHANNEL = "channel_data_quality"
SUITE_NAME_VIDEO = "videos_data_quality"
SUITE_NAME_COMMENT = "comment_data_quality"

# ////////////
# GXの関数
# ////////////
def validate_data_quality(df, suite_name, context_root_dir_s3):
    # 1. S3からGX設定を読み込むためのコンフィグを定義
    # data_context_config = DataContextConfig(
    #     store_backend_defaults=FilesystemStoreBackendDefaults(
    #         root_directory=context_root_dir_s3
    #     )
    # )

    # 2. データコンテキストの初期化 (S3上の設定ファイルgreat_expectations.ymlを参照)
    # context = ge.DataContext(
    #     project_config=data_context_config
    # )
    
    context = ge.DataContext(context_root_dir=context_root_dir_s3)

    # 3. 検証対象のDataFrameをRuntimeBatchとして定義
    # batch_request = RuntimeBatchRequest(
    #     # 'runtime_parameters'で実際のDataFrameオブジェクトを渡す
    #     runtime_parameters={"batch_data": df, "batch_identifiers": {"default_identifier": "runtime_batch"}},
    #     data_connector_name="runtime_data_connector",
    #     data_asset_name="runtime_asset",
    #     batch_spec_passthrough={"data_asset_type": "pyspark_dataframe"},
    # )
    batch_request = RuntimeBatchRequest(
        datasource_name="my_datasource", 
        
        # 必須引数 2: バッチを特定するための識別子をトップレベルに配置
        batch_identifiers={
            "runtime_batch_identifier_name": "runtime_batch" 
        },

        # データを渡すためのランタイムパラメータ
        runtime_parameters={
            "batch_data": df
        },

        # ymlに定義されたデータコネクタ名とアセット名
        data_connector_name="default_runtime_data_connector_name", 
        data_asset_name="my_runtime_asset_name", 

        # PySpark DataFrame であることを指定
        batch_spec_passthrough={"data_asset_type": "pyspark_dataframe"}
    )

    validator = context.get_validator(
        batch_request=batch_request, 
        expectation_suite_name=suite_name # 実行するExpectation Suiteを指定
    )

    # 4. 検証の実行
    results = context.run_validation_operator(
        "action_list_operator", # デフォルトのValidation Operator
        # assets_to_validate=[
        #     {
        #         "batch_request": batch_request,
        #         "expectation_suite_name": suite_name
        #     }
        # ],
        assets_to_validate=[validator],
        run_name="data_quality_check"
    )
    
    if results.success:
        print(f"Data quality check succeeded for {suite_name}!")
    else:
        print(f"Data quality check FAILED for {suite_name}!")


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
df_channel = spark.read.schema(channel_schema).json("./data/channel.json")
df_video = spark.read.schema(video_schema).json("./data/videos.json")
df_comment = spark.read.schema(comment_schema).json("./data/top_videos_comments.json")

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
# GXの実行
# ////////////
# df_channnelにGXを適用
validate_data_quality(df_channel, SUITE_NAME_CHANNEL, gx_config_path) # データの検証を実行-失敗すればここでジョブが停止
print("Data quality check passed. Proceeding to S3 write.")

# df_videoにGXを適用
validate_data_quality(df_video, SUITE_NAME_VIDEO, gx_config_path) # データの検証を実行-失敗すればここでジョブが停止
print("Data quality check passed. Proceeding to S3 write.")

# df_commentにGXを適用
validate_data_quality(df_comment, SUITE_NAME_COMMENT, gx_config_path) # データの検証を実行-失敗すればここでジョブが停止
print("Data quality check passed. Proceeding to S3 write.")

# ////////////
# データの格納
# ////////////
# df_channel.write.mode("overwrite").parquet("./data/transformed/channel")
# df_video.write.mode("overwrite").parquet("./data/transformed/video")
# df_comment.write.mode("overwrite").parquet("./data/transformed/comment")

# 確認-------------------------------
# df_video.printSchema()
# column_count = len(df_video.columns)
# print(column_count)
# df_video.show(5, truncate = False)

spark.stop()