from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, regexp_replace, trim, lit, sum
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyPySparkApp").getOrCreate()
spark.sparkContext.setLogLevel("ERROR") 

# ////////////
# スキーマ設計
# ////////////
# channelデータのスキーマ設計


# videoデータのスキーマ設計


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

df_video = spark.read.json("./data/videos.json")
df_video.printSchema()
# df_video.limit(5).show(truncate = False)

row_count = df_video.count()
print(f"行数: {row_count}")

column_count = len(df_video.columns)
print(f"列数: {column_count}")

column_types = df_video.dtypes
print(column_types)

spark.stop()