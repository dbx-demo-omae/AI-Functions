# Databricks notebook source
# MAGIC %run ./00_init

# COMMAND ----------

# MAGIC %md
# MAGIC ## データの事前準備

# COMMAND ----------

# MAGIC %md
# MAGIC raw_reviewテーブル作る

# COMMAND ----------

# MAGIC %sql
# MAGIC -- カタログ、スキーマ、ボリューム作成
# MAGIC CREATE CATALOG IF NOT EXISTS ${c.catalog};
# MAGIC CREATE SCHEMA IF NOT EXISTS ${c.catalog}.${c.schema};
# MAGIC CREATE VOLUME IF NOT EXISTS ${c.catalog}.${c.schema}.${c.volume};

# COMMAND ----------

# ソースデータをボリュームにコピー
dbutils.fs.cp(source_path, f'/Volumes/{catalog}/{schema}/{volume}', recurse=True)

# COMMAND ----------

# RAWテーブル作成
spark.read.format('parquet').option('header', True).load(volume_path).write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.raw_review")

# データ確認
spark.sql(f"SELECT * FROM {catalog}.{schema}.{table}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC raw_reviewから、プライマリーキー`id`を付与したreview_with_pkテーブルを作る

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# Databricks Unity Catalogからテーブルを読み込み
# raw_review_df = spark.table(f"komae.amazon.raw_review")
raw_review_df = spark.table(f"{catalog}.{schema}.{table}")

# idカラムを追加する（1列目として）
id_df = raw_review_df.withColumn("id", monotonically_increasing_id())

# idカラムが1列目に来るように順序を入れ替え
columns = ["id"] + raw_review_df.columns
review_with_id_df = id_df.select(columns)

# 新しいテーブルとして書き出す
# review_with_id_df.write.format("delta").mode("overwrite").saveAsTable(f"komae.amazon.review_with_id")
review_with_id_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.review_with_id")

# Vectorインデックス作るのでCDF有効化
# spark.sql(f"ALTER TABLE komae.amazon.review_with_id SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
spark.sql(f"ALTER TABLE {catalog}.{schema}.review_with_id SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# 結果確認
review_with_id_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 時系列データを取得
# MAGIC - データソース: [nyctaxi-with-zipcodes](https://qiita.com/taka_yayoi/items/3f8ccad13c6efd242be1#nyctaxi)

# COMMAND ----------

# RAWテーブル作成
path_nyctaxi = 'dbfs:/databricks-datasets/nyctaxi-with-zipcodes/subsampled/'

(spark.read
        .format("delta")
        .load(path_nyctaxi)
        .write.mode("overwrite")
        .saveAsTable(f"komae.amazon.raw_nyctaxi"))
        # .saveAsTable(f"{catalog}.{schema}.raw_nyctaxi"))

# データ確認
raw_nyctaxi_df = spark.sql(f"SELECT * FROM komae.amazon.raw_nyctaxi")
# raw_nyctaxi_df = spark.sql(f"SELECT * FROM {catalog}.{schema}.raw_nyctaxi")
raw_nyctaxi_df.display()
