# Databricks notebook source
# カタログ・スキーマパス
catalog = "komae"
schema = "amazon"
volume = "raw"
table = "raw_review"

# ファイルパス
source_path = '/databricks-datasets/amazon/test4K/' # ソースファイルパス
# volume_path = f'/Volumes/{catalog}/{schema}/{volume}' # 作業用ボリュームパス

# COMMAND ----------

# SQLで変数使うので設定
spark.conf.set("c.catalog", catalog)
spark.conf.set("c.schema", schema)
spark.conf.set("c.volume", volume)
spark.conf.set("c.table", table)

# COMMAND ----------

print(f"source_path: {source_path}")
print(f"catalog: {catalog}")
print(f"schema: {schema}")
print(f"table: {table}")
