-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## AI Functions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Vector Search
-- MAGIC - [vector_search()](https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/functions/vector_search)
-- MAGIC - SQL を使用して Mosaic AI ベクトル検索インデックスでクエリを実行できます。

-- COMMAND ----------

SELECT
  id
  , review
FROM
  VECTOR_SEARCH(index => "komae.amazon.review_vs_index", query => "iphone", num_results => 2)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ai_forecast
-- MAGIC - [ai_forecast()](https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/functions/ai_forecast)
-- MAGIC - 時系列データを将来に当てはめるように設計されたテーブル値関数です。 
-- MAGIC - 最も一般的な形式では、`ai_forecast()` は、グループ化、多変量、混合細分性データを受け入れ、そのデータを将来のある期間まで予測します。

-- COMMAND ----------

WITH　aggregated AS (
  SELECT
    DATE(tpep_pickup_datetime) AS ds,
    SUM(fare_amount) AS revenue
  FROM
    komae.amazon.raw_nyctaxi
  GROUP BY
    1
)
SELECT * FROM AI_FORECAST(
  TABLE(aggregated),
  horizon => '2016-03-31',
  time_col => 'ds',
  value_col => 'revenue'
)
