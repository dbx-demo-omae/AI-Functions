-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## AI Functions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 感情分析 - ai_analyze_sentiment
-- MAGIC - [ai_analyze_sentiment()](https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/functions/ai_analyze_sentiment)

-- COMMAND ----------

SELECT
  review
  , ai_analyze_sentiment(review) AS sentiment
FROM
  komae.amazon.review_with_id
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 分類 - ai_classify
-- MAGIC - [ai_classify()](https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/functions/ai_classify)

-- COMMAND ----------

-- 分類
SELECT
   review
   , ai_translate(review, "ja")
   , ai_classify(review, ARRAY("製品品質", "使用体験", "価格帯価格", "カスタマーサービス", "その他"))
FROM
   komae.amazon.review_with_id
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### エンティティ抽出 - ai_extract
-- MAGIC - [ai_extract()](https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/functions/ai_extract)

-- COMMAND ----------

-- -- 英語
-- SELECT ai_extract(
--   'John Doe lives in New York and works for Acme Corp.',
--   ARRAY('person', 'location', 'organization')
-- );

-- 日本語
SELECT ai_extract(
  'ジョン・ドウはニューヨークに住んでおり、Acme Corp.に勤めています。',
  ARRAY('人', '位置', '組織')
);

-- COMMAND ----------

-- -- 英語
-- SELECT ai_extract(
--   'Send an email to jane.doe@example.com about the meeting at 10am.',
--   ARRAY('email', 'time')
-- );

-- 日本語
SELECT ai_extract(
  '午前 10 時の会議について jane.doe@example.com にメールを送信します。',
  ARRAY('メールアドレス', '時間')
);

-- COMMAND ----------

WITH review_ja AS (
  SELECT
      review
      , ai_translate(review, "ja") AS review_ja
  FROM
      komae.amazon.review_with_id
  LIMIT 10
)
SELECT
  ai_extract(review_ja, ARRAY('感情', '商品名', '人')) AS result
FROM
  review_ja

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### グラマー修正 - ai_fix_grammar
-- MAGIC - [ai_fix_grammar()](https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/functions/ai_fix_grammar)

-- COMMAND ----------

-- グラマー修正
SELECT
    ai_fix_grammar('この文章は何か間違いがあryそうです');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### テキスト生成 - ai_gen
-- MAGIC - [ai_gen()](https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/functions/ai_gen)

-- COMMAND ----------

-- テキスト生成
SELECT
    ai_gen('20% 割引の夏の自転車セール用に、簡潔で陽気なメール タイトルを作成します。日本語でお願いします。');

-- COMMAND ----------

WITH summarize AS (
  SELECT
    review
    , ai_summarize(review, 15) AS review_summary
  FROM
    komae.amazon.review_with_id
  LIMIT 1
)
, generation AS (
  SELECT
    review
    , ai_gen(
      "You are an Amazon customer support professional. Write an appropriate response to the customer's review. Reviews:　" || review_summary
    ) AS summary
  FROM
    summarize
)
SELECT
  -- review
  ai_translate(review, "ja") AS review
  , ai_translate(summary, "ja") AS summary
FROM generation;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### マスク - ai_mask
-- MAGIC - [ai_mask()](https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/functions/ai_mask)
-- MAGIC - 最先端の生成 AI モデルを呼び出して、SQL を使用して特定のテキスト内の指定されたエンティティをマスクできます。

-- COMMAND ----------

-- 英語
-- SELECT ai_mask(
--   'John Doe lives in New York. His email is john.doe@example.com.',
--   ARRAY('person', 'email')
-- );

-- 日本語
SELECT ai_mask(
  'ジョンはニューヨークに住んでいます。彼のメールアドレスは john.doe@example.com　です。',
  ARRAY('人', 'メールアドレス')
);

-- COMMAND ----------

-- -- 英語
-- SELECT ai_mask(
--   'Contact me at 555-1234 or visit us at 123 Main St.',
--   ARRAY('phone', 'address')
-- );

-- 日本語
SELECT ai_mask(
  '555-1234までご連絡いただくか、123　Main St. までお越しください。',
  ARRAY('電話番号', '住所')
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 類似度 - ai_similarity
-- MAGIC - [ai_similarity()](https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/functions/ai_similarity)

-- COMMAND ----------

-- 類似度計算
SELECT
    review
    , ai_translate(review, "ja") AS review_ja
    , ai_similarity(review, 'Customer Support') AS similarity
FROM
    komae.amazon.review_with_id
ORDER BY
    similarity DESC
LIMIT 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 要約 - ai_summarize
-- MAGIC - [ai_summarize()](https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/functions/ai_summarize)

-- COMMAND ----------

-- 英語
-- DECLARE OR REPLACE VARIABLE original STRING DEFAULT NULL;
-- SET VARIABLE original = 'Apache Spark is a unified analytics engine for large-scale data processing. ' ||
--     'It provides high-level APIs in Java, Scala, Python and R, and an optimized ' ||
--     'engine that supports general execution graphs. It also supports a rich set ' ||
--     'of higher-level tools including Spark SQL for SQL and structured data ' ||
--     'processing, pandas API on Spark for pandas workloads, MLlib for machine ' ||
--     'learning, GraphX for graph processing, and Structured Streaming for incremental ' ||
--     'computation and stream processing.';
-- SELECT ai_summarize(original,　15);

-- 日本語
DECLARE OR REPLACE VARIABLE original STRING DEFAULT NULL;
SET VARIABLE original = 'Apache Sparkは、大規模なデータ処理のための統合されたアナリティクスエンジンです。' ||
    'Java, Scala, Python, Rで高水準のAPIを提供し、一般的な実行グラフをサポートする最適化されたエンジンも備えています。' ||
    'さらに、Spark SQL for SQLおよび構造化データ処理、pandas API on Spark for pandasワークロード、' ||
    'MLlib for マシンラーニング、GraphX for グラフ処理、Structured Streaming for 増分計算' ||
    'およびストリーミング処理などの高水準ツールもサポートしています。';

SELECT
  original
  , ai_translate(ai_summarize(original,　15), "ja") AS summary
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 翻訳 - ai_translate
-- MAGIC - [ai_translate()](https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/functions/ai_translate)

-- COMMAND ----------

SELECT
  review
  , ai_translate(review, "ja")
FROM
  komae.amazon.review_with_id
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ai_query
-- MAGIC - [ai_query()](https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/functions/ai_query)
-- MAGIC - 既存の Azure Databricks Model Serving エンドポイントを呼び出し、その応答を解析して返します。

-- COMMAND ----------

-- 変数定義
DECLARE OR REPLACE VARIABLE my_text STRING DEFAULT NULL;
SET VARIABLE my_text = 'Describe Databricks SQL in 30 words.';

-- ai_queryでLLMモデル実行
WITH explanation AS (
    SELECT
        -- 基盤モデル　/ DBRX
        ai_query('databricks-dbrx-instruct', my_text) AS text_dbrx
        -- 外部モデル / OpenAI gpt-3.5-turbo-instruct
        , ai_query('komae-openai-completions', my_text) AS text_external_openai
)
SELECT
    ai_translate(text_dbrx, "ja")
    , ai_translate(text_external_openai, "ja")
FROM
    explanation

-- COMMAND ----------

-- グラマー修正
CREATE OR REPLACE FUNCTION komae.amazon.correct_grammar(text STRING)
  RETURNS STRING
  COMMENT "【処理】英語のグラマーを修正します　【入力】修正したい英文　【モデル】llama-2-70b-chat"
  RETURN ai_query(
    'databricks-llama-2-70b-chat',
    CONCAT('Correct this to standard English:\n', text));

SELECT
  komae.amazon.correct_grammar("She don't likes to read book because it make her feel boring always.") AS text

-- COMMAND ----------

-- Embeddings
CREATE OR REPLACE FUNCTION komae.amazon.get_embeddings(text STRING)
  RETURNS ARRAY<FLOAT>
  COMMENT "【処理】文章をEmbeddingsに変換します　【入力】Embeddings変換したい文章(日本語OK)　【モデル】text-embedding-ada-002"
  RETURN ai_query('komae-text-embedding-ada-002', text);

-- 単発実行
-- SELECT
--   komae.amazon.get_embeddings("She don't likes to read book because it make her feel boring always.") AS embeddings
-- ;

-- テーブルに対して実行
SELECT
  review
  ,komae.amazon.get_embeddings(review) AS embeddings
FROM
  komae.amazon.review_with_id
LIMIT 100
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### vector_search
-- MAGIC - [vector_search()](https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/functions/vector_search)
-- MAGIC - SQL を使用して Mosaic AI ベクトル検索インデックスでクエリを実行できます。

-- COMMAND ----------

SELECT *
FROM
  VECTOR_SEARCH(index => "komae.amazon.review_vs_index", query => "iphone", num_results => 2)
