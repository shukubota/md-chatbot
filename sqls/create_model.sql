

-- embedding用のモデル作成
CREATE OR REPLACE MODEL `gig-sandbox-ai.example_dataset.example_embedding`
REMOTE WITH CONNECTION `us.example_connection`
  OPTIONS (ENDPOINT = 'text-multilingual-embedding-preview-0409');



-- embeddingの実行
CREATE OR REPLACE TABLE
  `gig-sandbox-ai.example_dataset.example_embedded_reviews` AS (
  SELECT
    *
  FROM
    ML.GENERATE_TEXT_EMBEDDING( MODEL `gig-sandbox-ai.example_dataset.example_embedding`,
      (
      SELECT
        review as content
      FROM
        `bigquery-public-data.imdb.reviews` ),
      STRUCT(TRUE AS flatten_json_output) ) );

-- ベクトル検索
DECLARE question_text STRING 
  DEFAULT "映画のレビューを教えてください";

WITH
  -- 質問文をベクトル化する
  embedded_question AS (
  SELECT
    *
  FROM
    ML.GENERATE_TEXT_EMBEDDING( MODEL `gig-sandbox-ai.example_dataset.example_embedding`,
      (SELECT question_text AS content),
      STRUCT(TRUE AS flatten_json_output)
    )
  ),
  -- ベクトル化された FAQ データを取得する
  embedded_faq AS (
  SELECT
    *
  FROM
    `gig-sandbox-ai.example_dataset.example_embedded_reviews` )
-- ベクトル距離（コサイン類似度）を算出する
SELECT
  q.content as question,
  f.content as reference,
  ML.DISTANCE(q.text_embedding, f.text_embedding, 'COSINE') AS vector_distance
FROM
  embedded_question AS q,
  embedded_faq AS f
ORDER BY
  vector_distance
