

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
