CREATE OR REPLACE EXTERNAL TABLE `gig-sandbox-ai.example_dataset.example_image`
-- Cloud Resource に接続できる外部接続ID
WITH CONNECTION `us.example_connection`
OPTIONS(
  object_metadata = 'SIMPLE',
-- 非構造化データを持つバケットの URI
  uris = ['gs://sandbox_rag/rulebook_masterrule20200401_ver1.0.pdf']
);

-- テキスト検出モデルの作成
CREATE OR REPLACE MODEL
`gig-sandbox-ai.example_dataset.annotate_image`
REMOTE WITH CONNECTION `us.example_connection`
OPTIONS (REMOTE_SERVICE_TYPE = 'CLOUD_AI_VISION_V1');
