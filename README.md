# md-chatbot
## 概要
bigqueryとgeminiを使ったchatbotの仕組み

## 開発
### スキーマ確認
```
select schema_name
from gig-sandbox-ai.`region-us`.INFORMATION_SCHEMA.SCHEMATA;
```

### 事前準備
#### bigquery
https://cloud.google.com/bigquery/docs/generate-text-tutorial?hl=ja#console
基本的にこのチュートリアルに従う。
使っているモデルがtext-bisonのところはgeminiを使う

#### 指定できるモデル
https://cloud.google.com/vertex-ai/generative-ai/docs/learn/models?hl=ja#gemini-models

gemini-1.5-proとかを指定できる
