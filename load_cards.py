import os
from typing import List, Dict, Any
from langchain_community.document_loaders import DirectoryLoader
from langchain_community.document_loaders import JSONLoader
from google.cloud import bigquery
from google.api_core import retry
import json
from datetime import datetime

class CardLoader:
    def __init__(self, project_id: str, dataset_id: str, table_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.client = bigquery.Client(project=project_id)
        self.schema = [
            bigquery.SchemaField("card_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("card_info", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
        ]
        
        self.table_ref = f"{project_id}.{dataset_id}.{table_id}"
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        try:
            self.client.get_table(self.table_ref)
        except Exception:
            table = bigquery.Table(self.table_ref, schema=self.schema)
            self.client.create_table(table)
            print(f"Created table {self.table_ref}")

    def load_json_files(self, directory_path: str) -> List[Dict[str, Any]]:
        """
        ディレクトリ内のJSONファイルを読み込む
        Args:
            directory_path: JSONファイルが格納されているディレクトリパス
        Returns:
            List[Dict]: カードデータのリスト
        """
        all_data = []
        for filename in os.listdir(directory_path):
            if filename.endswith('.json'):
                file_path = os.path.join(directory_path, filename)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        all_data.append(data)
                except Exception as e:
                    print(f"Error loading {filename}: {e}")
        return all_data

    @retry.Retry()
    def insert_to_bigquery(self, card_data: List[Dict[str, Any]], batch_size: int = 1000):
        """
        BigQueryにデータを挿入
        Args:
            card_data: カードデータのリスト
            batch_size: 一度に挿入するレコード数
        """
        for i in range(0, len(card_data), batch_size):
            batch = card_data[i:i + batch_size]
            
            errors = self.client.insert_rows_json(
                self.table_ref,
                batch,
                row_ids=[str(row['card_id']) for row in batch]
            )
            
            if errors:
                print(f"Errors occurred while inserting batch {i//batch_size + 1}:")
                for error in errors:
                    print(error)
            else:
                print(f"Successfully inserted batch {i//batch_size + 1}")

    def process_directory(self, directory_path: str, batch_size: int = 1000):
        """
        ディレクトリ内のすべてのJSONファイルを処理してBigQueryに挿入
        Args:
            directory_path: JSONファイルが格納されているディレクトリパス
            batch_size: 一度に挿入するレコード数
        """
        print(f"Loading files from {directory_path}")
        card_data = self.load_json_files(directory_path)
        print(f"Loaded {len(card_data)} card records")
        
        print("Inserting data into BigQuery")
        self.insert_to_bigquery(card_data, batch_size)
        print("Data insertion completed")

def main():
    # 設定
    project_id = "gig-sandbox-ai"  # あなたのGCPプロジェクトID
    dataset_id = "yugioh_dataset"     # 作成するデータセット名
    table_id = "cards"          # 作成するテーブル名
    directory_path = "card_data"    # JSONファイルが格納されているディレクトリ
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gig-sandbox-ai-a724e4b9b06e.json"

    # ローダーの初期化と実行
    loader = CardLoader(project_id, dataset_id, table_id)
    loader.process_directory(directory_path)

if __name__ == "__main__":
    main()
