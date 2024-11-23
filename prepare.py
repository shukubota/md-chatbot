from google.cloud import bigquery
import os

class YugiohDatasetPreparator:
    def __init__(self, project_id: str, location: str = "us-central1"):
        """
        初期化
        Args:
            project_id: GCPプロジェクトID
            location: BigQuery データセットのロケーション
        """
        self.project_id = project_id
        self.location = location
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = "yugioh_dataset"

    def create_dataset(self) -> str:
        """
        遊戯王カード用のデータセットを作成
        Returns:
            作成したデータセットのID
        """
        dataset_id = f"{self.project_id}.{self.dataset_id}"
        
        try:
            # データセットが存在するか確認
            self.client.get_dataset(dataset_id)
            print(f"Dataset {dataset_id} already exists")
        except Exception:
            # データセットが存在しない場合は作成
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = self.location
            
            # データセット作成時のパラメータ設定
            dataset.description = "遊戯王カード情報とその埋め込みベクトルを保存するデータセット"
            dataset.default_table_expiration_ms = None  # テーブルの有効期限を無期限に設定
            
            # データセットを作成
            dataset = self.client.create_dataset(dataset, timeout=30)
            print(f"Created dataset {dataset_id}")
        
        return dataset_id

def main():
    # 環境設定
    project_id = "gig-sandbox-ai"
    location = "us-central1"
    
    # 認証設定
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gig-sandbox-ai-a724e4b9b06e.json"
    
    # データセット作成
    preparator = YugiohDatasetPreparator(
        project_id=project_id,
        location=location
    )
    
    try:
        dataset_id = preparator.create_dataset()
        print(f"Successfully prepared dataset: {dataset_id}")
    except Exception as e:
        print(f"Error preparing dataset: {e}")

if __name__ == "__main__":
    main()
