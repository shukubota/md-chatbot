from langchain_google_vertexai.embeddings import VertexAIEmbeddings
from typing import List, Dict, Any, Generator
from google.cloud import bigquery
import datetime
import os

class CardEmbedding:
    def __init__(self, project_id: str, dataset_id: str, location: str = "us-central1"):
        """
        初期化
        Args:
            project_id: GCPプロジェクトID
            dataset_id: BigQueryデータセットID
            location: VertexAI APIのロケーション
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        
        # VertexAI Embeddings の初期化
        self.embeddings = VertexAIEmbeddings(
            project=project_id,
            location=location,
            model_name="text-multilingual-embedding-preview-0409"
        )
        
        self.embedding_schema = [
            bigquery.SchemaField("card_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("card_info", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
            bigquery.SchemaField("embedding_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("model_name", "STRING", mode="REQUIRED"),
        ]

    def create_embedding_table(self, table_id: str):
        """埋め込みベクトル保存用のテーブルを作成"""
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        try:
            self.client.get_table(table_ref)
            print(f"Table {table_ref} already exists")
        except Exception:
            table = bigquery.Table(table_ref, schema=self.embedding_schema)
            self.client.create_table(table)
            print(f"Created table {table_ref}")
        return table_ref

    def stream_card_info(self, source_table: str, batch_size: int = 1000) -> Generator[List[Dict[str, Any]], None, None]:
        """BigQueryからカード情報をストリーミングで取得"""
        query = f"""
        SELECT card_id, card_info
        FROM `{self.project_id}.{self.dataset_id}.{source_table}`
        ORDER BY card_id
        """
        
        query_job = self.client.query(query)
        current_batch = []
        
        for row in query_job:
            if row.card_info:  # card_infoが存在する場合のみ処理
                current_batch.append({
                    "card_id": row.card_id,
                    "card_info": row.card_info
                })
                
                if len(current_batch) >= batch_size:
                    yield current_batch
                    current_batch = []
        
        if current_batch:  # 残りのデータを処理
            yield current_batch

    def generate_embeddings(self, cards: List[Dict[str, Any]], embedding_batch_size: int = 10) -> List[Dict[str, Any]]:
        """カード情報のテキストから埋め込みベクトルを生成"""
        embeddings_data = []
        
        for i in range(0, len(cards), embedding_batch_size):
            batch = cards[i:i + embedding_batch_size]
            texts = [card["card_info"] for card in batch]
            
            try:
                # VertexAI で埋め込みベクトルを生成
                vectors = self.embeddings.embed_documents(texts)
                
                for card, vector in zip(batch, vectors):
                    embeddings_data.append({
                        "card_id": card["card_id"],
                        "card_info": card["card_info"],
                        "embedding": vector,
                        "embedding_timestamp": datetime.datetime.now().isoformat(),
                        "model_name": self.embeddings.model_name
                    })
                
                print(f"Generated embeddings for cards {i} to {i + len(batch)}")
                
            except Exception as e:
                print(f"Error generating embeddings for batch starting at {i}: {e}")
        
        return embeddings_data

    def save_embeddings(self, embeddings_data: List[Dict[str, Any]], table_ref: str):
        """生成した埋め込みベクトルをBigQueryに保存"""
        if not embeddings_data:
            return True
            
        errors = self.client.insert_rows_json(
            table_ref,
            embeddings_data,
            row_ids=[str(row['card_id']) for row in embeddings_data]
        )
        
        if errors:
            print("Errors occurred while inserting embeddings:")
            for error in errors:
                print(error)
            return False
        return True

    def process_cards(self, 
                     source_table: str, 
                     destination_table: str, 
                     streaming_batch_size: int = 1000,
                     embedding_batch_size: int = 10):
        """カード情報の処理を実行（ストリーミング処理）"""
        print(f"Starting card processing with {self.embeddings.model_name}")
        table_ref = self.create_embedding_table(destination_table)
        
        total_processed = 0
        total_success = 0
        
        try:
            for batch in self.stream_card_info(source_table, streaming_batch_size):
                print(f"Processing batch of {len(batch)} cards...")
                
                embeddings_data = self.generate_embeddings(batch, embedding_batch_size)
                
                if embeddings_data:
                    if self.save_embeddings(embeddings_data, table_ref):
                        total_success += len(embeddings_data)
                
                total_processed += len(batch)
                print(f"Progress: {total_processed} cards processed, {total_success} successfully embedded")
        
        except Exception as e:
            print(f"Error during processing: {e}")
            
        finally:
            print(f"Processing completed. Total: {total_processed}, Success: {total_success}")
            return total_processed, total_success

def main():
    # 環境設定
    project_id = "gig-sandbox-ai"
    dataset_id = "yugioh_dataset"
    source_table = "cards"
    embedding_table = "card_embeddings"
    location = "us-central1"  # VertexAI APIのロケーション
    
    # 認証設定
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gig-sandbox-ai-a724e4b9b06e.json"
    
    embedder = CardEmbedding(
        project_id=project_id,
        dataset_id=dataset_id,
        location=location
    )
    
    total, success = embedder.process_cards(
        source_table=source_table,
        destination_table=embedding_table,
        streaming_batch_size=10,
        embedding_batch_size=10
    )
    
    print(f"Final results - Total processed: {total}, Successfully embedded: {success}")

if __name__ == "__main__":
    main()
