import os
import time
import vertexai
from vertexai.generative_models import GenerativeModel, Part
import PyPDF2
import io

def ensure_output_directory():
    """出力ディレクトリの作成"""
    output_dir = "output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    return output_dir

def create_batch_pdf(pages):
    """複数ページをまとめた一時的なPDFを作成"""
    pdf_writer = PyPDF2.PdfWriter()
    for page in pages:
        pdf_writer.add_page(page)
    
    pdf_bytes = io.BytesIO()
    pdf_writer.write(pdf_bytes)
    pdf_bytes.seek(0)
    return pdf_bytes

def process_pdf_batch(model, pages, start_page, total_pages, prompt: str):
    """複数ページをまとめて処理"""
    try:
        # バッチ処理用のPDFを作成
        pdf_bytes = create_batch_pdf(pages)
        
        # PDFをPartオブジェクトとして準備
        pdf_part = Part.from_data(pdf_bytes.getvalue(), mime_type="application/pdf")
        contents = [pdf_part, prompt]
        
        # Geminiで処理
        response = model.generate_content(contents)
        result = response.text
        
    except Exception as e:
        result = f"Error processing pages {start_page}-{start_page + len(pages) - 1}: {str(e)}"
        print(result)
    
    finally:
        pdf_bytes.close()
    
    return result

def split_and_save_results(result, start_page, num_pages, output_dir):
    """バッチ処理の結果を個別のファイルに分割して保存"""
    # 結果をページごとに分割
    page_contents = []
    current_content = []
    lines = result.split('\n')
    
    for line in lines:
        if line.strip().startswith('[Page'):
            if current_content:
                page_contents.append('\n'.join(current_content))
                current_content = []
        current_content.append(line.replace('[Page ', '').replace(']', ''))
    
    if current_content:
        page_contents.append('\n'.join(current_content))
    
    # 各ページの内容を個別のファイルに保存
    for i, content in enumerate(page_contents):
        page_num = start_page + i
        output_file = os.path.join(output_dir, f"{page_num}.txt")
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(content.strip())
        print(f"Saved page {page_num} to {output_file}")

def process_pdf_pages(project_id: str, pdf_path: str, prompt: str, batch_size: int = 5):
    # Google Cloud の初期化
    vertexai.init(project=project_id, location="us-central1")
    model = GenerativeModel("gemini-1.5-pro-001")
    
    # 出力ディレクトリの準備
    output_dir = ensure_output_directory()
    
    # PDFを開いてページ数を取得
    with open(pdf_path, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        num_pages = len(pdf_reader.pages)
        
        print(f"Processing {num_pages} pages in {pdf_path}")
        
        # バッチ単位で処理
        for start_idx in range(0, num_pages, batch_size):
            if start_idx > 0:
                time.sleep(2)  # API制限を考慮した待機
            
            end_idx = min(start_idx + batch_size, num_pages)
            current_pages = [pdf_reader.pages[i] for i in range(start_idx, end_idx)]
            
            print(f"Processing pages {start_idx + 1}-{end_idx}/{num_pages}")
            
            # バッチ処理
            result = process_pdf_batch(
                model,
                current_pages,
                start_idx + 1,
                num_pages,
                prompt
            )
            
            # 結果を分割して保存
            split_and_save_results(result, start_idx + 1, num_pages, output_dir)

def main():
    # 設定
    project_id = "gig-sandbox-ai"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gig-sandbox-ai-a724e4b9b06e.json"
    
    prompt = """
    PDFの内容にはカードゲームのルールが記載されています。
    のちにLLMモデルに学習させるため、この内容を学習しやすい形で文字起こししてください。
    ページ番号は無視して大丈夫です。
    """
    
    # ローカルのPDFパス
    pdf_path = "rulebook.pdf"
    
    # バッチサイズを指定して処理実行
    process_pdf_pages(project_id, pdf_path, prompt, batch_size=5)
    
    print("Processing completed. Results saved to output directory")

if __name__ == "__main__":
    main()
