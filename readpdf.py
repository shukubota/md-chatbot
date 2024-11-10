import os
import time
import vertexai
from vertexai.generative_models import GenerativeModel, Part
import PyPDF2
import io

def process_pdf_pages(project_id: str, pdf_path: str, prompt: str):
    """PDFを1ページずつ処理"""
    # Google Cloud の初期化
    vertexai.init(project=project_id, location="us-central1")
    model = GenerativeModel("gemini-1.5-pro-001")
    
    # PDFを開いてページ数を取得
    with open(pdf_path, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        num_pages = len(pdf_reader.pages)

        print(f"Processing {num_pages} pages in {pdf_path}")
        
        # 1ページずつ処理
        all_summaries = []
        for page_num in range(num_pages):
            if page_num > 0:
                time.sleep(2)
            print(f"Processing page {page_num + 1}/{num_pages}")
            
            # 単一ページのPDFを作成
            pdf_writer = PyPDF2.PdfWriter()
            pdf_writer.add_page(pdf_reader.pages[page_num])
            
            # メモリ上に一時的なPDFを作成
            pdf_bytes = io.BytesIO()
            pdf_writer.write(pdf_bytes)
            pdf_bytes.seek(0)
            
            # Geminiで処理
            try:
                page_prompt = f"""
                {prompt}
                This is page {page_num + 1} of {num_pages}.
                Please focus on summarizing this specific page.
                """
                
                # PDFページをPartオブジェクトとして準備
                pdf_part = Part.from_data(pdf_bytes.getvalue(), mime_type="application/pdf")
                contents = [pdf_part, page_prompt]
                
                # Geminiで処理
                response = model.generate_content(contents)
                
                # 結果を保存
                summary = f"{response.text}"
                all_summaries.append(summary)
                print(f"Completed page {page_num + 1}")
                
            except Exception as e:
                error_msg = f"\n=== Error processing page {page_num + 1}/{num_pages} ===\n{str(e)}\n"
                all_summaries.append(error_msg)
                print(error_msg)
                
            # メモリを解放
            pdf_bytes.close()
    
    return "\n".join(all_summaries)

def main():
    # 設定
    project_id = "gig-sandbox-ai"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gig-sandbox-ai-a724e4b9b06e.json"
    
    prompt = """
    PDFの内容を文字起こししてください。ページ番号は不要です。
    """
    
    # ローカルのPDFパス
    pdf_path = "rulebook.pdf"
    
    # 処理実行
    result = process_pdf_pages(project_id, pdf_path, prompt)
    
    # 結果を保存
    output_file = "pdf_summary_result.txt"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(result)
    
    print(f"Processing completed. Results saved to {output_file}")

if __name__ == "__main__":
    main()