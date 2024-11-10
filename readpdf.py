import vertexai

from vertexai.generative_models import GenerativeModel, Part

# TODO(developer): Update and un-comment below lines
project_id = "gig-sandbox-ai"

vertexai.init(project=project_id, location="us-central1")

model = GenerativeModel("gemini-1.5-flash-001")

prompt = """
You are a very professional document summarization specialist.
Please summarize the given document.
"""

pdf_file_uri = "gs://sandbox_rag/rulebook_masterrule20200401_ver1.0.pd"
pdf_file = Part.from_uri(pdf_file_uri, mime_type="application/pdf")
contents = [pdf_file, prompt]

response = model.generate_content(contents)
print(response.text)