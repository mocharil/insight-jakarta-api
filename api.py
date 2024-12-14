from fastapi import FastAPI, HTTPException, Body, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from utils.ocr_document_ai import OCRProcessor
from utils.gcs import upload_to_gcs, download_from_gcs
from utils.gemini import GeminiConnector
from utils.embeddings import use_embedding_from_vertex_ai
from utils.vector_search import use_elasticsearch_searching
import os, re
from pydantic import BaseModel
from typing import List
from elasticsearch import Elasticsearch
from datetime import datetime
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

es = Elasticsearch(
    cloud_id=os.getenv("ES_CLOUD_ID"),
    http_auth=(os.getenv("ES_USERNAME"), os.getenv("ES_PASSWORD"))
)

# Initialize FastAPI app
app = FastAPI()

# Tambahkan middleware CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Ganti "*" dengan domain frontend jika ingin lebih aman
    allow_credentials=True,
    allow_methods=["*"],  # Mengizinkan semua method, termasuk OPTIONS
    allow_headers=["*"],
)

# Initialize OCRProcessor
ocr_processor = OCRProcessor()

gemini_connector = GeminiConnector()

# Add this class to your existing models
class KnowledgeBaseEntry(BaseModel):
    title: str
    content: str
    topic_classification: str
    keywords: list[str]

class EmbedRequest(BaseModel):
    text: str

class VectorSearchRequest(BaseModel):
    question: str
    index: str

class QuestionRequest(BaseModel):
    question: str

@app.post("/process-ocr/")
def process_ocr(filename: str = Body(..., embed=True)):
    """
    API endpoint to process a document using Google Document AI OCR.

    Parameters:
    - filename (str): Path to the file to be processed.

    Returns:
    dict: Extracted text from the processed document.
    """
    try:
        # Check if file exists
        if not os.path.exists(filename):
            raise HTTPException(status_code=400, detail=f"File not found: {filename}")

        # Process the file using OCRProcessor
        ocr_text = ocr_processor.process_file(filename)

        # Return the extracted text
        return {"filename": filename, "extracted_text": ocr_text}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload-to-gcs/")
def upload_to_gcs_api(source_file: str = Body(...), destination_blob_name: str = Body(...)):
    """
    Uploads a file to Google Cloud Storage.

    Parameters:
    - source_file (str): Local file path to be uploaded.
    - destination_blob_name (str): Destination path in the GCS bucket.

    Returns:
    dict: Confirmation message with uploaded file path.
    """
    try:
        # Use the GCS utility function
        message = upload_to_gcs(source_file, destination_blob_name)
        return {"message": message}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/download-from-gcs/")
def download_from_gcs_api(blob_name: str = Body(...), destination_file: str = Body(...)):
    """
    Downloads a file from Google Cloud Storage.

    Parameters:
    - blob_name (str): Path of the file in the GCS bucket.
    - destination_file (str): Local path to save the downloaded file.

    Returns:
    dict: Confirmation message with the downloaded file path.
    """
    try:
        # Use the GCS utility function
        downloaded_file = download_from_gcs(blob_name, destination_file)
        return {"message": f"File downloaded successfully to {downloaded_file}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
@app.post("/generate-content/")
def generate_content(prompt: str = Body(..., embed=True)):
    """
    API endpoint to generate content using the Gemini model.

    Parameters:
    - prompt (str): Text prompt for content generation.

    Returns:
    dict: Generated content.
    """
    try:
        result = gemini_connector.generate_content(prompt)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating content: {e}")
    
def summary_cluster(search_results: str) -> dict:
    prompt = f"""You are a News Analysis Expert. 
    Analyze and summarize the following list of news articles and extract the information in JSON format as shown below:
    {{
            "main_issue": "<summary of the main issues related to this topic>",
            "problem": "<explanation of the underlying reasons for these issues>",
            "suggestion": "<recommended actions to address these issues>",
            "urgency_score": "<a score from 0 to 100 indicating the urgency level, representing the negative impact this issue may have on the Jakarta government>"
    }}
    Here is the information:
    {search_results}
    Hard Rules:
    Answer in English Language and in simple way
    """
    
    response = gemini_connector.generate_content(prompt)
    try:
        extracted_data = eval(re.findall(r'\{.*?\}', response, flags=re.I | re.S)[0])
    except (IndexError, SyntaxError) as e:
        raise HTTPException(status_code=500, detail="Failed to parse response from Gemini model")
    return extracted_data

class SearchResultsRequest(BaseModel):
    search_results: List

@app.post("/summarize/")
def summarize_news(request: SearchResultsRequest):
    try:
        summary = summary_cluster(request.search_results)
        return {"summary": summary}
    except Exception as e:
        print('>>>>>>>>>>>',e,'<<<<<<<<<<')
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/knowledge-base")
async def add_knowledge_base_entry(
    title: str = Form(...),
    content: str = Form(...),
    topic_classification: str = Form(...),
    keywords: str = Form(...),
    file: UploadFile = File(None)
):
    # Initialize Elasticsearch client

    """
    Add entry to knowledge base. Can handle both text data and file uploads.
    
    Parameters:
    - title: Title of the knowledge base entry
    - content: Main content text
    - topic_classification: Category/topic of the content
    - keywords: Comma-separated keywords
    - file: Optional file attachment
    """
    try:
        # Process file upload if provided
        file_url = None
        file_content = None
        if file and file.filename:
            # Save uploaded file temporarily
            temp_file_path = f"temp_{file.filename}"
            with open(temp_file_path, "wb") as temp_file:
                temp_file.write(await file.read())
            
            # Upload to GCS
            gcs_path = f"knowledge_base/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}"
            upload_to_gcs(temp_file_path, gcs_path)
            file_url = f"gs://knowledge_base/{gcs_path}"
            
            #get file content
            file_content = ocr_processor.process_file(temp_file_path)
    
            # Clean up temp file
            os.remove(temp_file_path)
    
        content = content + (f"\n{file_content}" if file_content else '')
    
        text_embedding = use_embedding_from_vertex_ai(content)
    
        # Prepare document for Elasticsearch
        doc = {
            "title": title,
            "text": content,
            "topic_classification": topic_classification,
            "keywords": [k.strip() for k in keywords.split(",")],
            "timestamp": datetime.utcnow().isoformat(),
            "file_url": file_url,
            "text_vector": text_embedding
        }
    
        # Index document in Elasticsearch
        result = es.index(
            index="knowledge-base",
            document=doc,
            refresh=True  # Ensure document is immediately searchable
        )
    
        print({
            "message": "Knowledge base entry added successfully",
            "id": result["_id"],
            "file_url": file_url
        })
        return {
            "message": "Knowledge base entry added successfully",
            "id": result["_id"],
            "file_url": file_url
        }

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Error adding knowledge base entry: {str(e)}")

@app.post("/embedding")
async def embed_text(request: EmbedRequest):
    """
    API endpoint to generate embeddings for given text using Vertex AI.

    Parameters:
    - text (str): The text to be embedded.

    Returns:
    list: The embedding vector for the input text.
    """
    try:
        embedding = use_embedding_from_vertex_ai(request.text)
        return embedding
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating embedding: {str(e)}")

@app.post("/vector-search")
async def vector_search(request: VectorSearchRequest):
    """
    API endpoint to perform vector search using Elasticsearch.

    Parameters:
    - question (str): The question to search for.
    - index (str): The Elasticsearch index to search in.

    Returns:
    list: The search results.
    """

    try:
        results = use_elasticsearch_searching(request.question, request.index)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error performing vector search: {str(e)}")

@app.post("/answer-question")
async def answer_question(request: QuestionRequest):
    """
    API endpoint to answer a question using Elasticsearch search and Gemini content generation.

    Parameters:
    - question (str): The question to be answered.

    Returns:
    dict: The generated answer.

    """
    try:
        # Step 1: Use Elasticsearch to search for relevant information
        search_results = use_elasticsearch_searching(request.question, "knowledge-puu")
        
        print(search_results)
        # Step 2: Prepare the context for Gemini
        context = "\n-----".join([i['text'] for i in search_results])
        
        # Step 3: Generate the answer using Gemini
        prompt = f"""Based on the following context, please answer the question: "{request.question}"

            Context:
            {context}

            Hard Rule: Use Language that user use to answer"""
        
        answer = gemini_connector.generate_content(prompt)
        
        return {"question": request.question, "answer": answer}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error answering question: {str(e)}")

# You might want to add this initialization code at startup
@app.on_event("startup")
async def startup_event():
    # Create knowledge-base index if it doesn't exist
    if not es.indices.exists(index="knowledge-base"):
        es.indices.create(
            index="knowledge-base",
            body={
                "mappings": {
                    "properties": {
                        "title": {"type": "text"},
                        "content": {"type": "text"},
                        "topic_classification": {"type": "keyword"},
                        "keywords": {"type": "keyword"},
                        "timestamp": {"type": "date"},
                        "file_url": {"type": "keyword"}
                    }
                }
            }
        )
