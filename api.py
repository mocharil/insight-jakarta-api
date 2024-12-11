from fastapi import FastAPI, HTTPException, Body
from utils.ocr_document_ai import OCRProcessor
from utils.gcs import upload_to_gcs, download_from_gcs
from utils.gemini import GeminiConnector
from google.oauth2 import service_account
import os

# Initialize FastAPI app
app = FastAPI()

# Initialize OCRProcessor
ocr_processor = OCRProcessor()

gemini_connector = GeminiConnector()

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
