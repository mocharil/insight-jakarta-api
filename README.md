# API for Jakarta Insight

This API serves as the backend for the Jakarta Insight project, providing various endpoints for document processing, content generation, and data analysis.

## Project Structure

```
api/
├── .env
├── api.py
├── creds_docai.json
├── creds_gcs.json
├── Dockerfile
├── gemini-flash.json
├── requirements.txt
├── test_docai_import.py
└── utils/
```

## Description

This API is built using FastAPI and integrates various Google Cloud services, including Document AI, Cloud Storage, and Vertex AI. It also includes functionality for vector search using Elasticsearch.

## Key Components

- `api.py`: Main FastAPI application file containing all API endpoints.
- `.env`: Environment variables file for storing configuration settings.
- `Dockerfile`: Used for containerizing the application.
- `requirements.txt`: Lists all Python dependencies.
- `utils/`: Directory containing utility modules for various functionalities.

## Setup and Installation

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Set up environment variables in `.env` file.

3. Ensure you have the necessary Google Cloud credentials:
   - `creds_docai.json`: For Document AI
   - `creds_gcs.json`: For Google Cloud Storage
   - `gemini-flash.json`: For Gemini AI model

## Running the API

To run the API locally:

```
uvicorn api:app --host 0.0.0.0 --port 8080
```

## Docker Deployment

To build and run the Docker container:

```
docker build -t jakarta-insight-api .
docker run -p 8080:8080 jakarta-insight-api
```

## API Endpoints

The API includes endpoints for:

- OCR processing
- File upload and download to/from Google Cloud Storage
- Content generation using Gemini AI
- Knowledge base management
- Vector search
- Question answering

For detailed API documentation, run the server and visit `/docs` endpoint.

## Environment Variables

Ensure the following environment variables are set in the `.env` file:

- `CREDENTIAL_DOCAI_FILE_PATH`
- `CREDENTIAL_GEMINI_FILE_PATH`
- `CREDENTIAL_GCS_FILE_PATH`
- `PROJECT_ID`
- `PROJECT_ID_GEMINI`
- `LOCATION`
- `PROCESSOR_ID`
- `BUCKET_NAME`
- `GEMINI_MODEL`
- `ES_CLOUD_ID`
- `ES_USERNAME`
- `ES_PASSWORD`
- `REGION`
- `MODEL_ID`

## Testing

To test the Document AI import:

```
python test_docai_import.py
```

This project is part of the Jakarta Insight system, designed to provide advanced analytics and insights for the city of Jakarta.