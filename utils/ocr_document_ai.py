import os, json
from google.cloud import documentai_v1 as documentai
from google.oauth2 import service_account
from mimetypes import guess_type
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class OCRProcessor:
    def __init__(self):
        """
        Initializes the OCRProcessor with the required environment variables.
        """
        self.credentials_file_path = os.getenv('CREDENTIAL_DOCAI_FILE_PATH')
        self.project_id = os.getenv('PROJECT_ID') 
        self.location = os.getenv('LOCATION')
        self.processor_id = os.getenv('PROCESSOR_ID') 


        # Ensure required environment variables are set
        if not all([self.credentials_file_path, self.project_id, self.location, self.processor_id]):
            raise EnvironmentError("Missing one or more required environment variables: CREDENTIALS_FILE_PATH, PROJECT_ID, LOCATION, PROCESSOR_ID")

        # Load credentials
        with open(self.credentials_file_path, "r") as creds:
            service_account_info = json.load(creds)
        self.credentials = service_account.Credentials.from_service_account_info(service_account_info)

    def process_file(self, filename: str) -> str:
        """
        Processes a document using Google Document AI OCR with the provided filename.

        Parameters:
        - filename (str): Path to the file to be processed.

        Returns:
        str: Extracted text from the processed document.
        """
        # Read the file content
        with open(filename, 'rb') as file:
            file_content = file.read()

        # Get the MIME type of the file
        mime_type, _ = guess_type(filename)

        # Define the API endpoint based on the location
        api_endpoint = f"{self.location}-documentai.googleapis.com"
        client_options = {"api_endpoint": api_endpoint}

        # Create a Document AI client
        documentai_client = documentai.DocumentProcessorServiceClient(client_options=client_options, credentials=self.credentials)

        # Construct the processor resource name
        resource_name = documentai_client.processor_path(self.project_id, self.location, self.processor_id)

        # Create a raw document object
        raw_document = documentai.RawDocument(content=file_content, mime_type=mime_type)

        # Configure the process request
        request = documentai.ProcessRequest(name=resource_name, raw_document=raw_document)

        # Process the document
        result = documentai_client.process_document(request=request).document
        
        # Return the text content of the document
        return result.text

# Example usage
if __name__ == "__main__":
    ocr_processor = OCRProcessor()
    filename = os.getenv('FILENAME')
    ocr_text = ocr_processor.process_file(filename)
    print(ocr_text)