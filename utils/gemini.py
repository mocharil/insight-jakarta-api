from google.oauth2 import service_account
import vertexai
from google.cloud import documentai_v1 as documentai
from vertexai.generative_models import (
    GenerationConfig,
    GenerativeModel,
    HarmCategory,
    HarmBlockThreshold,
    Image
)
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

class GeminiConnector:
    def __init__(self):
        """
        Initialize the GeminiConnector with credentials and configurations.

        Parameters:
        - credentials_file_path (str): Path to the service account credentials file.
        - project_id (str): Google Cloud Project ID.
        - model (str): Name of the Gemini model.
        """
        
        self.credentials = service_account.Credentials.from_service_account_file(os.getenv("CREDENTIAL_GEMINI_FILE_PATH"))
        self.project_id =  os.getenv("PROJECT_ID_GEMINI")
        self.model = os.getenv("GEMINI_MODEL")

        # Initialize Vertex AI
        vertexai.init(project=self.project_id, credentials=self.credentials)

        # Initialize Models
        self.multimodal_model = GenerativeModel(self.model)

    def generate_content(self, prompt: str) -> str:
        """
        Generate content using the multimodal model.

        Parameters:
        - prompt (str): Text prompt for content generation.

        Returns:
        str: Generated content.
        """
        try:
            # Configure safety and generation settings
            safety_config = self._safety_config()
            config = self._generation_config()

            # Generate content
            responses = self.multimodal_model.generate_content(
                [prompt],
                safety_settings=safety_config,
                generation_config=config,
                stream=True
            )

            # Collect the full result
            full_result = ""
            for response in responses:
                full_result += response.text

            return full_result.strip()
        except Exception as e:
            raise Exception(f"Error generating content: {e}")

    def _safety_config(self):
        """
        Configure safety settings for content generation.

        Returns:
        dict: Safety configuration for harm categories.
        """
        return {
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
        }

    def _generation_config(self):
        """
        Configure generation settings.

        Returns:
        GenerationConfig: Configuration object for content generation.
        """
        return GenerationConfig(temperature=0.0, top_p=1, top_k=32)