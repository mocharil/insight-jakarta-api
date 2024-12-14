import os
import vertexai
from dotenv import load_dotenv
from vertexai.language_models import TextEmbeddingModel

load_dotenv()
GCLOUD_SECRETS = os.getenv("CREDENTIAL_DOCAI_FILE_PATH")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.expanduser(GCLOUD_SECRETS)

REGION = os.getenv("REGION")
MODEL_ID = os.getenv("MODEL_ID")
PROJECT_ID = os.getenv("PROJECT_ID")

vertexai.init(project=PROJECT_ID, location=REGION)
model = TextEmbeddingModel.from_pretrained(MODEL_ID)

def use_embedding_from_vertex_ai(text:str) -> list:
    """
    Generate embeddings for a given text using a model from Vertex AI.

    Args:
        text (str): The input text for which embeddings are to be generated.

    Returns:
        list: A list of values representing the first embedding vector for the input text.
    """
    embeddings = model.get_embeddings([text])
    return embeddings[0].values