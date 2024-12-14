import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from .embeddings import use_embedding_from_vertex_ai

load_dotenv()

elasticsearch = Elasticsearch(
    cloud_id=os.getenv("ES_CLOUD_ID"),
    http_auth=(os.getenv("ES_USERNAME"), os.getenv("ES_PASSWORD"))
)


def embedding_open_ai(text):
    import requests
    request = requests.post(
        url="http://52.230.101.0:1013/embeddings",
        json={
            "text":text,
            "token":"givemeazurecredsboys!"
        }
    )

    result = request.json()
    return result['detail']['vector']





def use_elasticsearch_searching(question: str, index: str, field:str = "text_vector") -> list:
    """
    Perform a combined k-Nearest Neighbors (kNN) and keyword search query in Elasticsearch to retrieve relevant documents.

    Args:
        question (str): The textual query to search for using keyword-based matching.
        index (str): The name of the Elasticsearch index to search in.

    Returns:
        list(dict): A list of documents that match the query, with only the specified fields (e.g., "text") included in the results.
    """
    
    # question_vector = use_embedding_from_vertex_ai(question)
    question_vector = embedding_open_ai(question)

    print(len(question_vector))


    knn_query = {
        "field": field,
        "query_vector": question_vector,
        "k": 5,
        "num_candidates": 100,
        "boost": 0.5
    }

    question_query = {
        "bool": {
            "must": {
                "multi_match": {
                    "query": question,
                    "fields": ["text"],
                    "type": "best_fields",
                    "boost": 0.5,
                }
            }
        }
    }

    search_query = {
        "knn": knn_query,
        "query": question_query,
        "size": 10,
        "_source": ["text"]
    }

    elasticsearch_search = elasticsearch.search(
        index=index,
        body=search_query
    )

    documents = []
    for hits in elasticsearch_search["hits"]["hits"]:
        documents.append(hits["_source"])
    return documents