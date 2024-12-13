import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers

load_dotenv()

ELASTIC_USERNAME = os.getenv("ELASTIC_USERNAME")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD")
ELASTIC_API_KEY = os.getenv("ELASTIC_API_KEY")
ELASTIC_CLOUD_ID = os.getenv("ELASTIC_CLOUD_ID")
elasticsearch = Elasticsearch(
    cloud_id=ELASTIC_CLOUD_ID,
    basic_auth=(os.getenv("ELASTIC_USERNAME"), os.getenv("ELASTIC_PASSWORD"))
)

def use_elasticsearch_searching(field:str, question:str, question_vector:list, elasticsearch:Elasticsearch, index:str) -> list:
    """
    Perform a combined k-Nearest Neighbors (kNN) and keyword search query in Elasticsearch to retrieve relevant documents.

    Args:
        field (str): The field in the Elasticsearch index to perform the kNN search on.
        question (str): The textual query to search for using keyword-based matching.
        question_vector (list): The vector representation of the query for the kNN search.
        elasticsearch (Elasticsearch): An instance of the Elasticsearch client.
        index (str): The name of the Elasticsearch index to search in.

    Returns:
        list(dict): A list of documents that match the query, with only the specified fields (e.g., "text") included in the results.
    """
    knn_query = {
        "field" : field,
        "query_vector" : question_vector,
        "k" : 5,
        "num_candidates" : 100,
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
