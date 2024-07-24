import logging
import json
from elasticsearch import exceptions as es_exceptions
from .client import es_sync
from typing import Union, Dict

def test_es_connection() -> bool:
    """
    Test connection to Elasticsearch and log the information.

    Returns:
        bool: True if the connection is successful, False otherwise.
    """
    try:
        if es_sync.ping():
            logging.info("Successfully connected to Elasticsearch.")
            return True
        else:
            logging.warning("Failed to connect to Elasticsearch.")
            return False
    except Exception as e:
        logging.error(f"An error occurred while connecting to Elasticsearch: {e}")
        return False


def create_index(index_name: str) -> int:
    """
    Create Elasticsearch index if it does not exist.

    Args:
        index_name (str): The name of the index to be created.

    Returns:
        int: Returns 1 if the index is created successfully,
             0 if the index already exists,
             -1 if an error occurs during the creation process.
    """
    try:
        if not es_sync.indices.exists(index=index_name):
            with open("json/settings.json", "r") as file:
                index_config = json.load(file)
            es_sync.indices.create(index=index_name, ignore=400, body=index_config)
            logging.info(f'Created index {index_name}')
            return 1
        else:
            logging.info(f'Index {index_name} already exists')
            return 0
    except es_exceptions.ConnectionError as err:
        logging.error(f'Error connecting to Elasticsearch: {err}')
        return -1
    except Exception as err:
        logging.error(f'An error occurred: {err}')
        return -1


def add_doc_to_index(index_name: str, doc: Dict[str, Union[str, int]]) -> bool:
    """
    Add document to Elasticsearch index.

    Args:
        index_name (str): The name of the Elasticsearch index.
        doc (Dict[str, Union[str, int]]): The document to be added to the index.

    Returns:
        bool: True if the document was successfully added or updated in the index, False otherwise.
    """
    try:
        doc_id = doc.get('id', None)
        if not doc_id:
            logging.warning(f"Document does not contain an 'id' field. Document: {doc}")
            return False
        
        response = es_sync.index(index=index_name, id=doc_id, document=doc)
        if response.get('result') in ['created', 'updated']:
            return True
        return False
    except es_exceptions.RequestError as e:
        logging.error(f"Failed to index document. Error: {e}")
        return False


def doc_exist_in_es(index_name: str, doc_id: str) -> bool:
    """Check if a document exists in Elasticsearch index.

    Args:
        index_name (str): The name of the Elasticsearch index.
        doc_id (str): The ID of the document to check.

    Returns:
        bool: True if the document exists, False otherwise.
    """
    try:
        response = es_sync.get(index=index_name, id=doc_id, ignore=404)
        return response and response.get('found', False)
    except Exception as e:
        logging.error(f"Failed to check existence of document with id {doc_id}. Error: {e}")
        return False


def get_last_doc_id(index_name: str) -> Union[str, int]:
    """
    Get the last document ID from Elasticsearch.

    Args:
        index_name (str): The name of the Elasticsearch index.

    Returns:
        Union[str, int]: The last document ID as a string or integer.

    Raises:
        Exception: If there is an error while retrieving the last document ID.

    """
    query = {
        "size": 1,
        "sort": [{"id": {"order": "desc"}}]
    }
    try:
        response = es_sync.search(index=index_name, body=query)
        if response['hits']['hits']:
            return response['hits']['hits'][0]['_id']
    except Exception as e:
        logging.error(f"Failed to get last document ID: {e}. Possibly elasticsearch db is empty.")
        return 0