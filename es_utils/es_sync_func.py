import json
import logging
from typing import Dict, Optional, Tuple, Union

from elasticsearch import exceptions as es_exceptions


def test_es_connection() -> bool:
    from .client import es_sync

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


def create_index(index_name: str, settings_path: str = "json/settings.json") -> int:
    from .client import es_sync

    """
    Create Elasticsearch index if it does not exist.

    Args:
        index_name (str): The name of the index to be created.
        settings_path (str, optional): Path to the JSON file containing index settings.
                                       Defaults to "json/settings.json".

    Returns:
        int: Returns 1 if the index is created successfully,
             0 if the index already exists,
             -1 if an error occurs during the creation process.
    """
    try:
        if not es_sync.indices.exists(index=index_name):
            try:
                with open(settings_path, "r") as file:
                    index_config = json.load(file)
            except FileNotFoundError as fnf_error:
                logging.error(f"Settings file not found: {fnf_error}")
                return -1

            # Use ignore parameter directly
            es_sync.indices.create(index=index_name, body=index_config, ignore=400)
            logging.info(f"Created index {index_name}")
            return 1
        else:
            logging.info(f"Index {index_name} already exists")
            return 0
    except es_exceptions.ConnectionError as err:
        logging.warning(f"Connection error while creating index, client will retry: {err}")
        return -1
    except Exception as err:
        logging.error(f"An error occurred: {err}")
        return -1


def add_doc_to_index(index_name: str, doc: Dict[str, Union[str, int]]) -> bool:
    from .client import es_sync

    """
    Add document to Elasticsearch index.

    Args:
        index_name (str): The name of the Elasticsearch index.
        doc (Dict[str, Union[str, int]]): The document to be added to the index.

    Returns:
        bool: True if the document was successfully added or updated in the index, False otherwise.
    """
    try:
        doc_id = doc.get("id", None)
        if not doc_id:
            logging.warning(f"Document does not contain an 'id' field. Document: {doc}")
            return False

        # Convert doc_id to string to fix type error
        doc_id_str = str(doc_id)
        response = es_sync.index(index=index_name, id=doc_id_str, document=doc)
        if response.get("result") in ["created", "updated"]:
            return True
        return False
    except es_exceptions.RequestError as e:
        logging.error(f"Failed to index document {doc_id}. Error: {e}")
        return False
    except es_exceptions.ConnectionError as e:
        # This is a connection error that the client will retry
        logging.warning(f"Connection error while indexing document {doc_id}, client will retry: {e}")
        raise
    except es_exceptions.TransportError as e:
        # This is a transport error that the client will retry
        logging.warning(f"Transport error while indexing document {doc_id}, client will retry: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error while indexing document {doc_id}: {e}")
        return False


def doc_exist_in_es(index_name: str, doc_id: str) -> bool:
    from .client import es_sync

    """Check if a document exists in Elasticsearch index.

    Args:
        index_name (str): The name of the Elasticsearch index.
        doc_id (str): The ID of the document to check.

    Returns:
        bool: True if the document exists, False otherwise.
    """
    try:
        # Use ignore parameter directly
        response = es_sync.get(index=index_name, id=doc_id, ignore=404)
        if response and response.get("found", False):
            logging.info(f"Document {doc_id} already exists in index {index_name}")
            return True
        else:
            logging.info(f"Document {doc_id} does not exist in index {index_name}")
            return False
    except es_exceptions.NotFoundError:
        logging.info(f"Document {doc_id} does not exist in index {index_name}")
        return False
    except es_exceptions.ConnectionError as e:
        # This is a connection error that the client will retry
        logging.warning(f"Connection error while checking document {doc_id}, client will retry: {e}")
        raise
    except es_exceptions.TransportError as e:
        # This is a transport error that the client will retry
        logging.warning(f"Transport error while checking document {doc_id}, client will retry: {e}")
        raise
    except Exception as e:
        logging.error(
            f"Failed to check existence of document with id {doc_id}. Error: {e}"
        )
        return False


def count_doc_es(
    index_name: str, field_name: str, field_value: str
) -> Optional[int]:
    from .client import es_sync

    """
    Count the number of documents in an Elasticsearch index where the given field matches a specific value.

    Args:
        index_name (str): The name of the Elasticsearch index.
        field_name (str): The field to query.
        field_value (str): The value to match for the field.

    Returns:
        Optional[int]: The count of documents if successful, None otherwise.
    """
    query = {"query": {"term": {field_name: field_value}}}

    try:
        response = es_sync.count(index=index_name, body=query)
        if response and "count" in response:
            logging.info(
                f"Found {response['count']} documents in index {index_name} where {field_name} = {field_value}."
            )
            return response["count"]
        logging.info(
            f"No documents found in index {index_name} where {field_name} = {field_value}."
        )
        return 0
    except es_exceptions.ConnectionError as e:
        # This is a connection error that the client will retry
        logging.warning(f"Connection error while counting documents, client will retry: {e}")
        raise
    except es_exceptions.TransportError as e:
        # This is a transport error that the client will retry
        logging.warning(f"Transport error while counting documents, client will retry: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to count documents in index {index_name}. Error: {e}")
        return None


def get_last_doc_id(index_name: str) -> Union[str, int]:
    from .client import es_sync

    """
    Get the last document ID from Elasticsearch.

    Args:
        index_name (str): The name of the Elasticsearch index.

    Returns:
        Union[str, int]: The last document ID as a string or integer.
    """
    query = {"size": 1, "sort": [{"id": {"order": "desc"}}]}
    try:
        response = es_sync.search(index=index_name, body=query)
        if response["hits"]["hits"]:
            return response["hits"]["hits"][0]["_id"]
        return 0  # Return 0 if no documents found
    except es_exceptions.ConnectionError as e:
        # This is a connection error that the client will retry
        logging.warning(f"Connection error while getting last document ID, client will retry: {e}")
        raise
    except es_exceptions.TransportError as e:
        # This is a transport error that the client will retry
        logging.warning(f"Transport error while getting last document ID, client will retry: {e}")
        raise
    except Exception as e:
        logging.error(
            f"Failed to get last document ID: {e}. Possibly elasticsearch db is empty."
        )
        return 0


def get_latest_value(index_name: str, field_name: str) -> Optional[str]:
    from .client import es_sync

    """
    Get the latest value for a specified field from Elasticsearch.

    Args:
        index_name (str): The name of the Elasticsearch index.
        field_name (str): The field name to retrieve the latest value for.

    Returns:
        Optional[str]: The latest value for the specified field, or None if not found.
    """
    query = {
        "size": 1,
        "sort": [{field_name: {"order": "desc"}}],
        "_source": [field_name],
    }
    try:
        response = es_sync.search(index=index_name, body=query)
        if response["hits"]["hits"]:
            return response["hits"]["hits"][0]["_source"][field_name]
        return None  # Return None if no documents found
    except es_exceptions.ConnectionError as e:
        # This is a connection error that the client will retry
        logging.warning(f"Connection error while getting latest value, client will retry: {e}")
        raise
    except es_exceptions.TransportError as e:
        # This is a transport error that the client will retry
        logging.warning(f"Transport error while getting latest value, client will retry: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to get the latest '{field_name}': {e}")
        return None


def sync_document(index_name: str, doc_source: Dict[str, Union[str, int]]) -> bool:
    from .client import es_sync

    """
    Sync document with Elasticsearch index. If the document exists and is different, update it.

    Args:
        index_name (str): The name of the Elasticsearch index.
        doc_source (Dict[str, Union[str, int]]): The source document to be synced.

    Returns:
        bool: True if the document was successfully synced, False otherwise.
    """
    try:
        doc_id = doc_source.get("id", None)
        if not doc_id:
            logging.warning(
                f"Document does not contain an 'id' field. Document: {doc_source}"
            )
            return False

        # Convert doc_id to string to fix type error
        doc_id_str = str(doc_id)
        
        # Check if the document exists
        try:
            existing_doc = es_sync.get(index=index_name, id=doc_id_str)
            doc_exists = True
        except es_exceptions.NotFoundError:
            doc_exists = False
        except es_exceptions.ConnectionError as e:
            # This is a connection error that the client will retry
            logging.warning(f"Connection error while checking document {doc_id}, client will retry: {e}")
            raise
        except es_exceptions.TransportError as e:
            # This is a transport error that the client will retry
            logging.warning(f"Transport error while checking document {doc_id}, client will retry: {e}")
            raise

        if doc_exists:
            doc_es = existing_doc["_source"]
            if doc_source != doc_es:
                logging.info(
                    f"Document with id {doc_id}. New version found. Updating document."
                )
                update_response = es_sync.index(
                    index=index_name, id=doc_id_str, document=doc_source
                )
                if update_response["result"] in ["created", "updated"]:
                    logging.info(f"Document with id {doc_id} has been updated.")
                    return True
                logging.error(
                    f"Failed to update document with id {doc_id}. Response: {update_response}"
                )
                return False
            else:
                logging.info(f"Document with id {doc_id} is already up-to-date.")
                return True
        else:
            # Document doesn't exist, create it
            create_response = es_sync.index(
                index=index_name, id=doc_id_str, document=doc_source
            )
            if create_response["result"] == "created":
                logging.info(f"Document with id {doc_id} has been created.")
                return True
            logging.error(
                f"Failed to create document with id {doc_id}. Response: {create_response}"
            )
            return False
    except es_exceptions.RequestError as e:
        logging.error(f"Document with id {doc_id} failed to sync. Error: {e}")
        return False
    except es_exceptions.ConnectionError as e:
        # This is a connection error that the client will retry
        logging.warning(f"Connection error while syncing document {doc_id}, client will retry: {e}")
        raise
    except es_exceptions.TransportError as e:
        # This is a transport error that the client will retry
        logging.warning(f"Transport error while syncing document {doc_id}, client will retry: {e}")
        raise
    except Exception as e:
        logging.error(
            f"An unexpected error occurred while syncing document with id {doc_id}. Error: {e}"
        )
        return False

def get_latest_es_doc_info(index_name: str) -> Tuple[int, Optional[str]]:
    """
    Get the largest document ID and latest publication date from Elasticsearch.
    These may come from different documents.
    
    Args:
        index_name (str): The name of the Elasticsearch index
        
    Returns:
        Tuple[int, Optional[str]]: Tuple containing (largest_id, latest_dt_wyd)
    """
    from .client import es_sync
    
    try:
        # Get document with highest ID
        id_query = {
            "size": 1,
            "sort": [{"id": {"order": "desc"}}],
            "_source": False
        }
        id_response = es_sync.search(index=index_name, body=id_query)
        
        largest_id = 0
        if id_response["hits"]["hits"]:
            largest_id = int(id_response["hits"]["hits"][0]["_id"])
            
        # Get document with latest publication date
        date_query = {
            "size": 1,
            "sort": [{"dokument.DT_WYD": {"order": "desc"}}],
            "_source": ["dokument.DT_WYD"]
        }
        date_response = es_sync.search(index=index_name, body=date_query)
        
        latest_dt_wyd = None
        if date_response["hits"]["hits"]:
            source = date_response["hits"]["hits"][0]["_source"]
            if "dokument" in source and "DT_WYD" in source["dokument"]:
                latest_dt_wyd = source["dokument"]["DT_WYD"]
        
        return largest_id, latest_dt_wyd
        
    except es_exceptions.ConnectionError as e:
        # This is a connection error that the client will retry
        logging.warning(f"Connection error while getting latest document info, client will retry: {e}")
        raise
    except es_exceptions.TransportError as e:
        # This is a transport error that the client will retry
        logging.warning(f"Transport error while getting latest document info, client will retry: {e}")
        raise
    except Exception as e:
        logging.error(f"Error getting latest document info from Elasticsearch: {e}")
        return 0, None
