import logging
import json
from elasticsearch import exceptions as es_exceptions
from typing import Union, Dict


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


def create_index(index_name: str) -> int:
    from .client import es_sync

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
            try:
                with open("json/settings.json", "r") as file:
                    index_config = json.load(file)
            except FileNotFoundError as fnf_error:
                logging.error(f"Settings file not found: {fnf_error}")
                return -1

            es_sync.indices.create(index=index_name, ignore=400, body=index_config)
            logging.info(f"Created index {index_name}")
            return 1
        else:
            logging.info(f"Index {index_name} already exists")
            return 0
    except es_exceptions.ConnectionError as err:
        logging.error(f"Error connecting to Elasticsearch: {err}")
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

        response = es_sync.index(index=index_name, id=doc_id, document=doc)
        if response.get("result") in ["created", "updated"]:
            return True
        return False
    except es_exceptions.RequestError as e:
        logging.error(f"Failed to index document {doc_id}. Error: {e}")
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
        response = es_sync.get(index=index_name, id=doc_id, ignore=404)
        if response and response.get("found", False):
            logging.info(f"Document {doc_id} already exists in index {index_name}")
            return True
        else:
            logging.info(f"Document {doc_id} does not exist in index {index_name}")
            return False
    except Exception as e:
        logging.error(
            f"Failed to check existence of document with id {doc_id}. Error: {e}"
        )
        return False


def count_doc_es(
    index_name: str, field_name: str, field_value: str
) -> Union[int, None]:
    from .client import es_sync

    """
    Count the number of documents in an Elasticsearch index where the given field matches a specific value.

    Args:
        index_name (str): The name of the Elasticsearch index.
        field_name (str): The field to query.
        field_value (str): The value to match for the field.

    Returns:
        Union[int, None]: The count of documents if successful, None otherwise.
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

    Raises:
        Exception: If there is an error while retrieving the last document ID.

    """
    query = {"size": 1, "sort": [{"id": {"order": "desc"}}]}
    try:
        response = es_sync.search(index=index_name, body=query)
        if response["hits"]["hits"]:
            return response["hits"]["hits"][0]["_id"]
    except Exception as e:
        logging.error(
            f"Failed to get last document ID: {e}. Possibly elasticsearch db is empty."
        )
        return 0


def get_latest_value(index_name: str, field_name: str) -> str:
    from .client import es_sync

    """
    Get the latest value for a specified field from Elasticsearch.

    Args:
        index_name (str): The name of the Elasticsearch index.
        field_name (str): The field name to retrieve the latest value for.

    Returns:
        str: The latest value for the specified field.

    Raises:
        Exception: If there is an error while retrieving the data.
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

        # Check if the document exists
        try:
            existing_doc = es_sync.get(index=index_name, id=doc_id)
            doc_exists = True
        except es_exceptions.NotFoundError:
            doc_exists = False

        if doc_exists:
            doc_es = existing_doc["_source"]
            if doc_source != doc_es:
                logging.info(
                    f"Document with id {doc_id}. New version found. Updating document."
                )
                update_response = es_sync.index(
                    index=index_name, id=doc_id, document=doc_source
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
                index=index_name, id=doc_id, document=doc_source
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
    except Exception as e:
        logging.error(
            f"An unexpected error occurred while syncing document with id {doc_id}. Error: {e}"
        )
        return False
