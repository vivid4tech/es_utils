import logging
from elasticsearch import exceptions as es_exceptions
from typing import Union, Dict, Any, Optional


async def add_document_to_index(index_name: str, doc: dict) -> bool:
    from .client import es_async

    """
    Adds a document to the specified Elasticsearch index.

    Args:
        index_name (str): The name of the Elasticsearch index.
        doc (dict): The document to be added.

    Returns:
        bool: True if the document was successfully added, False otherwise.
    """
    try:
        doc_id = doc.get("id", None)
        if not doc_id:
            logging.warning(f"Document does not contain an 'id' field. Document: {doc}")
            return False

        # Convert doc_id to string to fix type error
        doc_id_str = str(doc_id)
        response = await es_async.index(index=index_name, id=doc_id_str, document=doc)
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
        raise


async def sync_document(
    index_name: str,
    doc_source: Dict[str, Union[str, int]],
    doc_es: Dict[str, Union[str, int]],
) -> bool:
    from .client import es_async

    """
    Sync document with Elasticsearch index. If the document exists and is different, update it.

    Args:
        index_name (str): The name of the Elasticsearch index.
        doc_source (Dict[str, Union[str, int]]): The source document to be synced.
        doc_es (Dict[str, Union[str, int]]): The existing document from Elasticsearch to compare with the source document.

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
        if doc_source != doc_es:
            logging.info(
                f"Document with id {doc_id}. New version found. Updating document."
            )
            # Convert doc_id to string to fix type error
            doc_id_str = str(doc_id)
            # Update the document if they are different
            update_response = await es_async.index(
                index=index_name, id=doc_id_str, document=doc_source
            )
            if update_response.get("result") in ["created", "updated"]:
                logging.info(f"Document with id {doc_id} has been updated.")
                return True
            logging.error(
                f"Failed to update document with id {doc_id}. Response: {update_response}"
            )
            return False
        else:
            logging.info(f"Document with id {doc_id} is already up-to-date.")
            return True
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
        raise


async def document_exists_in_es(index_name: str, doc_id: str) -> Optional[Dict[str, Any]]:
    from .client import es_async

    """Check if a document exists in Elasticsearch index and return the document if it exists.

    Args:
        index_name (str): The name of the Elasticsearch index.
        doc_id (str): The ID of the document to check.

    Returns:
        Optional[Dict[str, Any]]: The document if it exists, None otherwise.
    """
    try:
        # Convert doc_id to string to fix type error
        doc_id_str = str(doc_id)
        # Use ignore parameter directly as it's supported in the client
        response = await es_async.get(index=index_name, id=doc_id_str, ignore=404)
        if response and response.get("found", False):
            logging.info(f"Document with id {doc_id} already exists in Elasticsearch.")
            return response["_source"]
        logging.info(f"Document with id {doc_id} does not exist in Elasticsearch.")
        return None
    except es_exceptions.NotFoundError:
        logging.info(f"Document with id {doc_id} does not exist in Elasticsearch.")
        return None
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
        raise


async def count_doc_es(
    index_name: str, field_name: str, field_value: str
) -> Optional[int]:
    from .client import es_async

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
        response = await es_async.count(index=index_name, body=query)
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
        raise

async def search_all_documents(index, sort_field="id"):
    from .client import es_async
    resp = await es_async.search(index=index, body={
        "query": {"match_all": {}}, 
        "size": 10000,
        "sort": [{f"{sort_field}.keyword": {"order": "asc"}}]
    })
    return resp['hits']['hits']

async def count_doc_es(index, field, value):
    from .client import es_async
    query = {
        "query": {
            "term": {
                field: value
            }
        }
    }
    try:
        resp = await es_async.count(index=index, body=query)
        return resp['count']
    except Exception as e:
        logging.error(f"Error counting documents: {e}")
        return None
Z
async def bulk_documents_exist(index_name: str, docs_to_check: list) -> dict:
    """
    Check if multiple documents exist in Elasticsearch using the mget API.
    
    Args:
        index_name (str): The name of the Elasticsearch index.
        docs_to_check (list): List of document IDs to check.
        
    Returns:
        dict: Dictionary mapping document IDs to boolean existence values.
    """
    from .client import es_async
    
    if not docs_to_check:
        return {}
    
    try:
        # Prepare the mget query
        body = {"docs": [{"_id": doc["_id"]} for doc in docs_to_check]}
        
        # Execute the mget query
        response = await es_async.mget(body=body, index=index_name)
        
        # Process the results
        results = {}
        if "docs" in response:
            for doc in response["docs"]:
                doc_id = doc["_id"]
                results[doc_id] = doc.get("found", False)
        
        return results
    except es_exceptions.ConnectionError as e:
        logging.warning(f"Connection error while checking document batch, client will retry: {e}")
        raise
    except es_exceptions.TransportError as e:
        logging.warning(f"Transport error while checking document batch, client will retry: {e}")
        raise
    except Exception as e:
        logging.error(f"Error checking batch of documents: {e}")
        # Return empty dict on error to avoid breaking the calling function
        return {}