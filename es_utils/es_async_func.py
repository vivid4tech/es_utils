from .client import es_async
import logging
from elasticsearch import exceptions as es_exceptions


async def add_document_to_index(index_name: str ,doc: dict) -> bool:
    """
    Adds a document to the specified Elasticsearch index.

    Args:
        index_name (str): The name of the Elasticsearch index.
        doc (dict): The document to be added.

    Returns:
        bool: True if the document was successfully added, False otherwise.
    """
    try:
        doc_id = doc.get('id', None)
        if not doc_id:
            logging.warning(f"Document does not contain an 'id' field. Document: {doc}")
            return False
        
        response = await es_async.index(index=index_name, id=doc_id, document=doc)
        if response.get('result') in ['created', 'updated']:
            return True
        return False
    except es_exceptions.RequestError as e:
        logging.error(f"Failed to index document. Error: {e}")
        return False

async def document_exists_in_es(index_name: str, doc_id: str) -> bool:
    """Check if a document exists in Elasticsearch index.

    Args:
        index_name (str): The name of the Elasticsearch index.
        doc_id (str): The ID of the document to check.

    Returns:
        bool: True if the document exists, False otherwise.
    """
    try:
        response = await es_async.get(index=index_name, id=doc_id, ignore=404)
        return response and response.get('found', False)
    except Exception as e:
        logging.error(f"Failed to check existence of document with id {doc_id}. Error: {e}")
        return False