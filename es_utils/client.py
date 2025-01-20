from elasticsearch import AsyncElasticsearch
from elasticsearch import Elasticsearch


if "es_async" not in globals():
    es_async = None

if "es_sync" not in globals():
    es_sync = None


def initialize_es_clients(es_connection_params: dict) -> None:
    """
    Initialize Elasticsearch clients.

    Args:
        es_connection_params (dict): Connection parameters for Elasticsearch.

    Returns:
        None
    """
    global es_async, es_sync
    es_async = AsyncElasticsearch(**es_connection_params)
    es_sync = Elasticsearch(**es_connection_params)
