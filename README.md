# Elasticsearch Utilities

This repository contains utility functions for interacting with Elasticsearch. It includes both synchronous and asynchronous functions to perform various operations.

## Installation

To install the package, use [Poetry](https://python-poetry.org/):

```sh
poetry install
```

## Usage

Before using the utility functions, you need to initialize the Elasticsearch client with your connection parameters:

```python
from es_utils import client, es_async_func, es_sync_func

ES_CONNECTION_PARAMS = {
    "hosts": [{"host": DOMAIN, "port": PORT, 'scheme': 'https'}],
    "http_auth": (ES_USER, ES_PASSWORD)
}

client.initialize_es_clients(ES_CONNECTION_PARAMS)

# Test the connection
es_sync_func.test_es_connection()
```

## Project Structure

```
es-utils/
├── es_utils/
│   ├── __init__.py
│   ├── client.py
│   ├── es_async_func.py
│   └── es_sync_func.py
├── poetry.lock
├── pyproject.toml
└── README.md
```