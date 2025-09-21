import os
from time import sleep
from opensearchpy import OpenSearch, ConnectionError

# --- OpenSearch Setup ---
OS_HOST = os.environ.get("OPENSEARCH_HOST", "opensearch")
OS_PORT = int(os.environ.get("OPENSEARCH_PORT", "9200"))
OS_USER = os.environ.get("OPENSEARCH_USER")
OS_PASSWORD = os.environ.get("OPENSEARCH_PASSWORD")
OS_USE_SSL = os.environ.get("OPENSEARCH_USE_SSL", "false").lower() == "true"

OS_INDEX_TEMPLATE_NAME = "machine_x_iot_template"

def connect_to_opensearch():
    """Establishes a connection to the OpenSearch instance."""
    print(f"Attempting to connect to OpenSearch at {OS_HOST}:{OS_PORT} (SSL={OS_USE_SSL})...")

    while True:
        try:
            client_args = {
                "hosts": [{"host": OS_HOST, "port": OS_PORT}],
                "use_ssl": OS_USE_SSL,
                "verify_certs": OS_USE_SSL,
                "ssl_show_warn": False
            }

            if OS_USER and OS_PASSWORD:
                client_args["http_auth"] = (OS_USER, OS_PASSWORD)

            client = OpenSearch(**client_args)

            # A simple call to ensure the connection is active
            client.info()
            print("Successfully connected to OpenSearch.")
            return client
        except ConnectionError as e:
            print(f"OpenSearch connection failed: {e}. Retrying in 5 seconds...")
            sleep(5)
        except Exception as e:
            print(f"An unexpected error occurred during OpenSearch connection: {e}")
            exit(1)

def create_index_template(client):
    """
    Creates an index template with a specified pattern and mappings.
    This ensures all future indices matching the pattern have the correct schema.
    """
    print("Creating OpenSearch index template...")
    
    template_body = {
        "index_patterns": ["machine_x_iot*"],
        "template": {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": {
                "properties": {
                    "id": { "type": "keyword" },
                    "timestamp": { "type": "date" },
                    "temperature": { "type": "float" }
                }
            }
        }
    }

    try:
        # Delete any existing template to ensure the new one is applied
        if client.indices.exists_index_template(name=OS_INDEX_TEMPLATE_NAME):
            print(f"Existing template '{OS_INDEX_TEMPLATE_NAME}' found. Deleting it...")
            client.indices.delete_index_template(name=OS_INDEX_TEMPLATE_NAME)

        # Create the new template
        response = client.indices.put_index_template(
            name=OS_INDEX_TEMPLATE_NAME,
            body=template_body
        )
        if response.get("acknowledged"):
            print(f"Successfully created index template '{OS_INDEX_TEMPLATE_NAME}'.")
        else:
            print(f"Failed to create index template: {response}")
            exit(1)

    except Exception as e:
        print(f"Error creating index template: {e}")
        exit(1)

if __name__ == "__main__":
    os_client = connect_to_opensearch()
    create_index_template(os_client)
    print("OpenSearch setup complete.")
