import os
from time import sleep
from opensearchpy import OpenSearch, ConnectionError

# --- OpenSearch Setup ---
OS_HOST = os.environ.get("OPENSEARCH_HOST", "opensearch")
OS_PORT = int(os.environ.get("OPENSEARCH_PORT", "9200"))
OS_AUTH = (os.environ.get("OPENSEARCH_USER", "admin"), os.environ.get("OPENSEARCH_PASSWORD", "admin"))
OS_INDEX_TEMPLATE_NAME = "machine_x_iot_template"

def connect_to_opensearch():
    """Establishes a connection to the OpenSearch instance."""
    print("Attempting to connect to OpenSearch...")
    while True:
        try:
            client = OpenSearch(
                hosts=[{'host': OS_HOST, 'port': OS_PORT}],
                http_auth=OS_AUTH,
                use_ssl=True,
                verify_certs=False,
                ssl_assert_hostname=False,
                ssl_show_warn=False
            )
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
