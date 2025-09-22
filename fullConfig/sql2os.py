import psycopg2
import os
from opensearchpy import OpenSearch, ConnectionError
from time import sleep

# --- Global state variables to track the last processed timestamp for each sensor ID ---
last_ts_id1 = None
last_ts_id2 = None

# --- Database Setup ---
DB_HOST = os.environ.get("POSTGRES_HOST", "localhost")
DB_NAME = os.environ.get("POSTGRES_DB", "machine_x")
DB_USER = os.environ.get("POSTGRES_USER", "user")
DB_PASS = os.environ.get("POSTGRES_PASSWORD", "password")
DB_PORT = 5432

# --- OpenSearch Setup ---
OS_HOST = os.environ.get("OPENSEARCH_HOST", "localhost")
OS_PORT = int(os.environ.get("OPENSEARCH_PORT", "9200"))
OS_AUTH = (os.environ.get("OPENSEARCH_USER", "admin"), os.environ.get("OPENSEARCH_PASSWORD", "admin"))
OS_INDEX = "machine_x_iot_temp"

def connect_to_db():
    """Establishes a connection to the PostgreSQL database with retries."""
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                port=DB_PORT
            )
            print("Successfully connected to the PostgreSQL database.")
            return conn
        except psycopg2.OperationalError as e:
            print(f"PostgreSQL connection failed: {e}. Retrying in 5 seconds...")
            sleep(5)

def connect_to_opensearch():
    """Establishes a connection to the OpenSearch instance with retries."""
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
            print("Successfully connected to OpenSearch.")
            return client
        except ConnectionError as e:
            print(f"OpenSearch connection failed: {e}. Retrying in 5 seconds...")
            sleep(5)

def ingest_new_data(pg_conn, os_client):
    """
    Reads new data from PostgreSQL based on the last processed timestamps and
    writes it to OpenSearch.
    """
    global last_ts_id1, last_ts_id2

    # Build the WHERE clause dynamically based on the last processed timestamps
    where_clauses = []
    if last_ts_id1:
        where_clauses.append(f"id = 'id_1' AND timestamp > '{last_ts_id1.isoformat()}'")
    if last_ts_id2:
        where_clauses.append(f"id = 'id_2' AND timestamp > '{last_ts_id2.isoformat()}'")
    
    # If this is the first run, ingest all data for both IDs
    if not last_ts_id1 and not last_ts_id2:
        query = "SELECT id, timestamp, temperature FROM sensor_readings ORDER BY timestamp;"
    elif where_clauses:
        query = f"SELECT id, timestamp, temperature FROM sensor_readings WHERE {' OR '.join(where_clauses)} ORDER BY timestamp;"
    else:
        print("No new data to ingest.")
        return

    try:
        with pg_conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            
            if rows:
                print(f"Found {len(rows)} new records. Starting ingestion to OpenSearch...")
                new_timestamps = {}
                for row in rows:
                    doc = {
                        'id': row[0],
                        'timestamp': row[1].isoformat(),
                        'temperature': row[2]
                    }
                    
                    # Update the latest timestamp for each ID
                    if row[0] not in new_timestamps or row[1] > new_timestamps[row[0]]:
                        new_timestamps[row[0]] = row[1]
                    
                    # Index the document in OpenSearch
                    os_client.index(index=OS_INDEX, body=doc, id=f"{doc['id']}_{doc['timestamp']}")
                
                # Update the global timestamps
                if 'id_1' in new_timestamps:
                    last_ts_id1 = new_timestamps['id_1']
                if 'id_2' in new_timestamps:
                    last_ts_id2 = new_timestamps['id_2']

                print("Data ingestion from PostgreSQL to OpenSearch completed.")
            else:
                print("No new records to ingest.")

    except psycopg2.Error as e:
        print(f"PostgreSQL error during ingestion: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during OpenSearch ingestion: {e}")

if __name__ == "__main__":
    db_conn = connect_to_db()
    os_client = connect_to_opensearch()
    
    print("Starting continuous ingestion from PostgreSQL to OpenSearch...")
    
    while True:
        ingest_new_data(db_conn, os_client)
        sleep(15)
