import paho.mqtt.client as mqtt
import psycopg2
import os
import json
import csv
from time import sleep
from opensearchpy import OpenSearch, ConnectionError, NotFoundError

# --- Database Setup ---
DB_HOST = os.environ.get("POSTGRES_HOST", "localhost")
DB_NAME = os.environ.get("POSTGRES_DB", "sensor_data")
DB_USER = os.environ.get("POSTGRES_USER", "user")
DB_PASS = os.environ.get("POSTGRES_PASSWORD", "password")
DB_PORT = 5432

def connect_to_db():
    """Establishes a connection to the PostgreSQL database."""
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
            # Create the table if it doesn't exist
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS sensor_readings (
                        id VARCHAR(50) NOT NULL,
                        timestamp TIMESTAMPTZ NOT NULL,
                        temperature REAL NOT NULL
                    );
                """)
            conn.commit()
            return conn
        except psycopg2.OperationalError:
            print("Database connection failed. Retrying in 5 seconds...")
            sleep(5)

# --- OpenSearch Setup ---
OS_HOST = os.environ.get("OPENSEARCH_HOST", "localhost")
OS_PORT = int(os.environ.get("OPENSEARCH_PORT", "9200"))
OS_AUTH = (os.environ.get("OPENSEARCH_USER", "admin"), os.environ.get("OPENSEARCH_PASSWORD", "admin"))
OS_INDEX = "jans_machine_temp_test"

def connect_to_opensearch():
    """Establishes a connection to the OpenSearch instance."""
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

            # Create the index if it doesn't exist
            if not client.indices.exists(index=OS_INDEX):
                print(f"Index '{OS_INDEX}' not found. Creating it...")
                client.indices.create(
                    index=OS_INDEX,
                    body={
                        "settings": {
                            "index": {
                                "number_of_shards": 1,
                                "number_of_replicas": 0
                            }
                        }
                    }
                )
            
            return client
        except ConnectionError:
            print("OpenSearch connection failed. Retrying in 5 seconds...")
            sleep(5)

def write_to_critical_file(data):
    """Appends a new row to the critical_hardware.csv file."""
    file_path = "/app/data/critical_hardware.csv"
    file_exists = os.path.isfile(file_path)

    with open(file_path, 'a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['timestamp', 'id', 'temperature'])
        writer.writerow([data['timestamp'], data['id'], data['temperature']])

# --- MQTT Setup ---
MQTT_BROKER_HOST = os.environ.get("MQTT_BROKER_HOST", "localhost")
MQTT_PORT = 1883
TOPIC = "sensors/temperature"

def on_connect(client, userdata, flags, rc):
    """Callback function for when the client connects to the MQTT broker."""
    if rc == 0:
        print("Successfully connected to MQTT Broker.")
        client.subscribe(TOPIC)
    else:
        print(f"Failed to connect to MQTT Broker, return code {rc}")
        exit(1)

def on_message(client, userdata, msg):
    """Callback function for when a message is received from the broker."""
    db_conn, os_client = userdata
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
        sensor_id = payload['id']
        timestamp = payload['timestamp']
        temperature = payload['temperature']

        # --- Threshold Detection ---
        THRESHOLD = 100
        if temperature > THRESHOLD:
            print(f"ALERT! Temperature for {sensor_id} is above threshold: {temperature}°C")
            write_to_critical_file(payload)

        # --- Data Ingestion to PostgreSQL ---
        with db_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO sensor_readings (id, timestamp, temperature) VALUES (%s, %s, %s)",
                (sensor_id, timestamp, temperature)
            )
        db_conn.commit()
        print(f"Ingested to PostgreSQL: {sensor_id} - {temperature}°C")

        # --- Data Ingestion to OpenSearch ---
        os_client.index(index=OS_INDEX, body=payload)
        print(f"Ingested to OpenSearch: {sensor_id} - {temperature}°C")

    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error parsing message: {e}")
    except psycopg2.Error as e:
        print(f"Database error: {e}. Attempting to reconnect...")
        db_conn = connect_to_db()
        client.user_data_set((db_conn, os_client))
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# --- Main Logic ---
if __name__ == "__main__":
    db_conn = connect_to_db()
    os_client = connect_to_opensearch()
    
    mqtt_client = mqtt.Client(userdata=(db_conn, os_client))
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    try:
        mqtt_client.connect(MQTT_BROKER_HOST, MQTT_PORT, 60)
        mqtt_client.loop_forever()
    except Exception as e:
        print(f"Connection to MQTT broker failed: {e}")
        exit(1)
