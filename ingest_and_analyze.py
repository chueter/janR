import paho.mqtt.client as mqtt
import psycopg2
import os
import json
import csv
from time import sleep

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
            print("Successfully connected to the database.")
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

def write_to_critical_file(data):
    """Appends a new row to the critical_hardware.csv file."""
    # The 'data' directory is mounted from the host to ensure persistence.
    file_path = "/app/data/critical_hardware.csv"
    file_exists = os.path.isfile(file_path)

    with open(file_path, 'a', newline='') as f:
        writer = csv.writer(f)
        # Write the header row only if the file is new
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
        exit(1) # Exit to trigger a container restart

def on_message(client, userdata, msg):
    """Callback function for when a message is received from the broker."""
    db_conn = userdata
    try:
        # Parse the JSON payload
        payload = json.loads(msg.payload.decode('utf-8'))
        sensor_id = payload['id']
        timestamp = payload['timestamp']
        temperature = payload['temperature']

        # --- Threshold Detection ---
        THRESHOLD = 25
        if temperature > THRESHOLD:
            print(f"ALERT! Temperature for {sensor_id} is above threshold: {temperature}°C")
            # Write the critical data point to the file
            write_to_critical_file(payload)

        # --- Data Ingestion ---
        with db_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO sensor_readings (id, timestamp, temperature) VALUES (%s, %s, %s)",
                (sensor_id, timestamp, temperature)
            )
        db_conn.commit()
        print(f"Ingested data for {sensor_id}: {temperature}°C at {timestamp}")

    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error parsing message: {e}")
    except psycopg2.Error as e:
        print(f"Database error: {e}. Attempting to reconnect...")
        db_conn = connect_to_db() # Reconnect on a database error
        client.user_data_set(db_conn) # Update user data with new connection
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# --- Main Logic ---
if __name__ == "__main__":
    db_conn = connect_to_db()
    
    mqtt_client = mqtt.Client(userdata=db_conn)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    try:
        mqtt_client.connect(MQTT_BROKER_HOST, MQTT_PORT, 60)
        mqtt_client.loop_forever()
    except Exception as e:
        print(f"Connection to MQTT broker failed: {e}")
        exit(1) # Exit to trigger a container restart
