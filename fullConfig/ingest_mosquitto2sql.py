import paho.mqtt.client as mqtt
import psycopg2
import os
import json
from time import sleep

# --- Database Setup ---
DB_HOST = os.environ.get("POSTGRES_HOST", "postgres")
DB_NAME = os.environ.get("POSTGRES_DB", "machine_x")
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
                    CREATE TABLE IF NOT EXISTS iot_temp (
                        id VARCHAR(50) NOT NULL,
                        timestamp TIMESTAMPTZ NOT NULL,
                        temperature REAL NOT NULL
                    );
                """)
            conn.commit()
            return conn
        except psycopg2.OperationalError as e:
            print(f"Database connection failed: {e}. Retrying in 5 seconds...")
            sleep(5)
        except Exception as e:
            print(f"An unexpected error occurred during database connection: {e}")
            exit(1)

# --- MQTT Setup ---
MQTT_BROKER_HOST = os.environ.get("MQTT_BROKER_HOST", "mosquitto")
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
    db_conn = userdata
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
        sensor_id = payload['id']
        timestamp = payload['timestamp']
        temperature = payload['temperature']

        # --- Data Ingestion to PostgreSQL ---
        with db_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO iot_temp (id, timestamp, temperature) VALUES (%s, %s, %s)",
                (sensor_id, timestamp, temperature)
            )
        db_conn.commit()
        print(f"Ingested to PostgreSQL: {sensor_id} - {temperature}°C")

    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error parsing message: {e}")
    except psycopg2.Error as e:
        print(f"Database error: {e}. Attempting to reconnect...")
        db_conn = connect_to_db()
        client.user_data_set(db_conn)
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
        exit(1)
