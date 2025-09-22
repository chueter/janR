import paho.mqtt.client as mqtt
import os
import time
import json
from datetime import datetime, timedelta

# --- MQTT Setup ---
MQTT_BROKER_HOST = os.environ.get("MQTT_BROKER_HOST", "localhost")
MQTT_PORT = 1883
TOPIC = "sensors/temperature"

# --- Main Logic ---
def simulate_and_publish():
    """Generates and publishes 10,000 data points to the MQTT broker."""
    mqtt_client = mqtt.Client()
    
    try:
        mqtt_client.connect(MQTT_BROKER_HOST, MQTT_PORT, 60)
    except Exception as e:
        print(f"Connection to MQTT broker failed: {e}")
        exit(1) # Exit to trigger a container restart
        
    mqtt_client.loop_start()

    # Initial values for data generation
    initial_temp_id1 = 20
    initial_temp_id2 = 20
    current_temp_id2 = initial_temp_id2
    start_time = datetime.now()
    
    # We will run the simulation for 10,000 data points (1e4)
    # This will take 50,000 seconds (5 seconds * 10,000 points)
    num_data_points = 10000

    for i in range(num_data_points):
        # Determine which ID and temperature to use
        is_id2 = i % 2 == 1
        sensor_id = "id_2" if is_id2 else "id_1"
        
        if is_id2:
            temperature = current_temp_id2
            current_temp_id2 += 2 
            current_temp_id2 = current_temp_id2 % 60
        else:
            temperature = initial_temp_id1

        # Generate timestamp in ISO format
        current_time = start_time + timedelta(seconds=i * 5)
        timestamp = current_time.isoformat(timespec='seconds')

        # Create the data payload as a dictionary
        data_point = {
            "timestamp": timestamp,
            "id": sensor_id,
            "temperature": temperature
        }

        # Convert to JSON and publish to MQTT
        payload = json.dumps(data_point)
        result = mqtt_client.publish(TOPIC, payload)
        
        # Check for successful publish and log
        status = result[0]
        if status == 0:
            print(f"Published data point {i+1}/{num_data_points}: {payload} to topic {TOPIC}")
        else:
            print(f"Failed to publish message to topic {TOPIC}")
            
        time.sleep(5) # Wait for 5 seconds before the next data point

    mqtt_client.loop_stop()
    mqtt_client.disconnect()

if __name__ == "__main__":
    simulate_and_publish()
