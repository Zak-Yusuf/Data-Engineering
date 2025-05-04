# The main production file

import os
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import random
import uuid
import time

# Retrieve latitude and longitude for Cities in UK - London and Birmingham
LONDON_COORDINATES = { "latitude": 51.5074, "longitude": -0.1278 }
BIRMINGHAM_COORDINATES = { "latitude": 52.4862, "longitude": -1.8904 }

# Calculate the movement increments for latitude and longitude
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["latitude"] - LONDON_COORDINATES["latitude"]) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["longitude"] - LONDON_COORDINATES["longitude"]) / 100

# Environment variables for configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "vehicle_data")
GPS_TOPIC = os.getenv("GPS_TOPIC", "gps_data")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic_data")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather_data")
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", "emergency_data")

# logic
random.seed(42) # for reproducibility
start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()

def get_next_time():
    global start_time
    # increment the time by 1 second for each vehicle data generation
    start_time += timedelta(seconds=random.randint(30,60)) # update frequency
    return start_time

def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    # generate GPS data for the vehicle
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(10, 40), # speed in miles/h
        'direction': 'North-East',
        'vehicleType': vehicle_type
    }

def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    # generate traffic data for the vehicle
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'cameraId': camera_id,
        'location': location,
        'snapshot': 'Base64EncodedString'
    }

def generate_weather_data(device_id, timestamp, location):
    return {   
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'location': location,
        'temperature': random.uniform(-4, 26), # temperature in Celsius
        'humidity': random.uniform(0, 100), # humidity in percentage
        'weatherCondition': random.choice(['Sunny', 'Rainy', 'Cloudy', 'Snowy']),
        'precipitation': random.uniform(0, 25), # precipitation in mm
        'windSpeed': random.uniform(0, 20), # wind speed in km/h
        'airQualityIndex': random.uniform(0, 500) # air quality index
    }

def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentId': uuid.uuid4(),
        'incidentType': random.choice(['Accident', 'RoadBlock', 'Fire', 'None']),
        'timestamp': timestamp,
        'location': location,
        'incidentStatus': random.choice(['Active', 'Resolved']),
        'severity': random.choice(['Low', 'Medium', 'High']),
        'description': f"Incident of type {random.choice(['Accident', 'RoadBlock', 'Fire', 'Flood'])} reported."
    }

def simulate_vehicle_movement():
    global start_location

    # move the vehicle towards Birmingham
    start_location["latitude"] += LATITUDE_INCREMENT
    start_location["longitude"] += LONGITUDE_INCREMENT

    # add some randomness to simulate actual road movement 
    start_location["latitude"] += random.uniform(-0.0005, 0.0005)
    start_location["longitude"] += random.uniform(-0.0005, 0.0005)

    return start_location

def generate_vehicle_data(device_id):
    location= simulate_vehicle_movement()

    # return object that represents description of vehicle
    return {
        'id' : uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location["latitude"], location["longitude"]),
        'speed': random.uniform(10, 40), # speed in miles/h
        'direction': 'North-East',
        # the information is usually collected from an IoT device and sent to the server
        'make': 'Mercedes',
        'model': 'A-Class',
        'year': 2025,
        'fuelType': 'Electric'
    }

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.name} not JSON serializable")

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def producer_data_to_kafka(producer, topic, data):
    producer.produce(
        topic=topic,
        key= str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report,
    )

    producer.flush()

def simulate_journey(producer, device_id):
    while True:
        # generate vehicle data and send to Kafka topic
        vehicle_data = generate_vehicle_data(device_id)
        # generate GPS data and send to Kafka topic
        gps_data = generate_gps_data(device_id,vehicle_data['timestamp'])
        # generate traffic data and send to Kafka topic
        traffic_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'], 'camera-Canon')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])

        if (vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES["latitude"] 
        and vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES["longitude"]):    
            print("Vehicle has reached Birmingham. Simulation ended.")
            break            

        # Write data to Kafka topics
        producer_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        producer_data_to_kafka(producer, GPS_TOPIC, gps_data)
        producer_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_data)
        producer_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        producer_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        time.sleep(3)

# entry point function 
if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f"Error: {err}")
    } 
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-hatchback')

    except KeyboardInterrupt:
        print("Journey simulation interrupted by user.")
    except Exception as e:
        print(f"An unxepected error occurred: {e}")