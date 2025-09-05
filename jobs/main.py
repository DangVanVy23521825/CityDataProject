# main.py (fixed)
import os
import random
import uuid
import time
import logging
from datetime import datetime, timedelta
from confluent_kafka import Producer
import simplejson as json
from jobs.utils.geo_utils import haversine_km, bearing_deg, bearing_to_cardinal, move_towards
from jobs.utils.weather_utils import get_seasonal_weather_condition, get_temperature_by_location, get_precipitation, get_wind_speed, get_humidity, get_air_quality_index

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data-producer")

# IMPORTANT: inside docker-compose use broker:29092
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'broker:29092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')

HCM_COORDINATES = {"latitude": 10.769444, "longitude": 106.681944}
DANANG_COORDINATES = {"latitude": 16.076667, "longitude": 108.222778}

start_time = datetime.now()
start_location = HCM_COORDINATES.copy()

def get_next_time():
    global start_time
    delta = random.randint(30, 60)
    start_time += timedelta(seconds=delta)
    return start_time, delta

def simulate_vehicle_movement(delta_seconds, speed_kmh):
    global start_location
    step_km = speed_kmh * (delta_seconds / 3600.0)
    rem_km = haversine_km(start_location["latitude"], start_location["longitude"],
                          DANANG_COORDINATES["latitude"], DANANG_COORDINATES["longitude"])
    if rem_km <= 0.05:
        return start_location, True
    if step_km >= rem_km:
        start_location = DANANG_COORDINATES.copy()
        return start_location, True
    brg = bearing_deg(start_location["latitude"], start_location["longitude"],
                      DANANG_COORDINATES["latitude"], DANANG_COORDINATES["longitude"])
    lat2, lon2 = move_towards(start_location["latitude"], start_location["longitude"], brg, step_km)
    lat2 += random.uniform(-0.00018, 0.00018)
    lon2 += random.uniform(-0.00018, 0.00018)
    start_location = {"latitude": lat2, "longitude": lon2}
    return start_location, False

def generate_vehicle_data(vehicle_id):
    ts, delta = get_next_time()
    speed_kmh = round(random.uniform(60, 90), 1)
    loc, _ = simulate_vehicle_movement(delta, speed_kmh)
    brg = bearing_deg(loc["latitude"], loc["longitude"],
                      DANANG_COORDINATES["latitude"], DANANG_COORDINATES["longitude"])
    return {
        "id": str(uuid.uuid4()),
        "vehicle_id": vehicle_id,
        "timestamp": ts.isoformat(),
        "location": {"latitude": round(loc["latitude"], 6), "longitude": round(loc["longitude"], 6)},
        "speed_kmh": speed_kmh,
        "direction": bearing_to_cardinal(brg),
        "bearing_deg": round(brg, 1),
        "make": "VinFast",
        "model": "VF 8",
        "year": 2024,
        "fuelType": "Electric",
    }

def generate_gps_data(vehicle_id, timestamp, speed_kmh, bearing):
    return {
        "id": str(uuid.uuid4()),
        "vehicle_id": vehicle_id,
        "timestamp": timestamp,
        "speed_kmh": speed_kmh,
        "direction": bearing_to_cardinal(bearing),
        "vehicleType": "private"
    }

def generate_traffic_camera_data(vehicle_id, timestamp, location, camera_id):
    # location expected as dict {"latitude":..,"longitude":..}
    return {
        "id": str(uuid.uuid4()),
        "vehicle_id": vehicle_id,
        "camera_id": camera_id,
        "location": {"latitude": round(location["latitude"], 6), "longitude": round(location["longitude"], 6)},
        "timestamp": timestamp if isinstance(timestamp, str) else timestamp.isoformat(),
        "snapshot": "Base64EncodedString"
    }

def generate_weather_data(vehicle_id, timestamp, location):
    if isinstance(timestamp, str):
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
    else:
        dt = timestamp
    month = dt.month
    hour = dt.hour
    # CORRECT: read numeric values from dict
    latitude = float(location["latitude"]) if isinstance(location, dict) else float(location[0])
    longitude = float(location["longitude"]) if isinstance(location, dict) else float(location[1])
    weather_condition = get_seasonal_weather_condition(month)
    temperature = get_temperature_by_location(latitude, month)
    precipitation = get_precipitation(weather_condition)
    wind_speed = get_wind_speed(weather_condition)
    humidity = get_humidity(weather_condition, temperature)
    aqi = get_air_quality_index((latitude, longitude), weather_condition)
    return {
        'id': str(uuid.uuid4()),
        'vehicle_id': vehicle_id,
        "location": {"latitude": round(latitude, 6), "longitude": round(longitude, 6)},
        'timestamp': timestamp if isinstance(timestamp, str) else timestamp.isoformat(),
        'temperature': temperature,
        'weatherCondition': weather_condition,
        'precipitation': precipitation,
        'windSpeed': wind_speed,
        'humidity': humidity,
        'airQualityIndex': aqi,
    }

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed for record {msg.key()}: {err}")
    else:
        logger.debug(f"Record {msg.key()} produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def produce_data_to_kafka(producer, topic, data):
    payload = json.dumps(data).encode("utf-8")
    # produce async
    producer.produce(topic=topic, key=str(data["id"]), value=payload, callback=delivery_report)

def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(
            vehicle_id=device_id,
            timestamp=vehicle_data["timestamp"],
            speed_kmh=vehicle_data["speed_kmh"],
            bearing=vehicle_data["bearing_deg"],
        )
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'],'AI camera')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])

        if (vehicle_data['location']["latitude"] >= DANANG_COORDINATES["latitude"]
                and vehicle_data['location']["longitude"] >= DANANG_COORDINATES["longitude"]):
            print("Vehicle has reached Da Nang. Simulation ending...")
            break     

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)

        producer.poll(0)   # serve delivery callbacks
        producer.flush()   # ensure delivery; in prod use periodic flush
        logger.info(f"Produced batch for {device_id} @ {vehicle_data['timestamp']}")
        time.sleep(0.2)  # throttle: 1s between batches

if __name__ == "__main__":
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'iot-producer'
    }
    producer = Producer(conf)
    try:
        simulate_journey(producer, 'DangVanVy')
    except KeyboardInterrupt:
        logger.info("Shutting down producer")
    except Exception as e:
        logger.exception("Producer error: %s", e)