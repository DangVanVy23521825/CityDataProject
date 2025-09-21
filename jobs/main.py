import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import random
import requests
import uuid
import time
import logging
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer
import simplejson as json
from utils.geo_utils import haversine, bearing_deg, bearing_to_cardinal, get_osrm_route

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data-producer")

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "xxxx")
OPENWEATHER_URL = "http://api.openweathermap.org/data/2.5/weather"

TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY", "xxxx")
TOMTOM_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/relative0/10/json"


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

def generate_vehicle_data(vehicle_id, ts, lat1, lon1, lat2, lon2):
    dist_km = haversine(lat1, lon1, lat2, lon2)
    delta = timedelta(seconds=random.randint(300, 600))
    ts = ts + delta

    speed_kmh = (dist_km / (delta.total_seconds() / 3600)) if delta.total_seconds() > 0 else 0

    speed_kmh = max(0, min(speed_kmh, 120))
    brg = bearing_deg(lat1, lon1, lat2, lon2)

    data = {
        "id": str(uuid.uuid4()),
        "vehicle_id": vehicle_id,
        "timestamp": ts.isoformat(),
        "location": {"latitude": round(lat2, 6), "longitude": round(lon2, 6)},
        "speed_kmh": round(speed_kmh, 2),
        "direction": bearing_to_cardinal(brg),
        "bearing_deg": round(brg, 1),
        "make": "VinFast",
        "model": "VF 8",
        "year": 2024,
        "fuelType": "Electric",
    }
    return ts, data

def generate_gps_data(vehicle_id, ts, speed_kmh, brg):
    return {
        "id": str(uuid.uuid4()),
        "vehicle_id": vehicle_id,
        "timestamp": ts.isoformat(),
        "speed_kmh": speed_kmh,
        "direction": bearing_to_cardinal(brg),
        "vehicleType": "private"
    }

def generate_traffic_camera_data(vehicle_id, timestamp, location, camera_id="simulated_camera"):
    lat, lon = location["latitude"], location["longitude"]

    params = {
        "point": f"{lat},{lon}",
        "unit": "KMPH",
        "key": TOMTOM_API_KEY
    }

    try:
        resp = requests.get(TOMTOM_URL, params=params)
        resp.raise_for_status()
        traffic_json = resp.json()

        current_speed = traffic_json["flowSegmentData"]["currentSpeed"]
        free_flow_speed = traffic_json["flowSegmentData"]["freeFlowSpeed"]
        road_coverage = traffic_json["flowSegmentData"]["confidence"]
        congestion = round((1 - current_speed / free_flow_speed) * 100, 2)

        return {
            "id": str(uuid.uuid4()),
            "vehicle_id": vehicle_id,
            "camera_id": camera_id,
            "location": {"latitude": round(lat, 6), "longitude": round(lon, 6)},
            "timestamp": timestamp if isinstance(timestamp, str) else timestamp.isoformat(),
            "currentSpeed": current_speed,
            "freeFlowSpeed": free_flow_speed,
            "congestionLevel": congestion,
            "confidence": road_coverage,
        }
    except Exception as e:
        logger.warning(f"Traffic API fetch failed: {e}")
        return {
            "id": str(uuid.uuid4()),
            "vehicle_id": vehicle_id,
            "camera_id": camera_id,
            "location": {"latitude": round(lat, 6), "longitude": round(lon, 6)},
            "timestamp": timestamp if isinstance(timestamp, str) else timestamp.isoformat(),
            "error": str(e)
        }

def generate_weather_data(vehicle_id, timestamp, location):
    lat, lon = location["latitude"], location["longitude"]

    weather_params = {
        "lat": lat,
        "lon": lon,
        "appid": OPENWEATHER_API_KEY,
        "units": "metric"
    }
    weather_resp = requests.get(OPENWEATHER_URL, params=weather_params, timeout=5)
    weather_resp.raise_for_status()
    weather_json = weather_resp.json()

    temperature = weather_json["main"]["temp"]
    weather_condition = weather_json["weather"][0]["main"]
    wind_speed = weather_json["wind"]["speed"]
    humidity = weather_json["main"]["humidity"]

    # ---- Air Pollution (OpenWeatherMap Air Pollution API) ----
    pollution_url = "http://api.openweathermap.org/data/2.5/air_pollution"
    pollution_params = {
        "lat": lat,
        "lon": lon,
        "appid": OPENWEATHER_API_KEY
    }
    try:
        pollution_resp = requests.get(pollution_url, params=pollution_params, timeout=5)
        pollution_resp.raise_for_status()
        pollution_json = pollution_resp.json()

        aqi = pollution_json["list"][0]["main"]["aqi"]   # AQI index (1-5)
        pm2_5 = pollution_json["list"][0]["components"]["pm2_5"]
        pm10 = pollution_json["list"][0]["components"]["pm10"]
    except Exception as e:
        logger.warning(f"Air Pollution API fetch failed: {e}")
        aqi = None

    return {
        "id": str(uuid.uuid4()),
        "vehicle_id": vehicle_id,
        "location": {"latitude": round(lat, 6), "longitude": round(lon, 6)},
        "timestamp": timestamp if isinstance(timestamp, str) else timestamp.isoformat(),
        "temperature": temperature,
        "weatherCondition": weather_condition,
        "precipitation": 0.0,
        "windSpeed": wind_speed,
        "humidity": humidity,
        "airQualityIndex": aqi
    }

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed for record {msg.key()}: {err}")
    else:
        logger.debug(f"Record {msg.key()} produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def produce_data_to_kafka(producer, topic, data):
    payload = json.dumps(data).encode("utf-8")
    producer.produce(topic=topic, key=str(data["id"]), value=payload, callback=delivery_report)

def simulate_journey(producer, vehicle_id):
    route_points = get_osrm_route(
        (HCM_COORDINATES["latitude"], HCM_COORDINATES["longitude"]),
        (DANANG_COORDINATES["latitude"], DANANG_COORDINATES["longitude"])
    )
    route_points = route_points[::20]

    ts = datetime.now(timezone.utc)
    for i in range(1, len(route_points)):
        lat1, lon1 = route_points[i - 1]
        lat2, lon2 = route_points[i]

        ts, vehicle_data = generate_vehicle_data(vehicle_id, ts, lat1, lon1, lat2, lon2)
        gps_data = generate_gps_data(vehicle_id, ts,
                                     vehicle_data["speed_kmh"],
                                     vehicle_data["bearing_deg"])
        traffic_data = generate_traffic_camera_data(vehicle_id, ts, vehicle_data["location"])
        weather_data = generate_weather_data(vehicle_id, ts, vehicle_data["location"])

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)

        producer.poll(0)
        logger.info(f"Produced data @ {vehicle_data['timestamp']}")
        time.sleep(0.01)

    producer.flush()
    print("Simulation completed: HCM -> Da Nang")

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

