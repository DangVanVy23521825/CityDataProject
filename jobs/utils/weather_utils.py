import random
from datetime import datetime

def get_seasonal_weather_condition(month):
    rain_months = {5: 0.6, 6: 0.8, 7: 0.9, 8: 0.9, 9: 0.8, 10: 0.6}  # Mùa mưa
    dry_months = {1: 0.1, 2: 0.1, 3: 0.2, 4: 0.3, 11: 0.3, 12: 0.1}  # Mùa khô
    
    rain_prob = rain_months.get(month, 0) or dry_months.get(month, 0.3)
    
    if random.random() < rain_prob:
        return random.choice(['Rain', 'Thunderstorm', 'Drizzle', 'Cloudy'])
    else:
        return random.choice(['Sunny', 'Cloudy', 'Fog'])

def get_temperature_by_location(latitude, month):
    temp_ranges = {
        1: (15, 28), 2: (18, 30), 3: (22, 32), 4: (25, 35), 5: (26, 36), 6: (25, 34),
        7: (24, 33), 8: (24, 33), 9: (24, 32), 10: (23, 31), 11: (20, 30), 12: (16, 28)
    }
    
    base_min, base_max = temp_ranges.get(month, (20, 30))
    
    # Điều chỉnh theo vĩ độ: càng xa xích đạo càng mát
    lat_adjustment = (latitude - 10) * 0.3
    
    adjusted_min = base_min - lat_adjustment
    adjusted_max = base_max - lat_adjustment
    
    return round(random.uniform(adjusted_min, adjusted_max), 1)

def get_precipitation(weather_condition):
    precipitation_map = {
        'Sunny': (0, 0), 'Cloudy': (0, 2), 'Fog': (0, 1),
        'Drizzle': (2, 10), 'Rain': (10, 50), 'Thunderstorm': (30, 100)
    }
    
    min_precip, max_precip = precipitation_map.get(weather_condition, (0, 5))
    return round(random.uniform(min_precip, max_precip), 1)

def get_wind_speed(weather_condition):
    """Tốc độ gió (km/h) dựa trên điều kiện thời tiết"""
    wind_map = {
        'Sunny': (5, 15), 'Cloudy': (10, 20), 'Fog': (0, 8),
        'Drizzle': (15, 25), 'Rain': (20, 40), 'Thunderstorm': (30, 80)
    }
    
    min_wind, max_wind = wind_map.get(weather_condition, (5, 20))
    return round(random.uniform(min_wind, max_wind), 1)

def get_humidity(weather_condition, temperature):
    """Độ ẩm (%) dựa trên thời tiết và nhiệt độ"""
    if weather_condition in ['Rain', 'Thunderstorm', 'Drizzle']:
        return random.randint(80, 95)
    elif weather_condition == 'Fog':
        return random.randint(85, 100)
    elif weather_condition == 'Sunny':
        return random.randint(50, 70) if temperature > 30 else random.randint(60, 80)
    else:  # Cloudy
        return random.randint(65, 85)

def get_air_quality_index(location, weather_condition):
    latitude, longitude = location
    
    # HCM có ô nhiễm cao hơn Đà Nẵng
    base_aqi = 80 if latitude < 12 else 60
    
    # Thời tiết ảnh hưởng đến AQI
    weather_aqi_adjustment = {
        'Sunny': 10, 'Cloudy': 0, 'Fog': 20,
        'Drizzle': -10, 'Rain': -20, 'Thunderstorm': -25
    }
    
    adjustment = weather_aqi_adjustment.get(weather_condition, 0)
    aqi = base_aqi + adjustment + random.randint(-15, 15)
    
    return max(10, min(300, aqi))

def get_visibility(weather_condition):
    visibility_map = {
        'Sunny': (15, 25), 'Cloudy': (10, 20), 'Fog': (0.5, 3),
        'Drizzle': (5, 15), 'Rain': (2, 10), 'Thunderstorm': (1, 5)
    }
    
    min_vis, max_vis = visibility_map.get(weather_condition, (5, 15))
    return round(random.uniform(min_vis, max_vis), 1)

def get_uv_index(weather_condition, hour):
    if hour < 6 or hour > 18:
        return 0
    
    # UV cao nhất vào buổi trưa
    base_uv = abs(12 - hour) / 6 * 8
    
    weather_multipliers = {
        'Sunny': 1.0, 'Cloudy': 0.6, 'Fog': 0.3,
        'Drizzle': 0.4, 'Rain': 0.2, 'Thunderstorm': 0.1
    }
    
    multiplier = weather_multipliers.get(weather_condition, 0.5)
    uv_index = base_uv * multiplier + random.uniform(-1, 1)
    
    return max(0, round(uv_index, 1))