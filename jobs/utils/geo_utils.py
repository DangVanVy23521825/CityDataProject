import math

EARTH_RADIUS_KM = 6371.0

def haversine_km(lat1, lon1, lat2, lon2):
    rlat1, rlon1, rlat2, rlon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = rlat2 - rlat1
    dlon = rlon2 - rlon1
    a = math.sin(dlat/2)**2 + math.cos(rlat1)*math.cos(rlat2)*math.sin(dlon/2)**2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))

def bearing_deg(lat1, lon1, lat2, lon2):
    φ1, λ1, φ2, λ2 = map(math.radians, [lat1, lon1, lat2, lon2])
    y = math.sin(λ2-λ1) * math.cos(φ2)
    x = math.cos(φ1)*math.sin(φ2) - math.sin(φ1)*math.cos(φ2)*math.cos(λ2-λ1)
    θ = math.degrees(math.atan2(y, x))
    return (θ + 360) % 360

def bearing_to_cardinal(deg):
    dirs = ["N","NE","E","SE","S","SW","W","NW","N"]
    return dirs[int((deg + 22.5)//45)]

def move_towards(lat1, lon1, bearing, distance_km):
    δ = distance_km / EARTH_RADIUS_KM
    θ = math.radians(bearing)
    φ1 = math.radians(lat1)
    λ1 = math.radians(lon1)

    φ2 = math.asin(math.sin(φ1)*math.cos(δ) + math.cos(φ1)*math.sin(δ)*math.cos(θ))
    λ2 = λ1 + math.atan2(math.sin(θ)*math.sin(δ)*math.cos(φ1),
                         math.cos(δ)-math.sin(φ1)*math.sin(φ2))
    return math.degrees(φ2), (math.degrees(λ2) + 540) % 360 - 180