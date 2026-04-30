import boto3
import os
import json
import requests
from datetime import datetime, timezone


# Set this in Lambda environment variables
REGION = os.environ['REGION']
STREAM_NAME = os.environ["FIREHOSE_STREAM_NAME"]

firehose = boto3.client("firehose", region_name=REGION)

# City definitions
# latitude/longitude match Open-Meteo's coordinate-based API
CITIES = [
    {"city": "Mumbai",      "country": "IN", "lat":  19.0760, "lon":  72.8777},
    {"city": "Delhi",       "country": "IN", "lat":  28.7041, "lon":  77.1025},
    {"city": "New York",    "country": "US", "lat":  40.7128, "lon": -74.0060},
    {"city": "London",      "country": "GB", "lat":  51.5074, "lon":  -0.1278},
    {"city": "Dubai",       "country": "AE", "lat":  25.2048, "lon":  55.2708},
    {"city": "Singapore",   "country": "SG", "lat":   1.3521, "lon": 103.8198},
    {"city": "Sydney",      "country": "AU", "lat": -33.8688, "lon": 151.2093},
    {"city": "Tokyo",       "country": "JP", "lat":  35.6762, "lon": 139.6503},
    {"city": "Paris",       "country": "FR", "lat":  48.8566, "lon":   2.3522},
    {"city": "Frankfurt",   "country": "DE", "lat":  50.1109, "lon":   8.6821},
]

# Open-Meteo variables to fetch
# current= params: https://open-meteo.com/en/docs
CURRENT_VARS = [
    "temperature_2m",           # °C at 2m height
    "relative_humidity_2m",     # %
    "apparent_temperature",     # feels-like °C
    "precipitation",            # mm in last hour
    "rain",                     # mm
    "weather_code",             # WMO code (0=clear, 61=rain, 71=snow etc.)
    "wind_speed_10m",           # km/h
    "wind_direction_10m",       # degrees
    "surface_pressure",         # hPa
    "cloud_cover",              # %
    "visibility",               # metres
    "is_day",                   # 1 = day, 0 = night
]

# WMO weather code → human-readable description
# https://open-meteo.com/en/docs#weathervariables
WMO_DESCRIPTIONS = {
    0:  "Clear sky",
    1:  "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Depositing rime fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
    95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Heavy thunderstorm",
}


def fetch_weather_batch() -> list[dict]:
    """
    Fetches weather for all 10 cities in a single API call using
    Open-Meteo's batch endpoint (comma-separated lat/lon params).
    Returns list of per-city weather dicts.
    """
    lats = ",".join(str(c["lat"]) for c in CITIES)
    lons = ",".join(str(c["lon"]) for c in CITIES)
    vars_param = ",".join(CURRENT_VARS)

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lats,
        "longitude": lons,
        "current": vars_param,
        "timezone": "UTC",
        "forecast_days": 1,
    }

    response = requests.get(url, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()

    # Batch response returns a list when multiple locations are requested
    if isinstance(data, dict):
        data = [data]   # single city fallback

    results = []
    for city_meta, city_data in zip(CITIES, data):
        current = city_data.get("current", {})
        weather_code = current.get("weather_code", -1)

        results.append({
            **city_meta,                            # city, country, lat, lon
            "observation_time": current.get("time"),
            "temperature_c": current.get("temperature_2m"),
            "feels_like_c": current.get("apparent_temperature"),
            "humidity_pct": current.get("relative_humidity_2m"),
            "precipitation_mm": current.get("precipitation"),
            "rain_mm": current.get("rain"),
            "weather_code": weather_code,
            "weather_description": WMO_DESCRIPTIONS.get(weather_code, "Unknown"),
            "wind_speed_kmh": current.get("wind_speed_10m"),
            "wind_direction_deg": current.get("wind_direction_10m"),
            "surface_pressure_hpa": current.get("surface_pressure"),
            "cloud_cover_pct": current.get("cloud_cover"),
            "visibility_m": current.get("visibility"),
            "is_day": bool(current.get("is_day", 1)),
            "timezone": city_data.get("timezone", "UTC"),
        })

    return results


def send_to_firehose(records: list[dict], run_id: str) -> int:
    ingested_at = datetime.now(timezone.utc).isoformat()
    ingestion_date = datetime.now(timezone.utc).date().isoformat() 
    firehose_records = []
    for r in records:
        payload = {
            **r,
            "dataset": "bronze_weather",
            "source": "open_meteo",
            "run_id": run_id,
            "ingested_at": ingested_at,
            "ingestion_date": ingestion_date
        }
        firehose_records.append({
            "Data": (json.dumps(payload) + "\n").encode("utf-8")
        })

    sent = 0
    for i in range(0, len(firehose_records), 500):
        batch = firehose_records[i : i + 500]
        response = firehose.put_record_batch(
            DeliveryStreamName=STREAM_NAME,
            Records=batch,
        )
        failed = response.get("FailedPutCount", 0)
        if failed:
            print(f"[WARN] {failed} records failed in batch at index {i}")
        sent += len(batch) - failed

    return sent


def lambda_handler(event, context):
    run_id = context.aws_request_id
    print(f"[weather] run_id={run_id}")

    city_records = fetch_weather_batch()
    print(f"[weather] fetched weather for {len(city_records)} cities")

    sent = send_to_firehose(city_records, run_id)
    print(f"[weather] sent {sent} records → dataset=bronze_weather")

    cities_logged = [r["city"] for r in city_records]
    print(f"[weather] cities: {cities_logged}")

    return {
        "statusCode": 200,
        "cities_sent": sent,
        "run_id": run_id,
    }