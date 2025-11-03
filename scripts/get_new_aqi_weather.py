# fetch_incremental_data.py
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
import time

# -----------------------------
# Setup
# -----------------------------
load_dotenv()
API_KEY = os.getenv("API_KEY")  # OpenWeatherMap API key
LAT = float(os.getenv("LAT", 33.6007))
LON = float(os.getenv("LON", 73.0679))
FILE_PATH = "data/realtime_data.csv"
os.makedirs("data", exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# -----------------------------
# Helper: Safe float conversion
# -----------------------------
def to_float_safe(val):
    try:
        return float(val)
    except:
        return None

# -----------------------------
# Fetch AQI
# -----------------------------
def fetch_aqi(timestamp):
    unix_time = int(timestamp.timestamp())
    url = (
        f"http://api.openweathermap.org/data/2.5/air_pollution/history?"
        f"lat={LAT}&lon={LON}&start={unix_time}&end={unix_time+3600}&appid={API_KEY}"
    )
    try:
        r = requests.get(url, timeout=20)
        if r.status_code == 200:
            data = r.json().get("list", [])
            if data:
                c = data[0]["components"]
                return {
                    "time": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    "pm10": to_float_safe(c.get("pm10")),
                    "pm2_5": to_float_safe(c.get("pm2_5")),
                    "carbon_monoxide": to_float_safe(c.get("co")),
                    "nitrogen_dioxide": to_float_safe(c.get("no2")),
                    "sulphur_dioxide": to_float_safe(c.get("so2")),
                    "ozone": to_float_safe(c.get("o3")),
                }
    except Exception as e:
        logging.warning(f"⚠️ AQI fetch error at {timestamp}: {e}")

    # If failed, return empty row
    return {
        "time": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "pm10": None, "pm2_5": None, "carbon_monoxide": None,
        "nitrogen_dioxide": None, "sulphur_dioxide": None, "ozone": None,
    }

# -----------------------------
# Fetch Weather
# -----------------------------
def fetch_weather(timestamp):
    date_str = timestamp.strftime("%Y-%m-%d")
    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={LAT}&longitude={LON}"
        f"&start_date={date_str}&end_date={date_str}"
        f"&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,"
        f"pressure_msl,precipitation,cloudcover"
        f"&timezone=auto"
    )
    try:
        w = requests.get(url, timeout=30).json()
        if "hourly" in w:
            times = w["hourly"]["time"]
            hour_index = next(
                (i for i, t in enumerate(times) if t.startswith(timestamp.strftime("%Y-%m-%dT%H"))),
                None
            )
            if hour_index is not None:
                return {
                    "time": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    "temperature_2m": to_float_safe(w["hourly"]["temperature_2m"][hour_index]),
                    "relative_humidity_2m": to_float_safe(w["hourly"]["relative_humidity_2m"][hour_index]),
                    "wind_speed_10m": to_float_safe(w["hourly"]["wind_speed_10m"][hour_index]),
                    "pressure_msl": to_float_safe(w["hourly"]["pressure_msl"][hour_index]),
                    "precipitation": to_float_safe(w["hourly"]["precipitation"][hour_index]),
                    "cloudcover": to_float_safe(w["hourly"]["cloudcover"][hour_index]),
                }
    except Exception as e:
        logging.warning(f"⚠️ Weather fetch error at {timestamp}: {e}")

    return {
        "time": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "temperature_2m": None, "relative_humidity_2m": None,
        "wind_speed_10m": None, "pressure_msl": None,
        "precipitation": None, "cloudcover": None
    }

# -----------------------------
# Merge AQI & Weather
# -----------------------------
def merge_and_add_features(aqi_data, weather_data):
    merged = {**aqi_data, **{k:v for k,v in weather_data.items() if k!="time"}}
    timestamp = datetime.strptime(aqi_data["time"], "%Y-%m-%d %H:%M:%S")
    merged["day_of_week"] = timestamp.strftime("%A")
    merged["month"] = timestamp.strftime("%B")
    return merged

# -----------------------------
# Fetch new data incrementally
# -----------------------------
def fetch_incremental_data():
    # Read last timestamp
    if os.path.exists(FILE_PATH):
        df_existing = pd.read_csv(FILE_PATH)
        df_existing['time'] = pd.to_datetime(df_existing['time'])
        last_time = df_existing['time'].max()
    else:
        # If no file, start from a default date
        last_time = datetime.strptime("2025-10-12 01:00", "%Y-%m-%d %H:%M")

    logging.info(f"Last timestamp: {last_time}")
    current_time = last_time + timedelta(hours=1)
    end_time = datetime.now()
    new_rows = []

    while current_time <= end_time:
        aqi_data = fetch_aqi(current_time)
        weather_data = fetch_weather(current_time)
        merged = merge_and_add_features(aqi_data, weather_data)
        new_rows.append(merged)
        logging.info(f"Fetched data for {current_time}")
        current_time += timedelta(hours=1)
        time.sleep(1)  # avoid hitting API too fast

    if new_rows:
        df_new = pd.DataFrame(new_rows)
        # Append to CSV
        df_new.to_csv(FILE_PATH, mode="a", header=not os.path.exists(FILE_PATH), index=False)
        logging.info(f"✅ Saved {len(df_new)} new rows to {FILE_PATH}")
        return df_new
    else:
        logging.info("No new data to fetch.")
        return pd.DataFrame()

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    fetch_incremental_data()
