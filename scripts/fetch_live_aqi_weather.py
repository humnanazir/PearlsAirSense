import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging

# -----------------------------
# Setup
# -----------------------------
load_dotenv()
API_KEY = os.getenv("API_KEY")  # OpenWeatherMap API key
LAT = float(os.getenv("LAT", 33.6007))
LON = float(os.getenv("LON", 73.0679))

BACKFILL_START = "2025-10-12 01:00"
BACKFILL_END = "2025-10-26"
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

    return {
        "time": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "pm10": "",
        "pm2_5": "",
        "carbon_monoxide": "",
        "nitrogen_dioxide": "",
        "sulphur_dioxide": "",
        "ozone": "",
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
        "temperature_2m": "",
        "relative_humidity_2m": "",
        "wind_speed_10m": "",
        "pressure_msl": "",
        "precipitation": "",
        "cloudcover": "",
    }

# -----------------------------
# Save merged data
# -----------------------------
def save_to_csv(data):
    df = pd.DataFrame([data])
    df.to_csv(FILE_PATH, mode="a", header=not os.path.exists(FILE_PATH), index=False)

# -----------------------------
# Merge and add features
# -----------------------------
def merge_and_enhance(aqi_data, weather_data):
    merged = {**aqi_data, **{k: v for k, v in weather_data.items() if k != "time"}}
    timestamp = datetime.strptime(aqi_data["time"], "%Y-%m-%d %H:%M:%S")
    merged["day_of_week"] = timestamp.strftime("%A")
    merged["month"] = timestamp.strftime("%B")
    return merged

# -----------------------------
# Backfill historical data
# -----------------------------
def run_backfill(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        aqi_data = fetch_aqi(current_date)
        weather_data = fetch_weather(current_date)
        merged = merge_and_enhance(aqi_data, weather_data)
        save_to_csv(merged)
        logging.info(f"✅ Backfilled {current_date}")
        current_date += timedelta(hours=1)
        time.sleep(1)

# -----------------------------
# Fetch live data
# -----------------------------
def fetch_live_data():
    now = datetime.now()
    aqi_data = fetch_aqi(now)
    weather_data = fetch_weather(now)
    merged = merge_and_enhance(aqi_data, weather_data)
    save_to_csv(merged)
    logging.info("✅ Live data fetched and saved!")

# -----------------------------
# Main Entry
# -----------------------------
if __name__ == "__main__":
    start_date = datetime.strptime(BACKFILL_START, "%Y-%m-%d %H:%M")
    end_date = datetime.strptime(BACKFILL_END, "%Y-%m-%d")
    
    # ✅ Historical backfill
    run_backfill(start_date, end_date)
    
    # ✅ Live data (future CI/CD triggers hourly/daily)
    fetch_live_data()
