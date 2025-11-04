# scripts/fetch_historical_weather.py
import os, requests
import pandas as pd
from datetime import datetime
from time import sleep

# ✅ Rawalpindi coordinates
LAT, LON = 33.6844, 73.0479

# ---------- Configure date range ----------
# Past 1 year (Oct 1, 2024 – Oct 11, 2025)
START_DATE = "2024-10-01"
END_DATE = "2025-10-11"

# ---------- API URL ----------
url = (
    f"https://archive-api.open-meteo.com/v1/archive?"
    f"latitude={LAT}&longitude={LON}"
    f"&start_date={START_DATE}&end_date={END_DATE}"
    f"&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,"
    f"pressure_msl,precipitation,cloudcover"
    f"&timezone=auto"
)

print("Fetching weather data for Rawalpindi...")
try:
    response = requests.get(url, timeout=60)
    if response.status_code == 200:
        data = response.json()
        hourly = data.get("hourly", {})

        df = pd.DataFrame({
            "time": hourly.get("time", []),
            "temperature_2m": hourly.get("temperature_2m", []),
            "relative_humidity_2m": hourly.get("relative_humidity_2m", []),
            "wind_speed_10m": hourly.get("wind_speed_10m", []),
            "pressure_msl": hourly.get("pressure_msl", []),
            "precipitation": hourly.get("precipitation", []),
            "cloudcover": hourly.get("cloudcover", []),
        })

        # ---------- Add day_of_week and month ----------
        df["time"] = pd.to_datetime(df["time"])
        df["day_of_week"] = df["time"].dt.day_name()
        df["month"] = df["time"].dt.month_name()

        # ---------- Save to CSV ----------
        os.makedirs("data", exist_ok=True)
        output_path = os.path.join("data", "historical_weather_data.csv")
        df.to_csv(output_path, index=False)
        print(f"✅ Saved: {output_path} | Rows: {len(df)}")
    else:
        print(f"⚠️ Warning: status {response.status_code} - {response.text[:200]}")
except Exception as e:
    print("⚠️ Exception:", e)

sleep(1)
