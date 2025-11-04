# scripts/fetch_historical_aqi.py
import os, time, requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables from .env file
load_dotenv()
API_KEY = os.getenv("API_KEY")

# ✅ Rawalpindi coordinates
LAT = float(os.getenv("LAT", 33.6007))
LON = float(os.getenv("LON", 73.0679))

# ---------- Configure date range ----------
# Fetch data for past 1 year (Oct 1, 2024 – Oct 11, 2025)
start_date = datetime(2024, 10, 1)
end_date = datetime(2025, 10, 11)

# ---------- Helper to convert to UNIX timestamps ----------
def to_unix(dt):
    return int(dt.replace(tzinfo=None).timestamp())

# ---------- Fetch loop ----------
all_rows = []
cur = start_date
while cur <= end_date:
    start_unix = to_unix(cur)
    end_unix = to_unix(cur + timedelta(days=1))

    url = (
        f"http://api.openweathermap.org/data/2.5/air_pollution/history"
        f"?lat={LAT}&lon={LON}&start={start_unix}&end={end_unix}&appid={API_KEY}"
    )

    print("Fetching:", cur.date())
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            payload = r.json().get("list", [])
            for rec in payload:
                dt = datetime.fromtimestamp(rec["dt"])
                comps = rec.get("components", {})
                all_rows.append({
                    "time": dt.isoformat(sep=' '),
                    "pm10": comps.get("pm10"),
                    "pm2_5": comps.get("pm2_5"),
                    "carbon_monoxide": comps.get("co"),
                    "carbon_dioxide": comps.get("co2", None),  # Not provided directly by API
                    "nitrogen_dioxide": comps.get("no2"),
                    "sulphur_dioxide": comps.get("so2"),
                    "ozone": comps.get("o3")
                })
        else:
            print(f"  ⚠️ Warning: status {r.status_code} for {cur.date()} - {r.text[:200]}")
    except Exception as e:
        print("  ⚠️ Exception:", e)

    time.sleep(1)
    cur += timedelta(days=1)

# ---------- Save to CSV ----------
df = pd.DataFrame(all_rows)

# Ensure only required columns (for safety)
df = df[["time", "pm10", "pm2_5", "carbon_monoxide",
         "carbon_dioxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone"]]

os.makedirs("data", exist_ok=True)
output_path = os.path.join("data", "historical_raw_data.csv")
df.to_csv(output_path, index=False)
print("✅ Saved:", output_path, "| Rows:", len(df))
