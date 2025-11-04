# view_features_auto.py
import pandas as pd
from feast import FeatureStore
from datetime import datetime, timezone
import psycopg2

# Initialize Feast FeatureStore
store = FeatureStore(repo_path=".")

# -----------------------------
# 1️⃣ Materialize all existing data to online store
# -----------------------------
start_date = datetime(2025, 10, 1, tzinfo=timezone.utc)  # earliest date in your table
end_date = datetime.now(timezone.utc)

print("---- MATERIALIZING FEATURES TO ONLINE STORE ----")
store.materialize(start_date=start_date, end_date=end_date)
print(f"✅ Materialization completed from {start_date} to {end_date}")

# -----------------------------
# 2️⃣ Fetch last N rows (id + timestamp) from PostgreSQL for offline features
# -----------------------------
conn = psycopg2.connect(
    dbname="aqi_feature_store",
    user="postgres",
    password="123",
    host="localhost",
    port="5432"
)
query = "SELECT id, time FROM aqi_data ORDER BY time DESC LIMIT 5;"  # last 5 rows
ids_df = pd.read_sql(query, conn)
conn.close()

# Rename timestamp column for Feast
ids_df = ids_df.rename(columns={"time": "event_timestamp"})

# -----------------------------
# 3️⃣ Define feature list
# -----------------------------
feature_list = [f"aqi_features:{f}" for f in [
    "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone",
    "temperature_2m", "relative_humidity_2m", "wind_speed_10m", "pressure_msl",
    "precipitation", "cloudcover", "day_of_week", "month", "log_pm10", "log_pm2_5",
    "log_carbon_monoxide", "log_nitrogen_dioxide", "log_sulphur_dioxide",
    "log_wind_speed_10m", "log_cloudcover", "aqi", "hour", "day",
    "aqi_change_rate", "aqi_rolling_mean_3hr", "aqi_rolling_mean_6hr",
    "pm2_5_rolling_mean_3hr", "pm10_rolling_mean_3hr", "temp_wind", "humidity_pressure"
]]

# -----------------------------
# 4️⃣ Get OFFLINE (historical) features
# -----------------------------
print("\n---- OFFLINE DATA ----")
offline_df = store.get_historical_features(
    entity_df=ids_df,
    features=feature_list
).to_df()
print(offline_df)

# -----------------------------
# 5️⃣ Get ONLINE (current) features
# -----------------------------
print("\n---- ONLINE DATA ----")
online_df = store.get_online_features(
    features=feature_list,
    entity_rows=[{"id": row} for row in ids_df["id"].tolist()]
).to_df()
print(online_df)
