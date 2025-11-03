# aqi_features.py (Feast + PostgreSQL offline + online for Neon)
import os
from datetime import datetime, timezone
from feast import Entity, FeatureView, Field, FeatureStore, ValueType
from feast.types import Float32, Int64, String
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres import PostgreSQLSource

# -----------------------------
# 1️⃣ Define entity and timestamp
# -----------------------------
entity_name = "id"
timestamp_name = "time"

# -----------------------------
# 2️⃣ Define PostgreSQLSource for Feast
# -----------------------------
aqi_source = PostgreSQLSource(
    table="aqi_data",
    timestamp_field=timestamp_name
)

# -----------------------------
# 3️⃣ Prepare Fields for all your columns
# -----------------------------
feature_fields = [
    Field(name="pm10", dtype=Float32),
    Field(name="pm2_5", dtype=Float32),
    Field(name="carbon_monoxide", dtype=Float32),
    Field(name="nitrogen_dioxide", dtype=Float32),
    Field(name="sulphur_dioxide", dtype=Float32),
    Field(name="ozone", dtype=Float32),
    Field(name="temperature_2m", dtype=Float32),
    Field(name="relative_humidity_2m", dtype=Float32),
    Field(name="wind_speed_10m", dtype=Float32),
    Field(name="pressure_msl", dtype=Float32),
    Field(name="precipitation", dtype=Float32),
    Field(name="cloudcover", dtype=Float32),
    Field(name="day_of_week", dtype=Int64),
    Field(name="month", dtype=Int64),
    Field(name="log_pm10", dtype=Float32),
    Field(name="log_pm2_5", dtype=Float32),
    Field(name="log_carbon_monoxide", dtype=Float32),
    Field(name="log_nitrogen_dioxide", dtype=Float32),
    Field(name="log_sulphur_dioxide", dtype=Float32),
    Field(name="log_wind_speed_10m", dtype=Float32),
    Field(name="log_cloudcover", dtype=Float32),
    Field(name="AQI", dtype=Float32),
    Field(name="hour", dtype=Int64),
    Field(name="day", dtype=Int64),
    Field(name="AQI_change_rate", dtype=Float32),
    Field(name="AQI_rolling_mean_3hr", dtype=Float32),
    Field(name="AQI_rolling_mean_6hr", dtype=Float32),
    Field(name="PM2_5_rolling_mean_3hr", dtype=Float32),
    Field(name="PM10_rolling_mean_3hr", dtype=Float32),
    Field(name="temp_wind", dtype=Float32),
    Field(name="humidity_pressure", dtype=Float32)
]

# -----------------------------
# 4️⃣ Define Entity
# -----------------------------
location = Entity(
    name=entity_name,
    value_type=ValueType.INT64,
    description="Unique row ID for AQI data"
)

# -----------------------------
# 5️⃣ Define FeatureView
# -----------------------------
aqi_features = FeatureView(
    name="aqi_features",
    entities=[location],
    ttl=None,
    schema=feature_fields,
    source=aqi_source,
    description="Dynamic feature view for AQI prediction"
)

# -----------------------------
# 6️⃣ Register and materialize features
# -----------------------------
if __name__ == "__main__":
    repo_path = os.path.dirname(os.path.abspath(__file__))
    store = FeatureStore(repo_path=repo_path)
    store.apply([location, aqi_features])
    print("✅ Feast FeatureView and Entity registered successfully!")

    utc_now = datetime.now(timezone.utc)
    store.materialize_incremental(end_date=utc_now)
    print(f"✅ Materialization completed at UTC time: {utc_now}")
