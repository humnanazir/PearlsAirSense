# aqi_features.py (Feast + PostgreSQL offline + online for Neon)

import os
from datetime import datetime, timezone
from feast import Entity, FeatureView, Field, FeatureStore, ValueType
from feast.types import Float32, Int64, String
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres import PostgreSQLSource
import psycopg2 


# -----------------------------
# Load environment variables
# -----------------------------


# -----------------------------
# 1️⃣ Connect to PostgreSQL to read table columns
# -----------------------------
conn = psycopg2.connect(
    dbname="aqi_feature_store",
    user="postgres",
    password="123",      # change to your password
    host="localhost",
    port="5432"
)

cur = conn.cursor()
cur.execute(
    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='aqi_data'"
)
columns = cur.fetchall()
conn.close()

# -----------------------------
# 2️⃣ Define entity and timestamp
# -----------------------------
entity_name = "id"       # unique row ID
timestamp_name = "time"  # timestamp column

# -----------------------------
# 3️⃣ Prepare Feast Fields dynamically
# -----------------------------
feature_fields = []
for col_name, data_type in columns:
    if col_name in [entity_name, timestamp_name]:
        continue
    if "int" in data_type:
        dtype = Int64
    elif "char" in data_type or "text" in data_type:
        dtype = String
    else:
        dtype = Float32
    feature_fields.append(Field(name=col_name, dtype=dtype))

# -----------------------------
# 4️⃣ Define PostgreSQLSource for Feast
# -----------------------------
aqi_source = PostgreSQLSource(
    table="aqi_data",        # your table name
    timestamp_field=timestamp_name
)

# -----------------------------
# 5️⃣ Define Entity
# -----------------------------
location = Entity(
    name=entity_name,
    value_type=ValueType.INT64,
    description="Unique row ID for AQI data"
)

# -----------------------------
# 6️⃣ Define FeatureView
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
# 7️⃣ Register and materialize features
# -----------------------------
if __name__ == "__main__":
    # Automatically detect feature_store.yaml location relative to this script
    repo_path = os.path.dirname(os.path.abspath(__file__))

    # Initialize the FeatureStore safely
    store = FeatureStore(repo_path=repo_path)

    # Register Entity & FeatureView
    store.apply([location, aqi_features])
    print("✅ Feast FeatureView and Entity registered successfully!")

    # Materialize features for the last 1 day
    utc_now = datetime.now(timezone.utc)
    store.materialize_incremental(end_date=utc_now)
    print(f"✅ Materialization completed at UTC time: {utc_now}")
