import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

# Load .env variables
load_dotenv()

# Load cleaned feature data
df = pd.read_csv("data/aqi_feature_set_v1.csv")

# Connect to PostgreSQL using env vars
conn = psycopg2.connect(
    dbname="aqi_feature_store",
    user="postgres",
    password="123",      # change to your password
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# 1️⃣ Create table if not exists
cur.execute("""
CREATE TABLE IF NOT EXISTS aqi_data (
    id SERIAL PRIMARY KEY,
    time TIMESTAMP UNIQUE,
    pm10 FLOAT,
    pm2_5 FLOAT,
    carbon_monoxide FLOAT,
    nitrogen_dioxide FLOAT,
    sulphur_dioxide FLOAT,
    ozone FLOAT,
    temperature_2m FLOAT,
    relative_humidity_2m FLOAT,
    wind_speed_10m FLOAT,
    pressure_msl FLOAT,
    precipitation FLOAT,
    cloudcover FLOAT,
    day_of_week VARCHAR(15),
    month INT,
    log_pm10 FLOAT,
    log_pm2_5 FLOAT,
    log_carbon_monoxide FLOAT,
    log_nitrogen_dioxide FLOAT,
    log_sulphur_dioxide FLOAT,
    log_wind_speed_10m FLOAT,
    log_cloudcover FLOAT,
    AQI FLOAT,
    hour INT,
    day INT,
    AQI_change_rate FLOAT,
    AQI_rolling_mean_3hr FLOAT,
    AQI_rolling_mean_6hr FLOAT,
    PM2_5_rolling_mean_3hr FLOAT,
    PM10_rolling_mean_3hr FLOAT,
    temp_wind FLOAT,
    humidity_pressure FLOAT
);
""")
conn.commit()

# 2️⃣ Insert data, skipping duplicates
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO aqi_data (
            time, pm10, pm2_5, carbon_monoxide, nitrogen_dioxide, sulphur_dioxide, ozone,
            temperature_2m, relative_humidity_2m, wind_speed_10m, pressure_msl, precipitation,
            cloudcover, day_of_week, month, log_pm10, log_pm2_5, log_carbon_monoxide,
            log_nitrogen_dioxide, log_sulphur_dioxide, log_wind_speed_10m, log_cloudcover,
            AQI, hour, day, AQI_change_rate, AQI_rolling_mean_3hr, AQI_rolling_mean_6hr,
            PM2_5_rolling_mean_3hr, PM10_rolling_mean_3hr, temp_wind, humidity_pressure
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (time) DO NOTHING;
    """, tuple(row))

conn.commit()
cur.close()
conn.close()

print("✅ New data inserted successfully — duplicates skipped.")
