import pandas as pd
import numpy as np
from datetime import datetime
import logging
import os

# Setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

FILE_PATH = "data/realtime_data.csv"
OUTPUT_PATH = "data/aqi_feature_set_v1.csv"
os.makedirs("data", exist_ok=True)

# Load data
df = pd.read_csv(FILE_PATH)
logging.info(f"✅ Loaded data | Shape: {df.shape}")



# Handle missing values
cols_medium_missing = ['pm10', 'pm2_5', 'carbon_monoxide', 'nitrogen_dioxide',
                       'sulphur_dioxide', 'ozone']
for col in cols_medium_missing:
    df[col] = df[col].interpolate(method='linear')
    if pd.isna(df[col].iloc[0]):
        df[col].iloc[0] = df[col].median()
    if pd.isna(df[col].iloc[-1]):
        df[col].iloc[-1] = df[col].median()

cols_numeric_min_missing = ['temperature_2m', 'relative_humidity_2m', 'wind_speed_10m',
                            'pressure_msl', 'precipitation', 'cloudcover']
for col in cols_numeric_min_missing:
    df[col] = df[col].fillna(df[col].median())

# Cap outliers (IQR)
numeric_cols = ['pm10', 'pm2_5', 'carbon_monoxide', 'nitrogen_dioxide', 'sulphur_dioxide', 'ozone',
                'temperature_2m', 'relative_humidity_2m', 'wind_speed_10m',
                'pressure_msl', 'precipitation', 'cloudcover']
for col in numeric_cols:
    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1
    df[col] = df[col].clip(Q1 - 1.5 * IQR, Q3 + 1.5 * IQR)

# Log transform
# Log transform for skewed columns and round to 2 decimals
skewed_cols = ['pm10', 'pm2_5', 'carbon_monoxide', 'nitrogen_dioxide', 'sulphur_dioxide', 'wind_speed_10m', 'cloudcover']

for col in skewed_cols:
    df['log_' + col] = np.log1p(df[col]).round(2)


# Datetime conversion and sorting
df['time'] = pd.to_datetime(df['time'])
df = df.sort_values('time').reset_index(drop=True)

# Time-based features
df['hour'] = df['time'].dt.hour
df['day'] = df['time'].dt.day
df['month'] = df['time'].dt.month
df['day_of_week'] = df['time'].dt.day_name()

# AQI Calculation
def calculate_aqi(pm25, pm10):
    def aqi_subindex(Cp, Bp_lo, Bp_hi, I_lo, I_hi):
        return ((I_hi - I_lo) / (Bp_hi - Bp_lo)) * (Cp - Bp_lo) + I_lo

    pm25_breakpoints = [
        (0.0, 12.0, 0, 50), (12.1, 35.4, 51, 100), (35.5, 55.4, 101, 150),
        (55.5, 150.4, 151, 200), (150.5, 250.4, 201, 300), (250.5, 500.4, 301, 500)
    ]
    pm10_breakpoints = [
        (0, 54, 0, 50), (55, 154, 51, 100), (155, 254, 101, 150),
        (255, 354, 151, 200), (355, 424, 201, 300), (425, 604, 301, 500)
    ]

    aqi_pm25 = next((aqi_subindex(pm25, Bp_lo, Bp_hi, I_lo, I_hi)
                     for Bp_lo, Bp_hi, I_lo, I_hi in pm25_breakpoints
                     if Bp_lo <= pm25 <= Bp_hi), None)
    aqi_pm10 = next((aqi_subindex(pm10, Bp_lo, Bp_hi, I_lo, I_hi)
                     for Bp_lo, Bp_hi, I_lo, I_hi in pm10_breakpoints
                     if Bp_lo <= pm10 <= Bp_hi), None)

    if aqi_pm25 is not None and aqi_pm10 is not None:
        return max(aqi_pm25, aqi_pm10)
    elif aqi_pm25 is not None:
        return aqi_pm25
    elif aqi_pm10 is not None:
        return aqi_pm10
    else:
        return None

df['AQI'] = df.apply(lambda row: calculate_aqi(row['pm2_5'], row['pm10']), axis=1)

# AQI change rate
df['AQI_change_rate'] = df['AQI'].diff().fillna(0)

# Rolling averages
df['AQI_rolling_mean_3hr'] = df['AQI'].rolling(window=3, min_periods=1).mean()
df['AQI_rolling_mean_6hr'] = df['AQI'].rolling(window=6, min_periods=1).mean()
df['PM2_5_rolling_mean_3hr'] = df['pm2_5'].rolling(window=3, min_periods=1).mean()
df['PM10_rolling_mean_3hr'] = df['pm10'].rolling(window=3, min_periods=1).mean()

# Weather interaction features
df['temp_wind'] = df['temperature_2m'] * df['wind_speed_10m']
df['humidity_pressure'] = df['relative_humidity_2m'] / df['pressure_msl']

# Round numeric columns
round_cols = ['AQI', 'AQI_change_rate', 'AQI_rolling_mean_3hr', 'AQI_rolling_mean_6hr',
              'PM2_5_rolling_mean_3hr', 'PM10_rolling_mean_3hr',
              'temp_wind', 'humidity_pressure']
df[round_cols] = df[round_cols].round(3)

round_cols = ['pm10', 'pm2_5', 'carbon_monoxide', 'nitrogen_dioxide', 'sulphur_dioxide', 'ozone']
df[round_cols] = df[round_cols].round(2)

# ✅ Append only new timestamps to output file
if os.path.exists(OUTPUT_PATH):
    existing_df = pd.read_csv(OUTPUT_PATH)
    existing_df['time'] = pd.to_datetime(existing_df['time'])

    # Filter only new rows (not already in existing file)
    new_rows = df[~df['time'].isin(existing_df['time'])]
    if not new_rows.empty:
        combined_df = pd.concat([existing_df, new_rows]).sort_values('time').reset_index(drop=True)
        combined_df.to_csv(OUTPUT_PATH, index=False)
        logging.info(f"✅ Appended {len(new_rows)} new rows to {OUTPUT_PATH}")
    else:
        logging.info("ℹ️ No new timestamps found. File not updated.")
else:
    df.to_csv(OUTPUT_PATH, index=False)
    logging.info(f"✅ Created new file at {OUTPUT_PATH}")
