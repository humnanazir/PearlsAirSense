from flask import Flask, render_template, jsonify
import pandas as pd
import joblib
import numpy as np

app = Flask(__name__)

# Load model
model_path = "models/aqi_rf_model.pkl"
model = joblib.load(model_path)

# Utility: get latest row
def get_latest():
    df = pd.read_csv("data/aqi_feature_set_v1.csv")
    df.columns = [c.lower() for c in df.columns]
    return df.iloc[-1]

# Home page
@app.route('/')
def home():
    latest = get_latest()
    return render_template('index.html', latest=latest)

# Past 24-hour AQI for chart
@app.route('/past24')
def past24():
    df = pd.read_csv("data/aqi_feature_set_v1.csv")
    df.columns = [c.lower() for c in df.columns]
    last24 = df.tail(24)
    data = {
        "time": last24["time"].tolist(),
        "aqi": last24["aqi"].astype(float).tolist()
    }
    return jsonify(data)

# Latest live metrics
@app.route('/latest')
def latest():
    latest_row = get_latest()
    data = {
        "aqi": float(latest_row["aqi"]),
        "pm10": float(latest_row["pm10"]),
        "pm25": float(latest_row["pm2_5"]),
        "temp": float(latest_row["temperature_2m"]),
        "humidity": float(latest_row["relative_humidity_2m"]),
        "wind": float(latest_row["wind_speed_10m"])
    }
    return jsonify(data)


@app.route('/stations')
def stations():
    # Read CSV (make sure it has 'station', 'lat', 'lon', 'aqi' columns)
    df = pd.read_csv("data/aqi_feature_set_v1.csv")
    df.columns = [c.lower() for c in df.columns]
    
    # Filter only Islamabad stations (assuming a 'city' column)
    if 'city' in df.columns:
        df_isb = df[df['city'].str.lower() == 'islamabad']
    else:
        # If no city column, just take latest values and assign known Islamabad coords
        # Approximate coordinates for 3 monitoring stations
        stations_coords = {
            "Station I-8": (33.6844, 73.0479),
            "Station G-9": (33.7100, 73.0600),
            "Station F-6": (33.6960, 73.0470)
        }
        latest = df.groupby('station').tail(1) if 'station' in df.columns else df.tail(1)
        station_list = []
        for i, row in latest.iterrows():
            name = row.get('station', f"Station {i+1}")
            aqi = float(row['aqi'])
            lat, lon = stations_coords.get(name, (33.6844, 73.0479))
            station_list.append({"name": name, "lat": lat, "lon": lon, "aqi": aqi})
        return jsonify(station_list)

    # For city-based data
    station_list = []
    for i, row in df_isb.groupby('station').tail(1).iterrows():
        station_list.append({
            "name": row['station'],
            "lat": float(row['lat']),
            "lon": float(row['lon']),
            "aqi": float(row['aqi'])
        })
    return jsonify(station_list)

@app.route('/forecast')
def forecast():
    df = pd.read_csv("data/aqi_feature_set_v1.csv")
    df.columns = [c.lower() for c in df.columns]
    last24 = df.tail(24)

    # Linear trend: (last - first) / 24 hours
    aqi_trend = (last24['aqi'].iloc[-1] - last24['aqi'].iloc[0]) / 24
    pm25_trend = (last24['pm2_5'].iloc[-1] - last24['pm2_5'].iloc[0]) / 24
    pm10_trend = (last24['pm10'].iloc[-1] - last24['pm10'].iloc[0]) / 24

    last_aqi = last24['aqi'].iloc[-1]
    last_pm25 = last24['pm2_5'].iloc[-1]
    last_pm10 = last24['pm10'].iloc[-1]

    forecast_aqi = []
    forecast_pm25 = []
    forecast_pm10 = []

    for day in range(1, 4):  # next 3 days
        forecast_aqi.append(round(last_aqi + aqi_trend * 24 * day, 1))
        forecast_pm25.append(round(last_pm25 + pm25_trend * 24 * day, 1))
        forecast_pm10.append(round(last_pm10 + pm10_trend * 24 * day, 1))

    data = {
        "aqi": forecast_aqi,
        "pm25": forecast_pm25,
        "pm10": forecast_pm10
    }
    return jsonify(data)

# 
if __name__ == "__main__":
    app.run(debug=True)

