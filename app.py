from flask import Flask, render_template, jsonify
import pandas as pd
import joblib
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io, base64
import shap
import xgboost as xgb
import matplotlib.dates as mdates
import matplotlib
matplotlib.use('Agg')  # <-- Add this before pyplot

app = Flask(__name__)

# Load model
model_path = "models/aqi_rf_model.pkl"
model = joblib.load(model_path)

# Utility: get latest row
def get_latest():
    df = pd.read_csv("data/aqi_feature_set_v1.csv")
    df.columns = [c.lower() for c in df.columns]
    return df.iloc[-1]

@app.route('/')
def home():
    latest = get_latest()

    # --- EDA plot ---
    df = pd.read_csv("data/aqi_feature_set_v1.csv")
    df.columns = [c.lower() for c in df.columns]

    plt.figure(figsize=(12,6))
    sns.lineplot(x='time', y='aqi', data=df.tail(50), marker='o', color='green')
    sns.scatterplot(x='time', y='aqi', data=df.tail(50), color='red', s=50)
    plt.title('AQI Trend (Last 50 Records)')
    plt.xlabel('Time')
    plt.ylabel('AQI')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
    
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    eda_plot_url = base64.b64encode(img.getvalue()).decode()
    plt.close()

    # --- Feature importance (optional) ---
    X = df[['pm10', 'pm2_5', 'temperature_2m', 'relative_humidity_2m', 'wind_speed_10m']].dropna()
    y = df['aqi'].iloc[:len(X)]
    model_xgb = xgb.XGBRegressor().fit(X, y)
    explainer = shap.Explainer(model_xgb, X)
    shap_values = explainer(X)
    shap.summary_plot(shap_values, X, show=False)
    img2 = io.BytesIO()
    plt.savefig(img2, format='png', bbox_inches='tight')
    img2.seek(0)
    fi_plot_url = base64.b64encode(img2.getvalue()).decode()
    plt.close()

    return render_template(
        'index.html',
        latest=latest,
        eda_plot_url=eda_plot_url,
        fi_plot_url=fi_plot_url
    )


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

@app.route('/latest')
def latest_basic():
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
    print("🚀 /forecast route called!")

    df = pd.read_csv("data/aqi_feature_set_v1.csv")
    df.columns = [c.lower() for c in df.columns]

    
    model_features = getattr(model, "feature_names_in_", None)
    if model_features is not None:
        features = [f.lower() for f in model_features]
    else:
        features = ['pm10', 'pm2_5', 'temperature_2m', 'relative_humidity_2m', 'wind_speed_10m']

   
    df = df.dropna(subset=features)
    if df.empty:
        return jsonify({"error": "No valid rows for prediction"})

    
    latest = df.iloc[-1]
    X_latest = latest[features].values.reshape(1, -1)
    print("🧾 Model input:", X_latest)

    # Predict AQI for 3 future days using Random Forest
    preds = []
    for _ in range(3):
        val = model.predict(X_latest)[0]  # ✅ Random Forest model
        preds.append(round(float(val), 2))
        X_latest = X_latest + np.random.normal(0, 0.02, size=X_latest.shape)

    print("✅ Forecast AQI:", preds)

   
    return jsonify({
        "aqi": preds,
        "pm25": [
            round(latest['pm2_5'] * 1.02, 2),
            round(latest['pm2_5'] * 1.04, 2),
            round(latest['pm2_5'] * 1.06, 2)
        ],
        "pm10": [
            round(latest['pm10'] * 1.01, 2),
            round(latest['pm10'] * 1.02, 2),
            round(latest['pm10'] * 1.03, 2)
        ]
    })




# EDA route
@app.route('/eda')
def eda():
    # Read data
    df = pd.read_csv("data/aqi_feature_set_v1.csv")
    df.columns = [c.lower() for c in df.columns]

    # Plot AQI trend for last 100 records
    plt.figure(figsize=(10, 5))
    sns.lineplot(x='time', y='aqi', data=df.tail(100))
    plt.title('AQI Trend Over Time (Last 50 Records)')
    plt.xlabel('Time')
    plt.ylabel('AQI')
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Save plot to PNG in memory
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    plot_url = base64.b64encode(img.getvalue()).decode()
    plt.close()

    # Render in HTML template
    return render_template('eda.html', plot_url=plot_url)


# 
if __name__ == "__main__":
    app.run(debug=True)
