import pandas as pd
import numpy as np
from feast import FeatureStore
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import joblib
import mlflow
import mlflow.sklearn
import os

# -----------------------------
# 🧭 Set MLflow tracking to local mlruns folder
# -----------------------------

mlflow.set_tracking_uri("http://127.0.0.1:5000")# Remove mlflow.set_registry_uri

# -----------------------------
# 1️⃣ Connect to Feast Feature Store
# -----------------------------
store = FeatureStore(repo_path="/Users/macbook-air/Desktop/pearls_aqi_predictor/aqi_feature_store/feature_repo")

# -----------------------------
# 2️⃣ Load target (AQI) from CSV
# -----------------------------
target_csv = "data/aqi_feature_set_v1.csv"
target_df = pd.read_csv(target_csv)

if "time" not in target_df.columns:
    raise KeyError("'time' column not found in the CSV file. Please check your dataset.")

target_df["event_timestamp"] = pd.to_datetime(target_df["time"])
target_df["id"] = range(1, len(target_df) + 1)
target_df = target_df[["event_timestamp", "id", "AQI"]]

# -----------------------------
# 3️⃣ Create entity dataframe for Feast
# -----------------------------
entity_df = target_df[["event_timestamp", "id"]]

# -----------------------------
# 4️⃣ Define feature list
# -----------------------------
feature_list = [
    "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide",
    "ozone", "temperature_2m", "relative_humidity_2m", "wind_speed_10m",
    "pressure_msl", "precipitation", "cloudcover", "month", "log_pm10",
    "log_pm2_5", "log_carbon_monoxide", "log_nitrogen_dioxide", "log_sulphur_dioxide",
    "log_wind_speed_10m", "log_cloudcover", "hour", "day",
    "aqi_change_rate", "aqi_rolling_mean_3hr", "aqi_rolling_mean_6hr",
    "pm2_5_rolling_mean_3hr", "pm10_rolling_mean_3hr", "temp_wind", "humidity_pressure",
    "day_of_week"
]
feature_list = [f"aqi_features:{f}" for f in feature_list]

# -----------------------------
# 5️⃣ Fetch historical features from Feast
# -----------------------------
print("📡 Fetching features from Feast...")
features_df = store.get_historical_features(
    entity_df=entity_df,
    features=feature_list
).to_df()

# -----------------------------
# 6️⃣ Merge features + target
# -----------------------------
training_df = pd.merge(features_df, target_df, on=["event_timestamp", "id"], how="inner")

# -----------------------------
# 7️⃣ Prepare features (X) and target (y)
# -----------------------------
X = training_df.drop(columns=["event_timestamp", "id", "AQI"])
y = training_df["AQI"]

# -----------------------------
# 8️⃣ Split data
# -----------------------------
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

# -----------------------------
# 9️⃣ Encode categorical columns safely
# -----------------------------
if "day_of_week" in X_train.columns:
    X_train = pd.get_dummies(X_train, columns=["day_of_week"])
    X_test = pd.get_dummies(X_test, columns=["day_of_week"])
    X_test = X_test.reindex(columns=X_train.columns, fill_value=0)

# -----------------------------
# 🔟 Train Random Forest
# -----------------------------
rf_model = RandomForestRegressor(n_estimators=150, random_state=42)
rf_model.fit(X_train, y_train)

# -----------------------------
# 1️⃣1️⃣ Evaluate model
# -----------------------------
y_pred = rf_model.predict(X_test)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f"✅ Random Forest RMSE: {rmse:.2f}")
print(f"✅ Random Forest MAE: {mae:.2f}")
print(f"✅ Random Forest R²: {r2:.3f}")

# -----------------------------
# 1️⃣2️⃣ Save model locally
# -----------------------------


# Create models folder in project root
os.makedirs("/Users/macbook-air/Desktop/pearls_aqi_predictor/models", exist_ok=True)

# Save model there
model_path = "/Users/macbook-air/Desktop/pearls_aqi_predictor/models/aqi_rf_model.pkl"
joblib.dump(rf_model, model_path)
print("✅ Model saved at:", model_path)

# -----------------------------
# 1️⃣3️⃣ Log to MLflow
# -----------------------------
# -----------------------------
# 1️⃣3️⃣ Log to MLflow
# -----------------------------
with mlflow.start_run(run_name="RandomForest_AQI"):
    mlflow.sklearn.log_model(
        sk_model=rf_model,
        name="aqi_rf_model",  # ✅ correct
        input_example=X_train.iloc[:5]
    )
    mlflow.log_param("n_estimators", rf_model.n_estimators)
    mlflow.log_param("random_state", rf_model.random_state)
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("mae", mae)
    mlflow.log_metric("r2", r2)
