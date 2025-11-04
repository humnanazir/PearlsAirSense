import pandas as pd
import os

# ✅ Load both datasets
aqi = pd.read_csv("data/historical_raw_data.csv")
weather = pd.read_csv("data/historical_weather_data.csv")

# ✅ Convert to datetime
aqi["time"] = pd.to_datetime(aqi["time"])
weather["time"] = pd.to_datetime(weather["time"])

# ✅ Round both to the hour (ensures alignment)
aqi["time"] = aqi["time"].dt.floor("H")
weather["time"] = weather["time"].dt.floor("H")

# ✅ Merge using full outer join (keep *all* timestamps)
merged = pd.merge(aqi, weather, on="time", how="outer")

# ✅ Sort by time
merged = merged.sort_values("time").reset_index(drop=True)

# ✅ Save combined dataset
os.makedirs("data", exist_ok=True)
output_path = "data/full_merged_features.csv"
merged.to_csv(output_path, index=False)

print(f"✅ Full merged dataset saved: {output_path}")
print(f"📊 Total rows: {len(merged)}")
print("⚠️ Note: Missing values are left blank — handle later during cleaning.")
