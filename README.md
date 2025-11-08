AQI Forecasting System 🌸

**Overview**

This project is an end-to-end machine learning system that predicts the Air Quality Index (AQI) for the next 3 days. 
It combines data engineering, feature management, model training, and real-time web visualization. Users can monitor live AQI readings, explore trends, and see future forecasts on an interactive dashboard.

**Key Highlights:**

1. Real-time AQI predictions using Random Forest & Neural Network models
2. Fully automated CI/CD pipeline using GitHub Actions
3. Feature management with Feast and data storage in PostgreSQL
4. Interactive dashboard built with Flask, HTML, and CSS
5. EDA and Feature Importance visualization using SHAP
6. Live monitoring of weather & pollutant data from OpenWeather & Open-Meteo APIs

**Features**

1. Live AQI Display: Color-coded AQI with a gradient marker
2. Data Details: PM2.5, PM10, temperature, humidity, and wind
3. AQI Trend Chart: 24-hour AQI variations using Chart.js
4. Interactive Map: Nearby monitoring stations with dynamic markers
5. 3-Day Forecast Cards: Predict AQI, PM2.5, PM10 using ML models
6. Model Explainability: SHAP-based feature importance plots

**Tech Stack**

Backend: Python, PostgreSQL, Feast, MLflow, GitHub Actions
Models: Random Forest Regressor, Neural Network, Linear Regression
Frontend: Flask, HTML, CSS, Chart.js, Leaflet.js
Data Sources: OpenWeather API, Open-Meteo API

**Installation & Usage**

1. Clone the repository
git clone https://github.com/humnanazir/PearlsAirSense.git
cd PearlsAirSense

2. Create a virtual environment
python3 -m venv venv
source venv/bin/activate

3. Install dependencies
pip install -r requirements.txt

4. Run the Flask app
python3 app.py

**Future Work**

1. Add email/SMS alerts for hazardous AQI levels
2. Integrate deep learning models for improved predictions
3. Enhance dashboard with user authentication & role-based access
