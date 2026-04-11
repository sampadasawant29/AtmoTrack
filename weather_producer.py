import requests
import json
import os
import time
from datetime import datetime, timedelta

# -----------------------------
# CONFIGURATION
# -----------------------------
API_KEY = "2JNZ8CSJEZGQJ3XQNPBY9VY33"
CITY = "Pune"
HISTORICAL_DIR = "weather_input/historical"
REALTIME_DIR = "weather_input/realtime"
REALTIME_INTERVAL = 60  # seconds

# -----------------------------
# CREATE DIRECTORIES
# -----------------------------
os.makedirs(HISTORICAL_DIR, exist_ok=True)
os.makedirs(REALTIME_DIR, exist_ok=True)

# -----------------------------
# HELPER FUNCTION: Save JSON + Display
# -----------------------------
def save_record(record, is_historical=False):
    timestamp_safe = record['time'].replace(':', '-').replace('.', '-').replace('T', '_')
    folder = HISTORICAL_DIR if is_historical else REALTIME_DIR
    filename = os.path.join(folder, f"weather_{timestamp_safe}.json")
    
    with open(filename, 'w') as f:
        json.dump(record, f, indent=2)
    
    source = "HISTORICAL" if is_historical else "REAL-TIME"
    print(f"[{source}] {record['time']} | Temp: {record['temp']}°C | Humidity: {record['humidity']}% | "
          f"Wind: {record['windspeed']} km/h | Cond: {record.get('conditions','N/A')} | City: {record['city']}")

# -----------------------------
# STEP 1: Historical past 24 hours
# -----------------------------
yesterday = datetime.now() - timedelta(days=1)
url_hist = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{CITY}/{yesterday.strftime('%Y-%m-%d')}/today?unitGroup=metric&key={API_KEY}&include=hours"

try:
    resp = requests.get(url_hist)
    resp.raise_for_status()
    data = resp.json()
    
    for hour in data["days"][0]["hours"]:
        # Full ISO timestamp for Spark
        full_time = f"{yesterday.strftime('%Y-%m-%d')}T{hour.get('datetime')}"
        record = {
            "time": full_time,
            "temp": hour.get("temp"),
            "humidity": hour.get("humidity"),
            "windspeed": hour.get("windspeed"),
            "wdir": hour.get("winddir"),
            "pressure": hour.get("pressure"),
            "cloudcover": hour.get("cloudcover"),
            "visibility": hour.get("visibility"),
            "conditions": hour.get("conditions"),
            "icon": hour.get("icon"),
            "precip": hour.get("precip"),
            "precipprob": hour.get("precipprob"),
            "uvindex": hour.get("uvindex"),
            "city": CITY
        }
        save_record(record, is_historical=True)

except Exception as e:
    print("Error fetching historical data:", e)

# -----------------------------
# STEP 2: Real-time producer
# -----------------------------
print(f"\n🌦 Real-time Producer started — writing to '{REALTIME_DIR}'\n")

while True:
    try:
        url_current = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{CITY}/today?unitGroup=metric&key={API_KEY}&include=current"
        resp = requests.get(url_current)
        resp.raise_for_status()
        current = resp.json().get("currentConditions", {})

        # ISO timestamp without microseconds
        now_iso = datetime.now().replace(microsecond=0).isoformat()
        record = {
            "time": now_iso,
            "temp": current.get("temp"),
            "humidity": current.get("humidity"),
            "windspeed": current.get("windspeed"),
            "wdir": current.get("winddir"),
            "pressure": current.get("pressure"),
            "cloudcover": current.get("cloudcover"),
            "visibility": current.get("visibility"),
            "conditions": current.get("conditions"),
            "icon": current.get("icon"),
            "precip": current.get("precip"),
            "precipprob": current.get("precipprob"),
            "uvindex": current.get("uvindex"),
            "city": CITY
        }
        save_record(record)
        time.sleep(REALTIME_INTERVAL)

    except KeyboardInterrupt:
        print("Producer stopped by user.")
        break
    except Exception as e:
        print("Error fetching current weather:", e)
        time.sleep(REALTIME_INTERVAL)