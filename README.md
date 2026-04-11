# 🌦️ Real-Time Weather Data Pipeline & Analytics Dashboard

This project is an end-to-end Big Data pipeline designed to ingest, process, and visualize real-time and historical weather data. It leverages PySpark for robust data stream processing, Streamlit for a highly interactive and real-time dashboard, and Facebook Prophet for AI-driven weather forecasting.

## 🏗️ Architecture & Core Components

This system runs on three primary python scripts working in tandem:

### 1. The Data Producer (`weather_producer.py`)
This script acts as the data ingestion layer. 
- It communicates with the **Visual Crossing Weather API**.
- It fetches the past 24 hours of hourly historical data and saves them as JSON files in `weather_input/historical`.
- It continually monitors current live weather conditions (fetching every 60 seconds) and writes the real-time weather logs into `weather_input/realtime`.

### 2. The Data Processing Engine (`spark_analytics.py`)
Built on top of **Apache Spark (PySpark)**, this application is responsible for the heavy lifting.
- **Batch Processing**: It reads the historical JSONs and computes basic batch aggregations (max, averages) grouped by city, exporting it to `historical_agg.csv`.
- **Structured Streaming**: It sets up a real-time stream over the `weather_input/realtime` directory. Utilizing Spark's robust streaming capabilities, it aggregates data over jumping time windows with a 10-minute watermark.
- The results of the streaming query are output to `latest_realtime.csv` every minute.

### 3. The Interactive Dashboard (`streamlit_dashboard.py`)
A **Streamlit** frontend interface that provides a user-friendly way to analyze the weather metrics:
- **Auto-Refresh**: Live updates the dashboard with the newest real-time data ingestion every few seconds without page reloads.
- **Historical Trends**: Plotly-powered interactive line charts for historical indicators (Temperature, Humidity, Windspeed, etc.).
- **Spark Analytics**: A real-time timeline visualizing the structured streaming outputs and aggregated rolling-window metrics outputted by Spark. 
- **AI Forecasting (Prophet)**: Uses Facebook's Prophet to run time-series forecasting. It predicts weather conditions over the next 7 days based on dynamic trends. 

## 🚀 Getting Started

### Prerequisites

Ensure you have Python 3.8+ and Java (required for PySpark) installed on your system. 

```bash
# Install the necessary pip packages
pip install pyspark streamlit pandas plotly requests prophet
```

*Note: You may also need to configure a local Spark environment/Hadoop binaries on Windows if you run into Spark initialization issues.*

### Usage

To start the pipeline, you need to launch all three services in terminal environments. Run them in the following order:

1. **Start the Producer**: 
   This will begin fetching historical logs and kick off the real-time polling loop.
   ```bash
   python weather_producer.py
   ```

2. **Start the PySpark Streaming Job**:
   This will immediately churn the historical data and start listening to the real-time folder for Spark Structured Streaming.
   ```bash
   python spark_analytics.py
   ```

3. **Launch the Streamlit App**:
   Finally, fire up the dashboard to view the data.
   ```bash
   streamlit run streamlit_dashboard.py
   ```

The Streamlit dashboard will dynamically update its metrics, visuals, and interactive widgets (including the Prophet AI predictor) directly in your browser.

## 🛠️ Built With

- **[PySpark](https://spark.apache.org/docs/latest/api/python/)**: Big Data Processing / Structured Streaming
- **[Streamlit](https://streamlit.io/)**: Dashboard API
- **[Facebook Prophet](https://facebook.github.io/prophet/)**: Machine Learning Time Series Forecasting
- **[Plotly](https://plotly.com/python/)**: Interactive visualizations
- **[Visual Crossing](https://www.visualcrossing.com/)**: REST API Weather Data source
