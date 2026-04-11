import streamlit as st
import pandas as pd
import json
import os
from glob import glob
from streamlit_autorefresh import st_autorefresh
import plotly.express as px
import datetime

# -----------------------------
# CONFIG
# -----------------------------
HISTORICAL_DIR = "weather_input/historical"
REALTIME_DIR = "weather_input/realtime"
HISTORICAL_AGG_CSV = "historical_agg.csv"
LATEST_REALTIME_CSV = "latest_realtime.csv"

st.set_page_config(page_title="🌦 Weather Dashboard", layout="wide")
st.title("🌦 Weather Dashboard")

# -----------------------------
# AUTO REFRESH
# -----------------------------
st_autorefresh(interval=30000, key="real_time_refresh")  # refresh every 5s

# -----------------------------
# HELPER FUNCTION: Read JSON files
# -----------------------------
def read_json_files(folder):
    files = sorted(glob(os.path.join(folder, "*.json")))
    if not files:
        return pd.DataFrame()

    dfs = []
    for f in files:
        try:
            with open(f, "r") as js:
                data = json.load(js)
            dfs.append(pd.DataFrame([data]))
        except json.JSONDecodeError:
            # Skip file if it's being written
            continue
        except Exception as e:
            print(f"Error reading {f}: {e}")
            continue

    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        if 'time' in combined.columns:
            combined['time'] = pd.to_datetime(combined['time'], errors='coerce')
            combined.dropna(subset=['time'], inplace=True)
            combined.set_index('time', inplace=True)
            combined.sort_index(inplace=True)
        return combined

    return pd.DataFrame()

# -----------------------------
# HISTORICAL DATA
# -----------------------------
hist_df = read_json_files(HISTORICAL_DIR)

if not hist_df.empty:
    st.header("📈 Historical Data")
    
    min_time = hist_df.index.min().to_pydatetime()
    max_time = hist_df.index.max().to_pydatetime()
    
    time_range = st.slider(
        "Select Time Range:",
        min_value=min_time,
        max_value=max_time,
        value=(min_time, max_time),
        format="YYYY-MM-DD HH:mm"
    )
    
    hist_df_filtered = hist_df.loc[time_range[0]:time_range[1]]
    
    if not hist_df_filtered.empty:
        # Define the parameters to plot
        params = ["temp", "humidity", "windspeed", "pressure", "cloudcover",
                  "uvindex"]

        for i in range(0, len(params), 3):
            cols = st.columns(3)
            for col, param in zip(cols, params[i:i+3]):
                if param in hist_df_filtered.columns:
                    fig = px.line(
                        hist_df_filtered, x=hist_df_filtered.index, y=param,
                        title=f"{param.capitalize()} over Time",
                        labels={param: param.capitalize(), "index": "Time"}
                    )
                    col.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available in selected range.")
else:
    st.info("No historical data found. Run weather_producer.py first.")

# -----------------------------
# SHOW LATEST WEATHER (REAL-TIME)
# -----------------------------
st.header("⚡ Latest Weather Update")
realtime_df = read_json_files(REALTIME_DIR)

if not realtime_df.empty:
    latest = realtime_df.iloc[-1]
    
    metrics = [
        ("Temperature (°C)", latest.get('temp', 'N/A')),
        ("Humidity (%)", latest.get('humidity', 'N/A')),
        ("Wind Speed (km/h)", latest.get('windspeed', 'N/A')),
        ("Pressure (hPa)", latest.get('pressure', 'N/A')),
        ("Cloud Cover (%)", latest.get('cloudcover', 'N/A')),
        ("Visibility (km)", latest.get('visibility', 'N/A')),
        ("UV Index", latest.get('uvindex', 'N/A')),
        ("Precipitation (mm)", latest.get('precip', 'N/A')),
        ("Precip Prob (%)", latest.get('precipprob', 'N/A')),
        ("Conditions", latest.get("conditions", "N/A"))
    ]
    
    for i in range(0, len(metrics), 4):
        cols = st.columns(4)
        for col, (label, value) in zip(cols, metrics[i:i+4]):
            col.metric(label, f"{value}")
else:
    st.info("Waiting for real-time data...")

# -----------------------------
# REAL-TIME VISULAIZATION FROM JSON (SEPARATE CHARTS)
# -----------------------------
st.header("📊 Real-time Visualization (from JSON)")
if not realtime_df.empty:
    params = ["temp", "humidity", "windspeed", "pressure", "cloudcover",
              "uvindex"]
    
    col_chunk = 3
    for i in range(0, len(params), col_chunk):
        cols = st.columns(col_chunk)
        for col, param in zip(cols, params[i:i+col_chunk]):
            if param in realtime_df.columns:
                fig = px.line(
                    realtime_df, x=realtime_df.index, y=param,
                    title=f"{param.capitalize()} over Time",
                    labels={param: param.capitalize(), "index": "Time"}
                )
                col.plotly_chart(fig, use_container_width=True)
else:
    st.info("Waiting for real-time data...")

# -----------------------------
# HISTORICAL AGGREGATE TABLE
# -----------------------------
st.header("📋 Historical Aggregate Table")
if os.path.exists(HISTORICAL_AGG_CSV) and os.path.getsize(HISTORICAL_AGG_CSV) > 0:
    try:
        df_hist_agg = pd.read_csv(HISTORICAL_AGG_CSV)
        st.dataframe(df_hist_agg)
    except pd.errors.EmptyDataError:
        st.info("historical_agg.csv is currently empty. Waiting for data...")
else:
    st.info("historical_agg.csv not found or is empty. Run spark_analytics.py first.")

# -----------------------------
# SPARK REAL-TIME ANALYTICS FROM CSV (SEPARATE CHARTS)
# -----------------------------
st.header("📊 Spark Real-time Analytics (from CSV)")

if os.path.exists(LATEST_REALTIME_CSV) and os.path.getsize(LATEST_REALTIME_CSV) > 0:
    try:
        df_realtime_csv = pd.read_csv(LATEST_REALTIME_CSV)
        
        if not df_realtime_csv.empty:
            # Ensure timestamp column exists and set as index
            if "time" in df_realtime_csv.columns:
                df_realtime_csv['time'] = pd.to_datetime(df_realtime_csv['time'], errors='coerce')
                df_realtime_csv.set_index('time', inplace=True)
                df_realtime_csv.sort_index(inplace=True)
            elif "window_start" in df_realtime_csv.columns:
                df_realtime_csv['time'] = pd.to_datetime(df_realtime_csv['window_start'], errors='coerce')
                df_realtime_csv.set_index('time', inplace=True)
                df_realtime_csv.sort_index(inplace=True)
            
            # List of Spark metrics to plot
            spark_params = [
                "Max_Temp_C", "Avg_Wind_Speed_Kmh", "Avg_Humidity_Perc",
                "Avg_Pressure_hPa", "Avg_Cloud_Cover_Perc",
                "Avg_UV_Index"
            ]
            
            col_chunk = 3  # 3 charts per row
            for i in range(0, len(spark_params), col_chunk):
                cols = st.columns(col_chunk)
                for col, param in zip(cols, spark_params[i:i+col_chunk]):
                    if param in df_realtime_csv.columns:
                        fig = px.line(
                            df_realtime_csv, x=df_realtime_csv.index, y=param,
                            title=f"{param.replace('_',' ')} over Time",
                            labels={param: param.replace("_", " "), "index": "Time"}
                        )
                        col.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No Spark real-time analytics yet.")
    except pd.errors.EmptyDataError:
        st.info("latest_realtime.csv is currently empty. Waiting for data...")
else:
    st.info("latest_realtime.csv not found or is empty. Run spark_analytics.py first.")

# -----------------------------
# PROPHET FORECASTING (7 DAYS)
# -----------------------------
st.header("🔮 7-Day Forecast (Prophet)")

if not hist_df.empty:
    target_metric = st.selectbox(
        "Select metric to forecast:",
        options=["temp", "humidity", "windspeed", "pressure", "cloudcover", "uvindex"],
        index=0
    )
    
    @st.cache_data(ttl=3600)  # cache for 1 hour
    def get_forecast(df, metric):
        import pandas as pd
        from prophet import Prophet
        
        # Prophet expects columns 'ds' and 'y'
        prophet_df = df[[metric]].reset_index()
        if "time" in prophet_df.columns:
            prophet_df.rename(columns={"time": "ds", metric: "y"}, inplace=True)
        elif "index" in prophet_df.columns:
            prophet_df.rename(columns={"index": "ds", metric: "y"}, inplace=True)
            
        prophet_df.dropna(inplace=True)
        
        # Prophet requires timezone-naive inside if using default configs
        if prophet_df['ds'].dt.tz is not None:
            prophet_df['ds'] = prophet_df['ds'].dt.tz_localize(None)
            
        m = Prophet(daily_seasonality=True, yearly_seasonality=False)
        m.fit(prophet_df)
        
        # Forecast 7 days into future
        # Assuming our data is at 10 min or hourly intervals, we want freq='H'
        future = m.make_future_dataframe(periods=168, freq='h')
        forecast = m.predict(future)
        return forecast

    with st.spinner(f"Training Prophet model for {target_metric}..."):
        try:
            forecast_df = get_forecast(hist_df, target_metric)
            
            # Create interactive plotly chart
            import plotly.graph_objs as go
            fig_prophet = go.Figure()
            
            # Actual Data points
            actuals_df = hist_df[[target_metric]].reset_index()
            if "time" in actuals_df.columns:
                time_col = "time"
            else:
                time_col = "index"
                
            if actuals_df[time_col].dt.tz is not None:
                actuals_df[time_col] = actuals_df[time_col].dt.tz_localize(None)
                
            fig_prophet.add_trace(go.Scatter(
                x=actuals_df[time_col], y=actuals_df[target_metric],
                mode='lines', name='Actual History',
                line=dict(color='orange')
            ))
            
            # Predict Line
            fig_prophet.add_trace(go.Scatter(
                x=forecast_df['ds'], y=forecast_df['yhat'],
                mode='lines', name='Forecast',
                line=dict(color='blue')
            ))
            
            # Upper/Lower Bounds
            fig_prophet.add_trace(go.Scatter(
                x=forecast_df['ds'], y=forecast_df['yhat_upper'],
                mode='lines', line=dict(width=0), showlegend=False,
                hoverinfo='skip'
            ))
            fig_prophet.add_trace(go.Scatter(
                x=forecast_df['ds'], y=forecast_df['yhat_lower'],
                mode='lines', line=dict(width=0), fill='tonexty',
                fillcolor='rgba(0, 0, 255, 0.2)', name='Uncertainty Interval',
                hoverinfo='skip'
            ))
            
            fig_prophet.update_layout(
                title=f"7-Day Forecast for {target_metric.capitalize()}",
                xaxis_title="Time",
                yaxis_title=f"Predicted {target_metric.capitalize()}",
                hovermode="x"
            )
            
            st.plotly_chart(fig_prophet, use_container_width=True)
            
            with st.expander("View Forecast Data Table"):
                st.dataframe(forecast_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(168))
        except Exception as e:
            st.error(f"Failed to generate forecast: {e}")
else:
    st.info("Historical data is required for forecasting.")