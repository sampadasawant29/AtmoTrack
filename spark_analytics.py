import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, current_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# -----------------------------
# CONFIG
# -----------------------------
HISTORICAL_DIR = "weather_input/historical"
REALTIME_DIR = "weather_input/realtime"
HISTORICAL_CSV = "historical_agg.csv"
REALTIME_CSV = "latest_realtime.csv"

# -----------------------------
# CREATE OUTPUT FILE IF NOT EXISTS
# -----------------------------
if not os.path.exists(HISTORICAL_CSV):
    with open(HISTORICAL_CSV, "w") as f:
        f.write("")

if not os.path.exists(REALTIME_CSV):
    with open(REALTIME_CSV, "w") as f:
        f.write("")

# -----------------------------
# INIT SPARK
# -----------------------------
spark = SparkSession.builder.appName("WeatherAnalytics").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# DEFINE SCHEMA
# -----------------------------
schema = StructType([
    StructField("time", StringType(), True),
    StructField("temp", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("windspeed", DoubleType(), True),
    StructField("wdir", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("cloudcover", DoubleType(), True),
    StructField("visibility", DoubleType(), True),
    StructField("conditions", StringType(), True),
    StructField("icon", StringType(), True),
    StructField("precip", DoubleType(), True),
    StructField("precipprob", DoubleType(), True),
    StructField("uvindex", DoubleType(), True),
    StructField("city", StringType(), True)
])

# -----------------------------
# 1️⃣ Historical Processing
# -----------------------------
hist_df = spark.read.schema(schema).option("multiLine", True).json(HISTORICAL_DIR)
hist_df = hist_df.withColumn("time", col("time").cast(TimestampType()))

# Debug: check if JSON was read correctly
print("===== Historical Data Preview =====")
hist_df.show(truncate=False)
print(f"Total historical rows read: {hist_df.count()}")
print(f"Columns: {hist_df.columns}")

# Aggregate
hist_agg = hist_df.groupBy("city").agg(
    max("temp").alias("Max_Temp_C"),
    avg("windspeed").alias("Avg_Wind_Speed_Kmh"),
    avg("humidity").alias("Avg_Humidity_Perc"),
    avg("pressure").alias("Avg_Pressure_hPa"),
    avg("cloudcover").alias("Avg_Cloud_Cover_Perc"),
    avg("visibility").alias("Avg_Visibility_Km"),
    avg("uvindex").alias("Avg_UV_Index"),
    avg("precip").alias("Avg_Precip_mm"),
    avg("precipprob").alias("Avg_Precip_Prob_Perc")
)

hist_agg.toPandas().to_csv(HISTORICAL_CSV, index=False)
print(f"✅ Historical aggregation saved: {HISTORICAL_CSV}")

# -----------------------------
# 2️⃣ Real-time Processing
# -----------------------------
realtime_df = spark.readStream.schema(schema).option("multiLine", True).json(REALTIME_DIR)
realtime_df = realtime_df.withColumn("time", col("time").cast(TimestampType()))

agg_realtime = realtime_df \
    .withWatermark("time", "10 minutes") \
    .groupBy(window(col("time"), "5 minutes"), col("city")) \
    .agg(
        max("temp").alias("Max_Temp_C"),
        avg("windspeed").alias("Avg_Wind_Speed_Kmh"),
        avg("humidity").alias("Avg_Humidity_Perc"),
        avg("pressure").alias("Avg_Pressure_hPa"),
        avg("cloudcover").alias("Avg_Cloud_Cover_Perc"),
        avg("visibility").alias("Avg_Visibility_Km"),
        avg("uvindex").alias("Avg_UV_Index"),
        avg("precip").alias("Avg_Precip_mm"),
        avg("precipprob").alias("Avg_Precip_Prob_Perc")
    ).withColumn("processed_at", current_timestamp())

# -----------------------------
# FUNCTION TO PROCESS EACH BATCH
# -----------------------------
def process_batch(batch_df, batch_id):
    if not batch_df.rdd.isEmpty():
        pdf = batch_df.toPandas()
        pdf['window_start'] = pdf['window'].apply(lambda x: x['start'])
        pdf['window_end'] = pdf['window'].apply(lambda x: x['end'])
        pdf.drop(columns=['window'], inplace=True)
        pdf.to_csv(REALTIME_CSV, index=False)
        print(f"\n=== Batch {batch_id} processed, {len(pdf)} rows ===")
        batch_df.show(truncate=False)

# -----------------------------
# START STREAMING QUERY
# -----------------------------
query = agg_realtime.writeStream \
    .outputMode("complete") \
    .foreachBatch(process_batch) \
    .trigger(processingTime="60 seconds") \
    .start()

query.awaitTermination()