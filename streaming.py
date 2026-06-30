"""
ASSIGNMENT 3 — Task 2 & 3: PySpark Structured Streaming
- Reads from Kafka topic 'weather-stream'
- Parses JSON messages into structured DataFrame
- Applies watermarking for late-arriving data (Task 3)
- Tumbling window aggregations (30-second windows)
- Classifies each window by temperature band
- Writes results to Redis via foreachBatch
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import redis
import json

KAFKA_BROKER  = "localhost:9092"
TOPIC_NAME    = "weather-stream"
REDIS_HOST    = "localhost"
REDIS_PORT    = 6379
WINDOW_SIZE   = "30 seconds"
WATERMARK     = "10 seconds"   


spark = SparkSession.builder \
    .appName("WeatherStructuredStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  TASK 2 & 3 — PYSPARK STRUCTURED STREAMING")
print("=" * 60)
print(f"  Kafka  : {KAFKA_BROKER} → topic: {TOPIC_NAME}")
print(f"  Redis  : {REDIS_HOST}:{REDIS_PORT}")
print(f"  Window : {WINDOW_SIZE} tumbling")
print(f"  Watermark: {WATERMARK} (Task 3 — late data handling)")
print("=" * 60)

schema = StructType([
    StructField("timestamp",        StringType(),  True),
    StructField("summary",          StringType(),  True),
    StructField("precip_type",      StringType(),  True),
    StructField("temp_c",           DoubleType(),  True),
    StructField("apparent_temp_c",  DoubleType(),  True),
    StructField("humidity",         DoubleType(),  True),
    StructField("wind_speed_kmh",   DoubleType(),  True),
    StructField("wind_bearing_deg", DoubleType(),  True),
    StructField("visibility_km",    DoubleType(),  True),
    StructField("pressure_mb",      DoubleType(),  True),
    StructField("replay_loop",      IntegerType(), True),
    StructField("produced_at",      StringType(),  True),
])

# ── Read from Kafka ───────────────────────────────────────────
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .load()

# ── Parse JSON ────────────────────────────────────────────────
parsed = raw_stream.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data")
).select("data.*")

# Cast timestamp string to proper timestamp type
parsed = parsed.withColumn(
    "event_time",
    F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSS Z")
).withColumn(
    "processed_at",
    F.current_timestamp()
)

watermarked = parsed.withWatermark("processed_at", WATERMARK)


windowed = watermarked \
    .groupBy(
        F.window("processed_at", WINDOW_SIZE),
        "precip_type"
    ).agg(
        F.count("*")                    .alias("record_count"),
        F.round(F.avg("temp_c"), 2)     .alias("avg_temp_c"),
        F.round(F.max("temp_c"), 2)     .alias("max_temp_c"),
        F.round(F.min("temp_c"), 2)     .alias("min_temp_c"),
        F.round(F.avg("humidity"), 2)   .alias("avg_humidity"),
        F.round(F.avg("wind_speed_kmh"), 2).alias("avg_wind_kmh"),
        F.round(F.avg("visibility_km"), 2) .alias("avg_visibility_km"),
        F.round(F.avg("pressure_mb"), 2)   .alias("avg_pressure_mb")
    )

classified = windowed.withColumn(
    "temp_band",
    F.when(F.col("avg_temp_c") >= 25,  " HOT (≥25°C)")
     .when(F.col("avg_temp_c") >= 15,  " WARM (15-25°C)")
     .when(F.col("avg_temp_c") >= 5,   " MILD (5-15°C)")
     .when(F.col("avg_temp_c") >= -5,  " COLD (−5 to 5°C)")
     .otherwise(                        "❄️  FREEZING (<−5°C)")
).withColumn(
    "wind_band",
    F.when(F.col("avg_wind_kmh") >= 30, "Very Windy")
     .when(F.col("avg_wind_kmh") >= 20, "Windy")
     .when(F.col("avg_wind_kmh") >= 10, "Breezy")
     .otherwise(                         "Calm")
)

def write_to_redis(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    for row in rows:
        window_start = str(row["window"]["start"])
        window_end   = str(row["window"]["end"])
        precip       = row["precip_type"] or "unknown"

        
        key = f"weather:window:{window_start}:{precip}"

        data = {
            "window_start":     window_start,
            "window_end":       window_end,
            "precip_type":      precip,
            "record_count":     str(row["record_count"]),
            "avg_temp_c":       str(row["avg_temp_c"]),
            "max_temp_c":       str(row["max_temp_c"]),
            "min_temp_c":       str(row["min_temp_c"]),
            "avg_humidity":     str(row["avg_humidity"]),
            "avg_wind_kmh":     str(row["avg_wind_kmh"]),
            "avg_visibility_km":str(row["avg_visibility_km"]),
            "avg_pressure_mb":  str(row["avg_pressure_mb"]),
            "temp_band":        str(row["temp_band"]),
            "wind_band":        str(row["wind_band"]),
            "batch_id":         str(batch_id),
        }

        
        r.hset(key, mapping=data)
        r.expire(key, 3600)

        r.lpush("weather:feed", json.dumps(data))
        r.ltrim("weather:feed", 0, 199)  

    r.set("weather:last_batch_id",   str(batch_id))
    r.set("weather:last_updated",    str(rows[0]["window"]["end"]))
    r.set("weather:total_windows",   str(r.dbsize()))

    print(f"[Batch {batch_id}] {len(rows)} window(s) written to Redis")
    for row in rows:
        print(f"  ▸ {row['window']['start']} | {row['precip_type']:8} | "
              f"avg {row['avg_temp_c']}°C | {row['temp_band']} | "
              f"n={row['record_count']}")

query = classified.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_redis) \
    .option("checkpointLocation", "/tmp/weather_checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()

print("\n[STREAMING] Query started. Waiting for micro-batches...")
print("[STREAMING] Press Ctrl+C to stop.\n")

query.awaitTermination()