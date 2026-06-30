import json
import time
import csv
from kafka import KafkaProducer
from datetime import datetime

KAFKA_BROKER  = "localhost:9092"
TOPIC_NAME    = "weather-stream"
CSV_FILE      = "weatherHistory_2.csv"
DELAY_SECONDS = 0.05   

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("=" * 60)
print("  TASK 1 — KAFKA REPLAY PRODUCER")
print("=" * 60)
print(f"  Topic  : {TOPIC_NAME}")
print(f"  Broker : {KAFKA_BROKER}")
print(f"  Rate   : 1 row every {DELAY_SECONDS}s ({int(1/DELAY_SECONDS)} rows/sec)")
print(f"  File   : {CSV_FILE}")
print("  Looping continuously — press Ctrl+C to stop")
print("=" * 60)

loop_count   = 0
total_sent   = 0

try:
    while True:
        loop_count += 1
        rows_this_loop = 0

        with open(CSV_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                try:
                   
                    message = {
                        "timestamp":        row["Formatted Date"].strip(),
                        "summary":          row["Summary"].strip(),
                        "precip_type":      row["Precip Type"].strip() if row["Precip Type"] else "unknown",
                        "temp_c":           float(row["Temperature (C)"]),
                        "apparent_temp_c":  float(row["Apparent Temperature (C)"]),
                        "humidity":         float(row["Humidity"]),
                        "wind_speed_kmh":   float(row["Wind Speed (km/h)"]),
                        "wind_bearing_deg": float(row["Wind Bearing (degrees)"]),
                        "visibility_km":    float(row["Visibility (km)"]),
                        "pressure_mb":      float(row["Pressure (millibars)"]),
                        "replay_loop":      loop_count,
                        "produced_at":      datetime.utcnow().isoformat()
                    }

                    producer.send(TOPIC_NAME, value=message)
                    rows_this_loop += 1
                    total_sent     += 1

                
                    if total_sent % 500 == 0:
                        print(f"[Loop {loop_count}] Sent {total_sent:,} rows total | "
                              f"Latest: {message['timestamp']} | "
                              f"Temp: {message['temp_c']}°C | "
                              f"Precip: {message['precip_type']}")

                    time.sleep(DELAY_SECONDS)

                except (ValueError, KeyError) as e:
                    print(f"[SKIP] Malformed row skipped: {e}")
                    continue

        producer.flush()
        print(f"\n[LOOP {loop_count} COMPLETE] {rows_this_loop:,} rows sent. "
              f"Total sent: {total_sent:,}. Restarting...\n")

except KeyboardInterrupt:
    print(f"\n[STOPPED] Producer stopped by user.")
    print(f"  Total rows sent : {total_sent:,}")
    print(f"  Loops completed : {loop_count}")
    producer.flush()
    producer.close()