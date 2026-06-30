
import streamlit as st
import redis
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time

st.set_page_config(
    page_title="Live Weather Stream",
    page_icon="🌩️",
    layout="wide"
)

@st.cache_resource
def get_redis():
    return redis.Redis(host="localhost", port=6379, decode_responses=True)

r = get_redis()


st_autorefresh_interval = 5  

st.title(" Live Weather Streaming Dashboard")
st.caption("")

last_batch    = r.get("weather:last_batch_id")
last_updated  = r.get("weather:last_updated")
feed_raw      = r.lrange("weather:feed", 0, 199)
feed          = [json.loads(x) for x in feed_raw] if feed_raw else []

if not feed:
    st.warning(" Waiting for streaming data... make sure the producer and streaming job are running.")
    time.sleep(st_autorefresh_interval)
    st.rerun()

df = pd.DataFrame(feed)
df["window_start"] = pd.to_datetime(df["window_start"])
df["avg_temp_c"]    = pd.to_numeric(df["avg_temp_c"])
df["avg_humidity"]  = pd.to_numeric(df["avg_humidity"])
df["avg_wind_kmh"]  = pd.to_numeric(df["avg_wind_kmh"])
df["record_count"]  = pd.to_numeric(df["record_count"])
df = df.sort_values("window_start")

s1, s2, s3, s4 = st.columns(4)
s1.metric(" Status", "LIVE")
s2.metric(" Last Batch ID", last_batch or "—")
s3.metric(" Last Updated", str(last_updated)[-8:] if last_updated else "—")
s4.metric(" Windows in Feed", len(df))

st.markdown("---")

latest = df.iloc[-1]
k1, k2, k3, k4 = st.columns(4)
k1.metric(" Latest Avg Temp", f"{latest['avg_temp_c']}°C")
k2.metric(" Latest Humidity", f"{latest['avg_humidity']}")
k3.metric(" Latest Wind",     f"{latest['avg_wind_kmh']} km/h")
k4.metric(" Temp Band",       latest["temp_band"])

st.markdown("---")

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Live Temperature Stream (last 200 windows)")
    fig1 = go.Figure()
    for precip in df["precip_type"].unique():
        sub = df[df["precip_type"] == precip]
        fig1.add_trace(go.Scatter(
            x=sub["window_start"], y=sub["avg_temp_c"],
            mode="lines+markers", name=precip
        ))
    fig1.update_layout(
        xaxis_title="Window Start", yaxis_title="Avg Temp (°C)", height=400
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.subheader(" Temperature Band Distribution")
    band_counts = df["temp_band"].value_counts().reset_index()
    band_counts.columns = ["temp_band", "count"]
    fig2 = px.pie(
        band_counts, names="temp_band", values="count", hole=0.4
    )
    fig2.update_layout(height=400, showlegend=True)
    st.plotly_chart(fig2, use_container_width=True)

col3, col4 = st.columns(2)

with col3:
    st.subheader(" Wind Speed Over Time")
    fig3 = px.area(
        df, x="window_start", y="avg_wind_kmh", color="precip_type",
        labels={"window_start": "Time", "avg_wind_kmh": "Wind (km/h)"}
    )
    fig3.update_layout(height=350)
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.subheader(" Humidity Over Time")
    fig4 = px.line(
        df, x="window_start", y="avg_humidity", color="precip_type",
        markers=True,
        labels={"window_start": "Time", "avg_humidity": "Humidity"}
    )
    fig4.update_layout(height=350)
    st.plotly_chart(fig4, use_container_width=True)

st.subheader(" Records Processed per Window (Throughput)")
fig5 = px.bar(
    df, x="window_start", y="record_count", color="precip_type",
    labels={"window_start": "Window", "record_count": "Records"}
)
fig5.update_layout(height=300)
st.plotly_chart(fig5, use_container_width=True)

st.markdown("---")
st.subheader(" Raw Live Feed (most recent 20 windows)")
display_cols = [
    "window_start", "precip_type", "record_count",
    "avg_temp_c", "avg_humidity", "avg_wind_kmh",
    "temp_band", "wind_band"
]
st.dataframe(
    df[display_cols].sort_values("window_start", ascending=False).head(20),
    use_container_width=True
)

time.sleep(st_autorefresh_interval)
st.rerun()