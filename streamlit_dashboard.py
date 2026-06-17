import os
import queue
import pandas as pd
import streamlit as st
import plotly.express as px

from streamlit_autorefresh import st_autorefresh
from dashboard_app import AzureIoTConnector, DashboardState

# ======================
# PAGE CONFIG
# ======================
st.set_page_config(
    page_title="Industrial Network Demonstration",
    layout="wide",
)

# ✅ Auto refresh
st_autorefresh(interval=3000, key="refresh")

# ======================
# SESSION STATE INIT
# ======================
if "state" not in st.session_state:
    st.session_state.state = DashboardState()

if "message_queue" not in st.session_state:
    st.session_state.message_queue = queue.Queue()

if "connector" not in st.session_state:
    st.session_state.connector = None


# ======================
# START CONNECTOR (ONCE)
# ======================
def start_connector():
    if st.session_state.connector is not None:
        return

    conn_str = os.getenv("AZURE_IOT_HUB_CONNECTION_STRING")
    if not conn_str:
        st.error("Set AZURE_IOT_HUB_CONNECTION_STRING")
        return

    connector = AzureIoTConnector(conn_str)

    q = st.session_state.message_queue

    def push_to_queue(msg):
        print("QUEUE PUT:", msg.device_id)
        q.put(msg)

    connector.start(push_to_queue)

    st.session_state.connector = connector
    st.info("✅ Connected to IoT Hub")


start_connector()

# ======================
# PROCESS QUEUE
# ======================
state = st.session_state.state
q = st.session_state.message_queue

processed = 0

while not q.empty():
    msg = q.get()
    print("QUEUE GET:", msg.device_id)
    state.update(msg)
    processed += 1

# DEBUG
st.sidebar.write(f"Processed messages: {processed}")
st.sidebar.write(f"Queue size: {q.qsize()}")

# ======================
# SNAPSHOT
# ======================
snapshot = state.snapshot()
devices = snapshot.get("devices", {})
latest = snapshot.get("latest_readings", {})
history = snapshot.get("history", {})

# ======================
# HEADER
# ======================
st.title("Industrial Network Demonstration")

# ======================
# DEVICE SELECTOR
# ======================
device_list = list(devices.keys())

if not device_list:
    st.info("⏳ Waiting for incoming IoT data...")
    st.write("Debug devices:", devices)
    st.stop()

selected_device = st.selectbox("Select Device", device_list)

# ======================
# SIDE BAR
# ======================

st.sidebar.header("⚙️ Controls")

refresh_rate = st.sidebar.slider("Refresh interval (sec)", 1, 10, 3)

device_filter = st.sidebar.multiselect(
    "Filter Devices",
    device_list,
    default=device_list
)

# ======================
# FILTER DEVICE DATA
# ======================
device_sensors = {
    k: v for k, v in latest.items()
    if k.startswith(f"{selected_device}:")
}

# ======================
# KPI METRICS
# ======================
st.markdown("## 📊 Live Metrics")

cols = st.columns(len(device_sensors) if device_sensors else 1)

for i, (key, val) in enumerate(device_sensors.items()):
    sensor = key.split(":")[1]

    cols[i % len(cols)].metric(
        label=sensor.upper(),
        value=f"{val['value']} {val.get('unit', '')}"
    )

# ======================
# SENSOR CHARTS (2 PER ROW)
# ======================
st.markdown("## 📈 Sensor Trends")

keys = list(device_sensors.keys())

for i in range(0, len(keys), 2):
    cols = st.columns(2)

    for j in range(2):
        if i + j >= len(keys):
            continue

        key = keys[i + j]
        sensor = key.split(":")[1]
        sensor_history = history.get(key, [])

        if not sensor_history:
            continue

        df = pd.DataFrame(sensor_history)

        if df.empty:
            continue

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

        df = df.dropna(subset=["value"])
        df = df.sort_values("timestamp")

        fig = px.line(
            df,
            x="timestamp",
            y="value",
            title=f"{sensor.upper()} Trend",
            markers=True
        )

        fig.update_layout(
            template="plotly_dark",
            height=300,
            margin=dict(l=10, r=10, t=40, b=10)
        )

        with cols[j]:
            st.plotly_chart(fig, width="stretch")


# ======================
# FOOTER
# ======================
st.markdown("---")
st.caption("✅ Real-time Azure IoT Monitoring Dashboard")