import os
import queue
import pandas as pd
import streamlit as st
import plotly.express as px
import streamlit.components.v1 as components

from streamlit_autorefresh import st_autorefresh
from dashboard_app import AzureIoTConnector, DashboardState


# ====================================================
# PAGE CONFIG
# ====================================================
st.set_page_config(
    page_title="Industrial Network Demonstration",
    layout="wide",
)


# ====================================================
# CSS STYLING
# ====================================================
st.markdown("""
<style>
.stApp {
    background: linear-gradient(135deg, #0b1f33, #102a43);
    color: white;
}

.kpi-card {
    background: rgba(255,255,255,0.08);
    padding: 15px;
    border-radius: 15px;
    text-align: center;
}

.kpi-title {
    font-size: 12px;
    color: #ccc;
}

.kpi-value {
    font-size: 22px;
    font-weight: bold;
}
</style>
""", unsafe_allow_html=True)


# ====================================================
# AUTO REFRESH
# ====================================================
st_autorefresh(interval=3000, key="refresh")


# ====================================================
# SESSION STATE
# ====================================================
if "state" not in st.session_state:
    st.session_state.state = DashboardState()

if "message_queue" not in st.session_state:
    st.session_state.message_queue = queue.Queue()

if "connector" not in st.session_state:
    st.session_state.connector = None


# ====================================================
# START CONNECTOR
# ====================================================
def start_connector():
    if st.session_state.connector is not None:
        return

    conn_str = os.getenv("AZURE_IOT_HUB_CONNECTION_STRING")

    if not conn_str:
        st.error("Set AZURE_IOT_HUB_CONNECTION_STRING")
        return

    connector = AzureIoTConnector(conn_str)

    # ✅ IMPORTANT: get queue reference OUTSIDE
    q = st.session_state.message_queue

    # ✅ thread-safe function
    def push_to_queue(msg):
        q.put(msg)   # ✅ use local variable, NOT session_state

    connector.start(push_to_queue)

    st.session_state.connector = connector


start_connector()


# ====================================================
# PROCESS QUEUE
# ====================================================
state = st.session_state.state
q = st.session_state.message_queue

while not q.empty():
    msg = q.get()
    state.update(msg)


# ====================================================
# DATA SNAPSHOT
# ====================================================
snapshot = state.snapshot()
devices = snapshot.get("devices", {})
latest = snapshot.get("latest_readings", {})
history = snapshot.get("history", {})


# ====================================================
# HEADER AND LOGO
# ====================================================
col1, col2 = st.columns([3, 1])

with col1:
    st.markdown("""
        <div style="
            display: flex;
            align-items: center;
            justify-content: center;
            height: 100%;
        ">
            <h1 style="
                text-align: center;
                font-size: 50px;
                margin: 0;
                line-height: 1.2;
                color: white;
            ">
                Industrial Network Demonstration
            </h1>
        </div>
    """, unsafe_allow_html=True)

with col2:
    if os.path.exists("logo.png"):
        st.markdown("""
            <div style="display: flex; align-items: center; justify-content: flex-end;">
        """, unsafe_allow_html=True)

        st.image("logo.png", width=180)

        st.markdown("</div>", unsafe_allow_html=True)


# ====================================================
# NO DATA CASE
# ====================================================
device_list = list(devices.keys())

if not device_list:
    st.info("Waiting for IoT data...")
    st.stop()


# ====================================================
# DEVICE SELECT
# ====================================================
selected_device = st.selectbox("Select Device", device_list)


# ====================================================
# FILTER DEVICE DATA
# ====================================================
device_sensors = {
    k: v for k, v in latest.items()
    if k.startswith(f"{selected_device}:")
}


# ====================================================
# KPI SECTION
# ====================================================
st.subheader("Device Overview")

cols = st.columns(3)

with cols[0]:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Device</div>
        <div class="kpi-value">{selected_device}</div>
    </div>
    """, unsafe_allow_html=True)

with cols[1]:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Sensors</div>
        <div class="kpi-value">{len(device_sensors)}</div>
    </div>
    """, unsafe_allow_html=True)

total_messages = sum(
    len(v)
    for k, v in history.items()
    if k.startswith(f"{selected_device}:")
)

with cols[2]:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Total Messages</div>
        <div class="kpi-value">{total_messages}</div>
    </div>
    """, unsafe_allow_html=True)


# ====================================================
# LIVE METRICS
# ====================================================

st.subheader("Live Sensor Values")

if not device_sensors:
    st.info("No sensor data available")
else:
    cols = st.columns(5)

    for i, (key, val) in enumerate(device_sensors.items()):
        sensor = key.split(":")[1]

        value = val.get("value")
        try:
            value = round(float(value), 2)
        except:
            pass

        unit = val.get("unit", "")

        with cols[i % 5]:
            components.html(f"""
                <div style="
                    background: linear-gradient(135deg, rgba(0,229,255,0.25), rgba(47,255,163,0.15));
                    padding: 18px;
                    border-radius: 16px;
                    text-align: center;
                    box-shadow: 0 6px 20px rgba(0,0,0,0.3);
                    color: white;
                ">
                    <div style="
                        color: #9fb8d0;
                        font-size: 13px;
                        margin-bottom: 8px;
                    ">
                        {sensor.upper()}
                    </div>

                    <div style="
                        font-size: 30px;
                        font-weight: 800;
                    ">
                        {value} {unit}
                    </div>
                </div>
            """, height=130)
# ====================================================
# SENSOR CHARTS
# ====================================================
st.subheader("Sensor Trends")

keys = list(device_sensors.keys())

for i in range(0, len(keys), 2):
    cols = st.columns(2)

    for j in range(2):
        if i + j >= len(keys):
            continue

        key = keys[i + j]
        sensor = key.split(":")[1]
        sensor_history = history.get(key, [])

        df = pd.DataFrame(sensor_history)

        if df.empty:
            continue

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

        df = df.dropna()
        df = df.sort_values("timestamp")

        fig = px.line(df, x="timestamp", y="value", title=sensor)

        fig.update_layout(height=300)

        with cols[j % 2]:
            st.plotly_chart(fig, width='stretch')
