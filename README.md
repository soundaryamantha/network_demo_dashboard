# Streamlit IoT Dashboard

This project uses `streamlit_dashboard.py` as the Streamlit UI and `dashboard_app.py` for the Azure IoT / Event Hub connector and state model.

## Run

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Set your IoT Hub connection string:

```bash
set AZURE_IOT_HUB_CONNECTION_STRING=your_connection_string
```

3. Start the dashboard:

```bash
streamlit run streamlit_dashboard.py
```

## Notes

- `dashboard_app.py` is the backend data processing code
- `streamlit_dashboard.py` is the UI entrypoint.
