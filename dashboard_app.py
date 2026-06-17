import json
import logging
import os
import re
import socket
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

# Note: Dash UI removed — Streamlit UI lives in `streamlit_dashboard.py`
from azure.eventhub import EventHubConsumerClient


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class SensorType(Enum):
    TEMPERATURE = "temperature"
    HUMIDITY = "humidity"
    PRESSURE = "pressure"
    ACCELEROMETER = "accelerometer"
    GYROSCOPE = "gyroscope"
    GPS = "gps"
    LIGHT = "light"
    SOUND = "sound"
    CUSTOM = "custom"


class BoardType(Enum):
    ESP32 = "esp32"
    ARDUINO = "arduino"
    RASPBERRY_PI = "raspberry_pi"
    JETSON_NANO = "jetson_nano"
    CUSTOM = "custom"


@dataclass
class SensorReading:
    sensor_id: str
    sensor_type: SensorType
    value: float
    unit: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sensor_id": self.sensor_id,
            "sensor_type": self.sensor_type.value,
            "value": self.value,
            "unit": self.unit,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
        }


@dataclass
class DeviceMessage:
    message_id: str = field(default_factory=lambda: str(datetime.now(timezone.utc).timestamp()))
    device_id: str = ""
    board_type: BoardType = BoardType.CUSTOM
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    readings: List[SensorReading] = field(default_factory=list)
    raw_payload: Optional[Dict[str, Any]] = None

    @classmethod
    def from_azure_event(cls, event_payload: Dict[str, Any]) -> "DeviceMessage":
        if "event" in event_payload:
            event = event_payload["event"]
            device_id = event.get("origin", "unknown")
            raw_payload = event.get("payload", "")
            if isinstance(raw_payload, dict):
                raw_payload = raw_payload.get("payload") or raw_payload.get("data") or json.dumps(raw_payload)
            payload_str = str(raw_payload).strip()
            
            sensor_full, value, unit = cls._parse_payload_string(payload_str)

            if ":" in sensor_full:
                device_type, sensor = sensor_full.split(":", 1)
            else:
                device_type, sensor = "unknown", sensor_full

            sensor_type = cls._determine_sensor_type(sensor, unit)
            return cls(
                message_id=str(datetime.now(timezone.utc).timestamp()),
                device_id=device_id,
                board_type=BoardType.CUSTOM,
                timestamp=datetime.now(timezone.utc),
                readings=[
                    SensorReading(
                        sensor_id=sensor,
                        sensor_type=sensor_type,
                        value=value,
                        unit=unit,
                        metadata={
                        "device_type": device_type,
                        "module": event.get("module", ""),
                        "interface": event.get("interface", ""),
                        "component": event.get("component", ""),
                    },
                    )
                ],
                raw_payload=event_payload,
                
            )

        return cls(
            message_id=event_payload.get("messageId", str(datetime.now(timezone.utc).timestamp())),
            device_id=event_payload.get("deviceId", event_payload.get("deviceId", "unknown")),
            board_type=BoardType(event_payload.get("boardType", "custom")),
            timestamp=datetime.fromisoformat(event_payload.get("timestamp", datetime.now(timezone.utc).isoformat())),
            readings=[
                SensorReading(
                    sensor_id=r.get("sensorId", "unknown"),
                    sensor_type=SensorType(r.get("sensorType", "custom")),
                    value=r.get("value", 0.0),
                    unit=r.get("unit", ""),
                    metadata=r.get("metadata", {}),
                )
                for r in event_payload.get("readings", [])
            ],
            raw_payload=event_payload,
        )

    @staticmethod
    def _parse_payload_string(payload_str: str) -> tuple[str, float, str]:
        payload_str = str(payload_str).strip()
        if not payload_str:
            return "unknown:unknown", 0.0, ""

        patterns = [
            r"^(?P<device>[^/]+)/(?P<sensor>[^:]+):\s*(?P<value>[+-]?\d*\.?\d+)\s*(?P<unit>.+)$",
            r"^(?P<sensor>[^:]+):\s*(?P<value>[+-]?\d*\.?\d+)\s*(?P<unit>.+)$",
        ]

        for pattern in patterns:
            match = re.match(pattern, payload_str)
            if match:
                device = match.groupdict().get("device")
                sensor = match.groupdict().get("sensor", "unknown").strip()
                value = float(match.group("value"))
                unit = match.groupdict().get("unit", "").strip()
                if device:
                    return f"{device}:{sensor}", value, unit
                return sensor, value, unit

        return "unknown:unknown", 0.0, ""

    @staticmethod
    def _determine_sensor_type(sensor_id: str, unit: str) -> SensorType:
        sensor_id_lower = sensor_id.lower()
        unit_lower = unit.lower()
        if "temp" in sensor_id_lower or unit_lower in {"c", "°c", "f", "°f", "k"}:
            return SensorType.TEMPERATURE
        if "humid" in sensor_id_lower or "%" in unit_lower:
            return SensorType.HUMIDITY
        return SensorType.CUSTOM

    @classmethod
    def from_raw_body(cls, body: str, device_id: str = "unknown", metadata: Optional[Dict[str, Any]] = None) -> "DeviceMessage":
        payload_str = str(body).strip()
        if not payload_str:
            return cls()
        sensor_full, value, unit = cls._parse_payload_string(payload_str)

        if ":" in sensor_full:
            device_type, sensor = sensor_full.split(":", 1)
        else:
            device_type, sensor = "unknown", sensor_full

        sensor_type = cls._determine_sensor_type(sensor, unit)

        merged_metadata = {"raw_body": payload_str}
        if metadata:
            merged_metadata.update(metadata)

        return cls(
            message_id=str(datetime.now(timezone.utc).timestamp()),
            device_id=device_id,
            board_type=BoardType.CUSTOM,
            timestamp=datetime.now(timezone.utc),
            readings=[
                SensorReading(
                    sensor_id=sensor,
                    sensor_type=sensor_type,
                    value=value,
                    unit=unit,
                    metadata=merged_metadata,
                )
            ],
            raw_payload={"body": payload_str, **(metadata or {})},
        )


class AzureIoTConnector:
    def __init__(self, iothub_connection_string: str, consumer_group: str = "$Default"):
        self.connection_string = iothub_connection_string
        self.consumer_group = consumer_group
        self._client: Optional[EventHubConsumerClient] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None

    @staticmethod
    def _parse_connection_string(connection_string: str) -> Dict[str, str]:
        parts = {}
        for part in connection_string.split(";"):
            if "=" in part:
                key, value = part.split("=", 1)
                parts[key] = value
        return parts

    def _validate_namespace(self, hostname: str) -> None:
        try:
            socket.gethostbyname(hostname)
        except OSError as exc:
            raise ConnectionError(
                f"Unable to resolve Event Hub namespace host '{hostname}'. "
                "Check your network, DNS, VPN, or proxy settings."
            ) from exc

    def _build_eventhub_connection(self) -> str:
        parts = self._parse_connection_string(self.connection_string)
        hostname = parts.get("HostName", "").strip("/")
        key_name = parts.get("SharedAccessKeyName", "").strip()
        key = parts.get("SharedAccessKey", "").strip()
        entity_path = parts.get("EntityPath", "").strip("/")

        if not hostname or not key_name or not key or not entity_path:
            raise ValueError("Invalid IoT Hub connection string: missing required fields.")

        eventhub_hostname = hostname.replace(".azure-devices.net", ".servicebus.windows.net")
        self._validate_namespace(eventhub_hostname)
        return f"Endpoint=sb://{eventhub_hostname}/;SharedAccessKeyName={key_name};SharedAccessKey={key};EntityPath={entity_path}"

    def start(self, callback: Callable[[DeviceMessage], None]) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._run, args=(callback,), daemon=True)
        self._thread.start()

    def _run(self, callback: Callable[[DeviceMessage], None]) -> None:
        connection_string = self._build_eventhub_connection()
        self._client = EventHubConsumerClient.from_connection_string(
            conn_str=connection_string,
            consumer_group=self.consumer_group,
        )

        def on_event(partition_context, event):
            if not self._running:
                return
            body = event.body_as_str(encoding="UTF-8")
            if not body or not body.strip():
                logger.warning("Received empty event body")
                return
            event_device_id = "unknown_device"
            event_metadata: Dict[str, Any] = {}

            if hasattr(event, "system_properties") and event.system_properties:
                event_metadata.update(event.system_properties)

                raw_device_id = event.system_properties.get(b"iothub-connection-device-id")

                if raw_device_id:
                    if isinstance(raw_device_id, bytes):
                        event_device_id = raw_device_id.decode()
                    else:
                        event_device_id = str(raw_device_id)

            print("✅ EXTRACTED DEVICE ID:", event_device_id)

            logger.info("Received Event Hub body: %r (device=%s)", body, event_device_id)

            def process_payload(payload: Any) -> None:
                if isinstance(payload, dict):
                    if "device_id" in payload:
                        message = DeviceMessage(
                            device_id=payload.get("device_id", event_device_id),
                            board_type=BoardType.CUSTOM,
                            timestamp=datetime.now(timezone.utc),
                            readings=[
                                SensorReading(
                                    sensor_id=payload.get("sensor", "unknown"),
                                    sensor_type=SensorType.TEMPERATURE,  # or infer later
                                    value=float(payload.get("value", 0)),
                                    unit=payload.get("unit", ""),
                                    metadata={
                                        "device_type": payload.get("device_type", "unknown")
                                    }
                                )
                            ],
                            raw_payload=payload,
                        )

                        callback(message)
                        return

                    # ✅ CASE 2: old event wrapper
                    message = DeviceMessage.from_azure_event(payload)
                    callback(message)
                elif isinstance(payload, list):
                    for item in payload:
                        process_payload(item)
                elif isinstance(payload, str):
                    # ✅ fallback to origin if available inside JSON wrapper
                    #origin_device_id = event_device_id

                    message = DeviceMessage.from_raw_body(
                        payload,
                        device_id=event_device_id,   # ✅ FIXED
                        metadata={"eventSystemProperties": event_metadata}
                    )
                    logger.info(
                        "Parsed raw text payload for device=%s sensor=%s value=%s unit=%s",
                        message.device_id,
                        message.readings[0].sensor_id if message.readings else "<none>",
                        message.readings[0].value if message.readings else "<none>",
                        message.readings[0].unit if message.readings else "",
                    )
                    callback(message)
                else:
                    logger.warning("Unsupported payload type: %s", type(payload))

            try:
                payload = json.loads(body)
                process_payload(payload)
            except json.JSONDecodeError:
                logger.info("Event body is not JSON; attempting multi-object parse: %r", body)
                decoder = json.JSONDecoder()
                idx = 0
                body_length = len(body)
                while idx < body_length:
                    while idx < body_length and body[idx].isspace():
                        idx += 1
                    if idx >= body_length:
                        break
                    try:
                        payload, consumed = decoder.raw_decode(body, idx)
                        process_payload(payload)
                        idx += consumed
                    except json.JSONDecodeError:
                        next_sep = body.find("\n\n", idx)
                        if next_sep == -1:
                            next_sep = body_length
                        chunk = body[idx:next_sep].strip()
                        if chunk:
                            try:
                                payload = json.loads(chunk)
                                process_payload(payload)
                            except json.JSONDecodeError:
                                for line in chunk.splitlines():
                                    line = line.strip()
                                    if not line:
                                        continue
                                    message = DeviceMessage.from_raw_body(
                                        line,
                                        device_id=event_device_id,
                                        metadata={"eventSystemProperties": event_metadata}
                                    )
                                    callback(message)
                        idx = next_sep + 2
            except Exception as exc:
                logger.warning("Error converting event body to DeviceMessage: %s", exc)
                return

            try:
                partition_context.update_checkpoint()
            except Exception as exc:
                logger.debug("Checkpoint update failed: %s", exc)

        try:
            logger.info("Starting Event Hub consumer...")
            with self._client:
                self._client.receive(on_event=on_event, starting_position="@latest")
        except Exception as exc:
            logger.error("Event Hub receive loop stopped: %s", exc)

    def stop(self) -> None:
        self._running = False
        if self._client:
            try:
                self._client.close()
            except Exception as exc:
                logger.debug("Error closing Event Hub client: %s", exc)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)


class TimeSeriesBuffer:
    def __init__(self, max_size: int = 1000, window_seconds: int = 3600):
        self.max_size = max_size
        self.window_seconds = window_seconds
        self._buffers: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_size))
        self._lock = threading.Lock()

    def add(self, device_id: str, sensor_id: str, value: float, timestamp: Optional[datetime] = None) -> None:
        timestamp = timestamp or datetime.now(timezone.utc)
        key = f"{device_id}:{sensor_id}"
        with self._lock:
            self._buffers[key].append({"value": value, "timestamp": timestamp})

    def get_recent(self, device_id: str, sensor_id: str, seconds: Optional[int] = None) -> List[Dict[str, Any]]:
        seconds = seconds or self.window_seconds
        key = f"{device_id}:{sensor_id}"
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=seconds)
        with self._lock:
            return [entry for entry in self._buffers[key] if entry["timestamp"] >= cutoff]


class DashboardState:
    def __init__(self):
            self.devices = {}
            self.latest_readings = {}
            self.history = {}


    def update(self, msg):
        device_id = msg.device_id

        # ✅ Add device
        if device_id not in self.devices:
            self.devices[device_id] = {}

        # ✅ Process readings
        for reading in msg.readings:
            key = f"{device_id}:{reading.sensor_id}"

            # latest reading
            self.latest_readings[key] = {
                "value": reading.value,
                "unit": reading.unit,
                "timestamp": reading.timestamp
            }
            # history
            if key not in self.history:
                self.history[key] = []

            self.history[key].append({
                "value": reading.value,
                "timestamp": reading.timestamp
            })
        print("UPDATING:", msg.device_id)
    def snapshot(self):
        return {
            "devices": self.devices,
            "latest_readings": self.latest_readings,
            "history": self.history
        }

    def device_options(self) -> List[Dict[str, str]]:
        with self._lock:
            return [
                {
                    "label": f"{device_id} ({info.get('device_type', 'unknown')})",
                    "value": device_id,
                }
                for device_id, info in self._devices.items()
            ]

    def sensor_options(self, device_id: str) -> List[Dict[str, str]]:
        with self._lock:
            return [
                {"label": key.split(":", 1)[1], "value": key}
                for key in self._latest_readings
                if key.startswith(f"{device_id}:")
            ]


# Note: Dash UI removed. Use `streamlit_dashboard.py` for the Streamlit interface.
