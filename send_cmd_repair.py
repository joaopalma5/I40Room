import time
import pysparkplug as psp
import paho.mqtt.client as mqtt  # For MQTT 5.0 and STATE publishing

# Config matching your setup
BROKER_ADDRESS = "localhost"
BROKER_PORT = 1883
GROUP_ID = "Automotive"
NODE_ID = "FactoryFloor"
DEVICE_ID = "SolenoidValve1"
HOST_ID = "SCADA_Primary"  # Matches the device's scadaHostIdentifier

# Create separate Paho client for STATE messages (plain text, not Sparkplug)
# Use MQTTv5 if desired, but since pysparkplug may use 3.1.1, keep consistent or separate.
state_client = mqtt.Client(client_id=f"{HOST_ID}_State")  # Default to MQTT 3.1.1, or add protocol=mqtt.MQTTv5 if needed
state_client.connect(BROKER_ADDRESS, BROKER_PORT, keepalive=60)
state_client.loop_start()

# Publish host STATE "ONLINE"
state_topic = f"STATE/{HOST_ID}"
state_client.publish(state_topic, "ONLINE", qos=1, retain=True)
print(f"Published STATE ONLINE to {state_topic}")

# Set last will for "OFFLINE" on disconnect
state_client.will_set(state_topic, "OFFLINE", qos=1, retain=True)

# Create Sparkplug Client for DCMD
client = psp.Client()

# Connect Sparkplug client (defaults to port 1883)
client.connect(BROKER_ADDRESS)

# Wait for connection
time.sleep(1)

# Create metrics including "Device Control/Repair" and "bdSeq" to match your example
metrics = [
    psp.Metric(
        timestamp=psp.get_current_timestamp(),
        name="Device Control/Repair",
        datatype=psp.DataType.BOOLEAN,
        value=True
    ),
    psp.Metric(
        timestamp=psp.get_current_timestamp(),
        name="bdSeq",
        datatype=psp.DataType.UINT64,
        value=0
    )
]

# Create DCMD payload using DCmd
payload = psp.DCmd(
    timestamp=psp.get_current_timestamp(),
    metrics=metrics
)

# Build DCMD topic
topic = psp.Topic(
    message_type=psp.MessageType.DCMD,
    group_id=GROUP_ID,
    edge_node_id=NODE_ID,
    device_id=DEVICE_ID
)

# Publish the DCMD
client.publish(
    psp.Message(
        topic=topic,
        payload=payload,
        qos=psp.QoS.AT_LEAST_ONCE,
        retain=False
    ),
    include_dtypes=True
)
print("Repair command sent.")

# Keep running briefly to ensure delivery
time.sleep(5)

# Disconnect
client.disconnect()
state_client.disconnect()
state_client.loop_stop()