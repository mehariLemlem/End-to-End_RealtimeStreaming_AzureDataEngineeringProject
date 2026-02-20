# Import Event Hub Libraries
from azure.eventhub import  EventHubProducerClient, EventData
import json

# Event Hub Configuration
EVENT_HUB_CONNECTION_STRING = "connection-string-key-from-keyVault"
EVENT_HUB_NAME = "eventhub-name"

# Initialize the Event Hub producer
producer = EventHubProducerClient.from_connection_string(conn_str=EVENT_HUB_CONNECTION_STRING, eventhub_name=EVENT_HUB_NAME)

# Function to send events to Event Hub
def send_event(event):
    event_data_batch =producer.create_batch()
    event_data_batch.add(EventData(json.dumps(event)))
    producer.send_batch(event_data_batch)

# Sample JSON event
event = {
    "event_id": 111,
    "event_name": "Test Event",
}  

# Send the Event
send_event(event)

# Close the producer
producer.close()