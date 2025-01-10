import json
import uuid
import time
from kafka import KafkaProducer
from generate_data import data_batch
from google.cloud import storage
from kafka.errors import KafkaError


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

client = storage.Client.from_service_account_json('potent-app-439210-c8-0e060647490f.json')
bucket = client.get_bucket('interaction_bucket')

def write_to_gcs(data):
    log_data = json.dumps(data)
    timestamp_ms = int(time.time() * 1000)  # Current time in milliseconds
    unique_id = uuid.uuid4().hex[:8]  # Generate a short unique ID for the batch (8 characters)
    
    gcs_filename = f'log_{timestamp_ms}_{unique_id}.json'
    blob = bucket.blob(gcs_filename)

    blob.upload_from_string(log_data, content_type='application/json')
    print(f"Log written to GCS: {gcs_filename}")
    

for data in data_batch:
    try:
        future = producer.send("interaction_data", data)
        result = future.get(timeout=10) 
        print(f"Sent message: {data}") 
    except KafkaError as e:
        print(f"Failed to send message: {e}")
    
write_to_gcs(data_batch)

producer.flush()
producer.close()
