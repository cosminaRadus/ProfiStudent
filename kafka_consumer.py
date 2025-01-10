from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json

consumer = KafkaConsumer(
    "interaction_data",
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',  
    # enable_auto_commit=True,        
    # group_id="test-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) 
)

cluster = Cluster(['localhost'])
session = cluster.connect('interaction_data')

insert_query = """
    INSERT INTO student_logs  (
        student_id, course_code, interaction_type, timestamp_interaction,
        duration, location_country, location_city
    ) VALUES(%s, %s, %s, %s, %s, %s, %s)
"""

print("Waiting for messages...")
for message in consumer:
    data = message.value
    print(f"Received data: {data}")
    session.execute(insert_query,( 
        data['student_id'],
        data['course_code'],
        data['interaction_type'],
        data['timestamp'],
        data['duration'],
        data['location']['country'],
        data['location']['city']))
    
    print("Data inserted into Cassandra successfully!")

   
cluster.shutdown()
