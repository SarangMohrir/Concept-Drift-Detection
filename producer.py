import pandas as pd
import json
import time
from kafka import KafkaProducer

# Load SEA dataset (ensure correct headers)
df1 = pd.read_csv("SEA_training_class.csv", names=['x4'], header=None)
df2 = pd.read_csv("SEA_training_data.csv", names=['x1', 'x2', 'x3'], header=None)

# Merge input and output datasets
df = pd.concat([df2, df1], axis=1)

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'real_time_stream'

# Stream data row by row
for _, row in df.iterrows():
    data = {
        "x1": row["x1"],
        "x2": row["x2"],
        "x3": row["x3"],
        "x4": row["x4"]
    }
    producer.send(topic, data)
    print(f"Sent: {data}")
    time.sleep(0.5)  # Simulate real-time streaming

producer.close()
