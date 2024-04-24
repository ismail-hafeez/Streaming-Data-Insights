import json
from kafka import KafkaProducer
from time import sleep

# Kafka producer configuration
bootstrap_servers = ['localhost:9092']
topic = 'test1-topic'

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Path to the JSON file
json_file_path = 'outputFile.json'

# Read JSON data from file
with open(json_file_path, 'r') as file:
    data = json.load(file)

# Send JSON data to Kafka topic
for item in data:
    producer.send(topic, value=item)
    print('Sent data to Kafka:', item)
    sleep(2)

# Close the producer
producer.close()
