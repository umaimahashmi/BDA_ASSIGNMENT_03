from kafka import KafkaProducer
import json
import time

topic = 'testtopic1'

# Kafka producer configuration
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Read and publish data
with open('Sampled_Amazon_Meta.json', 'r') as file:
    for line in file:
        data = json.loads(line)
        record = {
            "asin": data.get("asin", ""),
            "also_buy": data.get("also_buy", [])
        }
        producer.send(topic, value=record)
        print("Data published:", record)
        time.sleep(0.3)  # Adjust sleep time as per your requirement

# Flush and close the producer
producer.flush()
producer.close()

