from kafka import KafkaConsumer
import json

# Kafka settings
KAFKA_TOPIC = "hl7-events"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
CONSUMER_GROUP_ID = "hl7-consumer-group"

# Create Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=CONSUMER_GROUP_ID,
    auto_offset_reset='earliest',  # Read from beginning if new
    enable_auto_commit=True,
    value_deserializer=lambda x: x.decode('utf-8')
)

print(f"🔎 Listening to Kafka topic: {KAFKA_TOPIC}")
try:
    for message in consumer:
        hl7_data = message.value
        print("📥 Received HL7 Message:")
        print(json.dumps(hl7_data, indent=2))
        print("-" * 60)
except KeyboardInterrupt:
    print("❌ Consumer stopped manually.")
finally:
    consumer.close()
