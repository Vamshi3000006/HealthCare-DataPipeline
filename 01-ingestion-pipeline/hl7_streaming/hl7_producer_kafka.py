import json
from kafka import KafkaProducer
from hl7apy import parser
from hl7apy.core import Message


# Kafka config
KAFKA_BOOTSTRAP_SERVER = 'localhost:9092'
KAFKA_TOPIC = 'hl7-events'


def parse_hl7_to_json(hl7_string):
    try:
        msg = parser.parse_message(hl7_string.replace('\n', '\r'), find_groups=True)
        if isinstance(msg, Message):
            return msg.to_dict()
        else:
            print("[⚠️] Parsed object is not a full HL7 Message. Falling back to raw HL7 string.")
            return None
    except Exception as e:
        print(f"[❌] HL7 parse error: {e}")
        return None

if __name__ == "__main__":
    # Load HL7 message from file
    try:
        with open("sample_hl7.txt", "r") as f:
            hl7_raw = f.read().strip()
    except FileNotFoundError:
        print("[❌] 'sample_hl7.txt' not found.")
        exit(1)

    # Parse HL7 to JSON
    hl7_json = parse_hl7_to_json(hl7_raw)
    print(json.dumps(hl7_json, indent=2))

    # If parsing failed, send raw HL7 instead
    if hl7_json is None:
        hl7_json = {"hl7_raw": hl7_raw}
        print("[⚠️] Sending raw HL7 message instead of parsed JSON.")


    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        value_serializer=lambda v: v.encode('utf-8')
    )
    

    # Send to Kafka
    try:
        # hl7_producer_kafka.py
        producer.send("hl7-events", hl7_raw)

        producer.flush()
        print(f"✅ HL7 JSON message sent to Kafka topic: {KAFKA_TOPIC}")
    except Exception as e:
        print(f"[❌] Kafka send failed: {e}")
