from kafka import KafkaConsumer
import json

TOPIC = "sentiment-topic"

def main():
    print("Starting consumer...")

    # Create Kafka consumer
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",   # start from beginning
        enable_auto_commit=True,
        group_id="test-consumer-group", # new group so we read old messages
    )

    print("Consumer connected. Listening for messages on topic:", TOPIC)

    for msg in consumer:
        print("Received raw:", msg.value)

if __name__ == "__main__":
    main()
