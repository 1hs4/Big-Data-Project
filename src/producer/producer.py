from kafka import KafkaProducer
import time
import json

TOPIC = "sentiment-topic"

def main():
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    messages = [
        "I love this product!",
        "This is terrible, I hate it.",
        "It’s okay, not the best but fine.",
        "Today is amazing, I feel great.",
        "I'm so disappointed with this service.",
    ]

    for msg in messages:
        data = {"text": msg}
        producer.send(TOPIC, value=data)
        print("Sent:", data)
        time.sleep(1)

    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()
