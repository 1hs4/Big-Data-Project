import time
import json
import requests
from kafka import KafkaProducer

TOPIC = "sentiment-topic"
API_URL = "https://jsonplaceholder.typicode.com/comments"

def fetch_comments():
    """
    Fetch a batch of comments from the public API.
    Returns a list of comment texts.
    """
    resp = requests.get(API_URL, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    # Take only the 'body' field from each comment
    return [item["body"] for item in data]

def main():
    print("Starting API producer...")

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    comments = fetch_comments()
    print(f"Fetched {len(comments)} comments from API")

    # Stream comments one by one
    for idx, text in enumerate(comments, start=1):
        message = {"text": text}
        producer.send(TOPIC, value=message)
        print(f"[{idx}] Sent from API:", text[:80].replace("\n", " ") + "...")
        time.sleep(1)  # slow down a bit so you can see the stream

    producer.flush()
    producer.close()
    print("Finished sending all API comments.")

if __name__ == "__main__":
    main()
