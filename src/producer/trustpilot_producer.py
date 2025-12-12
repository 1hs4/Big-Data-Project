import time
import json
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer

TRUSTPILOT_URL = "https://www.trustpilot.com/review/mcdonalds.com"

TOPIC = "sentiment-topic"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}


def fetch_reviews():
    """
    Fetch reviews from a Trustpilot company page.
    Returns a list of dicts with rating + text.
    """
    resp = requests.get(TRUSTPILOT_URL, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    html = resp.text
    soup = BeautifulSoup(html, "lxml")

    reviews = []

    # Trustpilot layout may change; this is a best-effort scraper
    articles = soup.find_all("article")
    for art in articles:
        # rating is often stored as 'Rated 5 out of 5' in an aria-label or alt
        rating_tag = art.find(attrs={"data-service-review-rating": True})
        rating = None
        if rating_tag:
            try:
                rating = int(rating_tag["data-service-review-rating"])
            except Exception:
                rating = None

        # Fallback: look for "Rated X out of 5" text
        if rating is None:
            rating_span = art.find("span", attrs={"class": lambda c: c and "star-rating" in c})
            if rating_span and rating_span.get("data-service-review-rating"):
                try:
                    rating = int(rating_span["data-service-review-rating"])
                except Exception:
                    rating = None

        # review text (main paragraph)
        text_tag = art.find("p")
        if not text_tag:
            continue
        text = text_tag.get_text(strip=True)

        if not text:
            continue

        reviews.append(
            {
                "rating": rating,
                "text": text,
            }
        )

    return reviews


def main():
    print(f"Streaming Trustpilot reviews from: {TRUSTPILOT_URL}")

    while True:
        try:
            reviews = fetch_reviews()
            if not reviews:
                print("No reviews found on this page.")
            for idx, r in enumerate(reviews, start=1):
                # send text field so your Spark consumer + sentiment still work
                msg = {
                    "text": r["text"],
                    "rating": r["rating"],
                    "source": "trustpilot",
                }
                producer.send(TOPIC, msg)
                print(f"Sent review #{idx}: rating={r['rating']} text={r['text'][:80]}...")
                time.sleep(1)  # slow down a bit
        except Exception as e:
            print("Error fetching/sending reviews:", e)

        # wait before fetching the page again
        time.sleep(30)


if __name__ == "__main__":
    main()
