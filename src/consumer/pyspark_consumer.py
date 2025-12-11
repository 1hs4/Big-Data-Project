from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StringType, DoubleType

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ---------- Sentiment helpers ----------

analyzer = SentimentIntensityAnalyzer()

def get_label(text: str) -> str:
    if text is None:
        return None
    scores = analyzer.polarity_scores(text)
    c = scores["compound"]
    if c > 0.1:
        return "positive"
    elif c < -0.1:
        return "negative"
    else:
        return "neutral"

def get_score(text: str) -> float:
    if text is None:
        return 0.0
    scores = analyzer.polarity_scores(text)
    return float(scores["compound"])

label_udf = udf(get_label, StringType())
score_udf = udf(get_score, DoubleType())

# ---------- Spark session ----------

spark = SparkSession.builder \
    .appName("KafkaSentimentSpark") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------- Read from Kafka ----------

TOPIC = "sentiment-topic"

schema = StructType().add("text", StringType())

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

value_df = kafka_df.selectExpr("CAST(value AS STRING)")

json_df = value_df \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

scored_df = json_df \
    .withColumn("sentiment_label", label_udf(col("text"))) \
    .withColumn("sentiment_score", score_udf(col("text")))

# ---------- Write each micro-batch to PostgreSQL ----------

def write_to_postgres(batch_df, batch_id):
    import psycopg2

    if batch_df.rdd.isEmpty():
        return

    rows = batch_df.select("text", "sentiment_label", "sentiment_score").collect()

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="sentiment",
        password="sentiment",
        dbname="sentimentdb",
    )
    cur = conn.cursor()

    for r in rows:
        cur.execute(
            """
            INSERT INTO sentiments (text, sentiment_label, sentiment_score)
            VALUES (%s, %s, %s)
            """,
            (r["text"], r["sentiment_label"], float(r["sentiment_score"])),
        )

    conn.commit()
    cur.close()
    conn.close()

query = scored_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .start()

query.awaitTermination()
