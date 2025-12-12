from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StringType, DoubleType

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ---------- Sentiment ----------
analyzer = SentimentIntensityAnalyzer()

def get_label(text):
    if text is None:
        return None
    c = analyzer.polarity_scores(text)["compound"]
    if c > 0.1:
        return "positive"
    elif c < -0.1:
        return "negative"
    return "neutral"

def get_score(text):
    if text is None:
        return 0.0
    return float(analyzer.polarity_scores(text)["compound"])

label_udf = udf(get_label, StringType())
score_udf = udf(get_score, DoubleType())

# ---------- Spark session ----------
spark = SparkSession.builder \
    .appName("KafkaSparkSentiment") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------- Kafka Source ----------
TOPIC = "sentiment-topic"

schema = StructType().add("text", StringType())

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

value_df = kafka_df.selectExpr("CAST(value AS STRING)")

json_df = value_df.select(from_json(col("value"), schema).alias("data")) \
                  .select("data.*")

scored_df = json_df \
    .withColumn("sentiment_label", label_udf(col("text"))) \
    .withColumn("sentiment_score", score_udf(col("text")))

# ---------- Write to PostgreSQL ----------
def write_to_postgres(batch_df, batch_id):
    import psycopg2

    if batch_df.rdd.isEmpty():
        return

    rows = batch_df.collect()

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="sentiment",
        password="sentiment",
        dbname="sentimentdb"
    )
    cur = conn.cursor()

    for row in rows:
        cur.execute(
            """
            INSERT INTO sentiments (text, sentiment_label, sentiment_score)
            VALUES (%s, %s, %s)
            """,
            (row["text"], row["sentiment_label"],
             float(row["sentiment_score"]))
        )

    conn.commit()
    cur.close()
    conn.close()

query = scored_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .start()

query.awaitTermination()
