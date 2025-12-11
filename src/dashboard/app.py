import streamlit as st
import pandas as pd
import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "sentiment",
    "password": "sentiment",
    "dbname": "sentimentdb",
}

def load_data(limit: int = 200):
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
        SELECT id, text, sentiment_label, sentiment_score, created_at
        FROM sentiments
        ORDER BY created_at DESC
        LIMIT %s;
    """
    df = pd.read_sql(query, conn, params=(limit,))
    conn.close()
    return df

st.set_page_config(page_title="Sentiment Stream Dashboard", layout="wide")

st.title("📊 Sentiment Stream Dashboard")

st.caption("Live messages processed from Kafka → Consumer → PostgreSQL")

# Button to refresh data
if st.button("🔄 Refresh data"):
    st.experimental_rerun()

df = load_data()

if df.empty:
    st.warning("No data yet. Run the producer to generate some messages.")
else:
    # Top-level metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Total messages", len(df))

    counts = df["sentiment_label"].value_counts()
    col2.metric("Positive", int(counts.get("positive", 0)))
    col3.metric("Negative", int(counts.get("negative", 0)))

    st.subheader("Recent messages")
    st.dataframe(df[["created_at", "sentiment_label", "sentiment_score", "text"]])

    st.subheader("Sentiment distribution")
    st.bar_chart(counts)

    # Optional: time-based chart
    st.subheader("Sentiment over time (count)")
    time_df = (
        df.groupby(["sentiment_label"])
          .resample("1min", on="created_at")
          .size()
          .reset_index(name="count")
    )
    time_pivot = time_df.pivot_table(
        index="created_at",
        columns="sentiment_label",
        values="count",
        fill_value=0,
    )
    st.line_chart(time_pivot)
