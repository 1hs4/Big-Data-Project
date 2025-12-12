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

def load_data(limit: int = 15000):
    try:
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
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()

st.set_page_config(page_title="Sentiment Stream Dashboard", layout="wide")

st.title("📊 Sentiment Stream Dashboard")
st.caption("Live messages processed from Kafka → Spark → PostgreSQL")

if st.button("🔄 Refresh data"):
    st.rerun()

df = load_data()

# DEBUG: show how many rows we actually loaded from DB
st.write(f"🔎 Debug: loaded {len(df)} rows from PostgreSQL")

if df.empty:
    st.warning("No data yet. Run the producer to generate some messages.")
else:
    col1, col2, col3 = st.columns(3)
    col1.metric("Total messages", len(df))

    counts = df["sentiment_label"].value_counts()
    col2.metric("Positive", int(counts.get("positive", 0)))
    col3.metric("Negative", int(counts.get("negative", 0)))

    st.subheader("Recent messages")
    st.dataframe(df[["created_at", "sentiment_label", "sentiment_score", "text"]])

    st.subheader("Sentiment distribution")
    st.bar_chart(counts)

    st.subheader("Sentiment over time (per minute)")
    time_df = (
        df.set_index("created_at")
          .groupby("sentiment_label")
          .resample("1min")
          .size()
          .unstack(0)
          .fillna(0)
    )
    st.line_chart(time_df)
