📊 Real-Time Sentiment Analysis Pipeline

Kafka → Spark Structured Streaming → PostgreSQL → Streamlit Dashboard

This project is an end-to-end real-time data processing pipeline designed for ICS 474 (Big Data Systems).
It performs live sentiment analysis on streaming text messages using Kafka, PySpark, PostgreSQL, and Streamlit.

🚀 Architecture Overview
           ┌───────────────┐
           │   Producer     │
           │ (Python App)   │
           └───────┬───────┘
                   │ JSON messages
                   ▼
        ┌────────────────────────┐
        │        Kafka           │
        │   (Message Broker)     │
        └─────────┬──────────────┘
                  │ Streaming Data
                  ▼
       ┌──────────────────────────┐
       │  Spark Structured Stream │
       │  (Sentiment + ETL Layer) │
       └──────────┬───────────────┘
                  │ Cleaned/Scored Data
                  ▼
        ┌─────────────────────────┐
        │      PostgreSQL DB      │
        └──────────┬──────────────┘
                   │ Query Results
                   ▼
        ┌─────────────────────────┐
        │    Streamlit Dashboard  │
        └─────────────────────────┘


🧰 Tech Stack
| Component            | Technology                       |
| -------------------- | -------------------------------- |
| Messaging            | **Kafka** + Zookeeper            |
| Real-time processing | **PySpark Structured Streaming** |
| Storage              | **PostgreSQL**                   |
| Visualization        | **Streamlit**                    |
| Containerization     | **Docker Compose**               |
| Sentiment Model      | **VADER (NLTK)**                 |

📦 Project Structure

Big-Data-Project/
│
├── docker-compose.yml
├── README.md
├── requirements.txt
│
├── src/
│   ├── producer/
│   │   └── producer.py
│   │
│   ├── consumer/
│   │   ├── spark_consumer.py      (legacy simple consumer)
│   │   └── pyspark_consumer.py    (final Spark consumer)
│   │
│   ├── dashboard/
│   │   └── app.py
│   │
│   └── db/
│       └── init.sql
│
└── .gitignore


⚙️ Setup Instructions:

1️⃣ Install Required Tools
Make sure you have installed:
Docker Desktop
Python 3.10 (required for PySpark)
Java 11 (required for Spark)
Homebrew (macOS)

2️⃣ Create Virtual Environment:
python3.10 -m venv .venv310
source .venv310/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

3️⃣ Start Kafka + Postgres (Docker)
docker compose up -d

4️⃣ Run Spark Consumer (main processing engine)
source .venv310/bin/activate
python src/consumer/pyspark_consumer.py

5️⃣ Run the Producer (send messages)
source .venv310/bin/activate
python src/producer/producer.py

6️⃣ Run Streamlit Dashboard
streamlit run src/dashboard/app.py
then open : http://localhost:8501


📊 Database Schema
CREATE TABLE sentiments (
    id SERIAL PRIMARY KEY,
    text TEXT,
    sentiment_label VARCHAR(20),
    sentiment_score FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

🧠 Sentiment Analysis
Sentiment is computed using VADER, producing:
positive
negative
neutral
compound score

Spark UDFs apply this sentiment logic to each incoming message.


📈 Features

✔ Real-time streaming
✔ Automated sentiment scoring
✔ PySpark processing layer
✔ Kafka-backed ingestion
✔ PostgreSQL storage
✔ Interactive Streamlit Dashboard
✔ Fully containerized system
✔ Scalable architecture

🙌 Authors
Alhassan Alharbi, Alridha Al Maden, Husain Al Muallim, Basam Al-Ahmed.

🎓 License
This project is for educational purposes under ICS-474 (Big Data Systems).
King Fahd University of Petroleum and Minerals