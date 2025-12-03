# 🚀 Real-Time Data Pipeline: Airflow → Kafka → Spark Streaming → Cassandra

This project demonstrates a complete **real-time data engineering pipeline** built using:

- **Apache Airflow** (Producer / Orchestration)
- **Apache Kafka** (Message broker)
- **Apache Spark Structured Streaming** (Consumer + Processing)
- **Apache Cassandra** (NoSQL storage)
- **Docker Compose** (Environment setup)

The pipeline fetches live user data from the RandomUser API, streams it through Kafka, processes it using Spark Streaming, and finally stores the curated data in Cassandra.

---

## 📌 **Architecture Diagram**


---

## 🧰 **Tech Stack**

| Component | Purpose |
|----------|---------|
| **Airflow** | Schedules + orchestrates API → Kafka streaming |
| **Kafka** | Stores real-time messages in `user_created` topic |
| **Spark Streaming** | Reads Kafka stream, transforms JSON, writes to Cassandra |
| **Cassandra** | Stores final curated user records |
| **Docker Compose** | Runs Kafka, Zookeeper, Airflow, Cassandra, Spark |

---

## 📦 Folder Structure

kafka-spark-cassandra-project/
│
├── docker-compose.yml          # Runs Kafka, Zookeeper, Cassandra, Airflow, Spark
├── requirements.txt            # Airflow dependencies
├── spark_stream.py             # Spark Structured Streaming consumer
│
├── dags/
│   └── kafka_stream.py        # Airflow DAG (Kafka producer)
│
└── README.md


---

## 🔄 **Pipeline Flow Explanation**

### **1️⃣ Airflow DAG — Producer**
- Fetches real-time user data from **RandomUser API**
- Converts JSON → bytes
- Sends messages to **Kafka topic `user_created`**
- Sends 5 messages per manual run (configurable)

### **2️⃣ Kafka**
- Acts as the **message broker**
- Receives messages from Airflow
- Stores them in the topic `user_created`

### **3️⃣ Spark Streaming — Consumer**
- Connects to Kafka → reads the stream in real-time
- Parses and flattens nested JSON
- Generates UUID for each record
- Writes structured data to Cassandra

### **4️⃣ Cassandra**
Stores the final data in:

**Keyspace:** `spark_streams`  
**Table:** `created_users`

---

## 🗂️ **Cassandra Table Structure**

```sql
CREATE TABLE spark_streams.created_users (
    id UUID PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    address TEXT,
    postcode TEXT,
    email TEXT,
    username TEXT,
    registered_date TEXT,
    phone TEXT,
    picture TEXT
);
