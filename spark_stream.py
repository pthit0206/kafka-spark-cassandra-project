import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)
from pyspark.sql.functions import from_json, col, expr, concat_ws, to_json


# ---------- Cassandra helpers ----------

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
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
    """)
    print("Table created successfully!")


def create_cassandra_connection():
    try:
        # Use explicit IPv4 to avoid ::1 issues
        cluster = Cluster(['127.0.0.1'])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


# ---------- Spark + Kafka ----------

def create_spark_connection():
    s_conn = None
    try:
        s_conn = (
            SparkSession.builder
            .appName("SparkDataStreaming")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
            )
            .config("spark.cassandra.connection.host", "127.0.0.1")
            .config("spark.cassandra.connection.port", "9042")
            .config("spark.cassandra.connection.localDC", "datacenter1")
            .getOrCreate()
        )
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = (
            spark_conn.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "user_created")  # topic name
            .option("startingOffsets", "earliest")
            .load()
        )
        logging.info("Kafka dataframe created successfully")
    except Exception as e:
        logging.exception(f"kafka dataframe could not be created because: {e}")

    return spark_df


# ---------- Transform Kafka JSON → flat columns ----------

def create_selection_df_from_kafka(spark_df):
    if spark_df is None:
        logging.error("Kafka DataFrame is None – cannot create selection DataFrame")
        return None

    # Schema that matches randomuser-like structure (simplified)
    schema = StructType([
        StructField("gender", StringType(), True),

        StructField("name", StructType([
            StructField("title", StringType(), True),
            StructField("first", StringType(), True),
            StructField("last", StringType(), True),
        ]), True),

        StructField("location", StructType([
            StructField("street", StructType([
                StructField("number", LongType(), True),
                StructField("name", StringType(), True),
            ]), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("postcode", StringType(), True),
        ]), True),

        StructField("email", StringType(), True),

        StructField("login", StructType([
            StructField("uuid", StringType(), True),
            StructField("username", StringType(), True),
        ]), True),

        StructField("registered", StructType([
            StructField("date", StringType(), True),
            StructField("age", LongType(), True),
        ]), True),

        StructField("phone", StringType(), True),

        StructField("picture", StructType([
            StructField("large", StringType(), True),
            StructField("medium", StringType(), True),
            StructField("thumbnail", StringType(), True),
        ]), True),
    ])

    # 1) Parse JSON from Kafka
    parsed = (
        spark_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
    )

    # 2) Flatten / transform into Cassandra columns
    sel_with_uuid = parsed.select(
        # Generate a valid UUID for Cassandra primary key
        expr("uuid()").alias("id"),

        # Name fields
        col("name.first").alias("first_name"),
        col("name.last").alias("last_name"),

        # Gender
        col("gender"),

        # Build a simple address string
        concat_ws(
            ", ",
            col("location.street.name"),
            col("location.city"),
            col("location.country")
        ).alias("address"),

        # Postcode as string
        col("location.postcode").cast("string").alias("postcode"),

        # Contact / account info
        col("email"),
        col("login.username").alias("username"),
        col("registered.date").alias("registered_date"),
        col("phone"),

        # Store picture as JSON string
        to_json(col("picture")).alias("picture"),
    )

    sel_with_uuid.printSchema()
    return sel_with_uuid


# ---------- Main ----------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    spark_conn = create_spark_connection()

    if spark_conn is None:
        print("❌ Could not create Spark connection. Check logs above.")
        raise SystemExit(1)

    spark_df = connect_to_kafka(spark_conn)

    if spark_df is None:
        print("❌ Kafka DataFrame is None. Check 'kafka dataframe could not be created because:' logs above.")
        spark_conn.stop()
        raise SystemExit(1)

    selection_df = create_selection_df_from_kafka(spark_df)

    cas_session = create_cassandra_connection()

    if cas_session is not None:
        create_keyspace(cas_session)
        create_table(cas_session)

        # Start writing streaming data into Cassandra
        streaming_query = (
            selection_df.writeStream
            .format("org.apache.spark.sql.cassandra")
            .option("checkpointLocation", "/tmp/checkpoint_created_users")
            .option("keyspace", "spark_streams")
            .option("table", "created_users")
            .start()
        )

        streaming_query.awaitTermination()
    else:
        logging.error("Cassandra session is None, cannot create keyspace/table")
        spark_conn.stop()