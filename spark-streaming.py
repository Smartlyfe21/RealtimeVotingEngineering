import os
import json
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import psycopg2
import psycopg2.extras
from datetime import datetime

#  Setup Checkpoints
os.makedirs("checkpoints/checkpoint_postgres", exist_ok=True)
os.makedirs("checkpoints/checkpoint_console", exist_ok=True)

# Spark Session with exact .jar paths
spark = SparkSession.builder \
    .appName("RealtimeVotingSparkStreaming") \
    .config("spark.jars", ",".join([
        "/Users/smartlyfe/Desktop/RealtimeVotingEngineering/kafka_2.13-4.1.0/libs/spark-sql-kafka-0-10_2.12-3.5.1.jar",
        "/Users/smartlyfe/Desktop/RealtimeVotingEngineering/kafka_2.13-4.1.0/libs/scala-library-2.13.12.jar",
        "/Users/smartlyfe/Desktop/RealtimeVotingEngineering/kafka_2.13-4.1.0/libs/postgresql-42.7.8.jar",
        "/Users/smartlyfe/Desktop/RealtimeVotingEngineering/kafka_2.13-4.1.0/libs/kafka-clients-3.5.1.jar"
    ])) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka and Postgres Config
kafka_bootstrap_servers = "localhost:29092"
kafka_topic = "votes"

PG_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'dbname': 'votes_db',
    'user': 'admin',
    'password': 'admin'
}

#  Define vote schema
vote_schema = StructType([
    StructField("district", StringType(), True),
    StructField("candidate_id", StringType(), True),
    StructField("candidate_name", StringType(), True),
    StructField("slogan", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Read streaming votes from Kafka
votes_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

votes_parsed = votes_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), vote_schema).alias("data")) \
    .select("data.*")

#  Aggregate votes per candidate and district
vote_counts = votes_parsed.groupBy("district", "candidate_name") \
    .count() \
    .withColumnRenamed("count", "vote_count")

#  Function to write to Postgres ---
def write_to_postgres(df, epoch_id):
    if df.count() == 0:
        return

    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()

    # Convert Spark DF to list of tuples
    data_tuples = [
        (
            row['district'],
            row.get('candidate_id', 'unknown'),
            row['candidate_name'],
            row.get('slogan', ''),
            row['vote_count'],
            datetime.now()
        )
        for row in df.collect()
    ]

    # Bulk insert/upsert into Postgres
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO votes_stream (district, candidate_id, candidate_name, slogan, vote_count, timestamp)
        VALUES %s
        ON CONFLICT (district, candidate_name) DO UPDATE
        SET vote_count = EXCLUDED.vote_count,
            timestamp = EXCLUDED.timestamp
        """,
        data_tuples
    )

    conn.commit()
    cur.close()
    conn.close()

# Spark write streams
# Write to Postgres
postgres_query = vote_counts.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "checkpoints/checkpoint_postgres") \
    .start()

# Also write to console for debugging
console_query = vote_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "checkpoints/checkpoint_console") \
    .start()

#  termination
postgres_query.awaitTermination()
console_query.awaitTermination()
