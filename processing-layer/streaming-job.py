from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, round
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# -------------------------------------
# Spark Session
# -------------------------------------
spark = SparkSession.builder \
    .appName("EngagementEventsProcessor") \
    .config("spark.jars", "/path/to/postgresql-driver.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------------
# Schema for engagement events
# -------------------------------------
engagement_schema = StructType([
    StructField("event_id", StringType()),
    StructField("content_id", StringType()),
    StructField("user_id", StringType()),
    StructField("duration_ms", LongType()),
    StructField("event_time", TimestampType())
])

# -------------------------------------
# Stream from Kafka (engagement_events)
# -------------------------------------
engagement_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "engagement_events")
        .option("startingOffsets", "latest")
        .load()
)

parsed_events = engagement_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), engagement_schema).alias("event")) \
    .select("event.*")

# -------------------------------------
# Load content table from PostgreSQL
# -------------------------------------
content_df = (
    spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/mydb")
        .option("dbtable", "content")
        .option("user", "myuser")
        .option("password", "mypassword")
        .option("driver", "org.postgresql.Driver")
        .load()
        .select("content_id", "content_type", "length_seconds")
)

# Broadcast static content_df
from pyspark.sql.functions import broadcast
content_broadcast = broadcast(content_df)

# -------------------------------------
# Enrichment Join
# -------------------------------------
enriched = parsed_events.join(content_broadcast, "content_id", "left")

# -------------------------------------
# Transformations
# -------------------------------------
transformed = (
    enriched.withColumn("engagement_seconds", (col("duration_ms")/1000).cast("double"))
            .withColumn("engagement_pct", 
                        round(expr("CASE WHEN length_seconds IS NOT NULL AND duration_ms IS NOT NULL "
                                   "THEN (duration_ms/1000.0)/length_seconds ELSE NULL END"), 2))
)

# -------------------------------------
# Write to BigQuery (sink 1)
# -------------------------------------
(transformed.writeStream
    .format("bigquery")
    .option("table", "my_project.my_dataset.engagement_enriched")
    .option("checkpointLocation", "/tmp/chk/bigquery")
    .outputMode("append")
    .start()
)

# -------------------------------------
# Write to Redis (sink 2)
# -------------------------------------
def write_to_redis(batch_df, batch_id):
    import redis
    r = redis.Redis(host="localhost", port=6379, db=0)

    for row in batch_df.collect():
        key = f"content:{row.content_id}"
        r.hincrbyfloat(key, "total_engagement", row.engagement_seconds or 0.0)
        r.hincrby(key, "event_count", 1)
        r.expire(key, 600)  # keep for 10 mins to support "last 10 minutes"
        
(transformed.writeStream
    .foreachBatch(write_to_redis)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/chk/redis")
    .start()
)

# -------------------------------------
# Send to external system (sink 3)
# -------------------------------------
import requests
def send_to_http(batch_df, batch_id):
    for row in batch_df.toLocalIterator():
        payload = row.asDict()
        requests.post("http://external-system/api/events", json=payload)

(transformed.writeStream
    .foreachBatch(send_to_http)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/chk/http")
    .start()
)

# -------------------------------------
# Await termination
# -------------------------------------
spark.streams.awaitAnyTermination()
