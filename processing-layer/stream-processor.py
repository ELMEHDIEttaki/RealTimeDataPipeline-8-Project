import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, expr, round, broadcast, udf, concat, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
import uuid
import os
from dotenv import load_dotenv

# -------------------------------------
# Load Environment Variables
# -------------------------------------
load_dotenv()  # load from .env file

# -------------------------------------
# Logging Configuration
# -------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


# -------------------------------------
# Cassandra Writer
# -------------------------------------
class CassandraWriter:
    def __init__(self, keyspace: str, table: str):
        self.keyspace = keyspace
        self.table = table
    
    @staticmethod
    def ensure_uuid(x: str) -> str:
        try:
            return str(uuid.UUID(x))
        except Exception:
            return str(uuid.uuid4())

    def write(self, batch_df: DataFrame, batch_id: int):
        logger.info(f"Writing batch {batch_id} to Cassandra ({self.keyspace}.{self.table})")
        try:
            df_with_uuid = batch_df \
                .withColumn("event_id", udf(CassandraWriter.ensure_uuid, StringType())(col("event_id"))) \
                .withColumn("content_id", udf(CassandraWriter.ensure_uuid, StringType())(col("content_id"))) \
                .withColumn("user_id", udf(CassandraWriter.ensure_uuid, StringType())(col("user_id")))
            
            to_write = df_with_uuid.select(
                "event_id", "content_id", "user_id", "event_time", "duration_ms",
                "content_type", "length_seconds", "engagement_seconds", "engagement_pct"
            )
            
            to_write.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(keyspace=self.keyspace, table=self.table) \
                .save()
                
            logger.info(f"Batch {batch_id} written to Cassandra successfully")
        except Exception as e:
            logger.error(f"Error writing to Cassandra: {str(e)}")
            raise


# -------------------------------------
# Redis Writer Class (Optimized for Low Latency)
# -------------------------------------
class RedisWriter:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.redis_config = {
            "host": os.getenv("REDIS_HOST"),
            "port": int(os.getenv("REDIS_PORT", "6379")),
            "db": os.getenv("REDIS_DB", "0"),
            "timeout": os.getenv("REDIS_TIMEOUT"),  # 1 second timeout for low latency
            "max.pipeline.size": os.getenv("REDIS_PIPELINE_SIZE")  # Optimize pipeline size
        }
    
    def write_to_redis(self, batch_df: DataFrame, batch_id: int):
        """Write to Redis using Spark-Redis connector with optimizations for speed"""
        try:
            start_time = time.time()
            
            # Prepare Redis DataFrame with proper key structure
            redis_df = batch_df.select(
                col("event_id"),
                col("content_id"), 
                col("user_id"),
                col("event_time").cast("string").alias("event_time_str"),
                col("duration_ms").cast("string").alias("duration_ms_str"),
                col("content_type"),
                col("length_seconds").cast("string").alias("length_seconds_str"),
                col("engagement_seconds").cast("string").alias("engagement_seconds_str"),
                col("engagement_pct").cast("string").alias("engagement_pct_str")
            ).withColumn(
                "redis_key", 
                concat(lit("event:"), col("event_id"))
            )
            
            # Method 1: Write as Redis Hash (HSET) - Primary approach
            redis_df.write \
                .format("org.apache.spark.sql.redis") \
                .option("table", "engagement_events") \
                .option("key.column", "redis_key") \
                .option("host", self.redis_config["host"]) \
                .option("port", self.redis_config["port"]) \
                .option("db", self.redis_config["db"]) \
                .option("timeout", self.redis_config["timeout"]) \
                .option("max.pipeline.size", self.redis_config["max.pipeline.size"]) \
                .mode("append") \
                .save()
            
            elapsed = time.time() - start_time
            row_count = batch_df.count()
            
            logger.info(f"Batch {batch_id}: {row_count} events written to Redis in {elapsed:.2f}s")
            
            # Alert if latency exceeds threshold
            if elapsed > 3.0:
                logger.warning(f"Redis write latency {elapsed:.2f}s exceeds 3s threshold for batch {batch_id}")
                
        except Exception as e:
            logger.error(f"Error writing batch {batch_id} to Redis: {str(e)}")
            # Try alternative method for critical data
            self._write_redis_alternative(batch_df, batch_id)
    
    def _write_redis_alternative(self, batch_df: DataFrame, batch_id: int):
        """Alternative Redis write method using key-value pairs"""
        try:
            logger.info(f"Attempting alternative Redis write for batch {batch_id}")
            
            # Create key-value pairs for each event
            kv_df = batch_df.select(
                concat(lit("event:"), col("event_id")).alias("_key"),
                concat(
                    lit('{"event_id":"'), col("event_id"), lit('",'),
                    lit('"content_id":"'), col("content_id"), lit('",'),
                    lit('"user_id":"'), col("user_id"), lit('",'),
                    lit('"event_time":"'), col("event_time").cast("string"), lit('",'),
                    lit('"duration_ms":'), col("duration_ms"), lit(','),
                    lit('"content_type":"'), col("content_type"), lit('",'),
                    lit('"engagement_seconds":'), col("engagement_seconds"), lit(','),
                    lit('"engagement_pct":'), col("engagement_pct"), lit('}')
                ).alias("_value")
            )
            
            kv_df.write \
                .format("org.apache.spark.sql.redis") \
                .option("host", self.redis_config["host"]) \
                .option("port", self.redis_config["port"]) \
                .option("db", self.redis_config["db"]) \
                .mode("append") \
                .save()
                
            logger.info(f"Alternative Redis write successful for batch {batch_id}")
            
        except Exception as e:
            logger.error(f"Alternative Redis write failed for batch {batch_id}: {str(e)}")
            raise


# -------------------------------------
# Engagement Processor (Optimized for Low Latency)
# -------------------------------------
class EngagementProcessor:
    def __init__(self):

        # -- Load Spark Session with necessary packages and configs --
        self.spark_matser = os.getenv("SPARK_MASTER_URL")
        self.kafka_bootstrap = os.getenv("KAFKA_BROKER")
        self.kafka_topic = os.getenv("KAFKA_TOPIC")

        self.pg_url = os.getenv("POSTGRES_URL")
        self.pg_user = os.getenv("POSTGRES_USER")
        self.pg_password = os.getenv("POSTGRES_PASSWORD")
        self.pg_table = os.getenv("POSTGRES_TABLE")

        self.cassandra_host = os.getenv("CASSANDRA_HOST")
        self.cassandra_port = os.getenv("CASSANDRA_PORT")
        self.cassandra_user = os.getenv("CASSANDRA_USER")
        self.cassandra_pass = os.getenv("CASSANDRA_PASSWORD")
        self.cassandra_keyspace = os.getenv("CASSANDRA_KEYSPACE")
        self.cassandra_table = os.getenv("CASSANDRA_TABLE")


        # ----- SparkSession ----
        self.spark = SparkSession.builder \
            .appName(os.getenv("SPARK_APP_NAME")) \
            .master(self.spark_matser) \
            .config("spark.jars.packages",
                    "org.postgresql:postgresql:42.6.0,"
                    "com.redislabs:spark-redis_2.12:3.1.0,"
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1") \
            .config("spark.cassandra.connection.host", self.cassandra_host) \
            .config("spark.cassandra.connection.port", self.cassandra_port) \
            .config("spark.cassandra.auth.username", self.cassandra_user) \
            .config("spark.cassandra.auth.password", self.cassandra_pass) \
            .config("spark.redis.host", os.getenv("REDIS_HOST")) \
            .config("spark.redis.port", os.getenv("REDIS_PORT")) \
            .config("spark.redis.db", "0") \
            .config("spark.redis.timeout", "1000") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.streaming.backpressure.enabled", "true") \
            .config("spark.sql.streaming.metricsEnabled", "true") \
            .getOrCreate()

        # Set log level for better performance
        self.spark.sparkContext.setLogLevel("WARN")

        # Initialize writers
        self.cassandra_writer = CassandraWriter("content_analytics", "engagement_metrics")
        self.redis_writer = RedisWriter(self.spark)

        # Schema for Kafka messages
        self.engagement_schema = StructType([
            StructField("id", LongType()),
            StructField("device", StringType()),
            StructField("user_id", StringType()),
            StructField("event_ts", StringType()),
            StructField("content_id", StringType()),
            StructField("event_type", StringType()),
            StructField("duration_ms", LongType()),
        ])

    def read_kafka_stream(self):
        """Read from Kafka with optimizations for low latency"""
        stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "10000") \
            .option("kafka.session.timeout.ms", "30000") \
            .option("kafka.request.timeout.ms", "40000") \
            .load()

        parsed = (stream
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), self.engagement_schema).alias("event"))
            .select("event.*")
            .withColumn("event_time", col("event_ts").cast(TimestampType()))
            .withColumn("event_id", col("id").cast(StringType()))
            .withColumn("content_id", col("content_id").cast(StringType()))
            .withColumn("user_id", col("user_id").cast(StringType()))
            .withColumn("duration_ms", col("duration_ms").cast("bigint"))
        )
        return parsed

    def enrich_with_postgres(self, parsed_events: DataFrame):
        """PostgreSQL enrichment with caching for performance"""
        content_df = (self.spark.read
            .format("jdbc")
            .option("url", self.pg_url)
            .option("dbtable", self.pg_table)
            .option("user", self.pg_user)
            .option("password", self.pg_password)
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", "10000")  # Optimize fetch size
            .option("numPartitions", "4")   # Parallel reads
            .load()
            .select("id", "content_type", "length_seconds")
            .cache()  # Cache for reuse across micro-batches
        )

        content_broadcast = broadcast(content_df)

        transformed = (parsed_events
            .join(content_broadcast, parsed_events["content_id"] == content_df["id"], "left")
            .withColumn("engagement_seconds", (col("duration_ms") / 1000).cast("double"))
            .withColumn(
                "engagement_pct",
                round(expr(
                    "CASE WHEN length_seconds IS NOT NULL AND duration_ms IS NOT NULL "
                    "THEN (duration_ms/1000.0)/length_seconds ELSE NULL END"
                ), 2)
            )
        )
        return transformed

    def write_batch(self, batch_df: DataFrame, batch_id: int):
        """Optimized batch writer for both sinks"""
        import time
        
        if batch_df.rdd.isEmpty():
            logger.info(f"Batch {batch_id} is empty; skipping.")
            return

        batch_start = time.time()
        
        try:
            # Get batch size for monitoring
            batch_size = batch_df.count()
            logger.info(f"Processing batch {batch_id} with {batch_size} events")
            
            # Write to both sinks in parallel using threads
            from concurrent.futures import ThreadPoolExecutor, as_completed
            
            with ThreadPoolExecutor(max_workers=1) as executor:
                # Submit both writes
                cassandra_future = executor.submit(self.cassandra_writer.write, batch_df, batch_id)
                redis_future = executor.submit(self.redis_writer.write_to_redis, batch_df, batch_id)
                
                # Wait for completion and handle exceptions
                futures = [cassandra_future, redis_future]
                for future in as_completed(futures):
                    try:
                        future.result()  # This will raise any exception that occurred
                    except Exception as e:
                        logger.error(f"Writer failed in batch {batch_id}: {str(e)}")
                        raise
            
            batch_elapsed = time.time() - batch_start
            logger.info(f"Batch {batch_id} completed in {batch_elapsed:.2f}s")
            
            # Alert if total batch processing exceeds threshold
            if batch_elapsed > 4.0:
                logger.warning(f"Batch {batch_id} processing time {batch_elapsed:.2f}s exceeds 4s threshold")
                
        except Exception as e:
            logger.error(f"Error processing batch {batch_id}: {str(e)}")
            raise

    def start_streaming(self):
        """Start streaming with optimized trigger settings"""
        parsed = self.read_kafka_stream()
        print("PARSED SCHEMA:")
        parsed.printSchema()
        
        transformed = self.enrich_with_postgres(parsed)
        print("TRANSFORMED SCHEMA:")
        transformed.printSchema()

        # Optimized streaming query with small trigger interval
        query = (transformed.writeStream
            .foreachBatch(self.write_batch)
            .outputMode("append")
            .trigger(processingTime='2 seconds')  # Process every 2 seconds for low latency
            .option("checkpointLocation", "/tmp/chk/low-latency-streaming")
            .start())

        logger.info("Low-latency streaming job started: Target <5s end-to-end latency")
        self.spark.streams.awaitAnyTermination()


# -------------------------------------
# Main Entry Point
# -------------------------------------
if __name__ == "__main__":
    import time
    processor = EngagementProcessor()
    processor.start_streaming()