###################

# APPROACH 1:  stream Kafka logs into Google Cloud Storage (GCS) and then load the data into Spark for processing

###################

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime, timedelta
from pyspark.sql.utils import AnalysisException


spark = SparkSession.builder \
    .appName("Evaluate popularity") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"  
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1")  \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.output.consistency.level", "ONE") \
    .getOrCreate()


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "interaction_data") \
    .option("startingOffsets", "latest") \
    .load()


schema = StructType([
    StructField("student_id", StringType(), True),
    StructField("course_code", StringType(), True),
    StructField("interaction_type", StringType(), True),
    StructField("timestamp", TimestampType(), False),
    StructField("duration", IntegerType(), True),
    StructField("location", StructType([
        StructField("country", StringType(), True),
        StructField("city", StringType(), True)
    ]))
])


interaction_df = df.selectExpr("CAST (value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

interaction_df = interaction_df.withColumn("location_country", col("location.country")) \
                               .withColumn("location_city", col("location.city")) \
                               .drop("location")


interaction_df.printSchema()

interaction_df = interaction_df.filter(col("student_id").isNotNull()) \
                                # .withWatermark("timestamp", "1 hour")

checkpoint_interaction= "checkpoint_interaction"

try:
    interaction_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "interaction_data") \
        .option("table", "interaction_logs") \
        .option("checkpointLocation", checkpoint_interaction) \
        .outputMode("append") \
        .start() 
except AnalysisException as e:
    print(f"Cassandra write failed: {e}")


# interaction_df.writeStream\
#     .outputMode("append") \
#     .format("console") \
#     .option("checkpointLocation", checkpoint_interaction) \
#     .start() \
#     .awaitTermination()

filtered_df = interaction_df.filter(
    (col("timestamp") >= current_timestamp() - expr("INTERVAL 1 HOUR"))
)

checkpoint_popularity = "checkpoint_popularity"

def engangement_score(action):
    if action == "course_viewed":
        return 1
    elif action == "course_started":
        return 3
    elif action == "course_completed":
        return 7
    return 0

engagement_udf = udf(engangement_score, IntegerType())
engagement_df = interaction_df.withColumn("engagement_score", engagement_udf("interaction_type")) \
                           .withColumn("timestamp_eng", col("timestamp"))


engagement_df = engagement_df.withColumn("engagement_score", engagement_udf("interaction_type"))

popularity_df = engagement_df \
    .withWatermark("timestamp_eng", "1 hour") \
    .groupBy(
        "course_code",
        window("timestamp_eng", "1 hour") 
    ).agg(
        approx_count_distinct("student_id").alias("active_users"),  
        max("timestamp").alias("timestamp_au"),  
        sum("engagement_score").alias("engagement_score"),  
        max("timestamp_eng").alias("timestamp")  
)

popularity_df = popularity_df.withColumn(
    "total_popularity",
    col("active_users") + col("engagement_score")
).orderBy("total_popularity", ascending=False)

popularity_df = popularity_df.withColumn(
    "active_users", col("active_users").cast("int")
).withColumn(
    "engagement_score", col("engagement_score").cast("int")
).withColumn(
    "total_popularity", col("total_popularity").cast("int")
)

popularity_df = popularity_df.select("course_code", "timestamp", "active_users", "engagement_score", "total_popularity")

try:
    popularity_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "interaction_data") \
        .option("table", "course_popularity") \
        .option("checkpointLocation", checkpoint_popularity) \
        .option("confirm.truncate", "true") \
        .outputMode("complete") \
        .start() \
        .awaitTermination()
except AnalysisException as e:
    print(f"Cassandra write failed: {e}")

# q = popularity_df.writeStream\
#     .outputMode("complete") \
#     .format("console") \
#     .option("checkpointLocation", checkpoint_popularity) \
#     .start() 
        
# q.awaitTermination()


