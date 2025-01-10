###################

# APPROACH 2:  stream Kafka logs into Google Cloud Storage (GCS) and then load the data into Spark for processing

###################


import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import *
from pyspark.ml.recommendation import ALS
from pyspark.sql.utils import AnalysisException


spark = SparkSession.builder \
    .appName("Course Recommandation") \
    .config("spark.jars", 
            "gcs-connector-hadoop3-latest.jar,spark-cassandra-connector-assembly_2.12-3.0.0-beta.jar") \
    .config("spark.cassandra.output.consistency.level", "ONE") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.gs.auth.service.account.enable", "true") \
    .config("spark.hadoop.fs.gs.project.id", "potent-app-439210-c8") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "potent-app-439210-c8-0e060647490f.json") \
    .getOrCreate()


gcs_path = "gs://interaction_bucket/log_*"

interaction_df = spark.read.json(gcs_path)

#interaction_df.show(truncate=False)

student_logs = interaction_df.groupBy("student_id", "course_code").agg(
    count("interaction_type").alias("interacton_count"),
    max("timestamp").alias("timestamp")
)

# student_logs.show(100, truncate=False)

student_indexer = StringIndexer(inputCol="student_id", outputCol="student_numeric_id")
course_indexer = StringIndexer(inputCol="course_code", outputCol="course_numeric_code")

indexed_data = student_indexer.fit(student_logs).transform(student_logs)
indexed_data = course_indexer.fit(indexed_data).transform(indexed_data)

student_id_mapping = {float(i): label for i, label in enumerate(student_indexer.fit(student_logs).labels)}
course_code_mapping = {float(i): label for i, label in enumerate(course_indexer.fit(student_logs).labels)}

student_id_map_udf = udf(lambda x: student_id_mapping[int(x)], StringType())
course_code_map_udf = udf(lambda x: course_code_mapping[int(x)], StringType())


# indexed_data.show()

als = ALS(
    maxIter=10, regParam=0.01, userCol="student_numeric_id", 
    itemCol="course_numeric_code", ratingCol="interacton_count", 
    coldStartStrategy="drop"
    )

model = als.fit(indexed_data)

user_recs = model.recommendForAllUsers(5)

user_recs = user_recs.withColumn("student_numeric_id", col("student_numeric_id").cast("float"))
# user_recs.show()

user_recs_timestamp = indexed_data.join(
    user_recs,
    indexed_data.student_numeric_id == user_recs.student_numeric_id,
    "left"
) \
.select (indexed_data.student_id.alias("student_id"), "timestamp", "recommendations")

user_recs_timestamp = user_recs_timestamp.groupBy("student_id", "recommendations") \
    .agg(max("timestamp").alias("timestamp"))

#user_recs_timestamp.show()

user_recs_exploaded =  user_recs_timestamp.selectExpr("student_id", "explode(recommendations) as rec", "timestamp")

# user_recs_exploaded.show()

user_recs_final = user_recs_exploaded.select(
    col("student_id"),
    course_code_map_udf(col("rec.course_numeric_code")).alias("course_code"),
    col("timestamp").alias("recommendation_time"),
    col("rec.rating").alias("rating")
)

user_recs_final.show(truncate=False)

try:
    user_recs_final.write\
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "interaction_data") \
        .option("table", "student_recommendations") \
        .mode("append") \
        .save()
except AnalysisException as e:
    print(f"Cassandra write failed: {e}")
