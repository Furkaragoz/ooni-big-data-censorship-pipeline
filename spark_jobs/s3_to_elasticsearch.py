# -*- coding: utf-8 -*-

# Spark to Elasticsearch Ingestion Script



# 1. Spark Session Initialization


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SendToELK") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "ACCESSKEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "SECRETKEY") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0")\
    .getOrCreate()




# 2. Read Processed Parquet from S3


df = spark.read.schema(schema).option("ignoreMissingFiles", "true") \
    .option("basePath", "s3a://dehydration/ooni_parquet_data/") \
    .parquet("s3a://dehydration/ooni_parquet_data/date=2025-*/country=*/application=whatsapp/") # arrange the path according to your needs 




# 3. Elasticsearch Configuration


df.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "3.125.157.140") \
    .option("es.port", "9200") \
    .option("es.resource", "oonidata-2025") \
    .option("es.nodes.wan.only", "true")\
    .mode("append") \
    .save()


# 4. Stop Spark Session
spark.stop()