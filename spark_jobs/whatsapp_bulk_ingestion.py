# -*- coding: utf-8 -*-
# Spark script for bulk ingestion of Whatsapp data


# This script reads JSON files from S3, processes them using PySpark, and writes the output to S3 in Parquet format.


from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, col
from pyspark.sql.types import StructType, StructField, StringType

def main():
    spark = SparkSession.builder \
        .appName("S3Test_whatsapp") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
        .getOrCreate()

    schema_wp = StructType([
    StructField("annotations", StructType([
        StructField("platform", StringType(), True),
    ]), True),
    
    StructField("measurement_start_time", StringType(), True),
    StructField("probe_asn", StringType(), True),
    StructField("probe_cc", StringType(), True),
    StructField("probe_network_name", StringType(), True),
    StructField("resolver_network_name", StringType(), True),
    StructField("test_name", StringType(), True),
    StructField("dns_blocking", StringType(), True),
    StructField("tcp_blocking", StringType(), True),
    StructField("test_keys", StructType([
        StructField("whatsapp_web_failure", StringType(), True),
        StructField("whatsapp_web_status", StringType(), True),
    ]), True),
])

    metadata_schema = StructType([
        StructField("path", StringType(), True)
    ])

    metadata_path = "s3a://dehydration/metadata/2024/whatsapp/"
    df_metadata = spark.read.option("header", "true").schema(metadata_schema).csv(metadata_path)
    paths = [row['path'] for row in df_metadata.collect()]

    df_whatsapp = spark.read.schema(schema_wp).json(paths)

    df_whatsapp = df_whatsapp.withColumn(
        "measurement_start_time", to_date(to_timestamp(df_whatsapp["measurement_start_time"], "yyyy-MM-dd HH:mm:ss"))
    )

    df_selected_whatsapp = df_whatsapp.select(
        col("annotations.platform").alias("platform"),
        col("measurement_start_time").alias("date"),
        col("probe_asn").alias("autonomous_system_number"),
        col("probe_cc").alias("country"),
        col("probe_network_name").alias("telecommunication_company"),
        col("resolver_network_name").alias("resolver_network"),
        col("test_name").alias("application"),
        col("test_keys.whatsapp_web_failure").alias("failure"),
        col("test_keys.whatsapp_web_status").alias("status"),
        col("dns_blocking"),
        col("tcp_blocking")
    )

    df_selected_whatsapp = df_selected_whatsapp.repartition(30, "date", "country", "application")

    output_path = "s3a://dehydration/ooni_parquet_data/"
    df_selected_whatsapp.write \
        .partitionBy("date", "country", "application") \
        .option("compression", "snappy") \
        .mode("overwrite") \
        .parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    main()

