# TELEGRAM

from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, col
from pyspark.sql.types import StructType, StructField, StringType

def main():

    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    input_path = f"s3a://ooni-data-eu-fra/raw/{yesterday}/*/*/telegram/*.jsonl.gz"

    spark = SparkSession.builder \
        .appName("SendToELK") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", "ACCESSKEY") \
        .config("spark.hadoop.fs.s3a.secret.key", "SECRETKEY") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    schema_telegram = StructType([
        StructField("annotations", StructType([
            StructField("platform", StringType(), True),
        ]), True),
        StructField("measurement_start_time", StringType(), True),
        StructField("probe_asn", StringType(), True),
        StructField("probe_cc", StringType(), True),
        StructField("probe_network_name", StringType(), True),
        StructField("resolver_network_name", StringType(), True),
        StructField("test_name", StringType(), True),
        StructField("test_keys", StructType([
            StructField("telegram_http_blocking", StringType(), True),
            StructField("telegram_tcp_blocking", StringType(), True),
            StructField("telegram_web_failure", StringType(), True),
            StructField("telegram_web_status", StringType(), True)
        ]), True),
    ])

    df_telegram = spark.read.schema(schema_telegram).json(input_path)

    df_telegram = df_telegram.withColumn(
        "measurement_start_time", to_date(to_timestamp(df_telegram["measurement_start_time"], "yyyy-MM-dd HH:mm:ss"))
    )

    df_selected_telegram = df_telegram.select(
        col("annotations.platform").alias("platform"),
        col("measurement_start_time").alias("date"),
        col("probe_asn").alias("autonomous_system_number"),
        col("probe_cc").alias("country"),
        col("probe_network_name").alias("telecommunication_company"),
        col("resolver_network_name").alias("resolver_network"),
        col("test_name").alias("application"),
        col("test_keys.telegram_http_blocking").alias("dns_blocking"),
        col("test_keys.telegram_tcp_blocking").alias("tcp_blocking"),
        col("test_keys.telegram_web_failure").alias("failure"),
        col("test_keys.telegram_web_status").alias("status")
    )

    df_selected_telegram.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "18.192.5.212") \
        .option("es.port", "9200") \
        .option("es.resource", "oonidata-2025") \
        .option("es.nodes.wan.only", "true") \
        .mode("append") \
        .save()

    df_selected_telegram = df_selected_telegram.repartition(2, "date", "country", "application")

    output_path = "s3a://dehydration/ooni_parquet_data/"
    df_selected_telegram.write \
        .partitionBy("date", "country", "application") \
        .option("compression", "snappy") \
        .mode("overwrite") \
        .parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    main()
