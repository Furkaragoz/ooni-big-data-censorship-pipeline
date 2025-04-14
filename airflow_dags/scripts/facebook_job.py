# FACEBOOK

from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, col
from pyspark.sql.types import StructType, StructField, StringType

def main():

    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    input_path = f"s3a://ooni-data-eu-fra/raw/{yesterday}/*/*/facebookmessenger/*.jsonl.gz"

    spark = SparkSession.builder \
        .appName("SendToELK") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", "ACCESSKEY") \
        .config("spark.hadoop.fs.s3a.secret.key", "SECRETKEY") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.jars.packages", "/opt/spark/jars/elasticsearch-spark-30_2.12-8.12.0.jar") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    schema_facebook = StructType([
        StructField("annotations", StructType([
            StructField("platform", StringType(), True),
        ]), True),
        StructField("measurement_start_time", StringType(), True),
        StructField("probe_asn", StringType(), True),
        StructField("probe_cc", StringType(), True),
        StructField("probe_network_name", StringType(), True),
        StructField("resolver_network_name", StringType(), True),
        StructField("test_name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("failure", StringType(), True),
        StructField("test_keys", StructType([
            StructField("facebook_dns_blocking", StringType(), True),
            StructField("facebook_tcp_blocking", StringType(), True)
        ]), True),
    ])

    df_facebook = spark.read.schema(schema_facebook).json(input_path)

    df_facebook = df_facebook.withColumn(
        "measurement_start_time", to_date(to_timestamp(df_facebook["measurement_start_time"], "yyyy-MM-dd HH:mm:ss"))
    )

    df_selected_facebook = df_facebook.select(
        col("annotations.platform").alias("platform"),
        col("measurement_start_time").alias("date"),
        col("probe_asn").alias("autonomous_system_number"),
        col("probe_cc").alias("country"),
        col("probe_network_name").alias("telecommunication_company"),
        col("resolver_network_name").alias("resolver_network"),
        col("test_name").alias("application"),
        col("test_keys.facebook_dns_blocking").alias("dns_blocking"),
        col("test_keys.facebook_tcp_blocking").alias("tcp_blocking"),
        col("status"),
        col("failure")
    )

    df_selected_facebook.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "3.125.157.140") \
        .option("es.port", "9200") \
        .option("es.resource", "oonidata-2025") \
        .option("es.nodes.wan.only", "true") \
        .mode("append").save()

    df_selected_facebook = df_selected_facebook.repartition(2, "date", "country", "application")

    output_path = "s3a://dehydration/ooni_parquet_data/"
    df_selected_facebook.write \
        .partitionBy("date", "country", "application") \
        .option("compression", "snappy") \
        .mode("overwrite") \
        .parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    main()
