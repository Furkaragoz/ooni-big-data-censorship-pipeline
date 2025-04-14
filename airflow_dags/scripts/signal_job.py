from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, col
from pyspark.sql.types import StructType, StructField, StringType

def main():

    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    input_path = f"s3a://ooni-data-eu-fra/raw/{yesterday}/*/*/signal/*.jsonl.gz"

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

    schema_signal = StructType([
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
            StructField("signal_backend_status", StringType(), True),
            StructField("signal_backend_failure", StringType(), True)
        ]), True),
    ])

    df_signal = spark.read.schema(schema_signal).json(input_path)

    df_signal = df_signal.withColumn(
        "measurement_start_time", to_date(to_timestamp(df_signal["measurement_start_time"], "yyyy-MM-dd HH:mm:ss"))
    )

    df_selected_signal = df_signal.select(
        col("annotations.platform").alias("platform"),
        col("measurement_start_time").alias("date"),
        col("probe_asn").alias("autonomous_system_number"),
        col("probe_cc").alias("country"),
        col("probe_network_name").alias("telecommunication_company"),
        col("resolver_network_name").alias("resolver_network"),
        col("test_name").alias("application"),
        col("test_keys.signal_backend_status").alias("status"),
        col("test_keys.signal_backend_failure").alias("failure"),
        col("dns_blocking"),
        col("tcp_blocking")
    )

    df_selected_signal.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "18.192.5.212") \
        .option("es.port", "9200") \
        .option("es.resource", "oonidata-2025") \
        .option("es.nodes.wan.only", "true") \
        .mode("append").save()

    df_selected_signal = df_selected_signal.repartition(2, "date", "country", "application")

    output_path = "s3a://dehydration/ooni_parquet_data/"
    df_selected_signal.write \
        .partitionBy("date", "country", "application") \
        .option("compression", "snappy") \
        .mode("overwrite") \
        .parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    main()
