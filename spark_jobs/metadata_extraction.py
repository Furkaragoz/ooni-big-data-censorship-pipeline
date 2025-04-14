import boto3
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# This script extracts metadata from OONI data stored in S3, processes it, and saves the results as CSV files in another S3 bucket.
# It uses the boto3 library to interact with AWS S3 and pandas for data manipulation.
# Ensure you have the required libraries installed:
# pip install boto3 pandas
# Note: This script assumes that you have AWS credentials configured in your environment.


# AWS S3 Client Initialization
s3 = boto3.client("s3")
source_bucket = "ooni-data-eu-fra"
output_bucket = "dehydration"  # Replace with your own destination bucket

# Configuration
test_types = ["whatsapp", "telegram", "facebookmessenger", "webconnectivity", "signal"]
valid_suffix = ".jsonl.gz"
years = [str(y) for y in range(2020, 2026)]  # Years from 2020 to 2025

def generate_date_prefixes(year):
    """Generates daily prefixes in 'raw/YYYYMMDD/' format for a given year."""
    start_date = datetime(int(year), 1, 1)
    end_date = datetime(int(year), 12, 31)
    prefixes = []

    current_date = start_date
    while current_date <= end_date:
        date_prefix = f"raw/{current_date.strftime('%Y%m%d')}/"
        prefixes.append(date_prefix)
        current_date += timedelta(days=1)

    return prefixes

def get_test_files(year, test_name):
    """Lists relevant test files from source S3 bucket."""
    print(f"Searching for {test_name} files in year {year}...")
    files = []
    prefixes = generate_date_prefixes(year)
    paginator = s3.get_paginator("list_objects_v2")

    for prefix in prefixes:
        for page in paginator.paginate(Bucket=source_bucket, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]
                    size = obj["Size"]
                    last_modified = obj["LastModified"].isoformat()

                    if f"/{test_name}/" in key and key.endswith(valid_suffix):
                        files.append([f"s3a://{source_bucket}/{key}", size, last_modified])

    return files

def save_to_s3(year, test_name, files):
    """Converts list of files into CSV and uploads it to the output S3 bucket."""
    if not files:
        print(f"No data found for {test_name} in {year}.")
        return

    df = pd.DataFrame(files, columns=["path", "size", "last_modified"])
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_key = f"{year}/{test_name}/{test_name}_paths.csv"
    s3.put_object(Bucket=output_bucket, Key=s3_key, Body=csv_buffer.getvalue())

    print(f"Saved: s3://{output_bucket}/{s3_key}")

def process_year(year):
    """Processes all test types for a given year sequentially."""
    for test_name in test_types:
        files = get_test_files(year, test_name)
        save_to_s3(year, test_name, files)

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(process_year, year): year for year in years}

        for future in as_completed(futures):
            year = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"Error occurred while processing year {year}: {e}")

    print("All metadata successfully extracted and saved as CSV to S3.")
