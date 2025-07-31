import os
import time
import json
import logging
import threading
from threading import local
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp
from pyspark.sql.types import IntegerType

# Configuration
class Config:
    AZURE_FUNCTION_URL = "https://dv-func-sftpfilecp-01.azurewebsites.net/api/sftp-to-blob-copy-file"
    FUNCTION_KEY = os.getenv("AZURE_FUNCTION_KEY", "")
    MAX_WORKERS = 30
    BATCH_SIZE = 5000
    TIMEOUT_SECONDS = 300
    MAX_RETRIES = 3
    CONNECTION_POOL_SIZE = 50
    CONNECTION_POOL_MAXSIZE = 50

PATHS = {
    "INPUT": "Tables/dbo/document",
    "SUCCESS": "Tables/dbo/document_success",
    "FAILURE": "Tables/dbo/document_failure",
    "FAILURE_REASON": "Tables/dbo/document_failure_details"
}

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("caller")

# Thread-local session reuse
thread_local_data = local()

def create_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=Config.MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"],
        respect_retry_after_header=True
    )
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=Config.CONNECTION_POOL_SIZE,
        pool_maxsize=Config.CONNECTION_POOL_MAXSIZE
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "Azure-Functions-Client/1.0",
        "Accept": "application/json",
        "Connection": "keep-alive"
    })
    return session

def get_session():
    if not hasattr(thread_local_data, "session"):
        thread_local_data.session = create_session()
    return thread_local_data.session

@retry(
    retry=retry_if_exception_type((requests.RequestException, ValueError)),
    stop=stop_after_attempt(Config.MAX_RETRIES),
    wait=wait_random_exponential(multiplier=1, max=10)
)
def call_azure_function(record: dict):
    session = get_session()
    headers = {"x-functions-key": Config.FUNCTION_KEY}
    response = session.post(Config.AZURE_FUNCTION_URL, headers=headers, json=record, timeout=Config.TIMEOUT_SECONDS)
    if not response.ok:
        raise ValueError(f"HTTP {response.status_code}: {response.text}")
    return response.json()

def process_single_record(row):
    payload = row.asDict()
    try:
        call_azure_function(payload)
        return ("success", payload)
    except Exception as e:
        return ("fail", payload, str(e))

def process_batch(records):
    success_list, failed_list, failure_reason_list = [], [], []

    logger.info(f"Processing {len(records)} records using {Config.MAX_WORKERS} threads")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_single_record, row) for row in records]
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            if result[0] == "success":
                success_list.append(result[1])
            else:
                failed_list.append(result[1])
                failure_reason_list.append({"record": result[1], "reason": result[2]})

            if i % 10 == 0 or i == len(records):
                logger.info(f"Progress: {i}/{len(records)} - Success: {len(success_list)} - Failed: {len(failed_list)}")

    logger.info(f"Batch completed in {time.time() - start_time:.2f} sec")
    return success_list, failed_list, failure_reason_list

def save_results(spark, success_rows, fail_rows, failure_reasons, paths):
    try:
        if success_rows:
            logger.info(f"Saving {len(success_rows)} successful records...")
            df_success = spark.createDataFrame(success_rows)
            df_success = df_success.withColumn("Date_Created", current_timestamp()) \
                                   .withColumn("Date_Updated", current_timestamp())
            df_success.write.format("delta").mode("append").save(paths["SUCCESS"])

        if fail_rows:
            logger.info(f"Saving {len(fail_rows)} failed records...")
            df_fail = spark.createDataFrame(fail_rows)
            df_fail = df_fail.withColumn("Date_Created", current_timestamp()) \
                             .withColumn("Date_Updated", current_timestamp())
            df_fail.write.format("delta").mode("append").save(paths["FAILURE"])

        if failure_reasons:
            logger.info(f"Saving {len(failure_reasons)} failure reasons...")
            df_reasons = spark.createDataFrame(failure_reasons)
            df_reasons = df_reasons.withColumn("Date_Created", current_timestamp()) \
                                   .withColumn("Date_Updated", current_timestamp())
            df_reasons.write.format("delta").mode("append").save(paths["FAILURE_REASON"])

        logger.info("All results saved.")
    except Exception as e:
        logger.error(f"Error while saving results: {str(e)}")

def main():
    spark = SparkSession.builder.appName("SFTPBlobRetryMerge").getOrCreate()

    input_df = spark.read.format("delta").load(PATHS["INPUT"])
    input_df = input_df.withColumn("full_path", concat_ws("\\\\",
        col("carepro_RootFolderPath"),
        col("carepro_ContainerFolders"),
        col("carepro_DocumentBase_Annotation_carepro_FileName"))
    ).withColumn("error_code", col("error_code").cast(IntegerType()))

    records = input_df.collect()
    total_records = len(records)
    logger.info(f"Total records to process: {total_records}")

    for start in range(0, total_records, Config.BATCH_SIZE):
        end = min(start + Config.BATCH_SIZE, total_records)
        batch = records[start:end]
        logger.info(f"Processing batch {start} to {end}")

        success_rows, fail_rows, fail_reasons = process_batch(batch)
        save_results(spark, success_rows, fail_rows, fail_reasons, PATHS)

    logger.info("All batches processed.")

if __name__ == "__main__":
    main()
