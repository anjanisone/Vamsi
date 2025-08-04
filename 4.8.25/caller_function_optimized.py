import sys
import datetime
import requests
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, concat
from pyspark.sql.types import IntegerType
from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type,
)
import time
import logging
import threading
from threading import local

spark_session = SparkSession.builder.appName("SFTPBlobRetryMerge").getOrCreate()
correlation_id: str = str(uuid4())

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logger.info(correlation_id)

# Configuration
class Config:
    AZURE_FUNCTION_URL = "https://dv-func-sftpfilecp-01.azurewebsites.net/api/sftp-to-blob-copy-file?code=RONjnd2qM0pCAFg63UFh-k-92lct6EQ7B7PU0FqUggFfAzFuEZL7LQ=="
    MAX_WORKERS = 30
    BATCH_SIZE = 5000
    MAX_RETRIES = 3

PATHS = {
    "INPUT_PATH": "Tables/dbo/document",
    "SUCCESS_PATH": "Tables/dbo/document_success",
    "FAILURE_PATH": "Tables/dbo/document_failure",
    "FAILURE_REASON_PATH": "Tables/dbo/document_failure_details",
}

results_lock = threading.Lock()
thread_local_data = local()

def map_datetime_str(dt: datetime) -> str:
    date_str = dt.isoformat()
    return date_str if "+" in date_str else date_str + "Z"

def create_session():
    return requests.Session()

def get_thread_session():
    if not hasattr(thread_local_data, "session"):
        thread_local_data.session = create_session()
        logger.info(f"Initialized session for thread {threading.current_thread().name}")
    return thread_local_data.session

def cleanup_thread_session():
    if hasattr(thread_local_data, "session"):
        thread_local_data.session.close()
        delattr(thread_local_data, "session")
        logger.info(f"Cleaned up session for thread {threading.current_thread().name}")

@retry(
    stop=stop_after_attempt(Config.MAX_RETRIES),
    wait=wait_random_exponential(min=2, max=30),
    retry=retry_if_exception_type(
        (
            requests.RequestException,
            requests.Timeout,
            requests.ConnectionError,
            requests.HTTPError,
        )
    ),
    before_sleep=lambda retry_state: logger.warning(
        f"Retrying Azure Function call (attempt {retry_state.attempt_number}) "
        f"after {retry_state.next_action.sleep} seconds"
    ),
)
def call_azure_function(session, payload, headers):
    try:
        response = session.post(Config.AZURE_FUNCTION_URL, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Azure Function call failed: {str(e)}")
        sys.exit(0)

def process_partition(partition_df):
    session = get_thread_session()
    success_rows = []
    fail_rows = []

    for row in partition_df.toLocalIterator():
        headers = {
            "Content-Type": "application/json",
            "X-Correlation-ID": correlation_id,
        }

        payload = {
            "source_path": row["full_path"],
            "target_dir": "target-folder",
            "request_id": correlation_id,
            "auth_id": row["carepro_AuthrequestId"],
            "document_id": row["carepro_DocumentBase_Annotation_Id"],
        }

        try:
            response = call_azure_function(session, payload, headers)
            if response.get("status") == "success":
                success_rows.append(row)
            else:
                fail_rows.append(row)
        except Exception as e:
            logger.error(f"Error during partition processing: {str(e)}")
            sys.exit(0)

    cleanup_thread_session()
    return success_rows, fail_rows

def main():
    try:
        input_df = spark_session.read.format("delta").load(PATHS["INPUT_PATH"])
        fail_df = spark_session.read.format("delta").load(PATHS["FAILURE_PATH"])
        success_df = spark_session.read.format("delta").load(PATHS["SUCCESS_PATH"])

        total_df = input_df.subtract(success_df).subtract(fail_df)
        total_df = total_df.withColumn("full_path", concat(
            lit("\\\\"),
            col("carepro_RootFolderPath"),
            lit("\\"),
            col("carepro_ContainerFolders"),
            lit("\\"),
            col("carepro_DocumentBase_Annotation_carepro_FileName")
        ))

        partitions = total_df.randomSplit([1.0 / Config.MAX_WORKERS] * Config.MAX_WORKERS)

        all_success_rows = []
        all_fail_rows = []

        start_time = time.time()
        logger.info(f"Processing {total_df.count()} records using {Config.MAX_WORKERS} threads...")

        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            futures = [executor.submit(process_partition, p) for p in partitions]
            for i, future in enumerate(as_completed(futures), start=1):
                try:
                    success_batch, fail_batch = future.result()
                    all_success_rows.extend(success_batch)
                    all_fail_rows.extend(fail_batch)
                    logger.info(f"Completed {i}/{Config.MAX_WORKERS} partitions.")
                except Exception as e:
                    logger.error(f"Thread execution failed: {str(e)}")
                    sys.exit(0)

        duration = time.time() - start_time
        logger.info(f"Completed in {duration:.2f} seconds")
        logger.info(f"Total Success: {len(all_success_rows)} | Failed: {len(all_fail_rows)}")

    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
        sys.exit(0)

if __name__ == "__main__":
    main()
