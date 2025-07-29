from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, current_timestamp, concat
from pyspark.sql.types import IntegerType
from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type
import time
import requests
import logging
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AZURE_FUNCTION_URL = "https://dv-func-sftpfilecp-01.azurewebsites.net/api/sftp-to-blob-copy-file?code=RONjnd2qM0pCAFg63UFh-k-92lct6EQ7B7PU0FqUggFfAzFuEZL7LQ=="
MAX_WORKERS = 30
BATCH_SIZE = 5000

INPUT_PATH = "Tables/dbo/document"
SUCCESS_PATH = "Tables/dbo/document_success"
FAILURE_PATH = "Tables/dbo/document_failure"
FAILURE_REASON_PATH = "Tables/dbo/document_failure_details"

spark = SparkSession.builder.appName("SFTPBlobRetryMerge").getOrCreate()
results_lock = threading.Lock()
success_rows = []
fail_for_merge = []
fail_reason_logs = []

fail_df = spark.read.format("delta").load(FAILURE_PATH)
success_df = spark.read.format("delta").load(SUCCESS_PATH)
input_df = DeltaTable.forPath(spark, INPUT_PATH).toDF().filter(col("carepro_RootFolderPath").isNotNull())
input_df = input_df.withColumn("full_path", concat("carepro_RootFolderPath", lit("\\"), col("carepro_ContainerFolders"), lit("\\"), col("carepro_DocumentBase_Annotation_carepro_FileName")))
input_df = input_df.dropDuplicates(["carepro_AuthrequestId", "carepro_DocumentId"])

input_df = input_df.join(
    fail_df.select("carepro_AuthrequestId", "carepro_DocumentId", "retry_count"),
    ["carepro_AuthrequestId", "carepro_DocumentId"],
    how="left"
).fillna({"retry_count": 0})

records = input_df.collect()

@retry(
    stop=stop_after_attempt(3),
    wait=wait_random_exponential(min=2, max=30),
    retry=retry_if_exception_type(Exception)
)
def process_single_record(record):
    record_dict = record.asDict()
    full_path = record_dict["full_path"]
    payload = {
        "path": full_path,
        "auth_id": record_dict["carepro_AuthrequestId"],
        "doc_id": record_dict["carepro_DocumentId"],
        "retry_count": int(record_dict.get("retry_count", 0))
    }

    response = requests.post(AZURE_FUNCTION_URL, json=payload)
    status_code = response.status_code

    with results_lock:
        if status_code == 200:
            success_rows.append(payload)
        else:
            fail_for_merge.append({
                "carepro_AuthrequestId": payload["auth_id"],
                "carepro_DocumentId": payload["doc_id"],
                "full_path": payload["path"],
                "retry_count": payload["retry_count"] + 1,
                "reprocess_success": 0,
                "error_message": response.text,
                "error_code": status_code,
                "Date_updated": datetime.datetime.now()
            })
            fail_reason_logs.append({
                "carepro_AuthrequestId": payload["auth_id"],
                "carepro_DocumentId": payload["doc_id"],
                "full_path": payload["path"],
                "error_message": response.text,
                "error_code": status_code
            })

print(f"Processing {len(records)} records using {MAX_WORKERS} threads...")
start = time.time()

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(process_single_record, row) for row in records]
    for i, future in enumerate(as_completed(futures), start=1):
        if i % 10 == 0 or i == len(records):
            with results_lock:
                print(f"Progress: {i}/{len(records)} - Success: {len(success_rows)}, Failed: {len(fail_for_merge)}")

duration = time.time() - start
print(f"\nProcessing completed in {duration:.2f} seconds")
print(f"Total: {len(records)}, Success: {len(success_rows)}, Failed: {len(fail_for_merge)}")

if records:
    print(f"Success rate: {(len(success_rows) / len(records)) * 100:.2f}%")
    print(f"Throughput: {len(records) / duration:.2f} records/sec")

if success_rows:
    spark.createDataFrame(success_rows) \
        .withColumn("Date_Created", current_timestamp()) \
        .withColumn("Date_Updated", current_timestamp()) \
        .write.format("delta").mode("append").save(SUCCESS_PATH)

if fail_reason_logs:
    spark.createDataFrame(fail_reason_logs) \
        .withColumn("Date_Created", current_timestamp()) \
        .withColumn("Date_Updated", current_timestamp()) \
        .withColumn("error_code", col("error_code").cast(IntegerType())) \
        .write.format("delta").mode("append").save(FAILURE_REASON_PATH)

if fail_for_merge:
    df_fail_merge = spark.createDataFrame(fail_for_merge)
    df_fail_merge.createOrReplaceTempView("incoming_failures")

    spark.sql(f"""
        MERGE INTO delta.`{FAILURE_PATH}` AS target
        USING incoming_failures AS source
        ON target.carepro_AuthrequestId = source.carepro_AuthrequestId
           AND target.carepro_DocumentId = source.carepro_DocumentId
        WHEN MATCHED THEN UPDATE SET
            target.retry_count = source.retry_count,
            target.full_path = source.full_path,
            target.reprocess_success = source.reprocess_success,
            target.error_message = source.error_message,
            target.error_code = source.error_code,
            target.Date_updated = source.Date_updated
        WHEN NOT MATCHED THEN INSERT *
    """)
