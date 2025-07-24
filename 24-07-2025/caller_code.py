from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, current_timestamp
from pyspark.sql.types import IntegerType
from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import datetime
import time
import requests
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Spark session
spark = SparkSession.builder.appName("SFTPBlobRetryMerge").getOrCreate()

# Constants
AZURE_FUNCTION_URL = "<your_api_url>"
MAX_WORKERS = 10
INPUT_PATH = "Tables/dbo/document"
SUCCESS_PATH = "Tables/dbo/document_success"
FAILURE_PATH = "Tables/dbo/document_failure"
FAILURE_REASON_PATH = "Tables/dbo/document_failure_details"

# Load tables
delta_table = DeltaTable.forPath(spark, INPUT_PATH)
input_df = delta_table.toDF().filter(col("carepro_RootFolderPath").isNotNull())

fail_df = spark.read.format("delta").load(FAILURE_PATH)
success_df = spark.read.format("delta").load(SUCCESS_PATH)

# Prepare full file path
input_df = input_df.withColumn(
    "full_path",
    concat(
        col("carepro_RootFolderPath"),
        lit("\\"),
        col("carepro_ContainerFolders"),
        lit("\\"),
        col("carepro_DocumentBase_Annotation_carepro_FileName")
    )
).withColumnRenamed("carepro_DocumentBase_Annotation_Id", "carepro_DocumentId"
).select("carepro_AuthrequestId", "carepro_DocumentId", "full_path")

# New files not already processed
new_files_df = input_df.join(
    success_df.select("carepro_AuthrequestId", "carepro_DocumentId").distinct(),
    on=["carepro_AuthrequestId", "carepro_DocumentId"], how="left_anti"
)

# Retryable failed files
retryable_df = fail_df.filter(
    (col("retry_count") < 5) & (col("reprocess_success") == 0)
).select(
    "carepro_AuthrequestId", "carepro_DocumentId", "full_path", "retry_count", "reprocess_success"
)

# Final batch
new_files_df = new_files_df.withColumn("retry_count", lit(0)).withColumn("reprocess_success", lit(0))
final_df = new_files_df.unionByName(retryable_df)
records = final_df.toPandas().to_dict(orient="records")

# Shared collections
success_rows = []
fail_for_merge = []
fail_reason_logs = []
results_lock = threading.Lock()

def map_datetime_str(dt):
    return dt.isoformat() + ("Z" if '+' not in dt.isoformat() else "")

datetime_now = map_datetime_str(datetime.datetime.now(datetime.timezone.utc))

def call_http_api(row):
    try:
        res = requests.post(AZURE_FUNCTION_URL, json={"file_path": row["full_path"]})
        elapsed = int((time.time() - start) * 1000)
        res_json = res.json()
        if res.status_code == 200 and res_json.get("status") == "Success":
            return "success", {
                **row,
                "azure_blob_path": res_json["azure_blob_path"],
                "execution_time_ms": elapsed
            }
        else:
            return "failure", row, {
                **row,
                "message": res_json.get("message", ""),
                "status_code": res.status_code
            }
    except Exception as e:
        return "failure", row, {
            **row,
            "message": str(e),
            "status_code": 500
        }

def process_single_record(row):
    result = call_http_api(row)
    if result[0] == "success":
        record = {
            **result[1],
            "Date_Created": datetime_now,
            "Date_updated": datetime_now
        }
        with results_lock:
            success_rows.append(record)
        return "success", record

    fail_data, reason_data = result[1], result[2]
    fail_record = {
        **fail_data,
        "retry_count": fail_data["retry_count"] + 1,
        "reprocess_success": 0,
        "Date_Created": datetime_now,
        "Date_updated": datetime_now,
        "error_message": reason_data["message"],
        "error_code": reason_data["status_code"]
    }

    reason_record = {
        "carepro_AuthrequestId": reason_data["carepro_AuthrequestId"],
        "carepro_DocumentId": reason_data["carepro_DocumentId"],
        "error_code": reason_data["status_code"],
        "error_message": reason_data["message"],
        "Date_Created": datetime_now,
        "Date_updated": datetime_now
    }

    with results_lock:
        fail_for_merge.append(fail_record)
        fail_reason_logs.append(reason_record)
    return "failure", fail_record

# Execute parallel processing
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

# Save successes
if success_rows:
    spark.createDataFrame(success_rows) \
        .withColumn("Date_Created", current_timestamp()) \
        .withColumn("Date_Updated", current_timestamp()) \
        .write.format("delta").mode("append").save(SUCCESS_PATH)

# Save failure reasons
if fail_reason_logs:
    spark.createDataFrame(fail_reason_logs) \
        .withColumn("Date_Created", current_timestamp()) \
        .withColumn("Date_Updated", current_timestamp()) \
        .withColumn("error_code", col("error_code").cast(IntegerType())) \
        .write.format("delta").mode("append").save(FAILURE_REASON_PATH)

# Merge failures into failure table
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
