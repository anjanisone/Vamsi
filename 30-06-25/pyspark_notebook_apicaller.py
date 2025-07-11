from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit
import time
import datetime
import requests

spark = SparkSession.builder.appName("SFTPBlobRetryMerge").getOrCreate()

AZURE_FUNCTION_URL = ""

# Delta Table Paths
INPUT_PATH = "Tables/dbo/document"
SUCCESS_PATH = "Tables/dbo/document_success"
FAILURE_PATH = "Tables/dbo/document_failure"
FAILURE_REASON_PATH = "Tables/dbo/document_failure_details"

# Load tables
input_df = spark.read.format("delta").load(INPUT_PATH)
fail_df = spark.read.format("delta").load(FAILURE_PATH)
success_df = spark.read.format("delta").load(SUCCESS_PATH)

# Preprocess input
input_df = input_df.withColumn("full_path", concat_ws("\\\\",
    col("carepro_RootFolderPath"),
    col("carepro_ContainerFolders"),
    col("carepro_DocumentBase_Annotation_carepro_FileName"))
).withColumnRenamed("carepro_AuthrequestId", "carepro_AuthrequestId"
).withColumnRenamed("carepro_DocumentBase_Annotation_Id", "carepro_DocumentId"
).select("carepro_AuthrequestId", "carepro_DocumentId", "full_path")

# Filter new files (not in success)
new_files_df = input_df.join(
    success_df.select("carepro_AuthrequestId", "carepro_DocumentId").distinct(),
    on=["carepro_AuthrequestId", "carepro_DocumentId"], how="left_anti"
)

# Filter retryable failed files (retry < 5 and not reprocessed)
retryable_df = fail_df.filter(
    (col("retry_count") < 5) & (col("reprocess_success") == 0)
).select("carepro_AuthrequestId", "carepro_DocumentId", "full_path", "retry_count", "reprocess_success")

# Final candidates = new + retry
new_files_df = new_files_df.withColumn("retry_count", lit(0)).withColumn("reprocess_success", lit(0))
final_df = new_files_df.unionByName(retryable_df)

records = final_df.toPandas().to_dict(orient="records")

def map_datetime_str(dt:datetime) -> str:
    date_str = dt.isoformat()
    if '+' in date_str:
        return date_str
    else:
        return date_str +"Z"

# Call HTTP API
def call_http_api(row):
    payload = {
        "file_path": row["full_path"],
        "carepro_AuthrequestId": row["carepro_AuthrequestId"],
        "carepro_DocumentId": row["carepro_DocumentId"]
    }
    start = time.time()
    try:
        res = requests.post(AZURE_FUNCTION_URL, json=payload)
        elapsed = int((time.time() - start) * 1000)
        res_json = res.json()
        if res.status_code == 200 and res_json.get("status") == "Success":
            return ("success", {
                "carepro_AuthrequestId": row["carepro_AuthrequestId"],
                "carepro_DocumentId": row["carepro_DocumentId"],
                "full_path": row["full_path"],
                "azure_blob_path": res_json["azure_blob_path"],
                "execution_time_ms": elapsed
            })
        else:
            return ("failure", {
                "carepro_AuthrequestId": row["carepro_AuthrequestId"],
                "carepro_DocumentId": row["carepro_DocumentId"],
                "full_path": row["full_path"],
                "retry_count": row["retry_count"] + 1,
                "reprocess_success": 0
            }, {
                "carepro_AuthrequestId": row["carepro_AuthrequestId"],
                "carepro_DocumentId": row["carepro_DocumentId"],
                "full_path": row["full_path"],
                "message": res_json.get("message", ""),
                "status_code": res.status_code
            })
    except Exception as e:
        return ("failure", {
            "carepro_AuthrequestId": row["carepro_AuthrequestId"],
            "carepro_DocumentId": row["carepro_DocumentId"],
            "full_path": row["full_path"],
            "retry_count": row["retry_count"] + 1,
            "reprocess_success": 0
        }, {
            "carepro_AuthrequestId": row["carepro_AuthrequestId"],
            "carepro_DocumentId": row["carepro_DocumentId"],
            "full_path": row["full_path"],
            "message": str(e),
            "status_code": 500
        })

success_rows, fail_for_merge, fail_reason_logs = [], [], []

datetime_now = map_datetime_str(datetime.datetime.now(datetime.timezone.utc))

for row in records:
    result = call_http_api(row)
    if result[0] == "success":
        success_rows.append({
            "carepro_AuthrequestId": row["carepro_AuthrequestId"],
            "carepro_DocumentId": row["carepro_DocumentId"],
            "full_path": row["full_path"],
            "azure_blob_path": result["azure_blob_path"],
            "execution_time_ms": result["execution_time_ms"],
            "Date_Created": datetime_now,
            "Date_updated": datetime_now
        })
    else:
        fail_for_merge.append({
            "carepro_AuthrequestId": row["carepro_AuthrequestId"],
            "carepro_DocumentId": row["carepro_DocumentId"],
            "error_message": row["message"],
            "error_code": row["status_code"],
            "full_path": row["full_path"],
            "retry_count": row["retry_count"] + 1,
            "reprocess_success": 0,
            "Date_Created": datetime_now,
            "Date_updated": datetime_now
        })
        fail_reason_logs.append({
            "carepro_AuthrequestId": row["carepro_AuthrequestId"],
            "carepro_DocumentId": row["carepro_DocumentId"],
            "full_path": row["full_path"],
            "error_message": row["message"],
            "error_code": row["status_code"],
            "Date_Created": datetime_now,
            "Date_updated": datetime_now
        })

if success_rows:
    success_df_out = spark.createDataFrame(success_rows)
    success_df_out.write.format("delta").mode("append").save(SUCCESS_PATH)

if fail_reason_logs:
    reason_df = spark.createDataFrame(fail_reason_logs)
    reason_df.write.format("delta").mode("append").save(FAILURE_REASON_PATH)


if fail_for_merge:
    merge_failures_df = spark.createDataFrame(fail_for_merge)
    merge_failures_df.createOrReplaceTempView("incoming_failures")

    spark.sql(f"""
        MERGE INTO delta.`{FAILURE_PATH}` as target
        USING incoming_failures as source
        ON target.carepro_AuthrequestId = source.carepro_AuthrequestId AND target.carepro_DocumentId = source.carepro_DocumentId
        WHEN MATCHED THEN UPDATE SET
            target.retry_count = source.retry_count,
            target.full_path = source.full_path,
            target.reprocess_success = source.reprocess_success,
            target.error_message = source.error_message,
            target.error_code = source.error_code,
            target.Date_updated = source.Date_updated
        WHEN NOT MATCHED THEN INSERT *
    """)

if success_rows:
    reprocess_df = spark.createDataFrame([{
        "carepro_AuthrequestId": row["carepro_AuthrequestId"],
        "carepro_DocumentId": row["carepro_DocumentId"],
        "full_path": row["full_path"],
        "retry_count": 0,  # unchanged
        "reprocess_success": 1,
        "Date_updated": datetime_now
    } for row in success_rows])

    reprocess_df.createOrReplaceTempView("success_updates")

    spark.sql(f"""
        MERGE INTO delta.`{FAILURE_PATH}` as target
        USING success_updates as source
        ON target.carepro_AuthrequestId = source.carepro_AuthrequestId AND target.carepro_DocumentId = source.carepro_DocumentId
        WHEN MATCHED THEN UPDATE SET
            target.reprocess_success = 1,
            target.Date_updated = source.Date_updated
    """)
