from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit
import time
import requests

spark = SparkSession.builder.appName("SFTPBlobRetryMerge").getOrCreate()

AZURE_FUNCTION_URL = ""

# Delta Table Paths
INPUT_PATH = "abfss://<container>@<account>.dfs.core.windows.net/source/input_table"
SUCCESS_PATH = "abfss://<container>@<account>.dfs.core.windows.net/audit/success_table"
FAILURE_PATH = "abfss://<container>@<account>.dfs.core.windows.net/audit/failure_table"
FAILURE_REASON_PATH = "abfss://<container>@<account>.dfs.core.windows.net/audit/failure_reason_table"

# Load tables
input_df = spark.read.format("delta").load(INPUT_PATH)
fail_df = spark.read.format("delta").load(FAILURE_PATH)
success_df = spark.read.format("delta").load(SUCCESS_PATH)

# Preprocess input
input_df = input_df.withColumn("full_path", concat_ws("\\\\",
    col("carepro_RootFolderPath"),
    col("carepro_ContainerFolders"),
    col("carepro_DocumentBase_Annotation_carepro_FileName"))
).withColumnRenamed("carepro_AuthrequestId", "auth_request_id"
).withColumnRenamed("carepro_DocumentBase_Annotation_Id", "annotation_id"
).select("auth_request_id", "annotation_id", "full_path")

# Filter new files (not in success)
new_files_df = input_df.join(
    success_df.select("auth_request_id", "annotation_id").distinct(),
    on=["auth_request_id", "annotation_id"], how="left_anti"
)

# Filter retryable failed files (retry < 5 and not reprocessed)
retryable_df = fail_df.filter(
    (col("retry_number") < 5) & (col("reprocess_success") == 0)
).select("auth_request_id", "annotation_id", "full_path", "retry_number", "reprocess_success")

# Final candidates = new + retry
new_files_df = new_files_df.withColumn("retry_number", lit(0)).withColumn("reprocess_success", lit(0))
final_df = new_files_df.unionByName(retryable_df)

records = final_df.toPandas().to_dict(orient="records")

# Call HTTP API
def call_http_api(row):
    payload = {
        "file_path": row["full_path"],
        "carepro_AuthrequestId": row["auth_request_id"],
        "carepro_DocumentBase_Annotation_Id": row["annotation_id"]
    }
    start = time.time()
    try:
        res = requests.post(AZURE_FUNCTION_URL, json=payload)
        elapsed = int((time.time() - start) * 1000)
        res_json = res.json()
        if res.status_code == 200 and res_json.get("status") == "Success":
            return {**row, "azure_blob_path": res_json["azure_blob_path"], "execution_time_ms": elapsed, "status": "success"}
        else:
            return {**row, "message": res_json.get("message", ""), "status_code": res.status_code, "status": "failure"}
    except Exception as e:
        return {**row, "message": str(e), "status_code": 500, "status": "failure"}

success_rows = []
fail_for_merge = []
fail_reason_logs = []

for row in records:
    result = call_http_api(row)
    if result["status"] == "success":
        success_rows.append({
            "auth_request_id": row["auth_request_id"],
            "annotation_id": row["annotation_id"],
            "full_path": row["full_path"],
            "azure_blob_path": result["azure_blob_path"],
            "execution_time_ms": result["execution_time_ms"]
        })
    else:
        fail_for_merge.append({
            "auth_request_id": row["auth_request_id"],
            "annotation_id": row["annotation_id"],
            "full_path": row["full_path"],
            "retry_number": row["retry_number"] + 1,
            "reprocess_success": 0
        })
        fail_reason_logs.append({
            "auth_request_id": row["auth_request_id"],
            "annotation_id": row["annotation_id"],
            "full_path": row["full_path"],
            "message": result["message"],
            "status_code": result["status_code"]
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
        ON target.auth_request_id = source.auth_request_id AND target.annotation_id = source.annotation_id
        WHEN MATCHED THEN UPDATE SET
            target.retry_number = source.retry_number,
            target.full_path = source.full_path,
            target.reprocess_success = source.reprocess_success
        WHEN NOT MATCHED THEN INSERT *
    """)

if success_rows:
    reprocess_df = spark.createDataFrame([{
        "auth_request_id": row["auth_request_id"],
        "annotation_id": row["annotation_id"],
        "full_path": row["full_path"],
        "retry_number": 0,  # unchanged
        "reprocess_success": 1
    } for row in success_rows])

    reprocess_df.createOrReplaceTempView("success_updates")

    spark.sql(f"""
        MERGE INTO delta.`{FAILURE_PATH}` as target
        USING success_updates as source
        ON target.auth_request_id = source.auth_request_id AND target.annotation_id = source.annotation_id
        WHEN MATCHED THEN UPDATE SET
            target.reprocess_success = 1
    """)
