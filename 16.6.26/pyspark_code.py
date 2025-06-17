from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit
import time
import requests

# Initialize Spark
spark = SparkSession.builder.appName("SFTPBlobRetryUnion").getOrCreate()

# Configurations
AZURE_FUNCTION_URL = "http://localhost:7071/api/function_app"

# Input Paths
INPUT_PATH = "abfss://<container>@<account>.dfs.core.windows.net/source/input_table"
SUCCESS_PATH = "abfss://<container>@<account>.dfs.core.windows.net/audit/success_table"
FAILURE_PATH = "abfss://<container>@<account>.dfs.core.windows.net/audit/failure_table"
FAILURE_REASON_PATH = "abfss://<container>@<account>.dfs.core.windows.net/audit/failure_reason_table"

# Read data
input_df = spark.read.format("delta").load(INPUT_PATH)
fail_df = spark.read.format("delta").load(FAILURE_PATH)
success_df = spark.read.format("delta").load(SUCCESS_PATH)

# Prepare today's input
input_df = input_df.withColumn("full_path", concat_ws("\\\\",
    col("carepro_RootFolderPath"),
    col("carepro_ContainerFolders"),
    col("carepro_DocumentBase_Annotation_carepro_FileName"))
).withColumnRenamed("carepro_AuthrequestId", "auth_request_id"
).withColumnRenamed("carepro_DocumentBase_Annotation_Id", "annotation_id"
).select("auth_request_id", "annotation_id", "full_path")

# New files = input - success
new_files_df = input_df.join(
    success_df.select("auth_request_id", "annotation_id"), 
    on=["auth_request_id", "annotation_id"], how="left_anti"
).withColumn("retry_number", lit(0)).withColumn("reprocess_success", lit(0))

# Retry files = from failures only, no join to input
retry_files_df = fail_df.filter(
    (col("retry_number") < 5) & (col("reprocess_success") == 0)
).select("auth_request_id", "annotation_id", "full_path", "retry_number", "reprocess_success")

# Union both sets
final_df = new_files_df.unionByName(retry_files_df)

# Prepare to call API
records = final_df.toPandas().to_dict(orient="records")

success_records = []
failure_upserts = []
failure_reason_records = []
reprocess_success_updates = []

fail_map = {
    (row["auth_request_id"], row["annotation_id"]): row
    for row in fail_df.collect()
}

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

for row in records:
    key = (row["auth_request_id"], row["annotation_id"])
    retry = row["retry_number"]
    reprocess_success = row["reprocess_success"]

    result = call_http_api(row)

    if result["status"] == "success":
        success_records.append({
            "auth_request_id": row["auth_request_id"],
            "annotation_id": row["annotation_id"],
            "full_path": row["full_path"],
            "azure_blob_path": result["azure_blob_path"],
            "execution_time_ms": result["execution_time_ms"]
        })
        if key in fail_map and fail_map[key]["reprocess_success"] == 0:
            reprocess_success_updates.append({
                "auth_request_id": row["auth_request_id"],
                "annotation_id": row["annotation_id"],
                "full_path": row["full_path"],
                "retry_number": retry,
                "reprocess_success": 1
            })

    else:
        failure_upserts.append({
            "auth_request_id": row["auth_request_id"],
            "annotation_id": row["annotation_id"],
            "full_path": row["full_path"],
            "retry_number": retry + 1,
            "reprocess_success": 0
        })
        failure_reason_records.append({
            "auth_request_id": row["auth_request_id"],
            "annotation_id": row["annotation_id"],
            "full_path": row["full_path"],
            "message": result["message"],
            "status_code": result["status_code"]
        })

# Convert to DataFrames
if success_records:
    spark.createDataFrame(success_records).write.format("delta").mode("append").save(SUCCESS_PATH)

if failure_upserts:
    spark.createDataFrame(failure_upserts).write.format("delta").mode("append").save(FAILURE_PATH)

if failure_reason_records:
    spark.createDataFrame(failure_reason_records).write.format("delta").mode("append").save(FAILURE_REASON_PATH)

if reprocess_success_updates:
    spark.createDataFrame(reprocess_success_updates).write.format("delta").mode("append").save(FAILURE_PATH)
