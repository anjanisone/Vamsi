from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import requests

spark = SparkSession.builder.appName("SFTPBlobRetryMerge").getOrCreate()

AZURE_FUNCTION_URL = "http://localhost:7071/api/function_app"  # Replace with actual

INPUT_PATH = "abfss://<container>@<account>.dfs.core.windows.net/source/input_table"
SUCCESS_PATH = "abfss://<container>@<account>.dfs.core.windows.net/audit/success_table"
FAILURE_PATH = "abfss://<container>@<account>.dfs.core.windows.net/audit/failure_table"
FAILURE_REASON_PATH = "abfss://<container>@<account>.dfs.core.windows.net/audit/failure_reason_table"

input_df = spark.read.format("delta").load(INPUT_PATH)
fail_df = spark.read.format("delta").load(FAILURE_PATH)
success_df = spark.read.format("delta").load(SUCCESS_PATH)

input_df = input_df.withColumn("full_path", concat_ws("\\\\",
    col("carepro_RootFolderPath"),
    col("carepro_ContainerFolders"),
    col("carepro_DocumentBase_Annotation_carepro_FileName"))
).withColumnRenamed("carepro_AuthrequestId", "auth_request_id"
).withColumnRenamed("carepro_DocumentBase_Annotation_Id", "annotation_id"
).select("auth_request_id", "annotation_id", "full_path")

new_files_df = input_df.join(
    success_df.select("auth_request_id", "annotation_id").distinct(),
    on=["auth_request_id", "annotation_id"], how="left_anti"
)

retryable_df = fail_df.filter(
    (col("retry_number") < 5) & (col("reprocess_success") == 0)
).select("auth_request_id", "annotation_id", "full_path", "retry_number", "reprocess_success")

new_files_df = new_files_df.withColumn("retry_number", lit(0)).withColumn("reprocess_success", lit(0))
final_df = new_files_df.unionByName(retryable_df)
records = final_df.toPandas().to_dict(orient="records")

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
            return ("success", {
                "auth_request_id": row["auth_request_id"],
                "annotation_id": row["annotation_id"],
                "full_path": row["full_path"],
                "azure_blob_path": res_json["azure_blob_path"],
                "execution_time_ms": elapsed
            })
        else:
            return ("failure", {
                "auth_request_id": row["auth_request_id"],
                "annotation_id": row["annotation_id"],
                "full_path": row["full_path"],
                "retry_number": row["retry_number"] + 1,
                "reprocess_success": 0
            }, {
                "auth_request_id": row["auth_request_id"],
                "annotation_id": row["annotation_id"],
                "full_path": row["full_path"],
                "message": res_json.get("message", ""),
                "status_code": res.status_code
            })
    except Exception as e:
        return ("failure", {
            "auth_request_id": row["auth_request_id"],
            "annotation_id": row["annotation_id"],
            "full_path": row["full_path"],
            "retry_number": row["retry_number"] + 1,
            "reprocess_success": 0
        }, {
            "auth_request_id": row["auth_request_id"],
            "annotation_id": row["annotation_id"],
            "full_path": row["full_path"],
            "message": str(e),
            "status_code": 500
        })

success_rows, fail_for_merge, fail_reason_logs = [], [], []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(call_http_api, row): row for row in records}
    for future in as_completed(futures):
        res = future.result()
        if res[0] == "success":
            success_rows.append(res[1])
        else:
            fail_for_merge.append(res[1])
            fail_reason_logs.append(res[2])

if success_rows:
    spark.createDataFrame(success_rows).write.format("delta").mode("append").save(SUCCESS_PATH)

if fail_reason_logs:
    spark.createDataFrame(fail_reason_logs).write.format("delta").mode("append").save(FAILURE_REASON_PATH)

if fail_for_merge:
    df_merge = spark.createDataFrame(fail_for_merge)
    df_merge.createOrReplaceTempView("incoming_failures")
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
    df_success_update = spark.createDataFrame([
        {
            "auth_request_id": row["auth_request_id"],
            "annotation_id": row["annotation_id"],
            "full_path": row["full_path"],
            "retry_number": 0,
            "reprocess_success": 1
        }
        for row in success_rows
    ])
    df_success_update.createOrReplaceTempView("success_updates")
    spark.sql(f"""
        MERGE INTO delta.`{FAILURE_PATH}` as target
        USING success_updates as source
        ON target.auth_request_id = source.auth_request_id AND target.annotation_id = source.annotation_id
        WHEN MATCHED THEN UPDATE SET
            target.reprocess_success = 1
    """)
