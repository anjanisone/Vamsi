from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit
import datetime
from smb_blob_utils import (
    validate_smb_path,
    _get_secret,
    read_file_from_server_cached,
    upload_to_blob_cached
)

spark = SparkSession.builder.appName("SFTPBlobRetryMerge").getOrCreate()

INPUT_PATH = "Tables/dbo/document"
SUCCESS_PATH = "Tables/dbo/document_success"
FAILURE_PATH = "Tables/dbo/document_failure"
FAILURE_REASON_PATH = "Tables/dbo/document_failure_details"

input_df = spark.read.format("delta").load(INPUT_PATH)
fail_df = spark.read.format("delta").load(FAILURE_PATH)
success_df = spark.read.format("delta").load(SUCCESS_PATH)

input_df = input_df.withColumn("full_path", concat_ws("\\\\",
    col("carepro_RootFolderPath"),
    col("carepro_ContainerFolders"),
    col("carepro_DocumentBase_Annotation_carepro_FileName"))
).withColumnRenamed("carepro_AuthrequestId", "carepro_AuthrequestId"
).withColumnRenamed("carepro_DocumentBase_Annotation_Id", "carepro_DocumentId"
).select("carepro_AuthrequestId", "carepro_DocumentId", "full_path")

new_files_df = input_df.join(
    success_df.select("carepro_AuthrequestId", "carepro_DocumentId").distinct(),
    on=["carepro_AuthrequestId", "carepro_DocumentId"], how="left_anti"
)

retryable_df = fail_df.filter(
    (col("retry_count") < 5) & (col("reprocess_success") == 0)
).select("carepro_AuthrequestId", "carepro_DocumentId", "full_path", "retry_count", "reprocess_success")

new_files_df = new_files_df.withColumn("retry_count", lit(0)).withColumn("reprocess_success", lit(0))
final_df = new_files_df.unionByName(retryable_df)

records = final_df.toPandas().to_dict(orient="records")

secrets = {
    "storage_conn": _get_secret("storageacct-sce1dvstorum001-connstring"),
    "container": _get_secret("fhirlite-container-name"),
    "smb_server": _get_secret("dev-fileshare-server"),
    "smb_username": _get_secret("dev-fileshare-username"),
    "smb_password": _get_secret("dev-fileshare-password"),
}

datetime_now = datetime.datetime.now(datetime.timezone.utc).isoformat()

success_rows, fail_for_merge, fail_reason_logs = [], [], []

for row in records:
    try:
        validate_smb_path(row["full_path"])
        file_bytes = read_file_from_server_cached(row["full_path"], secrets)
        blob_key = row["full_path"].replace("\\", "/").lstrip("\\")
        upload_to_blob_cached(file_bytes, blob_key, secrets)

        success_rows.append({
            "carepro_AuthrequestId": row["carepro_AuthrequestId"],
            "carepro_DocumentId": row["carepro_DocumentId"],
            "full_path": row["full_path"],
            "azure_blob_path": blob_key,
            "execution_time_ms": 0,
            "Date_Created": datetime_now,
            "Date_updated": datetime_now
        })

    except Exception as e:
        fail_for_merge.append({
            "carepro_AuthrequestId": row["carepro_AuthrequestId"],
            "carepro_DocumentId": row["carepro_DocumentId"],
            "error_message": str(e),
            "error_code": 500,
            "full_path": row["full_path"],
            "retry_count": row.get("retry_count", 0) + 1,
            "reprocess_success": 0,
            "Date_Created": datetime_now,
            "Date_updated": datetime_now
        })

        fail_reason_logs.append({
            "carepro_AuthrequestId": row["carepro_AuthrequestId"],
            "carepro_DocumentId": row["carepro_DocumentId"],
            "full_path": row["full_path"],
            "error_message": str(e),
            "error_code": 500,
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
        "retry_count": 0,
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
