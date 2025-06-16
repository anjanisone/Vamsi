import requests
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from typing import Dict

# Config
AZURE_FUNCTION_URL = "http://localhost:7071/api/function_app"
INPUT_TABLE = "abfss://raw@<youraccount>.dfs.core.windows.net/path/input_table"
ADL_SUCCESS_PATH = "abfss://processed@<youraccount>.dfs.core.windows.net/path/success_table"
ADL_FAILURE_PATH = "abfss://processed@<youraccount>.dfs.core.windows.net/path/failure_table"
ADL_FAILURE_REASON_PATH = "abfss://processed@<youraccount>.dfs.core.windows.net/path/failure_reason_table"

def make_http_call(record: Dict) -> Dict:
    payload = {
        "file_path": record["full_path"],
        "carepro_AuthrequestId": record["auth_request_id"],
        "carepro_DocumentBase_Annotation_Id": record["annotation_id"]
    }
    start_time = time.time()
    try:
        response = requests.post(AZURE_FUNCTION_URL, json=payload)
        duration = int((time.time() - start_time) * 1000)
        response_json = response.json()
        if response.status_code == 200 and response_json.get("status") == "Success":
            return {
                "auth_request_id": record["auth_request_id"],
                "annotation_id": record["annotation_id"],
                "full_path": record["full_path"],
                "azure_blob_path": response_json.get("azure_blob_path"),
                "execution_time_ms": duration,
                "reprocess_success": 0,
                "type": "success"
            }
        else:
            return {
                "auth_request_id": record["auth_request_id"],
                "annotation_id": record["annotation_id"],
                "full_path": record["full_path"],
                "retry_number": record["retry_number"] + 1,
                "reprocess_success": 1,
                "message": response_json.get("message", "No message"),
                "status_code": response.status_code,
                "type": "failure_reason"
            }
    except Exception as e:
        return {
            "auth_request_id": record["auth_request_id"],
            "annotation_id": record["annotation_id"],
            "full_path": record["full_path"],
            "retry_number": record["retry_number"] + 1,
            "reprocess_success": 1,
            "type": "failure"
        }

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SFTP-to-Blob-Retry-Audit").getOrCreate()

    input_df = spark.read.format("delta").load(INPUT_TABLE)

    input_df = input_df.withColumn("full_path", concat_ws("\\\\",
        col("carepro_RootFolderPath"),
        col("carepro_ContainerFolders"),
        col("carepro_DocumentBase_Annotation_carepro_FileName")
    ))

    df_selected = input_df.select(
        col("full_path"),
        col("carepro_AuthrequestId").alias("auth_request_id"),
        col("carepro_DocumentBase_Annotation_Id").alias("annotation_id")
    ).withColumn("composite_key", concat_ws("::", col("auth_request_id"), col("annotation_id")))

    success_df = spark.read.format("delta").load(ADL_SUCCESS_PATH) \
        .withColumn("composite_key", concat_ws("::", col("auth_request_id"), col("annotation_id"))) \
        .select("composite_key")

    fail_df = spark.read.format("delta").load(ADL_FAILURE_PATH) \
        .withColumn("composite_key", concat_ws("::", col("auth_request_id"), col("annotation_id")))

    new_files_df = df_selected.join(success_df, on="composite_key", how="left_anti") \
        .withColumn("retry_number", lit(0)).withColumn("reprocess_success", lit(1))

    retry_candidates_df = fail_df.filter((col("retry_number") < 5) & (col("reprocess_success") == 1)) \
        .select("composite_key", "retry_number")

    retry_files_df = df_selected.join(retry_candidates_df, on="composite_key", how="inner")

    final_df = new_files_df.unionByName(retry_files_df)
    records = final_df.toPandas().to_dict(orient="records")

    success_logs = []
    failure_logs = []
    failure_reason_logs = []

    for record in records:
        result = make_http_call(record)
        if result["type"] == "success":
            success_logs.append(result)
        elif result["type"] == "failure":
            failure_logs.append(result)
        elif result["type"] == "failure_reason":
            failure_reason_logs.append(result)

    if success_logs:
        success_schema = StructType([
            StructField("auth_request_id", StringType(), True),
            StructField("annotation_id", StringType(), True),
            StructField("full_path", StringType(), True),
            StructField("azure_blob_path", StringType(), True),
            StructField("execution_time_ms", LongType(), True),
            StructField("reprocess_success", IntegerType(), True),
        ])
        spark.createDataFrame(success_logs, success_schema) \
            .write.format("delta").mode("append").save(ADL_SUCCESS_PATH)

    if failure_logs:
        failure_schema = StructType([
            StructField("auth_request_id", StringType(), True),
            StructField("annotation_id", StringType(), True),
            StructField("full_path", StringType(), True),
            StructField("retry_number", IntegerType(), True),
            StructField("reprocess_success", IntegerType(), True),
        ])
        spark.createDataFrame(failure_logs, failure_schema) \
            .write.format("delta").mode("append").save(ADL_FAILURE_PATH)

    if failure_reason_logs:
        failure_reason_schema = StructType([
            StructField("auth_request_id", StringType(), True),
            StructField("annotation_id", StringType(), True),
            StructField("full_path", StringType(), True),
            StructField("retry_number", IntegerType(), True),
            StructField("reprocess_success", IntegerType(), True),
            StructField("message", StringType(), True),
            StructField("status_code", IntegerType(), True),
        ])
        spark.createDataFrame(failure_reason_logs, failure_reason_schema) \
            .write.format("delta").mode("append").save(ADL_FAILURE_REASON_PATH)
