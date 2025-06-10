import requests
import json
import time
from pyspark.sql.functions import concat_ws, col
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor, as_completed

AZURE_FUNCTION_URL = "https://<your-function-app>.azurewebsites.net/api/function_app"
TABLE_PATH = "abfss://raw@<your-datalake-name>.dfs.core.windows.net/path/to/source_table"
AUDIT_WRITE_PATH = "abfss://processed@<your-datalake-name>.dfs.core.windows.net/path/to/audit_table"
THREAD_COUNT = 10

spark = SparkSession.builder.appName("SFTP-to-Blob-Audit-Parallel").getOrCreate()

df = spark.read.format("delta").load(TABLE_PATH)

df = df.withColumn("full_path", concat_ws("\\\\",
    col("carepro_RootFolderPath"),
    col("carepro_ContainerFolders"),
    col("carepro_DocumentBase_Annotation_carepro_FileName")
))

df_selected = df.select(
    col("full_path"),
    col("carepro_AuthrequestId").alias("auth_request_id"),
    col("carepro_DocumentBase_Annotation_Id").alias("annotation_id")
)

records = df_selected.toPandas().to_dict(orient="records")
audit_logs = []

def call_api(record):
    start_time = time.time()
    payload = {
        "file_path": record["full_path"],
        "carepro_AuthrequestId": record["auth_request_id"],
        "carepro_DocumentBase_Annotation_Id": record["annotation_id"]
    }
    try:
        response = requests.post(AZURE_FUNCTION_URL, json=payload)
        response_json = response.json()
        return {
            "auth_request_id": record["auth_request_id"],
            "annotation_id": record["annotation_id"],
            "full_path": record["full_path"],
            "status": response_json.get("status"),
            "status_code": response.status_code,
            "azure_blob_path": response_json.get("azure_blob_path", None),
            "execution_time_ms": int((time.time() - start_time) * 1000),
            "message": response_json.get("message", "")
        }
    except Exception as e:
        return {
            "auth_request_id": record["auth_request_id"],
            "annotation_id": record["annotation_id"],
            "full_path": record["full_path"],
            "status": "Failed",
            "status_code": 500,
            "azure_blob_path": None,
            "execution_time_ms": int((time.time() - start_time) * 1000),
            "message": str(e)
        }

with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
    futures = [executor.submit(call_api, record) for record in records]
    for future in as_completed(futures):
        audit_logs.append(future.result())

schema = StructType([
    StructField("auth_request_id", StringType(), True),
    StructField("annotation_id", StringType(), True),
    StructField("full_path", StringType(), True),
    StructField("status", StringType(), True),
    StructField("status_code", LongType(), True),
    StructField("azure_blob_path", StringType(), True),
    StructField("execution_time_ms", LongType(), True),
    StructField("message", StringType(), True),
])

audit_df = spark.createDataFrame(audit_logs, schema)
audit_df.write.format("delta").mode("append").save(AUDIT_WRITE_PATH)
