import requests
import json
import time
from pyspark.sql.functions import concat_ws, col
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql import SparkSession

AZURE_FUNCTION_URL = "https://<your-function-app>.azurewebsites.net/api/function_app"
TABLE_PATH = "abfss://raw@<your-datalake-name>.dfs.core.windows.net/path/to/source_table"
AUDIT_WRITE_PATH = "abfss://processed@<your-datalake-name>.dfs.core.windows.net/path/to/audit_table"

spark = SparkSession.builder.appName("SFTP-to-Blob-Audit").getOrCreate()

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

records = df_selected.toPandas()

audit_logs = []

for _, row in records.iterrows():
    payload = {
        "file_path": row["full_path"],
        "carepro_AuthrequestId": row["auth_request_id"],
        "carepro_DocumentBase_Annotation_Id": row["annotation_id"]
    }

    start_time = time.time()
    try:
        response = requests.post(AZURE_FUNCTION_URL, json=payload)
        end_time = time.time()
        response_json = response.json()

        audit_logs.append({
            "auth_request_id": row["auth_request_id"],
            "annotation_id": row["annotation_id"],
            "full_path": row["full_path"],
            "status": response_json.get("status"),
            "status_code": response.status_code,
            "azure_blob_path": response_json.get("azure_blob_path", None),
            "execution_time_ms": int((end_time - start_time) * 1000),
            "message": response_json.get("message", "")
        })

    except Exception as e:
        end_time = time.time()
        audit_logs.append({
            "auth_request_id": row["auth_request_id"],
            "annotation_id": row["annotation_id"],
            "full_path": row["full_path"],
            "status": "Failed",
            "status_code": 500,
            "azure_blob_path": None,
            "execution_time_ms": int((end_time - start_time) * 1000),
            "message": str(e)
        })

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
