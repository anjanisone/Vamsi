import datetime
import aiohttp
import asyncio
from pyspark.sql.functions import col, lit, concat
from delta.tables import DeltaTable
from typing import Dict, List, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type,
)


class Config:
    AZURE_FUNCTION_URL: str = "https://dv-func-sftpfilecp-01.azurewebsites.net/api/sftp-to-blob-copy-file"
    MAX_WORKERS: int = 30
    BATCH_SIZE: int = 5000
    MAX_RETRIES: int = 3


PATHS: Dict[str, str] = {
    "INPUT_PATH": "Tables/dbo/document",
    "SUCCESS_PATH": "Tables/dbo/document_success",
    "FAILURE_PATH": "Tables/dbo/document_failure",
    "FAILURE_DETAIL_PATH": "Tables/dbo/document_failure_details",
}


@retry(
    stop=stop_after_attempt(Config.MAX_RETRIES),
    wait=wait_random_exponential(min=2, max=5),
    retry=retry_if_exception_type((aiohttp.ClientError, aiohttp.ClientConnectionError)),
)
async def _call_azure_function(session: aiohttp.ClientSession, payload: dict, headers: dict) -> dict:
    async with session.post(Config.AZURE_FUNCTION_URL, json=payload, headers=headers) as response:
        if response.status != 200:
            text = await response.text()
            raise Exception(f"HTTP {response.status}: {text}")
        return await response.json()


async def _process_http_call(session: aiohttp.ClientSession, record: Row, correlation_id: str) -> Dict[str, Any]:
    record_dict = record.asDict()
    payload = {
        "file_path": record_dict["full_path"],
        "ar_number": record_dict["carepro_ARNumber"],
        "correlation_id": correlation_id,
    }
    headers = {
        "x-correlation-id": correlation_id,
        "Content-Type": "application/json",
    }

    start_time = datetime.datetime.now()
    result = await _call_azure_function(session, payload, headers)
    execution_time_ms = int((datetime.datetime.now() - start_time).total_seconds() * 1000)

    if result.get("status") == "failed":
        return {
            "type": "failure",
            "fail_data": {
                "carepro_AuthrequestId": record_dict["carepro_AuthrequestId"],
                "carepro_DocumentId": record_dict["carepro_DocumentId"],
                "Latest_Correlation_Id": correlation_id,
                "Latest_Error_Code": 500,
                "Latest_Error_Message": result.get("error"),
                "Full_Path": record_dict["full_path"],
                "Reprocess_Success": 0,
                "Date_Created": datetime.datetime.now(datetime.timezone.utc),
                "Date_Updated": datetime.datetime.now(datetime.timezone.utc),
                "carepro_ARNumber": record_dict["carepro_ARNumber"],
            },
        }

    return {
        "type": "success",
        "data": {
            "carepro_AuthrequestId": record_dict["carepro_AuthrequestId"],
            "carepro_DocumentId": record_dict["carepro_DocumentId"],
            "Correlation_Id": correlation_id,
            "Full_Path": record_dict["full_path"],
            "Azure_Blob_Path": result.get("azure_blob_path", ""),
            "Execution_Time_ms": execution_time_ms,
            "Date_Created": datetime.datetime.now(datetime.timezone.utc),
            "Date_Updated": datetime.datetime.now(datetime.timezone.utc),
            "carepro_ARNumber": record_dict["carepro_ARNumber"],
        },
    }


def prepare_file_path(spark: SparkSession) -> DataFrame:
    delta_table = DeltaTable.forPath(spark, PATHS["INPUT_PATH"])
    doc_df = delta_table.toDF().filter(col("carepro_RootFolderPath").isNotNull())

    doc_df = (
        doc_df.withColumn(
            "full_path",
            concat(
                col("carepro_RootFolderPath"),
                lit("\\\\"),
                col("carepro_ContainerFolders"),
                lit("\\\\"),
                col("carepro_DocumentBase_Annotation_carepro_FileName"),
            ),
        )
        .withColumnRenamed("carepro_DocumentBase_Annotation_Id", "carepro_DocumentId")
        .withColumnRenamed("carepro_AuthrequestBase_carepro_Name", "carepro_ARNumber")
        .select("carepro_AuthrequestId", "carepro_DocumentId", "carepro_ARNumber", "full_path")
    )

    fail_df = spark.read.format("delta").load(PATHS["FAILURE_PATH"])
    success_document_df = spark.read.format("delta").load(PATHS["SUCCESS_PATH"])

    new_files_df = (
        doc_df.join(
            success_document_df.select("carepro_AuthrequestId", "carepro_DocumentId").distinct(),
            on=["carepro_AuthrequestId", "carepro_DocumentId"],
            how="left_anti",
        )
        .join(
            fail_df.select("carepro_AuthrequestId", "carepro_DocumentId").distinct(),
            on=["carepro_AuthrequestId", "carepro_DocumentId"],
            how="left_anti",
        )
        .withColumn("retry_count", lit(0))
        .withColumn("reprocess_success", lit(0))
    )

    retry_fail_df = fail_df.filter(
        (col("retry_count") <= 5) & (col("reprocess_success") == 0)
    ).select("carepro_AuthrequestId", "carepro_DocumentId", "carepro_ARNumber", "full_path", "retry_count", "reprocess_success")

    final_df = new_files_df.unionByName(retry_fail_df)
    return final_df


def _merge_results(spark: SparkSession, success_path: List[Dict[str, Any]], fail_path: List[Dict[str, Any]]) -> None:
    success_count = len(success_path)
    failure_count = len(fail_path)

    failure_delta_table = DeltaTable.forPath(spark, PATHS["FAILURE_PATH"])

    if success_count:
        success_df = spark.createDataFrame(success_path)
        DeltaTable.forPath(spark, PATHS["SUCCESS_PATH"]).alias("target").merge(
            success_df.alias("source"),
            "target.carepro_AuthrequestId = source.carepro_AuthrequestId AND target.carepro_DocumentId = source.carepro_DocumentId"
        ).whenNotMatchedInsert(values={
            "carepro_AuthrequestId": col("source.carepro_AuthrequestId"),
            "carepro_DocumentId": col("source.carepro_DocumentId"),
            "Correlation_Id": col("source.Correlation_Id"),
            "Full_Path": col("source.Full_Path"),
            "Azure_Blob_Path": col("source.Azure_Blob_Path"),
            "Execution_Time_ms": col("source.Execution_Time_ms"),
            "Date_Updated": col("source.Date_Updated"),
            "Date_Created": col("source.Date_Created"),
            "carepro_ARNumber": col("source.carepro_ARNumber"),
        }).execute()

    if failure_count:
        df_fail_merge = spark.createDataFrame(fail_path)
        failure_delta_table.alias("target").merge(
            df_fail_merge.alias("source"),
            "target.carepro_AuthrequestId = source.carepro_AuthrequestId AND target.carepro_DocumentId = source.carepro_DocumentId"
        ).whenMatchedUpdate(set={
            "Retry_Count": col("target.Retry_Count") + lit(1),
            "Latest_Error_Message": col("source.Latest_Error_Message"),
            "Latest_Error_Code": col("source.Latest_Error_Code"),
            "Date_Updated": col("source.Date_Updated"),
            "Latest_Correlation_Id": col("source.Latest_Correlation_Id"),
        }).whenNotMatchedInsert(values={
            "carepro_AuthrequestId": col("source.carepro_AuthrequestId"),
            "carepro_DocumentId": col("source.carepro_DocumentId"),
            "Latest_Correlation_Id": col("source.Latest_Correlation_Id"),
            "Retry_Count": lit(0),
            "Full_Path": col("source.Full_Path"),
            "Reprocess_Success": col("source.Reprocess_Success"),
            "Latest_Error_Message": col("source.Latest_Error_Message"),
            "Latest_Error_Code": col("source.Latest_Error_Code"),
            "Date_Created": col("source.Date_Created"),
            "Date_Updated": col("source.Date_Updated"),
            "carepro_ARNumber": col("source.carepro_ARNumber"),
        }).execute()


async def copy_document_to_blob(spark: SparkSession, correlation_id: str, final_df: DataFrame):
    success_path = []
    fail_path = []

    records = final_df.collect()
    total_records = len(records)

    if total_records == 0:
        print("No records to process")
        return

    num_batches = (total_records + Config.BATCH_SIZE - 1) // Config.BATCH_SIZE

    for batch_num in range(num_batches):
        start = batch_num * Config.BATCH_SIZE
        end = min(start + Config.BATCH_SIZE, total_records)
        batch_records = records[start:end]

        async with aiohttp.ClientSession() as session:
            tasks = [_process_http_call(session, record, correlation_id) for record in batch_records]
            results = await asyncio.gather(*tasks)

        for result in results:
            if result["type"] == "success":
                success_path.append(result["data"])
            else:
                fail_path.append(result["fail_data"])

        print(f"Batch {batch_num + 1}/{num_batches} completed. Success: {len(success_path)}, Failure: {len(fail_path)}")
        _merge_results(spark, success_path, fail_path)
        success_path.clear()
        fail_path.clear()

        if batch_num < num_batches - 1:
            await asyncio.sleep(10)
