from datetime import datetime, timezone
from aiohttp import (
    ClientSession,
    ClientResponseError,
    ClientConnectionError,
    ServerTimeoutError,
)
from asyncio import sleep, gather
from pyspark.sql.functions import col, lit, concat
from delta.tables import DeltaTable
from typing import List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row
from constants import (
    DOCUMENT_TABLE_PATH,
    DOCUMENT_SUCCESS_TABLE_PATH,
    DOCUMENT_FAILURE_TABLE_PATH,
    DOCUMENT_FAILURE_DETAIL_TABLE_PATH,
    DOCUMENT_COPY_BATCH_SIZE,
    DOCUMENT_COPY_RETRYABLE_STATUS_CODES,
)
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception,
)
import logging


logger = logging.getLogger(__name__)


def _is_azure_function_error_retryable(exc: BaseException) -> bool:
    if isinstance(exc, (ClientConnectionError, ServerTimeoutError)):
        return True
    if isinstance(exc, ClientResponseError):
        # retry on transient HTTP codes only
        return exc.status in DOCUMENT_COPY_RETRYABLE_STATUS_CODES
    return False


@retry(
    stop=stop_after_attempt(3),
    wait=wait_random_exponential(multiplier=0.5, max=8),
    retry=retry_if_exception(_is_azure_function_error_retryable),
    reraise=True,
)
async def _call_azure_function(
    session: ClientSession, payload: dict, headers: dict, azure_function_url
) -> dict:
    """Calls the Azure Function to copy documents to blob storage."""
    async with session.post(azure_function_url, json=payload, headers=headers) as resp:
        status = resp.status
        # Try JSON and if not get the text
        try:
            data = await resp.json(content_type=None)
        except Exception:
            data = {"message": (await resp.text())}

        if 200 <= status < 300:
            return data if isinstance(data, dict) else {"data": data}

        # Ensure message
        msg = ""
        if isinstance(data, dict):
            msg = data.get("message") or data.get("error") or data.get("detail")
        elif isinstance(data, str):
            msg = data

        # Raise with the exact status so tenacity can decide correctly
        raise ClientResponseError(
            request_info=None,
            history=(),
            status=status,
            message=msg or "HTTP error",
            headers=resp.headers,
        )


async def _process_http_call(
    session: ClientSession, row: Row, correlation_id: str, azure_function_url: str
) -> Dict[str, Any]:
    """For a single input row, call the HTTP endpoint and return"""
    rec = row.asDict()
    file_path = rec.get("full_path")
    ar_number = rec.get("carepro_ARNumber")
    carepro_authrequestid = rec.get("carepro_AuthrequestId")
    carepro_documentid = rec.get("carepro_DocumentId")

    payload = {"file_path": file_path, "ar_number": ar_number}
    headers = {"Content-Type": "application/json"}

    try:
        start_time = datetime.now(timezone.utc)

        res = await _call_azure_function(session, payload, headers, azure_function_url)

        execution_time_ms = int(
            (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        )

        # On success, Azure function typically returns: azure_blob_path + message
        success_row = {
            "carepro_AuthrequestId": carepro_authrequestid,
            "carepro_DocumentId": carepro_documentid,
            "Correlation_Id": correlation_id,
            "Full_Path": file_path,
            "Azure_Blob_Path": res.get("azure_blob_path", ""),
            "Execution_Time_ms": execution_time_ms,
            "Date_Created": datetime.now(timezone.utc),
            "Date_Updated": datetime.now(timezone.utc),
            "carepro_ARNumber": ar_number,
        }
        return {"type": "success", "success_data": success_row}

    except ClientResponseError as cre:
        error_code = cre.status
        error_message = cre.message or "HTTP error"

    except (ClientConnectionError, ServerTimeoutError) as e:
        error_code = 503
        error_message = str(e)

    except Exception as e:
        error_code = 500
        error_message = str(e)
            
    fail_row = {
            "carepro_AuthrequestId": carepro_authrequestid,
            "carepro_DocumentId": carepro_documentid,
            "Latest_Correlation_Id": correlation_id,
            "Latest_Error_Code": error_code,
            "Latest_Error_Message": error_message,
            "Full_Path": file_path,
            "Reprocess_Success": 0,
            "Date_Created": datetime.now(timezone.utc),
            "Date_Updated": datetime.now(timezone.utc),
            "carepro_ARNumber": ar_number,
        }
    return {"type": "failure", "fail_data": fail_row}



# Merge results into Deltatables


def _merge_results(
    spark: SparkSession,
    success_path: List[Dict[str, Any]],
    fail_path: List[Dict[str, Any]],
) -> None:
    """Save processing results to Delta tables with error handling"""

    if not success_path and not fail_path:
        return

    try:
        source_auth_requestid = "source.carepro_AuthrequestId"
        source_documentid = "source.carepro_DocumentId"
        source_correlation_id = "source.Correlation_Id"
        source_full_path = "source.Full_Path"
        source_azure_blob_path = "source.Azure_Blob_Path"
        source_execution_time_ms = "source.Execution_Time_ms"
        source_date_updated = "source.Date_Updated"
        source_date_created = "source.Date_Created"
        source_ar_number = "source.carepro_ARNumber"
        source_latest_error_message = "source.Latest_Error_Message"
        source_latest_error_code = "source.Latest_Error_Code"
        source_latest_correlationid = "source.Latest_Correlation_Id"
        source_reprocess_success = "source.Reprocess_Success"
        source_error_code = "source.Error_Code"
        source_error_message = "source.Error_Message"


        success_count = len(success_path)
        failure_count = len(fail_path)
        failure_delta_table = DeltaTable.forPath(spark, DOCUMENT_FAILURE_TABLE_PATH)

        logger.info(
            f"Merging results: {success_count} successes, {failure_count} failures"
        )

        # Save successful records
        if success_count > 0:
            logger.info(f"Saving {success_count} successful records")
            success_df = spark.createDataFrame(success_path)
            success_delta_table = DeltaTable.forPath(spark, DOCUMENT_SUCCESS_TABLE_PATH)

            success_delta_table.alias("target").merge(
                success_df.alias("source"),
                """target.carepro_AuthrequestId = source.carepro_AuthrequestId 
                   AND target.carepro_DocumentId = source.carepro_DocumentId""",
            ).whenNotMatchedInsert(
                values={
                    "carepro_AuthrequestId": source_auth_requestid,
                    "carepro_DocumentId": source_documentid,
                    "Correlation_Id": source_correlation_id,
                    "Full_Path": source_full_path,
                    "Azure_Blob_Path": source_azure_blob_path,
                    "Execution_Time_ms": source_execution_time_ms,
                    "Date_Updated": source_date_updated,
                    "Date_Created": source_date_created,
                    "carepro_ARNumber": source_ar_number,
                }
            ).execute()

            logger.info(f"Successfully saved {success_count} success records")

            # Update reprocess success records in failure table
            failure_delta_table.alias("target").merge(
                success_df.select(
                    "carepro_AuthrequestId",
                    "carepro_DocumentId",
                    col("Date_Updated").alias("reprocess_date"),
                ).alias("source"),
                """target.carepro_AuthrequestId = source.carepro_AuthrequestId 
                   AND target.carepro_DocumentId = source.carepro_DocumentId 
                   AND target.Reprocess_Success = 0""",
            ).whenMatchedUpdate(
                set={
                    "Reprocess_Success": lit(1),
                    "Date_Updated": col("source.reprocess_date"),
                }
            ).execute()

        # Merge failure records for retry tracking
        if failure_count > 0:
            logger.info(f"Merging {failure_count} failure records")
            df_fail_merge = spark.createDataFrame(fail_path)
            failure_delta_table.alias("target").merge(
                df_fail_merge.alias("source"),
                """target.carepro_AuthrequestId = source.carepro_AuthrequestId 
                   AND target.carepro_DocumentId = source.carepro_DocumentId""",
            ).whenMatchedUpdate(
                set={
                    "Retry_Count": col("target.Retry_Count") + lit(1),
                    "Latest_Error_Message": source_latest_error_message,
                    "Latest_Error_Code": source_latest_error_code,
                    "Date_Updated": source_date_updated,
                    "Latest_Correlation_Id": source_latest_correlationid,
                }
            ).whenNotMatchedInsert(
                values={
                    "carepro_AuthrequestId": source_auth_requestid,
                    "carepro_DocumentId": source_documentid,
                    "Latest_Correlation_Id": source_latest_correlationid,
                    "Retry_Count": lit(0),
                    "Full_Path": source_full_path,
                    "Reprocess_Success": source_reprocess_success,
                    "Latest_Error_Message": source_latest_error_message,
                    "Latest_Error_Code": source_latest_error_code,
                    "Date_Created": source_date_created,
                    "Date_Updated": source_date_updated,
                    "carepro_ARNumber": source_ar_number,
                }
            ).execute()

            logger.info(f"Successfully merged {failure_count} failure records")
            logger.info("Saving failure details to document_failure_details")

            # Transform fail_path data to failure details format
            failure_details_df = df_fail_merge.select(
                col("carepro_AuthrequestId"),
                col("carepro_DocumentId"),
                col("Latest_Correlation_Id").alias("Correlation_Id"),
                col("Latest_Error_Code").alias("Error_Code"),
                col("Latest_Error_Message").alias("Error_Message"),
                col("Date_Created"),
                col("Date_Updated"),
                col("carepro_ARNumber"),
            )

            failure_detail_delta_table = DeltaTable.forPath(
                spark, DOCUMENT_FAILURE_DETAIL_TABLE_PATH
            )

            failure_detail_delta_table.alias("target").merge(
                failure_details_df.alias("source"),
                """target.carepro_AuthrequestId = source.carepro_AuthrequestId 
                   AND target.carepro_DocumentId = source.carepro_DocumentId
                   AND target.Correlation_Id = source.Correlation_Id""",
            ).whenNotMatchedInsert(
                values={
                    "carepro_AuthrequestId": source_auth_requestid,
                    "carepro_DocumentId": source_documentid,
                    "Correlation_Id": source_correlation_id,
                    "Error_Code": source_error_code,
                    "Error_Message": source_error_message,
                    "Date_Created": source_date_created,
                    "Date_Updated": source_date_updated,
                    "carepro_ARNumber": source_ar_number,
                }
            ).execute()

            logger.info(f"Successfully saved {failure_count} failure detail records")

    except Exception as e:
        logger.error(f"Error in merge_results: {e}")
        raise RuntimeError(f"Error in merge_results: {e}")


def _prepare_file_path(spark: SparkSession) -> DataFrame:
    """Prepare the final DataFrame for processing by constructing file paths and filtering records."""
    # Load input data and apply 
    delta_table = DeltaTable.forPath(spark, DOCUMENT_TABLE_PATH)
    doc_df = delta_table.toDF().filter(col("carepro_RootFolderPath").isNotNull())

    # Transform input DataFrame with path construction and column renaming
    doc_df = (
        doc_df.withColumn(
            "full_path",
            concat(
                col("carepro_RootFolderPath"),
                lit("\\"),
                col("carepro_ContainerFolders"),
                lit("\\"),
                col("carepro_DocumentBase_Annotation_carepro_FileName"),
            ),
        )
        .withColumnRenamed("carepro_DocumentBase_Annotation_Id", "carepro_DocumentId")
        .withColumnRenamed("carepro_AuthrequestBase_carepro_Name", "carepro_ARNumber")
        .select(
            "carepro_AuthrequestId",
            "carepro_DocumentId",
            "carepro_ARNumber",
            "full_path",
        )
    )

    # Load reference tables
    fail_df = spark.read.format("delta").load(DOCUMENT_FAILURE_TABLE_PATH)
    success_document_df = spark.read.format("delta").load(DOCUMENT_SUCCESS_TABLE_PATH)

    # Get new files (not in success and not in failure tables)
    new_files_df = (
        doc_df.join(
            success_document_df.select(
                "carepro_AuthrequestId", "carepro_DocumentId", "carepro_ARNumber"
            ).distinct(),
            on=["carepro_AuthrequestId", "carepro_DocumentId"],
            how="left_anti",
        )
        .join(
            fail_df.select(
                "carepro_AuthrequestId", "carepro_DocumentId", "carepro_ARNumber"
            ).distinct(),
            on=["carepro_AuthrequestId", "carepro_DocumentId"],
            how="left_anti",
        )
        .withColumn("retry_count", lit(0))
        .withColumn("reprocess_success", lit(0))
    )

    # Get failed files that need retry (retry_count <= 5 and reprocess_success = 0)
    retry_fail_df = fail_df.filter(
        (col("retry_count") <= 5) & (col("reprocess_success") == 0)
    ).select(
        "carepro_AuthrequestId",
        "carepro_DocumentId",
        "carepro_ARNumber",
        "full_path",
        "retry_count",
        "reprocess_success",
    )

    # Combine new files and retry files
    final_df = new_files_df.unionByName(retry_fail_df)

    return final_df


async def copy_document_to_blob(
    spark: SparkSession, correlation_id: str, azure_function_url: str
) -> None:
    """Process document records in batches, calling Azure Function and handling results."""

    records: List[Row] = _prepare_file_path(spark).collect()
    total = len(records)
    if total == 0:
        logger.info("No records to process.")
        return

    num_batches = (total + DOCUMENT_COPY_BATCH_SIZE - 1) // DOCUMENT_COPY_BATCH_SIZE
    logger.info(f"Processing {total} records in {num_batches} batch(es).")

    success_path: List[Dict[str, Any]] = []
    fail_path: List[Dict[str, Any]] = []

    async with ClientSession() as session:
        for batch_num in range(num_batches):
            start_idx = batch_num * DOCUMENT_COPY_BATCH_SIZE
            end_idx = min(start_idx + DOCUMENT_COPY_BATCH_SIZE, total)
            batch = records[start_idx:end_idx]

            tasks = [
                _process_http_call(session, row, correlation_id, azure_function_url)
                for row in batch
            ]
            results = await gather(*tasks)

            for result in results:
                if result["type"] == "success":
                    success_path.append(result["success_data"])
                else:
                    fail_path.append(result["fail_data"])

            logger.info(
                f"Batch {batch_num + 1}/{num_batches} completed. Success: {len(success_path)}, Failure: {len(fail_path)}"
            )
            _merge_results(spark, success_path, fail_path)
            success_path.clear()
            fail_path.clear()

            if batch_num < num_batches - 1:
                await sleep(10)
