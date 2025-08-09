from datetime import datetime, timezone
from aiohttp import ClientSession, ClientResponseError, ClientConnectionError, ServerTimeoutError
from asyncio import sleep, gather
from pyspark.sql.functions import col, lit, concat
from delta.tables import DeltaTable
from typing import List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception,
    RetryError,
)


class Config:
    # Update to your deployed HTTP trigger URL
    AZURE_FUNCTION_URL = "https://<your-func-app>.azurewebsites.net/api/sftp-to-blob-copy-file"
    BATCH_SIZE = 50
    MAX_RETRIES = 3
    RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

PATHS = {
    "INPUT_PATH": "abfss://<container>@<account>.dfs.core.windows.net/source/input_table",
    "SUCCESS_PATH": "abfss://<container>@<account>.dfs.core.windows.net/audit/success_table",
    "FAILURE_PATH": "abfss://<container>@<account>.dfs.core.windows.net/audit/failure_table",
    "FAILURE_REASON_PATH": "abfss://<container>@<account>.dfs.core.windows.net/audit/failure_reason_table",
}

# ---------------------------
# Retry predicate
# ---------------------------

def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, (ClientConnectionError, ServerTimeoutError)):
        return True
    if isinstance(exc, ClientResponseError):
        # retry on transient HTTP codes only
        return exc.status in Config.RETRYABLE_STATUS_CODES
    return False




@retry(
    stop=stop_after_attempt(Config.MAX_RETRIES),
    wait=wait_random_exponential(multiplier=0.5, max=8),
    retry=retry_if_exception(_is_retryable),
    reraise=True,
)
async def _call_azure_function(session: ClientSession, payload: dict, headers: dict) -> dict:
    """
    Calls the Azure Function. Behavior:
      - 200..299: returns parsed JSON dict
      - 404: raises ClientResponseError(status=404) so caller can treat as non-retryable
      - other non-2xx: raises ClientResponseError with exact status for retry policy
    """
    async with session.post(Config.AZURE_FUNCTION_URL, json=payload, headers=headers) as resp:
        status = resp.status
        # Try JSON, fallback to text message
        try:
            data = await resp.json(content_type=None)
        except Exception:
            data = {"message": (await resp.text())}

        if 200 <= status < 300:
            return data if isinstance(data, dict) else {"data": data}

        # Ensure message
        msg = ""
        if isinstance(data, dict):
            msg = data.get("message") or data.get("error") or data.get("detail") or ""
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




async def _process_http_call(session: ClientSession, row: Row, correlation_id: str) -> Dict[str, Any]:
    """
    For a single input row, call the HTTP endpoint and return:
      - {"type": "success", "success_data": {...}} OR
      - {"type": "failure", "fail_data": {...}}
    """
    rec = row.asDict()
    file_path = rec.get("full_path")
    ar_number = rec.get("carepro_ARNumber") or rec.get("carepro_ARNumber".upper()) or ""
    auth_id = rec.get("carepro_AuthrequestId") or rec.get("carepro_AuthrequestId".upper()) or ""
    doc_id = rec.get("carepro_DocumentId") or rec.get("carepro_DocumentId".upper()) or ""

    payload = {
        "file_path": file_path,
        "ar_number": ar_number,
        "mime_type": "application/json",
    }
    headers = {"Content-Type": "application/json"}

    start = datetime.now(timezone.utc)
    try:
        out = await _call_azure_function(session, payload, headers)
        end = datetime.now(timezone.utc)

        # On success, Azure function typically returns: azure_blob_path + message
        success_row = {
            "carepro_AuthrequestId": auth_id,
            "carepro_DocumentId": doc_id,
            "Correlation_Id": correlation_id,
            "Full_Path": file_path,
            "Azure_Blob_Path": out.get("azure_blob_path", ""),
            "Execution_Time_ms": max(1, int((end - start).total_seconds() * 1000)),
            "Date_Created": start,
            "Date_Updated": end,
            "carepro_ARNumber": ar_number,
        }
        return {"type": "success", "success_data": success_row}

    except ClientResponseError as cre:
        # Map HTTP error (e.g., 404 file not found) to failure record with exact code & message
        end = datetime.now(timezone.utc)
        fail_row = {
            "carepro_AuthrequestId": auth_id,
            "carepro_DocumentId": doc_id,
            "Latest_Correlation_Id": correlation_id,
            "Latest_Error_Code": cre.status,
            "Latest_Error_Message": cre.message or "HTTP error",
            "Full_Path": file_path,
            "Reprocess_Success": 0,
            "Date_Created": start,
            "Date_Updated": end,
            "carepro_ARNumber": ar_number,
        }
        return {"type": "failure", "fail_data": fail_row}

    except (ClientConnectionError, ServerTimeoutError) as e:
        end = datetime.now(timezone.utc)
        fail_row = {
            "carepro_AuthrequestId": auth_id,
            "carepro_DocumentId": doc_id,
            "Latest_Correlation_Id": correlation_id,
            "Latest_Error_Code": 503,
            "Latest_Error_Message": str(e),
            "Full_Path": file_path,
            "Reprocess_Success": 0,
            "Date_Created": start,
            "Date_Updated": end,
            "carepro_ARNumber": ar_number,
        }
        return {"type": "failure", "fail_data": fail_row}

    except Exception as e:
        end = datetime.now(timezone.utc)
        fail_row = {
            "carepro_AuthrequestId": auth_id,
            "carepro_DocumentId": doc_id,
            "Latest_Correlation_Id": correlation_id,
            "Latest_Error_Code": 500,
            "Latest_Error_Message": str(e),
            "Full_Path": file_path,
            "Reprocess_Success": 0,
            "Date_Created": start,
            "Date_Updated": end,
            "carepro_ARNumber": ar_number,
        }
        return {"type": "failure", "fail_data": fail_row}



# Merge results into Delta


def _merge_results(spark: SparkSession, success_path: List[Dict[str, Any]], fail_path: List[Dict[str, Any]]) -> None:
    """
    Writes success records to SUCCESS_PATH and failure to FAILURE_PATH & FAILURE_REASON_PATH.
    Uses MERGE for idempotent updates keyed by (carepro_AuthrequestId, carepro_DocumentId).
    """
    # Early exit
    if not success_path and not fail_path:
        return

    # Convert to DataFrames
    if success_path:
        success_df = spark.createDataFrame(success_path)
        success_delta = DeltaTable.forPath(spark, PATHS["SUCCESS_PATH"])
        s_alias = success_delta.alias("t")
        s_src = success_df.alias("s")
        (
            s_alias.merge(
                s_src,
                "t.carepro_AuthrequestId = s.carepro_AuthrequestId AND t.carepro_DocumentId = s.carepro_DocumentId",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    if fail_path:
        fail_df = spark.createDataFrame(fail_path)
        failure_delta = DeltaTable.forPath(spark, PATHS["FAILURE_PATH"])
        f_alias = failure_delta.alias("t")
        f_src = fail_df.alias("f")
        (
            f_alias.merge(
                f_src,
                "t.carepro_AuthrequestId = f.carepro_AuthrequestId AND t.carepro_DocumentId = f.carepro_DocumentId",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        # Optional: persist detailed reasons separately if needed
        fail_df.select(
            "carepro_AuthrequestId",
            "carepro_DocumentId",
            "Latest_Correlation_Id",
            "Latest_Error_Code",
            "Latest_Error_Message",
            "Full_Path",
            "Date_Created",
            "Date_Updated",
            "carepro_ARNumber",
        ).write.format("delta").mode("append").save(PATHS["FAILURE_REASON_PATH"])


# Prepare input DataFrame


def prepare_file_path(spark: SparkSession) -> DataFrame:
    """
    Reads the input table, removes already-processed and permanently failed records,
    and constructs a final DataFrame with a 'full_path' column ready for processing.
    """
    input_delta = DeltaTable.forPath(spark, PATHS["INPUT_PATH"]).toDF()
    fail_df = spark.read.format("delta").load(PATHS["FAILURE_PATH"])
    success_df = spark.read.format("delta").load(PATHS["SUCCESS_PATH"])

    # Build full path (adjust column names as per your schema)
    input_df = input_delta.withColumn(
        "full_path",
        concat(
            col("carepro_RootFolderPath"),
            lit("\\\\"),
            col("carepro_ContainerFolders"),
            lit("\\\\"),
            col("carepro_DocumentBase_Annotation_carepro_FileName"),
        ),
    ).withColumnRenamed("carepro_AuthrequestId", "carepro_AuthrequestId"
    ).withColumnRenamed("carepro_DocumentBase_Annotation_Id", "carepro_DocumentId")

    # Exclude those already succeeded or permanently failed (if you have a flag)
    # Left anti-joins by (AuthrequestId, DocumentId)
    not_failed = input_df.join(
        fail_df.select("carepro_AuthrequestId", "carepro_DocumentId").distinct(),
        on=["carepro_AuthrequestId", "carepro_DocumentId"],
        how="left_anti",
    )
    pending = not_failed.join(
        success_df.select("carepro_AuthrequestId", "carepro_DocumentId").distinct(),
        on=["carepro_AuthrequestId", "carepro_DocumentId"],
        how="left_anti",
    )

    return pending.select(
        "carepro_AuthrequestId",
        "carepro_DocumentId",
        "carepro_ARNumber",
        "full_path",
    )


# ---------------------------
# Orchestrate batches
# ---------------------------

async def copy_document_to_blob(spark: SparkSession, correlation_id: str, final_df: DataFrame) -> None:
    """
    Drives the processing in batches:
      - Collects rows
      - Calls Azure Function concurrently within a batch
      - Merges results after each batch
      - Sleeps between batches to be gentle on downstream
    """
    records: List[Row] = final_df.collect()
    total = len(records)
    if total == 0:
        print("No records to process.")
        return

    num_batches = (total + Config.BATCH_SIZE - 1) // Config.BATCH_SIZE
    print(f"Processing {total} records in {num_batches} batch(es).")

    success_path: List[Dict[str, Any]] = []
    fail_path: List[Dict[str, Any]] = []

    headers = {"Content-Type": "application/json"}

    async with ClientSession() as session:
        for batch_num in range(num_batches):
            start_idx = batch_num * Config.BATCH_SIZE
            end_idx = min(start_idx + Config.BATCH_SIZE, total)
            batch = records[start_idx:end_idx]

            tasks = [ _process_http_call(session, row, correlation_id) for row in batch ]
            results = await gather(*tasks)

            for result in results:
                if result["type"] == "success":
                    success_path.append(result["success_data"])
                else:
                    fail_path.append(result["fail_data"])

            print(f"Batch {batch_num + 1}/{num_batches} completed. Success: {len(success_path)}, Failure: {len(fail_path)}")
            _merge_results(spark, success_path, fail_path)
            success_path.clear()
            fail_path.clear()

            if batch_num < num_batches - 1:
                await sleep(10)
