import pytest
import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import documentcopy_helpers as mod


class DummyResponse:
    """
    A minimal async-compatible HTTP response stub
    to simulate aiohttp.ClientResponse for tests.
    """
    def __init__(self, status=200, json_data=None, text_data=""):
        self.status = status
        self._json = json_data or {}
        self._text = text_data

    async def json(self):
        return self._json

    async def text(self):
        return self._text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


# -------------------------------------------------------------------
# HAPPY PATH 1: _call_azure_function returns a valid JSON payload
# -------------------------------------------------------------------
@pytest.mark.asyncio
async def test_call_azure_function__happy_path(mocker):
    """
    GIVEN a mock aiohttp session
    WHEN _call_azure_function is called with a valid payload
    THEN it should return the JSON response without raising.
    """
    payload = {"full_path": "\\\\srv\\a\\x.pdf", "ar_number": "AR-1"}
    headers = {"Content-Type": "application/json"}

    # Patch the config to point to a dummy URL
    mocker.patch.object(mod.Config, "AZURE_FUNCTION_URL", "https://example.com/fn")

    # Mock session.post to return a successful dummy response
    session = SimpleNamespace()
    session.post = AsyncMock(return_value=DummyResponse(200, {"azure_blob_path": "wasb://container/a/x.pdf"}))

    # Run the call
    out = await mod._call_azure_function(session, payload, headers)

    # Assert the JSON is returned as-is
    assert out == {"azure_blob_path": "wasb://container/a/x.pdf"}
    session.post.assert_called_once_with("https://example.com/fn", json=payload, headers=headers)


# -------------------------------------------------------------------
# HAPPY PATH 2: copy_document_to_blob handles batching & merging
# -------------------------------------------------------------------
@pytest.mark.asyncio
async def test_copy_document_to_blob__happy_path_batches_and_merge(mocker):
    """
    GIVEN a DataFrame-like object with 3 rows
    AND a BATCH_SIZE of 2
    WHEN copy_document_to_blob is called
    THEN it should process in 2 batches and call _merge_results twice.
    """
    mocker.patch.object(mod.Config, "BATCH_SIZE", 2)

    # Three dummy rows -> will require 2 batches
    recs = [
        SimpleNamespace(asDict=lambda: {"full_path": "p1", "carepro_ARNumber": "AR", "carepro_AuthrequestId": "a1", "carepro_DocumentId": "d1"}),
        SimpleNamespace(asDict=lambda: {"full_path": "p2", "carepro_ARNumber": "AR", "carepro_AuthrequestId": "a2", "carepro_DocumentId": "d2"}),
        SimpleNamespace(asDict=lambda: {"full_path": "p3", "carepro_ARNumber": "AR", "carepro_AuthrequestId": "a3", "carepro_DocumentId": "d3"}),
    ]
    final_df = SimpleNamespace(collect=lambda: recs)

    # Simulate every record as a success
    async def _ok(session, row, corr):
        d = row.asDict()
        return {
            "type": "success",
            "data": {
                "carepro_AuthrequestId": d["carepro_AuthrequestId"],
                "carepro_DocumentId": d["carepro_DocumentId"],
                "Correlation_Id": corr,
                "Full_Path": d["full_path"],
                "Azure_Blob_Path": "wasb://ok",
                "Execution_Time_ms": 1,
                "Date_Created": datetime.datetime.now(datetime.timezone.utc),
                "Date_Updated": datetime.datetime.now(datetime.timezone.utc),
                "carepro_ARNumber": d.get("carepro_ARNumber", ""),
            },
        }

    # Patch dependent functions/classes
    mocker.patch.object(mod, "_process_http_call", side_effect=_ok)
    mocker.patch.object(mod, "_merge_results")
    mocker.patch.object(mod, "ClientSession", SimpleNamespace)
    mocker.patch.object(mod, "sleep", AsyncMock())  # skip real sleeping

    spark = SimpleNamespace()
    await mod.copy_document_to_blob(spark, "CORR-OK", final_df)

    # Should have called merge twice (first batch 2 recs, second batch 1 rec)
    assert mod._merge_results.call_count == 2
    first = mod._merge_results.call_args_list[0].args
    second = mod._merge_results.call_args_list[1].args
    assert len(first[1]) == 2 and len(first[2]) == 0
    assert len(second[1]) == 1 and len(second[2]) == 0


# -------------------------------------------------------------------
# NON-HAPPY PATH 1: _call_azure_function HTTP 500
# -------------------------------------------------------------------
@pytest.mark.asyncio
async def test_call_azure_function__http_500_raises(mocker):
    """
    GIVEN a mock aiohttp session returning HTTP 500
    WHEN _call_azure_function is invoked
    THEN it should raise an exception.
    """
    payload = {"full_path": "\\\\srv\\a\\x.pdf", "ar_number": "AR-1"}
    headers = {"Content-Type": "application/json"}

    mocker.patch.object(mod.Config, "AZURE_FUNCTION_URL", "https://example.com/fn")
    session = SimpleNamespace()
    session.post = AsyncMock(return_value=DummyResponse(500, text_data="boom"))

    with pytest.raises(Exception):
        await mod._call_azure_function(session, payload, headers)


# -------------------------------------------------------------------
# NON-HAPPY PATH 2: _process_http_call produces a failure record
# -------------------------------------------------------------------
@pytest.mark.asyncio
async def test_process_http_call__failure_record_emitted(mocker):
    """
    GIVEN a record that causes _call_azure_function to raise
    WHEN _process_http_call is called
    THEN it should return a 'failure' type with fail_data populated.
    """
    row = SimpleNamespace(asDict=lambda: {
        "full_path": "\\\\share\\bad\\file.pdf",
        "carepro_ARNumber": "ARX",
        "carepro_AuthrequestId": "AUTH-X",
        "carepro_DocumentId": "DOC-X",
    })
    session = SimpleNamespace()
    mocker.patch.object(mod, "_call_azure_function", AsyncMock(side_effect=RuntimeError("downstream exploded")))

    out = await mod._process_http_call(session, row, "CORR-ERR")

    assert out["type"] == "failure"
    fail = out["fail_data"]
    assert fail["carepro_AuthrequestId"] == "AUTH-X"
    assert fail["carepro_DocumentId"] == "DOC-X"
    assert fail["Latest_Correlation_Id"] == "CORR-ERR"
    assert fail["Reprocess_Success"] == 0
    assert isinstance(fail["Latest_Error_Code"], int)
    assert fail["Latest_Error_Message"]
