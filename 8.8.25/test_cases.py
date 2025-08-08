import pytest
import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import documentcopy_helpers as mod


class DummyResponse:
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


# ---------- HAPPY PATHS ----------

@pytest.mark.asyncio
async def test_call_azure_function__happy_path(mocker):
    payload = {"full_path": "\\\\srv\\a\\x.pdf", "ar_number": "AR-1"}
    headers = {"Content-Type": "application/json"}

    mocker.patch.object(mod.Config, "AZURE_FUNCTION_URL", "https://example.com/fn")
    session = SimpleNamespace()
    session.post = AsyncMock(return_value=DummyResponse(200, {"azure_blob_path": "wasb://container/a/x.pdf"}))

    out = await mod._call_azure_function(session, payload, headers)

    assert out == {"azure_blob_path": "wasb://container/a/x.pdf"}
    session.post.assert_called_once_with("https://example.com/fn", json=payload, headers=headers)


@pytest.mark.asyncio
async def test_copy_document_to_blob__happy_path_batches_and_merge(mocker):
    mocker.patch.object(mod.Config, "BATCH_SIZE", 2)

    recs = [
        SimpleNamespace(asDict=lambda: {"full_path": "p1", "carepro_ARNumber": "AR", "carepro_AuthrequestId": "a1", "carepro_DocumentId": "d1"}),
        SimpleNamespace(asDict=lambda: {"full_path": "p2", "carepro_ARNumber": "AR", "carepro_AuthrequestId": "a2", "carepro_DocumentId": "d2"}),
        SimpleNamespace(asDict=lambda: {"full_path": "p3", "carepro_ARNumber": "AR", "carepro_AuthrequestId": "a3", "carepro_DocumentId": "d3"}),
    ]
    final_df = SimpleNamespace(collect=lambda: recs)

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

    mocker.patch.object(mod, "_process_http_call", side_effect=_ok)
    mocker.patch.object(mod, "_merge_results")
    mocker.patch.object(mod, "ClientSession", SimpleNamespace)
    mocker.patch.object(mod, "sleep", AsyncMock())  # avoid real sleeps

    spark = SimpleNamespace()
    await mod.copy_document_to_blob(spark, "CORR-OK", final_df)

    # 3 records with BATCH_SIZE=2 -> two merges: [2] then [1]
    assert mod._merge_results.call_count == 2
    first = mod._merge_results.call_args_list[0].args
    second = mod._merge_results.call_args_list[1].args
    assert len(first[1]) == 2 and len(first[2]) == 0
    assert len(second[1]) == 1 and len(second[2]) == 0


# ---------- NON-HAPPY PATHS ----------

@pytest.mark.asyncio
async def test_call_azure_function__http_500_raises(mocker):
    payload = {"full_path": "\\\\srv\\a\\x.pdf", "ar_number": "AR-1"}
    headers = {"Content-Type": "application/json"}

    mocker.patch.object(mod.Config, "AZURE_FUNCTION_URL", "https://example.com/fn")
    session = SimpleNamespace()
    session.post = AsyncMock(return_value=DummyResponse(500, text_data="boom"))

    with pytest.raises(Exception):
        await mod._call_azure_function(session, payload, headers)


@pytest.mark.asyncio
async def test_process_http_call__failure_record_emitted(mocker):
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
    assert fail["Latest_Error_Code"] in (400, 404, 429, 500, 502, 503, 504) or isinstance(fail["Latest_Error_Code"], int)
    assert "Latest_Error_Message" in fail and fail["Latest_Error_Message"]
