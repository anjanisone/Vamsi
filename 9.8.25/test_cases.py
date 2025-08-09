import pytest
import datetime
from types import SimpleNamespace

import documentcopy_helpers as mod



class AsyncNoopContext:
    """A minimal stub to replace aiohttp.ClientSession for tests."""
    async def __aenter__(self): return self
    async def __aexit__(self, exc_type, exc, tb): return False


def make_df_stub(name="df"):
    """
    Very small chainable stub to stand-in for Spark DataFrame used only
    to validate call flow. Each method returns self.
    """
    class DFStub:
        def __init__(self): self._name = name
        def withColumn(self, *a, **k): return self
        def withColumnRenamed(self, *a, **k): return self
        def join(self, *a, **k): return self
        def select(self, *a, **k): return self
        def distinct(self, *a, **k): return self
        def toDF(self): return self
        def write(self): return self  # not used here
    return DFStub()



def test_prepare_file_path_calls_delta_and_joins_correctly(mocker):
    # Arrange: Spark and Delta stubs
    spark = mocker.Mock(name="spark")

    input_df = make_df_stub("input_df")
    fail_df = make_df_stub("fail_df")
    success_df = make_df_stub("success_df")
    pending_df = make_df_stub("pending_df")

    # spark.read.format("delta").load(path) -> return corresponding stub
    reader = mocker.Mock()
    spark.read.format.return_value = reader

    def _load_side_effect(path):
        if path == mod.PATHS["FAILURE_PATH"]:
            return fail_df
        if path == mod.PATHS["SUCCESS_PATH"]:
            return success_df
        raise AssertionError(f"Unexpected load path: {path}")

    reader.load.side_effect = _load_side_effect

    # DeltaTable.forPath(spark, INPUT).toDF() -> input_df
    delta_tbl = mocker.Mock()
    delta_tbl.toDF.return_value = input_df
    mocker.patch.object(mod, "DeltaTable", autospec=True)
    mod.DeltaTable.forPath.return_value = delta_tbl

    # We can't easily introspect chained return values on the stub; instead,
    # patch the join/select to return a sentinel pending_df at the end.
    # Simulate two anti-joins and final select.
    # We'll wrap methods to track they were called with expected args.
    join_calls = []

    def join_wrapper(self, *args, **kwargs):
        # capture 'how' to verify left_anti usage
        join_calls.append(kwargs.get("how"))
        return self

    def select_wrapper(self, *cols, **kw):
        return pending_df

    # Monkeypatch methods on the stub instance
    input_df.join = join_wrapper.__get__(input_df, type(input_df))
    input_df.select = select_wrapper.__get__(input_df, type(input_df))

    # Act
    out_df = mod.prepare_file_path(spark)

    # Assert: DeltaTable used with INPUT_PATH
    mod.DeltaTable.forPath.assert_called_once_with(spark, mod.PATHS["INPUT_PATH"])
    # FAILURE and SUCCESS were loaded
    assert reader.load.call_count == 2
    assert mod.PATHS["FAILURE_PATH"] in [c.args[0] for c in reader.load.call_args_list]
    assert mod.PATHS["SUCCESS_PATH"] in [c.args[0] for c in reader.load.call_args_list]
    # Two left_anti joins expected
    assert join_calls.count("left_anti") == 2
    # Returned object should be the result of final select
    assert out_df is pending_df


@pytest.mark.asyncio
async def test_copy_document_to_blob_batches_and_merges(mocker):
    # Arrange: 3 records, BATCH_SIZE = 2 => 2 batches
    mocker.patch.object(mod.Config, "BATCH_SIZE", 2)

    rows = [
        SimpleNamespace(asDict=lambda: {
            "full_path": "p1",
            "carepro_ARNumber": "AR",
            "carepro_AuthrequestId": "a1",
            "carepro_DocumentId": "d1",
        }),
        SimpleNamespace(asDict=lambda: {
            "full_path": "p2",
            "carepro_ARNumber": "AR",
            "carepro_AuthrequestId": "a2",
            "carepro_DocumentId": "d2",
        }),
        SimpleNamespace(asDict=lambda: {
            "full_path": "p3",
            "carepro_ARNumber": "AR",
            "carepro_AuthrequestId": "a3",
            "carepro_DocumentId": "d3",
        }),
    ]

    final_df = SimpleNamespace(collect=lambda: rows)

    # _process_http_call returns success for first two, 404 failure for third
    async def _proc(_, row, corr):
        d = row.asDict()
        if d["carepro_AuthrequestId"] in {"a1", "a2"}:
            return {
                "type": "success",
                "success_data": {
                    "carepro_AuthrequestId": d["carepro_AuthrequestId"],
                    "carepro_DocumentId": d["carepro_DocumentId"],
                    "Correlation_Id": corr,
                    "Full_Path": d["full_path"],
                    "Azure_Blob_Path": "wasb://ok",
                    "Execution_Time_ms": 1,
                    "Date_Created": datetime.datetime.now(datetime.timezone.utc),
                    "Date_Updated": datetime.datetime.now(datetime.timezone.utc),
                    "carepro_ARNumber": d["carepro_ARNumber"],
                },
            }
        return {
            "type": "failure",
            "fail_data": {
                "carepro_AuthrequestId": d["carepro_AuthrequestId"],
                "carepro_DocumentId": d["carepro_DocumentId"],
                "Latest_Correlation_Id": corr,
                "Latest_Error_Code": 404,
                "Latest_Error_Message": "File not found at SFTP path",
                "Full_Path": d["full_path"],
                "Reprocess_Success": 0,
                "Date_Created": datetime.datetime.now(datetime.timezone.utc),
                "Date_Updated": datetime.datetime.now(datetime.timezone.utc),
                "carepro_ARNumber": d["carepro_ARNumber"],
            },
        }

    patch_proc = mocker.patch.object(mod, "_process_http_call", side_effect=_proc)
    patch_merge = mocker.patch.object(mod, "_merge_results")
    # Avoid real HTTP client and sleeps
    mocker.patch.object(mod, "ClientSession", AsyncNoopContext)
    mocker.patch.object(mod, "sleep", mocker.AsyncMock())

    spark = SimpleNamespace()
    # Act
    await mod.copy_document_to_blob(spark, "CORR-XYZ", final_df)

    # Assert: two merges (first with 2 successes, second with 1 failure)
    assert patch_merge.call_count == 2
    first_args = patch_merge.call_args_list[0].args
    second_args = patch_merge.call_args_list[1].args

    assert len(first_args[1]) == 2 and len(first_args[2]) == 0
    assert len(second_args[1]) == 0 and len(second_args[2]) == 1

    # Ensure our patched processor was called 3 times
    assert patch_proc.call_count == 3


@pytest.mark.asyncio
async def test_copy_document_to_blob_no_records_returns_early(mocker):
    mocker.patch.object(mod.Config, "BATCH_SIZE", 2)
    final_df = SimpleNamespace(collect=lambda: [])
    patch_merge = mocker.patch.object(mod, "_merge_results")
    mocker.patch.object(mod, "ClientSession", AsyncNoopContext)

    spark = SimpleNamespace()
    await mod.copy_document_to_blob(spark, "CORR-EMPTY", final_df)

    # Should not call merge at all
    patch_merge.assert_not_called()


@pytest.mark.asyncio
async def test_copy_document_to_blob_sleeps_between_batches(mocker):
    mocker.patch.object(mod.Config, "BATCH_SIZE", 1)  # force 3 batches for 3 rows

    rows = [
        SimpleNamespace(asDict=lambda: {
            "full_path": f"p{i}",
            "carepro_ARNumber": "AR",
            "carepro_AuthrequestId": f"a{i}",
            "carepro_DocumentId": f"d{i}",
        }) for i in range(1, 4)
    ]
    final_df = SimpleNamespace(collect=lambda: rows)

    async def _ok(_, row, corr):
        d = row.asDict()
        return {
            "type": "success",
            "success_data": {
                "carepro_AuthrequestId": d["carepro_AuthrequestId"],
                "carepro_DocumentId": d["carepro_DocumentId"],
                "Correlation_Id": corr,
                "Full_Path": d["full_path"],
                "Azure_Blob_Path": "wasb://ok",
                "Execution_Time_ms": 1,
                "Date_Created": datetime.datetime.now(datetime.timezone.utc),
                "Date_Updated": datetime.datetime.now(datetime.timezone.utc),
                "carepro_ARNumber": d["carepro_ARNumber"],
            },
        }

    mocker.patch.object(mod, "_process_http_call", side_effect=_ok)
    mocker.patch.object(mod, "_merge_results")
    mocker.patch.object(mod, "ClientSession", AsyncNoopContext)
    patched_sleep = mocker.patch.object(mod, "sleep", mocker.AsyncMock())

    spark = SimpleNamespace()
    await mod.copy_document_to_blob(spark, "CORR-SLEEP", final_df)

    # With 3 batches, sleep should be called twice (between batch1->2 and batch2->3)
    assert patched_sleep.await_count == 2
