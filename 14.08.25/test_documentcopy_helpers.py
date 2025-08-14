import pytest
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

##### BOOTSTRAP #####
root_directory = (
    Path(__file__).parent.parent.parent.resolve().joinpath("fabric-workspace", "jobs")
)
job_path = root_directory.joinpath(
    f"{Path(__file__).parent.resolve().name}.SparkJobDefinition"
)

sys.path.append(job_path.joinpath("Libs").as_posix())
sys.path.append(job_path.joinpath("Main").as_posix())
##### BOOTSTRAP #####

from documentcopy_helpers import copy_document_to_blob, _merge_results


# =============================================================================
# TEST DATA
# =============================================================================

MOCK_DOCUMENT_DATA = [
    {
        "carepro_AuthrequestId": "AUTH-001",
        "carepro_DocumentId": "DOC-001",
        "carepro_ARNumber": "AR12345",
        "full_path": "\\\\server\\path\\document1.pdf",
        "retry_count": 0,
        "reprocess_success": 0,
    },
    {
        "carepro_AuthrequestId": "AUTH-002",
        "carepro_DocumentId": "DOC-002",
        "carepro_ARNumber": "AR67890",
        "full_path": "\\\\server\\path\\document2.pdf",
        "retry_count": 1,
        "reprocess_success": 0,
    },
]

MOCK_AZURE_FUNCTION_SUCCESS_RESPONSE = {
    "azure_blob_path": "https://storage.blob.core.windows.net/healthcare/AR12345/document1.pdf",
    "message": "Document copied successfully",
}

MOCK_AZURE_FUNCTION_ERROR_RESPONSE = {
    "error": "File not found",
    "message": "The specified document could not be located",
}


# =============================================================================
# SHARED FIXTURES
# =============================================================================


@pytest.fixture
def mock_spark_session(mocker):
    """Create a unified mock SparkSession that works for both Azure Functions and Delta table operations."""
    # Use MagicMock directly for better flexibility
    spark = MagicMock()

    # Mock DataFrame creation and operations for _merge_results
    mock_df = MagicMock()
    mock_df.alias.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.collect.return_value = [
        mocker.MagicMock(asDict=lambda: MOCK_DOCUMENT_DATA[0]),
        mocker.MagicMock(asDict=lambda: MOCK_DOCUMENT_DATA[1]),
    ]
    spark.createDataFrame.return_value = mock_df

    # Mock Delta table operations for _merge_results
    mock_delta_table = MagicMock()
    mock_merge = MagicMock()
    mock_merge.whenNotMatchedInsert.return_value = mock_merge
    mock_merge.whenMatchedUpdate.return_value = mock_merge
    mock_merge.execute.return_value = None
    mock_delta_table.alias.return_value.merge.return_value = mock_merge

    # Apply patches for PySpark functions used in _merge_results
    with (
        patch("documentcopy_helpers.DeltaTable.forPath", return_value=mock_delta_table),
        patch("documentcopy_helpers.col", return_value=MagicMock()),
        patch("documentcopy_helpers.lit", return_value=MagicMock()),
    ):
        yield spark


@pytest.fixture
def mock_prepare_file_path(mocker):
    """Mock the _prepare_file_path function to return test data."""
    mock_df = mocker.MagicMock()
    mock_df.collect.return_value = [
        mocker.MagicMock(asDict=lambda: MOCK_DOCUMENT_DATA[0]),
        mocker.MagicMock(asDict=lambda: MOCK_DOCUMENT_DATA[1]),
    ]
    return mocker.patch("documentcopy_helpers._prepare_file_path", return_value=mock_df)


@pytest.fixture
def mock_merge_results(mocker):
    """Mock the _merge_results function."""
    return mocker.patch("documentcopy_helpers._merge_results")


# =============================================================================
# AZURE FUNCTIONS INTEGRATION TESTS
# =============================================================================


@pytest.mark.asyncio
async def test_copy_document_to_blob_success(
    mock_spark_session, mock_prepare_file_path, mock_merge_results, mocker
):
    """Test successful Azure Functions document copying workflow."""

    # Mock Azure Function HTTP responses
    mock_session = AsyncMock()
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json.return_value = MOCK_AZURE_FUNCTION_SUCCESS_RESPONSE
    mock_response.headers = {}

    mock_session.post.return_value.__aenter__.return_value = mock_response

    # Mock ClientSession context manager
    mock_client_session = mocker.patch("documentcopy_helpers.ClientSession")
    mock_client_session.return_value.__aenter__.return_value = mock_session

    # Execute Azure Functions integration
    await copy_document_to_blob(
        mock_spark_session,
        "CORR-TEST-001",
        "https://azure-function-url.com/api/copy-document",
    )

    # Verify Azure Functions interactions
    assert mock_prepare_file_path.called
    assert mock_session.post.call_count == 2
    assert mock_merge_results.call_count == 1

    # Verify Azure Function payload structure
    call_args = mock_session.post.call_args_list[0]
    payload = call_args[1]["json"]
    assert "file_path" in payload
    assert "ar_number" in payload
    assert payload["file_path"] == MOCK_DOCUMENT_DATA[0]["full_path"]


@pytest.mark.asyncio
async def test_copy_document_to_blob_all_failures(
    mock_spark_session, mock_prepare_file_path, mock_merge_results, mocker
):
    """Test Azure Functions document copying with all failures."""

    # Create two 404 responses that turn into ClientResponseError
    def mk_404(msg: str):
        r = AsyncMock()
        r.status = 404
        r.headers = {}
        # make awaitable
        r.json = AsyncMock(return_value={"message": msg})
        r.text = AsyncMock(return_value=msg)
        r.request_info = SimpleNamespace(
            method="POST",
            real_url="https://azure-function-url.com/api/copy-document",
        )
        r.history = ()
        return r

    resp_404_a = mk_404("File not found A")
    resp_404_b = mk_404("File not found B")

    # Mock ClientSession
    mock_session = AsyncMock()
    # each "async with session.post as resp" yields a different 404 response
    mock_session.post.return_value.__aenter__.side_effect = [resp_404_a, resp_404_b]

    mock_client_session = mocker.patch("documentcopy_helpers.ClientSession")
    mock_client_session.return_value.__aenter__.return_value = mock_session

    await copy_document_to_blob(
        mock_spark_session,
        "CORR-TEST-FAIL-ALL",
        "https://azure-function-url.com/api/copy-document",
    )

    assert mock_prepare_file_path.called
    assert mock_session.post.call_count == 2
    assert mock_merge_results.call_count == 1
    args, _ = mock_merge_results.call_args
    # args[0] is spark; [1] success_list; [2] failure_list
    _, success_list, failure_list = args[:3]

    assert len(success_list) == 0
    for fail in failure_list:
        assert fail["Latest_Error_Code"] == 404
        assert "File not found" in fail["Latest_Error_Message"]


@pytest.mark.asyncio
async def test_copy_document_to_blob_empty_dataset(
    mock_spark_session, mock_merge_results, mocker
):
    """Test Azure Functions behavior with empty document dataset."""

    # Mock empty dataset
    mock_df = mocker.MagicMock()
    mock_df.collect.return_value = []
    mock_prepare_file_path = mocker.patch(
        "documentcopy_helpers._prepare_file_path", return_value=mock_df
    )

    await copy_document_to_blob(
        mock_spark_session,
        "CORR-TEST-006",
        "https://azure-function-url.com/api/copy-document",
    )

    assert mock_prepare_file_path.called
    assert not mock_merge_results.called




def test_merge_results_success_only(mock_spark_session):
    """Test _merge_results with only successful records - covers success path."""
    success_data = [
        {
            "carepro_AuthrequestId": "AUTH-001",
            "carepro_DocumentId": "DOC-001",
            "Correlation_Id": "CORR-001",
            "Full_Path": "\\\\server\\document1.pdf",
            "Azure_Blob_Path": "https://storage.blob.core.windows.net/docs/doc1.pdf",
            "Execution_Time_ms": 1500,
            "Date_Created": "2025-08-11T10:00:00Z",
            "Date_Updated": "2025-08-11T10:00:00Z",
            "carepro_ARNumber": "AR123",
        }
    ]

    fail_data = []

    # This should cover lines 177-207 (success record processing)
    _merge_results(mock_spark_session, success_data, fail_data)

    # Verify DataFrame creation was called with success data
    mock_spark_session.createDataFrame.assert_called_with(success_data)


def test_merge_results_empty_data(mock_spark_session):
    """Test _merge_results with empty data - covers early return path."""
    success_data = []
    fail_data = []

    # This should cover the early return (lines 140-141)
    _merge_results(mock_spark_session, success_data, fail_data)

    # Verify no DataFrame operations were called
    mock_spark_session.createDataFrame.assert_not_called()


def test_merge_results_failure_only(mock_spark_session):
    """Test _merge_results with only failure records - covers failure path."""
    success_data = []

    fail_data = [
        {
            "carepro_AuthrequestId": "AUTH-002",
            "carepro_DocumentId": "DOC-002",
            "Latest_Correlation_Id": "CORR-002",
            "Latest_Error_Code": 404,
            "Latest_Error_Message": "File not found",
            "Full_Path": "\\\\server\\document2.pdf",
            "Reprocess_Success": 0,
            "Date_Created": "2025-08-11T10:00:00Z",
            "Date_Updated": "2025-08-11T10:00:00Z",
            "carepro_ARNumber": "AR456",
        }
    ]

    _merge_results(mock_spark_session, success_data, fail_data)

    # Verify DataFrame creation was called with failure data
    mock_spark_session.createDataFrame.assert_called_with(fail_data)


def test_merge_results_mixed_records(mock_spark_session):
    """Test _merge_results with both success and failure records - covers both paths."""
    success_data = [
        {
            "carepro_AuthrequestId": "AUTH-003",
            "carepro_DocumentId": "DOC-003",
            "Correlation_Id": "CORR-003",
            "Full_Path": "\\\\server\\document3.pdf",
            "Azure_Blob_Path": "https://storage.blob.core.windows.net/docs/doc3.pdf",
            "Execution_Time_ms": 2000,
            "Date_Created": "2025-08-11T11:00:00Z",
            "Date_Updated": "2025-08-11T11:00:00Z",
            "carepro_ARNumber": "AR789",
        }
    ]

    fail_data = [
        {
            "carepro_AuthrequestId": "AUTH-004",
            "carepro_DocumentId": "DOC-004",
            "Latest_Correlation_Id": "CORR-004",
            "Latest_Error_Code": 503,
            "Latest_Error_Message": "Service unavailable",
            "Full_Path": "\\\\server\\document4.pdf",
            "Reprocess_Success": 0,
            "Date_Created": "2025-08-11T11:00:00Z",
            "Date_Updated": "2025-08-11T11:00:00Z",
            "carepro_ARNumber": "AR101",
        }
    ]

    _merge_results(mock_spark_session, success_data, fail_data)

    # Verify DataFrame creation was called for both success and failure data
    assert mock_spark_session.createDataFrame.call_count >= 2
