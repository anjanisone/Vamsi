from bootstrap import install_packages #noqa: F401
import logging
import asyncio
from pyspark.sql import SparkSession
from uuid import uuid4 
from fabric_helpers import ( 
    get_secret,
    send_event,
    build_event_data,
    create_table_if_not_exists,
)

from get_auth_info import import_auth_data_from_carepro
from authorization_helpers import submit_auth_to_fhir
from documentcopy_helpers import copy_document_to_blob
from argparse import ArgumentParser, BooleanOptionalAction, Namespace
from typing import Dict


from constants import (
    APP_NAME,
    KEY_VAULT_NAME,
    EVENT_HUB_NAME,
    EVENT_HUB_SECRET_NAME,
    FHIR_BASE_URL,
    FHIR_SUBSCRIPTION_KEY,
    AUTHORIZATION_SCHEMA,
    AUTHORIZATION_FHIR_TABLE,
    DOCUMENT_FHIR_TABLE,
    NOTE_FHIR_TABLE,
    MODIFICATIONHISTORY_FHIR_TABLE,
    CLAIMRESPONSE_FHIR_TABLE,
    AUTHORIZATION_FHIR_SCHEMA,
    DOCUMENT_FHIR_SCHEMA,
    NOTE_FHIR_SCHEMA,
    MODIFICATIONHISTORY_FHIR_SCHEMA,
    CLAIMRESPONSE_FHIR_SCHEMA,
    EVENT_TYPE_AUTH_LOAD_COMPLETED,
    EVENT_CONTEXT,
    FHIR_DOCUMENT_COPY_URI,
    DOCUMENT_SUCCESS_SCHEMA,
    DOCUMENT_SUCCESS_TABLE,
    DESTINATION_SCHEMA_NAME,
)



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_parser() -> ArgumentParser:
    """Configure and return the argument parser."""
    parser = ArgumentParser()
    parser.add_argument("--correlation-id", help="A correlation id")
    parser.add_argument(
        "--disable_documentcopy_process",
        action=BooleanOptionalAction,
        help="Disable document copy via Azure Function",
    )
    parser.add_argument(
        "--disable_events",
        action=BooleanOptionalAction,
        help="Disable sending events to Event Hub",
    )
    parser.add_argument(
        "--disable_fhir_submission",
        action=BooleanOptionalAction,
        help="Disable sending events to FHIR",
    )
    parser.add_argument(
        "--copy_pipeline_name",
        help="The name of the pipeline to execute to import data into the lakehouse",
        default="pl_import_from_carepro",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        help="The batch size used for importing data from CarePro",
        default=500,
    )
    return parser

def configure_spark() -> SparkSession:
    """Create and configure the Spark session."""
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    # Increase max message size for large DataFrame transfers
    spark.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs", False)
    spark.conf.set("spark.rpc.message.maxSize", "1024")
    return spark

def setup_cross_reference_tables(spark: SparkSession) -> None:
    """Ensure cross reference tables exist in the lakehouse."""
    create_table_if_not_exists(
        spark, AUTHORIZATION_SCHEMA, AUTHORIZATION_FHIR_TABLE,
        AUTHORIZATION_FHIR_SCHEMA, False,
    )
    create_table_if_not_exists(
        spark, AUTHORIZATION_SCHEMA, DOCUMENT_FHIR_TABLE,
        DOCUMENT_FHIR_SCHEMA, False,
    )
    create_table_if_not_exists(
        spark, AUTHORIZATION_SCHEMA, NOTE_FHIR_TABLE,
        NOTE_FHIR_SCHEMA, False,
    )
    create_table_if_not_exists(
        spark, AUTHORIZATION_SCHEMA, MODIFICATIONHISTORY_FHIR_TABLE,
        MODIFICATIONHISTORY_FHIR_SCHEMA, False,
    )
    create_table_if_not_exists(
        spark, AUTHORIZATION_SCHEMA, CLAIMRESPONSE_FHIR_TABLE,
        CLAIMRESPONSE_FHIR_SCHEMA, False,
    )
    create_table_if_not_exists(
        spark,
        DESTINATION_SCHEMA_NAME,
        DOCUMENT_SUCCESS_TABLE,
        DOCUMENT_SUCCESS_SCHEMA,
        False,
    )

def main() -> None:
    parser = get_parser()
    args: Namespace = parser.parse_args()

    spark: SparkSession = configure_spark()

    # Retrieve secrets and set up headers
    request_url: str = get_secret(KEY_VAULT_NAME, FHIR_BASE_URL)
    subscription_key: str = get_secret(KEY_VAULT_NAME, FHIR_SUBSCRIPTION_KEY)
    azure_function_url: str = get_secret(KEY_VAULT_NAME, FHIR_DOCUMENT_COPY_URI)
    headers: Dict[str, str] = {
        "Content-Type": "application/json",
        "Ocp-Apim-Subscription-Key": subscription_key,
        "x-ms-profile-validation": "true",
    }
    correlation_id: str = args.correlation_id or str(uuid4())

    # Import data from CarePro
    import_auth_data_from_carepro(spark, args.copy_pipeline_name, args.batch_size)

    # Ensure cross reference tables exist
    setup_cross_reference_tables(spark)

    # Conditional document copying based on command line argument
    if not args.disable_documentcopy_process:
        asyncio.run(copy_document_to_blob(spark, correlation_id, azure_function_url))
        logger.info("Document copying completed successfully.")
    else:
        logger.warning(
            "Document copying DISABLED (--disable_documentcopy_process is specified)"
        )

    # Submit authorization data to FHIR
    asyncio.run(
        submit_auth_to_fhir(
            spark, request_url, headers, args.disable_fhir_submission, correlation_id
        )
    )

    # Build and send event
    event_type: str = EVENT_TYPE_AUTH_LOAD_COMPLETED
    event_data: dict = build_event_data(
        correlation_id, event_type, APP_NAME, EVENT_CONTEXT
    )
    event_hub_connection_string: str = get_secret(
        KEY_VAULT_NAME, EVENT_HUB_SECRET_NAME
        )

    logger.info(f"Sending event: {event_data}")
    if not args.disable_events:
        send_event(EVENT_HUB_NAME, event_hub_connection_string, event_data)
        logger.info(f"Sent event '{event_type}'")
    else:
        logger.warning("Events disabled. No event sent")


if __name__ == "__main__":
    main()