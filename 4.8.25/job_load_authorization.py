import os
import sys
import subprocess
import asyncio
import argparse


parser = argparse.ArgumentParser()
parser.add_argument(
    "--run_copy_files",
    action="store_true",
    help="Flag to trigger file copy via Azure Function"
)


args = parser.parse_args()


def install_packages() -> None:
    """Install required Python packages at runtime."""
    args = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "azure-eventhub",
        "aiohttp",
        "--ignore-installed",
    ]
    install_process = subprocess.run(args, check=True, capture_output=True, text=True)
    print(install_process.stdout)


install_packages()

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
from argparse import ArgumentParser, BooleanOptionalAction, Namespace
from typing import Dict

# Constants and environment variables

CONFIG = {
    "APP_NAME": os.getenv("APP_NAME", "load_authorization_data"),
    "KEY_VAULT_NAME": os.getenv("KEY_VAULT_NAME", "dv-kv-fabric-01"),
    "EVENT_HUB_NAME": os.getenv("EVENT_HUB_NAME", "fabric01"),
    "EVENT_HUB_SECRET": os.getenv("EVENT_HUB_SECRET", "eventhubnamespaceconnection"),
}


def install_packages() -> None:
    """Install required Python packages at runtime."""
    args = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "azure-eventhub",
        "aiohttp",
        "--ignore-installed",
    ]
    install_process = subprocess.run(args, check=True, capture_output=True, text=True)
    print(install_process.stdout)


def get_parser() -> ArgumentParser:
    """Configure and return the argument parser."""
    parser = ArgumentParser()
    parser.add_argument("--correlation-id", help="A correlation id")
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
    spark = SparkSession.builder.appName(CONFIG["APP_NAME"]).getOrCreate()
    # Increase max message size for large DataFrame transfers
    spark.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs", False)
    spark.conf.set("spark.rpc.message.maxSize", "1024")
    return spark


def setup_cross_reference_tables(spark: SparkSession) -> None:
    """Ensure cross reference tables exist in the lakehouse."""
    create_table_if_not_exists(
        spark,
        "authorization",
        "authorization_fhir",
        {
            "carepro_AuthrequestId": "STRING",
            "FHIR_Id": "STRING",
            "FHIRNetwork": "INT",
            "Date_Created": "TIMESTAMP",
            "Date_Updated": "TIMESTAMP",
        },
        False,
    )
    create_table_if_not_exists(
        spark,
        "authorization",
        "document_fhir",
        {
            "carepro_AuthrequestId": "STRING",
            "carepro_DocumentBase_Annotation_Id": "STRING",
            "carepro_DocumentBase_Annotation_carepro_FileName": "STRING",
            "FHIR_Id": "STRING",
            "FHIRNetwork": "INT",
            "Date_Created": "TIMESTAMP",
            "Date_Updated": "TIMESTAMP",
        },
        False,
    )
    create_table_if_not_exists(
        spark,
        "authorization",
        "note_fhir",
        {
            "carepro_AuthrequestId": "STRING",
            "Note_Id": "STRING",
            "ActivityType": "STRING",
            "FHIR_Id": "STRING",
            "FHIRNetwork": "INT",
            "Date_Created": "TIMESTAMP",
            "Date_Updated": "TIMESTAMP",
        },
        False,
    )
    create_table_if_not_exists(
        spark,
        "authorization",
        "modificationhistory_fhir",
        {
            "carepro_AuthrequestId": "STRING",
            "ActivityId": "STRING",
            "carepro_GenericAttributelogBase_CareproName": "STRING",
            "FHIR_Id": "STRING",
            "FHIRNetwork": "INT",
            "Date_Created": "TIMESTAMP",
            "Date_Updated": "TIMESTAMP",
        },
        False,
    )
    create_table_if_not_exists(
        spark,
        "authorization",
        "claimresponse_fhir",
        {
            "carepro_AuthrequestId": "STRING",
            "FHIR_Id": "STRING",
            "FHIRNetwork": "INT",
            "Date_Created": "TIMESTAMP",
            "Date_Updated": "TIMESTAMP",
        },
        False,
    )


def main() -> None:
    parser = get_parser()
    args: Namespace = parser.parse_args()

    spark: SparkSession = configure_spark()

    # Retrieve secrets and set up headers
    request_url: str = get_secret(CONFIG["KEY_VAULT_NAME"], "fhirbaseurl")
    subscription_key: str = get_secret(CONFIG["KEY_VAULT_NAME"], "fhirsubscriptionkey")
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

    if args.run_copy_files:
        run_function()

    # Submit authorization data to FHIR
    asyncio.run(
        submit_auth_to_fhir(
            spark, request_url, headers, args.disable_fhir_submission, correlation_id
        )
    )

    # Build and send event
    event_type: str = "Evolent.FHIRLite.AuthDataLoadCompleted"
    event_data: dict = build_event_data(
        correlation_id, event_type, CONFIG["APP_NAME"], "FHIRLite"
    )
    event_hub_connection_string: str = get_secret(
        CONFIG["KEY_VAULT_NAME"], CONFIG["EVENT_HUB_SECRET"]
    )

    print(f"Sending event: {event_data}")
    if not args.disable_events:
        send_event(CONFIG["EVENT_HUB_NAME"], event_hub_connection_string, event_data)
        print(f"Sent event '{event_type}'")
    else:
        print("Events disabled. No event sent")


if __name__ == "__main__":
    main()
