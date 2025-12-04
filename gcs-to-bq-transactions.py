from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

# ========= CONFIG =========
GCP_PROJECT_ID = "learn-gcp-479215"
GCP_CONN_ID = "google_cloud_default"

BQ_DATASET = "raw"
BQ_TABLE_FINAL = "transactions"
BQ_TABLE_STAGING = "transactions_staging"   # temp/staging table
GCS_BUCKET = "confluent-topics"

# Source prefix (where Kafka→GCS sink writes)
GCS_SOURCE_PREFIX = "topics/transactions/"

# Archive prefix (where we move processed files)
GCS_ARCHIVE_PREFIX = "archive-topics/transactions/"
# ==========================

with DAG(
    dag_id="gcs_to_bq",
    start_date=datetime(2025, 1, 1),
    schedule="*/7 * * * *",  #run every 5 minutes
    catchup=False,
    description=(
        "Load ALL files from GCS topics/transactions into staging, "
        "insert into final table with record_inserted_datetime, "
        "then move files to archive-topics/transactions"
    )
) as dag:

    # Load all current files from topics/transactions → STAGING (overwrite each run)
    load_to_staging = GCSToBigQueryOperator(
        task_id="load_to_staging",
        bucket=GCS_BUCKET,
        source_objects=[f"{GCS_SOURCE_PREFIX}*"],   # all objects under topics/transactions/
        destination_project_dataset_table=(
            f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_STAGING}"
        ),
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",        # staging holds only this run's batch
        create_disposition="CREATE_IF_NEEDED",
        max_bad_records=10,
        gcp_conn_id=GCP_CONN_ID,
    )

    # Insert from STAGING → FINAL with record_inserted_datetime
    merge_to_final = BigQueryInsertJobOperator(
        task_id="merge_to_final",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_FINAL}`
                    SELECT
                        CAST(transaction_id AS INT64),
                        CAST(card_id AS INT64),
                        CAST(user_id AS STRING),
                        CAST(purchase_id AS INT64),
                        CAST(store_id AS INT64),
                        CURRENT_TIMESTAMP() AS record_inserted_datetime
                    FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_STAGING}`
                """,
                "useLegacySql": False,
            }
        },
    )

    # Move processed files from topics/transactions → archive-topics/transactions
    move_files_to_archive = GCSToGCSOperator(
        task_id="move_files_to_archive",
        source_bucket=GCS_BUCKET,
        source_object=f"{GCS_SOURCE_PREFIX}*",   # everything under topics/transactions/
        destination_bucket=GCS_BUCKET,
        destination_object=GCS_ARCHIVE_PREFIX,  # prefix; rest of path is preserved
        move_object=True,                       # copy then delete originals = move
        gcp_conn_id=GCP_CONN_ID,
    )

    load_to_staging >> merge_to_final >> move_files_to_archive