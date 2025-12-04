from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator, 
    BigQueryDeleteTableOperator
)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

# ========= CONFIG =========
GCP_PROJECT_ID = "learn-gcp-479215"
GCP_CONN_ID = "google_cloud_default"
BQ_DATASET = "raw"

# Source Data
BQ_SOURCE_TABLE = "transactions"
# Temp table to hold the 5-minute slice (Changes every run)
BQ_TEMP_TABLE = "temp_export_{{ ts_nodash }}"
# Audit Table
BQ_AUDIT_TABLE = "export_audit_log"

GCS_BUCKET = "confluent-topics"
# The file name will look like: exported_data/transactions_20250101T120000.json
GCS_DESTINATION_PATH = "exported_data/transactions_{{ ts_nodash }}.json"
# ==========================

with DAG(
    dag_id="bq_to_gcs",
    start_date=datetime(2025, 1, 1),
    schedule="*/10 * * * *", 
    catchup=False,
    tags=["export", "audit"]
) as dag:

    # 1. PREPARE INCREMENTAL DATA
    # Create a temporary table containing ONLY the data for this time interval.
    # Uses {{ data_interval_start }} and {{ data_interval_end }} from Airflow.
    create_temp_table = BigQueryInsertJobOperator(
        task_id="create_temp_table",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TEMP_TABLE}` AS
                    SELECT *
                    FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_SOURCE_TABLE}`
                    WHERE record_inserted_datetime >= TIMESTAMP_ADD(CAST('{{{{ data_interval_start }}}}' AS TIMESTAMP), INTERVAL -10 MINUTE)
                """,
                "useLegacySql": False,
            }
        },
    )

    # 2. EXPORT TO GCS
    # Exports the temp table we just created to a JSON file.
    export_to_gcs = BigQueryToGCSOperator(
        task_id="export_to_gcs",
        source_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TEMP_TABLE}",
        destination_cloud_storage_uris=[f"gs://{GCS_BUCKET}/{GCS_DESTINATION_PATH}"],
        export_format="NEWLINE_DELIMITED_JSON",
        gcp_conn_id=GCP_CONN_ID,
    )

    # 3. LOG TO AUDIT TABLE
    # Inserts the filename and timestamp into the audit table.
    write_audit_log = BigQueryInsertJobOperator(
        task_id="write_audit_log",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_AUDIT_TABLE}`
                    (filename, source_table, export_timestamp, status)
                    VALUES
                    (
                        'gs://{GCS_BUCKET}/{GCS_DESTINATION_PATH}', 
                        '{BQ_SOURCE_TABLE}', 
                        CURRENT_TIMESTAMP(), 
                        ''
                    )
                """,
                "useLegacySql": False,
            }
        },
    )

    # 4. CLEANUP
    # Delete the temporary table to save storage cost and clutter.
    delete_temp_table = BigQueryDeleteTableOperator(
        task_id="delete_temp_table",
        deletion_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TEMP_TABLE}",
        gcp_conn_id=GCP_CONN_ID,
        ignore_if_missing=True,
    )

    # Dependency Chain
    create_temp_table >> export_to_gcs >> write_audit_log >> delete_temp_table