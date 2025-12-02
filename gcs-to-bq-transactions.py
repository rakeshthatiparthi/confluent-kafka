from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# ========= CONFIGURE THESE =========
GCP_PROJECT_ID = "learn-gcp-479215"
BQ_DATASET = "raw"
BQ_TABLE = "transactions"

GCS_BUCKET = "confluent-topics"

# Matches:
# gs://confluent-topics/topics/transactions/day=XX/hour=YY/minute=ZZ/*.json
# GCS_OBJECTS_PATTERN = "topics/transactions/day=*/hour=*/minute=*/*.json"
GCS_OBJECTS_PATTERN = "topics/transactions/*"


GCP_CONN_ID = "google_cloud_default"
# ==================================


with DAG(
    dag_id="gcs_to_bq_transactions",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",   # ← NEW PARAM
    catchup=False,
    description="Load Kafka GCS sink JSON (5-min partitions) into BigQuery",
    tags=["gcs", "bigquery", "kafka", "transactions"],
) as dag:

    load_transactions_to_bq = GCSToBigQueryOperator(
        task_id="load_transactions_to_bq",
        bucket=GCS_BUCKET,
        source_objects=[GCS_OBJECTS_PATTERN],
        destination_project_dataset_table=(
            f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
        ),
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,                   # Let BigQuery infer schema from JSON
        write_disposition="WRITE_APPEND",  # Append into the same table
        create_disposition="CREATE_IF_NEEDED",
        max_bad_records=10,
        gcp_conn_id=GCP_CONN_ID,
    )

    load_transactions_to_bq