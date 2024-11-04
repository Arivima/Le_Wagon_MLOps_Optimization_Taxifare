import uuid
import pendulum

from airflow.decorators import dag
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)


@dag(
    "spark-pipeline",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["spark"],
)
def spark_pipeline_dag():
    """
    Airflow DAG to execute a Spark pipeline on Google Cloud Dataproc.

    Steps:
    1. `create_dataproc_cluster`: Creates a Dataproc cluster in the specified Google Cloud region.
    2. `copy_wheel_to_gcs`: Copies Python Wheel package to Google Cloud Storage (GCS).
    3. `copy_jobs_to_gcs`: Copies Spark job scripts to GCS.
    4. `write_to_raw`: Executes the Spark job to write raw data to GCS.
    5. `staging_transform`: Executes the Spark job to transform staged data.
    6. `create_processed`: Executes the Spark job to generate processed data.
    7. `delete_dataproc_cluster`: Deletes the Dataproc cluster.

    Parameters:
    - UUID (str): Auto-generated unique identifier for the DAG run.
    - BUCKET_NAME (str): GCS bucket name where data and code are stored.
    - PROJECT (str): Google Cloud Project ID.
    - REGION (str): Dataproc cluster's region.
    - NETWORK (str): VPC network for Dataproc.
    - DATE (str): Data partition identifier (e.g., "2009-01").
    """
    UUID = str(uuid.uuid4())
    BUCKET_NAME = ""
    PROJECT = ""
    REGION = "europe-west1"
    NETWORK = "spark-vpc"
    DATE = "2009-01"
    # $CHALLENGIFY_BEGIN

    copy_wheel_to_gcs = LocalFilesystemToGCSOperator(
        task_id="copy_wheel_to_gcs",
        src="/app/airflow/dist/*.whl",
        bucket=BUCKET_NAME,
        dst="python/taxi_spark.whl",
        gcp_conn_id="google_cloud_default",
    )

    copy_jobs_to_gcs = LocalFilesystemToGCSOperator(
        task_id="copy_jobs_to_gcs",
        src="/app/airflow/taxi_spark/jobs/*.py",
        bucket=BUCKET_NAME,
        dst="python/jobs/",
        gcp_conn_id="google_cloud_default",
    )

    def create_batch(task_id, python_file):
        return DataprocCreateBatchOperator(
            task_id=task_id,
            batch={
                "pyspark_batch": {
                    "main_python_file_uri": f"gs://{BUCKET_NAME}/python/jobs/{python_file}",
                    "python_file_uris": [f"gs://{BUCKET_NAME}/python/taxi_spark.whl"],
                    "args": [f"--date={DATE}", f"--bucket={BUCKET_NAME}"],
                },
                "environment_config": {
                    "execution_config": {"network_uri": NETWORK},
                },
            },
            project_id=PROJECT,
            region=REGION,
            batch_id=f"{task_id}-{UUID}",
            gcp_conn_id="google_cloud_default",
        )

    write_to_raw_batch = create_batch("write-to-raw", "write_to_raw.py")
    staging_transform_batch = create_batch("staging-transform", "staging_transform.py")
    create_processed_batch = create_batch("create-processed", "create_processed.py")

    (
        [copy_wheel_to_gcs, copy_jobs_to_gcs]
        >> write_to_raw_batch
        >> staging_transform_batch
        >> create_processed_batch
    )
    # $CHALLENGIFY_END


spark_pipeline_dag()
