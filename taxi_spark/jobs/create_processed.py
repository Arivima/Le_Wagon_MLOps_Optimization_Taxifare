import argparse

from taxi_spark.functions.ml import (
    prepare_features,
    train_linear_regression,
    select_features_for_ml
)
from taxi_spark.functions.processing import (
    add_pickup_date,
    add_time_bins,
    aggregate_metrics,
    drop_coordinates,
    sort_by_date_and_time,
)
from taxi_spark.functions.session import get_spark_session

from log import logger
from google.cloud import storage
from taxi_spark.functions import utils_gcp

import os, shutil
import tempfile
import json


def create_processed_pipeline(
    staging_uri: str, processed_uri_prefix: str, date, bucket_name
) -> None:
    """
    Process raw data and create machine learning models.

    Parameters:
    - staging_uri (str): URI where the staging data is stored.
    - processed_uri_prefix (str): URI where the processed data and models will be saved.
    - date (str/int/DateType): Date identifier for the data being processed.

    Output:
    Writes processed data and trained linear regression model to 'processed_uri'.

    Steps:
    1. Reads parquet data from 'staging_uri' into a DataFrame.
    2. Prepares features and writes to a parquet file.
    3. Trains a linear regression model and saves it.
    4. Enhances DataFrame with time bins, pickup date, etc.
    5. Writes enhanced DataFrame to a parquet file.

    Example:
    >>> create_processed_pipeline("gs://staging/data/data_1", "gs://processed/data", "2021-01-01")
    """
    spark = get_spark_session()
    logger.info('starting spark session')
    df = spark.read.parquet(staging_uri)

    # prepare model_data
    logger.info('prepare model data')
    ml_df = prepare_features(df)
    model_data_path = f"{processed_uri_prefix}/model_data_yellow_tripdata_{date}"
    ml_df.write.parquet(model_data_path, mode="overwrite")
    logger.info(f'analyst model saved at {model_data_path}')

    # train model
    logger.info('train model')
    ml_df = select_features_for_ml(df)
    lr_model = train_linear_regression(ml_df)
    logger.info('model trained')

    # Save weights and intercept to gcs
    # Extract weights (coefficients) and intercept
    weights = lr_model.coefficients.toArray().tolist()
    intercept = lr_model.intercept
    model_params = {
        "weights": weights,
        "intercept": intercept
    }
    model_json = json.dumps(model_params)
    prefix = "/".join(processed_uri_prefix.split("/")[3:])
    gcp_json_path = f"{prefix}/json_model_yellow_tripdata_{date}.json"

    # Initialize GCS client
    client = utils_gcp.connect_to_storage_client()
    bucket = client.bucket(bucket_name)

    # Delete existing JSON blob if it exists
    utils_gcp.delete_existing_blobs(bucket_name, gcp_json_path)
    blob = bucket.blob(gcp_json_path)

    # Upload the JSON string to GCS
    blob.upload_from_string(model_json, content_type="application/json")
    logger.info(f"Model weights and intercept saved to GCS at gs://{bucket_name}/{gcp_json_path}")
    blob.download_to_filename('/tmp/test.json')


    # # save model temporarily locally
    # with tempfile.TemporaryDirectory() as temp_dir:
    #     spark_model_path = os.path.join(temp_dir, "spark_model")
    #     pyfunc_model_path = os.path.join(temp_dir, "pyfunc_model")

    #     # Save Spark model to spark_model_path with overwrite mode
    #     logger.info(f"Saving Spark model at {spark_model_path}")
    #     # mlflow.spark.save_model(spark_model=lr_model, path=local_model_path)
    #     lr_model.write().overwrite().save(spark_model_path)

    #     # Save PyFunc model with the Spark model artifact included
    #     logger.info(f"Saving PyFunc model at {pyfunc_model_path}")
    #     mlflow.pyfunc.save_model(
    #         path=pyfunc_model_path,
    #         python_model=LinearRegressionWrapper(),
    #         artifacts={"model_path": spark_model_path},
    #     )
    #     logger.info(f"Model saved locally at {pyfunc_model_path}")

    #     # Checking model is saved correctly locally
    #     # if mlflow.spark.load_model(model_uri=local_model_path) is None:
    #     if mlflow.pyfunc.load_model(model_uri=pyfunc_model_path) is None:
    #         raise AssertionError("Model not saved correctly locally")

    #     # Delete existing blobs in GCS model path
    #     logger.info('Delete existing blobs in GCS model path')
    #     gcp_model_path = f"{processed_uri_prefix}/lr_model_yellow_tripdata_{date}"
    #     utils_gcp.delete_existing_blobs(bucket_name, gcp_model_path)

    #     # Upload local model directory to GCS
    #     logger.info('Uploading model directory to GCS')
    #     utils_gcp.upload_directory(pyfunc_model_path, bucket_name, gcp_model_path)
    #     logger.info(f"Model uploaded to {gcp_model_path}")

    # # checking model is saved correctly on gcp
    # logger.info('check model is saved correctly on gcp')
    # # test_model_gcp = mlflow.spark.load_model(model_uri=gcp_model_path)
    # test_model_gcp = mlflow.pyfunc.load_model(model_uri=gcp_model_path)
    # logger.info(f"Model loaded for testing: {test_model_gcp is not None}")


    # prepare analyst_data
    logger.info('prepare analyst data')
    df = add_time_bins(df)
    df = add_pickup_date(df)
    df = drop_coordinates(df)
    df = aggregate_metrics(df)
    df = sort_by_date_and_time(df)
    df.write.parquet(
        f"{processed_uri_prefix}/analyst_model_yellow_tripdata_{date}", mode="overwrite"
    )
    logger.info('analyst data saved')


    # lr_model.write().overwrite().save(file_path)
    # file_name = f"lr_model_yellow_tripdata_{date}.joblib"
    # joblib.dump(lr_model, "./" + file_name, compress = 3)

    # blob_path = f"/processed/taxi_data/{file_name}"

    # from google.cloud import storage
    # srcPath = "./" + file_name
    # storage_client = storage.Client()
    # bucket = storage_client.bucket(bucket_name)
    # blob = bucket.blob(blob_path)
    # with open(srcPath, "w") as f:
    #     blob.upload_from_file(f)



def main() -> None:
    """
    Main function that sets up argument parsing for command-line execution, builds the URL and GCS path,
    and then calls the function to download and save the Parquet file.
    """
    parser = argparse.ArgumentParser(
        description="Preprocess the raw data and save it to the staging area in GCS"
    )
    parser.add_argument("--date", required=True, help="Date in the format yyyy-MM")
    parser.add_argument("--bucket", required=True, help="GCS bucket name")
    args = parser.parse_args()

    raw_uri = f"gs://{args.bucket}/staging/taxi_data/yellow_tripdata_{args.date}"
    processed_uri_prefix = f"gs://{args.bucket}/processed/taxi_data"

    create_processed_pipeline(raw_uri, processed_uri_prefix, args.date, args.bucket)


if __name__ == "__main__":
    main()
