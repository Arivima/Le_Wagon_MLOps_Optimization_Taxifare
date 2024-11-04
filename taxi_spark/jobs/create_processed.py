import argparse

from taxi_spark.functions.ml import prepare_features, train_linear_regression
from taxi_spark.functions.processing import (
    add_pickup_date,
    add_time_bins,
    aggregate_metrics,
    drop_coordinates,
    sort_by_date_and_time,
)
from taxi_spark.functions.session import get_spark_session


def create_processed_pipeline(
    staging_uri: str, processed_uri_prefix: str, date
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
    # $CHALLENGIFY_BEGIN
    spark = get_spark_session()
    df = spark.read.parquet(staging_uri)
    ml_df = prepare_features(df)
    ml_df.write.parquet(
        f"{processed_uri_prefix}/model_data_yellow_tripdata_{date}", mode="overwrite"
    )
    lr_model = train_linear_regression(ml_df)
    lr_model.write().overwrite().save(
        f"{processed_uri_prefix}/lr_model_yellow_tripdata_{date}"
    )
    df = add_time_bins(df)
    df = add_pickup_date(df)
    df = drop_coordinates(df)
    df = aggregate_metrics(df)
    df = sort_by_date_and_time(df)
    df.write.parquet(
        f"{processed_uri_prefix}/analyst_model_yellow_tripdata_{date}", mode="overwrite"
    )
    # $CHALLENGIFY_END


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

    create_processed_pipeline(raw_uri, processed_uri_prefix, args.date)


if __name__ == "__main__":
    main()
