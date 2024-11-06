import argparse
from ..function import utils_gcp
from taxi_spark.functions.ml import prepare_features
from taxi_spark.functions.session import get_spark_session


def transform_to_gold_model_data(
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

    Example:
    >>> create_processed_pipeline("gs://staging/data/data_1", "gs://processed/data", "2021-01-01")
    """

    spark = get_spark_session()
    df = spark.read.parquet(staging_uri)
    ml_df = prepare_features(df)
    ml_df.write.parquet(
        f"{processed_uri_prefix}/model_data_yellow_tripdata_{date}", mode="overwrite"
    )



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

    transform_to_gold_model_data(raw_uri, processed_uri_prefix, args.date)


if __name__ == "__main__":
    main()
