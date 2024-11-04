from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


def enforce_schema(df):
    """
    Enforces a specific schema on a given DataFrame.

    Parameters:
    - df (DataFrame): The DataFrame to enforce the schema on.

    Returns:
    - DataFrame: The original DataFrame if the schema matches.

    Raises:
    - AssertionError: If the DataFrame schema does not match the expected schema.
    """
    # $CHALLENGIFY_BEGIN
    expected_schema = StructType(
        [
            StructField("vendor_name", StringType(), True),
            StructField("Trip_Pickup_DateTime", StringType(), True),
            StructField("Trip_Dropoff_DateTime", StringType(), True),
            StructField("Passenger_Count", LongType(), True),
            StructField("Trip_Distance", DoubleType(), True),
            StructField("Start_Lon", DoubleType(), True),
            StructField("Start_Lat", DoubleType(), True),
            StructField("Rate_Code", DoubleType(), True),
            StructField("store_and_forward", DoubleType(), True),
            StructField("End_Lon", DoubleType(), True),
            StructField("End_Lat", DoubleType(), True),
            StructField("Payment_Type", StringType(), True),
            StructField("Fare_Amt", DoubleType(), True),
            StructField("surcharge", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("Tip_Amt", DoubleType(), True),
            StructField("Tolls_Amt", DoubleType(), True),
            StructField("Total_Amt", DoubleType(), True),
        ]
    )

    assert (
        df.schema == expected_schema
    ), "DataFrame schema does not match expected schema"
    # $CHALLENGIFY_END
    return df
