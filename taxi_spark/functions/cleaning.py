import re
from pyspark.sql import DataFrame, functions as F


def remove_duplicates(df: DataFrame) -> DataFrame:
    """
    Removes duplicate rows based on all columns.

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: New DataFrame with all duplicate rows removed.
    """
    # $CHALLENGIFY_BEGIN
    return df.dropDuplicates()
    # $CHALLENGIFY_END


def handle_nulls(df: DataFrame) -> DataFrame:
    """
    Fills null values in specified columns with predefined constants.
    'Tip_Amt', 'Tolls_Amt', 'surcharge', 'mta_tax' are filled with 0.
    'Rate_Code' is filled with 'Standard'.
    'store_and_forward' is filled with 'N'.

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: New DataFrame with null values in specified columns replaced.
    """
    # $CHALLENGIFY_BEGIN
    return df.fillna(
        {
            "Tip_Amt": 0,
            "Tolls_Amt": 0,
            "surcharge": 0,
            "Rate_Code": "Standard",
            "store_and_forward": "N",
            "mta_tax": 0,
        }
    )
    # $CHALLENGIFY_END


def type_casting(df: DataFrame) -> DataFrame:
    """
    Casts 'Passenger_Count' to integer and 'Trip_Distance' to float.

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: A new DataFrame with columns cast to specified types.
    """
    # $CHALLENGIFY_BEGIN
    df = df.withColumn("Passenger_Count", F.col("Passenger_Count").cast("int"))
    return df.withColumn("Trip_Distance", F.col("Trip_Distance").cast("float"))
    # $CHALLENGIFY_END


def normalize_strings(df: DataFrame) -> DataFrame:
    """
    Trims and uppercases the 'vendor_name' and 'Payment_Type' columns.

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: A new DataFrame with normalized string values.
    """
    # $CHALLENGIFY_BEGIN
    df = df.withColumn("vendor_name", F.upper(F.trim(F.col("vendor_name"))))
    return df.withColumn("Payment_Type", F.upper(F.trim(F.col("Payment_Type"))))
    # $CHALLENGIFY_END


def format_dates(df: DataFrame) -> DataFrame:
    """
    Converts 'Trip_Pickup_DateTime' and 'Trip_Dropoff_DateTime' to timestamp format.

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: A new DataFrame with dates converted to timestamps.
    """
    # $CHALLENGIFY_BEGIN
    df = df.withColumn(
        "Trip_Pickup_DateTime",
        F.to_timestamp("Trip_Pickup_DateTime", "yyyy-MM-dd HH:mm:ss"),
    )
    return df.withColumn(
        "Trip_Dropoff_DateTime",
        F.to_timestamp("Trip_Dropoff_DateTime", "yyyy-MM-dd HH:mm:ss"),
    )
    # $CHALLENGIFY_END


def filter_coordinates(df: DataFrame) -> DataFrame:
    """
    Filters rows where coordinates are outside the valid range.
    (-180 <= Longitude <= 180, -90 <= Latitude <= 90)

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: A new DataFrame with rows having valid coordinates.
    """
    # $CHALLENGIFY_BEGIN
    df = df.filter((F.col("Start_Lon") >= -180) & (F.col("Start_Lon") <= 180))
    df = df.filter((F.col("Start_Lat") >= -90) & (F.col("Start_Lat") <= 90))
    df = df.filter((F.col("End_Lon") >= -180) & (F.col("End_Lon") <= 180))
    df = df.filter((F.col("End_Lat") >= -90) & (F.col("End_Lat") <= 90))
    return df
    # $CHALLENGIFY_END


def rename_columns(df: DataFrame) -> DataFrame:
    """
    Renames all columns by replacing non-alphanumeric characters with underscores
    and converting the text to lower case.

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: A new DataFrame with renamed columns.
    """
    # $CHALLENGIFY_BEGIN
    new_column_names = [
        re.sub(r"[^a-zA-Z0-9]", "_", col_name.lower()) for col_name in df.columns
    ]
    return df.toDF(*new_column_names)
    # $CHALLENGIFY_END
