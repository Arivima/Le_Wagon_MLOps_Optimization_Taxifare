from pyspark.sql import DataFrame, functions as F


def add_time_bins(df: DataFrame) -> DataFrame:
    """
    Add a 'time_bin' column to DataFrame based on the hour of 'trip_pickup_datetime'.

    morning = 5-11
    afternoon = 12-17
    evening = 18-22
    night = 23-4

    Parameters:
    - df (DataFrame): Input DataFrame with a 'trip_pickup_datetime' column.

    Returns:
    - DataFrame: DataFrame with an additional 'time_bin' column.
    """
    # $CHALLENGIFY_BEGIN
    hour = F.hour("trip_pickup_datetime")
    time_bins = (
        F.when(hour.between(5, 11), "morning")
        .when(hour.between(12, 17), "afternoon")
        .when(hour.between(18, 22), "evening")
        .otherwise("night")
    )
    return df.withColumn("time_bin", time_bins)
    # $CHALLENGIFY_END


def add_pickup_date(df: DataFrame) -> DataFrame:
    """
    Add a 'pickup_date' column to DataFrame by extracting the date from 'trip_pickup_datetime'.

    Parameters:
    - df (DataFrame): Input DataFrame with a 'trip_pickup_datetime' column.

    Returns:
    - DataFrame: DataFrame with an additional 'pickup_date' column.
    """
    # $CHALLENGIFY_BEGIN
    return df.withColumn("pickup_date", F.to_date("trip_pickup_datetime"))
    # $CHALLENGIFY_END


def drop_coordinates(df: DataFrame) -> DataFrame:
    """
    Drop coordinate columns from DataFrame.

    Parameters:
    - df (DataFrame): Input DataFrame with 'start_lon', 'start_lat', 'end_lon', 'end_lat' columns.

    Returns:
    - DataFrame: DataFrame without the coordinate columns.
    """
    # $CHALLENGIFY_BEGIN
    return df.drop("start_lon", "start_lat", "end_lon", "end_lat")
    # $CHALLENGIFY_END


def aggregate_metrics(df: DataFrame) -> DataFrame:
    """
    Aggregate metrics like average fare, total passengers, etc., by 'pickup_date' and 'time_bin'.

    Parameters:
    - df (DataFrame): Input DataFrame with 'pickup_date', 'time_bin', 'fare_amt', 'passenger_count', and 'haversine_distance'.

    Returns:
    - DataFrame: DataFrame with aggregated metrics: 'avg_fare', 'total_passengers', 'total_trips' and 'avg_distance'.
    """
    # $CHALLENGIFY_BEGIN
    agg_expr = [
        F.avg("fare_amt").alias("avg_fare"),
        F.sum("passenger_count").alias("total_passengers"),
        F.count("*").alias("total_trips"),
        F.avg("haversine_distance").alias("avg_distance"),
    ]
    return df.groupBy("pickup_date", "time_bin").agg(*agg_expr)
    # $CHALLENGIFY_END


def sort_by_date_and_time(df: DataFrame) -> DataFrame:
    """
    Sort DataFrame by 'pickup_date' and 'time_bin'.

    Parameters:
    - df (DataFrame): Input DataFrame with 'pickup_date' and 'time_bin' columns.

    Returns:
    - DataFrame: DataFrame sorted by 'pickup_date' and 'time_bin' in ascending order.
    """
    # $CHALLENGIFY_BEGIN
    time_order = (
        F.when(F.col("time_bin") == "morning", 1)
        .when(F.col("time_bin") == "afternoon", 2)
        .when(F.col("time_bin") == "evening", 3)
        .otherwise(4)
    )
    return df.orderBy("pickup_date", time_order)
    # $CHALLENGIFY_END
