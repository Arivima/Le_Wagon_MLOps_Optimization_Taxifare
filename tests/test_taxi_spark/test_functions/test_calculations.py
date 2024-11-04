from taxi_spark.functions.calculations import (
    calculate_trip_duration,
    calculate_haversine_distance,
)
from pyspark.sql import functions as F


def test_calculate_trip_duration(spark):
    # $CHALLENGIFY_BEGIN
    df = spark.createDataFrame(
        [("2021-01-01 12:00:00", "2021-01-01 12:01:00")],
        ["Trip_Pickup_DateTime", "Trip_Dropoff_DateTime"],
    )
    df = df.withColumn(
        "Trip_Pickup_DateTime",
        F.to_timestamp("Trip_Pickup_DateTime", "yyyy-MM-dd HH:mm:ss"),
    )
    df = df.withColumn(
        "Trip_Dropoff_DateTime",
        F.to_timestamp("Trip_Dropoff_DateTime", "yyyy-MM-dd HH:mm:ss"),
    )
    df = calculate_trip_duration(df)
    assert df.collect()[0]["trip_duration"] == 1
    # $CHALLENGIFY_END


def test_calculate_haversine_distance(spark):
    # $CHALLENGIFY_BEGIN
    df = spark.createDataFrame(
        [(40.7128, -74.0060, 34.0522, -118.2437)],
        ["start_lat", "start_lon", "end_lat", "end_lon"],
    )
    df = calculate_haversine_distance(df)
    assert df.collect()[0]["haversine_distance"] is not None
    # $CHALLENGIFY_END
