from datetime import date

from pyspark.sql.types import DateType

from taxi_spark.functions.processing import (
    add_time_bins,
    add_pickup_date,
    drop_coordinates,
    aggregate_metrics,
    sort_by_date_and_time,
)


def test_add_time_bins(spark):
    data = [
        "2023-10-29 08:30:00",  # morning
        "2023-10-29 14:45:00",  # afternoon
        "2023-10-29 20:15:00",  # evening
        "2023-10-29 03:00:00",  # night
        "2023-10-29 23:30:00",
    ]  # night
    df = spark.createDataFrame(data, "string").toDF("trip_pickup_datetime")

    result_df = add_time_bins(df)
    assert "time_bin" in result_df.columns

    expected_values = ["morning", "afternoon", "evening", "night", "night"]
    actual_values = [row["time_bin"] for row in result_df.select("time_bin").collect()]
    assert actual_values == expected_values


def test_add_pickup_date(spark):
    df = spark.createDataFrame(["2023-10-29 08:30:00"], "string").toDF(
        "trip_pickup_datetime"
    )

    result_df = add_pickup_date(df)

    assert result_df.schema["pickup_date"].dataType == DateType()
    result = result_df.collect()[0]["pickup_date"]
    assert result.year == 2023
    assert result.month == 10
    assert result.day == 29


def test_drop_coordinates(spark):
    cols = ["start_lon", "start_lat", "end_lon", "end_lat"]
    df = spark.createDataFrame([(0, 0, 0, 0)], cols)

    result_df = drop_coordinates(df)

    assert all(c not in result_df.schema for c in cols)


def test_aggregate_metrics(spark):
    input_df = spark.createDataFrame(
        [
            (date(2023, 10, 29), "morning", 3.0, 3.0, 4.5),
            (date(2023, 10, 29), "morning", 8.0, 8.0, 10.5),
            (date(2023, 10, 29), "afternoon", 3.0, 3.0, 4.5),
        ],
        [
            "pickup_date",
            "time_bin",
            "fare_amt",
            "passenger_count",
            "haversine_distance",
        ],
    )

    expected_df = spark.createDataFrame(
        [
            (date(2023, 10, 29), "morning", 5.5, 11.0, 2, 7.5),
            (date(2023, 10, 29), "afternoon", 3.0, 3.0, 1, 4.5),
        ],
        [
            "pickup_date",
            "time_bin",
            "avg_fare",
            "total_passengers",
            "total_trips",
            "avg_distance",
        ],
    )

    result_df = aggregate_metrics(input_df)

    assert expected_df.collect() == result_df.collect()


def test_sort_by_date_and_time(spark):
    input_df = spark.createDataFrame(
        [
            (date(2023, 10, 29), "afternoon"),
            (date(2023, 10, 28), "morning"),
            (date(2023, 10, 29), "morning"),
        ],
        ["pickup_date", "time_bin"],
    )

    expected_df = spark.createDataFrame(
        [
            (date(2023, 10, 28), "morning"),
            (date(2023, 10, 29), "morning"),
            (date(2023, 10, 29), "afternoon"),
        ],
        ["pickup_date", "time_bin"],
    )

    result_df = sort_by_date_and_time(input_df)

    assert expected_df.collect() == result_df.collect()
