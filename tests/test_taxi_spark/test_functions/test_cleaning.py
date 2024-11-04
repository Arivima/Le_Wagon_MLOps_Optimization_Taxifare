from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    TimestampType,
)

from taxi_spark.functions.cleaning import (
    remove_duplicates,
    handle_nulls,
    type_casting,
    normalize_strings,
    format_dates,
    filter_coordinates,
    rename_columns,
)


def test_remove_duplicates(spark):
    # $CHALLENGIFY_BEGIN
    df = spark.createDataFrame(
        [(1, "a"), (2, "b"), (2, "b")], ["Passenger_Count", "vendor_name"]
    )
    df = remove_duplicates(df)
    assert df.count() == 2
    # $CHALLENGIFY_END


def test_handle_nulls(spark):
    # $CHALLENGIFY_BEGIN
    schema = StructType(
        [
            StructField("Tip_Amt", FloatType(), True),
            StructField("Tolls_Amt", FloatType(), True),
            StructField("surcharge", FloatType(), True),
            StructField("Rate_Code", StringType(), True),
            StructField("store_and_forward", StringType(), True),
            StructField("mta_tax", FloatType(), True),
        ]
    )
    df = spark.createDataFrame([(None, None, None, None, None, None)], schema=schema)
    df = handle_nulls(df)
    assert df.filter(F.col("Tip_Amt").isNull()).count() == 0
    # $CHALLENGIFY_END


def test_type_casting(spark):
    # $CHALLENGIFY_BEGIN
    df = spark.createDataFrame([("1", "2.5")], ["Passenger_Count", "Trip_Distance"])
    df = type_casting(df)
    assert df.schema["Passenger_Count"].dataType == IntegerType()
    assert df.schema["Trip_Distance"].dataType == FloatType()
    # $CHALLENGIFY_END


def test_normalize_strings(spark):
    # $CHALLENGIFY_BEGIN
    df = spark.createDataFrame([(" a  ", " cash ")], ["vendor_name", "Payment_Type"])
    df = normalize_strings(df)
    assert df.collect()[0]["Payment_Type"] == "CASH"
    assert df.collect()[0]["vendor_name"] == "A"
    # $CHALLENGIFY_END


def test_format_dates(spark):
    # $CHALLENGIFY_BEGIN
    df = spark.createDataFrame(
        [("2021-01-01 12:34:56", "2021-01-01 12:35:56")],
        ["Trip_Pickup_DateTime", "Trip_Dropoff_DateTime"],
    )
    df = format_dates(df)
    assert df.schema["Trip_Pickup_DateTime"].dataType == TimestampType()
    # $CHALLENGIFY_END


def test_filter_coordinates(spark):
    # $CHALLENGIFY_BEGIN
    df = spark.createDataFrame(
        [(181, -181, 181, -181)], ["Start_Lon", "Start_Lat", "End_Lon", "End_Lat"]
    )
    df = filter_coordinates(df)
    assert df.count() == 0
    # $CHALLENGIFY_END


def test_rename_columns(spark):
    # $CHALLENGIFY_BEGIN
    df = spark.createDataFrame([(1,)], ["a b"])
    df = rename_columns(df)
    assert "a_b" in df.columns
    # $CHALLENGIFY_END
