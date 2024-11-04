from taxi_spark.functions.schema import enforce_schema
import pytest
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType


def test_enforce_schema(spark):
    data = [
        (
            "test_vendor",
            "2022-01-01 12:00:00",
            "2022-01-01 12:30:00",
            1,
            2.5,
            -73.991957,
            40.721567,
            1.0,
            1.0,
            -73.993803,
            40.695922,
            "CASH",
            5.5,
            0.5,
            0.5,
            1.0,
            1.0,
            9.5,
        )
    ]

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

    df = spark.createDataFrame(data, schema=expected_schema)

    result = enforce_schema(df)
    assert result.count() == 1, "Expected count not matching"

    df_incorrect = df.drop("vendor_name")

    with pytest.raises(AssertionError):
        enforce_schema(df_incorrect)
