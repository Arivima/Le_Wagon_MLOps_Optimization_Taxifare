from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_trip_duration(df: DataFrame) -> DataFrame:
    """
    Calculates the trip duration in minutes.
    Trip duration is calculated as the difference between 'Trip_Dropoff_DateTime'
    and 'Trip_Pickup_DateTime' in Unix timestamp format, divided by 60.

    Args:
        df (DataFrame): Input DataFrame with 'Trip_Dropoff_DateTime'
                        and 'Trip_Pickup_DateTime' columns.

    Returns:
        DataFrame: New DataFrame with a 'trip_duration' column added.
    """
    # $CHALLENGIFY_BEGIN
    return df.withColumn(
        "trip_duration",
        (
            F.unix_timestamp("Trip_Dropoff_DateTime")
            - F.unix_timestamp("Trip_Pickup_DateTime")
        )
        / 60,
    )
    # $CHALLENGIFY_END


def calculate_haversine_distance(df: DataFrame) -> DataFrame:
    """
    Calculates the haversine distance between start and end coordinates.
    Uses columns 'start_lat', 'start_lon', 'end_lat', 'end_lon' for calculations.
    The distance is calculated in kilometers.

    Args:
        df (DataFrame): Input DataFrame with 'start_lat', 'start_lon',
                        'end_lat', 'end_lon' columns.

    Returns:
        DataFrame: New DataFrame with a 'haversine_distance' column added.
    """
    # $CHALLENGIFY_BEGIN
    R = 6371  # Earth radius in km
    lat1_rad = F.radians(F.col("start_lat"))
    lon1_rad = F.radians(F.col("start_lon"))
    lat2_rad = F.radians(F.col("end_lat"))
    lon2_rad = F.radians(F.col("end_lon"))
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = F.sin(dlat / 2) ** 2 + F.cos(lat1_rad) * F.cos(lat2_rad) * F.sin(dlon / 2) ** 2
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    haversine_distance = R * c
    return df.withColumn("haversine_distance", haversine_distance)
    # $CHALLENGIFY_END
