from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, LinearRegressionModel
from pyspark.sql import DataFrame
from log import logger

def prepare_features(df: DataFrame) -> DataFrame:
    """
    Prepare features for machine learning model.

    Parameters:
    - df (DataFrame): Input DataFrame with columns 'trip_distance', 'trip_duration', and 'haversine_distance'.

    Returns:
    - DataFrame: DataFrame with 'features' and 'fare_amt' columns, ready for machine learning.
    """
    logger.info(df.columns)
    feature_cols = ["trip_distance", "trip_duration", "haversine_distance"]
    vec_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df = vec_assembler.transform(df)
    df = df.select("features", "fare_amt")
    return df

def select_features_for_ml(df : DataFrame) -> DataFrame:
    '''
    Selects features for ml (streamlit)
    returns df with 'features and 'fare_amt' columns.
    Features includes the following columns :
    column_names = {
        "start_lon" : "pickup_longitude",
        "start_lat" : "pickup_latitude",
        "end_lon" : "dropoff_longitude",
        "end_lat" : "dropoff_latitude",
        "passenger_count" : "passenger_count"
    }
    '''
    logger.info(df.columns)

    # apply same transfo
    # Convert to US/Eastern TZ-aware
    X_pred['pickup_datetime'] = pd.to_datetime(X_pred['pickup_datetime']).dt.tz_localize("US/Eastern")


    # assemble selected columns into features
    feature_cols = ["start_lon", "start_lat", "end_lon", "end_lat", "passenger_count"]
    vec_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df = vec_assembler.transform(df)
    df = df.select("features", "fare_amt")

    return df

def train_linear_regression(df: DataFrame) -> LinearRegressionModel:
    """
    Train a Linear Regression model using the 'features' and 'fare_amt' columns.

    Parameters:
    - df (DataFrame): Input DataFrame with 'features' and 'fare_amt' columns.

    Returns:
    - LinearRegression: Trained Linear Regression model.
    """
    lr = LinearRegression(featuresCol="features", labelCol="fare_amt")
    lr_model = lr.fit(df)
    return lr_model
