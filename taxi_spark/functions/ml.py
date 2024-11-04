from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, LinearRegressionModel
from pyspark.sql import DataFrame


def prepare_features(df: DataFrame) -> DataFrame:
    """
    Prepare features for machine learning model.

    Parameters:
    - df (DataFrame): Input DataFrame with columns 'trip_distance', 'trip_duration', and 'haversine_distance'.

    Returns:
    - DataFrame: DataFrame with 'features' and 'fare_amt' columns, ready for machine learning.
    """
    feature_cols = ["trip_distance", "trip_duration", "haversine_distance"]
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
