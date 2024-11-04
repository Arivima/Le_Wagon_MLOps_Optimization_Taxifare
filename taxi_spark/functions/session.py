import os
from pathlib import Path

from pyspark.sql import SparkSession


def get_spark_session():
    spark_builder = SparkSession.builder.appName("Taxifare PySpark App")

    if os.environ.get("LOCAL"):
        home_dir = Path.home()
        spark_builder = (
            spark_builder.config(
                "spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            )
            .config("spark.hadoop.fs.gs.project.id", os.environ["PROJECT_ID"])
            .config(
                "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                f"{home_dir}/.config/gcloud/application_default_credentials.json",
            )
            .config(
                "spark.jars",
                f"{home_dir}/spark/jars/gcs-connector-hadoop3-latest.jar",
            )
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
        )

    return spark_builder.getOrCreate()
