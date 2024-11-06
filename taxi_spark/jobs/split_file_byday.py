import argparse
from datetime import datetime
from google.cloud import storage
from taxi_spark.functions.session import get_spark_session

def split_monthly_data_by_day(bucket_name, monthly_blob_name, daily_prefix):
    spark = get_spark_session()

    print(f"Lecture du fichier mensuel : gs://{bucket_name}/{monthly_blob_name}")
    df = spark.read.parquet(f"gs://{bucket_name}/{monthly_blob_name}")

    print("Colonnes disponibles :", df.columns)

    date_column = "Trip_Pickup_DateTime"

    df = df.withColumn("year", df[date_column].substr(1, 4))
    df = df.withColumn("month", df[date_column].substr(6, 2))
    df = df.withColumn("day", df[date_column].substr(9, 2))

    for day in range(1, 32):
        day_str = f"{day:02d}"
        daily_df = df.filter(df["day"] == day_str)

        count = daily_df.count()
        print(f"Nombre de lignes pour le jour {day_str}: {count}")

        if count > 0:
            daily_blob_name = f"{daily_prefix}/yellow_tripdata_2009-01-{day_str}.parquet"
            print(f"Tentative d'enregistrement du fichier : gs://{bucket_name}/{daily_blob_name}")

            daily_df.write.mode("overwrite").parquet(f"gs://{bucket_name}/{daily_blob_name}")
            print(f"Fichier {daily_blob_name} enregistré avec succès.")
        else:
            print(f"Aucune donnée pour le jour {day_str}, fichier non créé.")

    print("Découpage du fichier mensuel en fichiers journaliers terminé.")

def main():
    parser = argparse.ArgumentParser(
        description="Split a monthly Parquet file into daily Parquet files."
    )
    parser.add_argument("--bucket", required=True, help="Nom du bucket GCS")
    parser.add_argument("--monthly_blob_name", required=True, help="Nom du blob pour le fichier Parquet mensuel")
    parser.add_argument("--daily_prefix", required=True, help="Préfixe pour les fichiers journaliers")
    args = parser.parse_args()

    split_monthly_data_by_day(args.bucket, args.monthly_blob_name, args.daily_prefix)

if __name__ == "__main__":
    main()
