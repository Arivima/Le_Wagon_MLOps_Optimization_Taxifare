import timeit
import csv
import argparse
import os
from google.cloud import storage
from datetime import datetime, timedelta
from taxi_spark.functions.session import get_spark_session
from taxi_spark.functions.calculations import (
    calculate_trip_duration,
    calculate_haversine_distance,
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

def initialize_csv(modification_name, bucket_name):
    """Initialise le fichier CSV dans GCS s'il n'existe pas"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    logs_path = "performance_logs/performance_log.csv"
    blob = bucket.blob(logs_path)

    if not blob.exists():
        print(f"Creating new performance log file in gs://{bucket_name}/{logs_path}")
        with open("/tmp/performance_log.csv", "w", newline="") as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(["function", modification_name])
        try:
            blob.upload_from_filename("/tmp/performance_log.csv")
            print(f"Successfully created performance log file in gs://{bucket_name}/{logs_path}")
        except Exception as e:
            print(f"Error creating performance log file: {e}")
            raise

def log_time(times, modification_name, bucket_name):
    try:
        initialize_csv(modification_name, bucket_name)
        temp_file = "/tmp/performance_log.csv"
        logs_path = "performance_logs/performance_log.csv"
        gcs_path = f"gs://{bucket_name}/{logs_path}"

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(logs_path)

        if blob.exists():
            blob.download_to_filename(temp_file)
        else:
            print(f"Warning: Could not find existing log file at {gcs_path}")
            with open(temp_file, "w", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["function", modification_name])

        with open(temp_file, "r") as csvfile:
            reader = csv.reader(csvfile)
            rows = list(reader)

        if modification_name not in rows[0]:
            rows[0].append(modification_name)
            col_index = len(rows[0]) - 1
        else:
            col_index = rows[0].index(modification_name)

        for func_name, elapsed_time in times.items():
            elapsed_time_str = f"{elapsed_time}"
            found_row = False
            for row in rows[1:]:
                if row[0] == func_name:
                    if len(row) <= col_index:
                        row.extend([""] * (col_index + 1 - len(row)))
                    row[col_index] = elapsed_time_str
                    found_row = True
                    break
            if not found_row:
                new_row = [func_name] + [""] * (col_index - 1) + [elapsed_time_str]
                rows.append(new_row)

        with open(temp_file, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(rows)

        blob.upload_from_filename(temp_file)
        print(f"Performance log successfully updated and saved to: {gcs_path}")

        print("\nCurrent performance log content:")
        print("-" * 50)
        for row in rows:
            print(",".join(row))
        print("-" * 50)

    except Exception as e:
        print(f"Error in log_time function: {e}")
        raise

def parse_input_format(input_string):
    """
    Détermine le format de l'entrée et renvoie le type approprié
    """
    try:
        if " to " in input_string:
            start_str, end_str = input_string.split(" to ")
            if len(start_str) == 7 and len(end_str) == 7:  # Format YYYY-MM
                return "monthly", input_string
            elif len(start_str) == 10 and len(end_str) == 10:  # Format YYYY-MM-DD
                return "daily", input_string
        elif len(input_string) == 7 and input_string.count('-') == 1:
            return "monthly", input_string
        elif len(input_string) == 10 and input_string.count('-') == 2:
            return "daily", input_string

        return "custom", input_string
    except Exception as e:
        print(f"Error parsing input format: {e}")
        return "custom", input_string


def get_uris(input_value, bucket_name):
    """
    Génère les URIs source et destination en gérant les différents formats d'entrée
    Les fichiers 'jours' existent seulement pour le mois de janvier2009 et sont stockés dans le bucket dans daily/2009-01
    Pour les fichiers input, il est possible de donner soit:
     - le nom du fichier
     - soit la date du mois ou jour (exemple: 2009-01 ou 2009-01-01)
     - soit un intervalle entre 2 dates (exemple: "2009-01 to 2009-03 ou 2009-01-01 to 2009-01-15)
    """
    input_type, formatted_input = parse_input_format(input_value)

    def get_path_by_type(date_str, input_type):
        """Helper function to get the correct path based on input type"""
        if input_type == "daily":
            return f"taxi_data/daily/2009-01/yellow_tripdata_{date_str}"
        else:
            # Pour les données mensuelles
            return f"taxi_data/yellow_tripdata_{date_str}"

    if input_type == "monthly" or input_type == "daily":
        if " to " in formatted_input:
            # Vérifie si c'est un intervalle de dates
            start_date_str, end_date_str = formatted_input.split(" to ")

            if input_type == "daily":
                start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
                end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

                # Vérifie si les dates sont en 'jours'
                if start_date.year != 2009 or start_date.month != 1 or \
                   end_date.year != 2009 or end_date.month != 1:
                    raise ValueError("Les données quotidiennes ne sont disponibles que pour janvier 2009")

                date_format = "%Y-%m-%d"
            else:  # format mois
                start_date = datetime.strptime(start_date_str, "%Y-%m")
                end_date = datetime.strptime(end_date_str, "%Y-%m")
                date_format = "%Y-%m"

            uris = []
            current_date = start_date

            while current_date <= end_date:
                date_str = current_date.strftime(date_format)
                file_path = get_path_by_type(date_str, input_type)

                raw_uri = f"gs://{bucket_name}/raw/{file_path}.parquet"
                staging_uri = f"gs://{bucket_name}/staging/{file_path}"
                uris.append((raw_uri, staging_uri))

                if input_type == "daily":
                    current_date += timedelta(days=1)
                else:
                    if current_date.month == 12:
                        current_date = current_date.replace(year=current_date.year + 1, month=1)
                    else:
                        current_date = current_date.replace(month=current_date.month + 1)
            return uris
        else:
            if input_type == "daily":
                date = datetime.strptime(formatted_input, "%Y-%m-%d")
                if date.year != 2009 or date.month != 1:
                    raise ValueError("Les données quotidiennes ne sont disponibles que pour janvier 2009")

            file_path = get_path_by_type(formatted_input, input_type)
            raw_uri = f"gs://{bucket_name}/raw/{file_path}.parquet"
            staging_uri = f"gs://{bucket_name}/staging/{file_path}"
            return [(raw_uri, staging_uri)]
    else:
        file_path = get_path_by_type(formatted_input, "monthly")
        raw_uri = f"gs://{bucket_name}/raw/{file_path}.parquet"
        staging_uri = f"gs://{bucket_name}/staging/{file_path}"
        return [(raw_uri, staging_uri)]


def staging_pipeline(spark, raw_uri: str, staging_uri: str) -> dict:
    print(f"\nProcessing file:")
    print(f"Reading from: {raw_uri}")
    print(f"Writing to: {staging_uri}")

    start_time = timeit.default_timer()
    df = spark.read.parquet(raw_uri)
    read_time = timeit.default_timer() - start_time
    print(f"read_parquet took {read_time} seconds")

    transform_start = timeit.default_timer()
    df = remove_duplicates(df)
    df = handle_nulls(df)
    df = type_casting(df)
    df = normalize_strings(df)
    df = format_dates(df)
    df = filter_coordinates(df)
    df = calculate_trip_duration(df)
    df = calculate_haversine_distance(df)
    df = rename_columns(df)
    transform_time = timeit.default_timer() - transform_start
    print(f"Total transformations took {transform_time} seconds")

    start_time = timeit.default_timer()
    df.write.parquet(staging_uri, mode="overwrite", compression="snappy")
    write_time = timeit.default_timer() - start_time
    print(f"write_parquet took {write_time} seconds")

    return {
        "read_parquet": read_time,
        "transformations": transform_time,
        "write_parquet": write_time
    }

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Preprocess the raw data and save it to the staging area in GCS"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input identifier (e.g., '2009-01' for single month, '2009-01 to 2009-03' for range)"
    )
    parser.add_argument("--bucket", required=True, help="GCS bucket name")
    parser.add_argument("modification", help="Description of the modification made")
    parser.add_argument("--save-local", action="store_true", help="Save a local copy of the performance log")
    args = parser.parse_args()

    job_start_time = timeit.default_timer()

    start_time = timeit.default_timer()
    spark = get_spark_session()
    spark_time = timeit.default_timer() - start_time
    print(f"get_spark_session took {spark_time} seconds")

    uri_pairs = get_uris(args.input, args.bucket)

    cumulative_times = {
        "read_parquet": 0,
        "transformations": 0,
        "write_parquet": 0
    }

    for raw_uri, staging_uri in uri_pairs:
        times = staging_pipeline(spark, raw_uri, staging_uri)
        for key in cumulative_times:
            cumulative_times[key] += times[key]

    cumulative_times["get_spark_session"] = spark_time
    cumulative_times["total_job_time"] = timeit.default_timer() - job_start_time

    log_time(cumulative_times, args.modification, args.bucket)

    if args.save_local:
        local_path = "performance_log.csv"
        storage_client = storage.Client()
        bucket = storage_client.bucket(args.bucket)
        blob = bucket.blob("performance_logs/performance_log.csv")
        blob.download_to_filename(local_path)
        print(f"\nLocal copy saved to: {local_path}")

    print(f"\nTotal job time: {cumulative_times['total_job_time']} seconds")

if __name__ == "__main__":
    main()
