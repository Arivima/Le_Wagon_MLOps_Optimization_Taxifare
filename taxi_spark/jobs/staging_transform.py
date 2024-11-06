import timeit
import csv
import argparse
import os
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

def initialize_csv(modification_name):
    file_exists = os.path.isfile("performance_log.csv")
    if not file_exists:
        with open("performance_log.csv", "w", newline="") as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(["function", modification_name])

def log_time(cumulative_times, modification_name):
    initialize_csv(modification_name)

    with open("performance_log.csv", "r") as csvfile:
        reader = csv.reader(csvfile)
        rows = list(reader)

    if modification_name not in rows[0]:
        rows[0].append(modification_name)
        col_index = len(rows[0]) - 1
    else:
        col_index = rows[0].index(modification_name)

    for func_name, elapsed_time in cumulative_times.items():
        for row in rows[1:]:
            if row[0] == func_name:
                if len(row) <= col_index:
                    row.extend([""] * (col_index + 1 - len(row)))
                row[col_index] = elapsed_time
                break
        else:
            new_row = [func_name] + [""] * (col_index - 1) + [elapsed_time]
            rows.append(new_row)

    with open("performance_log.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(rows)

def timeit_stage(func, df, cumulative_times):
    start_time = timeit.default_timer()
    result = func(df)
    elapsed_time = timeit.default_timer() - start_time
    cumulative_times[func.__name__] += elapsed_time
    print(f"{func.__name__} took {elapsed_time:.4f} seconds")
    return result

def staging_pipeline(raw_uri: str, staging_uri: str, cumulative_times: dict) -> None:
    start_time = timeit.default_timer()
    spark = get_spark_session()
    elapsed_time = timeit.default_timer() - start_time
    cumulative_times["get_spark_session"] += elapsed_time
    print(f"get_spark_session took {elapsed_time:.4f} seconds")

    start_time = timeit.default_timer()
    df = spark.read.parquet(raw_uri)
    elapsed_time = timeit.default_timer() - start_time
    cumulative_times["read_parquet"] += elapsed_time
    print(f"read_parquet took {elapsed_time:.4f} seconds")

    df = timeit_stage(remove_duplicates, df, cumulative_times)
    df = timeit_stage(handle_nulls, df, cumulative_times)
    df = timeit_stage(type_casting, df, cumulative_times)
    df = timeit_stage(normalize_strings, df, cumulative_times)
    df = timeit_stage(format_dates, df, cumulative_times)
    df = timeit_stage(filter_coordinates, df, cumulative_times)
    df = timeit_stage(calculate_trip_duration, df, cumulative_times)
    df = timeit_stage(calculate_haversine_distance, df, cumulative_times)
    df = timeit_stage(rename_columns, df, cumulative_times)

    start_time = timeit.default_timer()
    df.write.parquet(staging_uri, mode="overwrite")
    elapsed_time = timeit.default_timer() - start_time
    cumulative_times["write_parquet"] += elapsed_time
    print(f"write_parquet took {elapsed_time:.4f} seconds")

def parse_date_range(date_range):
    start_date_str, end_date_str = date_range.split(" to ")

    if len(start_date_str) == 7 and len(end_date_str) == 7:  # Format "yyyy-MM"
        start_date = datetime.strptime(start_date_str, "%Y-%m")
        end_date = datetime.strptime(end_date_str, "%Y-%m")
        return start_date, end_date, "monthly"
    elif len(start_date_str) == 10 and len(end_date_str) == 10:  # Format "yyyy-MM-dd"
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        return start_date, end_date, "daily"
    else:
        raise ValueError("Date format must be either 'yyyy-MM' or 'yyyy-MM-dd'.")

def generate_monthly_dates(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        yield current_date.strftime("%Y-%m")
        current_date += timedelta(days=32)
        current_date = current_date.replace(day=1)

def generate_daily_dates(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        yield current_date.strftime("%Y-%m-%d")
        current_date += timedelta(days=1)

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Preprocess the raw data and save it to the staging area in GCS"
    )
    parser.add_argument("modification", help="Description of the modification made")
    parser.add_argument("--date", required=True, help="Date in the format 'yyyy-MM' or 'yyyy-MM-dd to yyyy-MM-dd'")
    parser.add_argument("--bucket", required=True, help="GCS bucket name")
    args = parser.parse_args()

    cumulative_times = {
        "get_spark_session": 0,
        "read_parquet": 0,
        "remove_duplicates": 0,
        "handle_nulls": 0,
        "type_casting": 0,
        "normalize_strings": 0,
        "format_dates": 0,
        "filter_coordinates": 0,
        "calculate_trip_duration": 0,
        "calculate_haversine_distance": 0,
        "rename_columns": 0,
        "write_parquet": 0,
    }

    if " to " in args.date:
        start_date, end_date, date_type = parse_date_range(args.date)

        if date_type == "monthly":
            dates = list(generate_monthly_dates(start_date, end_date))
        elif date_type == "daily":
            dates = list(generate_daily_dates(start_date, end_date))
    else:
        dates = [args.date]

    for date in dates:
        if len(date) == 7:  # Format mensuel "yyyy-MM"
            raw_uri = f"gs://{args.bucket}/raw/taxi_data/yellow_tripdata_{date}.parquet"
            staging_uri = f"gs://{args.bucket}/staging/taxi_data/yellow_tripdata_{date}"
        elif len(date) == 10:  # Format quotidien "yyyy-MM-dd"
            month_folder = date[:7]  # Extraire le mois "yyyy-MM" de la date journalière
            raw_uri = f"gs://{args.bucket}/raw/taxi_data/daily/{month_folder}/yellow_tripdata_{date}.parquet"
            staging_uri = f"gs://{args.bucket}/staging/taxi_data/daily/{month_folder}/yellow_tripdata_{date}"

        print(f"Processing data for {date}")
        staging_pipeline(raw_uri, staging_uri, cumulative_times)

    log_time(cumulative_times, args.modification)

if __name__ == "__main__":
    main()
