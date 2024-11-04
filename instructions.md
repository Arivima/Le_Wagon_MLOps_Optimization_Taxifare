## New york taxis

### Outline 🎯

Your goal is to build a pipeline in spark that will do the following.

1. Take a parquet from [New York City Taxi and Limousine Commission website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (we'll be using the Yellow Taxi Trip records) for a given month and upload it to the raw folder in your bucket.
2. Perform some basic data cleaning and add some additional columns and store it to the staging area of your bucket.
3. Create two processed versions of the data one for a data analyst and one for a data scientist.
4. Implement the training of the the model and save that to the bucket for your data scientist.
5. Orchestrate the pipeline using airflow to automatically submit the jobs to dataproc.

Sounds like a lot but we will build it out step by step

### Setup

❓ First duplicate the `.env.sample` file and rename it to `.env`, fill in the values and `direnv reload` to load env
variables. The bucket can be a new one just for this exercise! Then create the bucket as well.

<details>
<summary markdown='span'>💡 Reminder</summary>
To create new bucket:

```bash
gsutil mb -l eu gs://$BUCKET_NAME
```

</details>

Next we will need to add add an extension to spark in order to allow us to work with google cloud storage.

```bash
wget -P ~/spark/jars https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
```

Next checkout the file `taxi_spark/functions/session` to see how we use the extension

Why do you think we don't need the extension when we are not `LOCAL`?

### 1. Download the data

This gets us one month of data

```bash
wget -P data https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-01.parquet
```

You can test the df with the notebook in `notebooks/test.ipynb` for spark it is strongly recommended to develop in a
notebook. To make it easier to tweak and rerun your transformations!

### 2. Check schema

The first function you need to implement is `check_schema` in `taxi_spark/functions/schema.py` this function should
check that the schema of the dataframe is correct. If it is not correct it should raise an error. For the exercise you
can presume that the schema you get from 2009 01 is valid.

Once you have created the function you can check it with

```bash
pytest tests/test_taxi_spark/test_functions/test_schema.py
```

### 3. Write the write_to_raw job

Now it is time to create the first job which should

- Download the data of the date
- Check the schema
- Write the data to the raw folder in the bucket

❓ Checkout the code in `taxi_spark/jobs/write_to_raw.py` and implement the missing parts.

To test the job run it with

```bash
python taxi_spark/jobs/write_to_raw.py --bucket=$BUCKET_NAME --date=2009-01
```

Check everything looks good in your bucket!

### 4. Submit the job

Now we want to submit the job to dataproc. To do this we will use the `gcloud` command line tool. There is one argument
that is quite difficult to find to allow us to make the external request as normally the cluster is not allowed to make
external requests.

Setup the network to allow external requests

```bash
gcloud compute networks create spark-vpc --subnet-mode=auto
```

```bash
gcloud compute routers create spark-router --network=spark-vpc --region=europe-west1
```

```bash
gcloud compute networks subnets update spark-vpc \
    --region=europe-west1 \
    --enable-private-ip-google-access
```

```bash
gcloud compute firewall-rules create allow-internal \
    --network=spark-vpc \
    --allow tcp,udp,icmp \
    --source-ranges=10.128.0.0/9
```

```bash
gcloud compute routers nats create spark-nat \
    --router=spark-router \
    --auto-allocate-nat-external-ips \
    --nat-all-subnet-ip-ranges \
    --enable-logging \
    --region=europe-west1
```

Quite a lot of steps 😅 but now we can create the cluster to submit our jobs to  using

```bash
make create-cluster
```

❓ Implement the rest of the command in the Makefile under `submit-write-to-raw` to submit the job to your dataproc cluster!

<details>
<summary markdown='span'>💡 Hint</summary>

Think about how to get your files from local to available in a bucket

</details>

<details>
<summary markdown='span'>💡 Submit solution</summary>

```bash
submit-write-to-raw:
	$(eval WHEEL_NAME=$(shell poetry build -f wheel --no-ansi 2>&1 | awk '/Built/ {print $$3}'))
	gsutil cp "dist/$(WHEEL_NAME)" "gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)"
	gcloud dataproc jobs submit pyspark \
		--cluster=$(CLUSTER_NAME) \
		--project=$(PROJECT_ID) \
		--region=europe-west1 \
		--py-files="gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)" \
		taxi_spark/jobs/write_to_raw.py -- "--date=2009-01" "--bucket=$(BUCKET_NAME)"
```


</details>


### 5. Write the functions for cleaning the data

There are nine functions you need to implement in `taxi_spark/functions/cleaning.py`
and `taxi_spark/functions/calculations.py` follow the instruction in the doc strings. Remember to test using a notebook
as you go!

### 6. Implement your own tests

This time you need to implement your own tests for your functions once you are done you can run make test to check that
everything is working as expected. You should end with an output like this

```
PYTHONDONTWRITEBYTECODE=1 pytest tests -m "not optional" -v --disable-warnings --color=yes
================================================================================================================================================================================================================================================= test session starts ==================================================================================================================================================================================================================================================
platform linux -- Python 3.8.14, pytest-7.4.2, pluggy-1.3.0 -- /home/oliver.giles/code/lewagon_dev/data-engineering-solutions/04-Data-at-Scale/02-Spark-Advanced/01-NYC-Taxis/.venv/bin/python
cachedir: .pytest_cache
rootdir: /home/oliver.giles/code/lewagon_dev/data-engineering-solutions/04-Data-at-Scale/02-Spark-Advanced/01-NYC-Taxis
configfile: pyproject.toml
plugins: anyio-4.0.0, mock-3.11.1
collected 10 items

tests/test_taxi_spark/test_functions/test_calculations.py::test_calculate_trip_duration PASSED                                                                                                                                                                                                                                                                                                                                                                                                                   [ 10%]
tests/test_taxi_spark/test_functions/test_calculations.py::test_calculate_haversine_distance PASSED                                                                                                                                                                                                                                                                                                                                                                                                              [ 20%]
tests/test_taxi_spark/test_functions/test_cleaning.py::test_remove_duplicates PASSED                                                                                                                                                                                                                                                                                                                                                                                                                             [ 30%]
tests/test_taxi_spark/test_functions/test_cleaning.py::test_handle_nulls PASSED                                                                                                                                                                                                                                                                                                                                                                                                                                  [ 40%]
tests/test_taxi_spark/test_functions/test_cleaning.py::test_type_casting PASSED                                                                                                                                                                                                                                                                                                                                                                                                                                  [ 50%]
tests/test_taxi_spark/test_functions/test_cleaning.py::test_normalize_strings PASSED                                                                                                                                                                                                                                                                                                                                                                                                                             [ 60%]
tests/test_taxi_spark/test_functions/test_cleaning.py::test_format_dates PASSED                                                                                                                                                                                                                                                                                                                                                                                                                                  [ 70%]
tests/test_taxi_spark/test_functions/test_cleaning.py::test_filter_coordinates PASSED                                                                                                                                                                                                                                                                                                                                                                                                                            [ 80%]
tests/test_taxi_spark/test_functions/test_cleaning.py::test_rename_columns PASSED                                                                                                                                                                                                                                                                                                                                                                                                                                [ 90%]
tests/test_taxi_spark/test_functions/test_schema.py::test_enforce_schema PASSED                                                                                                                                                                                                                                                                                                                                                                                                                                  [100%]

================================================================================================================================================================================================================================================= 10 passed in 16.97s ==================================================================================================================================================================================================================================================

```

### 7. Implement and submit

Finish off `staging_transform.py` and add `make-submit-staging-transform` to the Makefile!
To try the job run it with

```bash
python taxi_spark/jobs/staging_transform.py --bucket=$BUCKET_NAME --date=2009-01
```

### 8. Processing job

At this point you know the drill you need to implement `processing.py` and `ml.py` and then combine together to
make `create_processed.py` finally submitting the job!
To try the job run it with

```bash
python taxi_spark/jobs/create_processed.py --bucket=$BUCKET_NAME --date=2009-01
```

### 9. Orchestrate

Now we want to Orchestrate the whole process with airflow you can start it with

```bash
docker compose up
```

Then you can implement the dag in the dags folder!

The operators you will need
are from https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/dataproc/index.html


### Finish line 🏁

If you have come this far you have implemented a full data pipeline in spark! Congratulations 🎉

### Bonus

You can try and run the dag for many months and then create a new aggregation across an entire year of data!
