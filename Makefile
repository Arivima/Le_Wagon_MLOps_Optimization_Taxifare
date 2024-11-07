include .env

.PHONY: create_bucket switch_to_new_service_account create-cluster destroy-cluster submit-write-to-raw submit-staging-transform submit-create-processed test

UUID := $(shell uuidgen | tr '[:upper:]' '[:lower:]')

test: pytest-and-write-output

create_bucket:
	gcloud storage buckets create \
    	"gs://$(BUCKET_NAME)" \
    	--location=$(BUCKET_LOCATION) \
    	--project=$(GCP_PROJECT_ID)

switch_to_new_service_account:
	gcloud auth activate-service-account --key-file=$(PATH_JSON)

create-cluster:
	gcloud dataproc clusters create $(CLUSTER_NAME) \
		--project=$(PROJECT_ID) \
		--region=europe-west1 \
		--network=spark-vpc \
		--master-machine-type e2-standard-2 \
		--master-boot-disk-size 50 \
		--num-workers 2 \
		--worker-machine-type e2-standard-2 \
		--worker-boot-disk-size 50

destroy-cluster:
	gcloud dataproc clusters delete $(CLUSTER_NAME) \
		--project=$(PROJECT_ID) \
		--region=europe-west1 \
		--quiet

# submit-write-to-raw:
# 	$(eval WHEEL_NAME=$(shell poetry build -f wheel --no-ansi 2>&1 | awk '/Built/ {print $$3}'))
# 	gsutil cp "dist/$(WHEEL_NAME)" "gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)"
# 	gcloud dataproc jobs submit pyspark \
# 		--cluster=$(CLUSTER_NAME) \
# 		--project=$(PROJECT_ID) \
# 		--region=europe-west1 \
# 		--py-files="gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)" \
# 		taxi_spark/jobs/write_to_raw.py -- "--date=2009-01" "--bucket=$(BUCKET_NAME)"


submit-staging-transform:
	# Construire le wheel et extraire son nom
	$(eval WHEEL_NAME=$(shell poetry build -f wheel --no-ansi 2>&1 | awk '/Built/ {print $$3}'))
	# Copier le wheel dans le bucket GCS
	gsutil cp "dist/$(WHEEL_NAME)" "gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)"
	# Soumettre le job PySpark sur Dataproc avec les arguments dynamiques
	gcloud dataproc jobs submit pyspark \
		--cluster=$(CLUSTER_NAME) \
		--project=$(PROJECT_ID) \
		--region=europe-west1 \
		--py-files="gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)" \
		taxi_spark/jobs/staging_transform.py -- \
		"--date=$(DATE)" \
		"--bucket=$(BUCKET_NAME)" \
		"$(COM)" \
		--save-local
	sleep 5
	# Télécharger le fichier de performance en local
	gsutil cp gs://$(BUCKET_NAME)/performance_logs/performance_log.csv ./



# submit-create-processed:
# 	$(eval WHEEL_NAME=$(shell poetry build -f wheel --no-ansi 2>&1 | awk '/Built/ {print $$3}'))
# 	gsutil cp "dist/$(WHEEL_NAME)" "gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)"
# 	gcloud dataproc jobs submit pyspark \
# 		--cluster=$(CLUSTER_NAME) \
# 		--project=$(PROJECT_ID) \
# 		--region=europe-west1 \
# 		--py-files="gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)" \
# 		taxi_spark/jobs/create_processed.py -- "--date=2009-01" "--bucket=$(BUCKET_NAME)"
