# Mage Project 2 

The goal is to construct an ETL pipeline that loads data, performs transformations, and writes the data to a database (and Google Cloud!).

1. Create a new pipeline and name it `green_taxi_etl`.
2. Add a data loader block and use Pandas to read data for the final quarter of 2020 (months 10, 11, 12).
   - Utilize the same datatypes and date parsing methods demonstrated in the course.
   - Bonus: Load the final three months using a for loop and `pd.concat`.
3. Add a transformer block and perform the following:
   - Remove rows where the passenger count is equal to 0 or the trip distance is equal to zero.
   - Create a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date.
   - Rename columns in Camel Case to Snake Case, e.g., `VendorID` to `vendor_id`.
   - Add three assertions:
     - `vendor_id` is one of the existing values in the column (currently).
     - `passenger_count` is greater than 0.
     - `trip_distance` is greater than 0.
4. Using a Postgres data exporter (SQL or Python), write the dataset to a table called `green_taxi` in a schema `mage`. Replace the table if it already exists.
5. Write your data as Parquet files to a bucket in GCP, partitioned by `lpep_pickup_date`. Use the pyarrow library!
6. Schedule your pipeline to run daily at 5 AM UTC.

**Instructions:**

- Clone this repository to your local machine.
- Navigate to the repository: `cd mage-data-engineering-zoomcamp`.
- Rename `dev.env` to `.env`.
- Build the Docker container: `docker compose build`.
- Start the Docker container: `docker compose up`.
- Open your browser and navigate to [http://localhost:6789](http://localhost:6789).
- You're now ready to get started.

**Requirements:**

- Create a service account in your Google Cloud with the following roles:
  - Artifact Registry Reader
  - Artifact Registry Writer
  - Cloud Run Developer
  - Cloud SQL Admin
  - Service Account Token Creator
- Add a key for the service account and download the key JSON file, put the key in your-path-to-repo/datacamp2024/02-workflow-orchestration-mage/mage-zoomcamp folder.

# Solution:

- The dev profile is added to `io_config.yaml` to read PostgreSQL config from the `.env` file.
- An ETL pipeline named `green_taxi_etl` is created to load the data, perform transformations, and write the data to a database and google cloud storage. 
- This pipeline has the following data loader:
  - `load_api_green_taxi_data.py` to get green taxi data from the source URL.
  - `test_postgres.sql` to test connection to PostgreSQL.
  - `load_green_taxi_data_from_postgres.sql`, which can be used later to get `vendor_id` from `green_taxi` in PostgreSQL.
- The pipeline has the `transformer_green_taxi_data.py` to perform the required transformation in this assignment on green taxi data.
- The pipeline has two data exporters:
  - `green_taxi_data_to_postgres.py` to export the transformed data to PostgreSQL.
  - `green_taxi_data_to_gcs_partioned_parquet.py` to write transformed green taxi data as Parquet files to a bucket in GCP, partitioned by `lpep_pickup_date`.
  
**Required modifications before running the pipeline:**

Do the following changes in `green_taxi_data_to_gcs_partioned_parquet.py`:
- `os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/gcp-key.json"` replace `gcp-key.json` with your service account key name.
- `bucket_name = 'mage-zoomcamp-mahmoud'` replace with your bucket name.
- `project_id= 'mage-zoomcamp'` replace with your project id.

# Run the green_taxi_etl pipeline:

1. Ensure that you have run `docker compose up` and that the Mage container is running.
2. Open your browser and navigate to [http://localhost:6789](http://localhost:6789).
3. In the left side menu, go to Pipelines, click on the `green-taxi-etl` pipeline, and then select `edit pipeline`. In the middle, you can see all data loader, transformer, and data exporters. Scroll down until you reach the `green_taxi_data_to_gcs_partioned_parquet` data exporter window. On the top left window of this data exporter, click on the more action icon (circle icon with ... in it), then click on execute with all upstream blocks. This will run all blocks in this pipeline.
