# NYC Taxi Data Analysis

The goal is to analyze NYC green and yellow taxi data for the years 2019 and 2020, along with For-Hire Vehicle (FHV) data for 2019. The analysis includes loading data to Google Storage, creating external and native BigQuery tables, and using dbt to create models, seeds, fact tables, dimensions, and data marts. Google Looker Studio is used to create reports comparing total trips for each month in 2019  for green taxi, yellow taxi, and FHV trip data.

## Data Loading

To load the data, use the provided Python script `csv_from_web_to_parquet_gcs.py` with the following examples:

###  Load Green , Yellow Taxi Data
```bash
python3 csv_from_web_to_parquet_gcs.py --color green --year 2019
python3 csv_from_web_to_parquet_gcs.py --color green --year 2020
python3 csv_from_web_to_parquet_gcs.py --color yellow --year 2019
python3 csv_from_web_to_parquet_gcs.py --color yellow --year 2020
```
### Load FHV Trips Data
```bash
python3 csv_from_web_to_parquet_gcs.py --color fhv --year 2019
```
## BigQuery Tables
Create datasets and tables in BigQuery using the following SQL commands:
```sql
CREATE SCHEMA IF NOT EXISTS `mage-zoomcamp.nyc_taxi_all`;

CREATE OR REPLACE EXTERNAL TABLE
  `mage-zoomcamp.nyc_taxi_all.green-taxi-external` OPTIONS (
    format = 'PARQUET',
    uris = ['gs://nyc_taxi_data_trip/green/2019/*.parquet', 'gs://nyc_taxi_data_trip/green/2020/*.parquet']
  );
CREATE OR REPLACE TABLE
  `mage-zoomcamp.nyc_taxi_all.green-taxi` AS (
  SELECT
    *
  FROM
    `nyc_taxi_all.green-taxi-external`
);

CREATE OR REPLACE EXTERNAL TABLE
  `mage-zoomcamp.nyc_taxi_all.yellow-taxi-external` OPTIONS (
    format = 'PARQUET',
    uris = ['gs://nyc_taxi_data_trip/yellow/2019/*.parquet', 'gs://nyc_taxi_data_trip/yellow/2020/*.parquet']
  );
CREATE OR REPLACE TABLE
  `mage-zoomcamp.nyc_taxi_all.yellow-taxi` AS (
  SELECT
    *
  FROM
    `mage-zoomcamp.nyc_taxi_all.yellow-taxi-external`
);
CREATE OR REPLACE EXTERNAL TABLE
  `mage-zoomcamp.nyc_taxi_all.fhv-taxi-external` OPTIONS (
    format = 'PARQUET',
    uris = ['gs://nyc_taxi_data_trip/fhv/2019/*.parquet']
  );
CREATE OR REPLACE TABLE
  `mage-zoomcamp.nyc_taxi_all.fhv-taxi` AS (
  SELECT
    *
  FROM
    `mage-zoomcamp.nyc_taxi_all.fhv-taxi-external`
);
```
## dbt Modeling

Use dbt Cloud and Cloud IDE to create models, fact tables, dimension tables, and data marts. All dbt code is available [here](https://github.com/mminabian/datacamp2024/tree/main/04-analytics-engineering/taxi_ride_ny).

**Development Environment:**
- In the development environment, the `is_test_run` variable is defined with a default value of `true`.
- When running `dbt build` during development, it will use only 100 records from the sources table to expedite the process.

**Production Environment:**
- For production runs, it is essential to run dbt on the entire dataset.
- When running `dbt build` for production, pass the following variable to ensure processing all records:
  ```bash
  --vars '{"is_test_run": "false"}'
A nightly job is configured to run dbt every night at 12.
Additionally, a continuous integration job is set up to automatically trigger `dbt build` when a pull request is submitted. If all checks pass successfully, it allows the merge process to proceed. This setup is crucial for preventing potential production disruptions in case of any code issues.
