
# Google BigQuery
## Project Goal
The goal of this project is to obtain files for Green Taxi Trip Data for the year 2022 from the New York City Taxi Data, upload them to Google Cloud Storage, create tables in BigQuery, and execute queries on those tables.

## Data Source
For this project, we will be using the 2022 Green Taxi Trip Record Parquet Files from the New York City Taxi Data found [here](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

**NOTE:** You will need to use the PARQUET option files when creating an External Table.

## Setup
1. Create an external table using the Green Taxi Trip Records Data for 2022.
2. Create a table in BigQuery (BQ) using the Green Taxi Trip Records for 2022.

## Solution
### Requirements:
- Clone repo
- Put your service account key in `path_to_repo/datacamp2024/03-data-warehouse`
- Run `pip install requirments.txt`

### Running the Script
To put Green Taxi data for the year 2022 in Google Cloud Storage, use the script `from_web_to_gcs.py`. This script accepts the following parameters:
- `--bucket_name`: Google Cloud Storage bucket name (default: "nyc-taxi-2022")
- `--base_url`: Base URL NYC Taxi data (default: "https://d37ci6vzurychx.cloudfront.net/trip-data/")
- `--output_gcs_path`: Path to the output folder in Google Cloud Storage (default: "green-taxi")
- `--service_account_key_path`: Path to the Google Cloud service account key JSON file (default: "gcp-key.json")
- `--prefix`: Prefix for the file names including the year of data (default: "green_tripdata_2022")

Run the script:
```bash
python3 from_web_to_gcs.py --bucket_name your_bucket_name --output_gcs_path path_to_output_folder --service_account_key_path path_to_your_service_account_json_key
```
## BigQuery Operations
1. Create a dataset on BigQuery:
```sql
CREATE SCHEMA IF NOT EXISTS `mage-zoomcamp.nyc_taxi_2022`;
```
2. Create an external table using the Green Taxi Trip Records Data for 2022:
```sql
CREATE OR REPLACE EXTERNAL TABLE
  `mage-zoomcamp.nyc_taxi_2022.green-taxi-external` OPTIONS (
    format = 'PARQUET',
    uris = ['gs://nyc-taxi-2022/green-taxi/*.parquet']
  );

 ```
3. Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table):
```sql
CREATE OR REPLACE TABLE
  `mage-zoomcamp.nyc_taxi_2022.green-taxi-non-partitioned` AS (
  SELECT
    *
  FROM
    `nyc_taxi_2022.green-taxi-external`
);
 ```
### Queries
 Question 1: What is the count of records for the 2022 Green Taxi Data?
 840,402
External Table
```sql
SELECT COUNT(*) FROM `nyc_taxi_2022.green-taxi-external`;
 ```
Non-Partitioned Table:
```sql
SELECT COUNT(*) FROM `nyc_taxi_2022.green-taxi-non-partitioned`;
```
Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
External Table:
```sql
SELECT COUNT(DISTINCT PULocationID) FROM `nyc_taxi_2022.green-taxi-external`;
```
Non-Partitioned Table:
```sql
SELECT COUNT(DISTINCT PULocationID) FROM `nyc_taxi_2022.green-taxi-non-partitioned`;

```
0 for external and 6.41  Mb for table

Question 3: How many records have a fare_amount of 0?
1622
External Table:
```sql
SELECT COUNT(*) FROM `nyc_taxi_2022.green-taxi-external` WHERE fare_amount = 0;
```
Non-Partitioned Table:
```sql
SELECT COUNT(*) FROM `nyc_taxi_2022.green-taxi-non-partitioned` WHERE fare_amount = 0;

```
Question4: What is the best strategy for an optimized table in BigQuery if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime?
Create a new table with this strategy.

Partition by: lpep_pickup_datetime
Cluster on: PUlocationID
```sql
CREATE OR REPLACE TABLE `nyc_taxi_2022.green-taxi-partitioned-clustered`
 PARTITION BY DATE(lpep_pickup_datetime)
 CLUSTER BY PUlocationID
 AS SELECT * FROM `nyc_taxi_2022.green-taxi-external`;
```
Question 5: Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?
12.82 MB for non-partitioned table and 1.12 MB for the partitioned 
```sql
SELECT distinct PULocationID
FROM `nyc_taxi_2022.green-taxi-partitioned-clustered`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT DISTINCT PULocationID
FROM `nyc_taxi_2022.green-taxi-non-partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

```
Question 6: Where is the data stored in the External Table you created?
GCP Bucket
Question 7: Is it best practice in BigQuery to always cluster your data?
False