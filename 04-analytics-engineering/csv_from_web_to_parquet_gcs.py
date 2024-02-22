import argparse
import urllib
from pathlib import Path
import pandas as pd
from google.cloud import storage
import pyarrow.parquet as pq
import os
import pyarrow as pa
def generate_dataset_url(color, year, month):
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    return f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"


#
#
def process_color_chunk(chunk: pd.DataFrame, color: str) -> pd.DataFrame:
    """Process DataFrame chunk based on color"""
    # Make all column names lowercase for case-insensitive matching
    # chunk.columns = map(str.lower, chunk.columns)

    # print(chunk.head(10))
    if color == "fhv":
        chunk.columns = [col.lower() if 'datetime' in col else col for col in chunk.columns]
        if 'PUlocationID' in chunk.columns:
            chunk.rename(columns={'PUlocationID': 'PULocationID'}, inplace=True)
        if 'DOlocationID' in chunk.columns:
            chunk.rename(columns={'DOlocationID': 'DOLocationID'}, inplace=True)
        # Process chunk for 'fhv' color (modify as needed)
        chunk["pickup_datetime"] = pd.to_datetime(chunk["pickup_datetime"])
        chunk["dropoff_datetime"] = pd.to_datetime(chunk["dropoff_datetime"])
        chunk["PULocationID"] = chunk["PULocationID"].astype('Int64')
        chunk["DOLocationID"] = chunk["DOLocationID"].astype('Int64')
        chunk["SR_Flag"] = chunk["SR_Flag"].astype('Int64')
    else:
        # Process chunk for other colors (modify as needed)
        # chunk["vendorid"] = chunk["vendorid"].astype('Int64')
        # chunk["ratecodeid"] = chunk["ratecodeid"].astype('Int64')
        # chunk["pulocationid"] = chunk["pulocationid"].astype('Int64')
        # chunk["dolocationid"] = chunk["dolocationid"].astype('Int64')
        # chunk["passenger_count"] = chunk["passenger_count"].astype('Int64')
        # chunk["payment_type"] = chunk["payment_type"].astype('Int64')
        #
        # if color == "yellow":
        #     chunk["tpep_pickup_datetime"] = pd.to_datetime(chunk["tpep_pickup_datetime"])
        #
        #     chunk["tpep_dropoff_datetime"] = pd.to_datetime(chunk["tpep_dropoff_datetime"])
        # elif color == "green":
        #     chunk["lpep_pickup_datetime"] = pd.to_datetime(chunk["lpep_pickup_datetime"])
        #     chunk["lpep_dropoff_datetime"] = pd.to_datetime(chunk["lpep_dropoff_datetime"])
        #     chunk["trip_type"] = chunk["trip_type"].astype('Int64')
        chunk["VendorID"] = chunk["VendorID"].astype('Int64')
        chunk["RatecodeID"] = chunk["RatecodeID"].astype('Int64')
        chunk["PULocationID"] = chunk["PULocationID"].astype('Int64')
        chunk["DOLocationID"] = chunk["DOLocationID"].astype('Int64')
        chunk["passenger_count"] = chunk["passenger_count"].astype('Int64')
        chunk["payment_type"] = chunk["payment_type"].astype('Int64')
        if color == "yellow":
            # print(chunk.columns.tolist())
            chunk["tpep_pickup_datetime"] = pd.to_datetime(chunk["tpep_pickup_datetime"])
            chunk["tpep_dropoff_datetime"] = pd.to_datetime(chunk["tpep_dropoff_datetime"])
        elif color == "green":
            # print(chunk.columns.tolist())
            chunk["lpep_pickup_datetime"] = pd.to_datetime(chunk["lpep_pickup_datetime"])
            chunk["lpep_dropoff_datetime"] = pd.to_datetime(chunk["lpep_dropoff_datetime"])
            chunk["trip_type"] = chunk["trip_type"].astype('Int64')

    return chunk


def fetch(dataset_url: str, color: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame using chunking"""
    # Define chunk size based on your preference
    chunk_size = 10000

    # Initialize an empty DataFrame to concatenate chunks
    chunks = []
    try:
        encoding = 'unicode_escape'
        if color == 'fhv':
            encoding ='latin1'

        # Read CSV in chunks
        for chunk in pd.read_csv(dataset_url, compression='gzip', chunksize=chunk_size, encoding=encoding):
            processed_chunk = process_color_chunk(chunk, color)
            # print("processed_chunk date for tpep type is ")
            # print(processed_chunk['tpep_pickup_datetime'].dtype)
            chunks.append(processed_chunk)

        # Concatenate all chunks into a single DataFrame
        df = pd.concat(chunks, ignore_index=True)
        # print("date for tpep type is ")
        # print(df['tpep_pickup_datetime'].dtype)
    except urllib.error.HTTPError:
        print(f"url not found{dataset_url}")
        exit()
    return df


def upload_to_gcs(local_path, service_account_path, gcs_bucket_name, color, year):
    # Upload local Parquet file to GCS using service account
    client = storage.Client.from_service_account_json(service_account_path)
    bucket = client.bucket(gcs_bucket_name)

    # Extract the file name from the path
    file_name = Path(local_path).name
    print(f"parquet file name: {file_name}")

    # Set the path based on color, year, and month
    blob = bucket.blob(f"{color}/{year}/{file_name}")
    blob.upload_from_filename(local_path, timeout=600)
    print(f"Uploaded {local_path} to {blob.name}")

def write_df_to_parquet_locally(color, df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""

    # create directory if not exist
    dir = f"data/{color}"
    if not os.path.isdir(dir):
        os.makedirs(dir)

    path = Path(f"{dir}/{dataset_file}.parquet")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path, compression='gzip', use_deprecated_int96_timestamps=True)
    # df.to_parquet(path, engine='pyarrow', compression="gzip")
    # Specify the schema for the Parquet file




    parquet_table = pq.read_table(path)
    # check schema of parquet file
    print(parquet_table.schema)
    return path


def main():
    parser = argparse.ArgumentParser(description="Process and upload NYC TLC data to Google Cloud Storage")
    parser.add_argument("--color", choices=["fhv", "yellow", "green"], required=True, help="Color of the taxi data")
    parser.add_argument("--year", nargs='+', type=int, default=[2019, 2020, 2021],
                        help="List of years for the taxi data")
    parser.add_argument("--month", nargs='+', type=int, default=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                        help="List of months for the taxi data")
    parser.add_argument("--chunk-size", type=int, default=10000, help="Chunk size for processing the CSV file")
    parser.add_argument("--service-account-path", default="gcp-key.json", help="Path to the Google Cloud service "
                                                                               "account JSON file")
    parser.add_argument("--gcs-bucket-name", default="nyc_taxi_data_trip", help="Name of the Google Cloud Storage "
                                                                                "bucket")

    args = parser.parse_args()

    for year in args.year:
        for month in args.month:
            dataset_url = generate_dataset_url(args.color, year, month)
            print(dataset_url)
            df = fetch(dataset_url, args.color)
            dataset_file_name = f"{args.color}_tripdata_{year}-{month:02}"



            parquet_path = write_df_to_parquet_locally(args.color, df, dataset_file_name)
            print(f"{dataset_url} is writen to {parquet_path}")
            # Write DataFrame to Parquet file without compression


            # Upload Parquet file to GCS
            upload_to_gcs(parquet_path, args.service_account_path, args.gcs_bucket_name, args.color, year)


if __name__ == "__main__":
    main()
