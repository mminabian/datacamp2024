import os
import argparse
import requests
from google.cloud import storage

def download_and_upload_data(bucket_name, base_url, output_gcs_path, service_account_key_path, prefix):
    # Authenticate with Google Cloud using the provided service account key
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key_path

    # Create a Google Cloud Storage client
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Download Parquet files for each month and upload directly to GCS
    for month in range(1, 13):
        file_name = f"{prefix}-{month:02d}.parquet"
        url = f"{base_url}{file_name}"
        print(url)
        print(file_name)

        response = requests.get(url)
        file_content = response.content

        # Upload the Parquet file directly to Google Cloud Storage
        blob = bucket.blob(os.path.join(output_gcs_path, file_name))
        blob.upload_from_string(file_content)

        print(f"Data for {file_name} uploaded to {output_gcs_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and upload Green Taxi data to Google Cloud Storage.")
    parser.add_argument("--bucket_name", default="nyc-taxi-2022", help="Google Cloud Storage bucket name")
    parser.add_argument("--base_url", default="https://d37ci6vzurychx.cloudfront.net/trip-data/", help="Base URL NYC Taxi data ")
    parser.add_argument("--output_gcs_path", default="green-taxi", help="Path to the output folder in Google Cloud Storage")
    parser.add_argument("--service_account_key_path", default="gcp-key.json", help="Path to the Google Cloud service account key JSON file")
    parser.add_argument("--prefix", default="green_tripdata_2022", help="Prefix for the file names including year of data")

    args = parser.parse_args()

    download_and_upload_data(args.bucket_name, args.base_url, args.output_gcs_path, args.service_account_key_path, args.prefix)
