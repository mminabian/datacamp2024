import pyarrow as pa
import pyarrow.parquet as pq
import os 
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/gcp-key.json"
bucket_name = 'mage-zoomcamp-mahmoud'
project_id= 'mage-zoomcamp'
table_name= 'nyc_green_taxi_data'
root_path = f'{bucket_name}/{table_name}'
@data_exporter
def export_data(data, *args, **kwargs):
    table = pa.Table.from_pandas(data)
    gcs = pa.fs.GcsFileSystem()
    pq.write_to_dataset(table,
     root_path=root_path,
    partition_cols=['lpep_pickup_date'] ,
    filesystem =gcs)
    gcs_path = f'gs://{root_path}'

    # Read the Parquet dataset
    dataset = pq.ParquetDataset(gcs_path)

    # Get the number of partitions
    num_partitions = len(dataset.pieces)
    print(f'Number of partitions: {num_partitions}')
    # Specify your data exporting logic here


