### To bring both postgres and pgadmin containers togther:
docker-compose up -d

in Docker Compose, the services defined in the same docker-compose.yml file are, by default, connected to the same network. This network is created by Docker Compose, and its name is based on the directory where the docker-compose.yml file is located.

The network name is generated using the following format: 
    directory_name_default
    in our case the network name is docker_sql_default
### To  create green_taxi_trips and zones table and Ingest data  :
#### using jupyter note book:
open upload-green_taxi_data.ipynb and run all the cells 
#### Using  ingestion container :
in terminal 
  1. docker build -t green_taxi_ingest:v000 .
  2. URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"
  3. docker run -it \
  --network=docker_sql_default \
  green_taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url=${URL}
    note: this approch only create table and ingest for green_taxi_trips
### To connect to pgadmin and run query
 http://localhost:8080/browser/
 username admin@admin.com
 password root
 server -> register server -> enter the connection data
 open query tool to run query 
#### How many taxi trips were totally made on September 18th 2019?
-- Tip: started and finished on 2019-09-18.
select count(1)
from green_taxi_trips
where cast(lpep_pickup_datetime as date)='2019-09-18' and cast(lpep_dropoff_datetime as date) ='2019-09-18'
--Which was the pick up day with the largest trip distance Use the pick up time for your calculations.
select max(trip_distance) as max_trip, cast(lpep_pickup_datetime as date) as pickup
from green_taxi_trips
group by pickup
order by max_trip desc

#### Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?


select z."Borough" ,sum(total_amount) as sum_total_amount
from green_taxi_trips t
join zones z on t."PULocationID" = z."LocationID"
where 
z."Borough" <>'Unknown' and cast(lpep_pickup_datetime as date) ='2019-09-18'
group by z."Borough"
having sum(total_amount) > 5000
order by sum_total_amount desc
#### For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip? We want the name of the zone, not the id. -->


select zdo."Zone" as drop_zone, max(tip_amount) as max_tip
from green_taxi_trips t
join zones zpu on t."PULocationID" = zpu."LocationID"
join zones zdo on t."DOLocationID" = zdo."LocationID"
where 
extract(year from t.lpep_pickup_datetime)=2019 and
extract(month from t.lpep_pickup_datetime)=09 and
zpu."Zone" = 'Astoria'
GROUP BY zdo."Zone"
order by max_tip desc





### To to gracefully shut down and clean up the resources created by       docker-compose up:
docker-compose down
