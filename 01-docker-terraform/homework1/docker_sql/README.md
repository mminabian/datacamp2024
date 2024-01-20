# New York Taxi Database Project

## Goal
The goal of this project is to set up a PostgreSQL container for the New York Taxi Database. The setup includes using Docker Compose to bring up both PostgreSQL and PGAdmin containers.

## Instructions

1. **Docker Compose**
    - Run the following command to bring up PostgreSQL and PGAdmin containers:
        ```bash
        docker-compose up -d
        ```
    - Docker Compose will handle the configuration and orchestration of both containers.

    - **Note on Network:**
        - In Docker Compose, services defined in the same `docker-compose.yml` file are, by default, connected to the same network. The network is created by Docker Compose, and its name is based on the directory where the `docker-compose.yml` file is located. In this case, the network name is `docker_sql_default`.

2. **PostgreSQL Container**
    - Access the PostgreSQL container with the following configuration:
        - User: root
        - Password: root
        - Database: ny_taxi
        - Port: 5432

3. **Data Ingestion via Jupyter Notebook**
    - Use the provided Jupyter Notebook (`upload-green_taxi_data.ipynb`) to seamlessly download and ingest data into the New York Taxi Database for both Green Taxi Trip data and Zones data.

4. **Alternative: Python Script in a Container**
    - If preferred, run the Python script within the container to download and ingest Green Taxi Trip data into the PostgreSQL database.
    - Run the following commands in the terminal:
        ```bash
        docker build -t green_taxi_ingest:v000 .
        URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"
        docker run -it \
        --network=docker_sql_default \
        green_taxi_ingest:v001 \
        --user=root \
        --password=root \
        --host=pgdatabase \
        --port=5432 \
        --db=ny_taxi \
        --table_name=green_taxi_trips \
        --url=${URL}
        ```
        - Note: This approach only creates a table and ingests data for `green_taxi_trips`.

5. **Connect to PGAdmin and Run Queries**
    - Open your web browser and go to [http://localhost:8080/browser/](http://localhost:8080/browser/).
    - Log in with the following credentials:
        - Username: `admin@admin.com`
        - Password: `root`
    - Once logged in, follow these steps to register a server and set up the connection:
        - Navigate to the `Servers` tab on the left sidebar.
        - Right-click on `Servers` and choose `Register` > `Server...`.
        - In the `General` tab:
            - Name: Provide a name for the server (e.g., `Postgres Server`).
        - In the `Connection` tab:
            - Host name/address: `pgdatabase`
            - Port: `5432`
            - Maintenance database: `ny_taxi`
            - Username: `root`
            - Password: `root`
        - Click `Save`.
    - Now, you can open the query tool within pgAdmin to run queries against the connected PostgreSQL server.

6. **Sample Queries**
    - Execute the following sample queries:
        - How many taxi trips were totally made on September 18th, 2019?
            ```sql
            select count(1)
            from green_taxi_trips
            where cast(lpep_pickup_datetime as date)='2019-09-18' and cast(lpep_dropoff_datetime as date) ='2019-09-18'
            ```
        - Which was the pick-up day with the largest trip distance? Use the pick-up time for your calculations.
            ```sql
            select max(trip_distance) as max_trip, cast(lpep_pickup_datetime as date) as pickup
            from green_taxi_trips
            group by pickup
            order by max_trip desc
            ```
        - Consider `lpep_pickup_datetime` on '2019-09-18' and ignoring Boroughs labeled as 'Unknown'. Which were the 3 pick-up Boroughs that had a sum of `total_amount` superior to 50000?
            ```sql
            select z."Borough" ,sum(total_amount) as sum_total_amount
            from green_taxi_trips t
            join zones z on t."PULocationID" = z."LocationID"
            where 
            z."Borough" <>'Unknown' and cast(lpep_pickup_datetime as date) ='2019-09-18'
            group by z."Borough"
            having sum(total_amount) > 5000
            order by sum_total_amount desc
            ```
        - For passengers picked up in September 2019 in the zone named Astoria, which was the drop-off zone that had the largest tip? We want the name of the zone, not the ID.
            ```sql
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
            ```

7. **To Gracefully Shut Down and Clean Up Resources Created by `docker-compose up`:**
    ```bash
    docker-compose down
    ```

