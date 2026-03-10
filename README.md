# Yellow Taxi Pipeline
This project is an end-to-end data pipeline that ingests NYC Yellow Taxi Trip Data, transforms it using dbt and loads it into a PostgreSQL data warehouse.
The goal was to take raw data and turn it into clean and business-ready tables using data engineering tools.


##  Tools & Technologies
- **Apache Airflow** - pipeline orchestration
- **DBT** - transforming data
- **PostgreSQL** - Data warehouse
- **Docker** - Containerization 
- **Python** - Ingestion scripting

##  Architecture
The pipeline automatically ingests raw trip data into PostgreSQL, transforms it through multiple layers using dbt (staging, gold, marts) and orchestrates 
the entire process with Apache Airflow. Everything runs inside Docker containers.

## Pipeline Flow
```
ingest_csv_to_postgres
        └── dbt_stg_trips
                └── dbt_test_staging
                        └── dbt_seed
                                ├── dbt_fct_trips ──┐
                                ├── dbt_dim_vendor ─┼── dbt_test_gold
                                └── dbt_dim_rate ───┘        └── dbt_mart_monthly_summary
                                                                        └── dbt_test_marts
```

## Data Layers

- **Raw** - raw ingested data 
- **Silver** - cleaned and structured data
- **Gold** - fact and dimension tables
- **Mart** - aggregated business-ready tables

## How to Run

### Prerequisites
- Docker Desktop installed and running
- Git installed
- DBeaver or any PostgreSQL client (optional, for viewing data)

### Steps

**1. Clone the repository**
```bash
git clone https://github.com/musanebizade/yellow-taxi-pipeline.git
cd yellow-taxi-pipeline
```

**2. Add your CSV file**

Place your yellow taxi CSV file inside the `data/` folder and rename it to:
```
yellow_taxi.csv
```

**3. Create `.env` file**

Create a `.env` file in the root of the project with the following content:
```dotenv
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=.
AIRFLOW_IMAGE_NAME=apache/airflow:3.1.7
_AIRFLOW_WWW_USER_USERNAME=your_username
_AIRFLOW_WWW_USER_PASSWORD=your_password
AIRFLOW__API_AUTH__JWT_SECRET=your_secret_key
```

**4. Build the Docker image**
```bash
docker-compose build
```

**5. Start the containers**
```bash
docker-compose up -d
```

**6. Create the raw schema in PostgreSQL**
```bash
docker exec -it target-db psql -U postgres -d postgres -c "CREATE SCHEMA IF NOT EXISTS raw;"
```

**7. Open Airflow**

Go to `http://localhost:8080` and login with the credentials you set in `.env`

**8. Trigger the pipeline**

Find the `yellow_taxi_pipeline` DAG and click the play button to run it

**9. View the results**

Connect DBeaver to:
```
Host:     localhost
Port:     5432
Database: postgres
Username: postgres
Password: your_password
```

## What I Learned

In this project I learned the fundamentals of building a data pipeline  from scratch. I understood what Apache Airflow is and how to create a DAG with tasks that depend on each other. 
I learned what dbt is and how it transforms raw data into clean, structured tables using SQL models. One of the key concepts 
I understood is the difference between `source()` and `ref()` in dbt — source points to tables that were not created by dbt itself, while ref() refers to tables that dbt manages itself.  I also learned about Medallion architecture and how it organizes data with layers. I learned why Docker is important and how to create containers. 
