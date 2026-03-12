# Yellow Taxi Pipeline
This project is an end-to-end data pipeline that ingests NYC Yellow Taxi Trip Data, transforms it through bronze, silver, gold layers using dbt and PySpark, loads it into a PostgreSQL data warehouse and orchestrates the entire process with Apache Airflow. Everything runs inside Docker containers.
The goal was to take raw data and turn it into clean and business-ready tables using data engineering tools.


##  Tools & Technologies
- <img src="https://img.shields.io/badge/Apache%20Airflow-0176D2?style=for-the-badge&logo=Apache%20Airflow&logoColor=white" alt="Airflow">
- <img src="https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white" alt="dbt">
- <img src="https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL">
- <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker">
- ![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
- <img src="https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white" alt="PySpark">

##  Architecture
The pipeline is divided into 7 separate DAGs, each responsible for one layer. DAGs are connected using Airflow's dataset-based scheduling — when one DAG finishes,
it emits a signal that automatically triggers the next one.

`dag_start` is the only scheduled DAG, running every day at 00:06.


### Pipeline Flow
<p align="center">
<img width="70%" alt="image" src="https://github.com/user-attachments/assets/e3bd2f13-e7d2-41a1-ba6d-e944f0876d61" />
</p>

## Data Layers

- **Raw** - raw ingested data with minimal changes
- **Silver** - cleaned and structured data, invalid records filtered out
- **Gold** - fact and dimension tables
- **Mart** - aggregated business-ready tables

### dbt Models

| Model | Layer | Description |
|-------|-------|-------------|
| stg_trips | Silver | Cleans raw data and extracts new columns |
| fct_trips | Gold | Main fact table with all trip records |
| dim_vendor | Gold | Vendor lookup table |
| dim_rate | Gold | Rate code lookup table |
| dim_locations | Gold | Zone dimension based on coordinate boundaries |
| mart_monthly_summary | Mart | Monthly trip statistics by vendor |
| mart_top_zones | Mart | Top 5 pickup zones by trip count |
| mart_payment_breakdown | Mart | Trip analysis by payment method |

## How to Run

### Prerequisites
- Docker Desktop installed and running
- Git installed
- DBeaver or any PostgreSQL client (optional, for viewing data)

### Steps

**1. Clone the repository**
```bash
git clone https://github.com/musanebizade/final-yellow-taxi-pipeline.git
cd final-yellow-taxi-pipeline
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
_AIRFLOW_WWW_USER_USERNAME=your_username
_AIRFLOW_WWW_USER_PASSWORD=your_password
AIRFLOW__API_AUTH__JWT_SECRET=your_secret_key
TARGET_DB_USER=postgres
TARGET_DB_PASSWORD=your_db_password
TARGET_DB_HOST=target-db
TARGET_DB_PORT=5432
TARGET_DB_NAME=postgres
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

Find `dag_start` and click the trigger button. It will automatically chain through all 7 DAGs

**9. View the results**

Connect DBeaver to:
```
Host:     localhost
Port:     5434
Database: postgres
Username: postgres
Password: your_password
```

## What I Learned

In this project I learned the fundamentals of building a data pipeline  from scratch. I understood what Apache Airflow is and how to create DAGs with tasks that depend on
each other. I learned what dbt is and how it transforms raw data into clean, structured tables using SQL models. Difference between `source()` and `ref()`. I learned how PySpark processes large data in partitions, what JDBC is, and why Java is required for PySpark. I also learned how to separate a pipeline into multiple DAGs connected via dataset-based scheduling, and how `trigger_rule="all_done"` allows a pipeline to continue even when a task fails.  I also learned about Medallion architecture and how it organizes data with layers. I learned why Docker is important and how to create containers. 
