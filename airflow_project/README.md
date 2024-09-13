# Data ETL Pipeline with Airflow

This project sets up an **ETL (Extract, Transform, Load)** pipeline for ingesting COVID-19 data from an external source (currently supports HTTP), transforming the data to handle missing values, and loading it into a PostgreSQL database. The pipeline is orchestrated using **Apache Airflow**.

## Project Structure

```bash
.
├── dags/
│   ├── etl_pipeline.py       # The main DAG file defining the ETL pipeline
├── tasks/
│   ├── extractor.py          # Extracts the raw data from the COVID-19 data source
│   ├── parser.py             # Cleans and transforms the extracted data
│   ├── loader.py             # Loads the processed data into PostgreSQL
├── requirements.txt          # Python dependencies
├── Dockerfile                # Dockerfile to set up the Airflow environment
├── docker-compose.yaml       # Docker Compose configuration to run Airflow, PostgreSQL, and DBT
└── README.md                 # Project documentation
```

## Prerequisites
Docker and Docker Compose installed on your machine.
Basic understanding of Python and Airflow concepts.

## Usage
1. Clone this repository:
    ```sh
    git https://github.com/your-repo/airflow_project.git
    ```

2. Navigate to the project directory:
    ```sh
    cd airflow_project
    ```

3. Run the Docker container (Unit tests are automatically ran in this part as well)
    The following will be created
    Airflow Scheduler
    Airflow Webserver (Accessible at http://localhost:8081)
    PostgreSQL Database
    DBT

    ```sh
    docker-compose up --build
    ```

4. Access the web interface:
    Open your web browser and go to [http://localhost:8081](http://localhost:8081) to access the Toy Robot interface.

The default credentials are:
Username: airflow
Password: airflow

5. Set Airflow Variables:

Go to the Airflow UI and add the following variables:
URL: URL of the raw data. Currently supports only csv 
postgres_connection: Connection string for your PostgreSQL database.

## Running the ETL Pipeline
The ETL pipeline consists of the following tasks:

### Extractor:
Extracts the raw data from the COVID-19 data source (e.g., Johns Hopkins CSSEGISandData/COVID-19).
Fetches the daily reports and handles both flat and nested CSV file structures.

### Parser:
Cleans and transforms the data.
Handles missing values (NaN, None, etc.), replacing them with appropriate defaults (e.g., empty strings for non-numeric values, 0 for numeric values).
Converts the DataFrame into a list of dictionaries ready for insertion.

### Loader:
Inserts the transformed data into the PostgreSQL database.
Handles the additional_data field as JSONB in PostgreSQL.
Ensures proper typecasting for numeric and non-numeric fields to avoid errors like invalid input syntax for type double precision.

## Running the DAG
Once everything is configured, you can trigger the ETL pipeline DAG from the Airflow UI:

Go to DAGs in the Airflow UI.
Find the covid_data_ingestion_dag and click the Play button to trigger the ETL process.
Monitor the progress in the Graph View and check logs for details.

## Key Features
Modular ETL Pipeline: The ETL tasks are modular, with separate scripts for extracting, transforming, and loading the data.
Error Handling: The pipeline includes error handling for missing data and invalid values during parsing and loading.
JSONB Storage: Non-standard columns (dynamic fields) are stored in a JSONB field in PostgreSQL, enabling flexible data storage.
