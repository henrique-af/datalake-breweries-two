# Brewery Data Pipeline

This project implements a data pipeline to collect and store information about breweries using the OpenBreweryDB API and storing the data in a MinIO data lake.

## Overview

The pipeline is orchestrated by Apache Airflow and consists of a DAG (Directed Acyclic Graph) that performs the following operations:

1. Checking the availability of the OpenBreweryDB API
2. Extracting brewery data
3. Storing the data in the Bronze layer of MinIO

## Technologies

- Apache Airflow
- Python
- MinIO
- Docker

## Prerequisites

- Docker
- Docker Compose

## Setup

1. Clone the repository:
   ```
   git clone https://github.com/henrique-af/datalake-breweries-two.git
   cd datalake-breweries-two
   ```

2. Configure environment variables:
   Create a `.env` file in the project root with the following content:
   ```
   AIRFLOW_UID=50000
   AIRFLOW_GID=50000
   ```

## Execution

1. Start the services:
   ```
   docker-compose up -d
   ```

2. Access the Airflow web interface:
   ```
   http://localhost:8080
   ```

3. Log in to Airflow:
   - Username: admin
   - Password: admin

4. Activate the `dag_breweries` DAG in the Airflow interface.

## Access Credentials

- Airflow Web Interface:
  - Username: admin
  - Password: admin

- MinIO:
  - Access Key: minio
  - Secret Key: minio123

Note: When we implement the Gold layer and add a database, we will update this README with the necessary database credentials.

## DAG Structure

The `dag_breweries` DAG consists of three main tasks:

- `check_api`: Verifies the accessibility of the OpenBreweryDB API
- `extract_data`: Extracts data from the API
- `store_data`: Stores the data in the Bronze bucket of MinIO

## Future Development

- Implementation of transformations for Silver and Gold layers
- Addition of unit and integration tests
- Implementation of monitoring and alerts