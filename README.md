# Lakehouse ETL Project for Sales Orders & Supply Chain Data

## Overview

This project involves the extraction, transformation, and loading (ETL) of sales orders and supply chain data from a source system into a Delta Lake-based Lakehouse. The system is designed to support business analytics and reporting using a star schema structure.

## Architecture

### Source System
- **Database:** MSSQL
- **Change Data Capture (CDC):** Implemented on MSSQL to track changes in the source data.

### Transformation & Processing
- **Transformations:** Performed using PySpark
- **Orchestration:** Managed by Apache Airflow
- **Deployment:** Dockerized environment using Docker Compose

### Data Lakehouse
- **Format:** Delta Lake
- **Schema:** Star Schema

## Project Components

### 1. Data Ingestion
   - **CDC Implementation:** 
     - The source system uses CDC to track changes in the data tables. 
     - An Airflow job reads data from the CDC tables using Spark and loads it into the staging area in Delta Lake.
     - Upon successful loading, the CDC data is purged from the MSSQL CDC tables.

### 2. Data Transformation
   - **Staging Area to Dimensional Tables:**
     - Data from the staging area is transformed into dimension (dim) tables using Spark.
     - Each dimension table transformation is managed by a dedicated Airflow job.
   - **Fact and Dimension Tables:**
     - The final fact and dimension tables are stored in Delta Lake format.
     - The transformation processes include business logic to build the star schema.

### 3. Orchestration
   - **Airflow DAGs:**
     - The ETL workflow is orchestrated using Airflow DAGs, scheduled to run hourly.
     - Each job ensures data integrity and consistency across the data pipeline.

## Star Schema Details

### Source Tables
- **Order**
- **OrderItem**
- **Product**
- **Customer**
- **Supplier**

### Star Schema Tables
- **Fact Table:**
  - `factorderitem`: Contains detailed information about each order item.
- **Dimension Tables:**
  - `dimcustomer`: Contains customer details. (SCD Type 2)
  - `dimproduct`: Contains product details. (SCD Type 2)
  - `dimsupplier`: Contains supplier details. (SCD Type 2)
  - `dimdate`: Contains date details.

### Slowly Changing Dimensions (SCD)
- **SCD Type 2:** Implemented for `dimcustomer`, `dimproduct`, and `dimsupplier` to maintain historical data and track changes over time.

## Deployment

The entire project is containerized and deployed using Docker Compose. The Docker setup includes containers for:
- Spark
- Airflow
- MSSQL (for development purposes)
- Other necessary services

## Getting Started

To set up and run the project locally, follow these steps:

1. **Clone the Repository:**
   ```sh
   git clone git@github.com:AnikethSDeshpande/lakehouse.git
   cd lakehouse
   ```

2. **Build and Start Docker Containers:**
    ```sh
    docker-compose up --build
    ```

3. **Access Airflow:**
- Airflow Web UI: http://localhost:8080
- Airflow credentials: username: airflow, password: airflow

4. **Trigger the DAG:**
- Use the Airflow Web UI to manually trigger the ETL DAG or wait for the scheduled run.

## Conclusion
This Lakehouse ETL project provides a robust and scalable solution for managing sales orders and supply chain data. The implementation of CDC, PySpark transformations, and the star schema design ensures efficient data processing and storage.
