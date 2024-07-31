from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
     'owner': 'ani',
     'retries': 5,
     'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='Lakehouse_ETL',
    description='''
    Lakehouse ETL which reads the CDC data from MSSQL, saves it in staging area,
    then reads the data from staging area updates the fact and dimension tables of the lakehouse.
    ''',
    start_date=datetime(2024, 7, 5),
    schedule_interval='@daily',
    concurrency=3,
    max_active_runs=1
) as dag:
    
    staging = SparkSubmitOperator(
        task_id = 'staging_etl',
        application='jobs/staging/staging_etl.py',
        conn_id='spark_default',
        conf={
                'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
                'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
                'spark.jars': '/opt/bitnami/spark/jars/mssql-jdbc-12.6.3.jre8.jar'
            },
        packages='io.delta:delta-spark_2.12:3.2.0',
        jars='/opt/bitnami/spark/jars/mssql-jdbc-12.6.3.jre8.jar'
    )

    dimdate = SparkSubmitOperator(
        task_id = 'dimdate_etl',
        application='jobs/core/dimdate_etl.py',
        conn_id='spark_default',
        conf={
                'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
                'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'
            },
        packages='io.delta:delta-spark_2.12:3.2.0'
    )

    dimcustomer = SparkSubmitOperator(
        task_id = 'dimcustomer_etl',
        application='jobs/core/dimcustomer_etl.py',
        conn_id='spark_default',
        conf={
                'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
                'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'
            },
        packages='io.delta:delta-spark_2.12:3.2.0'
    )

    dimproduct = SparkSubmitOperator(
        task_id = 'dimproduct_etl',
        application='jobs/core/dimproduct_etl.py',
        conn_id='spark_default',
        conf={
                'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
                'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'
            },
        packages='io.delta:delta-spark_2.12:3.2.0'
    )

    dimsupplier = SparkSubmitOperator(
        task_id = 'dimsupplier_etl',
        application='jobs/core/dimsupplier_etl.py',
        conn_id='spark_default',
        conf={
                'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
                'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'
            },
        packages='io.delta:delta-spark_2.12:3.2.0'
    )

    factorderitem_etl = SparkSubmitOperator(
        task_id = 'factorderitem_etl_etl',
        application='jobs/core/factorderitem_etl.py',
        conn_id='spark_default',
        conf={
                'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
                'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'
            },
        packages='io.delta:delta-spark_2.12:3.2.0'
    )

    staging >> [dimdate, dimcustomer, dimproduct, dimsupplier]
    [dimdate, dimcustomer, dimproduct, dimsupplier] >> factorderitem_etl