import os
import datetime

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, DateType, IntegerType

from delta import *

from jobs.core.configs import configs

builder = SparkSession.builder.appName("DimCustomerETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    staging_customers_path = configs['lakehouse_root'] + configs['staging_tables']['customer']
    staging_customers = spark.read.format('delta').load(staging_customers_path)

    if staging_customers.count() == 0:
        print('Customer staging data empty!')
        return True

    dim_customer_path = configs['lakehouse_root'] + configs['core_tables']['customer']
    dim_customer_path

    schema = StructType([
        StructField("dimkey", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("start_date", DateType(), False),
        StructField("end_date", DateType(), False)
    ])

    if not os.path.exists(dim_customer_path):
        dimcustomer = spark.createDataFrame([], schema)
    else:
        dimcustomer = spark.read.format('delta').load(dim_customer_path)

    # insert records
    max_dimkey = dimcustomer.select(coalesce(max(col('dimkey')), lit(0)).alias('max_v')).collect()[0][0]

    cols = ['dimkey', 'customer_id', 'first_name', 'last_name', 'city', 'country', 'phone', 'start_date', 'end_date']
    staging_customers_insert = staging_customers.where(col('operation').isin(configs['cdc_codes']['INSERT']))\
            .withColumnRenamed('last_modified', 'start_date')\
            .withColumn('end_date', lit(configs['defaults']['max_date']))\
            .withColumn('dimkey', monotonically_increasing_id() + max_dimkey + 1)\
            .select(cols)
    dimcustomer_insert = dimcustomer.union(staging_customers_insert)


    # update records
    cols = ['dimkey', 'customer_id', 'first_name', 'last_name', 'city', 'country', 'phone', 'start_date', 'end_date']
    staging_customers_update = staging_customers.where(col('operation').isin(configs['cdc_codes']['UPDATE']))\
            .withColumnRenamed('last_modified', 'start_date')\
            .withColumn('end_date', lit(configs['defaults']['max_date']))\
            .withColumn('dimkey', monotonically_increasing_id() + staging_customers_insert.count() + max_dimkey + 1)\
            .select(cols)
    
    untouched = dimcustomer_insert.alias('a')\
        .join(
            staging_customers_update.select('customer_id', col('start_date').alias('b_sd')), 
            (dimcustomer_insert.customer_id != staging_customers_update.customer_id) |
            (dimcustomer_insert.end_date != configs['defaults']['max_date']) ,
            'inner'
        )\
        .select('a.*')
    
    update_existing = dimcustomer_insert.alias('a')\
        .join(
            staging_customers_update.select('customer_id', col('start_date').alias('b_sd')), 
            (dimcustomer_insert.customer_id == staging_customers_update.customer_id) &
            (dimcustomer_insert.end_date == lit(configs['defaults']['max_date'])),
            'inner'
        )\
        .withColumn('end_date', col('b_sd'))\
        .select('a.*', 'end_date')

    if (staging_customers_update.count() == 0) and (dimcustomer_insert.count() > 0):
        print('Insert-Only is True')
        untouched = dimcustomer_insert

    final_dimcustomer = untouched.union(
        update_existing
    ).union(
        staging_customers_update
    ).orderBy('dimkey')

    final_dimcustomer.write.format('delta').mode('overwrite').save(dim_customer_path)


if __name__ == '__main__':
    main()
    print(f'completed dimcustomer etl!')