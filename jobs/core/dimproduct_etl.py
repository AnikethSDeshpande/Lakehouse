import os
import datetime

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, BooleanType, FloatType

from delta import *

from jobs.core.configs import configs


builder = SparkSession.builder.appName("DimProductETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    staging_products_path = configs['lakehouse_root'] + configs['staging_tables']['product']
    staging_products = spark.read.format('delta').load(staging_products_path)

    if staging_products.count() == 0:
        print('Product staging data empty!')
        return True

    dim_product_path = configs['lakehouse_root'] + configs['core_tables']['product']
    schema = StructType([
        StructField("dimkey", IntegerType(), False),
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("supplier_id", StringType(), False),
        StructField("unit_price", FloatType(), True),
        StructField("package", StringType(), True),
        StructField("is_discontinued", BooleanType(), True),
        StructField("start_date", DateType(), False),
        StructField("end_date", DateType(), False)
    ])

    if not os.path.exists(dim_product_path):
        dimproduct = spark.createDataFrame([], schema)
    else:
        dimproduct = spark.read.format('delta').load(dim_product_path)

    # insert records
    max_dimkey = dimproduct.select(coalesce(max(col('dimkey')), lit(0)).alias('max_v')).collect()[0][0]
    cols = ['dimkey', 'product_id', 'product_name', 'supplier_id', 'unit_price', 'package', 'is_discontinued', 'start_date', 'end_date']
    staging_products_insert = staging_products.where(col('operation').isin(configs['cdc_codes']['INSERT']))\
            .withColumnRenamed('last_modified', 'start_date')\
            .withColumn('end_date', lit(configs['defaults']['max_date']))\
            .withColumn('dimkey', monotonically_increasing_id() + max_dimkey + 1)\
            .select(cols)

    dimproduct_insert = dimproduct.union(staging_products_insert)

    # update records
    cols = ['dimkey', 'product_id', 'product_name', 'supplier_id', 'unit_price', 'package', 'is_discontinued', 'start_date', 'end_date']
    staging_products_update = staging_products.where(col('operation').isin(configs['cdc_codes']['UPDATE']))\
            .withColumnRenamed('last_modified', 'start_date')\
            .withColumn('end_date', lit(configs['defaults']['max_date']))\
            .withColumn('dimkey', monotonically_increasing_id() + staging_products_insert.count() + max_dimkey + 1)\
            .select(cols)

    untouched = dimproduct_insert.alias('a')\
        .join(
            staging_products_update.select('product_id', col('start_date').alias('b_sd')), 
            (dimproduct_insert.product_id != staging_products_update.product_id) |
            (dimproduct_insert.end_date != configs['defaults']['max_date']) ,
            'inner'
        )\
        .select('a.*')

    update_existing = dimproduct_insert.alias('a')\
        .join(
            staging_products_update.select('product_id', col('start_date').alias('b_sd')), 
            (dimproduct_insert.product_id == staging_products_update.product_id) &
            (dimproduct_insert.end_date == lit(configs['defaults']['max_date'])),
            'inner'
        )\
        .withColumn('end_date', col('b_sd'))\
        .select('a.*', 'end_date')

    if (staging_products_update.count() == 0) and (dimproduct_insert.count() > 0):
        print('Insert-Only is True')
        untouched = dimproduct_insert

    final_dimproduct = untouched.union(
        update_existing
    ).union(
        staging_products_update
    ).orderBy('dimkey')

    final_dimproduct.write.format('delta').mode('overwrite').save(dim_product_path)


if __name__ == '__main__':
    main()
    print(f'completed dimproduct etl!')