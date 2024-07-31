import os
import datetime

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, BooleanType

from delta import *

from jobs.core.configs import configs


builder = SparkSession.builder.appName("DimSupplierETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    staging_suppliers_path = configs['lakehouse_root'] + configs['staging_tables']['supplier']
    staging_suppliers = spark.read.format('delta').load(staging_suppliers_path)

    if staging_suppliers.count() == 0:
        print('Supplier staging data empty!')
        return True

    dim_supplier_path = configs['lakehouse_root'] + configs['core_tables']['supplier']
    schema = StructType([
        StructField("dimkey", IntegerType(), False),
        StructField("supplier_id", IntegerType(), False),
        StructField("company_name", StringType(), False),
        StructField("contact_name", StringType(), False),
        StructField("contact_title", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("fax", StringType(), True),
        StructField("start_date", DateType(), False),
        StructField("end_date", DateType(), False)
    ])

    if not os.path.exists(dim_supplier_path):
        dimsupplier = spark.createDataFrame([], schema)
    else:
        dimsupplier = spark.read.format('delta').load(dim_supplier_path)

    print('count: ', dimsupplier.count())
    dimsupplier.show(2)

    # insert records
    max_dimkey = dimsupplier.select(coalesce(max(col('dimkey')), lit(0)).alias('max_v')).collect()[0][0]

    cols = ['dimkey', 'supplier_id', 'company_name', 'contact_name', 'contact_title', 'city', 'country', 'phone', 'fax', 'start_date', 'end_date']
    staging_suppliers_insert = staging_suppliers.where(col('operation').isin(configs['cdc_codes']['INSERT']))\
            .withColumnRenamed('last_modified', 'start_date')\
            .withColumn('end_date', lit(configs['defaults']['max_date']))\
            .withColumn('dimkey', monotonically_increasing_id() + max_dimkey + 1)\
            .select(cols)
    dimsupplier_insert = dimsupplier.union(staging_suppliers_insert)

    # update records
    cols = ['dimkey', 'supplier_id', 'company_name', 'contact_name', 'contact_title', 'city', 'country', 'phone', 'fax', 'start_date', 'end_date']
    staging_suppliers_update = staging_suppliers.where(col('operation').isin(configs['cdc_codes']['UPDATE']))\
            .withColumnRenamed('last_modified', 'start_date')\
            .withColumn('end_date', lit(configs['defaults']['max_date']))\
            .withColumn('dimkey', monotonically_increasing_id() + staging_suppliers_insert.count() + max_dimkey + 1)\
            .select(cols)

    untouched = dimsupplier_insert.alias('a')\
        .join(
            staging_suppliers_update.select('supplier_id', col('start_date').alias('b_sd')), 
            (dimsupplier_insert.supplier_id != staging_suppliers_update.supplier_id) |
            (dimsupplier_insert.end_date != configs['defaults']['max_date']) ,
            'inner'
        )\
        .select('a.*')

    update_existing = dimsupplier_insert.alias('a')\
        .join(
            staging_suppliers_update.select('supplier_id', col('start_date').alias('b_sd')), 
            (dimsupplier_insert.supplier_id == staging_suppliers_update.supplier_id) &
            (dimsupplier_insert.end_date == lit(configs['defaults']['max_date'])),
            'inner'
        )\
        .withColumn('end_date', col('b_sd'))\
        .select('a.*', 'end_date')

    if (staging_suppliers_update.count() == 0) and (dimsupplier_insert.count() > 0):
        print('Insert-Only is True')
        untouched = dimsupplier_insert

    final_dimsupplier = untouched.union(
        update_existing
    ).union(
        staging_suppliers_update
    ).orderBy('dimkey')

    final_dimsupplier.write.format('delta').mode('overwrite').save(dim_supplier_path)


if __name__ == '__main__':
    main()
    print(f'completed dimsupplier etl!')