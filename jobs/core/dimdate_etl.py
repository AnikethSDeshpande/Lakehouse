import os
import datetime

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DecimalType, DateType, TimestampType

from delta import *

from jobs.core.configs import configs


builder = SparkSession.builder.appName("DimDateETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


def dimdate_etl():
    min_date = configs['defaults']['min_date']
    max_date = configs['defaults']['max_date']

    dates = []
    curr_date = min_date
    while curr_date <= max_date:
        dates.append((curr_date,))
        curr_date += datetime.timedelta(days=1)

    schema = StructType([
        StructField('date', DateType(), False)
    ])

    df = spark.createDataFrame(dates, schema)

    df = df.withColumn('dimkey', date_format(col("date"), "yyyyMMdd"))\
           .withColumn('week', concat(year('date'), lit('-'), weekofyear('date')))\
           .withColumn('month', month('date'))\
           .withColumn('year', year('date'))\
           .withColumn('quarter', concat(year('date'), lit('-'), quarter('date')))\
           .withColumn('dayofweek', date_format('date', 'E'))\
           .withColumn(
                        'isweekend',
                        when(dayofweek('date').isin(1,7), True).otherwise(False)
                      )

    sink_path = configs['lakehouse_root'] + configs['core_tables']['date']

    df.write.format('delta').partitionBy('year').save(sink_path)


dimdate_path = configs['lakehouse_root'] + configs['core_tables']['date']
if not os.path.exists(dimdate_path):
    dimdate_etl()

print(f'dimdate table is populated: {os.path.exists(dimdate_path)}')