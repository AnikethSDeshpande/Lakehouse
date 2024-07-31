import os

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, BooleanType, DoubleType, FloatType

from delta import *

from jobs.core.configs import configs


builder = SparkSession.builder.appName("FactOrderItem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    # load dim tables
    dimpath = configs['lakehouse_root'] + configs['core_tables']['customer']
    dimcustomer = spark.read.format('delta').load(dimpath)

    dimpath = configs['lakehouse_root'] + configs['core_tables']['product']
    dimproduct = spark.read.format('delta').load(dimpath)

    dimpath = configs['lakehouse_root'] + configs['core_tables']['supplier']
    dimsupplier = spark.read.format('delta').load(dimpath)

    dimpath = configs['lakehouse_root'] + configs['core_tables']['date']
    dimdate = spark.read.format('delta').load(dimpath)

    # read orders from staging
    staging_orders_path = configs['lakehouse_root'] + configs['staging_tables']['order']
    staging_orders = spark.read.format('delta').load(staging_orders_path)

    staging_order_items_path = configs['lakehouse_root'] + configs['staging_tables']['order_item']
    staging_order_items = spark.read.format('delta').load(staging_order_items_path)

    # insert records
    factOrderItemSchema = StructType([
        StructField('factkey', IntegerType(), False),
        StructField('order_id', IntegerType(), False),
        StructField('order_item_id', IntegerType(), False),
        StructField('order_number', IntegerType(), False),
        StructField('unit_price', FloatType(), False),
        StructField('quantity', IntegerType(), False),
        StructField('amount', DoubleType(), False),
        StructField('total_amount', DoubleType(), False),
        StructField('datekey', StringType(), False),
        StructField('customerkey', IntegerType(), False),
        StructField('productkey', IntegerType(), False),
        StructField('supplierkey', IntegerType(), False)
    ])

    factorderitem_path = configs['lakehouse_root'] + configs['core_tables']['orderitem']

    if os.path.exists(factorderitem_path):
        factOrderItem = spark.read.format('delta').load(factorderitem_path)
    else:
        factOrderItem = spark.createDataFrame([], factOrderItemSchema)

    max_factkey = factOrderItem.select(coalesce(max('factKey'), lit(0) )).collect()[0][0]

    staging_order_item = staging_orders\
                        .filter(col('operation').isin(configs['cdc_codes']['INSERT']))\
                        .join(
                            staging_order_items.filter(col('operation').isin(configs['cdc_codes']['INSERT'])),
                            (staging_orders.order_id == staging_order_items.order_id),
                            'left'  
                        ).select(
                                staging_orders.order_id,
                                staging_orders.order_date,
                                staging_orders.order_number,
                                staging_orders.customer_id,
                                staging_orders.total_amount,
                                staging_order_items.order_item_id,
                                staging_order_items.product_id,
                                staging_order_items.unit_price,
                                staging_order_items.quantity,
                                staging_orders.last_modified.alias('last_modified')
                        )

    order_item_insert = staging_order_item.alias('order_item')\
                    .join(dimcustomer.alias('customer').select('dimkey', 'start_date', 'end_date', 'customer_id'),
                        (staging_order_item.last_modified >= dimcustomer.start_date) &
                        (staging_order_item.last_modified <= dimcustomer.end_date) &
                        (staging_order_item.customer_id == dimcustomer.customer_id),
                        'left'
                        )\
                    .join(dimdate.alias('date'),
                        to_date(staging_order_item.last_modified) == dimdate.date,
                        'left'
                        )\
                    .join(dimproduct.alias('product').select('dimkey', 'start_date', 'end_date', 'product_id', 'supplier_id'),
                        (staging_order_item.last_modified >= dimproduct.start_date) &
                        (staging_order_item.last_modified <= dimproduct.end_date) &
                        (staging_order_item.product_id == dimproduct.product_id),
                        'left'
                        )\
                    .join(dimsupplier.alias('supplier').select('dimkey', 'start_date', 'end_date', 'supplier_id'),
                        (staging_order_item.last_modified >= dimsupplier.start_date) &
                        (staging_order_item.last_modified <= dimsupplier.end_date) &
                        (dimproduct.supplier_id == dimsupplier.supplier_id),
                        'left'
                        )\
                    .withColumn('factkey', max_factkey + monotonically_increasing_id())\
                    .withColumn('amount', col('order_item.quantity') * col('order_item.unit_price'))\
    .select(
            'factkey'
            , col('order_item.order_id')
            , col('order_item.order_item_id').alias('order_item_id')
            , col('order_item.order_number').alias('order_number')
            , col('order_item.unit_price').alias('unit_price')
            , col('order_item.quantity').alias('quantity')
            , col('amount')
            , col('total_amount')
            , col('customer.dimkey').alias('customerkey')
            , col('product.dimkey').alias('productkey')
            , col('supplier.dimkey').alias('supplierkey')
            , col('date.dimkey').alias('datekey')
    )

    final_factorderitem = factOrderItem.union(order_item_insert)
    final_factorderitem.write.format('delta').mode('overwrite').save(factorderitem_path)

    # TEST
    try:
        assert float(staging_orders.select(sum('total_amount')).collect()[0][0] ) == float(final_factorderitem.select(sum('amount')).collect()[0][0])
    except Exception as e:
        print(f'record counts did not match!')


if __name__ == '__main__':
    main()
    print(f'completed factorderitem etl!')