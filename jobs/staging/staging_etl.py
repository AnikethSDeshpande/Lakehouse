import pymssql

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *

from delta import *

from jobs.staging.configs import configs, db_configs


jdbc_jre8  = "/opt/bitnami/spark/jars/mssql-jdbc-12.6.3.jre8.jar"
jdbc_jre11 = "/opt/bitnami/spark/jars/mssql-jdbc-12.6.3.jre11.jar"

builder = SparkSession.builder.appName("LakehouseStaging") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

jdbc_url = f"jdbc:sqlserver://{db_configs['host']};databaseName={db_configs['db']}"
connection_properties = {
    "user": db_configs["username"],
    "password": db_configs["password"],
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "trustServerCertificate": "true"
}


# utils function
def get_mssql_conn():
    conn = pymssql.connect(
        host=db_configs['host'],
        user=db_configs['username'],
        password=db_configs['password'],
        database=db_configs['db']
    )
    return conn


# load customer table
def customers_staging_etl():
    print(f'PROCESSING: CUSTOMERS')
    query = '''
        select
            * 
        from 
            cdc.dbo_Customer_CT mt
        where 
            mt.__$operation in (3, 4)
        and
            exists (
                select 
                    1
                from (
                    select 
                        __$start_lsn
                        , max(__$operation) as max_operation
                        , max(__$command_id) as max_command
                    from 
                        cdc.dbo_Customer_CT
                    group by 
                        __$start_lsn
                ) [update]
                where
                    mt.__$start_lsn = [update].__$start_lsn
                and 
                    mt.__$operation = [update].max_operation
                and 
                    mt.__$command_id = [update].max_command
            )
        union
        (
            select 
                * 
            from 
                cdc.dbo_Customer_CT
            where 
                __$operation not in (3, 4)
        )
    '''

    cdc_customer = spark.read.jdbc(url=jdbc_url, table=f"({query}) as subquery", properties=connection_properties)

    cols = ['start_lsn', 'operation', 'customer_id', 'first_name', 'last_name', 'city', 'country', 'phone', 'last_modified']

    cdc_customer = cdc_customer\
        .withColumnRenamed('Id', 'customer_id')\
        .withColumnRenamed('FirstName', 'first_name')\
        .withColumnRenamed('LastName', 'last_name')\
        .withColumnRenamed('City', 'city')\
        .withColumnRenamed('Country', 'country')\
        .withColumnRenamed('Phone', 'phone')\
        .withColumnRenamed('__$operation', 'operation')\
        .withColumnRenamed('__$start_lsn', 'start_lsn')\
        .select(cols)\
        .orderBy('start_lsn')

    cdc_customer.show(2)

    path = configs['lakehouse_root'] + configs['sink_tables']['customer']
    cdc_customer.write.mode('overwrite').format('delta').save(path)

    start_lsns = []
    for x in cdc_customer.select('start_lsn').collect():
        byte_array = x[0]
        hex_string = '0x' + ''.join(format(x, '02x') for x in byte_array)
        start_lsns.append(hex_string)

    if len(start_lsns) > 0:
        print(len(start_lsns) , 'records to be deleted.')
        delete_cdc_table_query = f'''delete from cdc.dbo_Customer_CT where __$start_lsn in ({','.join(start_lsns)})'''
        delete_cdc_table_query
        conn = get_mssql_conn()
        cursor = conn.cursor()
        cursor.execute(delete_cdc_table_query)
        conn.commit()
        conn.close()

def products_staging_etl():
    print(f'PROCESSING: PRODUCTS')
    query = '''
        
        select
            *
        from 
            cdc.dbo_Product_CT mt
        where 
            mt.__$operation in (3, 4)
        and
            exists (
                select 
                    1
                from (
                    select 
                        __$start_lsn
                        , max(__$operation) as max_operation
                        , max(__$command_id) as max_command
                    from 
                        cdc.dbo_Product_CT
                    group by 
                        __$start_lsn
                ) [update]
                where
                    mt.__$start_lsn = [update].__$start_lsn
                and 
                    mt.__$operation = [update].max_operation
                and 
                    mt.__$command_id = [update].max_command
            )
        union
        (
            select 
                * 
            from 
                cdc.dbo_Product_CT
            where 
                __$operation not in (3, 4)
        )
    '''

    cdc_product = spark.read.jdbc(url=jdbc_url, table=f"({query}) as subquery", properties=connection_properties)

    cols = ['start_lsn', 'operation', 'product_id', 'product_name', 'supplier_id', 'unit_price', 'package', 'is_discontinued', 'last_modified']

    cdc_product = cdc_product\
        .withColumnRenamed('Id', 'product_id')\
        .withColumnRenamed('ProductName', 'product_name')\
        .withColumnRenamed('SupplierId', 'supplier_id')\
        .withColumnRenamed('UnitPrice', 'unit_price')\
        .withColumnRenamed('Package', 'package')\
        .withColumnRenamed('IsDiscontinued', 'is_discontinued')\
        .withColumnRenamed('__$operation', 'operation')\
        .withColumnRenamed('__$start_lsn', 'start_lsn')\
        .select(cols)\
        .orderBy('start_lsn')

    path = configs['lakehouse_root'] + configs['sink_tables']['product']

    cdc_product.write.mode('overwrite').format('delta').save(path)

    start_lsns = []
    for x in cdc_product.select('start_lsn').collect():
        byte_array = x[0]
        hex_string = '0x' + ''.join(format(x, '02x') for x in byte_array)
        start_lsns.append(hex_string)

    print(len(start_lsns), ' records to be deleted.')

    if len(start_lsns) > 0:
        delete_cdc_table_query = f'''delete from cdc.dbo_Product_CT where __$start_lsn in ({','.join(start_lsns)})'''

        conn = get_mssql_conn()
        cursor = conn.cursor()
        cursor.execute(delete_cdc_table_query)
        conn.commit()
        conn.close()

def suppliers_staging_etl():
    print(f'PROCESSING: SUPPLIERS')
    query = '''
        select
            *
        from 
            cdc.dbo_Supplier_CT mt
        where 
            mt.__$operation in (3, 4)
        and
            exists (
                select 
                    1
                from (
                    select 
                        __$start_lsn
                        , max(__$operation) as max_operation
                        , max(__$command_id) as max_command
                    from 
                        cdc.dbo_Supplier_CT
                    group by 
                        __$start_lsn
                ) [update]
                where
                    mt.__$start_lsn = [update].__$start_lsn
                and 
                    mt.__$operation = [update].max_operation
                and 
                    mt.__$command_id = [update].max_command
            )
        union
        (
            select 
                * 
            from 
                cdc.dbo_Supplier_CT
            where 
                __$operation not in (3, 4)
        )

    '''

    cdc_supplier = spark.read.jdbc(url=jdbc_url, 
                                    table=f"({query}) as subquery", 
                                    properties=connection_properties
                                )

    cols = ['start_lsn', 'operation', 'supplier_id', 'company_name', 'contact_name', 'contact_title', 'city', 'country', 'phone', 'fax', 'last_modified']

    cdc_supplier = cdc_supplier\
        .withColumnRenamed('Id', 'supplier_id')\
        .withColumnRenamed('CompanyName', 'company_name')\
        .withColumnRenamed('ContactName', 'contact_name')\
        .withColumnRenamed('ContactTitle', 'contact_title')\
        .withColumnRenamed('City', 'city')\
        .withColumnRenamed('Country', 'country')\
        .withColumnRenamed('Phone', 'phone')\
        .withColumnRenamed('Fax', 'fax')\
        .withColumnRenamed('__$operation', 'operation')\
        .withColumnRenamed('__$start_lsn', 'start_lsn')\
        .select(cols)\
        .orderBy('start_lsn')

    path = configs['lakehouse_root'] + configs['sink_tables']['supplier']

    cdc_supplier.write.mode('overwrite').format('delta').save(path)

    start_lsns = []
    for x in cdc_supplier.select('start_lsn').collect():
        byte_array = x[0]
        hex_string = '0x' + ''.join(format(x, '02x') for x in byte_array)
        start_lsns.append(hex_string)

    print(len(start_lsns), ' records to be deleted.')

    if len(start_lsns) > 0:
        delete_cdc_table_query = f'''delete from cdc.dbo_Supplier_CT where __$start_lsn in ({','.join(start_lsns)})'''

        conn = get_mssql_conn()
        cursor = conn.cursor()
        cursor.execute(delete_cdc_table_query)
        conn.commit()
        conn.close()

def orders_staging_etl():
    print(f'PROCESSING: ORDERS')
    query = '''
        select
            *
        from 
            cdc.dbo_Order_CT mt
        where 
            mt.__$operation in (3, 4)
        and
            exists (
                select 
                    1
                from (
                    select 
                        __$start_lsn
                        , max(__$operation) as max_operation
                        , max(__$command_id) as max_command
                    from 
                        cdc.dbo_Order_CT
                    group by 
                        __$start_lsn
                ) [update]
                where
                    mt.__$start_lsn = [update].__$start_lsn
                and 
                    mt.__$operation = [update].max_operation
                and 
                    mt.__$command_id = [update].max_command
            )
        union
        (
            select 
                * 
            from 
                cdc.dbo_Order_CT
            where 
                __$operation not in (3, 4)
        )
    '''


    cdc_order = spark.read.jdbc(url=jdbc_url, 
                                    table=f"({query}) as subquery", 
                                    properties=connection_properties
                                )

    cols = ['start_lsn', 'operation', 'order_id', 'order_date', 'order_number', 'customer_id', 'total_amount', 'last_modified']

    cdc_order = cdc_order\
        .withColumnRenamed('Id', 'order_id')\
        .withColumnRenamed('OrderDate', 'order_date')\
        .withColumnRenamed('OrderNumber', 'order_number')\
        .withColumnRenamed('CustomerId', 'customer_id')\
        .withColumnRenamed('TotalAmount', 'total_amount')\
        .withColumnRenamed('__$operation', 'operation')\
        .withColumnRenamed('__$start_lsn', 'start_lsn')\
        .select(cols)\
        .orderBy('start_lsn')

    path = configs['lakehouse_root'] + configs['sink_tables']['order']

    cdc_order.write.mode('overwrite').format('delta').save(path)

    start_lsns = []
    for x in cdc_order.select('start_lsn').collect():
        byte_array = x[0]
        hex_string = '0x' + ''.join(format(x, '02x') for x in byte_array)
        start_lsns.append(hex_string)

    print(len(start_lsns), ' records to be deleted.')

    if len(start_lsns) > 0:
        delete_cdc_table_query = f'''delete from cdc.dbo_Order_CT where __$start_lsn in ({','.join(start_lsns)})'''

        conn = get_mssql_conn()
        cursor = conn.cursor()
        cursor.execute(delete_cdc_table_query)
        conn.commit()
        conn.close()

def order_items_staging_etl():
    print(f'PROCESSING: ORDER_ITEMS')
    query = '''
        select
            *
        from 
            cdc.dbo_OrderItem_CT mt
        where 
            mt.__$operation in (3, 4)
        and
            exists (
                select 
                    1
                from (
                    select 
                        __$start_lsn
                        , max(__$operation) as max_operation
                        , max(__$command_id) as max_command
                    from 
                        cdc.dbo_OrderItem_CT
                    group by 
                        __$start_lsn
                ) [update]
                where
                    mt.__$start_lsn = [update].__$start_lsn
                and 
                    mt.__$operation = [update].max_operation
                and 
                    mt.__$command_id = [update].max_command
            )
        union
        (
            select 
                * 
            from 
                cdc.dbo_OrderItem_CT
            where 
                __$operation not in (3, 4)
        )
    '''


    cdc_order_item = spark.read.jdbc(url=jdbc_url, 
                                    table=f"({query}) as subquery", 
                                    properties=connection_properties
                                )

    cols = ['start_lsn', 'operation', 'order_item_id', 'order_id', 'product_id', 'unit_price', 'quantity', 'last_modified']

    cdc_order_item = cdc_order_item\
        .withColumnRenamed('Id', 'order_item_id')\
        .withColumnRenamed('OrderId', 'order_id')\
        .withColumnRenamed('ProductId', 'product_id')\
        .withColumnRenamed('UnitPrice', 'unit_price')\
        .withColumnRenamed('Quantity', 'quantity')\
        .withColumnRenamed('__$operation', 'operation')\
        .withColumnRenamed('__$start_lsn', 'start_lsn')\
        .select(cols)\
        .orderBy('start_lsn')

    path = configs['lakehouse_root'] + configs['sink_tables']['order_item']

    cdc_order_item.write.mode('overwrite').format('delta').save(path)

    start_lsns = []
    for x in cdc_order_item.select('start_lsn').collect():
        byte_array = x[0]
        hex_string = '0x' + ''.join(format(x, '02x') for x in byte_array)
        start_lsns.append(hex_string)

    print(len(start_lsns), ' records to be deleted.')

    if len(start_lsns) > 0:
        delete_cdc_table_query = f'''delete from cdc.dbo_OrderItem_CT where __$start_lsn in ({','.join(start_lsns)})'''

        conn = get_mssql_conn()
        cursor = conn.cursor()
        cursor.execute(delete_cdc_table_query)
        conn.commit()
        conn.close()

def main():
    print(f'starting ETL for staging area...')

    customers_staging_etl()
    products_staging_etl()
    suppliers_staging_etl()
    orders_staging_etl()
    order_items_staging_etl()

    print(f'completed ETL for staging area...')


if __name__ == '__main__':
    main()
