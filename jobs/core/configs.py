import datetime

configs = {
    'lakehouse_root': '/opt/bitnami/spark/my_lakehouse/',
    'staging_tables': {
        'product': 'staging/product',
        'customer': 'staging/customer',
        'supplier': 'staging/supplier',
        'order' : 'staging/order',
        'order_item': 'staging/order_item'
    },
    'core_tables': {
        'product': 'core/dimproduct',
        'customer': 'core/dimcustomer',
        'supplier': 'core/dimsupplier',
        'date': 'core/dimdate',
        'orderitem': 'core/factorderitem'
    },
    'defaults': {
        'min_date': datetime.datetime.strptime('2012-06-01', '%Y-%m-%d'),
        'max_date': datetime.datetime.strptime('2030-12-30', '%Y-%m-%d')
    },
    'cdc_codes': {
        'INSERT': [2],
        'UPDATE': [3, 4],
        'DELETE': [1]
    }
}