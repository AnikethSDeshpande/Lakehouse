configs = {
    'lakehouse_root': '/opt/bitnami/spark/my_lakehouse/',
    'source_tables': {
        'product': 'cdc.dbo_Product_CT',
        'customer': 'cdc.dbo_Customer_CT',
        'supplier': 'cdc.dbo_Supplier_CT',
        'order' : 'cdc.dbo_Order_CT',
        'order_item': 'cdc.dbo_OrderItem_CT'
    },
    'sink_tables': {
        'product': 'staging/product/',
        'customer': 'staging/customer/',
        'supplier': 'staging/supplier/',
        'date': 'staging/date/',
        'order': 'staging/order/',
        'order_item': 'staging/order_item/'
    }
}

db_configs = {
    'db': 'mydb',
    'host': 'mssql',
    'username': 'SA',
    'password': 'pasS_123',
}