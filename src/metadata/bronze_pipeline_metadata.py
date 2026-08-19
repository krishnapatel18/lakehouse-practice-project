# src/metadata/pipeline_metadata.py

TABLE_METADATA = {
    "customers": {
        "source": "customers_raw",
        "target": "customers",
        "primary_key": "customer_id", 
        "watermark_column": "modified_date" 
    },

    "products": {
        "source": "products_raw",
        "target": "products",
        "primary_key": "product_id", 
        "watermark_column": "modified_date" 
    },

    "orders": {
        "source": "orders_raw",
        "target": "orders",
        "primary_key": "order_id", 
        "watermark_column": "modified_date" 
    },

    "payments": {
        "source": "payments_raw",
        "target": "payments",
        "primary_key": "payment_id", 
        "watermark_column": "modified_date" 
    },

    "inventory": {
        "source": "inventory_raw",
        "target": "inventory",
        "primary_key": "inventory_id", 
        "watermark_column": "modified_date" 
    }, 

    "order_items": {
        "source": "order_items_raw",
        "target": "order_items",
        "primary_key": "order_item_id", 
        "watermark_column": "modified_date" 
    }, 
    
    "suppliers": {
        "source": "suppliers_raw",
        "target": "suppliers",
        "primary_key": "supplier_id", 
        "watermark_column": "modified_date" 
    } 
} 