TABLE_CONFIG = {
    "dim_products": {
        "source": "clean_products",
        "target": "dim_products",
        "primary_key": "product_id",
        "surrogate_key": "product_key",
        "watermark_column": "modified_date",
        "scd1_columns": ["product_name", "category", "brand"],
        "scd2_columns": ["price"]
    }, 

    "dim_customers": {
        "source": "clean_customers", 
        "target": "dim_customers", 
        "primary_key": "customer_id", 
        "surrogate_key": "customer_key", 
        "watermark_column": "modified_date", 
        "scd1_columns": ["name", "email"], 
        "scd2_columns": ["city", "state"]
    }, 

    "dim_suppliers": {
        "source": "clean_suppliers", 
        "target": "dim_suppliers", 
        "primary_key": "supplier_id", 
        "surrogate_key": "supplier_key", 
        "watermark_column": "modified_date", 
        "scd1_columns": ["supplier_name", "contact_name", "contact_email"], 
        "scd2_columns": ["country", "city"]
    }, 

    "fact_sales": {
        "source_query": """
            SELECT 
                oi.order_item_id, 
                o.order_id, 
                o.customer_id, 
                p.product_id, 
                d.date_key, 
                oi.quantity, 
                oi.unit_price,  
                pay.payment_status, 
                o.status, 
                oi.modified_date,
                oi.quantity * oi.unit_price AS sales_amount  
            FROM catalog_project1.silver.clean_orders o 
            INNER JOIN catalog_project1.silver.clean_order_items oi 
                ON o.order_id = oi.order_id 
            INNER JOIN catalog_project1.silver.clean_products p 
                ON oi.product_id = p.product_id 
            INNER JOIN catalog_project1.silver.clean_payments pay 
                ON o.order_id = pay.order_id 
            INNER JOIN catalog_project1.gold.dim_date d 
                ON o.order_date = d.full_date
        """, 
        "target": "fact_sales", 
        "primary_key": "order_item_id", 
        "surrogate_key": "sales_key", 
        "watermark_column": "modified_date", 
        "scd1_columns": [], 
        "scd2_columns": []
    }
}
