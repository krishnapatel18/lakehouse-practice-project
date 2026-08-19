import sys
project_path = "/Workspace/Users/krishnasumanbhaipatel@gmail.com/Practice Folder/lakehouse-practice-project"
if project_path not in sys.path:
    sys.path.insert(0, project_path)

from src.utils.config import CUSTOMER_VALIDATIONS 

TABLE_CONFIG = {
    "customers": {
        "source": "customers", 
        "target": "clean_customers", 
        "primary_key": "customer_id", 
        "column_list": [
                        "customer_id", 
                        "name", 
                        "email", 
                        "city", 
                        "state", 
                        "signup_date", 
                        "created_date", 
                        "modified_date", 
                        "_ingestion_job_id" 
                    ],  
        "required_columns": ["customer_id", "name", "email", "city", "state", "signup_date", "created_date", "modified_date", "_ingestion_job_id"], 
        "type_mapping": {
                        "customer_id": "integer", 
                        "name": "string", 
                        "email": "string", 
                        "city": "string", 
                        "state": "string", 
                        "signup_date": "timestamp", 
                        "created_date": "timestamp", 
                        "modified_date": "timestamp", 
                        "_ingestion_job_id": "string"
                    }, 
        "columns_to_trim": ["name", "email", "city", "state"], 
        "columns_to_initcap": ["name", "city", "state"], 
        "columns_to_lowercase": ["email"], 
        "watermark_column": "modified_date", 
        "pattern": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$", 
        # "watermark_value": None, 
        "validation_rules": CUSTOMER_VALIDATIONS
    },

    "products": {
        "source": "products", 
        "target": "clean_products", 
        "primary_key": "product_id", 
        "column_list": [
                        "product_id", 
                        "product_name", 
                        "category", 
                        "brand", 
                        "price", 
                        "created_date", 
                        "modified_date", 
                        "_ingestion_job_id" 
                    ],  
        "required_columns": ["product_id", "product_name", "category", "brand", "price", "created_date", "modified_date", "_ingestion_job_id"], 
        "type_mapping": {
                        "product_id": "integer", 
                        "product_name": "string", 
                        "category": "string", 
                        "brand": "string", 
                        "price": "double", 
                        "created_date": "timestamp", 
                        "modified_date": "timestamp", 
                        "_ingestion_job_id": "string"
                    }, 
        "columns_to_trim": ["product_name", "category", "brand"], 
        "columns_to_initcap": ["product_name", "category", "brand"], 
        "columns_to_lowercase": [], 
        "watermark_column": "modified_date", 
        "pattern": None, 
        # "watermark_value": None, 
        "validation_rules": None
    },

    "orders": {
        "source": "orders", 
        "target": "clean_orders", 
        "primary_key": "order_id", 
        "column_list": [
                        "order_id", 
                        "customer_id", 
                        "order_date", 
                        "status", 
                        "created_date", 
                        "modified_date", 
                        "_ingestion_job_id" 
                    ],  
        "required_columns": ["order_id", "customer_id", "order_date", "status", "created_date", "modified_date", "_ingestion_job_id"], 
        "type_mapping": {
                        "order_id": "integer", 
                        "customer_id": "integer", 
                        "order_date": "timestamp", 
                        "status": "string", 
                        "created_date": "timestamp", 
                        "modified_date": "timestamp", 
                        "_ingestion_job_id": "string"
                    }, 
        "columns_to_trim": ["status"], 
        "columns_to_initcap": ["status"], 
        "columns_to_lowercase": [], 
        "watermark_column": "modified_date", 
        "pattern": None, 
        # "watermark_value": None, 
        "validation_rules": None
    },

    "order_items": {
        "source": "order_items", 
        "target": "clean_order_items", 
        "primary_key": "order_item_id", 
        "column_list": [
                        "order_item_id", 
                        "order_id", 
                        "product_id", 
                        "quantity", 
                        "unit_price", 
                        "created_date", 
                        "modified_date", 
                        "_ingestion_job_id" 
                    ],  
        "required_columns": ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "created_date", "modified_date", "_ingestion_job_id"], 
        "type_mapping": {
                        "order_item_id": "integer", 
                        "order_id": "integer", 
                        "product_id": "integer", 
                        "quantity": "integer", 
                        "unit_price": "double", 
                        "created_date": "timestamp", 
                        "modified_date": "timestamp", 
                        "_ingestion_job_id": "string"
                    }, 
        "columns_to_trim": [], 
        "columns_to_initcap": [], 
        "columns_to_lowercase": [], 
        "watermark_column": "modified_date", 
        "pattern": None, 
        # "watermark_value": None, 
        "validation_rules": None
    },

    "inventory": {
        "source": "inventory", 
        "target": "clean_inventory", 
        "primary_key": "inventory_id", 
        "column_list": [
                        "inventory_id", 
                        "product_id", 
                        "warehouse", 
                        "stock", 
                        "last_updated", 
                        "created_date", 
                        "modified_date", 
                        "_ingestion_job_id" 
                    ],  
        "required_columns": ["inventory_id", "product_id", "warehouse", "stock", "last_updated", "created_date", "modified_date", "_ingestion_job_id"], 
        "type_mapping": {
                        "inventory_id": "integer", 
                        "product_id": "integer", 
                        "warehouse": "string", 
                        "stock": "integer", 
                        "last_updated": "timestamp", 
                        "created_date": "timestamp", 
                        "modified_date": "timestamp", 
                        "_ingestion_job_id": "string"
                    }, 
        "columns_to_trim": ["warehouse"], 
        "columns_to_initcap": ["warehouse"], 
        "columns_to_lowercase": [], 
        "watermark_column": "modified_date", 
        "pattern": None, 
        # "watermark_value": None, 
        "validation_rules": None
    }, 

    "payments": {
        "source": "payments", 
        "target": "clean_payments", 
        "primary_key": "payment_id", 
        "column_list": [
                        "payment_id", 
                        "order_id", 
                        "payment_method", 
                        "payment_status", 
                        "amount", 
                        "created_date", 
                        "modified_date", 
                        "_ingestion_job_id" 
                    ],  
        "required_columns": ["payment_id", "order_id", "payment_method", "payment_status", "amount", "created_date", "modified_date", "_ingestion_job_id"], 
        "type_mapping": {
                        "payment_id": "integer", 
                        "order_id": "integer", 
                        "payment_method": "string", 
                        "payment_status": "string", 
                        "amount": "double", 
                        "created_date": "timestamp", 
                        "modified_date": "timestamp", 
                        "_ingestion_job_id": "string"
                    }, 
        "columns_to_trim": ["payment_method", "payment_status"], 
        "columns_to_initcap": ["payment_method", "payment_status"], 
        "columns_to_lowercase": [], 
        "watermark_column": "modified_date", 
        "pattern": None, 
        # "watermark_value": None, 
        "validation_rules": None
    }, 

    "suppliers": {
        "source": "suppliers", 
        "target": "clean_suppliers", 
        "primary_key": "supplier_id", 
        "column_list": [
                        "supplier_id", 
                        "supplier_name", 
                        "contact_name", 
                        "contact_email", 
                        "country", 
                        "city", 
                        "created_date", 
                        "modified_date", 
                        "_ingestion_job_id" 
                    ],  
        "required_columns": ["supplier_id", "supplier_name", "contact_name", "contact_email", "country", "city", "created_date", "modified_date", "_ingestion_job_id"], 
        "type_mapping": {
                        "supplier_id": "integer", 
                        "supplier_name": "string", 
                        "contact_name": "string", 
                        "contact_email": "string", 
                        "country": "string", 
                        "city": "string", 
                        "created_date": "timestamp", 
                        "modified_date": "timestamp", 
                        "_ingestion_job_id": "string"
                    }, 
        "columns_to_trim": ["supplier_name", "contact_name", "contact_email", "country", "city"], 
        "columns_to_initcap": ["supplier_name", "contact_name", "country", "city"], 
        "columns_to_lowercase": ["contact_email"], 
        "watermark_column": "modified_date", 
        "pattern": None, 
        # "watermark_value": None, 
        "validation_rules": None
    }
}