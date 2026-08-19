# from pyspark.sql import SparkSession 
# from pyspark.sql.functions import current_timestamp
# import uuid 

# spark = SparkSession.builder.getOrCreate()

# def get_batch_id(spark): 
#     batch_id = str(uuid.uuid4())
#     return batch_id
    
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, max as spark_max

spark = SparkSession.builder.getOrCreate()

def get_batch_id(spark, CATALOG, AUDIT_SCHEMA, AUDIT_TABLENAME): 
    """
    Get the next sequential batch ID.
    Only considers numeric batch_ids (ignores old UUID-format batch_ids).
    Returns "1" if no numeric batch_ids exist yet.
    """
    audit_table = f"{CATALOG}.{AUDIT_SCHEMA}.{AUDIT_TABLENAME}" 
    
    if not spark.catalog.tableExists(audit_table):
        return "1"
    
    # Get all batch_ids and filter to numeric ones only
    all_batch_ids = spark.table(audit_table).select("batch_id").collect()
    
    numeric_batch_ids = []
    for row in all_batch_ids:
        batch_id = row['batch_id']
        if batch_id and batch_id.isdigit():  # Only consider numeric strings
            numeric_batch_ids.append(int(batch_id))
    
    if numeric_batch_ids:
        return str(max(numeric_batch_ids) + 1)
    else:
        return "1"  # No numeric batch_ids yet, start from 1

