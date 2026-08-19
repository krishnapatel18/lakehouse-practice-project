from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

def write_audit_log(pipeline, table, start_time, end_time, rows_read, rows_written, status, error_message, CATALOG, AUDIT_SCHEMA, AUDIT_TABLENAME, batch_id, watermark_column, watermark_value): 
    # Get the active Spark session
    spark = SparkSession.builder.getOrCreate()
    
    # Ensure schema and table exist
    if not spark.catalog.tableExists(f"{CATALOG}.{AUDIT_SCHEMA}.{AUDIT_TABLENAME}"): 
        # Create schema if it doesn't exist
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{AUDIT_SCHEMA}")

        # Create audit table if it doesn't exist
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{AUDIT_SCHEMA}.{AUDIT_TABLENAME}
            (
                pipeline_name STRING,
                table_name STRING,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                rows_read BIGINT,
                rows_written BIGINT,
                status STRING,
                error_message STRING, 
                batch_id STRING, 
                watermark_column STRING,
                watermark_value TIMESTAMP 
            )
            USING DELTA
        """)
    
    else: 
        # Creating a python audit record dictionary 
        audit_record = [{
            "pipeline_name": pipeline,
            "table_name": table,
            "start_time": start_time,
            "end_time": end_time,
            "rows_read": rows_read,
            "rows_written": rows_written,
            "status": status,
            "error_message": error_message, 
            "batch_id": batch_id, 
            "watermark_column": watermark_column, 
            "watermark_value": watermark_value 
        }]

        # Define schema explicitly to handle None values
        audit_manualschema = StructType([
            StructField("pipeline_name", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("start_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
            StructField("rows_read", LongType(), True),
            StructField("rows_written", LongType(), True),
            StructField("status", StringType(), True),
            StructField("error_message", StringType(), True), 
            StructField("batch_id", StringType(), False), 
            StructField("watermark_column", StringType(), True), 
            StructField("watermark_value", TimestampType(), True) 
        ])
        
        # Convert to a spark dataframe with explicit schema
        audit_df = spark.createDataFrame(audit_record, schema=audit_manualschema)
    
        # Append to the audit table 
        # spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{AUDIT_SCHEMA}.{AUDIT_TABLENAME}")
        audit_table_name = f"{CATALOG}.{AUDIT_SCHEMA}.{AUDIT_TABLENAME}"
        audit_df.write \
            .mode("append") \
            .option("mergeSchema", "true")\
            .saveAsTable(audit_table_name) 
    
    return "Audit logs are written"
