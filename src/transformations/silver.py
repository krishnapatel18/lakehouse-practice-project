from pyspark.sql import SparkSession 

import sys

# Add project root to sys.path so 'src' becomes importable
project_path = "/Workspace/Users/krishnasumanbhaipatel@gmail.com/Practice Folder/lakehouse-practice-project"
if project_path not in sys.path:
    sys.path.insert(0, project_path) 

# Imports 
import pyspark.sql.functions as F
from datetime import datetime
import uuid
from pyspark.sql.window import Window 
from delta.tables import DeltaTable 

from src.utils.audit import write_audit_log
from src.orchestration.batch import get_batch_id 
from src.utils.config import get_logger, source_empty, get_rows_count, get_duplicate_records, get_null_count, CATALOG, BRONZE_SCHEMA, LOGS_VOLUME, AUDIT_SCHEMA, AUDIT_TABLENAME, table_exists, required_columns_check, deduplicate_source, cast_columns, validate_business_rules, CUSTOMER_VALIDATIONS, trim_columns, initcap_columns, lowercase_columns, SILVER_SCHEMA

spark = SparkSession.builder.getOrCreate() 

# Add metadata 
def add_metadata(table, CATALOG, BRONZE_SCHEMA, BRONZE_TABLE, SILVER_SCHEMA, SILVER_TABLE, pipeline_id, primary_key, watermark_column): 
    SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}"
    TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.{SILVER_TABLE}"

    # Cache columns to avoid repeated analyze RPC calls 
    table_columns = table.columns 
    
    table = table.withColumns({
        "_source_table": F.lit(SOURCE_TABLE), 
        "_ingestion_pipeline_id": F.lit(pipeline_id), 
        "ingestion_timestamp": F.current_timestamp(), 
        "updation_timestamp": F.current_timestamp()
        }) 

    # Write: Overwrite => Merge 
    # Check if table exists
    if not spark.catalog.tableExists(TARGET_TABLE):
        # Initial write if table doesn't exist
        table.write.mode("overwrite")\
                        .format("delta")\
                        .saveAsTable(TARGET_TABLE)
    else:
        # Merge logic for existing table
        target = DeltaTable.forName(spark, TARGET_TABLE) 
        (
            target.alias("t")\
              .merge(
                  source = table.alias("s"), 
                  condition = F.expr(f"t.{primary_key} = s.{primary_key}")
              )\
            #   .whenMatchedUpdateAll()\
              .whenMatchedUpdate(
                  condition = F.expr(f"t.{watermark_column} <> s.{watermark_column}"), 
                  set = {
                    #  Creates a dictionary(key-value pair) that maps each column name to its source value, after filtering 'updation_timestamp' column 
                    # The ** operator upacks the dictionary  
                      **{col: f"s.{col}" for col in table_columns if col not in ["updation_timestamp"]}, 
                    # Override value of 'updation_timestamp' column 
                      "updation_timestamp": "current_timestamp()"
                  }
              )\
              .whenNotMatchedInsertAll()\
              .execute()
        ) 
    
    return f"{SILVER_TABLE} table is saved as {TARGET_TABLE}" 

def get_last_watermark(spark, pipeline_name, target, CATALOG, AUDIT_SCHEMA, AUDIT_TABLENAME):
    audit_table = f"{CATALOG}.{AUDIT_SCHEMA}.{AUDIT_TABLENAME}"
    
    if not spark.catalog.tableExists(audit_table):
        return None
    
    try: 
        # last_run = spark.sql(f"""
        #                      SELECT watermark_value FROM {audit_table} 
        #                      WHERE pipeline_name = '{pipeline_name}' AND table_name = '{target}' AND status = 'SUCCESS' ORDER BY end_time DESC LIMIT 1
        #                      """).collect()
        last_run = spark.table(audit_table)\
                    .filter((F.col("pipeline_name") == pipeline_name) & (F.col("table_name") == target) & (F.col("status") == "SUCCESS"))\
                    .orderBy(F.col("end_time").desc())\
                    .limit(1)\
                    .select("watermark_value").collect()
        if last_run and last_run[0]['watermark_value'] is not None: 
            return last_run[0]['watermark_value']
        else: 
            return None
    
    except Exception as e: 
        raise e 

# Create Silver Transformation function 
def transform_table(source, 
                    target, 
                    primary_key, 
                    pipeline_name, 
                    column_list, 
                    required_columns, 
                    type_mapping, 
                    columns_to_trim, 
                    columns_to_initcap, 
                    columns_to_lowercase, 
                    watermark_column, 
                    pattern, 
                    validation_rules, 
                    CATALOG = CATALOG, 
                    BRONZE_SCHEMA = BRONZE_SCHEMA, 
                    SILVER_SCHEMA = SILVER_SCHEMA, 
                    AUDIT_SCHEMA = AUDIT_SCHEMA, 
                    AUDIT_TABLENAME = AUDIT_TABLENAME,
                    watermark_value = None):  
    start_time = datetime.now()
    error_message = None
    rows_read = 0
    rows_written = 0 
    batch_id = get_batch_id(spark, CATALOG, AUDIT_SCHEMA, AUDIT_TABLENAME)
    pipeline_id = str(uuid.uuid4()) 
    new_watermark = None 

    try: 
        logger = get_logger(f"silver.{target}", log_to_file=True)
        logger.info(f"Starting Silver pipeline at {start_time}")

        SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.{source}"
        TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.{target}"

        # Check if it exists 
        table_exists(spark, SOURCE_TABLE)

        # Fetch last watermark value 
        watermark_value = get_last_watermark(spark, pipeline_name, target, CATALOG, AUDIT_SCHEMA, AUDIT_TABLENAME)

        # Read bronze 
        if watermark_value is None: 
            # Initial load 
            table_bronze = spark.read.table(SOURCE_TABLE)
            logger.info(f"Initial load from {SOURCE_TABLE} table")
        else: 
            # Incremental load 
            table_bronze = spark.read.table(SOURCE_TABLE).filter(F.col(watermark_column) > F.lit(watermark_value))
            logger.info(f"Incremental load from {SOURCE_TABLE}")

        # Validate input 
        table_bronze.printSchema() 
        # table_bronze.show(10, truncate=False) 

        # Check for empty source table 
        source_empty(table_bronze)
        
        rows_read = get_rows_count(table_bronze)
        logger.info(f"Read {rows_read} records from {SOURCE_TABLE}") 

        # Required column validation 
        required_columns_check(table_bronze, *required_columns) 
        logger.info(f"All required columns exists in the {SOURCE_TABLE}") 

        # Schema evolution 
        # Cached columns to avoid repeated analyze RPC calls 
        bronze_columns_list = table_bronze.columns
        bronze_columns = set(bronze_columns_list)
        expected_columns = set(column_list)
        new_columns = bronze_columns - expected_columns
        if new_columns: 
            logger.warning(f"New columns detected in {SOURCE_TABLE}: {new_columns}")
            # Strict mode: ignore new columns 

        # Select the columns 
        table_clean = table_bronze.select(*column_list)
        
        # Standardize the data 
        # Trim the columns 
        table_clean = trim_columns(table_clean, *columns_to_trim)

        # InitCap columns 
        table_clean = initcap_columns(table_clean, *columns_to_initcap)
        
        # Lowercase columns 
        table_clean = lowercase_columns(table_clean, *columns_to_lowercase)
        logger.info(f"Data standardization completed for the {SOURCE_TABLE}")
        
        table_clean.printSchema() 

        # Cast datatypes 
        table_clean = cast_columns(table_clean, **type_mapping) 
        
        table_clean.printSchema() 
        
        logger.info(f"Data type casting completed for the {SOURCE_TABLE}")

        # Check for null customer ids 
        if table_clean.filter(F.col(primary_key).isNull()).count() > 0: 
            raise ValueError(f"Null {primary_key}s found in the {SOURCE_TABLE}")

        # Check for Nulls, and Validate  
        null_count = get_null_count(table_clean)
        if null_count > 0:
            raise Exception(f"Found {null_count} rows with null values in the table")
        
        # 2. Check for invalid values 
        if validation_rules: 
            validate_business_rules(table_clean, validation_rules, SOURCE_TABLE)
            logger.info(f"Business rule validation completed for {SOURCE_TABLE}")
        logger.info(f"Data validation completed for the {SOURCE_TABLE}")

        # Check for duplicates 
        duplicates = get_duplicate_records(table_clean, primary_key) 
        if duplicates > 0: 
            raise Exception(f"Duplicate records found in the {SOURCE_TABLE}")  

        # Deduplication logic to keep only latest records based on modified_date - SCD1 
        table_latest = deduplicate_source(table_clean, primary_key, watermark_column) 
        logger.info(f"Deduplication completed for the {SOURCE_TABLE}")

        # Add metadata and write to the TARGET_TABLE 
        add_metadata(table_latest, CATALOG, BRONZE_SCHEMA, source, SILVER_SCHEMA, target, pipeline_id, primary_key, watermark_column) 
        logger.info(f"Data written to the {TARGET_TABLE}")

        # Verify the output 
        silver_df = spark.read.table(TARGET_TABLE) 
        silver_df.printSchema()
        # Row count 
        rows_written = table_latest.count() 
        # Uniqueness 
        duplicates = get_duplicate_records(silver_df, primary_key)
        if duplicates > 0: 
            raise Exception(f"Duplicate records found in the {TARGET_TABLE}") 
        # 1. Check for Nulls 
        null_count = get_null_count(silver_df)
        if null_count > 0:
            raise Exception(f"Found {null_count} rows with null values in the {TARGET_TABLE}")
        
        # 2. Check for invalid values 
        if validation_rules:
            validate_business_rules(silver_df, validation_rules, TARGET_TABLE)
            logger.info(f"Business rule validation for {TARGET_TABLE} completed")
        logger.info(f"Data validation for {TARGET_TABLE} completed")
        logger.info(f"Silver pipeline for {TARGET_TABLE} completed successfully")
        status = "SUCCESS"

        # Calculate new watermark 
        if rows_written > 0: 
            new_watermark = table_latest.agg(F.max(F.col(watermark_column))).collect()[0][0]
            logger.info(f"New watermark: {new_watermark}")
        else: 
            new_watermark = watermark_value 
            logger.info(f"No new data, watermark value unchanged: {new_watermark}")
        
    except Exception as e: 
        status = "FAILURE"
        error_message = str(e) 
        logger.exception(f"Silver {TARGET_TABLE} pipeline failed: {error_message}")
        raise 

    finally: 
        end_time = datetime.now() 
        try: 
            write_audit_log(pipeline_name, target, start_time, end_time, rows_read, rows_written, status, error_message, CATALOG, AUDIT_SCHEMA, AUDIT_TABLENAME, batch_id, watermark_column, new_watermark) 
            logger.info(f"Audit for {TARGET_TABLE} is written successfully")
        except Exception as e: 
            logger.exception(f"Error writing audit log for {TARGET_TABLE}: {e}")
