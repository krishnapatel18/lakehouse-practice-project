# dbutils.library.restartPython()

from pyspark.sql import SparkSession 
# from pyspark.sql.functions import *
# Here, instead of using * (wildcard), use specific to avoid functions getting overwritten (ex. reduce of python and pyspark)
# from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.functions import col, lit, current_timestamp, max as max_
from functools import reduce
from operator import or_
    
import sys

# Add project root to sys.path so 'src' becomes importable
project_path = "/Workspace/Users/krishnasumanbhaipatel@gmail.com/Practice Folder/lakehouse-practice-project"
if project_path not in sys.path:
    sys.path.insert(0, project_path) 

# Now import specific names after reload
from src.utils.audit import write_audit_log
from src.utils.config import get_logger, source_empty, get_rows_count, get_duplicate_records, get_null_count, CATALOG, BRONZE_SCHEMA, LOGS_VOLUME, SOURCE_SCHEMA, AUDIT_SCHEMA, AUDIT_TABLENAME 

import uuid
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# Extract Metadata 
def add_metadata(source, BRONZE_TABLE, CATALOG, SOURCE_SCHEMA, SOURCE_TABLE, BRONZE_SCHEMA, job_id = None, pipeline_id = None): 
    # Get the active Spark session
    # spark = SparkSession.builder.getOrCreate()
    
    # Get job_id and pipeline_id from Databricks runtime context (Spark Connect compatible)
    try:
        if job_id is None:
            job_id = str(int(datetime.now().timestamp() * 1000))
        if pipeline_id is None:
            pipeline_id = str(uuid.uuid4())
    except Exception as e:
        # If unable to get context, use the provided values or None
        print(f"Error getting job_id and pipeline_id: {e}")
        raise e

    # Use .withColumns() for better performance (single pass instead of nested plans)
    bronze_df = source.withColumns({
        "_ingestion_timestamp": current_timestamp(),
        "_source_file": col("_metadata.file_path"),
        "_source_table": lit(f"{CATALOG}.{SOURCE_SCHEMA}.{SOURCE_TABLE}"),
        "_ingestion_job_id": lit(job_id),
        "_ingestion_pipeline_id": lit(pipeline_id)
    })

    # spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}")
    bronze_df.write.mode("append")\
                     .format("delta")\
                     .option("mergeSchema", "true")\
                     .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}") 

    return f"{BRONZE_TABLE} table is saved as {CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}"

def get_last_success_run(CATALOG, AUDIT_SCHEMA, AUDIT_TABLENAME, pipeline_name, target): 
    if spark.catalog.tableExists(f"{CATALOG}.{AUDIT_SCHEMA}.{AUDIT_TABLENAME}"):
        audit_df = spark.table(f"{CATALOG}.{AUDIT_SCHEMA}.{AUDIT_TABLENAME}") 
        last_success = (
            audit_df.filter(
                (col("pipeline_name") == pipeline_name) &
                (col("table_name") == target) &
                (col("status") == lit("SUCCESS"))
            )
            .orderBy(col("end_time").desc())
            .limit(1)
        )
        last_success_run = last_success.first() 
    else:
        last_success_run = None  # Fixed: set the correct variable

    return last_success_run 

def get_watermark_value(last_success_run): 
    # table.agg({watermark_column: "max"}) returns a DataFrame with one row and one column (max value)
    # Without .first(), it returns the DataFrame itself
    # Without [0], .first() returns a Row object, and one need [0] to get the value
    # return table.agg({watermark_column: "max"}).first()[0] 

    if last_success_run is None:
        watermark_value = None
    else:
        watermark_value = last_success_run["watermark_value"]
    return watermark_value

# Create Bronze Ingestion function 
def ingest_table(source, 
                 target, 
                 pipeline_name, 
                 primary_key, 
                 batch_id, 
                 bronze_watermark_column, 
                 AUDIT_TABLENAME=AUDIT_TABLENAME,
                 CATALOG=CATALOG,
                 BRONZE_SCHEMA=BRONZE_SCHEMA,
                 LOGS_VOLUME=LOGS_VOLUME,
                 SOURCE_SCHEMA=SOURCE_SCHEMA,
                 AUDIT_SCHEMA=AUDIT_SCHEMA 
                ): 
    
    start_time = datetime.now()
    
    # Initialize variables before try block to avoid UnboundLocalError in finally
    rows_read = 0
    rows_written = 0
    new_watermark = None

    logger = get_logger(f"bronze.{target}", log_to_file=True)

    # Table name 
    SOURCE_TABLE = source
    BRONZE_TABLE = target
    
    try: 
        logger.info("Checking for last successful run")

        last_success_run = get_last_success_run(CATALOG, AUDIT_SCHEMA, AUDIT_TABLENAME, pipeline_name, target) 
        watermark_value = get_watermark_value(last_success_run) 
        logger.info(f"Fetched watermark value: {watermark_value}") 

        logger.info(f"Reading source {SOURCE_TABLE} table")
        # raw = spark.read.table(f"{CATALOG}.{SOURCE_SCHEMA.lower()}.{SOURCE_TABLE}")
        # source_empty(raw)

        if watermark_value is None: 
            # first run 
            raw = spark.read.table(f"{CATALOG}.{SOURCE_SCHEMA.lower()}.{SOURCE_TABLE}") 
        else: 
            # incremental run 
            raw = spark.read.table(f"{CATALOG}.{SOURCE_SCHEMA.lower()}.{SOURCE_TABLE}").filter(
                col(bronze_watermark_column) > lit(watermark_value)
            )
        
        # ============================================================
        # PRE-WRITE VALIDATIONS (BLOCKING)
        # ============================================================
        logger.info("Starting pre-write validations")
        
        # Check 1: Empty source (blocks write if empty)
        source_empty(raw)
        
        # Check 2: Count rows to read
        rows_read = get_rows_count(raw)
        logger.info(f"Rows read from source: {rows_read}")
        
        # Check 3: Verify watermark column exists
        if bronze_watermark_column not in raw.columns:
            raise ValueError(f"❌ Watermark column '{bronze_watermark_column}' not found in source table")
        
        raw.printSchema()
        logger.info(f"Pre-write validations passed. Source {SOURCE_TABLE} ready for ingestion.")

        # ============================================================
        # WRITE TO BRONZE
        # ============================================================
        logger.info(f"Starting Bronze {BRONZE_TABLE} ingestion")
        logger.info(f"Source table: {SOURCE_TABLE}")
        logger.info(f"Target table: {BRONZE_TABLE}")

        add_metadata(raw, BRONZE_TABLE, CATALOG, SOURCE_SCHEMA, SOURCE_TABLE, BRONZE_SCHEMA)
        logger.info(f"Bronze {BRONZE_TABLE} table written successfully.") 

        # ============================================================
        # POST-WRITE VALIDATIONS (NON-BLOCKING - for monitoring)
        # ============================================================
        logger.info("Starting post-write validations")
        
        # Calculate new watermark from the data we just wrote
        new_watermark = raw.agg(max_(col(bronze_watermark_column))).first()[0]
        
        # If no new data found (incremental run with no updates), use previous watermark
        if new_watermark is None:
            new_watermark = watermark_value
            logger.info(f"No new data found. Using previous watermark: {watermark_value}")
        else:
            logger.info(f"New watermark: {new_watermark}")
        
        # Read bronze table for validation
        bronze = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}")

        # rows_written = get_rows_count(bronze)
        # rows_written should equal rows_read (we append all rows we read)
        rows_written = rows_read
        logger.info(f"Rows written in this run: {rows_written}")

        # Cache columns to avoid repeated Analyze RPC calls (SCPAP001)
        bronze_columns = bronze.columns
        
        # Validation 1: Verify write success by checking new rows
        # For initial load: total should equal rows_read
        # For incremental: total should equal (previous_total + rows_read)
        total_bronze_rows = bronze.count()
        logger.info(f"Total rows in bronze table: {total_bronze_rows}")
        
        # For initial runs, verify all source rows were written
        if watermark_value is None and total_bronze_rows != rows_read:
            logger.error(f"❌ Write verification FAILED: Expected {rows_read} rows, found {total_bronze_rows}")
            raise ValueError(f"❌ Write verification failed: rows mismatch (expected {rows_read}, got {total_bronze_rows})")
        elif watermark_value is None:
            logger.info(f"Write verification passed: {total_bronze_rows} rows written")
        
        # Validation 2: Check for PRIMARY KEY duplicates
        pk_duplicate_count = get_duplicate_records(bronze, primary_key)
        if pk_duplicate_count > 0:
            logger.warning(f"⚠️ PRIMARY KEY duplicates found: {pk_duplicate_count} groups")
        else:
            logger.info(f"No primary key duplicates found")
        
        # Validation 3: Check for full row duplicates (excluding metadata columns)
        metadata_columns = ['_ingestion_timestamp', '_source_file', '_source_table', '_ingestion_job_id', '_ingestion_pipeline_id']
        source_columns = [c for c in bronze_columns if c not in metadata_columns]
        full_duplicate_count = get_duplicate_records(bronze, *source_columns)
        if full_duplicate_count > 0:
            logger.warning(f"⚠️ Full row duplicates found: {full_duplicate_count} groups")
        else:
            logger.info(f"No full row duplicates found")

        # Validation 4: Check for NULL values in any column
        null_count = get_null_count(bronze)
        if null_count > 0:
            logger.warning(f"⚠️ Rows with NULL values: {null_count}")
        else:
            logger.info(f"No NULL values found")
        
        bronze.printSchema() 
        logger.info(f"Post-write validations completed. Bronze {BRONZE_TABLE} ingestion finished successfully.")

        status = "SUCCESS"
        error_message = None

    except Exception as e: 
        logger.exception(f"❌ Bronze {BRONZE_TABLE} ingestion failed") 
        status = "FAILED"
        error_message = str(e)
        raise 

    finally: 
        end_time = datetime.now()
        try: 
            write_audit_log(pipeline_name, BRONZE_TABLE, start_time, end_time, rows_read, rows_written, status, error_message, CATALOG, AUDIT_SCHEMA, AUDIT_TABLENAME, batch_id, bronze_watermark_column, new_watermark)
            logger.info("Audit log written successfully")
        except Exception as e: 
            logger.exception(f"Audit log write failed: {e}")
            raise 

    return f"✅ Pipeline {pipeline_name} ran successfully"