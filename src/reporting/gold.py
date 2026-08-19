import sys

# Add project root to sys.path so 'src' becomes importable
project_path = "/Workspace/Users/krishnasumanbhaipatel@gmail.com/Practice Folder/lakehouse-practice-project"
if project_path not in sys.path:
    sys.path.insert(0, project_path) 

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType 
spark = SparkSession.builder.getOrCreate()

import pyspark.sql.functions as F 
from pyspark.sql.window import Window
import uuid 
from delta.tables import DeltaTable 
from src.utils.audit import write_audit_log 
from datetime import datetime 
from src.utils.config import get_logger, CATALOG, GOLD_SCHEMA, AUDIT_SCHEMA, AUDIT_TABLENAME, SILVER_SCHEMA, get_rows_count
from src.orchestration.batch import get_batch_id 

def get_last_watermark(spark, CATALOG, AUDIT_SCHEMA, AUDIT_TABLENAME, target, pipeline_name): 
    audit_table = f"{CATALOG}.{AUDIT_SCHEMA}.{AUDIT_TABLENAME}"

    if not spark.catalog.tableExists(audit_table): 
        return None
    
    last_run = spark.table(audit_table)\
        .filter((F.col("table_name") == target) & (F.col("status") == F.lit("SUCCESS")) & (F.col("pipeline_name") == pipeline_name))\
        .orderBy(F.col("end_time").desc())\
        .limit(1)\
        .select("watermark_value").collect()
    if last_run and last_run[0]['watermark_value'] is not None: 
        return last_run[0]['watermark_value']
    else: 
        return None

def add_metadata(table, target, SOURCE_TABLE, pipeline_id, TARGET_TABLE, surrogate_key):  
            # Add Surrogate key, and Metadata - effective_from, effective_to, is_current, ingestion_timestamp, updation_timestamp,  source_table, pipeline_id 
            table = table.withColumns({
                surrogate_key: F.monotonically_increasing_id() + 1, 
                "effective_from": F.current_timestamp(), 
                "effective_to": F.lit("9999-12-31"), 
                "is_current": F.lit(True), 
                "ingestion_timestamp": F.current_timestamp(), 
                "updation_timestamp": F.current_timestamp(),
                "_source_table": F.lit(SOURCE_TABLE), 
                "_ingestion_pipeline_id": F.lit(pipeline_id)
            })

            # write to table 
            table.write.mode("append").format("delta").saveAsTable(TARGET_TABLE)
            
            print(f"{target} table is saved as {TARGET_TABLE}")

def add_scd_metadata(table, target, SOURCE_TABLE, pipeline_id, TARGET_TABLE, window_spec, max_key, surrogate_key):  
            # Add Surrogate key, and Metadata - effective_from, effective_to, is_current, ingestion_timestamp, updation_timestamp,  source_table, pipeline_id 
            table = table.withColumns({
                surrogate_key: F.row_number().over(window_spec) + max_key, 
                "effective_from": F.current_timestamp(), 
                "effective_to": F.lit("9999-12-31"), 
                "is_current": F.lit(True), 
                "ingestion_timestamp": F.current_timestamp(), 
                "updation_timestamp": F.current_timestamp(),
                "_source_table": F.lit(SOURCE_TABLE), 
                "_ingestion_pipeline_id": F.lit(pipeline_id)
            })

            # write to table 
            table.write.mode("append").format("delta").saveAsTable(TARGET_TABLE)
            
            print(f"{target} table is saved as {TARGET_TABLE}") 

# Gold table transformation function 
def create_gold_table(scd1_column_list, 
                      scd2_column_list, 
                      source, 
                      target, 
                      watermark_column, 
                      surrogate_key, 
                      primary_key, 
                      pipeline_name = "gold_pipeline", 
                      CATALOG = CATALOG, 
                      AUDIT_SCHEMA = AUDIT_SCHEMA, 
                      AUDIT_TABLENAME = AUDIT_TABLENAME, 
                      SILVER_SCHEMA = SILVER_SCHEMA, 
                      GOLD_SCHEMA = GOLD_SCHEMA): 
    
    pipeline_id = str(uuid.uuid4())
    batch_id = get_batch_id(spark, CATALOG, AUDIT_SCHEMA, AUDIT_TABLENAME)
    error_message = None 
    rows_read = 0
    rows_written = 0
    rows_updated = 0
    # Generating the new key window 
    # window_spec = Window.orderBy(F.monotonically_increasing_id())
    new_watermark = None

    try: 
        start_time = datetime.now()
        logger = get_logger(f"gold.{target}", log_to_file=True)
        logger.info(f"Starting Gold pipeline at {start_time}")

        # Fetch latest watermark value 
        watermark_value = get_last_watermark(spark, CATALOG, AUDIT_SCHEMA, AUDIT_TABLENAME, target, pipeline_name)

        SOURCE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.{source}"
        TARGET_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.{target}"

        # Fetch records based on watermark value 
        if watermark_value == None:  # Empty list or None
            silver_df = spark.table(SOURCE_TABLE)
        else: 
            # Use >= to include records with the same watermark timestamp
            silver_df = spark.table(SOURCE_TABLE).filter(F.col(watermark_column) > watermark_value)

        rows_read = get_rows_count(silver_df)
        logger.info(f"Read {rows_read} records from {SOURCE_TABLE} table")

        if spark.catalog.tableExists(TARGET_TABLE): 
            logger.info(f"Incremental load from {SOURCE_TABLE} table")
            # Get all current records for comparison
            current_df = spark.table(TARGET_TABLE).filter("is_current = true")

            # Fetch max surrogate_key value from ALL records (not just current) to ensure uniqueness
            max_key = spark.table(TARGET_TABLE).agg(F.max(surrogate_key)).collect()[0][0]
            max_key = max_key if max_key is not None else 0 
            window_spec = Window.orderBy(F.monotonically_increasing_id())

            # Compare with silver table records 
            comparision_df = (
               silver_df.alias('s')
                .join(
                    current_df.alias('d'), 
                    on = F.expr(f"s.{primary_key} = d.{primary_key}"), 
                    how = "left"
                )
            )
            # Cache schema once to avoid repeated Analyze RPCs
            comparison_schema = comparision_df.schema

            # New records (products that don't exist in Gold at all)
            new_products_df = comparision_df.filter(F.expr(f"d.{primary_key} IS NULL"))
            new_products_count = new_products_df.count()
            if new_products_count > 0: 
                new_products_df = comparision_df.filter(F.expr(f"d.{primary_key} IS NULL")).select("s.*")
                add_scd_metadata(new_products_df, target, SOURCE_TABLE, pipeline_id, TARGET_TABLE, window_spec, max_key, surrogate_key)
                rows_written = new_products_count 
                logger.info(f"Inserted {new_products_count} new records to {TARGET_TABLE} table")

                # Update max_key after inserting new records so SCD2 processing uses correct keys
                max_key = spark.table(TARGET_TABLE).agg(F.max(surrogate_key)).collect()[0][0]
                max_key = max_key if max_key is not None else 0

            # SCD2 - expire old records, insert new records 
            if scd2_column_list: 
                # Build SCD2 change detection condition: if any SCD2 column differs between source and target, mark as changed
                scd2_conditions = " OR ".join([f"NOT (s.{col} <=> d.{col})" for col in scd2_column_list])
                scd2_df = comparision_df.filter(F.expr(f"d.{primary_key} IS NOT NULL AND ({scd2_conditions})"))
                
                # Insert new versions 
                new_versions = scd2_df.select("s.*")
                new_version_rows = new_versions.collect()
                new_versions = spark.createDataFrame(new_version_rows, new_versions.schema)
                scd2_count = new_versions.count()
            else: 
                # If there are no SCD2 columns, create an empty DataFrame for scd2_df to ensure downstream logic works without errors
                scd2_df = spark.createDataFrame([], schema=comparison_schema)
                scd2_count = 0 
            
            if scd2_count > 0:                
                # Expire old records 
                expire_old = scd2_df.select(f"d.{surrogate_key}").distinct()
                expire_keys = [str(r[surrogate_key]) for r in expire_old.collect()]
                spark.sql(f"""
                          UPDATE {TARGET_TABLE} 
                          SET is_current = false,
                          effective_to = current_timestamp(), 
                          updation_timestamp = current_timestamp()
                          WHERE {surrogate_key} IN ({','.join(expire_keys)})
                """)

                # Update max_key after expire operation to get correct surrogate keys
                max_key = spark.table(TARGET_TABLE).agg(F.max(surrogate_key)).collect()[0][0]
                max_key = max_key if max_key is not None else 0
                window_spec = Window.orderBy(F.monotonically_increasing_id()) 
                            
                if scd2_count > 0:
                    logger.info("Rows going to get inserted: ", scd2_count)
                    new_versions.show(truncate = False)
                    # Insert new version 
                    add_scd_metadata(new_versions, target, SOURCE_TABLE, pipeline_id, TARGET_TABLE, window_spec, max_key, surrogate_key)
                    rows_written += scd2_count
                    logger.info(f"Inserted {scd2_count} new versions (SCD2) to {TARGET_TABLE} table")
                    logger.info(f"SCD2: Expired and inserted new versions for {scd2_count} records")
                else:
                    logger.warning(f"SCD2: No new versions to insert despite scd2_count={scd2_count}")
                
            # SCD1 - update 
            # Exclude records that already went through SCD2 processing
            if scd2_count > 0:
                scd2_product_ids = scd2_df.select(f"s.{primary_key}").distinct()
            else:
                # Create empty DataFrame with only primary_key column (matching the schema when scd2_count > 0)
                scd2_product_ids = spark.createDataFrame([], f"{primary_key} INT")
            
            if scd1_column_list: 
                scd1_conditions = " OR ".join([f"NOT (s.{col} <=> d.{col})" for col in scd1_column_list])
                scd1_filter = f"d.{primary_key} IS NOT NULL AND ({scd1_conditions})"
                scd1_df = comparision_df.filter(F.expr(scd1_filter))
            else: 
                scd1_df = spark.createDataFrame([], schema=comparison_schema)
            scd1_count = scd1_df.count()
            if scd1_count > 0:
                # Then exclude records that were processed in SCD2
                scd1_df = scd1_df.join(scd2_product_ids, on=F.expr(f"source.{primary_key} = {primary_key}"), how="left_anti")
                scd1_after_exclusion_count = scd1_df.count()

                if scd1_after_exclusion_count > 0:
                    update_dict = {col: F.col(f"source.{col}") for col in scd1_column_list + [watermark_column]}
                    update_dict["updation_timestamp"] = F.current_timestamp()

                    # Merge logic 
                    dim_table = DeltaTable.forName(spark, TARGET_TABLE)
                    dim_table.alias("target")\
                        .merge(
                            scd1_df.alias("source"),
                            F.expr(f"target.{primary_key} = source.{primary_key} AND target.is_current = true")
                        )\
                        .whenMatchedUpdate(set = update_dict)\
                        .execute()
                    rows_updated += scd1_after_exclusion_count
                    logger.info(f"Updated {scd1_after_exclusion_count} records (SCD1) in {TARGET_TABLE}")
            # Update watermark if ANY data was processed (inserts or updates)
            if rows_written > 0 or rows_updated > 0:
                new_watermark = silver_df.agg(F.max(F.col(watermark_column))).collect()[0][0]
                logger.info(f"New watermark: {new_watermark}")
            else:
                new_watermark = watermark_value
                logger.info(f"No new data processed, watermark unchanged: {new_watermark}")
            status = "SUCCESS"
            logger.info(f"Incremental load completed for {TARGET_TABLE}")
            logger.info(f"Gold pipeline for {TARGET_TABLE} completed successfully")

        else: 
            # Initial load 
            logger.info(f"Initial load from {SOURCE_TABLE} table")
            add_metadata(silver_df, target, SOURCE_TABLE, pipeline_id, TARGET_TABLE, surrogate_key)
            rows_written = silver_df.count()
            logger.info(f"Written {rows_written} records to {TARGET_TABLE}")
            if rows_written > 0:
                new_watermark = silver_df.agg(F.max(F.col(watermark_column))).collect()[0][0]
                logger.info(f"New watermark: {new_watermark}")
            else: 
                new_watermark = watermark_value
                logger.info(f"No new data, watermark value unchanged: {new_watermark}")
            status = "SUCCESS"
            logger.info(f"Initial load completed for {TARGET_TABLE}")
            logger.info(f"Gold pipeline for {TARGET_TABLE} completed successfully")

    except Exception as e: 
        status = "FAILURE"
        error_message = str(e)
        logger.exception(f"{pipeline_name} failed: {error_message}")
        raise 

    finally: 
        end_time = datetime.now()
        try: 
            write_audit_log(pipeline_name, target, start_time, end_time, rows_read, rows_written, status, error_message, CATALOG, AUDIT_SCHEMA,     AUDIT_TABLENAME, batch_id, watermark_column, new_watermark)
            logger.info(f"Audit for {TARGET_TABLE} is written successfully")
        except Exception as e: 
            logger.exception(f"Error writing audit log for {TARGET_TABLE}: {e}")
 