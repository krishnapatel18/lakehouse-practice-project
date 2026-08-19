import pyspark.sql.functions as F 
from pyspark.sql.window import Window
from functools import reduce
from operator import or_


def get_rows_count(table):
    rows_count = table.count()
    return rows_count


def get_duplicate_records(table, *columns):
    duplicate_count = table.groupBy(*columns).count().filter(F.col("count") > 1).count()
    return duplicate_count


def get_null_count(table): 
    null_count = table.filter(reduce(or_, [F.col(c).isNull() for c in table.columns])).count()
    return null_count


def source_empty(source):
    # source = spark.read.table(f"{CATALOG}.{SOURCE_SCHEMA.lower()}.{SOURCE_TABLE}")
    if source.isEmpty():
        # raise Exception ("Source table is empty.")
        print("Source table is empty")
    else: 
        print("Pipeline has started...") 


def required_columns_check(table, *required_columns): 
    actual_columns = set(table.columns)
    missing_columns = set(required_columns) - actual_columns 

    if missing_columns: 
        raise ValueError(f"Missing columns {missing_columns} in the table") 

# Check if table exists 
def table_exists(spark, table_name): 
    if not spark.catalog.tableExists(table_name): 
        raise ValueError(f"Table {table_name} does not exist.") 

# Deduplication logic to fetch latest records from source  
def deduplicate_source(table, primary_key, watermark_column): 
    window_spec = (
        Window
        .partitionBy(primary_key)
        .orderBy(F.desc(watermark_column))
    ) 

    latest_table = (
        table
        .withColumn("_row_number", F.row_number().over(window_spec))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    ) 
    return latest_table 


def cast_columns(df, **type_mapping):
    cast_cols = {
        col_name: F.col(col_name).cast(col_type) 
        for col_name, col_type in type_mapping.items()} 
    return df.select(*cast_cols.values())


def trim_columns(table, *columns_to_trim): 
    trimmed_table = table.select(*[F.trim(F.col(col)).alias(col) if col in columns_to_trim else F.col(col) for col in table.columns]) 
    return trimmed_table 
    

def initcap_columns(table, *columns_to_initcap): 
    initcap_table = table.select(*[F.initcap(F.col(col)).alias(col) if col in columns_to_initcap else F.col(col) for col in table.columns]) 
    return initcap_table 
        

def lowercase_columns(table, *columns_to_lowercase): 
    lowercase_table = table.select(*[F.lower(F.col(col)).alias(col) if col in columns_to_lowercase else F.col(col) for col in table.columns]) 
    return lowercase_table
        

def validate_business_rules(table, validation_rules, table_name):
    if not validation_rules:
        return  # No validations to perform
    
    validation_conditions = []
    
    # Numeric minimum validations
    for col_name, min_val in validation_rules.get("numeric_min", {}).items():
        validation_conditions.append(F.col(col_name) < min_val)
    
    # Numeric maximum validations
    for col_name, max_val in validation_rules.get("numeric_max", {}).items():
        validation_conditions.append(F.col(col_name) > max_val)
    
    # Regex pattern validations
    for col_name, regex_pattern in validation_rules.get("regex", {}).items():
        validation_conditions.append(F.col(col_name).rlike(regex_pattern) == False)
    
    # Date minimum validations
    for col_name, min_date in validation_rules.get("date_min", {}).items():
        validation_conditions.append(F.col(col_name) < min_date)
    
    # Date maximum validations
    for col_name, max_date in validation_rules.get("date_max", {}).items():
        validation_conditions.append(F.col(col_name) > max_date)
    
    # Execute validation if there are any conditions
    if validation_conditions:
        invalid_records = table.filter(
            reduce(or_, validation_conditions)
        ).count()
        
        if invalid_records > 0:
            raise Exception(f"Invalid records: {invalid_records} found in {table_name}")