from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType, BooleanType, DateType
from datetime import datetime, timedelta

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Configuration
CATALOG = "catalog_project1"
GOLD_SCHEMA = "gold"
TABLE_NAME = "dim_date"
TARGET_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.{TABLE_NAME}"

# Define date range (7 years: 2021-2027)
start_date = datetime(2021, 1, 1)
end_date = datetime(2027, 12, 31)

print(f"Generating date dimension from {start_date.date()} to {end_date.date()}")

# Generate date range
date_list = []
current_date = start_date
while current_date <= end_date:
    date_list.append((current_date,))
    current_date += timedelta(days=1)

print(f"Total dates to generate: {len(date_list)}")

# Create DataFrame with single date column
df = spark.createDataFrame(date_list, ["full_date"])

# Generate all date dimension attributes
dim_date = df.select(
    # date_key: Integer format YYYYMMDD (e.g., 20240115)
    F.date_format(F.col("full_date"), "yyyyMMdd").cast(IntegerType()).alias("date_key"),
    
    # full_date: Date
    F.col("full_date").cast(DateType()).alias("full_date"),
    
    # day: Day of month (1-31)
    F.dayofmonth(F.col("full_date")).alias("day"),
    
    # day_name: Full day name (Monday, Tuesday, etc.)
    F.date_format(F.col("full_date"), "EEEE").alias("day_name"),
    
    # day_of_week: Day of week (1=Sunday, Saturday=7)
    F.dayofweek(F.col("full_date")).alias("day_of_week"),

    # day_of_Week: ISO day of week(1 = Monday, 7 = Sunday)
    ((F.dayofweek(F.col("full_date")) + 5) % 7 + 1).alias("day_of_week_iso"),
    
    # week_of_year: Week number (1-53)
    F.weekofyear(F.col("full_date")).alias("week_of_year"),
    
    # month: Month number (1-12)
    F.month(F.col("full_date")).alias("month"),
    
    # month_name: Full month name (January, February, etc.)
    F.date_format(F.col("full_date"), "MMMM").alias("month_name"),
    
    # quarter: Quarter (1-4)
    F.quarter(F.col("full_date")).alias("quarter"),
    
    # year: Year (e.g., 2024)
    F.year(F.col("full_date")).alias("year"),
    
    # is_weekend: True if Saturday (7) or Sunday (1)
    F.when(F.dayofweek(F.col("full_date")).isin([1, 7]), True).otherwise(False).alias("is_weekend"),
    
    # is_month_start: True if first day of month
    F.when(F.dayofmonth(F.col("full_date")) == 1, True).otherwise(False).alias("is_month_start"),
    
    # is_month_end: True if last day of month
    F.when(
        F.dayofmonth(F.col("full_date")) == F.dayofmonth(F.last_day(F.col("full_date"))),
        True
    ).otherwise(False).alias("is_month_end")
)

# Show sample data
print("\nSample data:")
dim_date.show(10, truncate=False)

# Print schema
print("\nSchema:")
dim_date.printSchema()

# Write to Gold table
print(f"\nWriting to {TARGET_TABLE}...")
dim_date.write.mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

print(f"Successfully created {TARGET_TABLE}")

# Verify
result = spark.sql(f"SELECT COUNT(*) as total_dates FROM {TARGET_TABLE}").collect()[0]['total_dates']
print(f"Total dates in table: {result}")

# Show some examples
print("\nSample records:")
spark.sql(f"""
    SELECT 
        date_key,
        full_date,
        day_name,
        is_weekend,
        is_month_start,
        is_month_end,
        quarter,
        year
    FROM {TARGET_TABLE}
    ORDER BY full_date
    LIMIT 5
""").show(truncate=False)