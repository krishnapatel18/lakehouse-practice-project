# Imports
from datetime import datetime

# # Capture the start time 
# start_time = datetime.now()

import sys
import importlib
sys.path.append("/Workspace/Users/krishnasumanbhaipatel@gmail.com/Practice Folder/lakehouse-practice-project")

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
from functools import reduce
from operator import or_

import src.utils.logger
importlib.reload(src.utils.logger)
# from src.utils.logger import copy_logs_to_uc_volume 
from src.utils.logger import get_logger 

from src.validation.quality_checks import *
from src.validation.validation_rules import CUSTOMER_VALIDATIONS
from src.utils.audit import *

# # Create a logger 
# # logger = get_logger("bronze.customers") 
# # logger = get_logger("bronze.customers", log_to_file=True, log_dir="/Volumes/catalog_project1/bronze/logs")
# logger = get_logger("bronze.customers", log_to_file=True)

# Config variables
CATALOG = "catalog_project1"
SOURCE_SCHEMA = "source1"
BRONZE_SCHEMA = "bronze"
AUDIT_SCHEMA = "audit" 
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# # Table name
# SOURCE_TABLE = "customers_raw"
# BRONZE_TABLE = "customers" 
AUDIT_TABLENAME = "pipeline_run_audit"
# AUDIT_TABLENAME = "pipeline_audit"

# Volume name 
LOGS_VOLUME = "logs"

# Source path
# path = "catalog_project1.source1.customers_raw"

# Column names 
# BRONZE_WATERMARK_COLUMN = "modified_date" 