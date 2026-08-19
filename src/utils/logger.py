import logging
import sys
import os
from datetime import datetime

# # Import dbutils if available (Databricks environment)
# # Commented out: Not needed for workspace filesystem logging
# try:
#     from pyspark.dbutils import DBUtils
#     from pyspark.sql import SparkSession
#     spark = SparkSession.builder.getOrCreate()
#     dbutils = DBUtils(spark)
#     DBUTILS_AVAILABLE = True
# except:
#     DBUTILS_AVAILABLE = False


def get_logger(name: str, log_to_file: bool = False, log_dir: str = None) -> logging.Logger:
    """
    Creates and returns a configured logger.
    
    Args:
        name: Logger name (e.g., "bronze.customers")
        log_to_file: If True, also write logs to a file
        log_dir: Directory to store log files.
                 Defaults to project logs folder if not specified.
    """

    logger = logging.getLogger(name)

    # Prevent duplicate handlers if the notebook is run multiple times
    if logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    )

    # Console handler (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Optional: File handler for persistent logs
    if log_to_file:
        # Default to project logs folder if not specified
        if log_dir is None:
            log_dir = "/Workspace/Users/krishnasumanbhaipatel@gmail.com/Practice Folder/lakehouse-practice-project/logs"
        
        # Create directory if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)
        
        log_filename = f"{name.replace('.', '_')}_{datetime.now().strftime('%Y%m%d')}.log"
        log_file = os.path.join(log_dir, log_filename)
        
        # Use standard FileHandler
        file_handler = logging.FileHandler(log_file, mode='a')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        logger.info(f"Logging to file: {log_file}")

    return logger


# # UC VOLUMES LOGGING (COMMENTED OUT)
# # Uncomment this function if you want to use UC Volumes for log storage
# # This uses a two-step approach: log to /tmp, then copy to UC Volume
# 
# def copy_logs_to_uc_volume(logger: logging.Logger) -> bool:
#     """
#     Copy log file from /tmp to Unity Catalog Volume.
#     Call this at the end of your pipeline.
#     
#     Returns:
#         True if copy succeeded, False otherwise
#     """
#     if not hasattr(logger, '_uc_volume_dir'):
#         return False
#     
#     if not DBUTILS_AVAILABLE:
#         logger.error("dbutils not available, cannot copy to UC Volume")
#         return False
#     
#     try:
#         # Ensure UC Volume directory exists
#         dbutils.fs.mkdirs(logger._uc_volume_dir)
#         
#         # Read content from /tmp log file
#         src = logger._tmp_log_file
#         dst = os.path.join(logger._uc_volume_dir, logger._uc_log_filename)
#         
#         # Read from /tmp and write to UC Volume using standard Python file operations
#         with open(src, 'r') as src_file:
#             content = src_file.read()
#         
#         with open(dst, 'w') as dst_file:
#             dst_file.write(content)
#         
#         logger.info(f"Logs copied to UC Volume: {dst}")
#         return True
#         
#     except Exception as e:
#         logger.error(f"Failed to copy logs to UC Volume: {e}")
#         return False
