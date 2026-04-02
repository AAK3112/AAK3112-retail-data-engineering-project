
from pathlib import Path
from urllib.parse import quote
from pyspark.sql import functions as f 
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from urllib.parse import urlparse, unquote

def normalize_path(path: str) -> str:
    parsed = urlparse(path)
    
    # Extract actual file path and decode %20 etc
    clean_path = unquote(parsed.path)
    
    # Windows fix → remove leading /
    if clean_path.startswith("/"):
        clean_path = clean_path[1:]
        
    return clean_path.lower()

    
def get_new_files(spark, config, logger):

    from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

def get_new_files(spark, config, logger):

    logger.info("Scanning raw folder for CSV files")

    files_df = (
        spark.read.format("binaryFile")
        .option("pathGlobFilter", "*.csv")
        .load(config.raw_path)
        .select("path")
    )

    all_files_raw = [row["path"] for row in files_df.collect()]
    logger.info(f"Total files found in raw: {len(all_files_raw)}")

    # Normalize scanned files
    all_files = [normalize_path(p) for p in all_files_raw]

    # FIRST RUN
    if not DeltaTable.isDeltaTable(spark, config.tracker_path):
        logger.warning("Tracker table not found. First run")
        return all_files_raw   # return original spark paths for reading

    tracker_df = spark.read.format("delta").load(config.tracker_path)
    processed_raw = [row["source_file_name"] for row in tracker_df.collect()]

    # Normalize processed files
    processed_files = [normalize_path(p) for p in processed_raw]

    logger.info(f"Already processed files: {len(processed_files)}")

    # Compare normalized paths
    new_files_normalized = list(set(all_files) - set(processed_files))

    # Map back to original Spark URIs for reading
    new_files = [
        raw for raw in all_files_raw
        if normalize_path(raw) in new_files_normalized
    ]

    logger.info(f"New Files detected: {len(new_files)}")

    return new_files