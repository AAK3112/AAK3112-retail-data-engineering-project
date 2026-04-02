

from pyspark.sql import functions as f

def update_file_tracker(batch_df, config, batch_id):
    
    files_df = (
        batch_df.select("source_file_name") \
                .distinct() \
                .withColumn("ingestion_time", f.current_timestamp()) \
                .withColumn("batch_id", f.lit(batch_id))
    )
    
    (files_df.write \
             .format("delta") \
             .mode("append") \
             .save(config.tracker_path)
             )