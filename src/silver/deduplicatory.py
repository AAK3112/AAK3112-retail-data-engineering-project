

from pyspark.sql import functions as f 
from pyspark.sql.window import Window

def deduplicate_orders(df, logger):
    
    logger.info("Deduplicating Orders")
    
    window_spec = Window.partitionBy("order_id") \
                        .orderBy(f.col("ingestion_timestamp").desc())
                        
    dedup_df = (
        df.withColumn("rn", f.row_number() \
                                    .over(window_spec)) \
                                    .filter("rn = 1") \
                                    .drop("rn")
    )
                                    
    logger.info(f"Rows after Deduplication: {dedup_df.count()}")
    
    return dedup_df