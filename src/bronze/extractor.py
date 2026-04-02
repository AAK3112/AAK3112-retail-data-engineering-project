
def read_bronze_orders(spark, config, logger):
    
    logger.info("Reading Bronze Retail Table")
    
    df = (
         spark.read \
              .format("delta") \
              .load(config.bronze_path)
        )
         
    logger.info(f"Bronze Row Reads: {df.count()}")
    
    return df