
from delta.tables import DeltaTable

def merge_silver_orders(spark, df, config, logger):
    
    target_path = config.silver_path + "/silver_orders"
    
    logger.info("Starting merge into silver orders")
    
    #First Run. Silver Table does not exist. create a new one
    
    if not DeltaTable.isDeltaTable(spark, target_path):
        
        logger.warning("Silver table not found. Creating a new table")
        
        df.write \
          .format("delta") \
          .mode("overwrite") \
          .save(target_path)
          
        logger.info("Silver table created successfully")
        
        return
    
    #load the existing silver table
    
    delta_table = DeltaTable.forPath(spark, target_path)
    
    #Set Merge condition
    
    merge_condition = "t.order_id == s.order_id"
    
    #set update condition
    
    update_set = {
        "customer_name" : "s.customer_name",
        "product_name" : "s.product_name", 
        "order_date" : "s.order_date", 
        "email" : "s.email", 
        "price" : "s.price", 
        "quantity" : "s.quantity",
        "silver_loaded_at" : "current_timestamp()"
    }
    
    #Execute Merge (Upsert)
    
    (
        delta_table.alias("t") \
                   .merge(df.alias("s"), merge_condition) \
                   .whenMatchedUpdate(set = update_set) \
                   .whenNotMatchedInsertAll() \
                   .execute()
    )
    
    logger.info("Merge completed successfully")
    
def write_silver_quarantine(df, config, logger):
    
    logger.info('Writing Silver Quarantine records')
    
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .save(config.quarantine_path + "/quarantine_orders")