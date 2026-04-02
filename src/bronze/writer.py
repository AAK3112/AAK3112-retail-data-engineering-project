
from bronze.file_tracker import update_file_tracker
from bronze.quality import apply_basic_quality_checks
from bronze.log_metrics import log_metrics

def write_bronze_batch(df, config, logger, batch_id):
  
  #Data Quality
  valid_df, invalid_df = apply_basic_quality_checks(df)
    
  #Metrics
  raw_count = df.count()
  valid_count = valid_df.count()
  invalid_count = invalid_df.count()
  
  log_metrics(logger, raw_count, valid_count, invalid_count)
   
  #Write Bronze
  valid_df.write \
          .format("delta") \
          .mode("append") \
          .partitionBy("ingestion_date") \
          .save(config.bronze_path)
            
  #Write Quarantine
  invalid_df.write \
            .format("delta") \
            .mode("append") \
            .save(config.quarantine_path)
    
  update_file_tracker(valid_df, config, batch_id)
    
    
def write_bronze_stream(stream_df, config, logger):
  
  def process_batch(batch_df, batch_id):
      
    #Data Quality
    valid_df, invalid_df = apply_basic_quality_checks(batch_df)
      
    #Metrics  
    raw_count = batch_df.count()
    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    
    log_metrics(logger, raw_count, valid_count, invalid_count)  
    
    #Write Bronze
    valid_df.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("ingestion_date") \
            .save(config.bronze_path)
              
    #Write Quarantine
     
    invalid_df.write \
              .format("delta") \
              .mode("append") \
              .save(config.quarantine_path)   
              
    update_file_tracker(valid_df, config, batch_id)
    
    
      
  return (
    stream_df.writeStream \
             .foreachBatch(process_batch) \
             .option("checkpointLocation" , config.checkpoint_path) \
             .start() \
             .awaitTermination()
        )
          