
from pyspark.sql import functions as f 

def read_batch(spark, file_list, config, schema):
        
    if not file_list:
                return None
        
    files_to_read = ",".join(file_list)
 

        
    df = (spark.read \
               .format("csv") \
               .option("header", "true") \
               .option("mode", "PERMISSIVE") \
               .option("columnNameOfCorruptRecord", "_corrupt_record") \
               .option("badRecordsPath", config.quarantine_path) \
               .schema(schema) \
               .load(files_to_read) \
               .withColumn("source_file_name", f.input_file_name()) 
        )
    
    print("DEBUG -> Rows read:", df.count())
    print("DEBUG -> Files path:", file_list)
    
    return df
    
    
    
def read_stream(spark, file_list, config, schema):
        
    if not file_list:
                return None
        
    files_to_read = ",".join(file_list)
    

    df = (spark.readStream \
               .format("csv") \
               .option("header", "true") \
               .option("mode", "PERMISSIVE") \
               .option("columnNameOfCorruptRecord", "_corrupt_record") \
               .option("badRecordsPath", config.quarantine_path) \
               .schema(schema) \
               .load(files_to_read) \
               .withColumn("source_file_name", f.input_file_name())
        )
    return df