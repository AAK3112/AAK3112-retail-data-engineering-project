
from pyspark.sql import functions as f

def apply_basic_quality_checks(df):
    
    df = df.withColumn("dq_status", 
                       f.when(
                           (f.col("order_id").isNull()) |
                           (f.col("quantity") <= 0) |
                           (f.col("price") < 0) |
                           (f.col("order_date")).isNull(),
                           "Invalid"
                       ).otherwise("Valid") 
                    )
    
    valid_df = df.filter("dq_status = 'Valid'").drop("dq_status")
    invalid_df = df.filter("dq_status = 'Invalid'")
    
    
    return valid_df, invalid_df