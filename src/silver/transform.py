
from pyspark.sql import functions as f

def bronze_transform(df, batch_id):
    return(
        df.withColumn("ingestion_timestamp", f.current_timestamp()) \
          .withColumn("ingestion_date", f.current_date()) \
          .withColumn("batch_id", f.lit(batch_id))
    )