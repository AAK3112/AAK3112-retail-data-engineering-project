
from pyspark.sql import functions as f 

def transform_orders(df, logger):
    
    logger.info("Silver Transformations - Applying Standarization")
    
    df = (
        df.withColumn("order_id", f.trim("order_id")) \
          .withColumn("customer_name", f.trim("customer_name")) \
          .withColumn("product_name", f.trim("product_name")) \
          .withColumn("order_date", f.to_date("order_date")) \
          .withColumn("price", f.col("price").cast("double")) \
          .withColumn("quantity", f.col("quantity").cast("int")) \
          .withColumn("silver_loaded_at", f.current_timestamp())
    )
    
    base_df = df.cache()
    
    logger.info("Silver Transformation - Applying Data Qualtiy Rules")
    
    req_cols = ["order_id", "order_date", "customer_name", "product_name", "quantity", "price"]
    
    null_condition = None
    
    for c in req_cols:
        cond = f.col(c).isNull()
        null_condition = cond if null_condition is None else (null_condition | cond)
    
    invalid_null_df = base_df.filter(null_condition)
    
    valid_df = base_df.filter(~null_condition)
    
    logger.info("Silver Transformations - Applying Business Rules")
    
    invalid_business_df = valid_df.filter(
        (f.col("price") <= 0) | (f.col("quantity") <= 0)
    )
    
    valid_df = valid_df.filter(
        (f.col("price") > 0) & (f.col("quantity") > 0)
    )
    
    #invalid_df = invalid_df.unionByName(invalid_business_df)
    
    logger.info("Silver Transformation - Applying regex")
    
    customer_name_regex = r"^[A-Za-zÀ-ÖØ-öø-ÿ' -]+$"
    product_name_regex = r"^[A-Za-z0-9À-ÖØ-öø-ÿ'()./\\ -]+$"
    email_regex = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
    
    invalid_regex_df = valid_df.filter(
        (~f.col("customer_name").rlike(customer_name_regex)) |
        (~f.col("product_name").rlike(product_name_regex)) |
        (~f.col("email").rlike(email_regex))
    )
    
    valid_df = valid_df.filter(
        (f.col("customer_name").rlike(customer_name_regex)) &
        (f.col("product_name").rlike(product_name_regex)) &
        (f.col("email").rlike(email_regex))
    )
    
    invalid_df = (
        invalid_null_df.unionByName(invalid_regex_df) \
                                .unionByName(invalid_business_df) \
                                .dropDuplicates()
    )
    
    logger.info(f"Valid Rows: {valid_df.count()}")
    logger.info(f"Invalid Rows: {invalid_df.count()}")
    
    base_df.unpersist()

    return valid_df, invalid_df
   