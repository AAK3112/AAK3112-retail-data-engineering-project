
from datetime import date
from pyspark.sql import functions as f 

def get_date_window(past_years = 5, future_years = 10):
    
    today = date.today()
    
    start_date = date(today.year - past_years, 1, 1)
    end_date = date(today.year + future_years, 12, 31)
    
    return start_date.isoformat(), end_date.isoformat()

def generate_dates_between(spark, start_date, end_date):
    
    df = spark.sql(f"""
              select sequence(to_date('{start_date}'),
                              to_date('{end_date}'),
                              interval 1 day) as date_range
              """)
    
    df = df.select(f.explode("date_range").alias("date"))
    
    dim_date = (
        df.withColumn("date_key", f.date_format("date", "yyyyMMdd").cast("int"))
          .withColumn("year", f.year("date"))
          .withColumn("quarter", f.concat(f.lit("Q"),f.quarter("date")))
          .withColumn("month", f.month("date"))
          .withColumn("month_name", f.date_format("date", "MMMM"))
          .withColumn("day", f.dayofmonth("date"))
          .withColumn("day_of_week", f.date_format("date", "EEEE"))
          .withColumn("week_of_year", f.weekofyear("date"))
          .withColumn("is_weekend", f.dayofweek("date").isin([1,7]))
    )
    
    return dim_date
    
    