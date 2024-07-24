import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *  # type: ignore

def get_current_date(spark_session):
    print("Executing get_current_date function")
    output = spark_session.sql("SELECT current_date")
    print("Query executed, result:", output.collect())
    return output

def print_schema(df, dfName):
    
    sch = df.schema.fields
    
    for i in sch:
        print(i)
        
        
def check_nulls(df, dfName):
    
    check_null_df = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])
    
    return check_null_df
    
        
