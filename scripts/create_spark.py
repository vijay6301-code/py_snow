import findspark
findspark.init()
from pyspark.sql import SparkSession


def get_spark_object(envn, appName):
    
    if envn == 'Dev':
        master = 'local'

    else:
        master = 'yarn'
        
    spark = SparkSession.builder.master(master).appName(appName).getOrCreate()
    
    return spark