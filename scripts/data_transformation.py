from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

def data_report1(df_city_sel, df_fact_sel):
    
    df_city_sel = df_city_sel.withColumn('zipCounts',size(split(col('zips')," ")))
    
    df_fact_grp = df_fact_sel.groupBy(col('presc_state'),col('presc_city'))\
                             .agg(countDistinct("presc_id").alias('presc_counts'),sum('tx_cnt').alias('tx_counts'))
                             
    
    df_city_join = df_city_sel.join(df_fact_grp, (df_city_sel.state_id == df_fact_grp.presc_state) & (df_city_sel.city == df_fact_grp.presc_city), 'inner')

    df_final = df_city_join.select("city", "state_name", "county_name", "population", "zipcounts", "presc_counts")
    
    return df_final

def data_report2(df_fact_sel):
    
    window_spec= Window.partitionBy('presc_state').orderBy(col('tx_cnt') .desc())
    
    df_presc_report = df_fact_sel.select("presc_id", "full_name", "presc_state","presc_city",
                                     "years_of_exp", "tx_cnt", "total_day_supply", "total_drug_cost") \
                             .filter((col("years_of_exp") >= 20) & (col("years_of_exp") <= 50)) \
                             .withColumn('dense_rank', dense_rank().over(window_spec)) \
                             .filter(col('dense_rank') <= 5)
                                            
    return df_presc_report