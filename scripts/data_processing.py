from pyspark.sql.functions import *
from pyspark.sql.types import *

def data_clean(df1, df2):
    
    df_city_sel = df1.select ( upper(col('city')).alias('city'), df1.state_id,
                                  upper(df1.state_name).alias('state_name'),
                                  upper(df1.county_name).alias('county_name'),df1.population,df1.zips)
    
    df_city_sel =df_city_sel.withColumn('country',lit('USA'))
                                  
    
    df_fact_sel = df2.select(df2.npi.alias('presc_id'), df2.nppes_provider_last_org_name.alias('presc_lname'),
                                  df2.nppes_provider_first_name.alias('presc_fname'),
                                  df2.nppes_provider_city.alias('presc_city'),
                                  df2.nppes_provider_state.alias('presc_state'),
                                  df2.specialty_description.alias('presc_spclt'),
                                  df2.drug_name, df2.total_claim_count.alias('tx_cnt'), df2.total_day_supply,
                                  df2.total_drug_cost, df2.years_of_exp)
    
    df_fact_sel =( df_fact_sel.withColumn('years_of_exp',regexp_replace(col('years_of_exp'),r"^="," ").cast('int'))
                              .withColumn('full_name',concat_ws(" ",col('presc_fname'),col('presc_lname')))
                              .drop(col('presc_lname'),col('presc_fname')))
    
    df_fact_sel =  df_fact_sel.dropna(subset="presc_id")
    
    df_fact_sel = df_fact_sel.dropna(subset="drug_name") 
    
    mean_tx_cnt = df_fact_sel.select(mean(col('tx_cnt'))).collect()[0][0]
    df_fact_sel = df_fact_sel.fillna(mean_tx_cnt, 'tx_cnt')
    
    
    
    return df_city_sel,df_fact_sel