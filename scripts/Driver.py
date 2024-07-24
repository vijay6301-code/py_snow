import os
import sys
import get_env_variables as gav
from create_spark import get_spark_object
from validate import get_current_date
from ingest import load_files,display_df,df_count
from data_processing import data_clean
from validate import print_schema,check_nulls
from data_load_snowflake import data_load_snowflake
from data_transformation import data_report1,data_report2
import time
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
import pandas 

start_time = time.time()

def main():
    
    start_time = time.time()
    
    print('application starting time at ',str(start_time))
    
    print('Creating Spark object...')
    
    spark = get_spark_object(envn=gav.envn, appName=gav.appName)
    
    print("Validating Spark object",str(spark))
    
    current_date_df = get_current_date(spark)
    
    current_date_str = current_date_df.collect()[0][0]  # Collect the result and convert to string
    
    print('Validating with current date:', str(current_date_str))
    
    for file in os.listdir(gav.src_olap):
        
        print('available files are :', file)
        
        file_dir = gav.src_olap +'\\' + file
        
        print(file_dir)
        
        if file.endswith('.parquet'):
            
            file_format='parquet'
            
            header='NA'
            
            inferSchema='NA'
            
    df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header, inferSchema=inferSchema)
    
    #print("creating data frame...")
    
    #display_df(df_city, 'df_city')  
    
    #count = df_count(df_city, 'df_city')
    
    #print('file row count is ',str(count))  
    
    for file in os.listdir(gav.src_oltp):
        
        print("available files are : ", file)  
        
        file_dir = gav.src_oltp +'\\' + file
        
        print(file_dir)
        
        if file.endswith('.csv'):
            
            file_format = 'csv'
            
            header=gav.header
            
            inferSchema=gav.inferSchema
            
    df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header, inferSchema=inferSchema )
    
    #print("creating df_fact dataframe...")
    
    #display_df(df_fact, 'df_fact')
    
    ##count = df_count(df_fact, 'df_fact')
    
    #print('file row count is ',str(count))
    
    df_city_sel,df_fact_sel=data_clean(df_city,df_fact)
    
    #print('selecting required columns from df_city dataframe')
    
    #display_df(df_city_sel,'df_city')
    
    #print_schema(df_city_sel, 'df_city')

    #print('selecting required columns from df_fact dataframe')
    
    #display_df(df_fact_sel,'df_fact')
    
    #print_schema(df_fact_sel, 'df_fact')
    
    #print('checking for nulls if existed...')
    
    #df_city_sel_null_check=check_nulls(df_city_sel,'df_city')
    
    #display_df(df_city_sel_null_check,'df_city')
    
   #df_fact_sel_null_check=check_nulls(df_fact_sel,'df_fact')
    
    #display_df(df_fact_sel_null_check,'df_fact')
    
    print('creating data_report_1...')
    
   ## data_report_1 = data_report1(df_city_sel,df_fact_sel)
    
    #display_df(data_report_1,'data_report_1')
    
   # print('creating data_report_2...')
    
    #data_report_2 = data_report2(df_fact_sel)
    
    #display_df(data_report_2,'data_report_2') 
    
    print("loading dataframe to snowflake") 
    
    tbl_name = 'city'
    success, nchunks, nrows, output = data_load_snowflake(df_city, table_name=tbl_name)
    # Print the results
    print(f"Load success: {success}")
    print(f"Number of chunks: {nchunks}")   
    print(f"Number of rows loaded: {nrows}")
    print(f"Output: {output}")

    tbl_name = 'fact'
    success, nchunks, nrows, output = data_load_snowflake(df_fact, table_name=tbl_name)
    # Print the results
    print(f"Load success: {success}")
    print(f"Number of chunks: {nchunks}")   
    print(f"Number of rows loaded: {nrows}")
    print(f"Output: {output}")

    
    
    
    

if __name__ == '__main__':
    
    main()
    
    end_time = time.time()
    print(f"The process time: {end_time - start_time:.2f} seconds")