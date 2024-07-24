


def load_files(spark, file_dir, file_format, header, inferSchema):
    
    if file_format =='parquet':
        df = spark.read.format(file_format).load(file_dir)
    elif file_format =='csv':
        df =spark.read.format(file_format).options(header = header,inferSchema=inferSchema).load(file_dir)
        
    return df 

def display_df(df, dfName):
    df_show=df.show()
    return df_show

def df_count(df,dfName):
    df_c=df.count()
    return df_c
    