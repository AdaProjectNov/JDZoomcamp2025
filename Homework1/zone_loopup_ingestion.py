#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import psycopg2
import os
from sqlalchemy import create_engine
from time import time

# Define the parameters here
user = "root"
password = "root"
host = "localhost"
port = "5432"
db = "nyc-tlc-data"
table_name = "zone-lookup"
url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"  # e.g., "https://example.com/data.csv"

# Define the main logic
def main(user, password, host, port, db, table_name, url):
    csv_name = 'output.csv'

    # Download the CSV
    os.system(f"wget {url} -O {csv_name}")

    # Connect to PostgreSQL
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Load the first few rows to infer schema
    df = pd.read_csv(csv_name, nrows=100)
    #compression='gzip'
    #df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    #df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    # Create table schema
    schema = pd.io.sql.get_schema(df, name=table_name, con=engine)

    # Load data in chunks
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    #compression='gzip'
    
    first_chunk = next(df_iter)
    #first_chunk.lpep_pickup_datetime = pd.to_datetime(first_chunk.lpep_pickup_datetime)
    #first_chunk.lpep_dropoff_datetime = pd.to_datetime(first_chunk.lpep_dropoff_datetime)

    first_chunk.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    first_chunk.to_sql(name=table_name, con=engine, if_exists='append')
    print("Inserted the first chunk.")

    for df in df_iter:
        t_start = time()

        #df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        #df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('Inserted another chunk..., took %.3f seconds' % (t_end - t_start))

# Run the main function
main(user, password, host, port, db, table_name, url)


# In[ ]:




