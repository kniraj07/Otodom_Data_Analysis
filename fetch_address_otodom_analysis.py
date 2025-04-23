import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.point import Point
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import pd_writer
import time 
import dask.dataframe as dd

start_time = time.time()
#******************to tackle api limit issue start************
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

geolocator = Nominatim(user_agent="otodomprojectanalysis")
# Respect 1 request per second rule
reverse = RateLimiter(geolocator.reverse, min_delay_seconds=1, max_retries=2, error_wait_seconds=2.0)
#******************to tackle api limit issue end************

#geolocator = Nominatim(user_agent="otodomprojectanalysis")

"""
fetch_address_otodom_analysis.py

This script connects to a Snowflake database, retrieves location coordinates from the 
OTODOM_DATA_FLATTEN table, and uses the `geopy` library to convert latitude and longitude 
into structured addresses (suburb, city, country). The enriched data is then stored back 
into a new Snowflake table named OTODOM_DATA_FLATTEN_ADDRESS.

Requirements:
- geopy
- snowflake-connector-python
- pandas
- JSON key for Snowflake access

Ensure Snowflake credentials are set in the environment or provided via configuration.
"""





engine = create_engine(URL(
                    account = 'ldpwzbl-js06818',
                    user = 'niraj2780',
                    password = 'Crosslynx@12345',
                    database = 'TECHTFQ',
                    schema = 'public',
                    warehouse = 'SNOWFLAKE_LEARNING_WH'))


with engine.connect() as conn:
    try:
        query = """ SELECT RN, concat(latitude,',',longitude) as LOCATION
                    FROM (SELECT RN
                            , SUBSTR(location, REGEXP_INSTR(location,' ',1,4)+1) AS LATITUDE 
                            , SUBSTR(location, REGEXP_INSTR(location,' ',1,1)+1, (REGEXP_INSTR(location,' ',1,2) - REGEXP_INSTR(location,' ',1,1) - 1) ) AS LONGITUDE
                        FROM otodom_data_short_flatten WHERE rn between 1 and 20
                        ORDER BY rn  ) """
        print("--- %s seconds ---" % (time.time() - start_time))
        
        df = pd.read_sql(query,conn)
                      
        df.columns = map(lambda x: str(x).upper(), df.columns)
        
        ddf = dd.from_pandas(df,npartitions=10)
        print(ddf.head(5,npartitions=-1))
        #df['ADDRESS'] = df['LOCATION'].apply(lambda x: reverse(x).raw['address'] if reverse(x) else None)

        #ddf['ADDRESS'] = ddf['LOCATION'].apply(lambda x: geolocator.reverse(x).raw['address'],meta=(None, 'str'))
        ddf['ADDRESS'] = ddf['LOCATION'].apply(get_address, meta=('ADDRESS', 'object'))
        print("--- %s seconds ---" % (time.time() - start_time))

        pandas_df = ddf.compute()
        #pandas_df = df.compute()

        print("pandas_df:",pandas_df.head())
        print("--- %s seconds ---" % (time.time() - start_time))
        pandas_df.to_sql('otodom_data_flatten_address', con=engine, if_exists='append', index=False, chunksize=16000, method=pd_writer)
    except Exception as e:
        print('--- Error --- ',e)
    finally:
        conn.close()
engine.dispose()

print("--- %s seconds ---" % (time.time() - start_time))