import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import pd_writer
import time 
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe





"""
translate_text_gsheet_Otodom_Analysis.py

This script connects to a Snowflake database, fetches titles in Polish from the 
OTODOM_DATA_FLATTEN table, and creates Google Sheets with batched rows (~10k per file). 
It uses the Google Sheets formula `GOOGLETRANSLATE` to translate titles to English.

The script:
- Authenticates using a Google Service Account.
- Creates multiple Google Sheets files and inserts the fetched titles.
- Shares each sheet to a specified email for collaborative processing.

Requirements:
- gspread
- google-auth
- pandas
- Snowflake connector

Ensure that Google Sheets and Drive APIs are enabled and credentials are correctly configured.
"""






start_time = time.time()

engine = create_engine(URL(
                    account = 'ldpwzbl-js06818',
                    user = 'niraj2780',
                    password = 'Crosslynx@12345',
                    database = 'TECHTFQ',
                    schema = 'public',
                    warehouse = 'SNOWFLAKE_LEARNING_WH'))
                
with engine.connect() as conn:
    try:
        query = """ SELECT RN, TITLE FROM otodom_data_flatten ORDER BY rn limit 300"""

        df = pd.read_sql(query,conn)

        gc = gspread.service_account()

        loop_counter = 0
        chunk_size = 100
        file_name = 'OTODOM_ANALYSIS_'
        user_email = 'kniraj9783@gmail.com'

        for i in range(0,len(df),chunk_size):
            loop_counter += 1
            df_in = df.iloc[i:(i+chunk_size), :]

            spreadsheet_title = file_name + str(loop_counter)
            try:
                locals()['sh'+str(loop_counter)] = gc.open(spreadsheet_title)
            except gspread.SpreadsheetNotFound:
                locals()['sh'+str(loop_counter)] = gc.create(spreadsheet_title)

            locals()['sh'+str(loop_counter)].share(user_email, perm_type='user', role='writer')
            wks = locals()['sh'+str(loop_counter)].get_worksheet(0)
            wks.resize(len(df_in)+1)
            set_with_dataframe(wks, df_in)   
                
            column = 'C'   # Column to apply the formula 
            start_row = 2  # Starting row to apply the formula
            end_row = wks.row_count   # Ending row to apply the formula
            cell_range = f'{column}{start_row}:{column}{end_row}' 
            curr_row = start_row
            cell_list = wks.range(cell_range)
            
            for cell in cell_list:
                cell.value = f'=GOOGLETRANSLATE(B{curr_row},"pl","en")'
                curr_row += 1
                
            # Update the worksheet with the modified cells
            wks.update_cells(cell_list, value_input_option='USER_ENTERED')

            print(f'Spreadsheet {spreadsheet_title} created!')

            df_log = pd.DataFrame({'ID':[loop_counter], 'SPREADSHEET_NAME':[spreadsheet_title]})
            df_log.to_sql('otodom_data_log', con=engine, if_exists='append', index=False, chunksize=16000, method=pd_writer)


            # df_out = get_as_dataframe(wks, usecols=[0,1,2,3], nrows=end_row, header=None, skiprows=1)
            # print(f'Spreadsheet {locals()["sh"+str(loop_counter)]} loaded back to DataFrame!')
            
            # df_out.columns = ['RN', 'TITLE', 'LOCATION', 'TITLE_ENG']

            # df_out.to_sql('otodom_data_transformed', con=engine, if_exists='append', index=False, chunksize=16000, method=pd_writer)

    except Exception as e:
        print('--- Error --- ',e)
    finally:
        conn.close()
engine.dispose()

print("--- %s seconds ---" % (time.time() - start_time))