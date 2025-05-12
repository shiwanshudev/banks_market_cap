# Code for ETL operations on Country-GDP data

# Importing the required libraries
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import numpy as np
import sqlite3

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d-%H:%M:%S")
    with open("code_log.txt", "a") as logger:
        logger.write(f"{timestamp} : {message} \n")

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    df = pd.DataFrame(columns=table_attribs)
    html_content = requests.get(url).text
    soup = BeautifulSoup(html_content, 'html.parser')
    table_rows= soup.find_all('table')[0].find_all('tbody')[0].find_all('tr')
    data_list =[]
    for row in table_rows:
        cols = row.find_all('td')
        if len(cols) > 0:
            df = pd.concat([df, pd.DataFrame({
                    table_attribs[0]: [cols[1].text.strip("\n")],
                    table_attribs[1]: [float(cols[2].text.strip("\n"))]
                })], ignore_index=True)
            
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    csv_df = pd.read_csv(csv_path)
    csv_df = csv_df.set_index("Currency")
    exchange_rate = {
        'GBP': float(csv_df.loc["GBP", "Rate"]),
        'EUR': float(csv_df.loc["EUR", "Rate"]),
        'INR': float(csv_df.loc["INR", "Rate"]),
    }
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(name = table_name, con = sql_connection, if_exists="replace", index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(pd.read_sql(query_statement, sql_connection))

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Declaring known values')

banks_url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks' 
csv_path = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
db_name= 'Banks.db'
table_name="Largest_banks"
output_path_csv = 'Largest_banks_data.csv'
log_progress('Preliminaries complete. Initiating ETL process')

log_progress('Call extract() function')

extracted_data = extract(banks_url, ["Name", "MC_USD_Billion"])
#print(extracted_data)

log_progress('Data extraction complete. Initiating Transformation process')

transformed_data = transform(extracted_data, csv_path)
print(transformed_data)
# print(transformed_data['MC_EUR_Billion'][4])

log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(transformed_data, output_path_csv)

log_progress("Data saved to CSV file")
log_progress("Initiate SQLite3 connection")

conn = sqlite3.connect(db_name)

log_progress("SQL Connection initiated")

log_progress("Call load_to_db()")

load_to_db(transformed_data, conn, table_name)

log_progress("Data loaded to Database as a table, Executing queries")

log_progress("Call run_query()")

run_query('SELECT * FROM Largest_banks', conn)
run_query('SELECT AVG(MC_GBP_Billion) FROM Largest_banks', conn)
run_query('SELECT Name from Largest_banks LIMIT 5', conn)

log_progress("Process Complete")

log_progress("Close SQLite3 connection")

conn.close()

log_progress("Server Connection closed")