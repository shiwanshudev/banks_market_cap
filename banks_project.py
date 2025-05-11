# Code for ETL operations on Country-GDP data

# Importing the required libraries
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import requests

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d-%H:%M:%S")
    with open("code_log.txt", "a") as logger:
        logger.write(f"{timestamp} - {message} \n")

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
                    table_attribs[1]: [cols[2].text.strip("\n")]
                })], ignore_index=True)
            
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

# Declaring known values
banks_url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks' 
# Preliminaries complete. Initiating ETL process
extracted_data = extract(banks_url, ["Name", "MC_USD_Billion"])
print(extracted_data)