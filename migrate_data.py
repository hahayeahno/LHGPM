# CIS 497/498
# Fall 2023
# Josh Maier
# Adapted from Keelan Burnham

from sqlalchemy import create_engine, text
from influxdb import InfluxDBClient
import json
import re as regex
import urllib.parse
import logging
import os

# database name
db_name = 'terasort1'

# mysql connection
mysql_username = 'root'
mysql_password = "tuTw1l3r"
url_safe_password = urllib.parse.quote_plus(mysql_password)
mysql_database = db_name
sql_engine = create_engine(
    f'mysql+pymysql://{mysql_username}:{url_safe_password}@127.0.0.1:3306/{mysql_database}',
    pool_pre_ping=True
)

# sql query function
def run_sql_query(query):
    try:
        with sql_engine.connect() as connection:
            connection.execute(text(query))
    except Exception as error:
        print("Query: FAILED")
        f = open('error.txt', 'a')
        print(error, file=f)
        f.close

# influx connection
try:
    influx_client = InfluxDBClient(database=db_name)
except Exception as error:
    print(f'Influx Connection: not OK\n{error}')
    exit
else:
    print("Influx Connection: OK")

# get and loop through measurements
measurements = influx_client.get_list_measurements()

for i in measurements:
    measurement = (i['name'])

    print(f'Working on measure {measurement}')

    # shorten sql table identifiers (mysql limitation)
    sql_table_name = measurement
    sql_table_name = sql_table_name.replace(':', '')
    sql_table_name = regex.sub('_influx$', "", sql_table_name)
    sql_table_name = regex.sub('kubernetes', 'k8s', sql_table_name)
    sql_table_name = regex.sub('prometheus', 'prom', sql_table_name)
    sql_table_name = regex.sub('seconds', 'secs', sql_table_name)
    sql_table_name = regex.sub('evaluation', 'eval', sql_table_name)
    sql_table_name = regex.sub('algorithm', 'alg', sql_table_name)
    sql_table_name = regex.sub('scheduling', 'sching', sql_table_name)
    sql_table_name = regex.sub('scheduler', 'schr', sql_table_name)
    sql_table_name = regex.sub('string', 'str', sql_table_name)
    sql_table_name = regex.sub('total', 'tot', sql_table_name)
    sql_table_name = regex.sub('quantile', 'quant', sql_table_name)
    sql_table_name = regex.sub('generation', 'gen', sql_table_name)
    sql_table_name = regex.sub('containercontainer', 'container', sql_table_name)

    # create sql table
    create_table_query = f'CREATE TABLE {sql_table_name} (entry_id INT AUTO_INCREMENT PRIMARY KEY);'

    run_sql_query(create_table_query)

    #measurement = measurement.replace(":", "")

    series_obj = influx_client.query(f'select * from "{measurement}"').raw #put measurements in quotes bc someone decided colons in measure names was a good idea

    #create sql table columns
    if series_obj['series'] == []: #Skip tables that have no data (unfortunately these exist)
        continue

    columns = [regex.sub("^.*\\.", "", column) for column in series_obj["series"][0]['columns']] # removes table identifier prefix
    columns = [f"`{column}`" for column in columns] #escape characters for column names guard against SQL reserved words

    for column in columns:
        alter_table_query = f"ALTER TABLE {sql_table_name} ADD {column} varchar(500);"
        run_sql_query(alter_table_query)

    #insert sql rows
    values_list = series_obj['series'][0]["values"]

    values_str = ''
    row = 0 #row for splitting data
    for row_values in values_list:
        row += 1
        row_values_strings = map(str, row_values)
        row_values_quotes = [f"'{i}'" for i in row_values_strings] # for mysql string syntax
        escape_binds = [regex.sub(':', '\\:', i) for i in row_values_quotes] # escapes ':' for mysql bind params
        values_str += f'({(", ".join(escape_binds))}), ' #format for SQL query
        #every 5000 rows, insert into mySQL table. This cuts down on overhead. (16 hrs inserting one at a time, 40 mins like this)
        if row % 5000 == 0:
            print(f'{measurement}: row {row}')
            values_str = values_str[:-2] #remove trailing extra chars
            insert_query = f'INSERT INTO {sql_table_name} ({(", ").join(columns)}) VALUES {values_str};'
            values_str = '' #reset values_str

    #insert left over queries
    if len(values_str) > 0:
        print(f'{measurement}: row {row}')
        values_str = values_str[:-2]
        insert_query = f'INSERT INTO {sql_table_name} ({(", ").join(columns)}) VALUES {values_str};'

    run_sql_query(insert_query)

print('Script completed.')