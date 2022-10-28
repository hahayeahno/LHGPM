# CIS 497/498
# Spring 2022
# Keelan Burnham

from sqlalchemy import create_engine, text
from influxdb import InfluxDBClient
import json
import re as regex
import urllib.parse
import os

# mysql connection
mysql_username = 'root'
mysql_password = "tuTw1l3r"
url_safe_password = urllib.parse.quote_plus(mysql_password)
mysql_database = 'rf1'
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
        print(error)

# influx connection
try:
    influx_client = InfluxDBClient(database='rf1')
except Exception as error:
    print(f'Influx Connection: not OK\n{error}')
    exit
else:
    print("Influx Connection: OK")

# get and loop through measurements
measurements = influx_client.get_list_measurements()
measurements = measurements[2:]
for i in measurements:
    measurement = (i['name'])
    print(f'Working on measure {measurement}')
    # shorten sql table identifiers (mysql limitation)
    sql_table_name = measurement
    sql_table_name = regex.sub('_influx$', "", sql_table_name)
    sql_table_name = regex.sub('kubernetes', 'k8s', sql_table_name)
    sql_table_name = regex.sub('prometheus', 'prom', sql_table_name)
    sql_table_name = regex.sub('seconds', 'secs', sql_table_name)
    sql_table_name = regex.sub('evaluation', 'eval', sql_table_name)
    sql_table_name = regex.sub('algorithm', 'alg', sql_table_name)
    sql_table_name = regex.sub('scheduling', 'sching', sql_table_name)
    sql_table_name = regex.sub('scheduler', 'schr', sql_table_name)
    sql_table_name = regex.sub('containercontainer', 'container', sql_table_name)

    sql_table_name = sql_table_name.replace(':', '')

    # create sql table
    create_table_query = f'CREATE TABLE {sql_table_name} (entry_id INT AUTO_INCREMENT PRIMARY KEY);'

    run_sql_query(create_table_query)

    # TODO: remove this middle step
    # create json
    measurement = measurement.replace(":", "")

    measurement_table = influx_client.query(f'select * from {measurement}').raw
    json_object = json.dumps(measurement_table, indent=4)
    with open(f'{measurement}.json', 'w') as json_file:
        json_file.write(json_object)

    # read json & extract series obj
    with open(f'{measurement}.json', 'r') as json_file:
        json_load = json.load(json_file)
    series_obj = json_load["series"][0]

    os.remove(f'{measurement}.json')

    # create sql table columns
    columns = [regex.sub("^.*\\.", "", column) for column in series_obj["columns"]] # removes table identifier prefix
    for column in columns:
        alter_table_query = f"ALTER TABLE {sql_table_name} ADD `{column}` varchar(500);"
        run_sql_query(alter_table_query)

    # insert sql rows
    values_list = series_obj["values"]
    row_number = 0
    for row_values in values_list:
        row_number += 1
        print(f'{measurement}: {row_number}')
        row_values_strings = map(str, row_values)
        row_values_quotes = [f"'{i}'" for i in row_values_strings] # for mysql string syntax
        escape_binds = [regex.sub(':', '\\:', i) for i in row_values_quotes] # escapes ':' for mysql bind params
        insert_query = f'INSERT INTO {sql_table_name} ({(", ").join(columns)}) VALUES ({(", ".join(escape_binds))});'

        # check for sql reserved words
        insert_query = insert_query.replace('group', '`group`')
        run_sql_query(insert_query)

print('Script completed.')