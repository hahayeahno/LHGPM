# CIS 497/498
# Fall 2022
# Josh Maier
# Adapted from Keelan Burnham

# Before running:
# Make sure InfluxDB is running (influxd.exe)
# Make sure mySQL is running
# Make sure the information for connecting to Influx and mySQL is correct
# Make sure the schemas exist in mySQL
# Make sure you have enough storage available for data, more than 100 GB
# Make sure you have enough time to run the script, 5 to 10 hours (used to be 320 hours, be grateful)

# Things to consider:
# A file, error.txt, will be generated if any errors occur with mySQL
#   This could occur if:
#   There is already data in the tables it trys to create
#   Table or column names are too long/improperly formatted
#   Too much data is being transferred at once
# The most likely error to occur in the script is a 'MemError'
#   This is caused by setting your limit too high and having a very long string of data to handle
#   On the developer's system, a limit of 1000 ran without issue, but 5000 crashed the script
# There is significant slow down on the larger measurements.
#   This is because Influx's offset does not perform well with extremely large values
#   This is acceptable because the script cannot handle the entire measure, as some of them contain over 1.8 million
#       rows of data

from sqlalchemy import create_engine, text
from influxdb import InfluxDBClient
import re as regex
import urllib.parse
import gc

db_name_arr = ['rf1', 'rf2', 'rf3', 'rf4', 'rf5', 'svd1', 'svd2', 'svd3', 'svd4', 'svd5', 'terasort1', 'terasort2',
               'terasort3', 'terasort4', 'terasort5', 'wc1', 'wc2', 'wc3', 'wc4', 'wc5']

for name in db_name_arr:
    # Use this if mySQL and InfluxDB have the same database names
    db_name = name

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
        except Exception as err:
            print("Query: FAILED")
            f = open('error.txt', 'a')
            print(err, file=f)
            f.close

    # influx connection
    try:
        influx_client = InfluxDBClient(database=db_name)
    except Exception as error:
        print(f'Influx Connection: FAILED\n{error}')
        exit
    else:
        print("Influx Connection: OK")

    # get and loop through measurements
    measurements = influx_client.get_list_measurements()

    for i in measurements:
        # measurements is list of dictionaries with key 'name' ex. [{'name': 'ALERTS'}, {'name': 'ALERTS_FOR_STATE'}]
        measurement = (i['name'])

        print(f'Working on measure {measurement}')

        # shorten sql table identifiers (mysql limitation)
        # works with all measurements in current dataset, may need to be updated in the future
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

        # Create sql table
        create_table_query = f'CREATE TABLE {sql_table_name} (entry_id INT AUTO_INCREMENT PRIMARY KEY);'
        run_sql_query(create_table_query)

        # Get the first row from influx to format the new table in mySQL
        series_obj = influx_client.query(f'select * from "{measurement}" limit 1').raw

        # Skip tables that have no data (unfortunately these exist)
        if series_obj['series'] == []:
            continue

        # Create sql table columns
        columns = [regex.sub("^.*\\.", "", column) for column in series_obj["series"][0]['columns']] # removes table identifier prefix
        columns = [f"`{column}`" for column in columns] # escape column names to guard against SQL reserved words

        # Add columns to mySQL table
        for column in columns:
            alter_table_query = f"ALTER TABLE {sql_table_name} ADD {column} varchar(500);"
            run_sql_query(alter_table_query)

        # Transfer data from influxDB to mySQL
        # General flow:
        #   Grab first chunk of data from influxDB
        #   While there is still data:
        #       format data into SQL query
        #       submit query to mySQL
        #       grab next chunk of data

        limit = 1000  # Optimal number may vary based on system, bigger is more efficient but more memory consuming
        series_obj = influx_client.query(f'select * from "{measurement}" limit {limit}').raw
        row = 0  # Used for determining the offset for chunks of data

        while series_obj['series'] != []:
            # Series object is a dictionary with a list with a dictionary with a list in it
            values_list = series_obj['series'][0]["values"]
            values_str = ''

            # Format row values for SQL query
            for row_values in values_list:
                row += 1
                row_values_strings = map(str, row_values)  # Turn row values into strings
                row_values_quotes = [f"'{i}'" for i in row_values_strings]  # For mysql string syntax
                escape_binds = [regex.sub(':', '\\:', i) for i in row_values_quotes]  # Escapes ':' for mysql bind params
                values_str += f'({(", ".join(escape_binds))}), '  # format for SQL query

            # Submit data to mySQL
            if len(values_str) > 0:
                print(f'{db_name}, {measurement}: row {row}')  # Progress reporting
                values_str = values_str[:-2]  # Drop last two chars ', '
                insert_query = f'INSERT INTO {sql_table_name} ({(", ").join(columns)}) VALUES {values_str};'
                run_sql_query(insert_query)

            # Clear memory of large objects
            del series_obj
            del values_str
            gc.collect()

            # Grab next chunk of data from influx
            series_obj = influx_client.query(f'select * from "{measurement}" limit {limit} offset {row}').raw

print('Script completed.')