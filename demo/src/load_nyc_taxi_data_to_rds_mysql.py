# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
    This script is deployed as an AWS Glue Job and is used to populate 
    sample New York City Taxi - Yellow Trip Data to Amazon RDS for MySQL database.
"""

__author__ = "Srinivas Kandi"
__reviewer__ = "Ravi Itha"
__license__ = "Apache-2.0"
__version__ = "1.0"

import sys
import pymysql
import awswrangler as wr
import pandas as pd
from awsglue.utils import getResolvedOptions


def populate_trip_data_to_rds_table():
    """
    This function will read the sample NY Taxi Trips data from S3 and will be populate MySQL RDS table.

    The input dataset does not have PK_ID column, however, the MySQL Table has a columns with name of type primary key,
    and it is set to Sequence Identity Column. When we do not provide a value, the Database supplies the values. 
    Hence, we used `use_column_names` option to bind DataFrame columns to rest of the columns of the MySQL Table.
    """

    args = getResolvedOptions(sys.argv, ['input_sample_data_path', 'schema_name', 'table_name', 'rds_connection_name'])
    s3_path = args['input_sample_data_path']
    schema_name = args['schema_name']
    table_name = args['table_name']
    rds_connection_name = args['rds_connection_name']
    inp_df = pd.read_parquet(s3_path)
    con_mysql = wr.mysql.connect(rds_connection_name)
    wr.mysql.to_sql(inp_df, con_mysql, schema=schema_name, table=table_name, mode="append", use_column_names=True)
    with con_mysql.cursor() as cursor:
        cursor.execute(f"select * from {schema_name}.{table_name} limit 1")
        print(cursor.fetchall())

    con_mysql.close()


if __name__ == '__main__':
    """
    This is the main method which will populate MySQl RDS table with NY Taxi Trips sample data
    """
    try:
        populate_trip_data_to_rds_table()
    except Exception as e:
        raise
