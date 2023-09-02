# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
    This script is deployed as an AWS Glue Job and is used to populate configuration metadata
    for operational tables required by the ODPF File Processor.
"""

__author__ = "Srinivas Kandi"
__reviewer__ = "Ravi Itha"
__license__ = "Apache-2.0"
__version__ = "1.0"


import boto3
import sys
from awsglue.utils import getResolvedOptions


def populate_odpf_raw_table_config(raw_table_config_table_name, aws_region):
    """
    This function insert configuration metadata to odpf_raw_table_config DynamoDB Table.
    Args:
        raw_table_config_table_name:
        aws_region:

    Returns:

    """
    dynamodb = boto3.resource('dynamodb', region_name=aws_region)
    config_table = dynamodb.Table(raw_table_config_table_name)
    for i in range(1, 19):
        raw_file_config = {
            "refresh_cadence": "10_min_refresh",
            "source_table_name": f"taxi_trips_table_{str(i)}",
            "hudi_partition_key": "VENDORID",
            "hudi_precombine_field": "CDC_TIMESTAMP_SEQ",
            "hudi_primary_key": "PK_ID",
            "hudi_table_type": "copy_on_write",
            "is_active": "Y",
            "raw_database_name": "odpf_demo_taxi_trips_raw",
            "raw_database_S3_bucket": "odpf-hudi-raw-us-west-2",
            "raw_table_name": f"table_{str(i)}",
            "table_data_versioning_type": "S",
            "table_storage_type": "hudi"
        }
        config_table.put_item(Item=raw_file_config)


def populate_odpf_batch_config(batch_config_ddb_table_name, aws_region):
    """
    This function insert configuration metadata to odpf_batch_config DynamoDB Table.
    Args:
        batch_config_ddb_table_name:
        aws_region:

    Returns:

    """
    dynamodb = boto3.resource('dynamodb', region_name=aws_region)
    config_table = dynamodb.Table(batch_config_ddb_table_name)
    batch_config = {
        "refresh_cadence": "10_min_refresh",
        "datalake_format": "hudi",
        "glue_job_max_workers": 10,
        "glue_job_name": "odpf_file_processor",
        "glue_job_worker_type": "G.1X",
        "refresh_tables_batch_size": "5"
    }
    config_table.put_item(Item=batch_config)


if __name__ == '__main__':
    try:
        args = getResolvedOptions(sys.argv,
                                  ['batch_config_ddb_table_name', 'raw_table_config_ddb_table_name', 'aws_region'])
        populate_odpf_raw_table_config(args['raw_table_config_ddb_table_name'], args['aws_region'])
        populate_odpf_batch_config(args['batch_config_ddb_table_name'], args['aws_region'])
    except Exception as e:
        raise
