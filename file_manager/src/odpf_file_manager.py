# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0 

"""
    File Manager is deployed as an AWS Lambda function. It detects files emitted 
    by a CDC process such as AWS DMS and stores them in an Amazon DynamoDB table. 
"""

__author__ = "Srinivas Kandi"
__reviewer__ = "Ravi Itha"
__license__ = "Apache-2.0"
__version__ = "1.0"

import json
import datetime
import boto3
import os
import uuid
from botocore.exceptions import ClientError


def log_raw_file_ingestion(inp_rec):
    """
    This function inserts an audit record for every load file sent by DMS
    Parameters:
    log_rec(Dict): file ingestion audit rec with raw file ingestion details
    """
    # get the dynamodb table name
    table_name = os.environ['DMS_TRACKER_TABLE']
    # dynamodb = boto3.resource('dynamodb')
    # table = dynamodb.Table('table_name')
    ddb_client = boto3.client('dynamodb')
    ddb_client.transact_write_items(
        TransactItems=[
            {'Put': {'TableName': table_name,
                     'Item': {
                         'source_table_name': {'S': inp_rec['source_table_name']},
                         'file_id': {'S': inp_rec['file_id']},
                         'file_ingestion_status': {'S': inp_rec['file_ingestion_status']},
                         'file_ingestion_date_time': {'S': inp_rec['file_ingestion_date_time']},
                         'file_ingestion_s3_bucket': {'S': inp_rec['file_ingestion_s3_bucket']},
                         'file_ingestion_path': {'S': inp_rec['file_ingestion_path']},
                         'dms_file_type': {'S': inp_rec['dms_file_type']},
                         'schema_name': {'S': inp_rec['schema_name']},
                         'table_name': {'S': inp_rec['table_name']}
                     }
                     }
             }
        ]
    )


def create_raw_file_ingestion_audit_rec(ingestion_event):
    """
    This function parses the S3 notification event from SQS and 
    creates an audit record that will be used to log all raw file ingestion
    
    Parameters:
    ingestion_event(Dict): S3 event notification JSON
    
    Returns:
    audit_rec(Dict): Returns a dictionary with S3 file ingestion details
    """
    audit_rec = {}
    s3event = ingestion_event
    file_id = str(uuid.uuid4())
    s3_bucket_name = s3event['detail']['bucket']['name']
    audit_rec['file_id'] = file_id
    audit_rec["file_ingestion_s3_bucket"] = s3_bucket_name
    s3_raw_file_path = s3event['detail']['object']['key']
    audit_rec["file_ingestion_path"] = s3_raw_file_path
    s3_event_time = s3event['time']
    # convert the timestamp to yyyy-mm-dd hh:mm:ss format
    s3_file_create_time = datetime.datetime.strptime(
        s3_event_time, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
    audit_rec["file_ingestion_date_time"] = s3_file_create_time
    s3_raw_file_array = s3_raw_file_path.split('/')
    if len(s3_raw_file_array) > 5:
        audit_rec["dms_file_type"] = 'Incremental'
    else:
        audit_rec["dms_file_type"] = 'Full'
    source_schema_name = s3_raw_file_array[1]
    source_table_name = s3_raw_file_array[2]
    source_name = source_table_name.split('_')[0]
    qualified_table_name = '{}_{}'.format(source_schema_name, source_table_name)
    audit_rec["source_table_name"] = qualified_table_name.lower()
    audit_rec["schema_name"] = source_schema_name.lower()
    audit_rec["table_name"] = source_table_name.lower()

    audit_rec["file_ingestion_status"] = "raw_file_landed"

    return audit_rec


def lambda_handler(event, context):
    """
    This is the main Lambda handler which receives SQS message.
    The SQS message is S3 object create notification event.
    For every S3 object created by DMS, a SQS message is generated

    Parameters:
        event(Dict): SQS Message body
        context(context): SQS to Lambda trigger context
    Returns:
        Dictionary: Returns Lambda execution status
    """

    # loop through SQS messages

    for record in event['Records']:
        s3_event = json.loads(record['body'])
        audit_log_rec = create_raw_file_ingestion_audit_rec(s3_event)
        log_raw_file_ingestion(audit_log_rec)

    return {
        'statusCode': 200,
        'body': 'DMS File Manager executed'
    }
