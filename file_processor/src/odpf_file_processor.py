# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0 

"""
    File Processor is deployed as an AWS Glue job. 
    It processes files from staging S3 bucket, 
    creates source-aligned datasets in Raw S3 bucket, 
    and add / update metadata in AWS Glue Data Catalog.
"""

__author__ = "Srinivas Kandi"
__reviewer__ = "Ravi Itha"
__license__ = "Apache-2.0"
__version__ = "1.0"


import logging
import sys
import boto3
from boto3.dynamodb.conditions import Key, Attr
import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkConf  # Added so we can modify Spark Configuration


class FileTrackerStatus:
    """
    This is the class for CDC File tracking
    """

    def __init__(self, source_table_name):
        self.source_table_name = source_table_name
        self.dynamodb = boto3.resource('dynamodb')
        self.dynamodb_client = boto3.client('dynamodb')

    def get_raw_files(self):
        """
        This function will get the list of CDC files that needs to be processed for a Hudi table
        :return: full_refresh_list : A list of full load CDC files
               : incremental_refresh_list: A list of incremental CDC files
               : all_files: A list of dictionaries with table refresh configuration data
        """
        paginator = self.dynamodb.meta.client.get_paginator("query")
        pages = paginator.paginate(
            TableName="odpf_file_tracker",
            KeyConditionExpression=Key("source_table_name").eq(self.source_table_name),
            FilterExpression=Attr('file_ingestion_status').eq('raw_file_landed')
        )

        all_files = []
        full_refresh_list = []
        incremental_refresh_list = []

        for page in pages:
            all_files.extend(page['Items'])

        if len(all_files) > 0:
            # Convert the DynamoDB result set into a DataFrame for easy querying and filtering
            dms_df = sc.parallelize(all_files).toDF()
            dms_df.createOrReplaceTempView("v1")

            # Filter the incremental DMS files who are ingested after a full load file
            # If a full load file is not present for the table, then all incremental files are fetched
            # Omit the incremental DMS files which are created before the full load file, if a full load file is present

            dms_df_incremental = spark.sql("select \
                                        concat('s3://', file_ingestion_s3_bucket, '/', file_ingestion_path) dms_file \
                                            from v1 where dms_file_type = 'Incremental' and  \
                                                file_ingestion_status = 'raw_file_landed'   and  \
                                                file_ingestion_date_time > "
                                           "(select coalesce(max(file_ingestion_date_time), '0') \
                                            from v1 where dms_file_type = 'Full' \
                                                 and file_ingestion_status = 'raw_file_landed')")

            # Convert the incremental DMS files to a python list

            incremental_refresh_list = dms_df_incremental.rdd.map(lambda x: x[0]).collect()
            if len(incremental_refresh_list) > 0:
                logger.info('Number of Incremental Load Files : ' + str(len(incremental_refresh_list)))
            else:
                logger.info('No Incremental Load Files To Be Processed')

            # Convert the DMS full load files DataFrame to a python list
            dms_df_full = spark.sql(
                "select distinct concat('s3://', file_ingestion_s3_bucket, '/', file_ingestion_path) dms_file "
                "from v1 where where dms_file_type = 'Full' and file_ingestion_status = 'raw_file_landed'")

            # convert the DMS full load files DataFrame to a python list
            full_refresh_list = dms_df_full.rdd.map(lambda x: x[0]).collect()

            if len(full_refresh_list) > 0:
                logger.info('Number of Full Load Files : {}'.format(str(len(full_refresh_list))))
            else:
                logger.info('No Full Load Files To Be Processed')

        return full_refresh_list, incremental_refresh_list, all_files

    def update_dms_file_tracker(self, refresh_file_list):
        """
        This function will update the processing status of the DMS files (full and incremental)
        Parameters:
            refresh_file_list(List): The list of DMS files that were processed
        """

        for file in refresh_file_list:
            # Insert the processed file info into the aws_dms_file_tracker_history
            # Delete the processed file from aws_dms_file_tracker
            file["file_ingestion_status"] = 'raw_file_processed'
            file["glue_job_run_id"] = args['JOB_RUN_ID']
            self.dynamodb_client.transact_write_items(
                TransactItems=[
                    {'Put': {'TableName': 'odpf_file_tracker_history',
                             'Item': {
                                 'source_table_name': {'S': file['source_table_name']},
                                 'file_id': {'S': file['file_id']},
                                 'file_ingestion_status': {'S': file['file_ingestion_status']},
                                 'file_ingestion_date_time': {'S': file['file_ingestion_date_time']},
                                 'file_ingestion_s3_bucket': {'S': file['file_ingestion_s3_bucket']},
                                 'file_ingestion_path': {'S': file['file_ingestion_path']},
                                 'dms_file_type': {'S': file['dms_file_type']},
                                 'schema_name': {'S': file['schema_name']},
                                 'table_name': {'S': file['table_name']}
                             }
                             }
                     },
                    {'Delete': {'TableName': 'odpf_file_tracker',
                                'Key': {'source_table_name': {'S': file['source_table_name']},
                                        'file_id': {'S': file['file_id']}
                                        }
                                }
                     }
                ]
            )


class RawTableRefreshStatus:
    """
    This is the class for Hudi Table refresh status
    """

    def __init__(self, refresh_table_name):
        self.refresh_table_name = refresh_table_name
        self.dynamodb = boto3.resource('dynamodb')
        self.dynamodb_client = boto3.client('dynamodb')

    def check_table_refresh_status(self):
        """
        This function gets the current execution status of the Glue Job
        Parameters:
            self.refresh_table_name(String): The full qualified table name
        Returns:
            refresh_status(Dict): DynamoDB record of the table refresh status
        """

        raw_table_refresh_status = None
        raw_pipeline_refresh_table = self.dynamodb.Table('odpf_file_processing_tracker')
        resp = raw_pipeline_refresh_table.get_item(
            Key={"raw_table_name": self.refresh_table_name}
        )
        if 'Item' in resp:
            raw_table_refresh_status = resp['Item']['refresh_status']

        return raw_table_refresh_status

    def init_refresh_status_rec(self):
        """
        This function initializes a table refresh status record
        :return: raw_table_refresh_rec: A dictionary with initialized table refresh status record
        """
        raw_table_refresh_rec = {"raw_table_name": self.refresh_table_name,
                                 "glue_job_run_id": args['JOB_RUN_ID'],
                                 "refresh_start_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}
        return raw_table_refresh_rec

    def update_raw_table_refresh_status(self, raw_table_refresh_status_rec):
        """
        This function updates the refresh status of the raw table
        Parameters:
            raw_table_refresh_status_rec(String): The table refresh record for the raw table
        """

        item = {'raw_table_name': {'S': raw_table_refresh_status_rec['raw_table_name']},
                'glue_job_run_id': {'S': raw_table_refresh_status_rec['glue_job_run_id']},
                'refresh_status': {'S': raw_table_refresh_status_rec['refresh_status']},
                'refresh_start_time': {'S': raw_table_refresh_status_rec['refresh_start_time']},
                'refresh_end_time': {'S': raw_table_refresh_status_rec['refresh_end_time']
                if 'refresh_end_time' in raw_table_refresh_status_rec else ''}
                }
        self.dynamodb_client.transact_write_items(TransactItems=[
            {'Put': {'TableName': 'odpf_file_processing_tracker',
                     'Item': item
                     }
             }
        ]
        )

    def update_raw_table_refresh_details(self, raw_table_ref_status_rec, raw_table_ref_details_rec):
        """
        This function updates the refresh status of the raw table
        Parameters:
            raw_table_ref_status_rec(String): The table refresh record for the raw table
            raw_table_ref_details_rec(String): The table refresh record for the raw table
                                         into the refresh history DynamoDB table
        """
        raw_table_refresh_history_rec = {**raw_table_ref_status_rec, **raw_table_ref_details_rec}

        item = {'raw_table_name': {'S': raw_table_refresh_history_rec['raw_table_name']},
                'glue_job_run_id': {'S': raw_table_refresh_history_rec['glue_job_run_id']},
                'refresh_status': {'S': raw_table_refresh_history_rec['refresh_status']},
                'refresh_start_time': {'S': raw_table_refresh_history_rec['refresh_start_time']},
                'refresh_end_time': {'S': raw_table_refresh_history_rec['refresh_end_time']
                if 'refresh_end_time' in raw_table_refresh_history_rec else ''},
                'error_msg': {'S': raw_table_refresh_history_rec['error_msg']
                if 'error_msg' in raw_table_refresh_history_rec else ''},
                'number_of_rows_inserted': {'S': str(raw_table_refresh_history_rec['number_of_rows_inserted'])
                if 'number_of_rows_inserted' in raw_table_refresh_history_rec else str(0)},
                'number_of_rows_updated': {'S': str(raw_table_refresh_history_rec['number_of_rows_updated'])
                if 'number_of_rows_updated' in raw_table_refresh_history_rec else str(0)},
                'number_of_rows_deleted': {'S': str(raw_table_refresh_history_rec['number_of_rows_deleted'])
                if 'number_of_rows_deleted' in raw_table_refresh_history_rec else str(0)}
                }
        self.dynamodb_client.transact_write_items(TransactItems=[
            {'Put': {'TableName': 'odpf_file_processing_tracker_history',
                     'Item': item
                     }
             }
        ]
        )


class HudiConfig:
    """
    This is the class for creating Hudi write configurations
    """

    def __init__(self, refresh_table_hudi_config):
        self.hudi_table_config = refresh_table_hudi_config

    def set_hudi_common_config(self):
        """
        This function will create common Hudi write configurations for Hudi full and incremental loads
        :return: hudi_common_config: A dictionary with common Hudi write configuration
        """
        hudi_common_config = {
            "hoodie.datasource.write.hive_style_partitioning": "true",
            "className": "org.apache.hudi",
            "hoodie.datasource.hive_sync.use_jdbc": "false",
            "hoodie.datasource.write.recordkey.field": self.hudi_table_config['hudi_primary_key'],
            "hoodie.datasource.write.precombine.field": self.hudi_table_config["hudi_precombine_field"],
            "hoodie.table.name": self.hudi_table_config["raw_table_name"],
            "hoodie.consistency.check.enabled": "true",
            "hoodie.datasource.hive_sync.database": self.hudi_table_config["raw_database_name"],
            "hoodie.datasource.hive_sync.table": self.hudi_table_config["raw_table_name"],
            "hoodie.datasource.hive_sync.enable": "true",
            "hoodie.datasource.write.partitionpath.field": "",
            "hoodie.datasource.hive_sync.support_timestamp": "true",
            "hoodie.datasource.hive_sync.mode": "hms",
            "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
            "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator"}
        if 'hudi_partition_key' in self.hudi_table_config:
            hudi_common_config["hoodie.datasource.write.partitionpath.field"] = self.hudi_table_config[
                'hudi_partition_key']
            hudi_common_config["hoodie.datasource.hive_sync.partition_fields"] = self.hudi_table_config[
                'hudi_partition_key']
        if self.hudi_table_config['table_data_versioning_type'] == 'H':
            # Include the pre_combine_field as part of the record key
            hudi_common_config['hoodie.datasource.write.recordkey.field'] = '{0},{1}'.format(
                self.hudi_table_config['hudi_primary_key'],
                self.hudi_table_config['hudi_precombine_field']
            )
        else:
            # just populate the primary key columns of the source table. This will keep the current row from source
            hudi_common_config['hoodie.datasource.write.recordkey.field'] = '{}'.format(
                self.hudi_table_config['hudi_primary_key']
            )
        return hudi_common_config

    def set_hudi_full_load_config(self):
        """
        This function will create hudi configuration for performing full load on a Hudi table
        :return: final_hudi_config: A dictionary with hudi bulk insert write configuration
        """
        hudi_common_config = self.set_hudi_common_config()
        init_load_config = {'hoodie.datasource.write.operation': 'bulk_insert'}
        if 'hudi_blk_insert_shuffle_parallelism' in self.hudi_table_config:
            init_load_config['hoodie.bulkinsert.shuffle.parallelism'] = self.hudi_table_config[
                'hudi_blk_insert_shuffle_parallelism']
        # else:
        # init_load_config['hoodie.bulkinsert.shuffle.parallelism'] = 25
        if 'hudi_full_load_rec_size_estimate' in self.hudi_table_config:
            init_load_config["hoodi_full_load_size_estimate"] = self.hudi_table_config[
                'hudi_full_load_rec_size_estimate']
        final_hudi_config = {**hudi_common_config, **init_load_config}
        return final_hudi_config

    def set_hudi_incremental_load_config(self):
        """
        This function will create Hudi configuration for performing upsert and deletes on a Hudi table
        :return: final_hudi_config: A dictionary with Hudi configuration for hudi upsert and delete
        """
        hudi_common_config = self.set_hudi_common_config()
        incremental_load_config = {'hoodie.datasource.write.operation': 'upsert'}
        if 'upsert_shuffle_parallelism' in self.hudi_table_config:
            incremental_load_config['hoodie.upsert.shuffle.parallelism'] = self.hudi_table_config[
                'upsert_shuffle_parallelism']
        if 'delete_shuffle_parallelism' in self.hudi_table_config:
            incremental_load_config['hoodie.delete.shuffle.parallelism'] = self.hudi_table_config[
                'upsert_shuffle_parallelism']
        if 'hudi_commits_to_keep' in self.hudi_table_config:
            incremental_load_config['hoodie.cleaner.policy'] = 'KEEP_LATEST_COMMITS'
            incremental_load_config['hoodie.cleaner.commits.retained'] = self.hudi_table_config['hudi_commits_to_keep']
        else:
            incremental_load_config['hoodie.cleaner.policy'] = 'KEEP_LATEST_COMMITS'
            incremental_load_config['hoodie.cleaner.commits.retained'] = 1

        final_hudi_config = {**hudi_common_config, **incremental_load_config}

        return final_hudi_config


class CdcUpdates:
    """
    This is the class for performing Hudi writes
    """

    def __init__(self, refresh_table_config):
        self.refresh_table_config = refresh_table_config

    def bulk_insert(self, full_ref_file_list):
        """
        This function will perform full load of a Hudi table
        :param full_ref_file_list: A list of CDC full load files
        :return: full_load_result: A dictionary with insert rec count
        """

        hudi_config = HudiConfig(self.refresh_table_config)
        full_load_hudi_config = hudi_config.set_hudi_full_load_config()
        logger.info(f'Hudi Config Used For Full Load : {str(full_load_hudi_config)}')
        full_load_df = spark.read.parquet(*full_ref_file_list)
        full_load_df.createOrReplaceTempView("fl_vw")
        full_load_df.printSchema()

        # if Table exists, drop it. Else, no action is needed
        drop_table_sql = "drop table if exists {0}.{1}".format(
            self.refresh_table_config['raw_database_name'],
            self.refresh_table_config['raw_table_name'])
        logger.info(f'drop table sql: {drop_table_sql}')
        spark.sql(drop_table_sql)

        raw_table_s3_path = f"s3://{self.refresh_table_config['raw_database_S3_bucket']}/" \
                            f"{self.refresh_table_config['raw_table_name']}/"
        logger.info(f'raw table s3 path : {raw_table_s3_path}')
        full_load_df.write.format("hudi").options(**full_load_hudi_config).mode("overwrite").save(raw_table_s3_path)
        full_load_cnt = spark.sql("select count(1) rc from {0}.{1}".format(
            self.refresh_table_config['raw_database_name'], self.refresh_table_config['raw_table_name'])).first()['rc']
        logger.info("Number Of Rows Inserted For Full Load : {}".format(full_load_cnt))
        full_load_result = {'number_of_rows_inserted': str(full_load_cnt)}
        return full_load_result

    def incremental_updates(self, incremental_ref_file_list):
        """
        This function will apply incremental updates to Hudi tables
        :param incremental_ref_file_list: A list of CDC incremental load files
        :return: incremental_refresh_result : A dictionary with insert / update / delete record counts
        """

        ins_cnt = 0
        upd_cnt = 0
        del_cnt = 0
        hudi_config = HudiConfig(self.refresh_table_config)
        incremental_load_hudi_config = hudi_config.set_hudi_incremental_load_config()
        incremental_load_df = spark.read.parquet(*incremental_ref_file_list)
        raw_table_path = "s3://{0}/{1}/".format(self.refresh_table_config['raw_database_S3_bucket'],
                                                self.refresh_table_config['raw_table_name'])

        # Check the table data versioning option (historical / current snapshot)

        if self.refresh_table_config['table_data_versioning_type'] == 'H':
            # If the table data versioning is historical, then we write all CDC records (insert/update/delete)
            # No hard deletes are performed on the hudi table
            # Include the pre_combine_field as part of the record key if the table needs to store Historical versions
            incremental_load_hudi_config['record_key'] = '{0},{1}'.format(
                self.refresh_table_config['hudi_primary_key'],
                self.refresh_table_config['hudi_precombine_field']
            )
            logger.info(f'Table Data Versioning is Historical')
            logger.info(f'Hudi Config Used For Incremental Load : {str(incremental_load_hudi_config)}')
            drop_cols = ['Op']
            incremental_df_final = incremental_load_df.drop(*drop_cols)
            incremental_df_final.createOrReplaceTempView("v_inc")
            # Insert all incremental inserts ,update and delete rows
            # process them as upsert operation on the Hudi raw table
            # incremental delete records are not deleted but inserted for historical purposes

            logger.info('Incremental Load Hudi Options : ' + str(hudi_config))

            incremental_df_final.printSchema()
            incremental_df_final.write.format("hudi").options(**incremental_load_hudi_config).mode("append").save(
                raw_table_path)

            logger.info('Incremental Row Count : ' + str(incremental_df_final.count()))
            ins_cnt = spark.sql("select count(1) cnt from v_inc where CDC_OPERATION ='INSERT'").first()['cnt']
            logger.info('Number of Inserts For Incremental Load : {}'.format(ins_cnt))
            upd_cnt = spark.sql("select count(1) cnt from v_inc where CDC_OPERATION ='UPDATE'").first()['cnt']
            logger.info('Number of Updates For Incremental Load : {}'.format(upd_cnt))
        else:
            # The table data versioning is current snapshot, all deletes should be applied
            # just populate the primary key columns of the source table
            incremental_load_hudi_config['record_key'] = '{}'.format(
                self.refresh_table_config['hudi_primary_key']
            )

            # Filter delete rows and process them as delete operation on the Hudi raw table
            # order the key and cdc_timestamp_seq to determine the last action taken on a key
            # This is to address a scenario where delete is performed on a key followed by an insert / Updates
            # In the below example, a key (1111) had multiple CDC rows in the same file and the desired outcome is to
            # do an upsert with cdc_seq 5
            # example:  Key    cdc_seq     Op
            #           1111        01      insert
            #           1111        02      update 
            #           1111        03      delete 
            #           1111        04      insert
            #           1111        05      update
            incremental_load_df.createOrReplaceTempView("v_inc")
            tmp_df = spark.sql(f"select * from (select a.*, \
                                    row_number() over(partition by {self.refresh_table_config['hudi_primary_key']} \
                                            order by {self.refresh_table_config['hudi_precombine_field']} desc) as rn from v_inc a ) b \
                                    where b.rn=1")
            drop_cols = ["Op", "rn"]
            del_df = tmp_df.filter("Op in ('D')").drop(*drop_cols)
            upd_df = tmp_df.filter("Op in ('I', 'U')").drop(*drop_cols)
            upd_df.createOrReplaceTempView("v_upsert")
            del_df.createOrReplaceTempView("v_delete")

            if len(upd_df.head(1)) > 0:
                logger.info('Upsert Row Count : ' + str(upd_df.count()))
                incremental_load_hudi_config["hoodie.datasource.write.operation"] = "upsert"
                upd_df.write.format("hudi").options(**incremental_load_hudi_config).mode("append").save(raw_table_path)
                ins_cnt = spark.sql("select count(1) cnt from v_upsert where CDC_OPERATION ='INSERT'").first()['cnt']
                logger.info('Number of Inserts For Incremental Load : {}'.format(ins_cnt))
                upd_cnt = spark.sql("select count(1) cnt from v_upsert where CDC_OPERATION ='UPDATE'").first()['cnt']
                logger.info('Number of Updates For Incremental Load : {}'.format(upd_cnt))
            else:
                logger.info('No Upsert Operations Performed')

            if len(del_df.head(1)) > 0:
                incremental_load_hudi_config["hoodie.datasource.write.operation"] = "delete"
                del_cnt = del_df.count()
                del_df.write.format("hudi").options(**incremental_load_hudi_config).mode("append").save(raw_table_path)
                logger.info('Delete Row Count : ' + str(del_cnt))

            else:
                logger.info('No Delete Operations Performed')

        incremental_refresh_result = {'number_of_rows_inserted': ins_cnt,
                                      'number_of_rows_updated': upd_cnt,
                                      'number_of_rows_deleted': del_cnt}
        return incremental_refresh_result


def get_parameters():
    """
    This function fetches the Glue JOB input parameters
    Returns:
        table_list(List): A list of tables to be refreshed
    """

    input_params = args['input_params']
    raw_table_list = eval(input_params)['batch_chunk']

    return raw_table_list


def process_files():
    """
    This is the main method of CDC data refresh which will call other functions to perform
        1. Hudi table Full data refresh
        2. Hudi Incremental data refresh (insert / update / delete)
    :return: None
    """
    raw_tables = get_parameters()
    raw_table_refresh_status_obj = {}
    raw_table_refresh_status_rec = {}
    load_result = None
    try:
        for raw_table_config in raw_tables:
            load_result = {}
            raw_table = raw_table_config['raw_table_name']
            source_table = raw_table_config['source_table_name']
            logger.info(f"Processing Table : {raw_table_config}")
            # Initialize the raw table refresh status and refresh details records
            raw_table_refresh_status_obj = RawTableRefreshStatus(raw_table)
            raw_table_refresh_status_rec = raw_table_refresh_status_obj.init_refresh_status_rec()
            # Initialize the DMS file tracker status rec
            file_tracker_obj = FileTrackerStatus(source_table)
            raw_table_refresh_status = raw_table_refresh_status_obj.check_table_refresh_status()

            # None status is if the refresh job is run for the first time
            if raw_table_refresh_status in [None, "completed", "refresh_error"]:
                raw_table_refresh_status_rec["refresh_status"] = "updating_table"
                raw_table_refresh_status_obj.update_raw_table_refresh_status(raw_table_refresh_status_rec)
                # get the list of DMS files that need to be processed for the table
                full_refresh_files, incremental_refresh_files, full_file_list = file_tracker_obj.get_raw_files()
                if len(full_refresh_files) > 0:
                    logger.info(f'Processing Full Load For Table : {raw_table}')
                    cdc_load = CdcUpdates(raw_table_config)
                    load_result = cdc_load.bulk_insert(full_refresh_files)
                else:
                    logger.info(f'No Full Load CDC Files For Table : {raw_table}')
                if len(incremental_refresh_files) > 0:
                    print(f'Processing Incremental Load For Table : {raw_table}')
                    cdc_load = CdcUpdates(raw_table_config)
                    load_result = cdc_load.incremental_updates(incremental_refresh_files)
                else:
                    logger.info(f'No Incremental Load CDC Files For Table : {raw_table}')

                # update the status of the file tracker
                file_tracker_obj.update_dms_file_tracker(full_file_list)
                # log the table refresh status and details
                raw_table_refresh_status_rec["refresh_status"] = "completed"
                raw_table_refresh_status_rec["refresh_end_time"] = datetime.datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S.%f')
                raw_table_refresh_status_obj.update_raw_table_refresh_status(raw_table_refresh_status_rec)
                raw_table_refresh_status_obj.update_raw_table_refresh_details(raw_table_refresh_status_rec,
                                                                              load_result)

    except Exception as e:
        logger.exception(e)
        raw_table_refresh_status_rec["refresh_status"] = "refresh_error"
        raw_table_refresh_status_rec["refresh_end_time"] = datetime.datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S.%f')
        load_result['error_msg'] = str(e)
        raw_table_refresh_status_obj.update_raw_table_refresh_status(raw_table_refresh_status_rec)
        raw_table_refresh_status_obj.update_raw_table_refresh_details(raw_table_refresh_status_rec,
                                                                      load_result)


if __name__ == "__main__":
    # create logger for the Glue Job
    logger = logging.getLogger('raw tables refresh')
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # set Glue job and Spark Context
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_params"])
    config = SparkConf().setAll(
        [
            ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
            ("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
        ]
    )
    sc = SparkContext(conf=config)  # Modified code to enable KryoSerializer
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    process_files()
