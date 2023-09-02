/* 
  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
  SPDX-License-Identifier: Apache-2.0
*/

/*
  Usage instructions:
    1. Create 18 Tables using the following DDL.
    2. Run this command in either MySQL Workbench or DBeaver IDE
    3. Start with table_1 and each time you run, increment the number 
    to the next and stop when you finish creating all the 18 tables.
*/

CREATE TABLE `table_1` (
  `VendorID` bigint DEFAULT NULL,
  `tpep_pickup_datetime` timestamp NULL DEFAULT NULL,
  `tpep_dropoff_datetime` timestamp NULL DEFAULT NULL,
  `passenger_count` double DEFAULT NULL,
  `trip_distance` double DEFAULT NULL,
  `RatecodeID` double DEFAULT NULL,
  `store_and_fwd_flag` text,
  `PULocationID` bigint DEFAULT NULL,
  `DOLocationID` bigint DEFAULT NULL,
  `payment_type` bigint DEFAULT NULL,
  `fare_amount` double DEFAULT NULL,
  `extra` double DEFAULT NULL,
  `mta_tax` double DEFAULT NULL,
  `tip_amount` double DEFAULT NULL,
  `tolls_amount` double DEFAULT NULL,
  `improvement_surcharge` double DEFAULT NULL,
  `total_amount` double DEFAULT NULL,
  `congestion_surcharge` double DEFAULT NULL,
  `airport_fee` double DEFAULT NULL,
  `pk_id` bigint NOT NULL AUTO_INCREMENT,
   PRIMARY KEY (pk_id)
);
