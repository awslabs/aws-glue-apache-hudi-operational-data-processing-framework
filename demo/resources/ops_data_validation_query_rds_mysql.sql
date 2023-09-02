/* 
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
    SPDX-License-Identifier: Apache-2.0
*/

/*
    Usage instructions:
        1. These commands to be run together.
        2. Run these commands in either MySQL Workbench or DBeaver IDE
*/

select 'table_1' table_name, count(1) count from taxi_trips.table_1
union all
select 'table_2' table_name, count(1) count from taxi_trips.table_2
union all
select 'table_3' table_name, count(1) count from taxi_trips.table_3
union all
select 'table_4' table_name, count(1) count from taxi_trips.table_4
union all
select 'table_5' table_name, count(1) count from taxi_trips.table_5
union all
select 'table_6' table_name, count(1) count from taxi_trips.table_6
union all
select 'table_7' table_name, count(1) count from taxi_trips.table_7
union all
select 'table_8' table_name, count(1) count from taxi_trips.table_8
union all
select 'table_9' table_name, count(1) count from taxi_trips.table_9
union all
select 'table_10' table_name, count(1) count from taxi_trips.table_10
union all
select 'table_11' table_name, count(1) count from taxi_trips.table_11
union all
select 'table_12' table_name, count(1) count from taxi_trips.table_12
union all
select 'table_13' table_name, count(1) count from taxi_trips.table_13
union all
select 'table_14' table_name, count(1) count from taxi_trips.table_14
union all
select 'table_15' table_name, count(1) count from taxi_trips.table_15
union all
select 'table_16' table_name, count(1) count from taxi_trips.table_16
union all
select 'table_17' table_name, count(1) count from taxi_trips.table_17
union all
select 'table_18' table_name, count(1) count from taxi_trips.table_18;