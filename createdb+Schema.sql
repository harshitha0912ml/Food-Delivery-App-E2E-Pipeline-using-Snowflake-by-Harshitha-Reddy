--using sysadmin role 
use role sysadmin;

--CREATE DATABASE
create database if not exists foodapp_sandbox;
use database foodapp_sandbox;

--CREATE SCHEMAS
create schema if not exists stage_schema;
create schema if not exists clean_schema;
create schema if not exists consumption_schema;
create schema if not exists common;

--schema usage---
use schema stage_schema;

--create FILE FORMAT
create file format if not exists stage_schema.csv_file_format 
        type = 'csv' 
        compression = 'auto' 
        field_delimiter = ',' 
        record_delimiter = '\n' 
        skip_header = 1 
        field_optionally_enclosed_by = '\042' 
        null_if = ('\\N');

-- create snowflake internal stage
create stage stage_schema.csv_stg
    directory = ( enable = true )
    comment = 'this is the snowflake internal stage';


--creating PII Tags and masking policy
-- create tag objects
create or replace tag 
    common.pii_policy_tag 
    allowed_values 'PII','PRICE','SENSITIVE','EMAIL'
    comment = 'This is PII policy tag object';

-- create masking policies
create or replace masking policy 
    common.pii_masking_policy as (pii_text string)
    returns string -> 
    to_varchar('** PII **');

create or replace masking policy 
    common.email_masking_policy as (email_text string)
    returns string -> 
    to_varchar('** EAMIL **');

create or replace masking policy 
    common.phone_masking_policy as (phone string)
    returns string -> 
    to_varchar('** Phone **');

list @stage_schema.csv_stg;