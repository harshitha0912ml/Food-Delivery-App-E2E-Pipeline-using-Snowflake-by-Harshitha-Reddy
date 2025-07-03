use role sysadmin;
use warehouse adhoc_wh;
use database foodapp_sandbox;
use schema stage_schema;

-- query external locatiaon stage by adding audit columns
select 
    t.$1::text as locationid,
    t.$2::text as city,
    t.$3::text as state,
    t.$4::text as zipcode,
    t.$5::text as activeflag,
    t.$6::text as createddate,
    t.$7::text as modifieddate,
    
    -- audit columns for tracking & debugging
    metadata$filename as _stg_file_name,
    metadata$file_last_modified as _stg_file_load_ts,
    metadata$file_content_key as _stg_file_md5,
    current_timestamp as _copy_data_ts
    
from @stage_schema.csv_stg/initial/location/location-5rows.csv 
(file_format => 'stage_schema.csv_file_format') t;


----create a stage level location table---
create or replace table stage_schema.location (
    locationid text,
    city text,
    state text,
    zipcode text,
    activeflag text,
    createddate text,
    modifieddate text,
    -- audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
);


-- create a append only stream object on stage location table.
create or replace stream stage_schema.location_stm 
on table stage_schema.location
append_only = true
comment = 'this is the append-only stream object on location table that gets delta data based on changes';


--copying into stage level table----
copy into stage_schema.location (locationid, city, state, zipcode, activeflag, 
                    createddate, modifieddate, _stg_file_name, 
                    _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as locationid,
        t.$2::text as city,
        t.$3::text as state,
        t.$4::text as zipcode,
        t.$5::text as activeflag,
        t.$6::text as createddate,
        t.$7::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_schema.csv_stg/initial/location t
)
file_format = (format_name = 'stage_schema.csv_file_format')
on_error = abort_statement;

select * from stage_schema.location;

select * from stage_schema.location_stm;

------CURRENT COPY STATUS CHECK------------
select * from table(information_schema.copy_history(table_name=>'LOCATION', start_time=> dateadd(hours, -1, current_timestamp())));