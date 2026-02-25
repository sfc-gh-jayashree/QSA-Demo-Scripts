USE WAREHOUSE COMPUTE_WH;
SELECT CURRENT_WAREHOUSE();

USE ROLE SYSADMIN;
SELECT CURRENT_ROLE();
SHOW DATABASES;
USE DATABASE DEMO;

USE SCHEMA FILE_INGESTION_DEMO;

---------------------------------------------------------------------------------------------------------------------
-- Creates a separate row for each part of the json object
select *
from  DEMO.FILE_INGESTION_DEMO.CUSTOMER;

select *
from  DEMO.FILE_INGESTION_DEMO.CUSTOMER c
, lateral flatten(input => c.customer_record) flat;

CREATE OR REPLACE DYNAMIC TABLE DEMO.FILE_INGESTION_DEMO.customer_flattened
  TARGET_LAG = '60 seconds'
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = INCREMENTAL    -- Note Snowflake will tell you this will not work
  INITIALIZE = on_create
  AS
   select c_custkey, c.file_name, c.file_row_number, c.load_datetime, c.load_by, flat.key, flat.value
        from  DEMO.FILE_INGESTION_DEMO.CUSTOMER c
            , lateral flatten(input => c.customer_record) flat
    ;

     select c_custkey, c.file_name, c.file_row_number, c.load_datetime, c.load_by, flat.key, flat.value
        from  DEMO.FILE_INGESTION_DEMO.CUSTOMER c
            , lateral flatten(input => c.customer_record) flat
    ;

    SELECT COUNT(1)  FROM DEMO.FILE_INGESTION_DEMO.customer_flattened; -- 

    show dynamic tables;


--insert new records in customer and order
    set new_cust_key = (select max(C_CUSTKEY) from DEMO.FILE_INGESTION_DEMO.CUSTOMER) + 1;
select $new_cust_key;


insert into  DEMO.FILE_INGESTION_DEMO.CUSTOMER (C_CUSTKEY, CUSTOMER_RECORD, FILE_NAME, FILE_ROW_NUMBER, LOAD_DATETIME, LOAD_BY)
select 
    $new_cust_key,
    TO_VARIANT(PARSE_JSON('{ "C_ACCTBAL": 0.0, "C_ADDRESS": "Address of Ron Insert INCREMENT", "C_COMMENT": "blithely final packages. regular, final ideas along the regular, regular foxes haggle blithely furiously regular ",  "C_CUSTKEY": '||$new_cust_key||',  "C_MKTSEGMENT": "AUTOMOBILE",  "C_NAME": "Ron Insert INCREMENT",  "C_NATIONKEY": 22,  "C_PHONE": "32-957-348-8193" }')),
    NULL,
    NULL,
    sysdate(),
    current_user()
;

select * from DEMO.FILE_INGESTION_DEMO.CUSTOMER where C_CUSTKEY = $new_cust_key;

set max_order = (select max(O_ORDERKEY) from DEMO.FILE_INGESTION_DEMO.ORDERS);
select $max_order+1;

insert into DEMO.FILE_INGESTION_DEMO.ORDERS (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, FILE_NAME, LOAD_DATETIME, LOAD_BY)
select $max_order+1
    , $new_cust_key
    , 'O'
    , 77
    , sysdate()
    , '5-LOW'
    , 'Clerk#000000777'
    , 'INSERT'
    , sysdate()
    , current_user()
    ;

insert into DEMO.FILE_INGESTION_DEMO.ORDERS (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, FILE_NAME, LOAD_DATETIME, LOAD_BY)
select $max_order+2
    , $new_cust_key
    , 'O'
    , 147
    , sysdate()
    , '5-LOW'
    , 'Clerk#000000777'
    , 'INSERT'
    , sysdate()
    , current_user()
    ;

insert into DEMO.FILE_INGESTION_DEMO.ORDERS (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, FILE_NAME, LOAD_DATETIME, LOAD_BY)
select $max_order+3
    , $new_cust_key
    , 'O'
    , 227
    , sysdate()
    , '5-LOW'
    , 'Clerk#000000777'
    , 'INSERT'
    , sysdate()
    , current_user()
    ;

      SELECT COUNT(1)  FROM DEMO.FILE_INGESTION_DEMO.customer_flattened; -- 12000048

        SELECT *  FROM DEMO.FILE_INGESTION_DEMO.customer_flattened
        WHERE c_custkey = $new_cust_key;

        SHOW DYNAMIC TABLES;

        DROP TABLE CUSTOMER_FLATTENED;

        