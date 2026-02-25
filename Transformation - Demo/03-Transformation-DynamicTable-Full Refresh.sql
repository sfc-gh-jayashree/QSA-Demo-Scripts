-- comment added
USE WAREHOUSE COMPUTE_WH;
SELECT CURRENT_WAREHOUSE();

USE ROLE SYSADMIN;
SELECT CURRENT_ROLE();
SHOW DATABASES;
USE DATABASE DEMO;

USE SCHEMA TRANSFORMATION_DEMO_CURATED_LAYER;

---------------------------------------------------------------------------------------------------------------------
--DROP TABLE DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.customer_order_totals;

---------------------------------------------------------------------------------------------------------------------

-- Background queries on what the Dynamic table will do
select distinct c.CUSTOMER_RECORD:C_NAME:: STRING as cust_name
, count(O_ORDERKEY) as order_count
, sum(O_TOTALPRICE) as order_total
from DEMO.FILE_INGESTION_DEMO.ORDERS o
join DEMO.FILE_INGESTION_DEMO.CUSTOMER c on c.c_custkey = o.o_custkey
group by cust_name
limit 10;

-- Create the dynamic table
CREATE OR REPLACE DYNAMIC TABLE DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.customer_order_totals
  TARGET_LAG = '60 seconds'  -- This can be set to whatever value you need
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = full
  INITIALIZE = on_create
  AS
  select distinct c.CUSTOMER_RECORD:C_NAME:: STRING as cust_name
, count(O_ORDERKEY) as order_count
, sum(O_TOTALPRICE) as order_total
from DEMO.FILE_INGESTION_DEMO.ORDERS o
join DEMO.FILE_INGESTION_DEMO.CUSTOMER c on c.c_custkey = o.o_custkey
group by cust_name
    ;

-- Read like any other table or view
select * from DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.CUSTOMER_ORDER_TOTALS ;

/*
--insert a new customer
--insert customer orders

select top 10 * from SNOW_D_RAW.TPCH.ORDERS;

select max(C_CUSTKEY) from DEMO.FILE_INGESTION_DEMO.CUSTOMER;

select sysdate();

{
  "C_ACCTBAL": 0.0,
  "C_ADDRESS": "Address of Ron Insert",
  "C_COMMENT": "blithely final packages. regular, final ideas along the regular, regular foxes haggle blithely furiously regular ",
  "C_CUSTKEY": 1500003,
  "C_MKTSEGMENT": "AUTOMOBILE",
  "C_NAME": "Ron Insert",
  "C_NATIONKEY": 22,
  "C_PHONE": "32-957-348-8193"
}
*/

select max(C_CUSTKEY)+1 from DEMO.FILE_INGESTION_DEMO.CUSTOMER;

insert into DEMO.FILE_INGESTION_DEMO.CUSTOMER (C_CUSTKEY, CUSTOMER_RECORD, FILE_NAME, FILE_ROW_NUMBER, LOAD_DATETIME, LOAD_BY)
select 
    1500004,
    TO_VARIANT(PARSE_JSON('{ "C_ACCTBAL": 0.0, "C_ADDRESS": "Address of Ron Insert", "C_COMMENT": "blithely final packages. regular, final ideas along the regular, regular foxes haggle blithely furiously regular ",  "C_CUSTKEY": 1500004,  "C_MKTSEGMENT": "AUTOMOBILE",  "C_NAME": "Ron Insert",  "C_NATIONKEY": 22,  "C_PHONE": "32-957-348-8193" }')),
    NULL,
    NULL,
    sysdate(),
    current_user()
;

SELECT * FROM DEMO.FILE_INGESTION_DEMO.CUSTOMER
WHERE C_CUSTKEY = 1500004;


-- insert 3 records in the order table
set max_order = (select max(O_ORDERKEY) from DEMO.FILE_INGESTION_DEMO.ORDERS);
select $max_order+1; --6000001

insert into DEMO.FILE_INGESTION_DEMO.ORDERS (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, FILE_NAME, LOAD_DATETIME, LOAD_BY)
select $max_order+1
    , 1500004
    , 'O'
    , 7
    , sysdate()
    , '5-LOW'
    , 'Clerk#000000777'
    , 'INSERT'
    , sysdate()
    , current_user()
    ;

insert into DEMO.FILE_INGESTION_DEMO.ORDERS (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, FILE_NAME, LOAD_DATETIME, LOAD_BY)
select $max_order+2
    , 1500004
    , 'O'
    , 14
    , sysdate()
    , '5-LOW'
    , 'Clerk#000000777'
    , 'INSERT'
    , sysdate()
    , current_user()
    ;

insert into DEMO.FILE_INGESTION_DEMO.ORDERS (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, FILE_NAME, LOAD_DATETIME, LOAD_BY)
select $max_order+3
    , 1500004
    , 'O'
    , 22
    , sysdate()
    , '5-LOW'
    , 'Clerk#000000777'
    , 'INSERT'
    , sysdate()
    , current_user()
    ;

-- inserted data
      select * from  DEMO.FILE_INGESTION_DEMO.ORDERS 
    where O_CUSTKEY = 1500004;

    select * from DEMO.FILE_INGESTION_DEMO.CUSTOMER where customer_record:C_NAME = 'Ron Insert';

select O_CUSTKEY, count(O_ORDERKEY), SUM(O_TOTALPRICE) 
    from DEMO.FILE_INGESTION_DEMO.ORDERS 
    where O_CUSTKEY = 1500004
    GROUP BY O_CUSTKEY    
    ;
    --3 orders, Total = 43

     select * from  customer_order_totals where cust_name = 'Ron Insert';

     show dynamic tables;

     select $max_order + 3;
    select * from DEMO.FILE_INGESTION_DEMO.ORDERS where O_ORDERKEY = $max_order + 3;
     update DEMO.FILE_INGESTION_DEMO.ORDERS set O_TOTALPRICE = 77 where O_ORDERKEY = $max_order + 3;

     select O_CUSTKEY, count(O_ORDERKEY), SUM(O_TOTALPRICE) 
    from DEMO.FILE_INGESTION_DEMO.ORDERS
    where O_CUSTKEY = 1500004
    GROUP BY O_CUSTKEY;

    
    --wait for dynamic table lag
    select * from DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.CUSTOMER_ORDER_TOTALS where cust_name = 'Ron Insert';

    --drop table DEMO.FILE_INGESTION_DEMO.CUSTOMER_ORDER_TOTALS ;

  