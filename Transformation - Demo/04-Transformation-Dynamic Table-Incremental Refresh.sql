USE WAREHOUSE COMPUTE_WH;
SELECT CURRENT_WAREHOUSE();

USE ROLE SYSADMIN;
SELECT CURRENT_ROLE();
SHOW DATABASES;
USE DATABASE DEMO;

USE SCHEMA TRANSFORMATION_DEMO_CURATED_LAYER;

---------------------------------------------------------------------------------------------------------------------



select distinct c.CUSTOMER_RECORD:C_NAME:: STRING as cust_name
, count(O_ORDERKEY) as order_count
, sum(O_TOTALPRICE) as order_total
from DEMO.FILE_INGESTION_DEMO.ORDERS o
join DEMO.FILE_INGESTION_DEMO.CUSTOMER c on c.c_custkey = o.o_custkey
group by cust_name
limit 10;

select c.CUSTOMER_RECORD
from  DEMO.FILE_INGESTION_DEMO.ORDERS o
join DEMO.FILE_INGESTION_DEMO.CUSTOMER c on c.c_custkey = o.o_custkey
limit 10;

-----------------------------------------------------------------cleanup needed?
select * from DEMO.FILE_INGESTION_DEMO.ORDERS where o_custkey in (select c_custkey from DEMO.FILE_INGESTION_DEMO.CUSTOMER where CUSTOMER_RECORD:C_NAME:: STRING = 'Ron Insert INCREMENT');

--delete from DEMO.FILE_INGESTION_DEMO.ORDERS where o_custkey in (select c_custkey from DEMO.FILE_INGESTION_DEMO.CUSTOMER where CUSTOMER_RECORD:C_NAME:: STRING = 'Ron Insert INCREMENT');
--delete from DEMO.FILE_INGESTION_DEMO.CUSTOMER where CUSTOMER_RECORD:C_NAME:: STRING = 'Ron Insert INCREMENT'
---------------------------------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.customer_order_totals_increment
  TARGET_LAG = '60 seconds'
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = INCREMENTAL
  INITIALIZE = on_create
  AS
    select distinct c.c_custkey as cust_key
    , count(O_ORDERKEY) as order_count
    , sum(O_TOTALPRICE) as order_total
    from DEMO.FILE_INGESTION_DEMO.ORDERS o
    join DEMO.FILE_INGESTION_DEMO.CUSTOMER c on c.c_custkey = o.o_custkey
    group by cust_key
    ;

    show dynamic tables;

-- insert new records for customer and orders table
set new_cust_key = (select max(C_CUSTKEY) from DEMO.FILE_INGESTION_DEMO.CUSTOMER ) + 1;
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


    select 227+147+77; -- 451

      select O_CUSTKEY, count(O_ORDERKEY), SUM(O_TOTALPRICE) 
    from DEMO.FILE_INGESTION_DEMO.ORDERS
    where O_CUSTKEY = $new_cust_key
    GROUP BY O_CUSTKEY    
    ;
    --3 orders, Total = 451

    select distinct c.c_custkey as cust_key
    , count(O_ORDERKEY) as order_count
    , sum(O_TOTALPRICE) as order_total
    from DEMO.FILE_INGESTION_DEMO.ORDERS o
    join DEMO.FILE_INGESTION_DEMO.CUSTOMER c on c.c_custkey = o.o_custkey
    where c.c_custkey = $new_cust_key
    group by cust_key;
    --3 orders, Total = 451

     --wait for dynamic table lag
    select * from DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.CUSTOMER_ORDER_TOTALS_INCREMENT where cust_key = $new_cust_key;
    --3 orders, Total = 451

    select $max_order + 3;
    select * from DEMO.FILE_INGESTION_DEMO.ORDERS where O_ORDERKEY = $max_order + 3;
    
    update DEMO.FILE_INGESTION_DEMO.ORDERS set O_TOTALPRICE = 777 where O_ORDERKEY = $max_order + 3;

    
    select 777+147+77; -- 1001

    select O_CUSTKEY, count(O_ORDERKEY), SUM(O_TOTALPRICE) 
    from DEMO.FILE_INGESTION_DEMO.ORDERS
    where O_CUSTKEY = $new_cust_key
    GROUP BY O_CUSTKEY;
    --3 orders, Total = 1001

     --wait for dynamic table lag
    select * from DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.CUSTOMER_ORDER_TOTALS_INCREMENT where cust_key = $new_cust_key;
 drop table   DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.CUSTOMER_ORDER_TOTALS_INCREMENT ;