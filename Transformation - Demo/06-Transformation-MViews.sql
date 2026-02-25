USE WAREHOUSE COMPUTE_WH;
SELECT CURRENT_WAREHOUSE();

USE ROLE SYSADMIN;
SELECT CURRENT_ROLE();
SHOW DATABASES;
USE DATABASE DEMO;

USE SCHEMA TRANSFORMATION_DEMO_CURATED_LAYER;

---------------------------------------------------------------------------------------------------------------------
DROP TABLE DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.LINEITEM_CURATED
DROP MATERIALIZED VIEW DEMO.FILE_INGESTION_DEMO.LINEITEM_M_VW;
DROP MATERIALIZED VIEW DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.LINEITEM_SHIPDATE;

---------------------------------------------------------------------------------------------------------------------

ALTER WAREHOUSE COMPUTE_WH SUSPEND;
ALTER WAREHOUSE COMPUTE_WH RESUME;
ALTER SESSION SET USE_CACHED_RESULT = FALSE;


CREATE OR REPLACE TABLE DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.LINEITEM_CURATED
AS
SELECT
    * EXCLUDE (
        LOAD_BY,   -- Column 1 to exclude
        LOAD_DATETIME     -- Column 2 to exclude
    )
FROM
    DEMO.FILE_INGESTION_DEMO.LINEITEM;

CREATE OR REPLACE MATERIALIZED VIEW DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.LINEITEM_M_VW
AS
       select
       l_returnflag,
       l_linestatus,
       sum(l_quantity) as sum_qty,
       sum(l_extendedprice) as sum_base_price,
       sum(l_extendedprice * (1-l_discount)) as sum_disc_price,
       sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge,
       avg(l_quantity) as avg_qty,
       avg(l_extendedprice) as avg_price,
       avg(l_discount) as avg_disc,
       count(*) as count_order
 from
       DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.LINEITEM_CURATED
 where
       l_shipdate <= dateadd(day, -90, to_date('1998-12-01'))
 group by
       l_returnflag,
       l_linestatus; 

SELECT * FROM DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.LINEITEM_M_VW; --345ms

ALTER WAREHOUSE COMPUTE_WH SUSPEND;
ALTER WAREHOUSE COMPUTE_WH RESUME;

 select
       l_returnflag,
       l_linestatus,
       sum(l_quantity) as sum_qty,
       sum(l_extendedprice) as sum_base_price,
       sum(l_extendedprice * (1-l_discount)) as sum_disc_price,
       sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge,
       avg(l_quantity) as avg_qty,
       avg(l_extendedprice) as avg_price,
       avg(l_discount) as avg_disc,
       count(*) as count_order
 from
       DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.LINEITEM_CURATED
 where
       l_shipdate <= dateadd(day, -90, to_date('1998-12-01'))
 group by
       l_returnflag,
       l_linestatus; --688ms

       

-- Second Example - COmmon use case is having an alternative clustering key
create OR REPLACE materialized view DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.LINEITEM_SHIPDATE_M_VW --7.3s
   cluster by (l_shipdate)
as select L_ORDERKEY ,
	L_PARTKEY ,
	L_SUPPKEY ,
	L_LINENUMBER ,
	L_QUANTITY ,
	L_EXTENDEDPRICE ,
	L_DISCOUNT ,
	L_TAX ,
	L_RETURNFLAG ,
	L_LINESTATUS,
	L_SHIPDATE,
	L_COMMITDATE ,
	L_RECEIPTDATE ,
	L_SHIPINSTRUCT ,
	L_SHIPMODE ,
	L_COMMENT
 from
       DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.LINEITEM_CURATED
;

select max(l_shipdate) 
from DEMO.FILE_INGESTION_DEMO.LINEITEM;

select * 
from  DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.LINEITEM_CURATED
where l_shipdate = '1998-12-01'; --798ms


-- Note it only scnas 1 partition
select * 
from DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.LINEITEM_SHIPDATE_M_VW
where l_shipdate = '1998-12-01'; 


-- Refresh lag
select max(l_orderkey) from  DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.LINEITEM_CURATED; --6000000
select max(l_orderkey ) from LINEITEM_SHIPDATE_M_VW;--6000000

insert into DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.LINEITEM_CURATED
(select  L_ORDERKEY + 1,
	L_PARTKEY ,
	L_SUPPKEY ,
	L_LINENUMBER ,
	L_QUANTITY ,
	L_EXTENDEDPRICE ,
	L_DISCOUNT ,
	L_TAX ,
	L_RETURNFLAG ,
	L_LINESTATUS,
	L_SHIPDATE,
	L_COMMITDATE ,
	L_RECEIPTDATE ,
	L_SHIPINSTRUCT ,
	L_SHIPMODE ,
	L_COMMENT,
    'TED_TEST'
 from DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.LINEITEM_CURATED
 where l_orderkey = 6000000);

select max(l_orderkey) from  DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.LINEITEM_CURATED; --6000000
select max(l_orderkey ) from LINEITEM_SHIPDATE_M_VW;


SHOW MATERIALIZED VIEWS;


