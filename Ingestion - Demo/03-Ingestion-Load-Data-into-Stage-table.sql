
USE WAREHOUSE COMPUTE_WH;
SELECT CURRENT_WAREHOUSE();

USE ROLE SYSADMIN;
SELECT CURRENT_ROLE();
--SHOW DATABASES;
USE DATABASE DEMO;

USE SCHEMA FILE_INGESTION_DEMO;

-- ------------------------------------------------------------
-- Load Raw Layer
-- ------------------------------------------------------------
--
-- Load the orders table and review the output
--    1.  Note the rows_parsed and the errors_seen
--     

select * from DEMO.FILE_INGESTION_DEMO.ORDERS ;
COPY INTO DEMO.FILE_INGESTION_DEMO.ORDERS (O_ORDERKEY,O_CUSTKEY,O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY,O_COMMENT,FILE_NAME,LOAD_DATETIME,LOAD_BY)
FROM (
    SELECT 
        $1,
        $2, 
        $3, 
        $4, 
        $5, 
        $6, 
        $7, 
        $8, 
        $9, 
        METADATA$FILENAME,
        CURRENT_TIMESTAMP(),
        CURRENT_USER() 
    FROM @DEMO.FILE_INGESTION_DEMO.TPCH_INT_STG/ORDERS/     
)
FILE_FORMAT = DEMO.FILE_INGESTION_DEMO.TPCH_ORDERS_CSV
ON_ERROR=CONTINUE
;
select count(*) from  DEMO.FILE_INGESTION_DEMO.ORDERS ; -- 1.5M

/* show data stored in JSON format in source files */
SELECT $1  -- Full Json Field
    , METADATA$FILENAME,METADATA$FILE_ROW_NUMBER,CURRENT_TIMESTAMP(),CURRENT_USER() 
from @DEMO.FILE_INGESTION_DEMO.TPCH_INT_STG/CUSTOMER/ 
(FILE_FORMAT => DEMO.FILE_INGESTION_DEMO.TPCH_CUSTOMER_JSON) 
LIMIT 10;

-- Load the customers table using the JSON data
COPY INTO DEMO.FILE_INGESTION_DEMO.CUSTOMER
FROM (
    SELECT 
        PARSE_JSON($1):C_CUSTKEY,
        $1, 
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        CURRENT_TIMESTAMP(),
        CURRENT_USER()        
    FROM @DEMO.FILE_INGESTION_DEMO.TPCH_INT_STG/CUSTOMER
) 
FILE_FORMAT = DEMO.FILE_INGESTION_DEMO.TPCH_CUSTOMER_JSON
ON_ERROR=CONTINUE
;

select count(*) from DEMO.FILE_INGESTION_DEMO.CUSTOMER; -- 1.5M
select count(*) from DEMO.FILE_INGESTION_DEMO.ORDERS; 

-- Execute the copy to table command again and 0 files should be processed now



