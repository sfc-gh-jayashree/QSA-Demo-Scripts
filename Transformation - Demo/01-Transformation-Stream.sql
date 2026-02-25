
-- comment added
USE WAREHOUSE COMPUTE_WH;
SELECT CURRENT_WAREHOUSE();

USE ROLE SYSADMIN;
SELECT CURRENT_ROLE();
SHOW DATABASES;
USE DATABASE DEMO;

USE SCHEMA TRANSFORMATION_DEMO_CURATED_LAYER;

-------------------------------------------------------------------------------------------------------------------------------------
-- cleanup scripts
--DROP STREAM DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.customer_stream;
--DROP TABLE DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.new_customer_by_month;
-------------------------------------------------------------------------------------------------------------------------------------

SELECT COUNT(1) FROM DEMO.FILE_INGESTION_DEMO.CUSTOMER;


create or replace stream DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.customer_stream 
on table DEMO.FILE_INGESTION_DEMO.CUSTOMER;

select * from DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.customer_stream;

-- insert records into customer table

select max(c_custkey) + 1 from DEMO.FILE_INGESTION_DEMO.customer; --1500001

insert into DEMO.FILE_INGESTION_DEMO.CUSTOMER(c_custkey,customer_record,file_name,file_row_number,load_datetime,load_by)
select (select max(c_custkey) + 1 from DEMO.FILE_INGESTION_DEMO.customer), 
PARSE_JSON('{
  "C_ACCTBAL": 1200,
  "C_ADDRESS": "Curridabat, San Jose",
  "C_COMMENT": "This is my comment",
  "C_CUSTKEY": 1500001,
  "C_MKTSEGMENT": "AUTOMOBILE",
  "C_NAME": "Jafet Welsh",
  "C_NATIONKEY": 10,
  "C_PHONE": "506-83461420"
}'), 'dummy.json', 1001, current_timestamp, 'JSURESH';

insert into DEMO.FILE_INGESTION_DEMO.CUSTOMER(c_custkey,customer_record,file_name,file_row_number,load_datetime,load_by)
select (select max(c_custkey) + 1 from DEMO.FILE_INGESTION_DEMO.customer), 
PARSE_JSON('{
  "C_ACCTBAL": 1200,
  "C_ADDRESS": "California",
  "C_COMMENT": "Insert #2",
  "C_CUSTKEY": 1500002,
  "C_MKTSEGMENT": "AUTOMOBILE",
  "C_NAME": "snow",
  "C_NATIONKEY": 10,
  "C_PHONE": "234242242"
}'), 'dummy.json', 1002, current_timestamp, 'JSURESH';


-- Review the metadata columns
select * from DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.customer_stream;

-- Create the destination table
create or replace table DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.new_customer_by_month
(
year_month varchar,
counting number
);

select * from DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.new_customer_by_month;

-- Populate the destination table using the stream
   merge into DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.new_customer_by_month
   using 
   (
   select year(load_datetime)||'-'||lpad(month(load_datetime),2,'0') year_month, count(*) counting 
          from customer_stream 
          where metadata$action = 'INSERT' group by year(load_datetime)||'-'||lpad(month(load_datetime),2,'0'))
          as src 
      on new_customer_by_month.year_month = src.year_month
   when matched then update set new_customer_by_month.counting = new_customer_by_month.counting + src.counting
   when not matched then insert(year_month, counting) values(src.year_month, src.counting);

   select * from DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.new_customer_by_month;

   select count(1) from DEMO.FILE_INGESTION_DEMO.CUSTOMER;

-- Since we have consumed, now the stream will be empty
   select * from customer_stream;

