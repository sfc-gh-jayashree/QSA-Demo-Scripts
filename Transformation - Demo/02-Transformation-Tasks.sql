-- comment added
USE WAREHOUSE COMPUTE_WH;
SELECT CURRENT_WAREHOUSE();

USE ROLE SYSADMIN;
SELECT CURRENT_ROLE();
SHOW DATABASES;
USE DATABASE DEMO;

USE SCHEMA DEMO.TRANSFORMATION_DEMO_CURATED_LAYER;
------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Cleanup scripts

--DROP TASK DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.task_new_customer_by_month;


------------------------------------------------------------------------------------------------------------------------------------------------------------
-- create task
create or replace task DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.task_new_customer_by_month
warehouse = 'COMPUTE_WH' -- We can use serverless as well, SF will determine the appropriate warehouse size based on history.
schedule = '1 minute'
when system$stream_has_data('customer_stream')  -- Use to avoid extra cost!
as
begin
   merge into new_customer_by_month
   using (select year(load_datetime)||'-'||lpad(month(load_datetime),2,'0') year_month, count(*) counting 
          from customer_stream where metadata$action = 'INSERT' group by year(load_datetime)||'-'||lpad(month(load_datetime),2,'0')) as src 
      on new_customer_by_month.year_month = src.year_month
   when matched then update set new_customer_by_month.counting = new_customer_by_month.counting + src.counting
   when not matched then insert(year_month, counting) values(src.year_month, src.counting);
end;
-- Grant execute privileges to Sysadmin
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;

show tasks in SCHEMA DEMO.TRANSFORMATION_DEMO_CURATED_LAYER;  -- Note the state is "suspended", we must resume the task
alter task TRANSFORMATION_DEMO_CURATED_LAYER.task_new_customer_by_month resume;
DESC TASK DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.task_new_customer_by_month;  -- Note the state is "started"

-- No data in stream now
select * from DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.customer_stream;

select max(c_custkey) + 1 from DEMO.FILE_INGESTION_DEMO.CUSTOMER;

-- Add another record to show task being executed
insert into DEMO.FILE_INGESTION_DEMO.CUSTOMER(c_custkey,customer_record,file_name,file_row_number,load_datetime,load_by)
select (select max(c_custkey) + 1 from DEMO.FILE_INGESTION_DEMO.CUSTOMER), 
PARSE_JSON('{
  "C_ACCTBAL": 1204,
  "C_ADDRESS": "Thomas St, San Jose",
  "C_COMMENT": "This is my comment",
  "C_CUSTKEY": 1500003,
  "C_MKTSEGMENT": "AUTOMOBILE",
  "C_NAME": "Tom Anderson",
  "C_NATIONKEY": 10,
  "C_PHONE": "506-8346550"
}'), 'dummy.json', 1003, current_timestamp, 'JSURESH';

DESC TASK DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.task_new_customer_by_month;  -- Note the state is "started"

-- Let's monitor the task, like all things in Snowflake we can monitor via the command line or the UI
-- Commands below will show in real time, UI has upto a 45 minute delay
SET task_id=(SELECT "id" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(ROOT_TASK_ID=>$task_id));  

-- stream is consumed
  select * from DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.customer_stream;

  select * from DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.new_customer_by_month;

-- Suspend the task
alter task DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.task_new_customer_by_month suspend;

show tasks;  -- Now the state should be in suspended