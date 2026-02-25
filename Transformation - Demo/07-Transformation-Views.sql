USE WAREHOUSE COMPUTE_WH;
SELECT CURRENT_WAREHOUSE();

USE ROLE SYSADMIN;
SELECT CURRENT_ROLE();
SHOW DATABASES;
USE DATABASE DEMO;

USE SCHEMA transformation_demo_curated_layer;

---------------------------------------------------------------------------------------------------------------------

-- Use three part name for cross zone access if required.
create or replace table demo.transformation_demo_curated_layer.customer_by_nation as
select c.CUSTOMER_RECORD:C_NAME:: STRING as cust_name, n.n_name country
from DEMO.FILE_INGESTION_DEMO.CUSTOMER c join DEMO.FILE_INGESTION_DEMO.NATION n on c.customer_record:C_NATIONKEY = n.n_nationkey
group by 1,2;
--order by 3 desc;

select * 
from customer_by_nation; 


SELECT * FROM DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.CUSTOMER_ORDER_TOTALS;

create or replace view demo.transformation_demo_curated_layer.customer_sales_order_view
as
SELECT c.cust_name as customer_name, country, order_count, order_total
FROM DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.CUSTOMER_ORDER_TOTALS dyn, customer_by_nation c
where c.cust_name = dyn.cust_name;



SELECT * FROM demo.transformation_demo_curated_layer.customer_sales_order_view;




