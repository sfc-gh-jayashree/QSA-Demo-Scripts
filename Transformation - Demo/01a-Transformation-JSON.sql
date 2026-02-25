-- comment added
USE WAREHOUSE COMPUTE_WH;
SELECT CURRENT_WAREHOUSE();

USE ROLE SYSADMIN;
SELECT CURRENT_ROLE();
SHOW DATABASES;
USE DATABASE DEMO;

USE SCHEMA FILE_INGESTION_DEMO;

--DROP SCHEMA TRANSFORMATION_DEMO_CURATED_LAYER;
CREATE SCHEMA TRANSFORMATION_DEMO_CURATED_LAYER;

USE SCHEMA TRANSFORMATION_DEMO_CURATED_LAYER;

---------------------------------------------------------------------------------------------------------------------
--Clean up scripts

--DROP TABLE DEMO.FILE_INGESTION_DEMO.sales_data;
--DROP TABLE DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.SALES_HEADER;
--DROP TABLE DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.SALES_ITEMS;

---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE sales_data_staging (
  sale_id INT,
  sale_json_raw VARCHAR
);

INSERT INTO sales_data_staging (sale_id, sale_json_raw)
VALUES
(1, '{ "order_id": 1001, "customer": { "id": "cust_A", "name": "Alice" }, "items": [ { "item_id": "item_X", "price": 25.00 }, { "item_id": "item_Y", "price": 10.50 } ], "timestamp": "2025-01-15T10:00:00Z", "status": "Shipped" }'),
(2, '{ "order_id": 1002, "customer": { "id": "cust_B", "name": "Bob" }, "items": [ { "item_id": "item_Z", "price": 50.00 } ], "timestamp": "2025-01-15T11:30:00Z", "status": "Pending" }'),
(3, '{ "order_id": 1003, "customer": { "id": "cust_C", "name": "Charlie" }, "items": [ { "item_id": "item_X", "price": 25.00 }, { "item_id": "item_A", "price": 5.00 } ], "timestamp": "2025-01-16T09:15:00Z", "status": "Shipped" }'),
(4, '{ "order_id": 1004, "customer": { "id": "cust_D", "name": "Diana" }, "items": [ { "item_id": "item_B", "price": 15.00 } ], "timestamp": "2025-01-16T14:45:00Z", "status": "Pending" }'),
(5, '{ "order_id": 1005, "customer": { "id": "cust_E", "name": "Eve" }, "items": [ { "item_id": "item_Y", "price": 10.50 }, { "item_id": "item_B", "price": 15.00 } ], "timestamp": "2025-01-17T08:00:00Z", "status": "Shipped" }'),
(6, '{ "order_id": 1006, "customer": { "id": "cust_A", "name": "Alice" }, "items": [ { "item_id": "item_X", "price": 25.00 } ], "timestamp": "2025-01-17T12:00:00Z", "status": "Shipped" }'),
(7, '{ "order_id": 1007, "customer": { "id": "cust_F", "name": "Frank" }, "items": [ { "item_id": "item_C", "price": 30.00 } ], "timestamp": "2025-01-18T10:30:00Z", "status": "Pending" }'),
(8, '{ "order_id": 1008, "customer": { "id": "cust_G", "name": "Grace" }, "items": [ { "item_id": "item_D", "price": 45.00 } ], "timestamp": "2025-01-18T16:00:00Z", "status": "Shipped" }'),
(9, '{ "order_id": 1009, "customer": { "id": "cust_H", "name": "Heidi" }, "items": [ { "item_id": "item_Y", "price": 10.50 } ], "timestamp": "2025-01-19T09:00:00Z", "status": "Shipped" }'),
(10, '{ "order_id": 1010, "customer": { "id": "cust_B", "name": "Bob" }, "items": [ { "item_id": "item_X", "price": 25.00 }, { "item_id": "item_Z", "price": 50.00 } ], "timestamp": "2025-01-19T11:00:00Z", "status": "Shipped" }');

SELECT * FROM DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.sales_data_staging;

--DROP TABLE DEMO.FILE_INGESTION_DEMO.sales_data;

CREATE OR REPLACE TABLE DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.sales_data (
  sale_id INT,
  sale_json VARIANT
);

INSERT INTO  DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.sales_data (sale_id, sale_json)
SELECT
  sale_id,
  PARSE_JSON(sale_json_raw)
FROM DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.sales_data_staging;


select * from DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.sales_data;

SELECT
  sale_id,
  sale_json:order_id::INT AS order_id,
  sale_json:customer.name::VARCHAR AS customer_name,
  sale_json:timestamp::TIMESTAMP AS sale_timestamp,
  sale_json:status::VARCHAR AS order_status,
  sale_json:items AS items
FROM DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.sales_data;


SELECT
  sale_id,
  sale_json:order_id::INT AS order_id,
  sale_json:customer.name::VARCHAR AS customer_name,
  sale_json:timestamp::TIMESTAMP AS sale_timestamp,
  sale_json:status::VARCHAR AS order_status,
  sale_json:items[0]::VARCHAR AS first_item,
  sale_json:items[0]:item_id::VARCHAR AS first_item_id,
FROM DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.sales_data;

select * from DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.sales_data;

SELECT
  s.sale_id,
  s.sale_json:customer.name::VARCHAR AS customer_name,
  f.value:item_id::VARCHAR AS item_id,
  f.value:price::FLOAT AS item_price
FROM DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.sales_data s,
LATERAL FLATTEN(INPUT => s.sale_json:items) f;



CREATE OR REPLACE TABLE DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.SALES_HEADER (
  sale_id INT PRIMARY KEY,
  order_id INT,
  customer_id VARCHAR,
  customer_name VARCHAR,
  sale_timestamp TIMESTAMP,
  order_status VARCHAR
);

CREATE OR REPLACE TABLE DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.SALES_ITEMS (
  sale_id INT,
  item_id VARCHAR,
  item_price FLOAT,
  FOREIGN KEY (sale_id) REFERENCES sales_header(sale_id)
);

 INSERT INTO DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.SALES_HEADER (sale_id, order_id, customer_id, customer_name, sale_timestamp, order_status)
  SELECT
    sale_id,
    sale_json:order_id::INT,
    sale_json:customer.id::VARCHAR,
    sale_json:customer.name::VARCHAR,
    sale_json:timestamp::TIMESTAMP,
    sale_json:status::VARCHAR
  FROM DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.SALES_DATA;

  INSERT INTO DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.SALES_ITEMS (sale_id, item_id, item_price)
  SELECT
    s.sale_id,
    f.value:item_id::VARCHAR,
    f.value:price::FLOAT
  FROM DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.SALES_DATA s,
  LATERAL FLATTEN(INPUT => s.sale_json:items) f;

  select * from DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.SALES_HEADER;
    select * from DEMO.TRANSFORMATION_DEMO_CURATED_LAYER.SALES_ITEMS;