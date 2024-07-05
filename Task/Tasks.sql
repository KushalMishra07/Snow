/*--
Tasty Bytes is a fictitious, global food truck network, that is on a mission to serve unique food options with high
quality items in a safe, convenient and cost effective way. In order to drive forward on their mission, Tasty Bytes
is beginning to leverage the Snowflake Data Cloud.

Within this Worksheet, we will walk through the end to end process required to load a CSV file containing Menu specific data
that is currently hosted in Blob Storage.
--*/

-------------------------------------------------------------------------------------------
    -- Step 1: To start, let's set the Role and Warehouse context
        -- USE ROLE: https://docs.snowflake.com/en/sql-reference/sql/use-role
        -- USE WAREHOUSE: https://docs.snowflake.com/en/sql-reference/sql/use-warehouse
-------------------------------------------------------------------------------------------

/*-- 
    - To run a single query, place your cursor in the query editor and select the Run button (⌘-Return).
    - To run the entire worksheet, select 'Run All' from the dropdown next to the Run button (⌘-Shift-Return).
--*/

---> set the Role
USE ROLE accountadmin;

---> set the Warehouse
USE WAREHOUSE compute_wh;

-------------------------------------------------------------------------------------------
    -- Step 2: With context in place, let's now create a Database, Schema, and Table
        -- CREATE DATABASE: https://docs.snowflake.com/en/sql-reference/sql/create-database
        -- CREATE SCHEMA: https://docs.snowflake.com/en/sql-reference/sql/create-schema
        -- CREATE TABLE: https://docs.snowflake.com/en/sql-reference/sql/create-table
-------------------------------------------------------------------------------------------

---> create the Tasty Bytes Database
CREATE OR REPLACE DATABASE tasty_bytes_sample_data;

---> create the Raw POS (Point-of-Sale) Schema
CREATE OR REPLACE SCHEMA tasty_bytes_sample_data.raw_pos;

---> create the Raw Menu Table
CREATE OR REPLACE TABLE tasty_bytes_sample_data.raw_pos.menu
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
);

---> confirm the empty Menu table exists
SELECT * FROM tasty_bytes_sample_data.raw_pos.menu;


-------------------------------------------------------------------------------------------
    -- Step 3: To connect to the Blob Storage, let's create a Stage
        -- Creating an S3 Stage: https://docs.snowflake.com/en/user-guide/data-load-s3-create-stage
-------------------------------------------------------------------------------------------

---> create the Stage referencing the Blob location and CSV File Format
CREATE OR REPLACE STAGE tasty_bytes_sample_data.public.blob_stage
url = 's3://sfquickstarts/tastybytes/'
file_format = (type = csv);

---> query the Stage to find the Menu CSV file
LIST @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;


-------------------------------------------------------------------------------------------
    -- Step 4: Now let's Load the Menu CSV file from the Stage
        -- COPY INTO <table>: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
-------------------------------------------------------------------------------------------

---> copy the Menu file into the Menu table
COPY INTO tasty_bytes_sample_data.raw_pos.menu
FROM @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;


-------------------------------------------------------------------------------------------
    -- Step 5: Query the Menu table
        -- SELECT: https://docs.snowflake.com/en/sql-reference/sql/select
        -- TOP <n>: https://docs.snowflake.com/en/sql-reference/constructs/top_n
        -- FLATTEN: https://docs.snowflake.com/en/sql-reference/functions/flatten
-------------------------------------------------------------------------------------------

---> how many rows are in the table?
SELECT COUNT(*) AS row_count FROM tasty_bytes_sample_data.raw_pos.menu;

---> what do the top 10 rows look like?
SELECT TOP 10 * FROM tasty_bytes_sample_data.raw_pos.menu;

---> what menu items does the Freezing Point brand sell?
SELECT 
   menu_item_name
FROM tasty_bytes_sample_data.raw_pos.menu
WHERE truck_brand_name = 'Freezing Point';

---> what is the profit on Mango Sticky Rice?
SELECT 
   menu_item_name,
   (sale_price_usd - cost_of_goods_usd) AS profit_usd
FROM tasty_bytes_sample_data.raw_pos.menu
WHERE 1=1
AND truck_brand_name = 'Freezing Point'
AND menu_item_name = 'Mango Sticky Rice';

---> to finish, let's extract the Mango Sticky Rice ingredients from the semi-structured column
SELECT 
    m.menu_item_name,
    obj.value:"ingredients"::ARRAY AS ingredients
FROM tasty_bytes_sample_data.raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj
WHERE 1=1
AND truck_brand_name = 'Freezing Point'
AND menu_item_name = 'Mango Sticky Rice';


-----------------------------------------------------------------------------------------------------------
----Creating Target Table----------------------------------------------------------------------------------
CREATE TABLE target_table (
    id INT,
    name STRING,
    age INT
);


INSERT INTO target_table (id, name, age) VALUES 
(1, 'Alice', 30),
(2, 'Bob', 25);

CREATE TABLE source_table (
    id INT,
    name STRING,
    age INT
);
---------------------------------------------------------------------------------------------------------------
--------------------Create--Sequence ---------------------------------------------------------------
CREATE OR REPLACE SEQUENCE seq_01 START = 1 INCREMENT = 4;

INSERT INTO source_table (id, name, age) VALUES 
(seq_01.nextval, seq_01.nextval, seq_01.nextval),  
(seq_01.nextval, seq_01.nextval, seq_01.nextval);

------------------------------------------------------------------------------
---Procedure-------------------------------------------------------------


CREATE OR REPLACE PROCEDURE my_procedure_insert()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

INSERT INTO source_table (id, name, age) VALUES 
(seq_01.nextval, seq_01.nextval, seq_01.nextval),  
(seq_01.nextval, seq_01.nextval, seq_01.nextval);
END;
$$;

---------------Task------------------------------------------------------------------------------------------
CREATE OR REPLACE TASK my_insert_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON */1 * * * * America/Los_Angeles'
  COMMENT = 'Runs every 1 minutes in America/Los_Angeles time zone'
AS
  -- Your SQL query or stored procedure here
  CALL my_procedure_insert();

ALTER TASK my_insert_task Resume;  


----------Stream_-------------------------------------

CREATE STREAM target_table_stream3 ON TABLE target_table append_only = TRUE;

SELECT * FROM target_table_stream3;
SELECT * from target_table

CREATE OR REPLACE TABLE CLONING_STREAM AS sELECT * FROM  target_table_stream3;

Select * from CLONING_STREAM;



CREATE OR REPLACE PROCEDURE my_procedure_insert_onstream()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
MERGE INTO CLONING_STREAM AS t
USING target_table_stream3 AS s
ON t.id = s.id
WHEN NOT MATCHED THEN
    INSERT (id, name, age,METADATA$ACTION,METADATA$ISUPDATE,METADATA$ROW_ID) VALUES (s.id, s.name, s.age,s.METADATA$ACTION,s.METADATA$ISUPDATE,s.METADATA$ROW_ID);
    RETURN 'Procedure executed successfully';
END;
$$;

CREATE OR REPLACE TASK my_task_stream
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON */1 * * * * America/Los_Angeles'
  COMMENT = 'Runs every .5 minutes in America/Los_Angeles time zone'
AS
  -- Your SQL query or stored procedure here
  CALL my_procedure_insert_onstream();

  Alter task my_task_stream resume;

  Select * from CLONING_STREAM;





MERGE INTO target_table AS t
USING source_table_stream AS s
ON t.id = s.id
WHEN MATCHED THEN
    UPDATE SET t.name = s.name, t.age = s.age
WHEN NOT MATCHED THEN
    INSERT (id, name, age) VALUES (s.id, s.name, s.age);

CREATE OR REPLACE PROCEDURE my_procedure()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
MERGE INTO target_table AS t
USING source_table_stream AS s
ON t.id = s.id
WHEN MATCHED THEN
    UPDATE SET t.name = s.name, t.age = s.age
WHEN NOT MATCHED THEN
    INSERT (id, name, age) VALUES (s.id, s.name, s.age);
    RETURN 'Procedure executed successfully';
END;
$$;

CREATE OR REPLACE TASK my_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON */2 * * * * America/Los_Angeles'
  COMMENT = 'Runs every 2 minutes in America/Los_Angeles time zone'
AS
  -- Your SQL query or stored procedure here
  CALL my_procedure();

Alter Task my_task resume;


Alter task MY_TASK_STREAM SUSPEND;

CREATE OR REPLACE SEQUENCE seq_01 START = 1 INCREMENT = 4;




call SYSTEM$USER_TASK_CANCEL_ONGOING_EXECUTIONS(); 


  
