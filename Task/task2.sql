
CREATE DATABASE IF NOT EXISTS dag_demo;
CREATE SCHEMA IF NOT EXISTS dag_demo.etl;
USE SCHEMA dag_demo.etl;

-- Base (landing) table
CREATE OR REPLACE TABLE base_txn (
    txn_id        STRING,
    account_id    NUMBER,
    txn_type      STRING,
    amount        NUMBER(12,2),
    txn_ts        TIMESTAMP_NTZ,
    inserted_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Final table
CREATE OR REPLACE TABLE final_txn (
    txn_id        STRING,
    account_id    NUMBER,
    txn_type      STRING,
    amount        NUMBER(12,2),
    txn_ts        TIMESTAMP_NTZ,
    loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

Select * from daily_txn_summary;

-- Summary table (third task in the DAG)
CREATE OR REPLACE TABLE daily_txn_summary (
    txn_date      DATE,
    txn_type      STRING,
    txn_count     NUMBER,
    total_amount  NUMBER(18,2),
    refreshed_at  TIMESTAMP_NTZ
);

-- Stream on base table (insert-only, so it's cheaper and cleaner)
CREATE OR REPLACE STREAM base_txn_stream
    ON TABLE base_txn
    APPEND_ONLY = TRUE;

Select * from  base_txn limit 10; 
Select * from  base_txn_stream limit 10;   


Create or replace stream base_x
on table base_txn
append_only = True;




alter task root_generate_txns suspend;
-- ROOT: runs every minute, inserts 5 random rows
CREATE OR REPLACE TASK root_generate_txns
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = '1 MINUTE'
AS
INSERT INTO base_txn (txn_id, account_id, txn_type, amount, txn_ts)
SELECT
    UUID_STRING(),
    UNIFORM(1000, 1100, RANDOM()),
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'DEPOSIT'
        WHEN 2 THEN 'WITHDRAWAL'
        WHEN 3 THEN 'TRANSFER'
        ELSE 'PAYMENT'
    END,
    ROUND(UNIFORM(10, 50000, RANDOM()) / 100, 2),
    DATEADD('second', -UNIFORM(0, 59, RANDOM()), CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
FROM TABLE(GENERATOR(ROWCOUNT => 5));

Select ROUND(UNIFORM(10, 50000, RANDOM()) / 100, 2);
Select UNIFORM(10, 50000, RANDOM());
Select random();




-- CHILD 1: consumes the stream into the final table

ALTER TASK LOAD_FINAL_TXN suspend;

CREATE OR REPLACE TASK load_final_txn
    WAREHOUSE = COMPUTE_WH
    AFTER root_generate_txns
    WHEN SYSTEM$STREAM_HAS_DATA('BASE_TXN_STREAM')
AS
INSERT INTO final_txn (txn_id, account_id, txn_type, amount, txn_ts)
SELECT txn_id, account_id, txn_type, amount, txn_ts
FROM base_txn_stream;

Select * from final_txn;


alter task agg_daily_summary suspend;

Select * from daily_txn_summary;

-- CHILD 2: refreshes the summary after the final load
CREATE OR REPLACE TASK agg_daily_summary
    WAREHOUSE = COMPUTE_WH
    AFTER load_final_txn
AS
MERGE INTO daily_txn_summary t
USING (
    SELECT txn_ts::DATE AS txn_date,
           txn_type,
           COUNT(*)     AS txn_count,
           SUM(amount)  AS total_amount
    FROM final_txn
    GROUP BY 1, 2
) s
ON t.txn_date = s.txn_date AND t.txn_type = s.txn_type
WHEN MATCHED THEN UPDATE SET
    t.txn_count = s.txn_count,
    t.total_amount = s.total_amount,
    t.refreshed_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
    (txn_date, txn_type, txn_count, total_amount, refreshed_at)
VALUES
    (s.txn_date, s.txn_type, s.txn_count, s.total_amount, CURRENT_TIMESTAMP());
