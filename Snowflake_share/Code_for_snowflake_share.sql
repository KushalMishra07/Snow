-- #Code on the data provide side
-- #For a share tp happen there are two things needed one is provider account and the other is reciever account
--provider account as the name suggest is the provider of the data, pays for the storage by default. 
-- reader of the data reads the data and pays for the compute if read big data

--code
--source --provider code

show tables;
Select * from acct_txn;

CREATE SHARE Test_questions;
GRANT USAGE ON DATABASE test_questions TO SHARE Test_questions;
GRANT USAGE ON SCHEMA test_questions.test TO SHARE Test_questions;
GRANT SELECT ON TABLE test_questions.test.acct_txn TO SHARE Test_questions;
ALTER SHARE Test_questions ADD ACCOUNTS = MNMPPZW.MW14334;

SELECT CURRENT_ORGANIZATION_NAME();
SELECT CURRENT_ACCOUNT_NAME();


--code for the reader 
--reader -- target

SELECT CURRENT_ORGANIZATION_NAME();
SELECT CURRENT_ACCOUNT_NAME();
CREATE DATABASE sales_from_provider FROM SHARE COFQAFY.AE82328.Test_questions;
SELECT * FROM sales_from_provider.test.TEST;

use database sales_from_provider;
show schemas;
use schema test;
show tables;

Select * from acct_txn;
