CREATE DATABASE Test;
Create or replace file format customer_csv_ff
  type = 'csv'
  compression = 'none'
  field_delimiter = ','
  skip_header = 1;


Select current_version(),current_region();

Create table TEST.TEST1.custom_csv(
Id NUMBER PRIMARY KEY,
MSSubClass NUMBER,
MSZoning VARCHAR,
LotFrontage VARCHAR,
LotArea VARCHAR,
Street VARCHAR,
Alley VARCHAR,
LotShape VARCHAR,
LandContour VARCHAR,
Utilities VARCHAR
)

list @~;
Select * from TEST.TEST1.custom_csv;

list @%custom_csv;
show stages;

CREATE stage "TEST"."TEST1"."stg01" comment = 'This is my demo internal stage';
Show stages like '%01%';

Drop table TEST.TEST1.DATA;

CREATE OR REPLACE TABLE TEST.TEST1.DATA (
title varchar,
author varchar,
date varchar, 
noviews number(38,0),
likes number(38,0),
link varchar
)

Select count(*) from TEST.TEST1.DATA;


CREATE or REPLACE File Format data_csv_ff 
type = 'csv'
compression = 'none'
field_delimiter = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '0x22'
skip_header= 1;

Select current_version(),current_region();

Select * from TEST.TEST1.DATA;

Create or Replace SEQUENCE Intsequence 
START with 1
Increment by 1;

CREATE or REPLACE TABLE Customer_table(
id number,
first_name varchar,
last_name varchar,
date_of_birth date,
active_flag boolean, 
city varchar, 
insert_time timestamp default current_timestamp()
)


Create or Replace task task_01
warehouse = compute_wh
schedule = '1 minute'
as 
insert into customer_table (id,first_name,last_name,date_of_birth,active_flag,city)
values (Intsequence.nextval,'F_name','L_name',current_date(),TRUE,'MY_City');

Show Tasks;

Grant execute task to role sysadmin;
Alter task task_01 resume;
Show tasks;

Select max(id),max(id)/60 from Customer_table order by id;

