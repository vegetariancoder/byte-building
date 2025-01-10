-- create database

create database if not exists TELCO_PROJECT;

-- create schema for staging tables
create schema if not exists telco_staging_tables;

-- create schema (for stages)
create schema if not exists telco_external_stages;

-- create schema (for file formats)
create schema if not exists telco_file_formats;

-- create schema (for integrations)
create schema if not exists telco_integrations;

-- create table

CREATE TABLE if not exists TELCO_PROJECT.telco_staging_tables.customer (
                          customer_id VARCHAR PRIMARY KEY, -- Primary Key
                          gender VARCHAR,
                          age INT,
                          under_30 VARCHAR,
                          senior_citizen VARCHAR,
                          partner VARCHAR,
                          dependents VARCHAR,
                          number_of_dependents INT,
                          married VARCHAR
);


CREATE TABLE if not exists TELCO_PROJECT.telco_staging_tables.location (
                          customer_id VARCHAR PRIMARY KEY REFERENCES TELCO_PROJECT.telco_staging_tables.customer(customer_id), -- Foreign Key to customer
                          country VARCHAR,
                          state VARCHAR,
                          city VARCHAR,
                          zip_code INT,
                          total_population INT,
                          latitude FLOAT,
                          longitude FLOAT
);


CREATE TABLE if not exists TELCO_PROJECT.telco_staging_tables.online_service (
                                customer_id VARCHAR PRIMARY KEY REFERENCES TELCO_PROJECT.telco_staging_tables.customer(customer_id), -- Foreign Key to customer
                                phone_service VARCHAR,
                                internet_service VARCHAR,
                                online_security VARCHAR,
                                online_backup VARCHAR,
                                device_protection VARCHAR,
                                premium_tech_support VARCHAR,
                                streaming_tv VARCHAR,
                                streaming_movies VARCHAR,
                                streaming_music VARCHAR,
                                internet_type VARCHAR
);


CREATE TABLE if not exists TELCO_PROJECT.telco_staging_tables.payment_info (
                              customer_id VARCHAR PRIMARY KEY REFERENCES TELCO_PROJECT.telco_staging_tables.customer(customer_id), -- Foreign Key to customer
                              contract VARCHAR,
                              paperless_billing VARCHAR,
                              payment_method VARCHAR,
                              monthly_charges FLOAT, -- Corrected column name from the sample data
                              avg_monthly_long_distance_charges FLOAT,
                              total_charges FLOAT,
                              total_refunds FLOAT,
                              total_extra_data_charges FLOAT,
                              total_long_distance_charges FLOAT,
                              total_revenue FLOAT
);


CREATE TABLE if not exists TELCO_PROJECT.telco_staging_tables.service_option (
                                customer_id VARCHAR PRIMARY KEY REFERENCES TELCO_PROJECT.telco_staging_tables.customer(customer_id), -- Foreign Key to customer
                                tenure INT,
                                internet_service VARCHAR,
                                phone_service VARCHAR,
                                multiple_lines VARCHAR,
                                avg_monthly_gb_download FLOAT,
                                unlimited_data VARCHAR,
                                offer VARCHAR,
                                referred_a_friend VARCHAR,
                                number_of_referrals INT
);


CREATE TABLE if not exists TELCO_PROJECT.telco_staging_tables.status_analysis (
                                 customer_id VARCHAR PRIMARY KEY REFERENCES TELCO_PROJECT.telco_staging_tables.customer(customer_id), -- Foreign Key to customer
                                 satisfaction_score INT,
                                 cltv FLOAT,
                                 customer_status VARCHAR,
                                 churn_score INT,
                                 churn_label VARCHAR,
                                 churn_value INT,
                                 churn_category VARCHAR,
                                 churn_reason VARCHAR
);

-- create file format

create or replace file format TELCO_PROJECT.TELCO_FILE_FORMATS.telco_ff_csv
type = 'CSV'
field_delimiter = ','
skip_header = 1
field_optionally_enclosed_by ='"';

-- create integration



-- create external stage



list @TELCO_PROJECT.telco_external_stages.telco_azure_stage_csv;

-- insert into queries
copy into TELCO_PROJECT.telco_staging_tables.CUSTOMER
from @TELCO_PROJECT.telco_external_stages.telco_azure_stage_csv files = ('Customer_Info.csv');

copy into TELCO_PROJECT.telco_staging_tables.LOCATION
from @TELCO_PROJECT.telco_external_stages.telco_azure_stage_csv files = ('Location_Data.csv');

copy into TELCO_PROJECT.telco_staging_tables.online_service
from @TELCO_PROJECT.telco_external_stages.telco_azure_stage_csv files = ('Online_Services.csv');

copy into TELCO_PROJECT.telco_staging_tables.payment_info
from @TELCO_PROJECT.telco_external_stages.telco_azure_stage_csv files = ('Payment_Info.csv');

copy into TELCO_PROJECT.telco_staging_tables.service_option
from @TELCO_PROJECT.telco_external_stages.telco_azure_stage_csv files = ('Service_Options.csv');

copy into TELCO_PROJECT.telco_staging_tables.status_analysis
from @TELCO_PROJECT.telco_external_stages.telco_azure_stage_csv files = ('Status_Analysis.csv');




