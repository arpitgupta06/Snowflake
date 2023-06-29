-- In this file we mix up

USE ROLE SYSADMIN;

-- create a developement database for datascience work
CREATE OR REPLACE DATABASE frostbyte_tasty_dev;

-- create raw, transformed and analytics schemas
-- raw zone for data integration
CREATE OR REPLACE SCHEMA frostbyte_tasty_dev.raw;

-- transformed zone for data processing
CREATE OR REPLACE SCHEMA frostbyte_tasty_dev.transform;

-- analytics zone for developement
CREATE OR REPLACE SCHEMA frostbyte_tasty_dev.analytics;

-- create csv file format for external staging
CREATE OR REPLACE FILE FORMAT frostbyte_tasty_dev.raw.csv_ff
type = 'csv';

-- create an external stage pointing to s3
CREATE OR REPLACE STAGE frostbyte_tasty_dev.raw.s3load
COMMENT = 's3 stage connection'
URL = 's3://sfquickstarts/frostbyte_tastybytes/'
FILE_FORMAT = frostbyte_tasty_dev.raw.csv_ff;

-- define shift sales table
CREATE OR REPLACE TABLE frostbyte_tasty_dev.raw.shift_sales(
    location_id NUMBER (19,0),
    city VARCHAR (16777216),
    date  DATE,
    shift_sales FLOAT,
    shift VARCHAR(2),
    month NUMBER(2,0),
    day_of_week NUMBER(2,0),
    city_population NUMBER(38,0)
);

-- create and use a compute warehouse
CREATE OR REPLACE WAREHOUSE tasty_dsci_wh AUTO_SUSPEND = 120;
USE WAREHOUSE tasty_dsci_wh;

-- ingest from s3 into shift sales table
COPY INTO frostbyte_tasty_dev.raw.shift_sales
FROM @frostbyte_tasty_dev.raw.s3load/analytics/shift_sales/;

-- join in SafeGraph data
CREATE OR REPLACE TABLE frostbyte_tasty_dev.transform.shift_sales
AS
    SELECT
        a.location_id,
        a.city,
        a.date,
        a.shift_sales,
        a.shift,
        a.month,
        a.day_of_week,
        a.city_population,
        b.latitude,
        b.longitude
    FROM frostbyte_tasty_dev.raw.shift_sales a
    JOIN FROSTBYTE_SAFEGRAPH.PUBLIC.FROSTBYTE_TB_SAFEGRAPH_S b
    ON a.location_id = b.location_id;

-- promote the transformed table to the analytics layer for data science development
CREATE OR REPLACE VIEW frostbyte_tasty_dev.analytics.shift_sales_v
AS
    SELECT * FROM frostbyte_tasty_dev.transform.shift_sales;

-- view shift sales data
SELECT * FROM frostbyte_tasty_dev.analytics.shift_sales_v;