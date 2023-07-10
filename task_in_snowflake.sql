/*
 Taks are scheduler scripts that are run inside Snowflake environment. In simple words, a task is a scheduler that
 can help you to schedule a single sql or a stored procedure.
A task can be very useful when combined with Streams to make an end-to-end data pipeline.

 -- Snowflake ensures only one instance of a task with a schedule is executed at a given time.
 -- If the task is still sunning when the next scheduled execution time occurs then that scheduled time is skipped.
 -- Tasks have a max duration of 60 minutes by default.
 -- Tasks run on a schedule which is defined at the time a task is created. Alternatively, you can establish
 task dependencies whereby a task can be triggered by a predecessor task.
 */

// We won't practice on task tree and stored procedure here.

USE ROLE SYSADMIN;
USE WAREHOUSE PRACTICE_TEST1;
USE DATABASE DEMO_DB;
USE SCHEMA BANKING;

// Example 1
-- Let's create a table called customer_dim --
CREATE OR REPLACE TABLE CUSTOMER_DIM(
    ID NUMBER,
    FIRST_NAME VARCHAR,
    LAST_NAME VARCHAR,
    DATE_OF_BIRTH DATE,
    ACTIVE_FLAG BOOLEAN,
    CITY VARCHAR,
    INSERT_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                              )
COMMENT = 'CUSTOMER DIMENSION TABLE';
;

SELECT * FROM CUSTOMER_DIM;

-- Let's create a sequence object --
// Sequences are used to generate unique numbers across sessions and statements, including concurrent statements.
// They can be used to generate values for a primary key or any column that requires a unique value.
CREATE OR REPLACE SEQUENCE CUSTOMER_SEQ
    START 2
    INCREMENT 2
    COMMENT = 'DEMO SEQUENCE FOR CUSTOMER DIMENSION TABLE';

// Sequences may be accessed in queries as expressions of the form seq_name.NEXTVAL.
SELECT CUSTOMER_SEQ.NEXTVAL;

// CREATE statements for each task type, that tasks is scheduled
CREATE OR REPLACE TASK TASK01
    WAREHOUSE = 'PRACTICE_TEST1'
    SCHEDULE = '1 minute'
    AS
        INSERT INTO CUSTOMER_DIM(ID, FIRST_NAME, LAST_NAME, DATE_OF_BIRTH, ACTIVE_FLAG, CITY)
        VALUES (CUSTOMER_SEQ.nextval, 'F-NAME', 'L-NAME', CURRENT_DATE(), TRUE, 'MY-CITY');

-- Assign to any role for execute task --
// This role can alter the task to resume or suspend
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT TO ROLE SYSADMIN;

-- If you want to check the status of any created Task --
DESCRIBE TASK TASK01;
SHOW TASKS;


// Example 2
USE ROLE SYSADMIN;
USE WAREHOUSE PRACTICE_TEST1;
USE DATABASE DEMO_DB;
USE SCHEMA BANKING;

CREATE OR REPLACE TABLE DEMO_DB.BANKING.PRODUCT
(
    PROD_ID INT,
    PROD_DESC VARCHAR(),
    CATEGORY VARCHAR(30),
    SEGMENT VARCHAR(20),
    MFG_ID INT,
    MFG_NAME VARCHAR(50)
);

INSERT INTO DEMO_DB.BANKING.PRODUCT VALUES
                                        (1201, 'PROD 1201', 'CATE 1201', 'SEG 1201', 1201, 'MFG 1201');
INSERT INTO DEMO_DB.BANKING.PRODUCT VALUES
                                        (1202, 'PROD 1202', 'CATE 1202', 'SEG 1202', 1201, 'MFG 1202');
INSERT INTO DEMO_DB.BANKING.PRODUCT VALUES
                                        (1203, 'PROD 1203', 'CATE 1203', 'SEG 1203', 1203, 'MFG 1203');
INSERT INTO DEMO_DB.BANKING.PRODUCT VALUES
                                        (1204, 'PROD 1204', 'CATE 1204', 'SEG 1204', 1204, 'MFG 1204');
INSERT INTO DEMO_DB.BANKING.PRODUCT VALUES
                                        (1205, 'PROD 1205', 'CATE 1205', 'SEG 1205', 1205, 'MFG 1205');
INSERT INTO DEMO_DB.BANKING.PRODUCT VALUES
                                        (1206, 'PROD 1206', 'CATE 1206', 'SEG 1206', 1206, 'MFG 1206');

// Next, we’ll create a sales table and then a stream on the sales table:
CREATE OR REPLACE TABLE PRODUCT_SALES(
    PROD_ID INT,
    CUSTOMER VARCHAR(),
    ZIP VARCHAR(),
    QTY INT,
    REVENUE DECIMAL(10, 2)
);

// We’ll want our table stream to record insert-only values on the sales table:
CREATE OR REPLACE STREAM DEMO_DB.BANKING.PRODUCT_SALES_STREAM
ON TABLE DEMO_DB.BANKING.PRODUCT_SALES
APPEND_ONLY = TRUE;

// let's insert the values in Sales table:
INSERT INTO PRODUCT_SALES VALUES (
                                  1201, 'CUST 1201', 123456, 45, 2345.25);
INSERT INTO PRODUCT_SALES VALUES (
                                  1202, 'CUST 1202', 123457, 4, 45.78);
INSERT INTO PRODUCT_SALES VALUES (
                                  1203, 'CUST 1203', 133457, 5, 478.00);
INSERT INTO PRODUCT_SALES VALUES (
                                  1204, 'CUST 1204', 133400, 89, 11478.70);
INSERT INTO PRODUCT_SALES VALUES (
                                  1204, 'CUST 1204', 133400, 89, 11478.70);
INSERT INTO PRODUCT_SALES VALUES (
                                  1205, 'CUST 1205', 189799, 9, 1899.70);

// we’ll be able to see that the values we entered into the sales table were then captured in the SALES_STREAM:
SELECT * FROM DEMO_DB.BANKING.PRODUCT_SALES_STREAM;

/*
 it’s time to create our sales transaction table which combines the
SALES_STREAM with the product table and adds a timestamp so that we can know
when the sales data was recorded:
 */

CREATE OR REPLACE TABLE DEMO_DB.BANKING.PRODUCT_SALES_TRANSACT
(
    PROD_ID INT,
    PROD_DESC VARCHAR(),
    CATEGORY VARCHAR(30),
    SEGMENT VARCHAR(20),
    MFG_ID INT,
    MFG_NAME VARCHAR(50),
    CUSTOMER VARCHAR(),
    ZIP VARCHAR(),
    QTY INT,
    REVENUE DECIMAL(10, 2),
    TS TIMESTAMP
);

/*
 We’ll eventually want to automate the creation of the sales transaction table, but first
we need to see what happens when we manually insert data. If things work as
expected, we’ll proceed to creating a task for the insertion:
 */

INSERT INTO
    DEMO_DB.BANKING.PRODUCT_SALES_TRANSACT(PROD_ID, PROD_DESC, CATEGORY, SEGMENT, MFG_ID,
                                           MFG_NAME, CUSTOMER, ZIP, QTY, REVENUE, TS)
SELECT
    S.PROD_ID,
    P.PROD_DESC,
    P.CATEGORY,
    P.SEGMENT,
    P.MFG_ID,
    P.MFG_NAME,
    S.CUSTOMER,
    S.ZIP,
    S.QTY,
    S.REVENUE,
    CURRENT_TIMESTAMP
FROM PRODUCT_SALES_STREAM S
JOIN PRODUCT P ON S.PROD_ID = P.PROD_ID;

SELECT * FROM PRODUCT_SALES_TRANSACT;

// Now it’s time to automate the insertion by creating a task:
CREATE OR REPLACE TASK DEMO_DB.BANKING.PRODUCT_SALES_TASK
WAREHOUSE = PRACTICE_TEST1
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('DEMO_DB.BANKING.PRODUCT_SALES_STREAM')
AS
    INSERT INTO
        DEMO_DB.BANKING.PRODUCT_SALES_TRANSACT(PROD_ID, PROD_DESC, CATEGORY, SEGMENT,
                                               MFG_ID, MFG_NAME, CUSTOMER, ZIP, QTY, REVENUE, TS)
SELECT
    S.PROD_ID,
    P.PROD_DESC,
    P.CATEGORY,
    P.SEGMENT,
    P.MFG_ID,
    P.MFG_NAME,
    S.CUSTOMER,
    S.ZIP,
    S.QTY,
    S.REVENUE,
    CURRENT_TIMESTAMP
FROM PRODUCT_SALES_STREAM S
JOIN PRODUCT P ON S.PROD_ID = P.PROD_ID;

DESC TASK DEMO_DB.BANKING.PRODUCT_SALES_TASK;
ALTER TASK DEMO_DB.BANKING.PRODUCT_SALES_TASK RESUME;

/*
 Now let’s see what happens when we insert values into the sales table. Once we insert
values into the sales table, the SALES_STREAM should reflect the newly inserted
records. Then the task should insert the new sales records after joining with the product
table and generating a timestamp.
 */

INSERT INTO DEMO_DB.BANKING.PRODUCT_SALES VALUES (
                                                   1201, 'CUST 1201A', 123456, 45, 2345.25);
INSERT INTO PRODUCT_SALES VALUES (
                                  1202, 'CUST 1202A', 123457, 4, 45.78);
INSERT INTO PRODUCT_SALES VALUES (
                                  1203, 'CUST 1203A', 133457, 5, 478.00);
INSERT INTO PRODUCT_SALES VALUES (
                                  1204, 'CUST 1204A', 133400, 89, 11478.70);
INSERT INTO PRODUCT_SALES VALUES (
                                  1204, 'CUST 1204A', 133400, 89, 11478.70);
INSERT INTO PRODUCT_SALES VALUES (
                                  1205, 'CUST 1205A', 189799, 9, 1899.70);

SELECT * FROM DEMO_DB.BANKING.PRODUCT_SALES_STREAM;

SELECT * FROM DEMO_DB.BANKING.PRODUCT_SALES_TRANSACT;

ALTER TASK DEMO_DB.BANKING.PRODUCT_SALES_TASK SUSPEND;
