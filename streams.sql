/*
 Streams record Data Manipulation Language (DML) changes made to tables, including Inserts, Updates and Deletes
 as well as maintain matadata for each changes.
 Snowflake table stream creates a change table that shows what has changed, at a row level,
 between two transactional points in time.

-- Streams don't contain table data. They only store offsets.
-- Stream logically takes an initial snapshot of all rows of source table by initializing a point in time (called an offset).
-- a stream can return the CDC (Change Data Capture) records by leveraging the versioning history.
 */

USE ROLE SYSADMIN;
USE WAREHOUSE PRACTICE_TEST1;
USE DATABASE DEMO_DB;
CREATE OR REPLACE SCHEMA BANKING_NEW;

CREATE OR REPLACE TABLE BRANCH(
    ID VARCHAR,
    CITY VARCHAR,
    AMOUNT NUMBER(20,2));

INSERT INTO BRANCH (ID, CITY, AMOUNT) VALUES (
                                              12001, 'MUMBAI', 5698.33
                                             ),
                                          (
                                           12002, 'DELHI', 8978.33
                                          ),
                                          (
                                           12003, 'JAIPUR', 5412.87
                                        );

SELECT * FROM BRANCH;

/*
 We’ll create two streams and then use the SHOW STREAMS command to see the created
streams:
 */
CREATE OR REPLACE STREAM STREAM_a ON TABLE BRANCH;
CREATE OR REPLACE STREAM STREAM_b ON TABLE BRANCH;
SHOW STREAMS;

/*
 If you run SELECT * statements on these streams, you will see that both are empty.
 */
SELECT * FROM STREAM_a;
SELECT * FROM STREAM_b;

/*
 What happens if we insert some new records into the table?
 */
INSERT INTO BRANCH(ID, CITY, AMOUNT) VALUES (
                                             12004, 'BANGLORE', 7889
                                            ),
                                         (12005, 'GURGAON', 8995.36),
                                         (12006, 'PUNE', 10000.36);

SELECT * FROM STREAM_a;
SELECT * FROM STREAM_b;
SELECT * FROM BRANCH;

/*
 Now, if we add another stream, STREAM_C, we will expect that there
will be no records in that stream because Stream C is built on updated table Branch
 */
CREATE OR REPLACE STREAM STREAM_c ON TABLE BRANCH;
SELECT * FROM STREAM_c;

// Let's insert more records
INSERT INTO BRANCH(ID, CITY, AMOUNT) VALUES (
                                             12007, 'HYDERABAD', 9887
                                            ),
                                         (
                                          12008, 'SURAT', 6554.64
                                         );

// Both Stream_a and Stream_b have 5 records but Stream_c has 2
SELECT * FROM STREAM_a;
SELECT * FROM STREAM_b;
SELECT * FROM STREAM_c;

/*
 Let’s re-create STREAM_b to see what happens in the next.
 At this point, STREAM_b will have zero records.
 */
CREATE OR REPLACE STREAM STREAM_b ON TABLE BRANCH;
SELECT * FROM STREAM_b;

// Let's see the impact of deleting the records
DELETE FROM BRANCH WHERE ID = 12004;
DELETE FROM BRANCH WHERE ID = 12008;
DELETE FROM BRANCH WHERE ID = 12001;

/*
 Let's understand the concept with Stream_a
 After STREAM_a was created, three records were entered: 12004, 12005, and 12006.
 Then two more records were entered: 12007 and 12008.
 When records 12004 and 12008 were deleted, they were removed from STREAM_A. When record 12001 was
deleted, this showed up as a new entry in STREAM_A because record 12001 didn’t previously exist.
 */
SELECT * FROM STREAM_a; // Have 3 records of inserts, 1 deletes
SELECT * FROM STREAM_c; // Have 1 records of inserts, 2 deletes
SELECT * FROM STREAM_b; // Have 3 records of delete

// Let's update the record in the table see what we get in METADATA$ACTION
UPDATE BRANCH
SET CITY = 'DELHI NCR' WHERE ID = 12005;
SELECT * FROM BRANCH;

SELECT * FROM STREAM_a; // Record 12005 already existed in STREAM_a and, no new entry was needed.

/*
 Record 12005 did not previously exist in STREAM_b and STREAM_c. Therefore, we see that there is
an entry for the deletion of record 12005 with the city of GURGAON and then an entry for
the insertion of the new 12005 record with the value of DELHI NCR. You can see
that those two new entries show as having a value of TRUE in the METADATA$ISUPDATE column.
With the addition of those two new entries, STREAM_b now has five records
 */
SELECT * FROM STREAM_b;
SELECT * FROM STREAM_c;

/*
 To summarize the metadata, let’s review the stream metadata columns. The METADATA$ACTION column tells us
 whether the row was inserted or deleted. If the row is updated, the METADATA$ISUPDATE column will be TRUE.
 And lastly, there is a unique hash key for the METADATA$ROW_ID column.
 */