-- DROP GOOD DATA TABLES
DROP TABLE good_stg_reported_crime CASCADE CONSTRAINTS;
DROP TABLE good_stg_location CASCADE CONSTRAINTS;
DROP TABLE good_stg_crime_type CASCADE CONSTRAINTS;
DROP TABLE good_stg_status CASCADE CONSTRAINTS;
DROP TABLE good_stg_time CASCADE CONSTRAINTS;

-- DROP BAD DATA TABLES
DROP TABLE bad_stg_reported_crime CASCADE CONSTRAINTS;
DROP TABLE bad_stg_location CASCADE CONSTRAINTS;
DROP TABLE bad_stg_crime_type CASCADE CONSTRAINTS;
DROP TABLE bad_stg_status CASCADE CONSTRAINTS;
DROP TABLE bad_stg_time CASCADE CONSTRAINTS;

-- DROP ERROR LOG TABLE
DROP TABLE error_log CASCADE CONSTRAINTS;

-- DROP PROCESSING LOG TABLE
DROP TABLE process_log CASCADE CONSTRAINTS;

-- GOOD DATA TABLES
CREATE TABLE good_stg_reported_crime AS SELECT * FROM stg_reported_crime WHERE 1=0;
CREATE TABLE good_stg_location AS SELECT * FROM stg_location WHERE 1=0;
CREATE TABLE good_stg_crime_type AS SELECT * FROM stg_crime_type WHERE 1=0;
CREATE TABLE good_stg_status AS SELECT * FROM stg_status WHERE 1=0;
CREATE TABLE good_stg_time AS SELECT * FROM stg_time WHERE 1=0;

-- BAD DATA TABLES
CREATE TABLE bad_stg_reported_crime AS SELECT * FROM stg_reported_crime WHERE 1=0;
CREATE TABLE bad_stg_location AS SELECT * FROM stg_location WHERE 1=0;
CREATE TABLE bad_stg_crime_type AS SELECT * FROM stg_crime_type WHERE 1=0;
CREATE TABLE bad_stg_status AS SELECT * FROM stg_status WHERE 1=0;
CREATE TABLE bad_stg_time AS SELECT * FROM stg_time WHERE 1=0;

-- ERROR LOG
-- Purpose: Stores all invalid/bad data records captured 
-- during ETL processing with details on what went wrong.
CREATE TABLE error_log (
    error_id      NUMBER GENERATED ALWAYS AS IDENTITY,  -- Unique ID for each error entry
    table_name    VARCHAR2(100),                        -- Name of table where the error occurred
    record_id     VARCHAR2(200),                        -- Identifier of the specific bad record
    error_type    VARCHAR2(200),                        -- Category/type of error (NULL value, invalid format, etc.)
    error_detail  VARCHAR2(4000),                       -- Detailed explanation of the error
    error_date    DATE DEFAULT SYSDATE                  -- Timestamp when error was logged
);

-- PROCESS LOG
-- Purpose: Tracks each ETL run, recording total, good,
-- and bad rows processed along with start and end time.
CREATE TABLE process_log (
    log_id        NUMBER GENERATED ALWAYS AS IDENTITY,  -- Unique ID for each ETL execution
    process_name  VARCHAR2(200),                        -- Name of the ETL process/procedure
    table_name    VARCHAR2(100),                        -- Table being loaded during the ETL run
    total_rows    NUMBER,                               -- Number of rows read from the source
    good_rows     NUMBER,                               -- Number of valid rows successfully loaded
    bad_rows      NUMBER,                               -- Number of invalid rows recorded in error_log
    start_time    DATE,                                 -- Timestamp when ETL process started
    end_time      DATE                                  -- Timestamp when ETL process finished
);