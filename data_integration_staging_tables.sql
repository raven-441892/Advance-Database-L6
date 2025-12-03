---------------------------------------------------------------
-- DROP SECTION
-- Dropping existing staging tables and sequences if they already exist.
-- This ensures that the schema is clean before creating new tables,
-- sequences, and triggers for staging data.
---------------------------------------------------------------

-- Dropping existing staging tables
DROP TABLE stg_reported_crime CASCADE CONSTRAINTS;
DROP TABLE stg_location CASCADE CONSTRAINTS;
DROP TABLE stg_crime_type CASCADE CONSTRAINTS;
DROP TABLE stg_status CASCADE CONSTRAINTS;
DROP TABLE stg_time CASCADE CONSTRAINTS;

-- Dropping existing sequences
DROP SEQUENCE seq_stg_reported_crime;
DROP SEQUENCE seq_stg_location;
DROP SEQUENCE seq_stg_crime_type;
DROP SEQUENCE seq_stg_status;
DROP SEQUENCE seq_stg_time;


---------------------------------------------------------------
-- 1. STG_REPORTED_CRIME
---------------------------------------------------------------

-- Create sequence
CREATE SEQUENCE seq_stg_reported_crime
START WITH 1000
INCREMENT BY 1
NOCACHE
NOCYCLE;

-- Create table
CREATE TABLE stg_reported_crime (
    stg_reported_crime_id INTEGER,
    reported_crime_id_original INTEGER,
    date_reported_raw VARCHAR2(20),
    crime_postcode_raw VARCHAR2(20),
    crime_status_raw VARCHAR2(20),
    date_closed_raw VARCHAR2(20),
    crime_type_id_original INTEGER,
    station_id_original INTEGER,
    officer_id_original INTEGER,
    source_system VARCHAR2(20),
    CONSTRAINT pk_stg_reported_crime_id PRIMARY KEY (stg_reported_crime_id)
);

-- Create trigger
CREATE OR REPLACE TRIGGER trg_stg_reported_crime
BEFORE INSERT ON stg_reported_crime
FOR EACH ROW
BEGIN
    IF :NEW.stg_reported_crime_id IS NULL THEN
        SELECT seq_stg_reported_crime.NEXTVAL
        INTO :NEW.stg_reported_crime_id
        FROM dual;
    END IF;
END;
/

---------------------------------------------------------------
-- 2. STG_LOCATION
---------------------------------------------------------------

CREATE SEQUENCE seq_stg_location 
START WITH 1000
INCREMENT BY 1 
NOCACHE 
NOCYCLE;

CREATE TABLE stg_location (
    stg_location_id INTEGER,
    station_id_original INTEGER,
    station_name_raw VARCHAR2(100),
    area_id_original INTEGER,
    area_name_raw VARCHAR2(100),
    region_id_raw INTEGER,
    region_name_raw VARCHAR2(100),
    source_system VARCHAR2(20),
    CONSTRAINT pk_stg_location_id PRIMARY KEY (stg_location_id)
);

CREATE OR REPLACE TRIGGER trg_stg_location
BEFORE INSERT ON stg_location
FOR EACH ROW
BEGIN
    IF :NEW.stg_location_id IS NULL THEN
        SELECT seq_stg_location.NEXTVAL
        INTO :NEW.stg_location_id
        FROM dual;
    END IF;
END;
/

---------------------------------------------------------------
-- 3. STG_CRIME_TYPE
---------------------------------------------------------------

CREATE SEQUENCE seq_stg_crime_type
START WITH 1000
INCREMENT BY 1 
NOCACHE 
NOCYCLE;

CREATE TABLE stg_crime_type (
    stg_crime_type_id INTEGER,
    crime_type_id_original INTEGER,
    crime_type_desc_raw VARCHAR2(200),
    source_system VARCHAR2(20),
    CONSTRAINT pk_stg_crime_type_id PRIMARY KEY (stg_crime_type_id)
);

CREATE OR REPLACE TRIGGER trg_stg_crime_type
BEFORE INSERT ON stg_crime_type
FOR EACH ROW
BEGIN
    IF :NEW.stg_crime_type_id IS NULL THEN
        SELECT seq_stg_crime_type.NEXTVAL
        INTO :NEW.stg_crime_type_id
        FROM dual;
    END IF;
END;
/

---------------------------------------------------------------
-- 4. STG_STATUS
---------------------------------------------------------------

CREATE SEQUENCE seq_stg_status 
START WITH 1000
INCREMENT BY 1 
NOCACHE 
NOCYCLE;

CREATE TABLE stg_status (
    stg_status_id INTEGER,
    status_raw VARCHAR2(20),
    source_system VARCHAR2(20),
    CONSTRAINT pk_stg_status_id PRIMARY KEY (stg_status_id)
);

CREATE OR REPLACE TRIGGER trg_stg_status
BEFORE INSERT ON stg_status
FOR EACH ROW
BEGIN
    IF :NEW.stg_status_id IS NULL THEN
        SELECT seq_stg_status.NEXTVAL
        INTO :NEW.stg_status_id
        FROM dual;
    END IF;
END;
/

---------------------------------------------------------------
-- 6. STG_TIME
---------------------------------------------------------------

CREATE SEQUENCE seq_stg_time
START WITH 1000
INCREMENT BY 1 
NOCACHE 
NOCYCLE;

CREATE TABLE stg_time (
    stg_time_id INTEGER,
    time_id_raw INTEGER,
    date_full_raw VARCHAR2(20),
    source_system VARCHAR2(20),
    CONSTRAINT pk_stg_time_id PRIMARY KEY (stg_time_id)
);

CREATE OR REPLACE TRIGGER trg_stg_time
BEFORE INSERT ON stg_time
FOR EACH ROW
BEGIN
    IF :NEW.stg_time_id IS NULL THEN
        SELECT seq_stg_time.NEXTVAL
        INTO :NEW.stg_time_id
        FROM dual;
    END IF;
END;
/
