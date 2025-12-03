CREATE OR REPLACE PACKAGE PKG_GOOD_BAD_INSERTION AS

    -- Clear staging tables
    PROCEDURE CLEAR_STG_TABLES;

    -- Cleaning procedures
    PROCEDURE PROC_POLISH_STG_REPORTED_CRIME;
    PROCEDURE PROC_POLISH_STG_LOCATION;
    PROCEDURE PROC_POLISH_STG_CRIME_TYPE;
    PROCEDURE PROC_POLISH_STG_STATUS;
    PROCEDURE PROC_POLISH_STG_TIME;

    -- Main procedure to run all procedures
    PROCEDURE PROC_ALL_POLISHED;

END PKG_GOOD_BAD_INSERTION;
/

CREATE OR REPLACE PACKAGE BODY PKG_GOOD_BAD_INSERTION AS

--PROCEDURE 1: REMOVES ALL GOOD + BAD DATA FROM TABLES

PROCEDURE CLEAR_STG_TABLES AS
BEGIN
    -- Delete all data from the 'good' tables
    DELETE FROM good_stg_reported_crime;
    DELETE FROM good_stg_location;
    DELETE FROM good_stg_crime_type;
    DELETE FROM good_stg_status;
    DELETE FROM good_stg_time;

    -- Delete all data from the 'bad' tables
    DELETE FROM bad_stg_reported_crime;
    DELETE FROM bad_stg_location;
    DELETE FROM bad_stg_crime_type;
    DELETE FROM bad_stg_status;
    DELETE FROM bad_stg_time;

    --deletion inside pl/sql requires COMMIT for changes to be saved
    COMMIT;
END CLEAR_STG_TABLES;


--PROCEDURE 2: SEPERATES THE DATA FROM STG_REPORTED_CRIME INTO ITS GOOD AND BAD TABLE WITH ITS PROCESS AND ERROR LOG
PROCEDURE PROC_POLISH_STG_REPORTED_CRIME AS
    v_total NUMBER := 0;
    v_good  NUMBER := 0;
    v_bad   NUMBER := 0;

    v_reported DATE;
    v_closed   DATE;
    v_status   VARCHAR2(20);
    v_postcode VARCHAR2(20);
BEGIN
    FOR r IN (SELECT * FROM stg_reported_crime) LOOP
        v_total := v_total + 1;

        -- DATE REPORTED
        BEGIN
            v_reported := TO_DATE(r.date_reported_raw, 'YYYY-MM-DD');
        EXCEPTION WHEN OTHERS THEN
            BEGIN
                v_reported := TO_DATE(r.date_reported_raw, 'DD-MM-YYYY');
            EXCEPTION WHEN OTHERS THEN
                INSERT INTO bad_stg_reported_crime(
                    stg_reported_crime_id,
                    reported_crime_id_original,
                    date_reported_raw,
                    crime_postcode_raw,
                    crime_status_raw,
                    date_closed_raw,
                    crime_type_id_original,
                    station_id_original,
                    officer_id_original,
                    source_system)
                VALUES (
                    r.stg_reported_crime_id,
                    r.reported_crime_id_original,
                    r.date_reported_raw,
                    r.crime_postcode_raw,
                    r.crime_status_raw,
                    r.date_closed_raw,
                    r.crime_type_id_original,
                    r.station_id_original,
                    r.officer_id_original,
                    r.source_system
                );

                INSERT INTO error_log(table_name, record_id, error_type, error_detail)
                VALUES ('STG_REPORTED_CRIME', r.stg_reported_crime_id,
                        'INVALID_DATE_REPORTED',
                        'Unable to parse reported date: ' || r.date_reported_raw);

                v_bad := v_bad + 1;
                CONTINUE;
            END;
        END;

        -- CLOSED DATE
        v_closed := NULL;
        IF r.date_closed_raw IS NOT NULL THEN
            BEGIN
                v_closed := TO_DATE(r.date_closed_raw, 'YYYY-MM-DD');
            EXCEPTION WHEN OTHERS THEN
                BEGIN
                    v_closed := TO_DATE(r.date_closed_raw, 'DD-MM-YYYY');
                EXCEPTION WHEN OTHERS THEN
                    v_closed := NULL;
                    INSERT INTO error_log(table_name, record_id, error_type, error_detail)
                    VALUES ('STG_REPORTED_CRIME', r.stg_reported_crime_id,
                            'INVALID_DATE_CLOSED', 'Closed date set to NULL: ' || r.date_closed_raw);
                END;
            END;

            IF v_closed IS NOT NULL AND v_closed < v_reported THEN
                INSERT INTO error_log(table_name, record_id, error_type, error_detail)
                VALUES ('STG_REPORTED_CRIME', r.stg_reported_crime_id,
                        'CLOSED_BEFORE_REPORTED', 'Closed date of crime is earlier than reported; set to NULL');
                v_closed := NULL;
            END IF;
        END IF;

        -- STATUS STANDARDIZATION
        v_status := UPPER(TRIM(NVL(r.crime_status_raw, '')));
        IF v_status IN ('OPEN','OPENED') THEN
            v_status := 'OPEN';
        ELSIF v_status IN ('CLOSED','CLOSE') THEN
            v_status := 'CLOSED';
        ELSIF v_status LIKE 'ESCALAT%' OR v_status='ESCALATE' THEN
            v_status := 'ESCALATED';
        ELSE
            INSERT INTO bad_stg_reported_crime(
                stg_reported_crime_id,
                reported_crime_id_original,
                date_reported_raw,
                crime_postcode_raw,
                crime_status_raw,
                date_closed_raw,
                crime_type_id_original,
                station_id_original,
                officer_id_original,
                source_system)
            VALUES (
                r.stg_reported_crime_id,
                r.reported_crime_id_original,
                r.date_reported_raw,
                r.crime_postcode_raw,
                r.crime_status_raw,
                r.date_closed_raw,
                r.crime_type_id_original,
                r.station_id_original,
                r.officer_id_original,
                r.source_system
            );

            INSERT INTO error_log(table_name, record_id, error_type, error_detail)
            VALUES ('STG_REPORTED_CRIME', r.stg_reported_crime_id,
                    'INVALID_STATUS', r.crime_status_raw);

            v_bad := v_bad + 1;
            CONTINUE;
        END IF;

        -- POSTCODE VALIDATION
        v_postcode := UPPER(TRIM(NVL(r.crime_postcode_raw, '')));
        IF v_postcode IS NULL OR LENGTH(v_postcode) < 4 THEN
            INSERT INTO bad_stg_reported_crime(
                stg_reported_crime_id,
                reported_crime_id_original,
                date_reported_raw,
                crime_postcode_raw,
                crime_status_raw,
                date_closed_raw,
                crime_type_id_original,
                station_id_original,
                officer_id_original,
                source_system)
            VALUES (
                r.stg_reported_crime_id,
                r.reported_crime_id_original,
                r.date_reported_raw,
                r.crime_postcode_raw,
                r.crime_status_raw,
                r.date_closed_raw,
                r.crime_type_id_original,
                r.station_id_original,
                r.officer_id_original,
                r.source_system
            );

            INSERT INTO error_log(table_name, record_id, error_type, error_detail)
            VALUES ('STG_REPORTED_CRIME', r.stg_reported_crime_id,
                    'INVALID_POSTCODE', r.crime_postcode_raw);

            v_bad := v_bad + 1;
            CONTINUE;
        END IF;

        -- INSERT GOOD RECORD
        INSERT INTO good_stg_reported_crime(
            stg_reported_crime_id,
            reported_crime_id_original,
            date_reported_raw,
            crime_postcode_raw,
            crime_status_raw,
            date_closed_raw,
            crime_type_id_original,
            station_id_original,
            officer_id_original,
            source_system)
        VALUES (
            r.stg_reported_crime_id,
            r.reported_crime_id_original,
            TO_CHAR(v_reported,'YYYY-MM-DD'),
            v_postcode,
            v_status,
            CASE WHEN v_closed IS NULL THEN NULL ELSE TO_CHAR(v_closed,'YYYY-MM-DD') END,
            r.crime_type_id_original,
            r.station_id_original,
            r.officer_id_original,
            r.source_system
        );

        v_good := v_good + 1;
    END LOOP;

    INSERT INTO process_log(process_name, table_name, total_rows, good_rows, bad_rows, start_time, end_time)
    VALUES ('PROC_POLISH_STG_REPORTED_CRIME', 'STG_REPORTED_CRIME', v_total, v_good, v_bad, SYSDATE, SYSDATE);

    COMMIT;
END PROC_POLISH_STG_REPORTED_CRIME;

--PROCEDURE 3: LOCATION 
-- Cleans location-related data. Removes whitespace, formats to proper case,
-- checks station name validity, and separates GOOD/BAD records.

PROCEDURE PROC_POLISH_STG_LOCATION AS
    v_total NUMBER := 0;
    v_good  NUMBER := 0;
    v_bad   NUMBER := 0;

    v_station VARCHAR2(200);
    v_area    VARCHAR2(200);
    v_region  VARCHAR2(200);
BEGIN
    FOR r IN (SELECT * FROM stg_location) LOOP
        v_total := v_total + 1;

        v_station := INITCAP(TRIM(NVL(r.station_name_raw, '')));
        v_area    := INITCAP(TRIM(NVL(r.area_name_raw, '')));
        v_region  := INITCAP(TRIM(NVL(r.region_name_raw, '')));

        IF v_station IS NULL THEN
            INSERT INTO bad_stg_location(
                stg_location_id,
                station_id_original,
                station_name_raw,
                area_id_original,
                area_name_raw,
                region_id_raw,
                region_name_raw,
                source_system)
            VALUES (
                r.stg_location_id,
                r.station_id_original,
                r.station_name_raw,
                r.area_id_original,
                r.area_name_raw,
                r.region_id_raw,
                r.region_name_raw,
                r.source_system
            );

            INSERT INTO error_log(table_name, record_id, error_type, error_detail)
            VALUES ('STG_LOCATION', r.stg_location_id,
                    'INVALID_STATION_NAME', r.station_name_raw);

            v_bad := v_bad + 1;
            CONTINUE;
        END IF;

        INSERT INTO good_stg_location(
            stg_location_id,
            station_id_original,
            station_name_raw,
            area_id_original,
            area_name_raw,
            region_id_raw,
            region_name_raw,
            source_system)
        VALUES (
            r.stg_location_id,
            r.station_id_original,
            v_station,
            r.area_id_original,
            v_area,
            r.region_id_raw,
            v_region,
            r.source_system
        );

        v_good := v_good + 1;
    END LOOP;

    INSERT INTO process_log(process_name, table_name, total_rows, good_rows, bad_rows, start_time, end_time)
    VALUES ('PROC_POLISH_STG_LOCATION', 'STG_LOCATION',
            v_total, v_good, v_bad, SYSDATE, SYSDATE);

    COMMIT;
END PROC_POLISH_STG_LOCATION;

--PROCEDURE 4: CRIME TYPE
-- Inserts non-null crime types into GOOD table.
-- Null descriptions are treated as BAD.

PROCEDURE PROC_POLISH_STG_CRIME_TYPE AS
BEGIN
    INSERT INTO good_stg_crime_type(
        stg_crime_type_id, crime_type_id_original, crime_type_desc_raw, source_system)
    SELECT
        stg_crime_type_id,
        crime_type_id_original,
        INITCAP(crime_type_desc_raw),
        source_system
    FROM stg_crime_type
    WHERE crime_type_desc_raw IS NOT NULL;

    INSERT INTO bad_stg_crime_type(
        stg_crime_type_id, crime_type_id_original, crime_type_desc_raw, source_system)
    SELECT
        stg_crime_type_id,
        crime_type_id_original,
        crime_type_desc_raw,
        source_system
    FROM stg_crime_type
    WHERE crime_type_desc_raw IS NULL;

    INSERT INTO process_log(process_name, table_name, total_rows, good_rows, bad_rows, start_time, end_time)
    VALUES ('PROC_POLISH_STG_CRIME_TYPE', 'STG_CRIME_TYPE',
            (SELECT COUNT(*) FROM stg_crime_type),
            (SELECT COUNT(*) FROM good_stg_crime_type),
            (SELECT COUNT(*) FROM bad_stg_crime_type),
            SYSDATE, SYSDATE);

    COMMIT;
END PROC_POLISH_STG_CRIME_TYPE;

--PROCEDURE 5: STATUS
-- Standardizes statuses by applying INITCAP.
-- Null statuses are flagged as BAD.

PROCEDURE PROC_POLISH_STG_STATUS AS
BEGIN
    INSERT INTO good_stg_status(
        stg_status_id, status_raw, source_system)
    SELECT stg_status_id, INITCAP(status_raw), source_system
    FROM stg_status
    WHERE status_raw IS NOT NULL;

    INSERT INTO bad_stg_status(
        stg_status_id, status_raw, source_system)
    SELECT stg_status_id, status_raw, source_system
    FROM stg_status
    WHERE status_raw IS NULL;

    INSERT INTO process_log(process_name, table_name, total_rows, good_rows, bad_rows, start_time, end_time)
    VALUES ('PROC_POLISH_STG_STATUS', 'STG_STATUS',
            (SELECT COUNT(*) FROM stg_status),
            (SELECT COUNT(*) FROM good_stg_status),
            (SELECT COUNT(*) FROM bad_stg_status),
            SYSDATE, SYSDATE);

    COMMIT;
END PROC_POLISH_STG_STATUS;

--PROCEDURE 6: TIME
-- Attempts multiple date format conversions.
-- Moves bad dates to BAD table with error log.
PROCEDURE PROC_POLISH_STG_TIME AS
    v_total NUMBER := 0;
    v_good  NUMBER := 0;
    v_bad   NUMBER := 0;

    v_date DATE;
BEGIN
    FOR r IN (SELECT * FROM stg_time) LOOP
        v_total := v_total + 1;
        BEGIN
            v_date := NULL;
            BEGIN
                v_date := TO_DATE(r.date_full_raw, 'YYYY-MM-DD');
            EXCEPTION WHEN OTHERS THEN
                BEGIN
                    v_date := TO_DATE(r.date_full_raw, 'DD-MM-YYYY');
                EXCEPTION WHEN OTHERS THEN
                    BEGIN
                        v_date := TO_DATE(r.date_full_raw, 'MM/DD/YYYY');
                    EXCEPTION WHEN OTHERS THEN
                        INSERT INTO bad_stg_time(
                            stg_time_id, time_id_raw, date_full_raw, source_system)
                        VALUES (
                            r.stg_time_id,
                            r.time_id_raw,
                            r.date_full_raw,
                            r.source_system
                        );

                        INSERT INTO error_log(table_name, record_id, error_type, error_detail)
                        VALUES ('STG_TIME', r.stg_time_id,
                                'INVALID_DATE_FULL', r.date_full_raw);

                        v_bad := v_bad+1;
                        CONTINUE;
                    END;
                END;
            END;

        INSERT INTO good_stg_time(
            stg_time_id, time_id_raw, date_full_raw, source_system)
        VALUES (
            r.stg_time_id,
            r.time_id_raw,
            TO_CHAR(v_date, 'YYYY-MM-DD'),
            r.source_system
        );
        v_good := v_good+1;
        END;
    END LOOP;

    INSERT INTO process_log(process_name, table_name, total_rows, good_rows, bad_rows, start_time, end_time)
    VALUES ('PROC_POLISH_STG_TIME', 'STG_TIME', 
            v_total, v_good, v_bad,
            SYSDATE, SYSDATE);

    COMMIT;
END PROC_POLISH_STG_TIME;

-- MAIN PROCEDURE:
-- Runs full ETL: clears staging tables → then cleans all 5 raw tables.
PROCEDURE PROC_ALL_POLISHED AS
BEGIN

    CLEAR_STG_TABLES;
    PROC_POLISH_STG_REPORTED_CRIME;
    PROC_POLISH_STG_LOCATION;
    PROC_POLISH_STG_CRIME_TYPE;
    PROC_POLISH_STG_STATUS;
    PROC_POLISH_STG_TIME;

END PROC_ALL_POLISHED;

END PKG_GOOD_BAD_INSERTION;
/

BEGIN
    PKG_GOOD_BAD_INSERTION.PROC_ALL_POLISHED;
END;
/




