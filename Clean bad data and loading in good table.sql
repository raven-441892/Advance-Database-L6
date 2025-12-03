CREATE OR REPLACE PACKAGE PKG_CLEAN_BAD_DATA AS
    -- Procedures to clean each BAD staging table and move cleaned data to GOOD tables
    PROCEDURE PROC_CLEAN_BAD_REPORTED_CRIME;
    PROCEDURE PROC_CLEAN_BAD_LOCATION;
    PROCEDURE PROC_CLEAN_BAD_CRIME_TYPE;
    PROCEDURE PROC_CLEAN_BAD_STATUS;
    PROCEDURE PROC_CLEAN_BAD_TIME;

    -- Procedure to run ALL cleaning procedures in one go
    PROCEDURE PROC_ALL_CLEANED;
END PKG_CLEAN_BAD_DATA;
/

CREATE OR REPLACE PACKAGE BODY PKG_CLEAN_BAD_DATA AS
--Procedure to clean bad data in reported crime and inserted into its good table
PROCEDURE PROC_CLEAN_BAD_REPORTED_CRIME AS
    v_total NUMBER := 0;          -- total rows processed
    v_good  NUMBER := 0;          -- total good rows inserted
    v_reported DATE;              -- cleaned reported date
    v_closed   DATE;              -- cleaned closed date
    v_status   VARCHAR2(20);      -- cleaned status
    v_postcode VARCHAR2(20);      -- cleaned postcode
BEGIN
    FOR r IN (SELECT * FROM bad_stg_reported_crime) LOOP
        v_total := v_total + 1;

        -- DATE REPORTED: Try multiple formats and only set to NULL if all fail
        IF r.date_reported_raw IS NOT NULL THEN
            BEGIN
                v_reported := TO_DATE(r.date_reported_raw, 'YYYY-MM-DD');
            EXCEPTION WHEN OTHERS THEN
                BEGIN
                    v_reported := TO_DATE(r.date_reported_raw, 'DD-MM-YYYY');
                EXCEPTION WHEN OTHERS THEN
                    BEGIN
                        v_reported := TO_DATE(r.date_reported_raw, 'MM/DD/YYYY');
                    EXCEPTION WHEN OTHERS THEN
                        v_reported := NULL;
                    END;
                END;
            END;
        ELSE
            v_reported := NULL;
        END IF;

        -- DATE CLOSED: Try multiple formats
        IF r.date_closed_raw IS NOT NULL THEN
            BEGIN
                v_closed := TO_DATE(r.date_closed_raw, 'YYYY-MM-DD');
            EXCEPTION WHEN OTHERS THEN
                BEGIN
                    v_closed := TO_DATE(r.date_closed_raw, 'DD-MM-YYYY');
                EXCEPTION WHEN OTHERS THEN
                    BEGIN
                        v_closed := TO_DATE(r.date_closed_raw, 'MM/DD/YYYY');
                    EXCEPTION WHEN OTHERS THEN
                        v_closed := NULL;
                    END;
                END;
            END;
        ELSE
            v_closed := NULL;
        END IF;

        -- STATUS STANDARDIZATION
        IF r.crime_status_raw IS NOT NULL THEN
            v_status := UPPER(TRIM(r.crime_status_raw));
            IF v_status IN ('OPEN','OPENED') THEN
                v_status := 'OPEN';
            ELSIF v_status IN ('CLOSED','CLOSE') THEN
                v_status := 'CLOSED';
            ELSIF v_status LIKE 'ESCALAT%' OR v_status='ESCALATE' THEN
                v_status := 'ESCALATED';
            ELSE
                v_status := NULL;
            END IF;
        ELSE
            v_status := NULL;
        END IF;

        -- POSTCODE
        IF r.crime_postcode_raw IS NOT NULL THEN
            v_postcode := UPPER(TRIM(r.crime_postcode_raw));
            IF LENGTH(v_postcode) < 4 THEN v_postcode := NULL; END IF;
        ELSE
            v_postcode := NULL;
        END IF;

        -- INSERT INTO GOOD TABLE
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
            CASE WHEN v_reported IS NULL THEN NULL ELSE TO_CHAR(v_reported,'YYYY-MM-DD') END,
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

    -- Delete from BAD table
    DELETE FROM bad_stg_reported_crime;

    -- Process log
    INSERT INTO process_log(process_name, table_name, total_rows, good_rows, bad_rows, start_time, end_time)
    VALUES('FIX_BAD_REPORTED_CRIME','STG_REPORTED_CRIME',v_total,v_good,0,SYSDATE,SYSDATE);

    COMMIT;
END PROC_CLEAN_BAD_REPORTED_CRIME;


PROCEDURE PROC_CLEAN_BAD_LOCATION AS
    v_total NUMBER := 0;
    v_good  NUMBER := 0;
    v_station VARCHAR2(200);
    v_area    VARCHAR2(200);
    v_region  VARCHAR2(200);
BEGIN
    FOR r IN (SELECT * FROM bad_stg_location) LOOP
        v_total := v_total + 1;

        v_station := CASE WHEN r.station_name_raw IS NOT NULL THEN INITCAP(TRIM(r.station_name_raw)) ELSE NULL END;
        v_area    := CASE WHEN r.area_name_raw IS NOT NULL THEN INITCAP(TRIM(r.area_name_raw)) ELSE NULL END;
        v_region  := CASE WHEN r.region_name_raw IS NOT NULL THEN INITCAP(TRIM(r.region_name_raw)) ELSE NULL END;

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

    DELETE FROM bad_stg_location;

    INSERT INTO process_log(process_name, table_name, total_rows, good_rows, bad_rows, start_time, end_time)
    VALUES('FIX_BAD_STG_LOCATION','STG_LOCATION',v_total,v_good,0,SYSDATE,SYSDATE);

    COMMIT;
END PROC_CLEAN_BAD_LOCATION;



PROCEDURE PROC_CLEAN_BAD_CRIME_TYPE AS
BEGIN
    INSERT INTO good_stg_crime_type(
        stg_crime_type_id, crime_type_id_original, crime_type_desc_raw, source_system)
    SELECT
        stg_crime_type_id,
        crime_type_id_original,
        CASE WHEN crime_type_desc_raw IS NOT NULL THEN INITCAP(crime_type_desc_raw) ELSE NULL END,
        source_system
    FROM bad_stg_crime_type;

    DELETE FROM bad_stg_crime_type;

    INSERT INTO process_log(process_name, table_name, total_rows, good_rows, bad_rows, start_time, end_time)
    VALUES('FIX_BAD_CRIME_TYPE','STG_CRIME_TYPE',
           (SELECT COUNT(*) FROM good_stg_crime_type),
           (SELECT COUNT(*) FROM good_stg_crime_type),
           0,
           SYSDATE,SYSDATE);

    COMMIT;
END PROC_CLEAN_BAD_CRIME_TYPE;



PROCEDURE PROC_CLEAN_BAD_STATUS AS
BEGIN
    INSERT INTO good_stg_status(
        stg_status_id, status_raw, source_system)
    SELECT
        stg_status_id,
        CASE WHEN status_raw IS NOT NULL THEN INITCAP(status_raw) ELSE NULL END,
        source_system
    FROM bad_stg_status;

    DELETE FROM bad_stg_status;

    INSERT INTO process_log(process_name, table_name, total_rows, good_rows, bad_rows, start_time, end_time)
    VALUES('FIX_BAD_STATUS','STG_STATUS',
           (SELECT COUNT(*) FROM good_stg_status),
           (SELECT COUNT(*) FROM good_stg_status),
           0,
           SYSDATE,SYSDATE);

    COMMIT;
END PROC_CLEAN_BAD_STATUS;


PROCEDURE PROC_CLEAN_BAD_TIME AS
    v_total NUMBER := 0;
    v_good  NUMBER := 0;
    v_date  DATE;
BEGIN
    FOR r IN (SELECT * FROM bad_stg_time) LOOP
        v_total := v_total + 1;

        IF r.date_full_raw IS NOT NULL THEN
            BEGIN
                v_date := TO_DATE(r.date_full_raw,'YYYY-MM-DD');
            EXCEPTION WHEN OTHERS THEN
                BEGIN
                    v_date := TO_DATE(r.date_full_raw,'DD-MM-YYYY');
                EXCEPTION WHEN OTHERS THEN
                    BEGIN
                        v_date := TO_DATE(r.date_full_raw,'MM/DD/YYYY');
                    EXCEPTION WHEN OTHERS THEN
                        v_date := NULL;
                    END;
                END;
            END;
        ELSE
            v_date := NULL;
        END IF;

        INSERT INTO good_stg_time(
            stg_time_id,
            time_id_raw,
            date_full_raw,
            source_system)
        VALUES(
            r.stg_time_id,
            r.time_id_raw,
            CASE WHEN v_date IS NULL THEN NULL ELSE TO_CHAR(v_date,'YYYY-MM-DD') END,
            r.source_system
        );

        v_good := v_good + 1;
    END LOOP;

    DELETE FROM bad_stg_time;

    INSERT INTO process_log(process_name, table_name, total_rows, good_rows, bad_rows, start_time, end_time)
    VALUES('FIX_BAD_TIME','STG_TIME',v_total,v_good,0,SYSDATE,SYSDATE);

    COMMIT;
END PROC_CLEAN_BAD_TIME;


PROCEDURE PROC_ALL_CLEANED AS
BEGIN

    PROC_CLEAN_BAD_REPORTED_CRIME;
    PROC_CLEAN_BAD_LOCATION;
    PROC_CLEAN_BAD_CRIME_TYPE;
    PROC_CLEAN_BAD_STATUS;
    PROC_CLEAN_BAD_TIME;

END PROC_ALL_CLEANED;
END PKG_CLEAN_BAD_DATA;
/

BEGIN
    PKG_CLEAN_BAD_DATA.PROC_ALL_CLEANED;
END;
/

