------------------------------------------------------------
-- PACKAGE SPEC
------------------------------------------------------------
CREATE OR REPLACE PACKAGE PKG_STAGING_LOAD AS

    -- Individual loaders
    PROCEDURE PROC_LOAD_STG_REPORTED_CRIME;
    PROCEDURE PROC_LOAD_STG_LOCATION;
    PROCEDURE PROC_LOAD_STG_CRIME_TYPE;
    PROCEDURE PROC_LOAD_STG_STATUS;
    PROCEDURE PROC_LOAD_STG_TIME;

    -- Master orchestrator
    PROCEDURE RUN_ALL_STAGING_LOADS;

END PKG_STAGING_LOAD;
/


-- Procedure to load reported crime data into staging table

CREATE OR REPLACE PACKAGE BODY PKG_STAGING_LOAD AS

PROCEDURE PROC_LOAD_STG_REPORTED_CRIME AS
    ----------------------------------------------------------------
    -- Cursor: PRCS reported crime data
    ----------------------------------------------------------------
    CURSOR cur_prcs IS
        SELECT
            reported_crime_id,
            date_reported,
            crime_postcode,
            crime_status,
            date_closed,
            fk1_crime_type_id,
            fk2_station_id,
            NULL AS officer_id
        FROM pl_reported_crime;

    ----------------------------------------------------------------
    -- Cursor: PS_WALES reported crime data
    ----------------------------------------------------------------
    CURSOR cur_wales IS
        SELECT
            crime_id,
            reported_date,
            NULL AS crime_postcode,
            crime_status,
            closed_date,
            NULL AS crime_type_id,
            location_id,
            police_id
        FROM crime_register;

BEGIN
    ----------------------------------------------------------------
    -- Load PRCS data into staging table
    ----------------------------------------------------------------
    FOR r IN cur_prcs LOOP
        MERGE INTO stg_reported_crime dst
        USING (
            SELECT
                r.reported_crime_id AS reported_crime_id_original,
                TO_CHAR(r.date_reported) AS date_reported_raw,
                r.crime_postcode AS crime_postcode_raw,
                r.crime_status AS crime_status_raw,
                TO_CHAR(r.date_closed) AS date_closed_raw,
                r.fk1_crime_type_id AS crime_type_id_original,
                r.fk2_station_id AS station_id_original,
                r.officer_id AS officer_id_original,
                'PRCS' AS source_system
            FROM dual
        ) src
        ON (dst.reported_crime_id_original = src.reported_crime_id_original
            AND dst.source_system = src.source_system)
        WHEN NOT MATCHED THEN
            INSERT (
                reported_crime_id_original,
                date_reported_raw,
                crime_postcode_raw,
                crime_status_raw,
                date_closed_raw,
                crime_type_id_original,
                station_id_original,
                officer_id_original,
                source_system
            )
            VALUES (
                src.reported_crime_id_original,
                src.date_reported_raw,
                src.crime_postcode_raw,
                src.crime_status_raw,
                src.date_closed_raw,
                src.crime_type_id_original,
                src.station_id_original,
                src.officer_id_original,
                src.source_system
            );
    END LOOP;

    ----------------------------------------------------------------
    -- Load PS_WALES data into staging table
    ----------------------------------------------------------------
    FOR w IN cur_wales LOOP
        MERGE INTO stg_reported_crime dst
        USING (
            SELECT
                w.crime_id AS reported_crime_id_original,
                TO_CHAR(w.reported_date) AS date_reported_raw,
                w.crime_postcode AS crime_postcode_raw,
                w.crime_status AS crime_status_raw,
                TO_CHAR(w.closed_date) AS date_closed_raw,
                w.crime_type_id AS crime_type_id_original,
                w.location_id AS station_id_original,
                w.police_id AS officer_id_original,
                'PS_WALES' AS source_system
            FROM dual
        ) src
        ON (dst.reported_crime_id_original = src.reported_crime_id_original
            AND dst.source_system = src.source_system)
        WHEN NOT MATCHED THEN
            INSERT (
                reported_crime_id_original,
                date_reported_raw,
                crime_postcode_raw,
                crime_status_raw,
                date_closed_raw,
                crime_type_id_original,
                station_id_original,
                officer_id_original,
                source_system
            )
            VALUES (
                src.reported_crime_id_original,
                src.date_reported_raw,
                src.crime_postcode_raw,
                src.crime_status_raw,
                src.date_closed_raw,
                src.crime_type_id_original,
                src.station_id_original,
                src.officer_id_original,
                src.source_system
            );
    END LOOP;
END PROC_LOAD_STG_REPORTED_CRIME;


-- Procedure to load location/station data
PROCEDURE PROC_LOAD_STG_LOCATION AS
    ----------------------------------------------------------------
    -- Cursor: PRCS location data
    ----------------------------------------------------------------
    CURSOR cur_prcs IS
        SELECT 
            s.station_id AS station_id_original,
            s.station_name AS station_name_raw,
            a.area_id AS area_id_original,
            a.area_name AS area_name_raw,
            NULL AS region_id_raw,
            NULL AS region_name_raw
        FROM pl_station s
        JOIN pl_area a ON s.fk1_area_id = a.area_id;

    ----------------------------------------------------------------
    -- Cursor: PS_WALES location data
    ----------------------------------------------------------------
    CURSOR cur_wales IS
        SELECT
            l.location_id AS station_id_original,
            l.street_name AS station_name_raw,
            r.region_id AS area_id_original,
            r.region_name AS area_name_raw,
            r.region_id AS region_id_raw,
            r.region_name AS region_name_raw
        FROM location l
        JOIN region r ON l.region_id = r.region_id;

BEGIN
    ----------------------------------------------------------------
    -- Load PRCS locations
    ----------------------------------------------------------------
    FOR r IN cur_prcs LOOP
        MERGE INTO stg_location dst
        USING (
            SELECT
                r.station_id_original AS station_id_original,
                r.station_name_raw AS station_name_raw,
                r.area_id_original AS area_id_original,
                r.area_name_raw AS area_name_raw,
                r.region_id_raw AS region_id_raw,
                r.region_name_raw AS region_name_raw,
                'PRCS' AS source_system
            FROM dual
        ) src
        ON (dst.station_id_original = src.station_id_original
            AND dst.source_system = src.source_system)
        WHEN NOT MATCHED THEN
            INSERT (
                station_id_original,
                station_name_raw,
                area_id_original,
                area_name_raw,
                region_id_raw,
                region_name_raw,
                source_system
            )
            VALUES (
                src.station_id_original,
                src.station_name_raw,
                src.area_id_original,
                src.area_name_raw,
                src.region_id_raw,
                src.region_name_raw,
                src.source_system
            );
    END LOOP;

    ----------------------------------------------------------------
    -- Load PS_WALES locations
    ----------------------------------------------------------------
    FOR w IN cur_wales LOOP
        MERGE INTO stg_location dst
        USING (
            SELECT
                w.station_id_original AS station_id_original,
                w.station_name_raw AS station_name_raw,
                w.area_id_original AS area_id_original,
                w.area_name_raw AS area_name_raw,
                w.region_id_raw AS region_id_raw,
                w.region_name_raw AS region_name_raw,
                'PS_WALES' AS source_system
            FROM dual
        ) src
        ON (dst.station_id_original = src.station_id_original
            AND dst.source_system = src.source_system)
        WHEN NOT MATCHED THEN
            INSERT (
                station_id_original,
                station_name_raw,
                area_id_original,
                area_name_raw,
                region_id_raw,
                region_name_raw,
                source_system
            )
            VALUES (
                src.station_id_original,
                src.station_name_raw,
                src.area_id_original,
                src.area_name_raw,
                src.region_id_raw,
                src.region_name_raw,
                src.source_system
            );
    END LOOP;
END PROC_LOAD_STG_LOCATION;

--------------------------------------------------------------------
-- 3. Procedure to load crime type data
--------------------------------------------------------------------
PROCEDURE PROC_LOAD_STG_CRIME_TYPE AS
    ----------------------------------------------------------------
    -- Cursor: PRCS crime types
    ----------------------------------------------------------------
    CURSOR cur_prcs IS
        SELECT crime_type_id, crime_type_desc
        FROM pl_crime_type;
    ----------------------------------------------------------------
    -- Cursor: PS_WALES crime types
    ----------------------------------------------------------------
    CURSOR cur_wales IS
        SELECT DISTINCT crime_type
        FROM crime_register
	WHERE crime_type IS NOT NULL
	AND TRIM(crime_type)<> '';

BEGIN
    ----------------------------------------------------------------
    -- Load PRCS crime types
    ----------------------------------------------------------------
    FOR r IN cur_prcs LOOP
        MERGE INTO stg_crime_type dst
        USING (
            SELECT
                r.crime_type_id AS crime_type_id_original,
                r.crime_type_desc AS crime_type_desc_raw,
                'PRCS' AS source_system
            FROM dual
        ) src
        ON (dst.crime_type_id_original = src.crime_type_id_original
            AND dst.source_system = src.source_system)
        WHEN NOT MATCHED THEN
            INSERT (
                crime_type_id_original,
                crime_type_desc_raw,
                source_system
            )
            VALUES (
                src.crime_type_id_original,
                src.crime_type_desc_raw,
                src.source_system
            );
    END LOOP;

    ----------------------------------------------------------------
    -- Load PS_WALES crime types
    ----------------------------------------------------------------
    FOR w IN cur_wales LOOP
        MERGE INTO stg_crime_type dst
        USING (
            SELECT
                NULL AS crime_type_id_original,
                w.crime_type AS crime_type_desc_raw,
                'PS_WALES' AS source_system
            FROM dual
        ) src
        ON (dst.crime_type_desc_raw = src.crime_type_desc_raw
            AND dst.source_system = src.source_system)
        WHEN NOT MATCHED THEN
            INSERT (
                crime_type_id_original,
                crime_type_desc_raw,
                source_system
            )
            VALUES (
                src.crime_type_id_original,
                src.crime_type_desc_raw,
                src.source_system
            );
    END LOOP;
END PROC_LOAD_STG_CRIME_TYPE;

-------------------------------------------------------------------
-- Procedure to load crime status into staging table
--------------------------------------------------------------------
PROCEDURE PROC_LOAD_STG_STATUS AS
    ----------------------------------------------------------------
    -- Load distinct PRCS crime statuses
    ----------------------------------------------------------------
BEGIN
    MERGE INTO stg_status dst
    USING (
        SELECT DISTINCT crime_status AS status_raw, 'PRCS' AS source_system
        FROM pl_reported_crime
    ) src
    ON (dst.status_raw = src.status_raw AND dst.source_system = src.source_system)
    WHEN NOT MATCHED THEN
        INSERT (status_raw, source_system)
        VALUES (src.status_raw, src.source_system);

    ----------------------------------------------------------------
    -- Load distinct PS_WALES crime statuses
    ----------------------------------------------------------------
    MERGE INTO stg_status dst
    USING (
        SELECT DISTINCT crime_status AS status_raw, 'PS_WALES' AS source_system
        FROM crime_register
    ) src
    ON (dst.status_raw = src.status_raw AND dst.source_system = src.source_system)
    WHEN NOT MATCHED THEN
        INSERT (status_raw, source_system)
        VALUES (src.status_raw, src.source_system);
END PROC_LOAD_STG_STATUS;


--------------------------------------------------------------------
-- Procedure to load time data into staging table
--------------------------------------------------------------------
PROCEDURE PROC_LOAD_STG_TIME AS
    ----------------------------------------------------------------
    -- Cursor: PRCS reported dates
    ----------------------------------------------------------------
    CURSOR cur_dates_prcs IS SELECT DISTINCT date_reported FROM pl_reported_crime;
    ----------------------------------------------------------------
    -- Cursor: PS_WALES reported dates
    ----------------------------------------------------------------
    CURSOR cur_dates_wales IS SELECT DISTINCT reported_date FROM crime_register;

    ----------------------------------------------------------------
    -- Variable to generate sequential time IDs
    ----------------------------------------------------------------
    id_raw NUMBER := 1;
BEGIN
    ----------------------------------------------------------------
    -- Load PRCS dates into staging table
    ----------------------------------------------------------------
    FOR r IN cur_dates_prcs LOOP
        MERGE INTO stg_time dst
        USING (
            SELECT id_raw AS time_id_raw, TO_CHAR(r.date_reported) AS date_full_raw, 'PRCS' AS source_system
            FROM dual
        ) src
        ON (dst.date_full_raw = src.date_full_raw AND dst.source_system = src.source_system)
        WHEN NOT MATCHED THEN
            INSERT (time_id_raw, date_full_raw, source_system)
            VALUES (src.time_id_raw, src.date_full_raw, src.source_system);
        id_raw := id_raw + 1;
    END LOOP;

    ----------------------------------------------------------------
    -- Load PS_WALES dates into staging table
    ----------------------------------------------------------------
    FOR w IN cur_dates_wales LOOP
        MERGE INTO stg_time dst
        USING (
            SELECT id_raw AS time_id_raw, TO_CHAR(w.reported_date) AS date_full_raw, 'PS_WALES' AS source_system
            FROM dual
        ) src
        ON (dst.date_full_raw = src.date_full_raw AND dst.source_system = src.source_system)
        WHEN NOT MATCHED THEN
            INSERT (time_id_raw, date_full_raw, source_system)
            VALUES (src.time_id_raw, src.date_full_raw, src.source_system);
        id_raw := id_raw + 1;
    END LOOP;
END PROC_LOAD_STG_TIME;

PROCEDURE RUN_ALL_STAGING_LOADS AS
BEGIN
    PROC_LOAD_STG_REPORTED_CRIME;
    PROC_LOAD_STG_LOCATION;
    PROC_LOAD_STG_CRIME_TYPE;
    PROC_LOAD_STG_STATUS;
    PROC_LOAD_STG_TIME;
END RUN_ALL_STAGING_LOADS;

END PKG_STAGING_LOAD;
/

BEGIN
    PKG_STAGING_LOAD.RUN_ALL_STAGING_LOADS;
END;
/
