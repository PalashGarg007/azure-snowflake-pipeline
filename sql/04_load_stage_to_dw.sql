-- ================================================================
-- 04_load_stage_to_dw.sql
-- Run as: CHINOOK_USER (via ADF Script Activity or Snowflake Worksheet)
-- Purpose: Load all DW tables from STAGE in correct dependency order
--
-- Execution order:
--   1. DATE_DIM      — no dependencies
--   2. TIME_DIM      — no dependencies
--   3. CUSTOMER_DIM  — no dependencies (source: STAGE.Customer)
--   4. ARTIST_DIM    — no dependencies (source: STAGE.Artist)
--   5. SALES_FACT    — needs all 4 above to exist first
-- ================================================================

-- ── CONFIGURATION — only edit this section ──────────────────
SET SOURCE_ID  = 'CHINOOK_AZ_SQL';  -- tag stored in every DW row
SET DATE_START = '2000-01-01';      -- first date in DATE_DIM
SET DATE_ROWS  = 36525;             -- 100 years of dates
SET TIME_ROWS  = 1440;              -- 1440 = per minute, 86400 = per second

-- ── SETUP ────────────────────────────────────────────────────
USE WAREHOUSE CHINOOK_WH;
USE DATABASE  chinookDB;

-- ════════════════════════════════════════════════════════════
-- 1. LOAD DATE_DIM
--    Uses Snowflake GENERATOR to create one row per day.
--    DATE_KEY format: YYYYMMDD integer (e.g. 20060115)
--    This format is used as the FK in SALES_FACT.
-- ════════════════════════════════════════════════════════════
INSERT INTO DW.DATE_DIM (
    DATE_KEY, FULL_DATE, DAY_NUM, WEEKDAY_ABBR, WEEKDAY_NUM,
    DAY_OF_YEAR_NUM, WEEK_OF_YEAR, MONTH_NUM, MONTH_ABBR,
    QUARTER_NUM, QUARTER_NAME, YEAR_NUM,
    FIRST_DAY_OF_MONTH, LAST_DAY_OF_MONTH, IS_WEEKEND
)
WITH DATE_SERIES AS (
    SELECT DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, $DATE_START::DATE) AS D
    FROM TABLE(GENERATOR(ROWCOUNT => $DATE_ROWS))
)
SELECT
    TO_NUMBER(TO_CHAR(D, 'YYYYMMDD'))       AS DATE_KEY,
    D                                        AS FULL_DATE,
    DAYOFMONTH(D)                            AS DAY_NUM,
    LEFT(DAYNAME(D), 3)                      AS WEEKDAY_ABBR,
    DAYOFWEEK(D)                             AS WEEKDAY_NUM,
    DAYOFYEAR(D)                             AS DAY_OF_YEAR_NUM,
    WEEKOFYEAR(D)                            AS WEEK_OF_YEAR,
    MONTH(D)                                 AS MONTH_NUM,
    LEFT(MONTHNAME(D), 3)                    AS MONTH_ABBR,
    QUARTER(D)                               AS QUARTER_NUM,
    'Q' || QUARTER(D)                        AS QUARTER_NAME,
    YEAR(D)                                  AS YEAR_NUM,
    DATE_TRUNC('MONTH', D)                   AS FIRST_DAY_OF_MONTH,
    LAST_DAY(D)                              AS LAST_DAY_OF_MONTH,
    CASE WHEN DAYOFWEEK(D) IN (0, 6)
         THEN 'Y' ELSE 'N' END               AS IS_WEEKEND
FROM DATE_SERIES;

-- ════════════════════════════════════════════════════════════
-- 2. LOAD TIME_DIM
--    One row per minute (1440 rows).
--    TIME_KEY = minutes since midnight (0 = 00:00, 1439 = 23:59)
-- ════════════════════════════════════════════════════════════
INSERT INTO DW.TIME_DIM (
    TIME_KEY, HOUR_NUM, MINUTE_NUM, TIME_24_HR
)
WITH TIME_SERIES AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS S
    FROM TABLE(GENERATOR(ROWCOUNT => $TIME_ROWS))
)
SELECT
    S                                                                AS TIME_KEY,
    FLOOR(S / 60)                                                    AS HOUR_NUM,
    MOD(S, 60)                                                       AS MINUTE_NUM,
    LPAD(FLOOR(S / 60), 2, '0') || ':' || LPAD(MOD(S, 60), 2, '0') AS TIME_24_HR
FROM TIME_SERIES;

-- ════════════════════════════════════════════════════════════
-- 3. LOAD CUSTOMER_DIM — SCD Type 2 + SHA-256 Change Detection
--
--    Step 1 (MERGE):
--      - WHEN NOT MATCHED: insert new customer, compute hash, EFF_START = today
--      - WHEN MATCHED AND hash differs: expire old record (IS_ACTIVE='N', EFF_END=today)
--
--    Step 2 (INSERT):
--      - Insert new active version for all records just expired in Step 1
--
--    ROW_HASH: SHA-256 of 7 columns concatenated with '|' separator.
--    COALESCE handles NULLs so the hash is consistent regardless of missing data.
-- ════════════════════════════════════════════════════════════

-- Step 1: Expire changed records / insert new records
MERGE INTO DW.CUSTOMER_DIM AS TGT
USING (
    SELECT
        CUSTOMERID, FIRSTNAME, LASTNAME, COMPANY,
        CITY, STATE, COUNTRY, POSTALCODE, SUPPORTREPID,
        SHA2(
            COALESCE(FIRSTNAME,  '') || '|' ||
            COALESCE(LASTNAME,   '') || '|' ||
            COALESCE(COMPANY,    '') || '|' ||
            COALESCE(CITY,       '') || '|' ||
            COALESCE(STATE,      '') || '|' ||
            COALESCE(COUNTRY,    '') || '|' ||
            COALESCE(POSTALCODE, ''),
            256
        ) AS COMPUTED_HASH
    FROM chinookDB.STAGE.Customer
) AS SRC
ON  TGT.CUSTOMER_ID = SRC.CUSTOMERID
AND TGT.IS_ACTIVE   = 'Y'

-- New customer: insert with hash and open EFF dates
WHEN NOT MATCHED THEN INSERT (
    CUSTOMER_ID, FIRST_NAME, LAST_NAME, COMPANY_NAME,
    CITY, STATE, COUNTRY, ZIP_CODE, EMPLOYEE_ID,
    ROW_HASH, IS_ACTIVE, EFF_START_DATE, EFF_END_DATE, SOURCE_ID
) VALUES (
    SRC.CUSTOMERID, SRC.FIRSTNAME, SRC.LASTNAME, SRC.COMPANY,
    SRC.CITY, SRC.STATE, SRC.COUNTRY, SRC.POSTALCODE, SRC.SUPPORTREPID,
    SRC.COMPUTED_HASH, 'Y', CURRENT_DATE(), NULL, $SOURCE_ID
)

-- Changed customer: close the old version
WHEN MATCHED AND TGT.ROW_HASH <> SRC.COMPUTED_HASH THEN UPDATE SET
    TGT.IS_ACTIVE    = 'N',
    TGT.EFF_END_DATE = CURRENT_DATE();

-- Step 2: Insert new active version for records that were just expired
INSERT INTO DW.CUSTOMER_DIM (
    CUSTOMER_ID, FIRST_NAME, LAST_NAME, COMPANY_NAME,
    CITY, STATE, COUNTRY, ZIP_CODE, EMPLOYEE_ID,
    ROW_HASH, IS_ACTIVE, EFF_START_DATE, EFF_END_DATE, SOURCE_ID
)
SELECT
    S.CUSTOMERID, S.FIRSTNAME, S.LASTNAME, S.COMPANY,
    S.CITY, S.STATE, S.COUNTRY, S.POSTALCODE, S.SUPPORTREPID,
    SHA2(
        COALESCE(S.FIRSTNAME,  '') || '|' ||
        COALESCE(S.LASTNAME,   '') || '|' ||
        COALESCE(S.COMPANY,    '') || '|' ||
        COALESCE(S.CITY,       '') || '|' ||
        COALESCE(S.STATE,      '') || '|' ||
        COALESCE(S.COUNTRY,    '') || '|' ||
        COALESCE(S.POSTALCODE, ''),
        256
    ),
    'Y', CURRENT_DATE(), NULL, $SOURCE_ID
FROM chinookDB.STAGE.Customer S
JOIN chinookDB.DW.CUSTOMER_DIM D
    ON  S.CUSTOMERID   = D.CUSTOMER_ID
    AND D.IS_ACTIVE    = 'N'
    AND D.EFF_END_DATE = CURRENT_DATE();

-- ════════════════════════════════════════════════════════════
-- 4. LOAD ARTIST_DIM — SCD Type 2
--
--    Simpler than CUSTOMER_DIM — only one tracked attribute (ARTIST_NAME)
--    so a direct column comparison is used instead of a hash.
-- ════════════════════════════════════════════════════════════

-- Step 1: Insert new artists / expire changed artists
MERGE INTO DW.ARTIST_DIM AS TGT
USING chinookDB.STAGE.Artist AS SRC
ON  TGT.ARTIST_ID  = SRC.ARTISTID
AND TGT.IS_CURRENT = 'Y'

-- New artist
WHEN NOT MATCHED THEN INSERT (
    ARTIST_ID, ARTIST_NAME,
    IS_CURRENT, EFF_START_DATE, EFF_END_DATE, SOURCE_ID
) VALUES (
    SRC.ARTISTID, SRC.NAME,
    'Y', CURRENT_DATE(), NULL, $SOURCE_ID
)

-- Changed artist: expire old version
WHEN MATCHED AND TGT.ARTIST_NAME <> SRC.NAME THEN UPDATE SET
    TGT.IS_CURRENT   = 'N',
    TGT.EFF_END_DATE = CURRENT_DATE();

-- Step 2: Insert new active version for changed artists
INSERT INTO DW.ARTIST_DIM (
    ARTIST_ID, ARTIST_NAME,
    IS_CURRENT, EFF_START_DATE, EFF_END_DATE, SOURCE_ID
)
SELECT
    S.ARTISTID, S.NAME,
    'Y', CURRENT_DATE(), NULL, $SOURCE_ID
FROM chinookDB.STAGE.Artist S
JOIN chinookDB.DW.ARTIST_DIM D
    ON  S.ARTISTID     = D.ARTIST_ID
    AND D.IS_CURRENT   = 'N'
    AND D.EFF_END_DATE = CURRENT_DATE();

-- ════════════════════════════════════════════════════════════
-- 5. LOAD SALES_FACT
--
--    Joins STAGE tables to DW dimensions to resolve surrogate keys.
--    KEY: DATE_DIM_KEY is resolved via TO_NUMBER(TO_CHAR(InvoiceDate,'YYYYMMDD'))
--    This matches the DATE_KEY format used when populating DATE_DIM.
--
--    Only IS_ACTIVE='Y' and IS_CURRENT='Y' dimension records are joined
--    so SALES_FACT always links to the current version of each dimension.
-- ════════════════════════════════════════════════════════════
INSERT INTO DW.SALES_FACT (
    CUSTOMER_KEY, ARTIST_KEY,
    INVOICE_ID, DATE_DIM_KEY,
    TOTAL_SALE_AMT, SOURCE_ID
)
SELECT
    CD.CUSTOMER_KEY,
    AD.ARTIST_KEY,
    I.INVOICEID,
    DD.DATE_KEY,    -- surrogate key, not raw date
    I.TOTAL,
    I.INVOICEID
FROM chinookDB.STAGE.Invoice      I
JOIN chinookDB.STAGE.InvoiceLine  IL  ON  I.INVOICEID   = IL.INVOICEID
JOIN chinookDB.STAGE.Album        ALB ON  IL.TRACKID    = ALB.ALBUMID
JOIN chinookDB.DW.CUSTOMER_DIM    CD  ON  I.CUSTOMERID  = CD.CUSTOMER_ID  AND CD.IS_ACTIVE  = 'Y'
JOIN chinookDB.DW.ARTIST_DIM      AD  ON  ALB.ARTISTID  = AD.ARTIST_ID    AND AD.IS_CURRENT = 'Y'
-- Convert InvoiceDate to YYYYMMDD integer to match DATE_DIM.DATE_KEY
JOIN chinookDB.DW.DATE_DIM        DD  ON  DD.DATE_KEY   = TO_NUMBER(TO_CHAR(I.INVOICEDATE, 'YYYYMMDD'));

-- ════════════════════════════════════════════════════════════
-- VERIFY — check row counts for all DW tables
-- ════════════════════════════════════════════════════════════
SELECT 'DATE_DIM'     AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM chinookDB.DW.DATE_DIM     UNION ALL
SELECT 'TIME_DIM',                   COUNT(*)              FROM chinookDB.DW.TIME_DIM     UNION ALL
SELECT 'CUSTOMER_DIM',               COUNT(*)              FROM chinookDB.DW.CUSTOMER_DIM UNION ALL
SELECT 'ARTIST_DIM',                 COUNT(*)              FROM chinookDB.DW.ARTIST_DIM   UNION ALL
SELECT 'SALES_FACT',                 COUNT(*)              FROM chinookDB.DW.SALES_FACT
ORDER BY TABLE_NAME;

-- Expected results:
-- DATE_DIM:     36,525
-- TIME_DIM:      1,440
-- CUSTOMER_DIM:     59
-- ARTIST_DIM:      275
-- SALES_FACT:    2,240

-- ════════════════════════════════════════════════════════════
-- END-TO-END VALIDATION
-- Joins all dimensions to confirm surrogate key resolution works
-- ════════════════════════════════════════════════════════════
SELECT
    DD.FULL_DATE,
    DD.YEAR_NUM,
    DD.QUARTER_NAME,
    AD.ARTIST_NAME,
    CD.FIRST_NAME || ' ' || CD.LAST_NAME AS CUSTOMER_NAME,
    CD.COUNTRY,
    SF.TOTAL_SALE_AMT
FROM chinookDB.DW.SALES_FACT    SF
JOIN chinookDB.DW.DATE_DIM      DD ON SF.DATE_DIM_KEY    = DD.DATE_KEY
JOIN chinookDB.DW.ARTIST_DIM    AD ON SF.ARTIST_KEY      = AD.ARTIST_KEY
JOIN chinookDB.DW.CUSTOMER_DIM  CD ON SF.CUSTOMER_KEY    = CD.CUSTOMER_KEY
ORDER BY DD.FULL_DATE DESC
LIMIT 10;
