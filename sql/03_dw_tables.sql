-- ================================================================
-- 03_dw_tables.sql
-- Run as: CHINOOK_USER
-- Purpose: Create the dimensional model in DW schema
--
-- Design decisions:
--   - AUTOINCREMENT for surrogate keys (sequences fail in MERGE)
--   - SCD Type 2 on CUSTOMER_DIM and ARTIST_DIM (EFF dates + IS_ACTIVE/IS_CURRENT)
--   - ROW_HASH VARCHAR(128) on CUSTOMER_DIM for SHA-256 change detection
--   - SALES_FACT stores DATE_DIM_KEY (integer FK), never a raw date
-- ================================================================

USE WAREHOUSE CHINOOK_WH;
USE DATABASE  chinookDB;
USE SCHEMA    chinookDB.DW;

-- ── DATE_DIM ──────────────────────────────────────────────────
-- Populated via GENERATOR function — not loaded from source
-- DATE_KEY format: YYYYMMDD integer (e.g. 20060115)
CREATE OR REPLACE TABLE DW.DATE_DIM (
    DATE_KEY             NUMBER(10)  NOT NULL PRIMARY KEY,
    FULL_DATE            DATE        NOT NULL,
    DAY_NUM              NUMBER(3),
    WEEKDAY_ABBR         VARCHAR(3),
    WEEKDAY_NUM          NUMBER(1),
    DAY_OF_YEAR_NUM      NUMBER(3),
    WEEK_OF_YEAR         NUMBER(2),
    MONTH_NUM            NUMBER(2),
    MONTH_ABBR           VARCHAR(3),
    QUARTER_NUM          NUMBER(1),
    QUARTER_NAME         VARCHAR(3),
    YEAR_NUM             NUMBER(4),
    FIRST_DAY_OF_MONTH   DATE,
    LAST_DAY_OF_MONTH    DATE,
    IS_WEEKEND           VARCHAR(1)  -- 'Y' or 'N'
);

-- ── TIME_DIM ──────────────────────────────────────────────────
-- TIME_KEY = minutes since midnight (0-1439)
CREATE OR REPLACE TABLE DW.TIME_DIM (
    TIME_KEY    NUMBER(4)  NOT NULL PRIMARY KEY,
    HOUR_NUM    INTEGER,
    MINUTE_NUM  INTEGER,
    TIME_24_HR  STRING     -- e.g. '09:30'
);

-- ── CUSTOMER_DIM ──────────────────────────────────────────────
-- SCD Type 2: historical versions preserved via EFF dates + IS_ACTIVE
-- ROW_HASH: SHA-256 fingerprint of all tracked columns for change detection
CREATE OR REPLACE TABLE DW.CUSTOMER_DIM (
    CUSTOMER_KEY       NUMBER(10)  AUTOINCREMENT NOT NULL PRIMARY KEY,
    CUSTOMER_ID        NUMBER(10),
    FIRST_NAME         VARCHAR(100),
    LAST_NAME          VARCHAR(100),
    COMPANY_NAME       VARCHAR(100),
    CITY               VARCHAR(100),
    STATE              VARCHAR(50),
    COUNTRY            VARCHAR(50),
    ZIP_CODE           VARCHAR(10),
    EMPLOYEE_ID        NUMBER(10),
    ROW_HASH           VARCHAR(128),            -- SHA-256 = 64 chars; VARCHAR(128) for future SHA-512
    IS_ACTIVE          VARCHAR(1) DEFAULT 'Y',  -- 'Y' = current version, 'N' = expired
    EFF_START_DATE     DATE,
    EFF_END_DATE       DATE,                    -- NULL while IS_ACTIVE = 'Y'
    SOURCE_ID          VARCHAR(50),
    DATE_TO_WAREHOUSE  DATETIME DEFAULT CURRENT_TIMESTAMP()
);

-- ── ARTIST_DIM ────────────────────────────────────────────────
-- SCD Type 2: tracks artist name changes over time
CREATE OR REPLACE TABLE DW.ARTIST_DIM (
    ARTIST_KEY         NUMBER(10)  AUTOINCREMENT NOT NULL PRIMARY KEY,
    ARTIST_ID          NUMBER(10),
    ARTIST_NAME        VARCHAR(150),
    IS_CURRENT         VARCHAR(1) DEFAULT 'Y',  -- 'Y' = current version
    EFF_START_DATE     DATE,
    EFF_END_DATE       DATE,                    -- NULL while IS_CURRENT = 'Y'
    SOURCE_ID          VARCHAR(50),
    DATE_TO_WAREHOUSE  DATETIME DEFAULT CURRENT_TIMESTAMP()
);

-- ── SALES_FACT ────────────────────────────────────────────────
-- Central fact table. DATE_DIM_KEY is a surrogate FK — never store raw dates.
-- Join to DATE_DIM to access any date attribute (day of week, quarter, etc.)
CREATE OR REPLACE TABLE DW.SALES_FACT (
    SALES_KEY          NUMBER(10)  AUTOINCREMENT NOT NULL PRIMARY KEY,
    CUSTOMER_KEY       NUMBER(10),              -- FK → CUSTOMER_DIM
    ARTIST_KEY         NUMBER(10),              -- FK → ARTIST_DIM
    INVOICE_ID         NUMBER(10),
    DATE_DIM_KEY       NUMBER(10),              -- FK → DATE_DIM (YYYYMMDD integer)
    TOTAL_SALE_AMT     NUMBER(10, 2),
    SOURCE_ID          NUMBER(10),
    DATE_TO_WAREHOUSE  DATETIME DEFAULT CURRENT_TIMESTAMP()
);

-- Verify
SHOW TABLES IN SCHEMA chinookDB.DW;
-- Expected: DATE_DIM, TIME_DIM, CUSTOMER_DIM, ARTIST_DIM, SALES_FACT
