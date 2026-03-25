-- ================================================================
-- 01_snowflake_setup.sql
-- Run as: ACCOUNTADMIN
-- Purpose: Create warehouse, database, schemas, role, and user
-- ================================================================

-- Compute warehouse
CREATE OR REPLACE WAREHOUSE CHINOOK_WH
    WAREHOUSE_SIZE = 'XSMALL';

-- Database
CREATE OR REPLACE DATABASE chinookDB;

-- Schemas
CREATE OR REPLACE SCHEMA chinookDB.STAGE;  -- raw data landing zone
CREATE OR REPLACE SCHEMA chinookDB.DW;     -- dimensional model

-- Role
CREATE OR REPLACE ROLE CHINOOK_ROLE;

-- Grant role access to warehouse, database, and both schemas
GRANT USAGE ON WAREHOUSE CHINOOK_WH  TO ROLE CHINOOK_ROLE;
GRANT USAGE ON DATABASE  chinookDB   TO ROLE CHINOOK_ROLE;
GRANT ALL   ON SCHEMA chinookDB.STAGE TO ROLE CHINOOK_ROLE;
GRANT ALL   ON SCHEMA chinookDB.DW    TO ROLE CHINOOK_ROLE;

-- Create user with role and warehouse as defaults
CREATE OR REPLACE USER CHINOOK_USER
    PASSWORD          = 'ChinookPass123!'
    DEFAULT_ROLE      = CHINOOK_ROLE
    DEFAULT_WAREHOUSE = CHINOOK_WH;

GRANT ROLE CHINOOK_ROLE TO USER CHINOOK_USER;

-- Verify: log out and log back in as CHINOOK_USER, then run:
-- USE ROLE CHINOOK_ROLE;
-- USE WAREHOUSE CHINOOK_WH;
-- SHOW SCHEMAS IN DATABASE chinookDB;
-- Expected: STAGE and DW listed
