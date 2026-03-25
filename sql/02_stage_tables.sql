-- ================================================================
-- 02_stage_tables.sql
-- Run as: CHINOOK_USER
-- Purpose: Create STAGE tables — exact mirrors of Azure SQL source
--
-- Design principle: STAGE tables are a raw landing zone.
-- No foreign keys, no extra columns, no transformations.
-- Column names and types must match the source exactly.
-- ================================================================

USE WAREHOUSE CHINOOK_WH;
USE DATABASE  chinookDB;
USE SCHEMA    chinookDB.STAGE;

CREATE OR REPLACE TABLE STAGE.Artist (
    ArtistId  INTEGER,
    Name      STRING(120)
);

CREATE OR REPLACE TABLE STAGE.Genre (
    GenreId  INTEGER,
    Name     STRING(120)
);

CREATE OR REPLACE TABLE STAGE.Album (
    AlbumId   INTEGER,
    Title     STRING(160),
    ArtistId  INTEGER       -- not a FK constraint — just a raw value
);

CREATE OR REPLACE TABLE STAGE.Customer (
    CustomerId    INTEGER,
    FirstName     STRING(40),
    LastName      STRING(20),
    Company       STRING(80),
    Address       STRING(70),
    City          STRING(40),
    State         STRING(40),
    Country       STRING(40),
    PostalCode    STRING(10),
    Phone         STRING(24),
    Fax           STRING(24),
    Email         STRING(60),
    SupportRepId  INTEGER
);

CREATE OR REPLACE TABLE STAGE.Invoice (
    InvoiceId          INTEGER,
    CustomerId         INTEGER,
    InvoiceDate        DATETIME,
    BillingAddress     STRING(70),
    BillingCity        STRING(40),
    BillingState       STRING(40),
    BillingCountry     STRING(40),
    BillingPostalCode  STRING(10),
    Total              NUMBER(10, 2)
);

CREATE OR REPLACE TABLE STAGE.InvoiceLine (
    InvoiceLineId  INTEGER,
    InvoiceId      INTEGER,
    TrackId        INTEGER,
    UnitPrice      NUMBER(10, 2),
    Quantity       INTEGER
);

-- Verify
SHOW TABLES IN SCHEMA chinookDB.STAGE;
-- Expected: 6 tables listed
