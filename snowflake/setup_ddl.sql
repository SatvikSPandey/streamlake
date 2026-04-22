-- StreamLake - Snowflake setup DDL
-- Creates database, schema, and warehouse for loading Gold tables from S3.
-- Runs once (idempotent - IF NOT EXISTS clauses).

-- Warehouse (compute resource) - XS is cheapest, auto-suspend to minimize credit burn
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'StreamLake default compute warehouse';

-- Database - logical grouping of schemas
CREATE DATABASE IF NOT EXISTS STREAMLAKE_DB
    COMMENT = 'StreamLake data warehouse - Gold layer tables loaded from S3 Delta Lake';

-- Schemas mirroring Medallion Architecture
CREATE SCHEMA IF NOT EXISTS STREAMLAKE_DB.GOLD
    COMMENT = 'Gold layer - business-ready aggregate tables';

-- Verify
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STREAMLAKE_DB;
USE SCHEMA GOLD;

SHOW WAREHOUSES LIKE 'COMPUTE_WH';
SHOW DATABASES LIKE 'STREAMLAKE_DB';
SHOW SCHEMAS IN DATABASE STREAMLAKE_DB;