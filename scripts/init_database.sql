/* 
===========================================================
Create Database & Schemas
===========================================================

Purpose: This script creates the new database called DataWarehouse and Schemas bronze, silver, gold

Warnings: Backup the data in your datawarehouse before running this script.
*/


-- Create the database
CREATE DATABASE DataWarehouse;

-- Use the database
USE DataWarehouse;

-- create bronze schema
CREATE SCHEMA bronze;

-- create silver schema
CREATE SCHEMA sliver;

-- create gold schema
CREATE SCHEMA gold;
