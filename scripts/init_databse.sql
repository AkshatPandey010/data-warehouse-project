/*
==================================================================
Create Database and Schemas
==================================================================
Script Purpose:
        This script creates a new database named 'DataWarehouse' after checking if it already exists.
        If the database exists, it is dropped and recreated. Additionally, the script sets up three scheams
        within the database: 'bronze', 'silver' and 'gold'.
WARNING:
        RUNNING THIS SCRIPT WILL DROP THE ENTIRE 'DataWarehouse' DATABASE IF IT EXISTS.
        ALL DATA IN THE DATABSE WILL BE PERMANENTLY DELETED. PROCEED WITH CAUTION AND ENSURE YOU HAVE
        PROPER BACKUPS BEFORE RUNNING THIS SCRIPT.
*/


USE master;
GO
-- Drop nd recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases where name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE
    DROP DATABASE dataWarehouse;
END
GO

-- Creating the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- CREATE SCHEMAS 
CREATE SCHEMA bronze;
GO 
  
CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
