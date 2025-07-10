/*
===========================================================
Create Database and Schemas
===========================================================
Script purpose:
  This script creates a new database named 'DataWarehouse' after chekinh if it already exists.
  If the database exsts, it is dropped an recreated. Additionally, the scrpt sets up three schemas within the database: 'bronze', 'silver', and 'gold'.
WARNING:
  Running this script will drop the entire 'DataWarehouse' database if it exists.
  All data in the databasewill be permanently deleted. Proceed with caution 
  and ensure you have proper backups before running this script.
  
*/

USE master;
GO

--Drop and recreate the 'Datawarehouse' database
IF EXISTS( SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE USER WITH ROLLBACK INMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO

  --Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

--Create Schemas
CREATE SCHEMA bronze;
GO

--Create Schemas
CREATE SCHEMA silver;
GO

--Create Schemas
CREATE SCHEMA gold;
GO
