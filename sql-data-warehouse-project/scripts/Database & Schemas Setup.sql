/*
===============================================================================
Database Initialization Script
===============================================================================
Purpose:
    1. Recreates the 'DataWarehouse' database (Drops if exists).
    2. Creates the 3-Layer Schema Architecture:
       - 'bronze': Raw Data
       - 'silver': Cleaned & Standardized Data
       - 'gold':   Business Ready Data
===============================================================================
*/

Use master;
Go

If Exists ( Select 1 from sys.databases where name = 'DataWarehouse')

Begin 
 Alter DataBase DataWarehouse Set Single_User with rollback immediate;
 Drop Database DataWarehouse;
 End;


Create Database DataWarehouse;
Go

use DataWarehouse;
Go

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze') EXEC('CREATE SCHEMA [bronze]');
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver') EXEC('CREATE SCHEMA [silver]');
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')   EXEC('CREATE SCHEMA [gold]');
Go
