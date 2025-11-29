/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

Use DataWarehouse;
Go

If OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    Drop Table bronze.crm_cust_info;
Go

Create Table bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);
Go

If OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    Drop Table bronze.crm_prd_info;
Go

Create Table bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);
Go

If OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    Drop Table bronze.crm_sales_details;
Go

Create Table bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
Go

If OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    Drop Table bronze.erp_loc_a101;
Go

Create Table bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
Go

If OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    Drop Table bronze.erp_cust_az12;
Go

Create Table bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
Go

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    Drop Table bronze.erp_px_cat_g1v2;
Go

Create Table bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
Go


-- Bulk Insert to Table
-- ===================================================
 If OBJECT_ID('load_bronze', 'P') IS NOT NULL
    Drop Procedure load_bronze
Go


    Declare @start_time DateTime, @end_time DateTime;
    Begin try

        Print '==================================================================================';
        Print 'Loading Bronze Layer';
        Print '==================================================================================';

        Print '-----------------------------------------------------';
        Print 'Loading CRM Tables';
        Print '-----------------------------------------------------';

        Set @start_time = GETDATE();
        Print '>> Truncating Table: bronze.crm_cust_info'
        Truncate table bronze.crm_cust_info;

        Print '>> Inserting Data Into: bronze.crm_cust_info'
        Bulk insert bronze.crm_cust_info
        from 'C:\Users\Kazim Akbarov\Documents\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        with (
        firstrow = 2, 
        Fieldterminator = ',',
        Tablock)
        -- select count(*) from bronze.crm_cust_info;
            Set @end_time = GETDATE();
            Print '>> Load Duration: '+  Cast(DateDiff(second, @start_time, @end_time) as Nvarchar) + ' seconds';
        
        Set @start_time = GETDATE();
        Print '>> Truncating Table: bronze.crm_prd_info'
        Truncate table bronze.crm_prd_info;

        Print '>> Inserting Data Into: bronze.crm_prd_info'
        Bulk insert bronze.crm_prd_info
        from 'C:\Users\Kazim Akbarov\Documents\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        with (
        firstrow = 2, 
        Fieldterminator = ',',
        Tablock)
        -- select count(*) from bronze.crm_prd_info;
            Set @end_time = GETDATE();
            Print '>> Load Duration: '+  Cast(DateDiff(second, @start_time, @end_time) as Nvarchar) + ' seconds';
        
        Set @start_time = GETDATE();
        Print '>> Truncating Table: bronze.crm_sales_details'
        Truncate table bronze.crm_sales_details;

        Print '>> Inserting Data Into: bronze.crm_sales_details'
        Bulk insert bronze.crm_sales_details
        from 'C:\Users\Kazim Akbarov\Documents\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        with (
        firstrow = 2, 
        Fieldterminator = ',',
        Tablock)
        -- select count(*) from bronze.crm_sales_details;
            Set @end_time = GETDATE();
            Print '>> Load Duration: '+  Cast(DateDiff(second, @start_time, @end_time) as Nvarchar) + ' seconds';

        Print '-----------------------------------------------------';
        Print 'Loading ERP Tables';
        Print '-----------------------------------------------------';


        Set @start_time = GETDATE();
        Print '>> Truncating Table: bronze.erp_loc_a101'
        Truncate table bronze.erp_loc_a101;

        Print '>> Inserting Data Into: bronze.erp_loc_a101'
        Bulk insert bronze.erp_loc_a101
        from 'C:\Users\Kazim Akbarov\Documents\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        with (
        firstrow = 2, 
        Fieldterminator = ',',
        Tablock)
        -- select count(*) from bronze.erp_loc_a101;
            Set @end_time = GETDATE();
            Print '>> Load Duration: '+  Cast(DateDiff(second, @start_time, @end_time) as Nvarchar) + ' seconds';

        Set @start_time = GETDATE();
        Print '>> Truncating Table: bronze.erp_cust_az12'
        Truncate table bronze.erp_cust_az12;

        Print '>> Inserting Data Into: bronze.erp_cust_az12'
        Bulk insert bronze.erp_cust_az12
        from 'C:\Users\Kazim Akbarov\Documents\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        with (
        firstrow = 2, 
        Fieldterminator = ',',
        Tablock)
        -- select count(*) from bronze.erp_cust_az12;
            Set @end_time = GETDATE();
            Print '>> Load Duration: '+  Cast(DateDiff(second, @start_time, @end_time) as Nvarchar) + ' seconds';

        Set @start_time = GETDATE();
        Print '>> Truncating Table: bronze.erp_px_cat_g1v2'
        Truncate table bronze.erp_px_cat_g1v2;

        Print '>> Inserting Data Into: bronze.erp_px_cat_g1v2'
        Bulk insert bronze.erp_px_cat_g1v2
        from 'C:\Users\Kazim Akbarov\Documents\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        with (
        firstrow = 2, 
        Fieldterminator = ',',
        Tablock)
        -- select count(*) from bronze.erp_px_cat_g1v2;
            Set @end_time = GETDATE();
            Print '>> Load Duration: '+  Cast(DateDiff(second, @start_time, @end_time) as Nvarchar) + ' seconds';

    End Try
    Begin Catch
        Print '==================================================================================';
        Print 'Error Occured During Loading Bronze Layer';
        Print 'Error Message' + Error_Message();
        Print 'Error Message' + Cast(Error_Number() as NVarchar)
        Print '==================================================================================';
    End Catch
Go


-- Creating Procedure for Bronze Layer
-- ====================================================================

Create or Alter Procedure bronze.load_bronze as 
Begin 
    Declare @start_time DateTime, @end_time DateTime;
    Begin try

        Print '==================================================================================';
        Print 'Loading Bronze Layer';
        Print '==================================================================================';

        Print '-----------------------------------------------------';
        Print 'Loading CRM Tables';
        Print '-----------------------------------------------------';

        Set @start_time = GETDATE();
        Print '>> Truncating Table: bronze.crm_cust_info'
        Truncate table bronze.crm_cust_info;

        Print '>> Inserting Data Into: bronze.crm_cust_info'
        Bulk insert bronze.crm_cust_info
        from 'C:\Users\Kazim Akbarov\Documents\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        with (
        firstrow = 2, 
        Fieldterminator = ',',
        Tablock)
        -- select count(*) from bronze.crm_cust_info;
            Set @end_time = GETDATE();
            Print '>> Load Duration: '+  Cast(DateDiff(second, @start_time, @end_time) as Nvarchar) + ' seconds';
        
        Set @start_time = GETDATE();
        Print '>> Truncating Table: bronze.crm_prd_info'
        Truncate table bronze.crm_prd_info;

        Print '>> Inserting Data Into: bronze.crm_prd_info'
        Bulk insert bronze.crm_prd_info
        from 'C:\Users\Kazim Akbarov\Documents\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        with (
        firstrow = 2, 
        Fieldterminator = ',',
        Tablock)
        -- select count(*) from bronze.crm_prd_info;
            Set @end_time = GETDATE();
            Print '>> Load Duration: '+  Cast(DateDiff(second, @start_time, @end_time) as Nvarchar) + ' seconds';
        
        Set @start_time = GETDATE();
        Print '>> Truncating Table: bronze.crm_sales_details'
        Truncate table bronze.crm_sales_details;

        Print '>> Inserting Data Into: bronze.crm_sales_details'
        Bulk insert bronze.crm_sales_details
        from 'C:\Users\Kazim Akbarov\Documents\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        with (
        firstrow = 2, 
        Fieldterminator = ',',
        Tablock)
        -- select count(*) from bronze.crm_sales_details;
            Set @end_time = GETDATE();
            Print '>> Load Duration: '+  Cast(DateDiff(second, @start_time, @end_time) as Nvarchar) + ' seconds';

        Print '-----------------------------------------------------';
        Print 'Loading ERP Tables';
        Print '-----------------------------------------------------';


        Set @start_time = GETDATE();
        Print '>> Truncating Table: bronze.erp_loc_a101'
        Truncate table bronze.erp_loc_a101;

        Print '>> Inserting Data Into: bronze.erp_loc_a101'
        Bulk insert bronze.erp_loc_a101
        from 'C:\Users\Kazim Akbarov\Documents\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        with (
        firstrow = 2, 
        Fieldterminator = ',',
        Tablock)
        -- select count(*) from bronze.erp_loc_a101;
            Set @end_time = GETDATE();
            Print '>> Load Duration: '+  Cast(DateDiff(second, @start_time, @end_time) as Nvarchar) + ' seconds';

        Set @start_time = GETDATE();
        Print '>> Truncating Table: bronze.erp_cust_az12'
        Truncate table bronze.erp_cust_az12;

        Print '>> Inserting Data Into: bronze.erp_cust_az12'
        Bulk insert bronze.erp_cust_az12
        from 'C:\Users\Kazim Akbarov\Documents\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        with (
        firstrow = 2, 
        Fieldterminator = ',',
        Tablock)
        -- select count(*) from bronze.erp_cust_az12;
            Set @end_time = GETDATE();
            Print '>> Load Duration: '+  Cast(DateDiff(second, @start_time, @end_time) as Nvarchar) + ' seconds';

        Set @start_time = GETDATE();
        Print '>> Truncating Table: bronze.erp_px_cat_g1v2'
        Truncate table bronze.erp_px_cat_g1v2;

        Print '>> Inserting Data Into: bronze.erp_px_cat_g1v2'
        Bulk insert bronze.erp_px_cat_g1v2
        from 'C:\Users\Kazim Akbarov\Documents\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        with (
        firstrow = 2, 
        Fieldterminator = ',',
        Tablock)
        -- select count(*) from bronze.erp_px_cat_g1v2;
            Set @end_time = GETDATE();
            Print '>> Load Duration: '+  Cast(DateDiff(second, @start_time, @end_time) as Nvarchar) + ' seconds';

    End Try
    Begin Catch
        Print '==================================================================================';
        Print 'Error Occured During Loading Bronze Layer';
        Print 'Error Message' + Error_Message();
        Print 'Error Message' + Cast(Error_Number() as NVarchar)
        Print '==================================================================================';
    End Catch
End;
