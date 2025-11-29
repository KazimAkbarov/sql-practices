/*
===============================================================================
Silver Layer Script
===============================================================================
Purpose:
    1. Creates tables in the 'silver' schema.
    2. Performs data cleaning (handling nulls, duplicates, standardizing formats).
    3. Loads cleaned data from Bronze to Silver.
===============================================================================
*/

Use DataWarehouse;
Go

If OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    Drop Table silver.crm_cust_info;
Go

Create Table silver.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE,
    dwh_created_date Datetime2 Default getdate()
);
Go

If OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    Drop Table silver.crm_prd_info;
Go

Create Table silver.crm_prd_info (
    prd_id       INT,
    cat_id       NVARCHAR(50),
    prd_key      NVARCHAR(50),
    prd_nm      NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME,
    dwh_created_date Datetime2 Default getdate()
);
Go

If OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    Drop Table silver.crm_sales_details;
Go

Create Table silver.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt Date,
    sls_ship_dt  Date,
    sls_due_dt   Date,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
    dwh_created_date Datetime2 Default getdate()
);
Go

If OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    Drop Table silver.erp_loc_a101;
Go

Create Table silver.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50),
    dwh_created_date Datetime2 Default getdate()
);
Go

If OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    Drop Table silver.erp_cust_az12;
Go

Create Table silver.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50),
    dwh_created_date Datetime2 Default getdate()
);
Go

If OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    Drop Table silver.erp_px_cat_g1v2;
Go

Create Table silver.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50),
    dwh_created_date Datetime2 Default getdate()
);
Go



-- ============================================================================
-- Cleaning and Loading Cleaned Data from Bronze to Silver CRM_CUST_INFO table
-- ============================================================================

-- Finding Duplicated values in Primary Keys Columns CRM_CUST_INFO table

Select cst_id, Count(*) 
from bronze.crm_cust_info 
Group by cst_id
Having Count(*) >1;

Select * 
from (
select *, ROW_NUMBER () over (Partition by cst_id Order by cst_create_date Desc) as flag_last 
from bronze.crm_cust_info) t
where flag_last != 1 -- Select the most recent record per customer


-- Checking for unwanted spaced

Select cst_firstname
from silver.crm_cust_info
where cst_firstname != Trim(cst_firstname)

Select cst_lastname
from bronze.crm_cust_info
where cst_lastname != Trim(cst_lastname)



-- Insterting Cleaned Values to Silver Tables
-- ==========================================================================================

Truncate table silver.crm_cust_info

Insert into silver.crm_cust_info  (cst_id, cst_key,cst_firstname,cst_lastname,cst_marital_status, cst_gndr, cst_create_date)
    Select 
        cst_id, 
        cst_key,
        Trim(cst_firstname) as cst_firstname,
        Trim(cst_lastname) as cst_lastname,
        Case When Upper(Trim(cst_marital_status)) = 'S' then 'Single'
             When Upper(Trim(cst_marital_status)) = 'M' then 'Married'
             Else 'n/a'
        end cst_marital_status,
        Case When Upper(Trim(cst_gndr)) = 'F' then 'Female'
             When Upper(Trim(cst_gndr)) = 'M' then 'Male'
             Else 'n/a'
        end cst_gndr,
        cst_create_date
     from (
        select *, ROW_NUMBER () over (Partition by cst_id Order by cst_create_date Desc) as flag_last 
        from bronze.crm_cust_info) t
        where flag_last = 1;
Go

-- Select * from Silver.crm_cust_info


-- ============================================================================
-- Cleaning and Loading Cleaned Data from Bronze to Silver CRM_PRD_INFO table
-- ============================================================================

-- Finding Duplicated values in Primary Keys Columns CRM_PRD_INFO table

Select prd_id, count(*)
from bronze.crm_prd_info
Group by prd_id
Having Count(*) >1 or prd_id is Null


-- Checking for unwanted spaced

Select prd_nm
from silver.crm_prd_info
where prd_nm != Trim(prd_nm)

-- Checking Nulls or Negative Numbers

Select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is Null

Select * from bronze.crm_prd_info
where prd_end_dt < prd_start_dt


-- Insterting Cleaned Values to Silver Tables

Truncate table silver.crm_prd_info

Insert Into silver.crm_prd_info(prd_id,cat_id, prd_key, prd_nm, prd_cost,prd_line, prd_start_dt,prd_end_dt)
    Select 
        prd_id, 
        Replace(Substring(prd_key, 1, 5), '-', '_') as cat_id,
        Substring(prd_key, 7, Len(prd_key)) as prd_key,
        prd_nm,
        Isnull(prd_cost, 0) as prd_cost,
        Case Upper(Trim(prd_line))
             When 'M' Then 'Mountain'
             When 'R' Then 'Road'
             When 'S' Then 'Other Sales'
             When 'T' Then 'Touring'
             Else 'n/a'
        End as prd_line,
        Cast (prd_start_dt as Date) as prd_start_dt,
        Lead (prd_start_dt) Over (Partition by prd_key Order By prd_start_dt)-1 as prd_end_dt
    from bronze.crm_prd_info;
    Go

-- select * from Silver.crm_prd_info


-- ===============================================================================
-- Cleaning and Loading Cleaned Data from Bronze to Silver CRM_Sales_Details table
-- ===============================================================================

Select 
    sls_sales, 
    sls_quantity, 
    sls_price
from bronze.crm_sales_details
    where sls_sales != sls_quantity*sls_price
    or sls_sales Is Null or sls_quantity Is Null or sls_price Is Null
    or sls_sales <= 0 or sls_quantity <=0 or sls_price <= 0
    order by sls_sales,sls_quantity,sls_price

-- Insterting Cleaned Values to Silver Tables

Truncate table silver.crm_sales_details

Insert Into silver.crm_sales_details (sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_price,sls_quantity)
    Select 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        Case When sls_order_dt = 0 or Len(sls_order_dt) !=8 Then Null
             Else Cast(Cast(sls_order_dt as Varchar) as Date)
        End as sls_order_dt,
        Case When sls_ship_dt = 0 or Len(sls_ship_dt) !=8 Then Null
             Else Cast(Cast(sls_ship_dt as Varchar) as Date)
        End as sls_ship_dt,
        Case When sls_due_dt = 0 or Len(sls_due_dt) !=8 Then Null
             Else Cast(Cast(sls_due_dt as Varchar) as Date)
        End as sls_due_dt,
        Case When sls_sales Is Null or sls_sales <=0 or sls_sales != sls_quantity*ABS(sls_price)
             Then sls_quantity*ABS(sls_price)
             Else sls_sales
        End as sls_sales,
        Case When sls_price Is Null or sls_sales <=0 
             Then sls_sales/Nullif(sls_quantity,0)
             Else sls_price
        End as sls_price,
        sls_quantity
    from bronze.crm_sales_details;
Go

--select * from silver.crm_sales_details
--where sls_quantity !=1


-- ===========================================================================
-- Cleaning and Loading Cleaned Data from Brinze to Silver ERP Customer table
-- ===========================================================================

Truncate table silver.erp_cust_az12

Insert Into silver.erp_cust_az12 (cid, bdate, gen)
    Select 
        Case When cid like 'NAS%' Then Substring (cid, 4, Len(cid))
             Else cid
        End as cid,
        Case When bdate > Getdate() Then Null
             Else bdate
        End as bdate,
        Case When Upper(Trim(gen)) in ('F', 'Female') Then 'Female'
             When Upper(Trim(gen)) in ('M', 'Male') Then 'Male'
             Else 'n/a'
        End as gen
    from bronze.erp_cust_az12;
Go

-- select * from silver.erp_cust_az12;


-- ===========================================================================
-- Cleaning and Loading Cleaned Data from Brinze to Silver ERP Loc_A101 table
-- ===========================================================================

Truncate table silver.erp_loc_a101

Insert Into silver.erp_loc_a101 (cid, cntry)
    Select 
        Replace(cid, '-', '') cid,
        Case When Trim(cntry) = 'DE' Then 'Germany'
             When Trim(cntry) IN ('US', 'USA') Then 'United States'
             When Trim(cntry) = '' or Trim(cntry) Is Null Then 'n/a'
             Else Trim(cntry)
        End as cntry
    from bronze.erp_loc_a101;
Go

-- Select * from silver.erp_loc_a101


-- ==============================================================================
-- Cleaning and Loading Cleaned Data from Bronze to Silver ERP PX_CAT_G1V2 table
-- ==============================================================================

Truncate table silver.erp_px_cat_g1v2

Insert Into silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
    Select id, cat,subcat, maintenance
    from bronze.erp_px_cat_g1v2;
Go

-- select * from silver.erp_px_cat_g1v2
